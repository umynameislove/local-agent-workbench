from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    ApprovalRepository,
    Database,
    JobRepository,
    PlannerAlreadyExistsError,
    PlannerNotFoundError,
    PlannerPromotionConflictError,
    PlannerPromotionRecord,
    PlannerRepository,
    PlannerRepositoryError,
    PlannerValidationError,
    ProjectRepository,
)
from engine import (
    ApprovalCreate,
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    PlannerItemCreate,
    PlannerItemKind,
    ProjectConfig,
    Sensitivity,
)

NOW = datetime(2030, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)


def project(project_id: str = "alpha") -> ProjectConfig:
    return ProjectConfig(
        id=project_id,
        root=f"/workspace/{project_id}",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def planner_item(**overrides: Any) -> PlannerItemCreate:
    values: dict[str, Any] = {
        "id": "planner-001",
        "kind": PlannerItemKind.DEADLINE,
        "title": "Submit the reviewed deliverable",
        "project_id": "alpha",
        "details": "Confirm the final checks before submission.",
        "due_at": NOW + timedelta(days=1),
        "source": None,
        "source_key": None,
    }
    values.update(overrides)
    return PlannerItemCreate(**values)


def promotion_job(job_id: str = "job-from-planner", **overrides: Any) -> JobCreate:
    values: dict[str, Any] = {
        "id": job_id,
        "project_id": "alpha",
        "request": "Prepare the planner item for completion.",
        "request_snapshot": {
            "planner_item_id": "planner-001",
            "policy_version": 1,
        },
        "state": JobState.CREATED,
        "runtime": JobRuntime.AUTO,
        "model": None,
        "worktree_path": None,
    }
    values.update(overrides)
    return JobCreate(**values)


def initialized_repository(
    tmp_path: Path,
) -> tuple[Database, PlannerRepository, JobRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    return database, PlannerRepository(database), JobRepository(database)


def test_version_six_database_adds_planner_without_losing_state(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:6])
    assert legacy.initialize() == 6
    ProjectRepository(legacy).create(project())
    expected_job = JobRepository(legacy).create(promotion_job("existing-job"))
    expected_approval = ApprovalRepository(legacy).create(
        ApprovalCreate(
            id="approval-001",
            job_id=expected_job.id,
            payload={"action": "review"},
            expires_at=NOW + timedelta(days=2),
        )
    )

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION
    assert JobRepository(upgraded).get(expected_job.id) == expected_job
    assert ApprovalRepository(upgraded).get(expected_approval.id) == expected_approval
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name LIKE 'planner%'
            ORDER BY type, name
            """
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert objects == [
        ("index", "planner_kind_due_idx"),
        ("index", "planner_project_created_idx"),
        ("index", "planner_promoted_job_idx"),
        ("table", "planner"),
        ("trigger", "planner_protect_promoted_delete"),
        ("trigger", "planner_require_pending_insert"),
        ("trigger", "planner_validate_promotion"),
    ]


def test_every_item_kind_round_trips_and_filters_after_restart(tmp_path: Path) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    deadline = repository.create(planner_item())
    blocker = repository.create(
        planner_item(
            id="blocker-001",
            kind=PlannerItemKind.BLOCKER,
            title="Waiting for review input",
            due_at=None,
            details=None,
        )
    )
    watcher = repository.create(
        planner_item(
            id="watcher-001",
            kind=PlannerItemKind.WATCHER,
            title="Repository issue needs triage",
            project_id=None,
            due_at=None,
            source="github",
            source_key="issue-42",
        )
    )
    restarted = PlannerRepository(Database(database.path))
    assert restarted.database.initialize() == LATEST_SCHEMA_VERSION

    assert deadline.due_at == NOW + timedelta(days=1)
    assert restarted.get(blocker.id) == blocker
    assert restarted.get(watcher.id) == watcher
    assert restarted.list(kind=PlannerItemKind.BLOCKER) == (blocker,)
    assert restarted.list(project_id="alpha") == (deadline, blocker)
    assert restarted.list(promoted=False) == (deadline, blocker, watcher)


def test_due_time_is_normalized_to_utc_without_losing_microseconds(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    offset = timezone(timedelta(hours=7))
    local_due = datetime(2030, 1, 3, 10, 4, 5, 123456, tzinfo=offset)

    stored = repository.create(planner_item(due_at=local_due))

    assert stored.due_at == datetime(2030, 1, 3, 3, 4, 5, 123456, tzinfo=UTC)


def test_matching_promotion_retry_returns_one_job_after_restart(tmp_path: Path) -> None:
    database, repository, jobs = initialized_repository(tmp_path)
    repository.create(planner_item())
    expected = repository.promote("planner-001", promotion_job())
    equivalent = promotion_job(
        request_snapshot={"policy_version": 1, "planner_item_id": "planner-001"}
    )

    restarted = PlannerRepository(Database(database.path))
    assert restarted.database.initialize() == LATEST_SCHEMA_VERSION
    assert restarted.promote("planner-001", equivalent) == expected
    assert restarted.get("planner-001") == expected.item
    assert JobRepository(restarted.database).get(expected.job.id) == expected.job
    assert jobs.list() == (expected.job,)
    assert expected.item.promoted_job_id == expected.job.id
    assert expected.item.promoted_at is not None


@pytest.mark.parametrize(
    "conflicting",
    [
        promotion_job("different-job"),
        promotion_job(project_id="different-project"),
        promotion_job(request="Use a different request."),
        promotion_job(request_snapshot={"planner_item_id": "different"}),
        promotion_job(state=JobState.QUEUED),
        promotion_job(runtime=JobRuntime.CODEX),
        promotion_job(model="gpt-5.6-codex"),
        promotion_job(worktree_path="/runtime/worktrees/other"),
    ],
)
def test_conflicting_promotion_retry_is_rejected(tmp_path: Path, conflicting: JobCreate) -> None:
    _, repository, jobs = initialized_repository(tmp_path)
    repository.create(planner_item())
    expected = repository.promote("planner-001", promotion_job())

    with pytest.raises(PlannerPromotionConflictError):
        repository.promote("planner-001", conflicting)

    assert repository.get("planner-001") == expected.item
    assert jobs.list() == (expected.job,)


def test_concurrent_matching_promotions_create_one_job(tmp_path: Path) -> None:
    _, repository, jobs = initialized_repository(tmp_path)
    repository.create(planner_item())

    with ThreadPoolExecutor(max_workers=12) as executor:
        records = tuple(
            executor.map(
                lambda _: repository.promote("planner-001", promotion_job()),
                range(24),
            )
        )

    assert all(record == records[0] for record in records)
    assert jobs.list() == (records[0].job,)


def test_concurrent_conflicting_promotions_allow_one_winner(tmp_path: Path) -> None:
    _, repository, jobs = initialized_repository(tmp_path)
    repository.create(planner_item())

    def submit(job_id: str) -> object:
        try:
            return repository.promote("planner-001", promotion_job(job_id))
        except PlannerPromotionConflictError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(submit, ("job-alpha", "job-beta")))

    stored_jobs = jobs.list()
    assert len(stored_jobs) == 1
    assert sum(isinstance(item, PlannerPromotionRecord) for item in outcomes) == 1
    assert sum(isinstance(item, PlannerPromotionConflictError) for item in outcomes) == 1
    assert repository.get("planner-001").promoted_job_id == stored_jobs[0].id


def test_promotion_failure_rolls_back_job_insert(tmp_path: Path) -> None:
    database, repository, jobs = initialized_repository(tmp_path)
    pending = repository.create(planner_item())
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_test_promotion
            BEFORE UPDATE ON planner
            BEGIN
                SELECT RAISE(ABORT, 'Injected promotion failure.');
            END
            """
        )

    with pytest.raises(PlannerRepositoryError, match="could not be promoted"):
        repository.promote(pending.id, promotion_job())

    assert repository.get(pending.id) == pending
    assert jobs.list() == ()


def test_missing_and_conflicting_references_fail_without_partial_state(tmp_path: Path) -> None:
    database, repository, jobs = initialized_repository(tmp_path)
    pending = repository.create(planner_item())
    unscoped = repository.create(planner_item(id="unscoped", project_id=None))
    existing = jobs.create(promotion_job())

    with pytest.raises(PlannerAlreadyExistsError):
        repository.create(planner_item(title="Different title"))
    with pytest.raises(PlannerValidationError, match="project does not exist"):
        repository.create(planner_item(id="orphan-item", project_id="missing"))
    with pytest.raises(PlannerNotFoundError):
        repository.get("missing")
    with pytest.raises(PlannerNotFoundError):
        repository.promote("missing", promotion_job("new-job"))
    with pytest.raises(PlannerPromotionConflictError, match="existing job"):
        repository.promote(pending.id, promotion_job())
    with pytest.raises(PlannerValidationError, match="item project"):
        repository.promote(pending.id, promotion_job("beta-job", project_id="beta"))
    with pytest.raises(PlannerValidationError, match="project does not exist"):
        repository.promote(
            unscoped.id,
            promotion_job("missing-project-job", project_id="missing"),
        )

    assert jobs.list() == (existing,)
    assert repository.get(pending.id) == pending
    assert repository.get(unscoped.id) == unscoped
    assert database.path.is_file()


@pytest.mark.parametrize(
    "invalid",
    [
        planner_item(id=""),
        planner_item(id=" planner-001"),
        planner_item(id="unsafe\x00id"),
        planner_item(id="x" * 129),
        planner_item(id="é" * 65),
        planner_item(kind="deadline"),
        planner_item(title=""),
        planner_item(title=" title"),
        planner_item(title="unsafe\x00title"),
        planner_item(title="é" * 2049),
        planner_item(project_id=" alpha"),
        planner_item(details=" "),
        planner_item(details="unsafe\x00details"),
        planner_item(details="x" * 262_145),
        planner_item(source=" github"),
        planner_item(source="é" * 65),
        planner_item(source_key="key", source=None),
        planner_item(source_key="é" * 257, source="github"),
        planner_item(kind=PlannerItemKind.WATCHER, due_at=None, source=None),
        planner_item(due_at=None),
        planner_item(due_at=datetime(2030, 1, 2)),
        planner_item(due_at="2030-01-02T03:04:05Z"),
    ],
)
def test_invalid_planner_item_is_rejected_before_write(
    tmp_path: Path, invalid: PlannerItemCreate
) -> None:
    _, repository, _ = initialized_repository(tmp_path)

    with pytest.raises(PlannerValidationError):
        repository.create(invalid)

    assert repository.list() == ()


def test_invalid_promotion_job_and_filters_fail_before_mutation(tmp_path: Path) -> None:
    _, repository, jobs = initialized_repository(tmp_path)
    pending = repository.create(planner_item())

    with pytest.raises(PlannerValidationError, match="job is invalid"):
        repository.promote(pending.id, promotion_job(project_id=""))
    with pytest.raises(PlannerValidationError):
        repository.list(kind="deadline")
    with pytest.raises(PlannerValidationError):
        repository.list(project_id=" alpha")
    with pytest.raises(PlannerValidationError):
        repository.list(promoted=1)

    assert repository.get(pending.id) == pending
    assert jobs.list() == ()


def test_database_constraints_and_triggers_protect_promotions(tmp_path: Path) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    pending = repository.create(planner_item())
    due_us = PlannerRepository._datetime_to_epoch_us(NOW + timedelta(days=1))
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        invalid_rows = [
            ("bad-kind", "unknown", "title", due_us, None),
            ("missing-deadline", "deadline", "title", None, None),
            ("missing-source", "watcher", "title", None, None),
        ]
        for item_id, kind, title, due_at, source in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO planner (id, kind, title, due_at, source)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (item_id, kind, title, due_at, source),
                )
        with pytest.raises(sqlite3.IntegrityError, match="begin pending"):
            connection.execute(
                """
                INSERT INTO planner (
                    id, kind, title, promoted_job_id, promoted_at
                ) VALUES ('prepromoted', 'blocker', 'title', 'missing', 'timestamp')
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE planner SET title = 'Changed' WHERE id = ?",
                (pending.id,),
            )

    promoted = repository.promote(pending.id, promotion_job())
    second = repository.create(
        planner_item(id="planner-002", kind=PlannerItemKind.BLOCKER, due_at=None)
    )
    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE planner
                SET promoted_job_id = ?, promoted_at = '2030-01-02T03:04:05Z'
                WHERE id = ?
                """,
                (promoted.job.id, second.id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM planner WHERE id = ?", (promoted.item.id,))


def test_corrupted_and_unavailable_planner_storage_fails_safely(tmp_path: Path) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            """
            INSERT INTO planner (id, kind, title, created_at, updated_at)
            VALUES ('corrupt', 'unknown', 'title', 'bad', 'bad')
            """
        )

    with pytest.raises(PlannerRepositoryError, match="Stored planner data is invalid"):
        repository.get("corrupt")

    unavailable = PlannerRepository(Database(tmp_path / "missing" / "state.db"))
    with pytest.raises(PlannerRepositoryError) as failure:
        unavailable.list()
    assert str(tmp_path) not in str(failure.value)


def test_planner_queries_treat_text_fields_as_data(tmp_path: Path) -> None:
    _, repository, jobs = initialized_repository(tmp_path)
    unusual_id = "planner'); DROP TABLE planner; SELECT ('"
    unusual_title = "Review'); DROP TABLE jobs; SELECT ('"
    unusual_source = "source'); SELECT ('"
    created = repository.create(
        planner_item(
            id=unusual_id,
            kind=PlannerItemKind.WATCHER,
            title=unusual_title,
            due_at=None,
            source=unusual_source,
            source_key="external'); SELECT ('",
        )
    )
    promoted = repository.promote(unusual_id, promotion_job("safe-job"))

    assert created.id == unusual_id
    assert repository.get(unusual_id) == promoted.item
    assert promoted.item.title == unusual_title
    assert promoted.item.source == unusual_source
    assert jobs.list() == (promoted.job,)
