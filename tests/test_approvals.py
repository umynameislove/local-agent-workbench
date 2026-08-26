from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    ApprovalAlreadyExistsError,
    ApprovalDecisionConflictError,
    ApprovalExpiredError,
    ApprovalNotFoundError,
    ApprovalPayloadMismatchError,
    ApprovalRepository,
    ApprovalRepositoryError,
    ApprovalValidationError,
    Database,
    EventRepository,
    JobRepository,
    ProjectRepository,
)
from engine import (
    ApprovalCreate,
    ApprovalDecision,
    ApprovalResolution,
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
)

NOW = datetime(2030, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def project() -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root="/workspace/alpha",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(job_id: str = "job-001") -> JobCreate:
    return JobCreate(
        id=job_id,
        project_id="alpha",
        request="Review and approve the prepared action.",
        request_snapshot={"policy_version": 1, "project_id": "alpha"},
        state=JobState.WAITING_APPROVAL,
        runtime=JobRuntime.CODEX,
        model="gpt-5.6-codex",
        worktree_path=f"/runtime/worktrees/{job_id}",
    )


def approval(**overrides: Any) -> ApprovalCreate:
    values: dict[str, Any] = {
        "id": "approval-001",
        "job_id": "job-001",
        "payload": {"action": "apply_patch", "diff_hash": "abc123"},
        "expires_at": NOW + timedelta(minutes=5),
    }
    values.update(overrides)
    return ApprovalCreate(**values)


def resolution(**overrides: Any) -> ApprovalResolution:
    values: dict[str, Any] = {
        "decision": ApprovalDecision.APPROVED,
        "actor": "local-user",
        "channel": "desktop",
        "payload": {"action": "apply_patch", "diff_hash": "abc123"},
    }
    values.update(overrides)
    return ApprovalResolution(**values)


def initialized_repository(
    tmp_path: Path,
    *,
    clock: MutableClock | None = None,
) -> tuple[Database, ApprovalRepository, MutableClock]:
    active_clock = clock or MutableClock()
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    JobRepository(database).create(job())
    return database, ApprovalRepository(database, clock=active_clock), active_clock


def test_version_five_database_adds_approvals_without_losing_state(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:5])
    assert legacy.initialize() == 5
    ProjectRepository(legacy).create(project())
    JobRepository(legacy).create(job())
    expected_event = EventRepository(legacy).append(
        EventCreate("job-001", "job.progress", {"ready": True}, "event-001")
    )

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION
    assert EventRepository(upgraded).list("job-001") == (expected_event,)
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name IN (
                'approvals', 'approvals_job_created_idx',
                'approvals_prevent_delete', 'approvals_require_pending_insert',
                'approvals_validate_decision'
            )
            ORDER BY type, name
            """
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert objects == [
        ("index", "approvals_job_created_idx"),
        ("table", "approvals"),
        ("trigger", "approvals_prevent_delete"),
        ("trigger", "approvals_require_pending_insert"),
        ("trigger", "approvals_validate_decision"),
    ]


def test_approval_round_trip_stores_hash_only_and_survives_restart(tmp_path: Path) -> None:
    database, repository, clock = initialized_repository(tmp_path)
    payload = {"zeta": [3, 2, 1], "alpha": {"ready": True}}
    created = repository.create(approval(payload=payload))
    payload["zeta"] = []
    canonical = json.dumps(
        {"alpha": {"ready": True}, "zeta": [3, 2, 1]},
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    restarted = ApprovalRepository(Database(database.path), clock=clock)
    assert restarted.database.initialize() == LATEST_SCHEMA_VERSION
    assert restarted.get(created.id) == created
    assert restarted.list(job_id="job-001") == (created,)
    assert created.payload_hash == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert created.decision is None
    assert created.expires_at == NOW + timedelta(minutes=5)
    with sqlite3.connect(database.path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(approvals)")}
    assert "payload" not in columns
    assert "payload_hash" in columns


@pytest.mark.parametrize("decision", list(ApprovalDecision))
def test_every_decision_round_trips_with_actor_channel_and_restart(
    tmp_path: Path, decision: ApprovalDecision
) -> None:
    database, repository, clock = initialized_repository(tmp_path)
    repository.create(approval())

    decided = repository.decide("approval-001", resolution(decision=decision))
    restarted = ApprovalRepository(Database(database.path), clock=clock)

    assert decided.decision is decision
    assert decided.actor == "local-user"
    assert decided.channel == "desktop"
    assert decided.decided_at == "2030-01-02T03:04:05.678901Z"
    assert restarted.get("approval-001") == decided


def test_mutated_payload_cannot_be_approved(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    original = {"action": "apply_patch", "files": ["engine.py"]}
    repository.create(approval(payload=original))
    original["files"].append("unsafe.py")

    with pytest.raises(ApprovalPayloadMismatchError, match="does not match"):
        repository.decide("approval-001", resolution(payload=original))

    pending = repository.get("approval-001")
    assert pending.decision is None
    approved = repository.decide(
        "approval-001",
        resolution(payload={"files": ["engine.py"], "action": "apply_patch"}),
    )
    assert approved.decision is ApprovalDecision.APPROVED


def test_expiry_boundary_fails_closed_without_recording_decision(tmp_path: Path) -> None:
    _, repository, clock = initialized_repository(tmp_path)
    expires_at = NOW + timedelta(seconds=1)
    repository.create(approval(expires_at=expires_at))
    clock.value = expires_at

    with pytest.raises(ApprovalExpiredError, match="expired"):
        repository.decide("approval-001", resolution())

    assert repository.get("approval-001").decision is None


def test_matching_decision_retry_is_idempotent_after_expiry(tmp_path: Path) -> None:
    _, repository, clock = initialized_repository(tmp_path)
    expires_at = NOW + timedelta(seconds=1)
    repository.create(approval(expires_at=expires_at))
    expected = repository.decide("approval-001", resolution())
    clock.value = expires_at + timedelta(days=1)

    assert repository.decide("approval-001", resolution()) == expected
    with pytest.raises(ApprovalDecisionConflictError):
        repository.decide(
            "approval-001",
            resolution(decision=ApprovalDecision.REJECTED),
        )
    with pytest.raises(ApprovalDecisionConflictError):
        repository.decide("approval-001", resolution(actor="another-user"))
    with pytest.raises(ApprovalDecisionConflictError):
        repository.decide("approval-001", resolution(channel="telegram"))
    with pytest.raises(ApprovalPayloadMismatchError):
        repository.decide("approval-001", resolution(payload={"changed": True}))


def test_concurrent_matching_decisions_record_one_result(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    repository.create(approval())

    with ThreadPoolExecutor(max_workers=12) as executor:
        records = tuple(
            executor.map(
                lambda _: repository.decide("approval-001", resolution()),
                range(24),
            )
        )

    assert all(record == records[0] for record in records)
    assert repository.list() == (records[0],)


def test_concurrent_conflicting_decisions_allow_one_winner(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    repository.create(approval())

    def submit(decision: ApprovalDecision) -> object:
        try:
            return repository.decide("approval-001", resolution(decision=decision))
        except ApprovalDecisionConflictError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            executor.map(
                submit,
                (ApprovalDecision.APPROVED, ApprovalDecision.REJECTED),
            )
        )

    stored = repository.get("approval-001")
    assert stored.decision in {ApprovalDecision.APPROVED, ApprovalDecision.REJECTED}
    assert sum(isinstance(outcome, ApprovalDecisionConflictError) for outcome in outcomes) == 1
    assert sum(outcome == stored for outcome in outcomes) == 1


@pytest.mark.parametrize(
    "invalid",
    [
        approval(id=""),
        approval(id=" approval-001"),
        approval(id="unsafe\x00id"),
        approval(id="x" * 129),
        approval(id="é" * 65),
        approval(job_id=""),
        approval(payload=[]),
        approval(payload={1: "value"}),
        approval(payload={"score": float("nan")}),
        approval(payload={"value": object()}),
        approval(payload={"body": "x" * 1_048_577}),
        approval(expires_at=datetime(2030, 1, 2)),
        approval(expires_at="2030-01-02T03:09:05Z"),
    ],
)
def test_invalid_approval_is_rejected_before_write(tmp_path: Path, invalid: ApprovalCreate) -> None:
    _, repository, _ = initialized_repository(tmp_path)

    with pytest.raises(ApprovalValidationError):
        repository.create(invalid)

    assert repository.list() == ()


def test_recursive_payload_and_nonfuture_expiry_are_rejected(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    recursive: dict[str, Any] = {}
    recursive["self"] = recursive

    with pytest.raises(ApprovalValidationError, match="payload is invalid"):
        repository.create(approval(payload=recursive))
    with pytest.raises(ApprovalExpiredError):
        repository.create(approval(expires_at=NOW))
    with pytest.raises(ApprovalExpiredError):
        repository.create(approval(expires_at=NOW - timedelta(microseconds=1)))

    assert repository.list() == ()


@pytest.mark.parametrize(
    "invalid",
    [
        resolution(decision="approved"),
        resolution(actor=""),
        resolution(actor=" local-user"),
        resolution(actor="unsafe\x00actor"),
        resolution(actor="x" * 129),
        resolution(actor="é" * 65),
        resolution(channel=""),
        resolution(channel=" desktop"),
        resolution(channel="x" * 65),
        resolution(channel="é" * 33),
        resolution(payload=[]),
        resolution(payload={"score": float("nan")}),
    ],
)
def test_invalid_resolution_is_rejected_without_mutation(
    tmp_path: Path, invalid: ApprovalResolution
) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    repository.create(approval())

    with pytest.raises(ApprovalValidationError):
        repository.decide("approval-001", invalid)

    assert repository.get("approval-001").decision is None


def test_duplicate_missing_job_and_unavailable_storage_fail_safely(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    expected = repository.create(approval())

    with pytest.raises(ApprovalAlreadyExistsError, match="already exists"):
        repository.create(approval(payload={"different": True}))
    with pytest.raises(ApprovalValidationError, match="job does not exist"):
        repository.create(approval(id="orphan", job_id="missing"))
    with pytest.raises(ApprovalNotFoundError):
        repository.get("missing")
    with pytest.raises(ApprovalNotFoundError):
        repository.decide("missing", resolution())

    assert repository.list() == (expected,)
    unavailable = ApprovalRepository(Database(tmp_path / "missing" / "state.db"))
    with pytest.raises(ApprovalRepositoryError) as failure:
        unavailable.list()
    assert str(tmp_path) not in str(failure.value)


def test_database_constraints_and_triggers_protect_approval_records(tmp_path: Path) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    pending = repository.create(approval())
    expiry_us = ApprovalRepository._datetime_to_epoch_us(NOW + timedelta(minutes=5))

    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        invalid_rows = [
            ("invalid-hash", "x" * 64, None, None, None, None, expiry_us),
            ("invalid-expiry", "0" * 64, None, None, None, None, 0),
            (
                "partial-decision",
                "0" * 64,
                "approved",
                None,
                None,
                None,
                expiry_us,
            ),
            (
                "decided-insert",
                "0" * 64,
                "approved",
                "local-user",
                "desktop",
                "2030-01-02T03:04:05.678901Z",
                expiry_us,
            ),
        ]
        for row in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO approvals (
                        id, job_id, payload_hash, decision, actor, channel,
                        decided_at, expires_at
                    ) VALUES (?, 'job-001', ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE approvals SET payload_hash = ? WHERE id = ?",
                ("0" * 64, pending.id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM approvals WHERE id = ?", (pending.id,))

    decided = repository.decide("approval-001", resolution())
    with (
        sqlite3.connect(database.path) as connection,
        pytest.raises(sqlite3.IntegrityError, match="immutable"),
    ):
        connection.execute(
            "UPDATE approvals SET decision = 'rejected' WHERE id = ?",
            (decided.id,),
        )
    assert repository.get(decided.id) == decided


def test_corrupted_approval_and_invalid_clock_fail_closed(tmp_path: Path) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    expiry_us = ApprovalRepository._datetime_to_epoch_us(NOW + timedelta(minutes=5))
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            """
            INSERT INTO approvals (
                id, job_id, payload_hash, expires_at, created_at
            ) VALUES ('corrupt', 'job-001', 'bad', ?, 'not-a-timestamp')
            """,
            (expiry_us,),
        )

    with pytest.raises(ApprovalRepositoryError, match="Stored approval data is invalid"):
        repository.get("corrupt")

    invalid_clock = ApprovalRepository(database, clock=lambda: datetime(2030, 1, 2))
    with pytest.raises(ApprovalRepositoryError, match="clock is invalid"):
        invalid_clock.create(approval(id="invalid-clock"))


def test_approval_queries_treat_all_text_fields_as_data(tmp_path: Path) -> None:
    _, repository, _ = initialized_repository(tmp_path)
    unusual_id = "approval'); DROP TABLE approvals; SELECT ('"
    unusual_actor = "actor'); DROP TABLE jobs; SELECT ('"
    unusual_channel = "channel'); SELECT ('"
    created = repository.create(approval(id=unusual_id))

    decided = repository.decide(
        unusual_id,
        resolution(actor=unusual_actor, channel=unusual_channel),
    )

    assert created.id == unusual_id
    assert decided.actor == unusual_actor
    assert decided.channel == unusual_channel
    assert repository.get(unusual_id) == decided
