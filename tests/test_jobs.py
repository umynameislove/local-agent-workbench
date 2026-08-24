from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    JobAlreadyExistsError,
    JobNotFoundError,
    JobRepository,
    JobRepositoryError,
    JobValidationError,
    ProjectRepository,
    ProjectRepositoryError,
)
from engine import (
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
)


def project(project_id: str = "alpha") -> ProjectConfig:
    return ProjectConfig(
        id=project_id,
        root=f"/workspace/{project_id}",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(job_id: str = "job-001", **overrides: Any) -> JobCreate:
    values: dict[str, Any] = {
        "id": job_id,
        "project_id": "alpha",
        "request": "Improve retry handling without changing public behavior.",
        "request_snapshot": {
            "policy_version": 1,
            "project": {"id": "alpha", "sensitivity": "private"},
            "repo_head": "abc123",
        },
        "state": JobState.WAITING_APPROVAL,
        "runtime": JobRuntime.CODEX,
        "model": "gpt-5.6-codex",
        "worktree_path": "/runtime/worktrees/job-001",
    }
    values.update(overrides)
    return JobCreate(**values)


def initialized_repositories(tmp_path: Path) -> tuple[Database, ProjectRepository, JobRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    projects = ProjectRepository(database)
    projects.create(project())
    return database, projects, JobRepository(database)


def test_version_two_database_upgrades_without_losing_projects(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:2])
    assert legacy.initialize() == 2
    ProjectRepository(legacy).create(project())
    assert Database(path).initialize() == LATEST_SCHEMA_VERSION
    assert ProjectRepository(Database(path)).get("alpha").to_config() == project()
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            "SELECT type, name FROM sqlite_master WHERE name LIKE 'jobs%' ORDER BY name"
        ).fetchall()
    assert objects == [("table", "jobs"), ("index", "jobs_project_created_idx")]


def test_job_round_trip_survives_restart_with_canonical_snapshot(tmp_path: Path) -> None:
    database, _, repository = initialized_repositories(tmp_path)
    expected = job("job'); DROP TABLE jobs; SELECT ('")
    created = repository.create(expected)
    updated = repository.update(
        JobUpdate(expected.id, JobState.COMPLETED, JobRuntime.LOCAL, "qwen3.6", None)
    )
    restarted = Database(database.path)
    assert restarted.initialize() == LATEST_SCHEMA_VERSION

    assert JobRepository(restarted).get(expected.id) == updated
    assert (updated.project_id, updated.request, updated.request_snapshot, updated.created_at) == (
        created.project_id,
        created.request,
        created.request_snapshot,
        created.created_at,
    )
    with sqlite3.connect(database.path) as connection:
        raw_snapshot = connection.execute(
            "SELECT request_snapshot FROM jobs WHERE id = ?", (expected.id,)
        ).fetchone()[0]
    assert raw_snapshot == json.dumps(
        expected.request_snapshot,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def test_job_list_is_deterministic_and_can_filter_by_project(tmp_path: Path) -> None:
    _, projects, repository = initialized_repositories(tmp_path)
    projects.create(project("beta"))
    beta = repository.create(job("beta-job", project_id="beta", model=None, worktree_path=None))
    zeta = repository.create(job("zeta"))
    alpha = repository.create(job("alpha"))

    expected = tuple(sorted((alpha, beta, zeta), key=lambda item: (item.created_at, item.id)))
    alpha_jobs = tuple(sorted((alpha, zeta), key=lambda item: (item.created_at, item.id)))
    assert repository.list() == expected
    assert repository.list(project_id="alpha") == alpha_jobs
    assert repository.list(project_id="missing") == ()


@pytest.mark.parametrize(
    "invalid",
    [
        job(id=""),
        job(project_id=" alpha"),
        job(request=" \n "),
        job(request="unsafe\x00request"),
        job(request="x" * 262_145),
        job(state="running"),
        job(runtime="codex"),
        job(model=""),
        job(worktree_path=" path"),
        job(request_snapshot=[]),
        job(request_snapshot={1: "value"}),
        job(request_snapshot={"score": float("nan")}),
        job(request_snapshot={"value": object()}),
    ],
)
def test_invalid_job_is_rejected_before_write(tmp_path: Path, invalid: JobCreate) -> None:
    _, _, repository = initialized_repositories(tmp_path)
    with pytest.raises(JobValidationError):
        repository.create(invalid)

    assert repository.list() == ()


def test_recursive_snapshot_is_rejected_before_write(tmp_path: Path) -> None:
    _, _, repository = initialized_repositories(tmp_path)
    recursive: dict[str, Any] = {}
    recursive["self"] = recursive

    with pytest.raises(JobValidationError, match="snapshot is invalid"):
        repository.create(job(request_snapshot=recursive))

    assert repository.list() == ()


def test_duplicate_and_missing_project_fail_atomically(tmp_path: Path) -> None:
    _, _, repository = initialized_repositories(tmp_path)
    original = repository.create(job())

    with pytest.raises(JobAlreadyExistsError, match="already exists"):
        repository.create(job(request="A different request."))
    with pytest.raises(JobValidationError, match="project does not exist"):
        repository.create(job("orphan", project_id="missing"))

    assert repository.list() == (original,)


def test_database_constraints_reject_invalid_job_rows(tmp_path: Path) -> None:
    database, _, _ = initialized_repositories(tmp_path)
    stored = job()
    valid_snapshot = json.dumps(stored.request_snapshot)
    invalid_rows = [
        ("[]", stored.state.value, stored.runtime.value),
        (valid_snapshot, "unknown", stored.runtime.value),
        (valid_snapshot, stored.state.value, "remote"),
    ]
    with sqlite3.connect(database.path) as connection:
        for index, (snapshot, state, runtime) in enumerate(invalid_rows):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO jobs (
                        id, project_id, request, request_snapshot, state, runtime
                    ) VALUES (?, 'alpha', ?, ?, ?, ?)
                    """,
                    (f"invalid-{index}", stored.request, snapshot, state, runtime),
                )


def test_every_supported_state_and_runtime_round_trips(tmp_path: Path) -> None:
    _, _, repository = initialized_repositories(tmp_path)
    expected_states = {
        repository.create(job(f"state-{state.value}", state=state)).state for state in JobState
    }
    expected_runtimes = {
        repository.create(job(f"runtime-{runtime.value}", runtime=runtime)).runtime
        for runtime in JobRuntime
    }

    assert expected_states == set(JobState)
    assert expected_runtimes == set(JobRuntime)


def test_project_with_jobs_cannot_be_deleted(tmp_path: Path) -> None:
    _, projects, repository = initialized_repositories(tmp_path)
    expected = repository.create(job())

    with pytest.raises(ProjectRepositoryError, match="storage operation failed"):
        projects.delete("alpha")

    assert repository.get(expected.id) == expected
    assert projects.get("alpha").to_config() == project()


def test_missing_corrupted_and_uninitialized_jobs_fail_safely(tmp_path: Path) -> None:
    database, _, repository = initialized_repositories(tmp_path)
    with pytest.raises(JobNotFoundError, match="does not exist"):
        repository.get("missing")
    with pytest.raises(JobNotFoundError, match="does not exist"):
        repository.update(JobUpdate("missing", JobState.QUEUED, JobRuntime.AUTO))
    with pytest.raises(JobValidationError):
        repository.update(JobUpdate("missing", "queued", JobRuntime.AUTO))
    repository.create(job())
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE jobs SET request_snapshot = '[]' WHERE id = 'job-001'")
    with pytest.raises(JobRepositoryError, match="Stored job data is invalid"):
        repository.get("job-001")

    unavailable = JobRepository(Database(tmp_path / "missing" / "state.db"))
    with pytest.raises(JobRepositoryError) as failure:
        unavailable.list()
    assert str(tmp_path) not in str(failure.value)
