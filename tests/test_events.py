from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    EventNotFoundError,
    EventRepository,
    EventRepositoryError,
    EventValidationError,
    JobRepository,
    ProjectRepository,
)
from engine import (
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
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


def job(job_id: str = "job-001", *, project_id: str = "alpha") -> JobCreate:
    return JobCreate(
        id=job_id,
        project_id=project_id,
        request="Verify the durable event stream.",
        request_snapshot={"project_id": project_id, "request_version": 1},
        state=JobState.RUNNING,
        runtime=JobRuntime.LOCAL,
        model="qwen3.6",
        worktree_path=f"/runtime/worktrees/{job_id}",
    )


def event(**overrides: Any) -> EventCreate:
    values: dict[str, Any] = {
        "job_id": "job-001",
        "event_type": "job.progress",
        "payload": {"message": "Verification started.", "percent": 25},
    }
    values.update(overrides)
    return EventCreate(**values)


def initialized_event_repository(tmp_path: Path) -> tuple[Database, EventRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    JobRepository(database).create(job())
    return database, EventRepository(database)


def test_version_three_database_upgrades_without_losing_jobs(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:3])
    assert legacy.initialize() == 3
    ProjectRepository(legacy).create(project())
    expected_job = JobRepository(legacy).create(job())

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION
    assert JobRepository(upgraded).get(expected_job.id) == expected_job
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name IN (
                'events', 'events_prevent_delete', 'events_prevent_update'
            )
            ORDER BY type, name
            """
        ).fetchall()
    assert objects == [
        ("table", "events"),
        ("trigger", "events_prevent_delete"),
        ("trigger", "events_prevent_update"),
    ]


def test_event_round_trip_preserves_order_hash_and_restart_state(tmp_path: Path) -> None:
    database, repository = initialized_event_repository(tmp_path)
    payload = {"zeta": [3, 2, 1], "alpha": {"ready": True}}
    first = repository.append(event(payload=payload))
    payload["zeta"] = []
    second = repository.append(event(event_type="job.completed", payload={"result": "ok"}))

    canonical = json.dumps(
        {"alpha": {"ready": True}, "zeta": [3, 2, 1]},
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert first.sequence == 1
    assert second.sequence == 2
    assert first.payload == {"alpha": {"ready": True}, "zeta": [3, 2, 1]}
    assert first.payload_hash == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert repository.get(first.id) == first
    returned = repository.get(first.id)
    returned.payload["alpha"]["ready"] = False
    assert repository.get(first.id) == first

    restarted = Database(database.path)
    assert restarted.initialize() == LATEST_SCHEMA_VERSION
    assert EventRepository(restarted).list("job-001") == (first, second)
    with sqlite3.connect(database.path) as connection:
        stored_payload = connection.execute(
            "SELECT payload FROM events WHERE id = ?", (first.id,)
        ).fetchone()[0]
    assert stored_payload == canonical


def test_sequences_are_independent_per_job_and_safe_under_concurrency(tmp_path: Path) -> None:
    database, repository = initialized_event_repository(tmp_path)
    ProjectRepository(database).create(project("beta"))
    JobRepository(database).create(job("job-002", project_id="beta"))
    beta = repository.append(event(job_id="job-002"))

    with ThreadPoolExecutor(max_workers=8) as executor:
        records = tuple(
            executor.map(
                lambda index: repository.append(event(payload={"index": index})),
                range(16),
            )
        )

    stored = repository.list("job-001")
    assert beta.sequence == 1
    assert [item.sequence for item in stored] == list(range(1, 17))
    assert {item.id for item in records} == {item.id for item in stored}
    assert {item.payload["index"] for item in stored} == set(range(16))
    with sqlite3.connect(database.path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_database_rejects_event_update_and_delete(tmp_path: Path) -> None:
    database, repository = initialized_event_repository(tmp_path)
    expected = repository.append(event())

    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append only"):
            connection.execute(
                "UPDATE events SET event_type = 'changed' WHERE id = ?", (expected.id,)
            )
        with pytest.raises(sqlite3.IntegrityError, match="append only"):
            connection.execute("DELETE FROM events WHERE id = ?", (expected.id,))

    assert repository.get(expected.id) == expected
    assert not hasattr(repository, "update")
    assert not hasattr(repository, "delete")


@pytest.mark.parametrize(
    "invalid",
    [
        event(job_id=""),
        event(job_id=" job-001"),
        event(event_type=""),
        event(event_type=" progress"),
        event(event_type="unsafe\x00event"),
        event(event_type="x" * 129),
        event(payload=[]),
        event(payload={1: "value"}),
        event(payload={"score": float("nan")}),
        event(payload={"value": object()}),
        event(payload={"body": "x" * 1_048_577}),
    ],
)
def test_invalid_event_is_rejected_before_write(tmp_path: Path, invalid: EventCreate) -> None:
    _, repository = initialized_event_repository(tmp_path)

    with pytest.raises(EventValidationError):
        repository.append(invalid)

    assert repository.list("job-001") == ()


def test_recursive_payload_and_missing_job_fail_atomically(tmp_path: Path) -> None:
    _, repository = initialized_event_repository(tmp_path)
    recursive: dict[str, Any] = {}
    recursive["self"] = recursive

    with pytest.raises(EventValidationError, match="payload is invalid"):
        repository.append(event(payload=recursive))
    with pytest.raises(EventValidationError, match="job does not exist"):
        repository.append(event(job_id="missing"))

    assert repository.list("job-001") == ()


def test_database_constraints_reject_invalid_event_rows(tmp_path: Path) -> None:
    database, _ = initialized_event_repository(tmp_path)
    valid_hash = hashlib.sha256(b"{}").hexdigest()
    invalid_rows = [
        (0, "job.progress", "{}", valid_hash),
        (1, " job.progress", "{}", valid_hash),
        (1, "é" * 65, "{}", valid_hash),
        (1, "job.progress", "[]", valid_hash),
        (1, "job.progress", "{}", "A" * 64),
        (1, "job.progress", "{}", "0" * 63),
    ]
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for sequence, event_type, payload, payload_hash in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO events (
                        job_id, sequence, event_type, payload, payload_hash
                    ) VALUES ('job-001', ?, ?, ?, ?)
                    """,
                    (sequence, event_type, payload, payload_hash),
                )

        connection.execute(
            """
            INSERT INTO events (
                job_id, sequence, event_type, payload, payload_hash
            ) VALUES ('job-001', 1, 'job.progress', '{}', ?)
            """,
            (valid_hash,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO events (
                    job_id, sequence, event_type, payload, payload_hash
                ) VALUES ('job-001', 1, 'job.completed', '{}', ?)
                """,
                (valid_hash,),
            )


def test_corrupted_and_unavailable_events_fail_safely(tmp_path: Path) -> None:
    database, repository = initialized_event_repository(tmp_path)
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            INSERT INTO events (
                job_id, sequence, event_type, payload, payload_hash
            ) VALUES ('job-001', 1, 'job.progress', '{"value":1}', ?)
            """,
            ("0" * 64,),
        )
    with pytest.raises(EventRepositoryError, match="Stored event data is invalid"):
        repository.list("job-001")
    with pytest.raises(EventNotFoundError, match="does not exist"):
        repository.get(999)
    with pytest.raises(EventValidationError):
        repository.get(0)

    unavailable = EventRepository(Database(tmp_path / "missing" / "state.db"))
    with pytest.raises(EventRepositoryError) as failure:
        unavailable.list("job-001")
    assert str(tmp_path) not in str(failure.value)


def test_event_queries_treat_identifiers_and_types_as_data(tmp_path: Path) -> None:
    database = Database(tmp_path / "state.db")
    database.initialize()
    unusual_id = "job'); DROP TABLE events; SELECT ('"
    ProjectRepository(database).create(project())
    JobRepository(database).create(job(unusual_id))
    repository = EventRepository(database)

    stored = repository.append(
        event(job_id=unusual_id, event_type="tool'); DROP TABLE jobs; SELECT ('")
    )

    assert repository.list(unusual_id) == (stored,)
