from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    EventRepository,
    JobRepository,
    MemoryReferenceNotFoundError,
    MemoryReferenceRepository,
    MemoryReferenceRepositoryError,
    MemoryReferenceValidationError,
    ProjectRepository,
)
from engine import (
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    MemoryReferenceCreate,
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
        request="Persist projmem provenance without duplicating memory content.",
        request_snapshot={"policy_version": 1},
        state=JobState.COMPLETED,
        runtime=JobRuntime.CODEX,
        model="gpt-5.6-codex",
    )


def memory_ref(**overrides: Any) -> MemoryReferenceCreate:
    values: dict[str, Any] = {
        "projmem_record_id": "mem-01JZ8FH2T3S5A7K9M1P4Q6R8VW",
        "job_id": "job-001",
        "event_id": None,
    }
    values.update(overrides)
    return MemoryReferenceCreate(**values)


def initialized_repository(
    tmp_path: Path,
) -> tuple[Database, MemoryReferenceRepository, int]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    JobRepository(database).create(job())
    event = EventRepository(database).append(
        EventCreate(
            job_id="job-001",
            event_type="memory.candidate.validated",
            payload={"candidate_count": 1},
            idempotency_key="memory-candidate-001",
        )
    )
    return database, MemoryReferenceRepository(database), event.id


def test_version_eight_database_adds_memory_refs_without_losing_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:8])
    assert legacy.initialize() == 8
    ProjectRepository(legacy).create(project())
    expected_job = JobRepository(legacy).create(job())
    expected_event = EventRepository(legacy).append(
        EventCreate(
            job_id=expected_job.id,
            event_type="job.completed",
            payload={"result": "verified"},
        )
    )

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION
    assert JobRepository(upgraded).get(expected_job.id) == expected_job
    assert EventRepository(upgraded).get(expected_event.id) == expected_event
    with sqlite3.connect(path) as connection:
        columns = connection.execute("PRAGMA table_info(memory_refs)").fetchall()
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name LIKE 'memory_refs%'
            ORDER BY type, name
            """
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    assert [column[1] for column in columns] == [
        "id",
        "projmem_record_id",
        "job_id",
        "event_id",
        "created_at",
    ]
    assert objects == [
        ("index", "memory_refs_event_created_idx"),
        ("index", "memory_refs_event_scope_idx"),
        ("index", "memory_refs_job_created_idx"),
        ("index", "memory_refs_job_scope_idx"),
        ("table", "memory_refs"),
        ("trigger", "memory_refs_prevent_delete"),
        ("trigger", "memory_refs_prevent_update"),
    ]


def test_job_and_event_references_round_trip_without_semantic_content(
    tmp_path: Path,
) -> None:
    database, repository, event_id = initialized_repository(tmp_path)

    job_reference = repository.link(memory_ref())
    event_reference = repository.link(
        memory_ref(projmem_record_id="mem-event-001", event_id=event_id)
    )

    assert job_reference.job_id == "job-001"
    assert job_reference.event_id is None
    assert event_reference.event_id == event_id
    restarted = MemoryReferenceRepository(Database(database.path))
    assert restarted.get(job_reference.id) == job_reference
    assert restarted.get(event_reference.id) == event_reference
    assert restarted.list(job_id="job-001") == (job_reference, event_reference)
    assert restarted.list(event_id=event_id) == (event_reference,)
    assert restarted.list(projmem_record_id=job_reference.projmem_record_id) == (job_reference,)

    with sqlite3.connect(database.path) as connection:
        stored = connection.execute("SELECT * FROM memory_refs ORDER BY id").fetchall()
    assert all(len(row) == 5 for row in stored)


def test_matching_retries_are_idempotent_across_restart_and_concurrency(
    tmp_path: Path,
) -> None:
    database, repository, event_id = initialized_repository(tmp_path)
    reference = memory_ref(event_id=event_id)

    first = repository.link(reference)
    restarted = MemoryReferenceRepository(Database(database.path))
    assert restarted.link(reference) == first
    with ThreadPoolExecutor(max_workers=4) as executor:
        linked = tuple(executor.map(lambda _: restarted.link(reference), range(8)))

    assert linked == (first,) * 8
    assert restarted.list() == (first,)


def test_same_projmem_record_can_have_distinct_provenance_scopes(tmp_path: Path) -> None:
    database, repository, first_event_id = initialized_repository(tmp_path)
    second_event = EventRepository(database).append(
        EventCreate(
            job_id="job-001",
            event_type="memory.written",
            payload={"record_count": 1},
        )
    )

    job_reference = repository.link(memory_ref())
    first_event_reference = repository.link(memory_ref(event_id=first_event_id))
    second_event_reference = repository.link(memory_ref(event_id=second_event.id))

    assert repository.list(projmem_record_id=job_reference.projmem_record_id) == (
        job_reference,
        first_event_reference,
        second_event_reference,
    )


@pytest.mark.parametrize(
    "invalid",
    [
        memory_ref(projmem_record_id=""),
        memory_ref(projmem_record_id=" mem-001"),
        memory_ref(projmem_record_id="mem\x00001"),
        memory_ref(projmem_record_id="x" * 513),
        memory_ref(job_id=""),
        memory_ref(job_id=" job-001"),
        memory_ref(event_id=0),
        memory_ref(event_id=True),
        memory_ref(event_id=9_223_372_036_854_775_808),
    ],
)
def test_invalid_reference_is_rejected_before_write(
    tmp_path: Path,
    invalid: MemoryReferenceCreate,
) -> None:
    _, repository, _ = initialized_repository(tmp_path)

    with pytest.raises(MemoryReferenceValidationError):
        repository.link(invalid)

    assert repository.list() == ()


def test_missing_or_mismatched_provenance_rolls_back_atomically(tmp_path: Path) -> None:
    database, repository, event_id = initialized_repository(tmp_path)
    ProjectRepository(database).create(project("beta"))
    JobRepository(database).create(job("job-002", project_id="beta"))

    for invalid in (
        memory_ref(job_id="missing"),
        memory_ref(event_id=event_id + 100),
        memory_ref(job_id="job-002", event_id=event_id),
    ):
        with pytest.raises(MemoryReferenceValidationError, match="provenance does not exist"):
            repository.link(invalid)

    assert repository.list() == ()


def test_memory_references_are_immutable_at_the_database_boundary(tmp_path: Path) -> None:
    database, repository, event_id = initialized_repository(tmp_path)
    stored = repository.link(memory_ref(event_id=event_id))

    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE memory_refs SET projmem_record_id = 'other' WHERE id = ?",
                (stored.id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM memory_refs WHERE id = ?", (stored.id,))

    assert repository.get(stored.id) == stored


def test_database_constraints_reject_invalid_or_duplicate_references(
    tmp_path: Path,
) -> None:
    database, repository, event_id = initialized_repository(tmp_path)
    stored = repository.link(memory_ref(event_id=event_id))

    invalid_rows = (
        ("", "job-001", None),
        (" mem-002", "job-001", None),
        ("x" * 513, "job-001", None),
        ("mem-002", "missing", None),
        ("mem-002", "job-001", 0),
        ("mem-002", "job-001", event_id + 100),
    )
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for row in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO memory_refs (projmem_record_id, job_id, event_id)
                    VALUES (?, ?, ?)
                    """,
                    row,
                )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO memory_refs (projmem_record_id, job_id, event_id)
                VALUES (?, ?, ?)
                """,
                (stored.projmem_record_id, stored.job_id, stored.event_id),
            )

    assert repository.list() == (stored,)


def test_missing_invalid_corrupted_and_unavailable_storage_fail_safely(
    tmp_path: Path,
) -> None:
    database, repository, _ = initialized_repository(tmp_path)
    stored = repository.link(memory_ref())

    with pytest.raises(MemoryReferenceNotFoundError):
        repository.get(stored.id + 1)
    with pytest.raises(MemoryReferenceValidationError):
        repository.get(True)
    with pytest.raises(MemoryReferenceValidationError):
        repository.list(job_id=" job-001")
    with pytest.raises(MemoryReferenceValidationError):
        repository.list(projmem_record_id="")

    with sqlite3.connect(database.path) as connection:
        connection.execute("DROP TRIGGER memory_refs_prevent_update")
        connection.execute(
            "UPDATE memory_refs SET created_at = 'invalid' WHERE id = ?",
            (stored.id,),
        )
    with pytest.raises(MemoryReferenceRepositoryError, match="Stored memory reference"):
        repository.get(stored.id)

    unavailable = MemoryReferenceRepository(Database(tmp_path / "missing" / "state.db"))
    with pytest.raises(MemoryReferenceRepositoryError) as failure:
        unavailable.list()
    assert str(tmp_path) not in str(failure.value)
    assert "unavailable" in str(failure.value)


def test_queries_treat_projmem_and_job_identifiers_as_data(tmp_path: Path) -> None:
    database = Database(tmp_path / "state.db")
    database.initialize()
    unusual_job_id = "job'); DROP TABLE memory_refs; SELECT ('"
    unusual_record_id = "mem'); DELETE FROM events; SELECT ('"
    ProjectRepository(database).create(project())
    JobRepository(database).create(job(unusual_job_id))
    repository = MemoryReferenceRepository(database)

    stored = repository.link(memory_ref(projmem_record_id=unusual_record_id, job_id=unusual_job_id))

    assert repository.list(
        job_id=unusual_job_id,
        projmem_record_id=unusual_record_id,
    ) == (stored,)
    assert repository.get(stored.id) == stored


def test_memory_reference_create_is_immutable_input_data() -> None:
    original = memory_ref()

    changed = replace(original, projmem_record_id="mem-002")

    assert original.projmem_record_id != changed.projmem_record_id
