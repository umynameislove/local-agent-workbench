from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from db import (
    AtomicTransitionConflictError,
    AtomicTransitionError,
    AtomicTransitionRecord,
    AtomicTransitionService,
    AtomicTransitionValidationError,
    Database,
    EventRepository,
    EventRepositoryError,
    JobRepository,
    ProjectRepository,
)
from engine import (
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
)


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
        request="Record each state change with its durable event.",
        request_snapshot={"policy_version": 1, "repo_head": "abc123"},
        state=JobState.CREATED,
        runtime=JobRuntime.AUTO,
    )


def update(**overrides: Any) -> JobUpdate:
    values: dict[str, Any] = {
        "id": "job-001",
        "state": JobState.QUEUED,
        "runtime": JobRuntime.CODEX,
        "model": "gpt-5.6-codex",
        "worktree_path": "/runtime/worktrees/job-001",
    }
    values.update(overrides)
    return JobUpdate(**values)


def event(**overrides: Any) -> EventCreate:
    values: dict[str, Any] = {
        "job_id": "job-001",
        "event_type": "job.queued",
        "payload": {"state": "queued"},
        "idempotency_key": "transition-001",
    }
    values.update(overrides)
    return EventCreate(**values)


def initialized_service(
    tmp_path: Path,
) -> tuple[Database, AtomicTransitionService, JobRepository, EventRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    jobs = JobRepository(database)
    jobs.create(job())
    return database, AtomicTransitionService(database), jobs, EventRepository(database)


def test_transition_updates_job_and_appends_event_in_one_result(tmp_path: Path) -> None:
    _, service, jobs, events = initialized_service(tmp_path)
    original = jobs.get("job-001")

    result = service.transition(update(), event())

    assert result.job.state is JobState.QUEUED
    assert result.job.runtime is JobRuntime.CODEX
    assert result.job.model == "gpt-5.6-codex"
    assert result.job.worktree_path == "/runtime/worktrees/job-001"
    assert result.job.request == original.request
    assert result.job.request_snapshot == original.request_snapshot
    assert result.event.job_id == result.job.id
    assert result.event.sequence == 1
    assert result.event.payload == {"state": "queued"}
    assert jobs.get("job-001") == result.job
    assert events.list("job-001") == (result.event,)


def test_event_failure_rolls_back_job_update(tmp_path: Path) -> None:
    database, service, jobs, events = initialized_service(tmp_path)
    original = jobs.get("job-001")
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_transition_event
            BEFORE INSERT ON events
            BEGIN
                SELECT RAISE(ABORT, 'simulated crash before event persistence');
            END
            """
        )

    with pytest.raises(AtomicTransitionError, match="could not be recorded"):
        service.transition(update(), event())

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


def test_job_failure_does_not_append_event(tmp_path: Path) -> None:
    database, service, jobs, events = initialized_service(tmp_path)
    original = jobs.get("job-001")
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_transition_job
            BEFORE UPDATE ON jobs
            BEGIN
                SELECT RAISE(ABORT, 'simulated crash during job update');
            END
            """
        )

    with pytest.raises(AtomicTransitionError, match="could not be recorded"):
        service.transition(update(), event())

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


@pytest.mark.parametrize(
    ("job_update", "job_event"),
    [
        (update(id="job-001"), event(job_id="job-002")),
        (update(state="queued"), event()),
        (update(), event(payload=[])),
        (update(), event(idempotency_key=" invalid")),
    ],
)
def test_invalid_transition_is_rejected_before_write(
    tmp_path: Path,
    job_update: JobUpdate,
    job_event: EventCreate,
) -> None:
    _, service, jobs, events = initialized_service(tmp_path)
    original = jobs.get("job-001")

    with pytest.raises(AtomicTransitionValidationError):
        service.transition(job_update, job_event)

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


def test_missing_job_fails_without_creating_an_event(tmp_path: Path) -> None:
    _, service, _, events = initialized_service(tmp_path)

    with pytest.raises(AtomicTransitionValidationError, match="does not exist"):
        service.transition(
            update(id="missing"),
            event(job_id="missing"),
        )

    assert events.list("job-001") == ()


def test_matching_retry_returns_committed_pair_after_restart(tmp_path: Path) -> None:
    database, service, _, _ = initialized_service(tmp_path)
    first = service.transition(update(), event())

    restarted = Database(database.path)
    restarted.initialize()
    retry = AtomicTransitionService(restarted).transition(update(), event())

    assert retry == first
    assert JobRepository(restarted).get("job-001") == first.job
    assert EventRepository(restarted).list("job-001") == (first.event,)


def test_conflicting_idempotency_payload_does_not_change_job(tmp_path: Path) -> None:
    _, service, jobs, events = initialized_service(tmp_path)
    first = service.transition(update(), event())
    conflicting_update = update(
        state=JobState.RUNNING,
        worktree_path="/runtime/worktrees/job-001-running",
    )

    with pytest.raises(AtomicTransitionConflictError, match="committed data"):
        service.transition(
            conflicting_update,
            event(
                event_type="job.running",
                payload={"state": "running"},
            ),
        )

    assert jobs.get("job-001") == first.job
    assert events.list("job-001") == (first.event,)


def test_stale_retry_cannot_move_job_behind_newer_event(tmp_path: Path) -> None:
    _, service, jobs, events = initialized_service(tmp_path)
    queued = service.transition(update(), event())
    running_update = update(
        state=JobState.RUNNING,
        worktree_path="/runtime/worktrees/job-001-running",
    )
    running = service.transition(
        running_update,
        event(
            event_type="job.running",
            payload={"state": "running"},
            idempotency_key="transition-002",
        ),
    )

    with pytest.raises(AtomicTransitionConflictError, match="current job state"):
        service.transition(update(), event())

    assert jobs.get("job-001") == running.job
    assert events.list("job-001") == (queued.event, running.event)


def test_concurrent_matching_retries_commit_one_event(tmp_path: Path) -> None:
    _, service, jobs, events = initialized_service(tmp_path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(executor.map(lambda _: service.transition(update(), event()), range(16)))

    assert len({result.event.id for result in results}) == 1
    assert len({result.event.sequence for result in results}) == 1
    assert all(result.job.state is JobState.QUEUED for result in results)
    assert events.list("job-001") == (results[0].event,)
    assert jobs.get("job-001") == results[-1].job


def test_concurrent_conflicts_leave_one_coherent_pair(tmp_path: Path) -> None:
    _, service, jobs, events = initialized_service(tmp_path)
    requests = (
        (
            update(state=JobState.QUEUED),
            event(payload={"state": "queued"}),
        ),
        (
            update(state=JobState.RUNNING),
            event(event_type="job.running", payload={"state": "running"}),
        ),
    )

    def submit(request: tuple[JobUpdate, EventCreate]) -> object:
        try:
            return service.transition(*request)
        except AtomicTransitionConflictError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(submit, requests))

    stored_events = events.list("job-001")
    assert len(stored_events) == 1
    assert sum(isinstance(outcome, AtomicTransitionConflictError) for outcome in outcomes) == 1
    winner = next(outcome for outcome in outcomes if not isinstance(outcome, Exception))
    assert isinstance(winner, AtomicTransitionRecord)
    assert jobs.get("job-001") == winner.job
    assert stored_events == (winner.event,)


def test_corrupted_idempotent_event_fails_closed_without_job_change(tmp_path: Path) -> None:
    database, service, jobs, events = initialized_service(tmp_path)
    original = jobs.get("job-001")
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            INSERT INTO events (
                job_id, sequence, event_type, payload, payload_hash,
                idempotency_key
            ) VALUES ('job-001', 1, 'job.queued', '{"state":"queued"}', ?, 'transition-001')
            """,
            ("0" * 64,),
        )

    with pytest.raises(AtomicTransitionError, match="invalid stored data"):
        service.transition(update(), event())

    assert jobs.get("job-001") == original
    with pytest.raises(EventRepositoryError, match="Stored event data is invalid"):
        events.list("job-001")


def test_unavailable_storage_uses_safe_public_error(tmp_path: Path) -> None:
    service = AtomicTransitionService(Database(tmp_path / "missing" / "state.db"))

    with pytest.raises(AtomicTransitionError) as failure:
        service.transition(update(), event())

    assert str(tmp_path) not in str(failure.value)
    assert "unavailable" in str(failure.value)
