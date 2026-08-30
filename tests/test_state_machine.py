from __future__ import annotations

from itertools import product
from pathlib import Path
from typing import Any

import pytest

from db import (
    AtomicTransitionService,
    AtomicTransitionStateError,
    Database,
    EventRepository,
    JobRepository,
    ProjectRepository,
)
from engine import (
    ALLOWED_JOB_TRANSITIONS,
    TERMINAL_JOB_STATES,
    EventCreate,
    ForbiddenJobTransitionError,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
    validate_job_transition,
)

STOP_STATES = frozenset(
    {
        JobState.FAILED,
        JobState.BLOCKED,
        JobState.CANCELLED,
    }
)


def with_stop_states(*states: JobState) -> frozenset[JobState]:
    return frozenset(states) | STOP_STATES


EXPECTED_TRANSITIONS = {
    JobState.CREATED: with_stop_states(JobState.CLASSIFIED),
    JobState.CLASSIFIED: with_stop_states(JobState.PLANNING),
    JobState.PLANNING: with_stop_states(
        JobState.QUEUED,
        JobState.WAITING_INPUT,
    ),
    JobState.QUEUED: with_stop_states(JobState.RUNNING),
    JobState.RUNNING: with_stop_states(
        JobState.WAITING_INPUT,
        JobState.WAITING_APPROVAL,
        JobState.VERIFYING,
    ),
    JobState.WAITING_INPUT: with_stop_states(
        JobState.PLANNING,
        JobState.QUEUED,
        JobState.RUNNING,
    ),
    JobState.WAITING_APPROVAL: with_stop_states(
        JobState.RUNNING,
        JobState.APPROVED,
        JobState.REJECTED,
    ),
    JobState.VERIFYING: with_stop_states(
        JobState.RUNNING,
        JobState.REVIEW_READY,
    ),
    JobState.REVIEW_READY: with_stop_states(
        JobState.RUNNING,
        JobState.WAITING_APPROVAL,
        JobState.APPROVED,
        JobState.REJECTED,
    ),
    JobState.APPROVED: with_stop_states(JobState.APPLYING),
    JobState.APPLYING: with_stop_states(JobState.COMPLETED),
    JobState.COMPLETED: frozenset(),
    JobState.REJECTED: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.BLOCKED: frozenset(),
    JobState.CANCELLED: frozenset(),
}

STATE_PAIRS = tuple(product(JobState, repeat=2))
ALLOWED_PAIRS = tuple(
    (current, target) for current, target in STATE_PAIRS if target in EXPECTED_TRANSITIONS[current]
)
FORBIDDEN_PAIRS = tuple(pair for pair in STATE_PAIRS if pair not in ALLOWED_PAIRS)


def pair_id(pair: tuple[JobState, JobState]) -> str:
    return f"{pair[0].value}_to_{pair[1].value}"


def project() -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root="/workspace/alpha",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def initialized_service(
    tmp_path: Path,
    state: JobState,
) -> tuple[AtomicTransitionService, JobRepository, EventRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    jobs = JobRepository(database)
    jobs.create(
        JobCreate(
            id="job-001",
            project_id="alpha",
            request="Verify the complete job state transition contract.",
            request_snapshot={"policy_version": 1},
            state=state,
            runtime=JobRuntime.AUTO,
        )
    )
    return AtomicTransitionService(database), jobs, EventRepository(database)


def transition_request(target: JobState) -> tuple[JobUpdate, EventCreate]:
    return (
        JobUpdate(
            id="job-001",
            state=target,
            runtime=JobRuntime.AUTO,
        ),
        EventCreate(
            job_id="job-001",
            event_type=f"job.{target.value}",
            payload={"state": target.value},
            idempotency_key=f"transition-to-{target.value}",
        ),
    )


def test_transition_map_covers_every_state_and_matches_the_contract() -> None:
    assert set(ALLOWED_JOB_TRANSITIONS) == set(JobState)
    assert dict(ALLOWED_JOB_TRANSITIONS) == EXPECTED_TRANSITIONS


def test_terminal_states_are_explicit_and_have_no_outgoing_transition() -> None:
    expected = frozenset(
        {
            JobState.COMPLETED,
            JobState.REJECTED,
            JobState.FAILED,
            JobState.BLOCKED,
            JobState.CANCELLED,
        }
    )

    assert expected == TERMINAL_JOB_STATES
    assert all(not ALLOWED_JOB_TRANSITIONS[state] for state in expected)


def test_transition_map_and_targets_are_immutable() -> None:
    mutable_view: Any = ALLOWED_JOB_TRANSITIONS

    with pytest.raises(TypeError):
        mutable_view[JobState.CREATED] = frozenset()
    with pytest.raises(AttributeError):
        ALLOWED_JOB_TRANSITIONS[JobState.CREATED].add(JobState.COMPLETED)


@pytest.mark.parametrize(
    ("current", "target"),
    ALLOWED_PAIRS,
    ids=[pair_id(pair) for pair in ALLOWED_PAIRS],
)
def test_every_allowed_domain_transition_passes(
    current: JobState,
    target: JobState,
) -> None:
    validate_job_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    FORBIDDEN_PAIRS,
    ids=[pair_id(pair) for pair in FORBIDDEN_PAIRS],
)
def test_every_forbidden_domain_transition_has_a_typed_error(
    current: JobState,
    target: JobState,
) -> None:
    with pytest.raises(ForbiddenJobTransitionError) as failure:
        validate_job_transition(current, target)

    assert failure.value.current_state is current
    assert failure.value.target_state is target
    assert current.value in str(failure.value)
    assert target.value in str(failure.value)


@pytest.mark.parametrize(
    ("current", "target"),
    ALLOWED_PAIRS,
    ids=[pair_id(pair) for pair in ALLOWED_PAIRS],
)
def test_every_allowed_transition_is_recorded_atomically(
    tmp_path: Path,
    current: JobState,
    target: JobState,
) -> None:
    service, jobs, events = initialized_service(tmp_path, current)

    result = service.transition(*transition_request(target))

    assert result.job.state is target
    assert jobs.get("job-001") == result.job
    assert events.list("job-001") == (result.event,)


@pytest.mark.parametrize(
    ("current", "target"),
    FORBIDDEN_PAIRS,
    ids=[pair_id(pair) for pair in FORBIDDEN_PAIRS],
)
def test_every_forbidden_transition_fails_without_a_partial_write(
    tmp_path: Path,
    current: JobState,
    target: JobState,
) -> None:
    service, jobs, events = initialized_service(tmp_path, current)
    original = jobs.get("job-001")

    with pytest.raises(AtomicTransitionStateError) as failure:
        service.transition(*transition_request(target))

    assert isinstance(failure.value.__cause__, ForbiddenJobTransitionError)
    assert failure.value.__cause__.current_state is current
    assert failure.value.__cause__.target_state is target
    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


def test_idempotent_retry_can_return_an_already_committed_target(tmp_path: Path) -> None:
    service, jobs, events = initialized_service(tmp_path, JobState.CREATED)
    request = transition_request(JobState.CLASSIFIED)

    first = service.transition(*request)
    retry = service.transition(*request)

    assert retry == first
    assert jobs.get("job-001") == first.job
    assert events.list("job-001") == (first.event,)


def test_same_state_with_a_new_event_is_forbidden(tmp_path: Path) -> None:
    service, jobs, events = initialized_service(tmp_path, JobState.RUNNING)
    original = jobs.get("job-001")

    with pytest.raises(AtomicTransitionStateError):
        service.transition(*transition_request(JobState.RUNNING))

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("created", JobState.CLASSIFIED),
        (JobState.CREATED, "classified"),
    ],
)
def test_domain_validator_rejects_untyped_states(current: object, target: object) -> None:
    with pytest.raises(TypeError, match="JobState"):
        validate_job_transition(current, target)  # type: ignore[arg-type]
