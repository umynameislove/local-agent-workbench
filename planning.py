from __future__ import annotations

from dataclasses import dataclass

from db import (
    AtomicTransitionConflictError,
    AtomicTransitionError,
    AtomicTransitionService,
    EventRecord,
    EventRepository,
    EventRepositoryError,
    JobNotFoundError,
    JobRecord,
    JobRepository,
    JobRepositoryError,
    JobValidationError,
)
from engine import EventCreate, JobState, JobUpdate

_PLAN_EVENT_TYPE = "job.plan.recorded"
_DEMO_PLAN = (
    "Demo planning preview. No repository files have been inspected.\n"
    "1. Locate the source and tests relevant to the request.\n"
    "2. Propose a bounded change in an isolated worktree.\n"
    "3. Run focused verification and review the resulting diff.\n"
    "4. Request approval before applying any change."
)


class PlanningError(RuntimeError):
    """Base error for sanitized planning failures."""


class PlanningNotFoundError(PlanningError):
    """Raised when the requested job does not exist."""


class PlanningConflictError(PlanningError):
    """Raised when the job cannot enter or repeat the planning stage."""


class PlanningUnavailableError(PlanningError):
    """Raised when durable planning state cannot be loaded or recorded."""


@dataclass(frozen=True)
class PlanBlock:
    job_id: str
    state: JobState
    event_id: int
    source: str
    read_only: bool
    text: str

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "state": self.state.value,
            "event_id": self.event_id,
            "plan": {
                "source": self.source,
                "read_only": self.read_only,
                "text": self.text,
            },
        }


class ReadOnlyPlanningService:
    """Record a durable demo plan without touching a project repository."""

    def __init__(
        self,
        jobs: JobRepository,
        events: EventRepository,
        transitions: AtomicTransitionService,
    ) -> None:
        if not isinstance(jobs, JobRepository) or not isinstance(events, EventRepository):
            raise TypeError("Planning requires job and event repositories.")
        if not isinstance(transitions, AtomicTransitionService):
            raise TypeError("Planning requires atomic job transitions.")
        self._jobs = jobs
        self._events = events
        self._transitions = transitions

    def run(self, job_id: str) -> PlanBlock:
        for _ in range(5):
            job = self._load_job(job_id)
            if job.worktree_path is not None:
                raise PlanningConflictError("Planning cannot use an existing worktree.")
            if job.state is JobState.QUEUED:
                return self._read_recorded(job)
            if job.state is JobState.CREATED:
                self._advance(job, JobState.CLASSIFIED, "job.classified", "plan-classified")
            elif job.state is JobState.CLASSIFIED:
                self._advance(
                    job,
                    JobState.PLANNING,
                    "job.planning.started",
                    "plan-started",
                )
            elif job.state is JobState.PLANNING:
                if not self._has_demo_start(job.id):
                    raise PlanningConflictError("This planning session is not a demo session.")
                self._advance(job, JobState.QUEUED, _PLAN_EVENT_TYPE, "plan-recorded")
            else:
                raise PlanningConflictError("Job cannot enter the planning stage.")
        raise PlanningConflictError("Planning state changed during the request.")

    def read(self, job_id: str) -> PlanBlock:
        return self._read_recorded(self._load_job(job_id))

    def _load_job(self, job_id: str) -> JobRecord:
        try:
            return self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise PlanningNotFoundError("Job does not exist.") from error
        except JobRepositoryError as error:
            raise PlanningUnavailableError("Planning state is unavailable.") from error

    def _events_for(self, job_id: str) -> tuple[EventRecord, ...]:
        try:
            return self._events.list(job_id)
        except EventRepositoryError as error:
            raise PlanningUnavailableError("Planning events are unavailable.") from error

    def _has_demo_start(self, job_id: str) -> bool:
        return any(
            event.event_type == "job.planning.started" and event.payload == {"source": "demo"}
            for event in self._events_for(job_id)
        )

    def _read_recorded(self, job: JobRecord) -> PlanBlock:
        records = tuple(
            event for event in self._events_for(job.id) if event.event_type == _PLAN_EVENT_TYPE
        )
        if len(records) != 1:
            raise PlanningConflictError("Job has no coherent recorded plan.")
        event = records[0]
        payload = event.payload
        if (
            payload.keys() != {"source", "read_only", "text"}
            or payload["source"] != "demo"
            or payload["read_only"] is not True
            or payload["text"] != _DEMO_PLAN
        ):
            raise PlanningConflictError("Recorded plan data is invalid.")
        return PlanBlock(
            job_id=job.id,
            state=job.state,
            event_id=event.id,
            source=payload["source"],
            read_only=payload["read_only"],
            text=payload["text"],
        )

    def _advance(
        self,
        job: JobRecord,
        target: JobState,
        event_type: str,
        key: str,
    ) -> None:
        payload: dict[str, object] = {"source": "demo"}
        if target is JobState.CLASSIFIED:
            payload = {"basis": "stored_snapshot"}
        elif target is JobState.QUEUED:
            payload = {
                "source": "demo",
                "read_only": True,
                "text": _DEMO_PLAN,
            }
        update = JobUpdate(
            id=job.id,
            state=target,
            runtime=job.runtime,
            model=job.model,
            worktree_path=job.worktree_path,
        )
        event = EventCreate(
            job_id=job.id,
            event_type=event_type,
            payload=payload,
            idempotency_key=key,
        )
        try:
            self._transitions.transition(update, event)
        except AtomicTransitionConflictError:
            return
        except AtomicTransitionError as error:
            raise PlanningUnavailableError("Planning could not be recorded.") from error
