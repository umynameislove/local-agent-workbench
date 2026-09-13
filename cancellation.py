from __future__ import annotations

from types import MappingProxyType

from db import (
    AtomicTransitionConflictError,
    AtomicTransitionError,
    AtomicTransitionRecord,
    AtomicTransitionService,
    JobRecord,
    JobRepository,
    JobRepositoryError,
)
from engine import (
    TERMINAL_JOB_STATES,
    AdapterError,
    AdapterSession,
    EventCreate,
    ForbiddenJobTransitionError,
    JobState,
    JobUpdate,
    ProviderAdapter,
    ProviderCapabilities,
    RuntimeEvent,
    RuntimeEventKind,
    validate_job_transition,
)

_CANCEL_EVENT_TYPE = "job.cancelled"
_CANCEL_IDEMPOTENCY_KEY = "job-cancel"
_CANCEL_PAYLOAD = MappingProxyType({"state": JobState.CANCELLED.value})


class CancellationError(RuntimeError):
    """Base error for sanitized cancellation failures."""


class CancellationConflictError(CancellationError):
    """Raised when cancellation conflicts with adapter or durable state."""


class CancellationProviderError(CancellationError):
    """Raised when the provider does not acknowledge cancellation."""


class CancellationPersistenceError(CancellationError):
    """Raised when cancellation cannot be recorded durably."""


class CancellationService:
    """Request provider cancellation and persist only confirmed termination."""

    def __init__(
        self,
        jobs: JobRepository,
        transitions: AtomicTransitionService,
    ) -> None:
        if not isinstance(jobs, JobRepository):
            raise TypeError("Cancellation jobs must use JobRepository.")
        if not isinstance(transitions, AtomicTransitionService):
            raise TypeError("Cancellation transitions must use AtomicTransitionService.")
        self._jobs = jobs
        self._transitions = transitions

    async def request(
        self,
        adapter: ProviderAdapter,
        session: AdapterSession,
    ) -> None:
        capabilities = getattr(adapter, "capabilities", None)
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("Cancellation adapter must expose ProviderCapabilities.")
        if not isinstance(session, AdapterSession):
            raise TypeError("Cancellation requires an AdapterSession.")
        if capabilities.runtime is not session.runtime:
            raise CancellationConflictError("Cancellation adapter and session do not match.")

        job = self._load_job(session)
        if job.state is JobState.CANCELLED:
            return
        self._require_cancellable(job)

        try:
            await adapter.cancel(session)
        except AdapterError as error:
            raise CancellationProviderError("Provider did not acknowledge cancellation.") from error

    def confirm_cancelled(
        self,
        session: AdapterSession,
        event: RuntimeEvent,
    ) -> AtomicTransitionRecord:
        if not isinstance(session, AdapterSession):
            raise TypeError("Cancellation confirmation requires an AdapterSession.")
        if not isinstance(event, RuntimeEvent):
            raise TypeError("Cancellation confirmation requires a RuntimeEvent.")
        if event.job_id != session.job_id or event.runtime is not session.runtime:
            raise CancellationConflictError("Cancellation event and session do not match.")
        if event.kind is not RuntimeEventKind.COMPLETION or event.payload != {
            "status": JobState.CANCELLED.value
        }:
            raise CancellationConflictError("Provider event does not confirm cancellation.")

        job = self._load_job(session)
        if job.state is not JobState.CANCELLED:
            self._require_cancellable(job)
        request = JobUpdate(
            id=job.id,
            state=JobState.CANCELLED,
            runtime=job.runtime,
            model=job.model,
            worktree_path=job.worktree_path,
        )
        try:
            return self._transitions.transition(request, self._event(job.id))
        except AtomicTransitionConflictError as error:
            raise CancellationConflictError(
                "Cancellation conflicts with durable job state."
            ) from error
        except AtomicTransitionError as error:
            raise CancellationPersistenceError(
                "Confirmed cancellation could not be recorded durably."
            ) from error

    def _load_job(self, session: AdapterSession) -> JobRecord:
        try:
            job = self._jobs.get(session.job_id)
        except JobRepositoryError as error:
            raise CancellationPersistenceError("Cancellation state could not be loaded.") from error
        if job.runtime is not session.runtime:
            raise CancellationConflictError("Cancellation session does not own the durable job.")
        return job

    @staticmethod
    def _require_cancellable(job: JobRecord) -> None:
        if job.state in TERMINAL_JOB_STATES:
            raise CancellationConflictError("Terminal job cannot be cancelled.")
        try:
            validate_job_transition(job.state, JobState.CANCELLED)
        except ForbiddenJobTransitionError as error:
            raise CancellationConflictError("Job cannot enter the cancelled state.") from error

    @staticmethod
    def _event(job_id: str) -> EventCreate:
        return EventCreate(
            job_id=job_id,
            event_type=_CANCEL_EVENT_TYPE,
            payload=_CANCEL_PAYLOAD,
            idempotency_key=_CANCEL_IDEMPOTENCY_KEY,
        )
