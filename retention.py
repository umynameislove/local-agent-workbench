from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from approval_service import ApprovalService, ApprovalStateError, ApprovalUnavailableError
from db import (
    ApprovalRecord,
    ApprovalRepository,
    ApprovalWorkflowRepository,
    DatabaseError,
    EventRecord,
    EventRepository,
    JobNotFoundError,
    JobRecord,
    JobRepository,
    JobValidationError,
    ReviewBundleRecord,
    ReviewBundleRepository,
)
from engine import ApprovalDecision, ConfigurationError, EventCreate, JobState, RetentionConfig
from promotion_target import TargetConflictError, TargetUnavailableError, target_lock
from worktree import WorktreeConflictError, WorktreeManager, WorktreeUnavailableError

_STARTED = "job.worktree.cleanup.started"
_COMPLETED = "job.worktree.cleanup.completed"


class RetentionError(RuntimeError):
    """Base error for sanitized retention failures."""


class RetentionInputError(RetentionError):
    """The cleanup request does not identify a displayed retention record."""


class RetentionMissingError(RetentionError):
    """The requested job does not exist."""


class RetentionConflictError(RetentionError):
    """Retained evidence or cleanup eligibility is not safe."""


class RetentionUnavailableError(RetentionError):
    """Retention storage or inspection is unavailable."""


@dataclass(frozen=True)
class _RejectedReview:
    job: JobRecord
    approval: ApprovalRecord
    bundle: ReviewBundleRecord
    retained: EventRecord | None
    deadline: datetime | None
    started: EventRecord | None
    completed: EventRecord | None

    @property
    def key(self) -> str:
        return f"job-{hashlib.sha256(self.job.id.encode('utf-8')).hexdigest()[:32]}"

    @property
    def intent(self) -> dict[str, object]:
        return {
            "retention_event_id": self.retained.id,
            "approval_id": self.approval.id,
            "bundle_hash": self.bundle.payload_hash,
            "worktree_key": self.key,
            "actor": "local-user",
            "branch_retained": True,
        }


class RetentionService:
    """Expose retained review evidence and explicitly dispose of one eligible tree."""

    def __init__(
        self,
        jobs: JobRepository,
        approvals: ApprovalRepository,
        workflow: ApprovalWorkflowRepository,
        bundles: ReviewBundleRepository,
        events: EventRepository,
        worktrees: WorktreeManager,
        live_review: ApprovalService,
        locks_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._jobs = jobs
        self._approvals = approvals
        self._workflow = workflow
        self._bundles = bundles
        self._events = events
        self._worktrees = worktrees
        self._live_review = live_review
        self._locks_root = locks_root
        self._clock = clock or (lambda: datetime.now(UTC))

    def read(self, job_id: str) -> dict[str, object]:
        review = self._load(job_id)
        now = self._now()
        status = (
            "cleaned"
            if review.completed is not None and self._artifact_absent(review.job)
            else "needs_attention"
            if review.completed is not None or review.started is not None
            else "legacy_retained"
            if review.retained is None
            else "needs_attention"
            if not isinstance(review.retained.payload.get("worktree_identity"), dict)
            else "eligible"
            if now >= review.deadline
            else "retained"
        )
        return {
            "job_id": review.job.id,
            "job_state": review.job.state.value,
            "approval_id": review.approval.id,
            "bundle_hash": review.bundle.payload_hash,
            "worktree_key": review.key,
            "retention_event_id": None if review.retained is None else review.retained.id,
            "retain_until": None if review.deadline is None else self._timestamp(review.deadline),
            "status": status,
            "cleanup_due": status == "eligible",
            "started_event_id": None if review.started is None else review.started.id,
            "completed_event_id": None if review.completed is None else review.completed.id,
            "effect": "Discard the unchanged rejected worktree. Keep its branch and audit records.",
        }

    async def cleanup(self, job_id: str, body: object) -> dict[str, object]:
        if (
            not isinstance(body, dict)
            or set(body) != {"retention_event_id"}
            or type(body["retention_event_id"]) is not int
            or body["retention_event_id"] < 1
        ):
            raise RetentionInputError("Cleanup requires the displayed retention event identifier.")
        initial = self._load(job_id)
        project = initial.job.request_snapshot.get("project")
        if not isinstance(project, dict) or not isinstance(project.get("root"), str):
            raise RetentionConflictError("Retained project snapshot is invalid.")
        try:
            with target_lock(Path(project["root"]), self._locks_root):
                return await self._cleanup_locked(job_id, body["retention_event_id"])
        except TargetConflictError as error:
            raise RetentionConflictError("Another operation is using this project.") from error
        except TargetUnavailableError as error:
            raise RetentionUnavailableError("Cleanup lock is unavailable.") from error

    async def _cleanup_locked(self, job_id: str, event_id: int) -> dict[str, object]:
        review = self._load(job_id)
        if review.retained is None or review.retained.id != event_id:
            raise RetentionConflictError("Displayed retention record is not current.")
        if review.completed is not None:
            if not self._artifact_absent(review.job):
                raise RetentionConflictError("A worktree artifact reappeared. Inspect it manually.")
            return self.read(job_id)
        if review.started is not None:
            raise RetentionConflictError("Cleanup outcome is uncertain. Reconcile manually.")
        if self._now() < review.deadline:
            raise RetentionConflictError("The inspection period has not expired.")
        identity = review.retained.payload["worktree_identity"]
        if not isinstance(identity, dict):
            raise RetentionConflictError("Retained worktree identity requires manual inspection.")
        try:
            await self._worktrees.verify_removable(review.job, identity)
            await self._live_review.assert_live(review.job, review.bundle)
        except (WorktreeConflictError, ApprovalStateError) as error:
            raise RetentionConflictError(
                "Retained worktree changed or cannot be disposed safely."
            ) from error
        except (WorktreeUnavailableError, ApprovalUnavailableError, DatabaseError) as error:
            raise RetentionUnavailableError("Retained worktree cannot be inspected.") from error

        try:
            started = self._workflow.record_cleanup(
                review.job,
                review.approval,
                review.bundle,
                review.retained,
                EventCreate(job_id, _STARTED, review.intent, "worktree.cleanup.started"),
            )
        except DatabaseError as error:
            raise RetentionUnavailableError("Cleanup intent could not be stored.") from error

        async def before_remove() -> None:
            await self._live_review.assert_live(review.job, review.bundle)
            if self._jobs.get(job_id) != review.job:
                raise RetentionConflictError("Retained job changed before disposal.")
            if self._now() < review.deadline:
                raise RetentionConflictError("The inspection period has not expired.")

        try:
            await self._worktrees.remove_retained(review.job, identity, before_remove=before_remove)
            self._workflow.record_cleanup(
                review.job,
                review.approval,
                review.bundle,
                review.retained,
                EventCreate(
                    job_id,
                    _COMPLETED,
                    {**review.intent, "started_event_id": started.id},
                    "worktree.cleanup.completed",
                ),
            )
        except (
            DatabaseError,
            WorktreeConflictError,
            WorktreeUnavailableError,
            ApprovalStateError,
            ApprovalUnavailableError,
            RetentionError,
        ) as error:
            raise RetentionConflictError(
                "Cleanup outcome is uncertain. Reconcile manually."
            ) from error
        return self.read(job_id)

    def _load(self, job_id: str) -> _RejectedReview:
        try:
            job = self._jobs.get(job_id)
            if job.state is not JobState.REJECTED:
                raise RetentionConflictError("Only rejected jobs have this cleanup policy.")
            rejected = tuple(
                approval
                for approval in self._approvals.list(job_id=job.id)
                if approval.decision is ApprovalDecision.REJECTED
            )
            if len(rejected) != 1:
                raise RetentionConflictError("Rejected decision evidence is incomplete.")
            approval = rejected[0]
            bundle = self._bundles.get(job.id)
            requested = self._workflow.get_request_event(approval.id, job.id)
            decided = self._workflow.get_decision_event(approval.id, job.id)
            binding = ApprovalService._event_binding(requested, approval)
            if (
                binding
                != ApprovalService._binding(job.id, bundle.scan_event_id, bundle.payload_hash)
                or ApprovalRepository._payload_hash(binding) != approval.payload_hash
                or decided.payload
                != {
                    "approval_id": approval.id,
                    "decision": "rejected",
                    "bundle_hash": bundle.payload_hash,
                    "job_state": "rejected",
                    "channel": approval.channel,
                }
            ):
                raise RetentionConflictError("Rejection does not bind the retained review.")
            events = self._events.list(job.id)
            policies = tuple(
                event for event in events if event.event_type == "job.worktree.retained"
            )
            if len(policies) > 1:
                raise RetentionConflictError("Retained policy evidence is inconsistent.")
            retained = policies[0] if policies else None
            deadline = None
            if retained is not None:
                payload = retained.payload
                if set(payload) != {
                    "approval_id",
                    "bundle_hash",
                    "retention_days",
                    "retain_until",
                    "worktree_identity",
                }:
                    raise RetentionConflictError("Retained policy evidence is invalid.")
                days = RetentionConfig(payload["retention_days"]).rejected_worktree_days
                deadline = datetime.fromisoformat(approval.decided_at) + timedelta(days=days)
                if (
                    payload["approval_id"] != approval.id
                    or payload["bundle_hash"] != bundle.payload_hash
                    or payload["retain_until"] != self._timestamp(deadline)
                    or retained.idempotency_key != f"worktree.retained:{approval.id}"
                    or retained.sequence >= decided.sequence
                ):
                    raise RetentionConflictError("Retained deadline does not match rejection.")
            starts = tuple(event for event in events if event.event_type == _STARTED)
            finishes = tuple(event for event in events if event.event_type == _COMPLETED)
            if len(starts) > 1 or len(finishes) > 1 or (finishes and not starts):
                raise RetentionConflictError("Cleanup audit evidence is inconsistent.")
            review = _RejectedReview(
                job,
                approval,
                bundle,
                retained,
                deadline,
                starts[0] if starts else None,
                finishes[0] if finishes else None,
            )
            if review.started is not None and (
                retained is None
                or review.started.payload != review.intent
                or review.started.idempotency_key != "worktree.cleanup.started"
                or review.started.sequence <= decided.sequence
            ):
                raise RetentionConflictError("Cleanup intent evidence is invalid.")
            if review.completed is not None and (
                review.completed.payload != {**review.intent, "started_event_id": review.started.id}
                or review.completed.idempotency_key != "worktree.cleanup.completed"
                or review.completed.sequence <= review.started.sequence
            ):
                raise RetentionConflictError("Cleanup completion evidence is invalid.")
            return review
        except (JobNotFoundError, JobValidationError) as error:
            raise RetentionMissingError("Job does not exist.") from error
        except (
            DatabaseError,
            ApprovalStateError,
            ConfigurationError,
            TypeError,
            ValueError,
            OverflowError,
        ) as error:
            raise RetentionUnavailableError(
                "Retained evidence is unavailable or invalid."
            ) from error

    def _now(self) -> datetime:
        try:
            now = self._clock()
            if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
                raise ValueError
            return now.astimezone(UTC)
        except (TypeError, ValueError, OverflowError) as error:
            raise RetentionUnavailableError("Retention clock is invalid.") from error

    @staticmethod
    def _artifact_absent(job: JobRecord) -> bool:
        if not job.worktree_path or not Path(job.worktree_path).is_absolute():
            raise RetentionConflictError("Retained worktree binding is invalid.")
        try:
            Path(job.worktree_path).lstat()
        except FileNotFoundError:
            return True
        except OSError as error:
            raise RetentionUnavailableError("Worktree absence cannot be inspected.") from error
        return False

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
