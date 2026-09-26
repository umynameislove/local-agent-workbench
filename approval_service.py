from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from db import (
    ApprovalDecisionConflictError,
    ApprovalExpiredError,
    ApprovalNotFoundError,
    ApprovalPayloadMismatchError,
    ApprovalRecord,
    ApprovalRepository,
    ApprovalValidationError,
    ApprovalWorkflowConflictError,
    ApprovalWorkflowRecord,
    ApprovalWorkflowRepository,
    DatabaseError,
    EventRecord,
    JobNotFoundError,
    JobRecord,
    JobRepository,
    JobValidationError,
    ReviewBundleNotFoundError,
    ReviewBundleRecord,
    ReviewBundleRepository,
)
from diff_service import DiffServiceError, ReadOnlyDiffService
from diff_types import valid_object_id
from engine import ApprovalCreate, ApprovalDecision, ApprovalResolution, JobState
from secret_gate import SecretScanner


class ApprovalServiceError(RuntimeError):
    """Base error for sanitized approval API failures."""


class ApprovalInputError(ApprovalServiceError):
    """Raised when a request does not follow the approval contract."""


class ApprovalMissingError(ApprovalServiceError):
    """Raised when a requested job or approval does not exist."""


class ApprovalStateError(ApprovalServiceError):
    """Raised when review content or committed state has changed."""


class ApprovalUnavailableError(ApprovalServiceError):
    """Raised when review or durable state cannot be checked safely."""


class ApprovalService:
    """Bind a human decision to one frozen, still current review bundle."""

    def __init__(
        self,
        jobs: JobRepository,
        approvals: ApprovalRepository,
        bundles: ReviewBundleRepository,
        workflow: ApprovalWorkflowRepository,
        *,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._jobs = jobs
        self._approvals = approvals
        self._bundles = bundles
        self._workflow = workflow
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self._scanner = SecretScanner()

    async def request(self, job_id: str, body: object) -> dict[str, object]:
        values = self._body(body, {"bundle_hash"})
        viewed_hash = self._hash(values["bundle_hash"])
        job = self._job(job_id)
        if job.state not in {JobState.REVIEW_READY, JobState.WAITING_APPROVAL}:
            raise ApprovalStateError("Job is not ready to request approval.")
        bundle = self._bundle(job.id)
        if viewed_hash != bundle.payload_hash:
            raise ApprovalStateError("Displayed review bundle is no longer current.")
        await self._assert_live(job, bundle)
        payload = self._binding(job.id, bundle.scan_event_id, bundle.payload_hash)
        now = self._clock()
        try:
            approval = ApprovalCreate(
                id=self._id_factory(),
                job_id=job.id,
                payload=payload,
                expires_at=now + timedelta(minutes=15),
            )
            result = self._workflow.request(approval, job, bundle)
        except (ApprovalExpiredError, ApprovalWorkflowConflictError) as error:
            raise ApprovalStateError(str(error)) from error
        except DatabaseError as error:
            raise ApprovalUnavailableError("Approval request could not be stored.") from error
        return self._request_response(result, bundle.payload_hash)

    async def decide(
        self,
        approval_id: str,
        body: object,
        *,
        actor: str = "local-user",
        channel: str = "local-api",
    ) -> dict[str, object]:
        values = self._body(body, {"decision", "bundle_hash"})
        viewed_hash = self._hash(values["bundle_hash"])
        try:
            decision = ApprovalDecision(values["decision"])
        except (TypeError, ValueError) as error:
            raise ApprovalInputError("Approval decision is invalid.") from error
        try:
            approval = self._approvals.get(approval_id)
            requested = self._workflow.get_request_event(approval.id, approval.job_id)
        except ApprovalNotFoundError as error:
            raise ApprovalMissingError("Approval does not exist.") from error
        except ApprovalValidationError as error:
            raise ApprovalInputError("Approval identifier is invalid.") from error
        except ApprovalWorkflowConflictError as error:
            raise ApprovalStateError(str(error)) from error
        except DatabaseError as error:
            raise ApprovalUnavailableError("Approval state is unavailable.") from error
        binding = self._event_binding(requested, approval)
        if viewed_hash != binding["bundle_hash"]:
            raise ApprovalStateError("Displayed review bundle does not match approval.")
        expected_job: JobRecord | None = None
        expected_bundle: ReviewBundleRecord | None = None
        if approval.decision is None:
            expected_job = self._job(approval.job_id)
            expected_bundle = self._bundle(approval.job_id)
            if (
                expected_job.state is not JobState.WAITING_APPROVAL
                or expected_bundle.payload_hash != viewed_hash
                or expected_bundle.scan_event_id != binding["scan_event_id"]
            ):
                raise ApprovalStateError("Review changed while approval was pending.")
            await self._assert_live(expected_job, expected_bundle)
        resolution = ApprovalResolution(
            decision=decision,
            actor=actor,
            channel=channel,
            payload=binding,
        )
        try:
            result = self._workflow.resolve(approval_id, resolution, expected_job, expected_bundle)
        except (
            ApprovalDecisionConflictError,
            ApprovalExpiredError,
            ApprovalPayloadMismatchError,
            ApprovalWorkflowConflictError,
        ) as error:
            raise ApprovalStateError(str(error)) from error
        except ApprovalValidationError as error:
            raise ApprovalInputError("Approval actor or channel is invalid.") from error
        except DatabaseError as error:
            raise ApprovalUnavailableError("Approval decision could not be stored.") from error
        return self._decision_response(result, viewed_hash)

    def _job(self, job_id: str) -> JobRecord:
        try:
            return self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise ApprovalMissingError("Job does not exist.") from error
        except DatabaseError as error:
            raise ApprovalUnavailableError("Job state is unavailable.") from error

    def _bundle(self, job_id: str) -> ReviewBundleRecord:
        try:
            return self._bundles.get(job_id)
        except ReviewBundleNotFoundError as error:
            raise ApprovalStateError("Review bundle is not ready.") from error
        except DatabaseError as error:
            raise ApprovalUnavailableError("Review bundle is unavailable.") from error

    async def _assert_live(self, job: JobRecord, bundle: ReviewBundleRecord) -> None:
        base = job.request_snapshot.get("repo_head")
        if job.worktree_path is None or not isinstance(base, str) or not valid_object_id(base):
            raise ApprovalStateError("Job worktree snapshot is invalid.")
        snapshot = json.dumps(
            job.request_snapshot,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        evidence = bundle.payload
        if (
            evidence.get("job_id") != job.id
            or evidence.get("project_id") != job.project_id
            or evidence.get("runtime") != job.runtime.value
            or evidence.get("model") != job.model
            or evidence.get("request_snapshot_hash")
            != hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
            or evidence.get("scan_event_id") != bundle.scan_event_id
        ):
            raise ApprovalStateError("Review bundle no longer matches job identity.")
        try:
            diff = await ReadOnlyDiffService(Path(job.worktree_path), base).collect()
        except (DiffServiceError, OSError, TypeError, ValueError) as error:
            raise ApprovalUnavailableError("Live review content could not be checked.") from error
        scan = self._scanner.scan(diff)
        if (
            not scan.passed
            or scan.digest != evidence.get("diff_digest")
            or diff.to_dict() != evidence.get("diff")
        ):
            raise ApprovalStateError("Worktree content changed after review.")

    @staticmethod
    def _body(value: object, fields: set[str]) -> Mapping[str, object]:
        if not isinstance(value, dict) or set(value) != fields:
            raise ApprovalInputError("Approval request fields are invalid.")
        return value

    @staticmethod
    def _hash(value: object) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ApprovalInputError("Review bundle hash is invalid.")
        return value

    @staticmethod
    def _binding(job_id: str, scan_event_id: int, bundle_hash: str) -> dict[str, object]:
        return {
            "job_id": job_id,
            "scan_event_id": scan_event_id,
            "bundle_hash": bundle_hash,
        }

    @classmethod
    def _event_binding(cls, event: EventRecord, approval: ApprovalRecord) -> dict[str, object]:
        bundle_hash = event.payload.get("bundle_hash")
        scan_event_id = event.payload.get("scan_event_id")
        if (
            event.job_id != approval.job_id
            or event.payload.get("approval_id") != approval.id
            or event.payload.get("expires_at")
            != approval.expires_at.isoformat(timespec="microseconds").replace("+00:00", "Z")
            or not isinstance(scan_event_id, int)
            or isinstance(scan_event_id, bool)
            or scan_event_id < 1
        ):
            raise ApprovalStateError("Approval request binding is invalid.")
        try:
            cls._hash(bundle_hash)
        except ApprovalInputError as error:
            raise ApprovalStateError("Approval request binding is invalid.") from error
        return cls._binding(approval.job_id, scan_event_id, bundle_hash)

    @staticmethod
    def _request_response(result: ApprovalWorkflowRecord, bundle_hash: str) -> dict[str, object]:
        return {
            "approval_id": result.approval.id,
            "job_id": result.approval.job_id,
            "bundle_hash": bundle_hash,
            "expires_at": result.approval.expires_at.isoformat(),
            "state": "pending",
            "event_id": result.event.id,
        }

    @staticmethod
    def _decision_response(result: ApprovalWorkflowRecord, bundle_hash: str) -> dict[str, object]:
        return {
            "approval_id": result.approval.id,
            "job_id": result.approval.job_id,
            "bundle_hash": bundle_hash,
            "decision": result.approval.decision.value,
            "job_state": result.event.payload["job_state"],
            "decided_at": result.approval.decided_at,
            "event_id": result.event.id,
        }
