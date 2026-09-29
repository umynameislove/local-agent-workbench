from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path

from approval_service import ApprovalService, ApprovalStateError, ApprovalUnavailableError
from db import (
    ApprovalNotFoundError,
    ApprovalRecord,
    ApprovalRepository,
    ApprovalRepositoryError,
    ApprovalValidationError,
    ApprovalWorkflowConflictError,
    ApprovalWorkflowRepository,
    AtomicTransitionConflictError,
    AtomicTransitionError,
    AtomicTransitionService,
    JobNotFoundError,
    JobRecord,
    JobRepository,
    JobRepositoryError,
    ProjectNotFoundError,
    ProjectRepository,
    ProjectRepositoryError,
    ReviewBundleNotFoundError,
    ReviewBundleRecord,
    ReviewBundleRepository,
    ReviewBundleRepositoryError,
)
from diff_service import DiffServiceError, ReadOnlyDiffService
from diff_types import valid_object_id
from engine import (
    ApprovalDecision,
    EventCreate,
    JobState,
    JobUpdate,
    PermissionMode,
    ProcessRunnerError,
)
from promotion_target import (
    TargetConflictError,
    TargetUnavailableError,
    apply_patch,
    assert_applied,
    file_state,
    require_clean_target,
    target_lock,
)
from worktree import WorktreeConflictError, WorktreeManager, WorktreeUnavailableError
from write_boundary import WriteBoundary, WriteBoundaryError


class PromotionError(RuntimeError):
    """Base error for sanitized local promotion failures."""


class PromotionInputError(PromotionError):
    """Raised when a promotion request does not identify one reviewed payload."""


class PromotionMissingError(PromotionError):
    """Raised when the approval or its job does not exist."""


class PromotionConflictError(PromotionError):
    """Raised before any write when reviewed or target state has changed."""


class PromotionUnavailableError(PromotionError):
    """Raised when a safe preflight or durable transition is unavailable."""


class PromotionReconciliationError(PromotionError):
    """Raised after promotion entered applying and may have changed the target."""


class PromotionService:
    """Apply one frozen, human approved text patch to its original local project."""

    def __init__(
        self,
        projects: ProjectRepository,
        jobs: JobRepository,
        approvals: ApprovalRepository,
        workflow: ApprovalWorkflowRepository,
        bundles: ReviewBundleRepository,
        transitions: AtomicTransitionService,
        worktrees: WorktreeManager,
        live_review: ApprovalService,
        locks_root: Path,
    ) -> None:
        self._projects = projects
        self._jobs = jobs
        self._approvals = approvals
        self._workflow = workflow
        self._bundles = bundles
        self._transitions = transitions
        self._worktrees = worktrees
        self._live_review = live_review
        self._locks_root = locks_root
        self._locks: dict[str, asyncio.Lock] = {}
        self._project_locks: dict[Path, asyncio.Lock] = {}

    async def promote(self, approval_id: str, body: object) -> dict[str, object]:
        viewed_hash = self._request_hash(body)
        if not isinstance(approval_id, str) or not approval_id or "\x00" in approval_id:
            raise PromotionInputError("Approval identifier is invalid.")
        lock = self._locks.setdefault(approval_id, asyncio.Lock())
        async with lock:
            return await self._promote_locked(approval_id, viewed_hash)

    async def _promote_locked(self, approval_id: str, viewed_hash: str) -> dict[str, object]:
        try:
            approval = self._approvals.get(approval_id)
            job = self._jobs.get(approval.job_id)
            bundle = self._bundles.get(job.id)
            requested = self._workflow.get_request_event(approval.id, job.id)
            decided = self._workflow.get_decision_event(approval.id, job.id)
        except (ApprovalNotFoundError, JobNotFoundError) as error:
            raise PromotionMissingError("Approval or job does not exist.") from error
        except ReviewBundleNotFoundError as error:
            raise PromotionConflictError("Approved review bundle is missing.") from error
        except (ApprovalValidationError, ApprovalWorkflowConflictError) as error:
            raise PromotionConflictError("Approval record is not complete.") from error
        except (ApprovalRepositoryError, JobRepositoryError, ReviewBundleRepositoryError) as error:
            raise PromotionUnavailableError("Promotion state is unavailable.") from error

        binding = {
            "job_id": job.id,
            "scan_event_id": bundle.scan_event_id,
            "bundle_hash": bundle.payload_hash,
        }
        if (
            viewed_hash != bundle.payload_hash
            or approval.decision is not ApprovalDecision.APPROVED
            or approval.decided_at is None
            or approval.payload_hash != ApprovalRepository._payload_hash(binding)
            or requested.payload
            != {
                "approval_id": approval.id,
                "bundle_hash": bundle.payload_hash,
                "scan_event_id": bundle.scan_event_id,
                "expires_at": approval.expires_at.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
            }
            or decided.payload
            != {
                "approval_id": approval.id,
                "decision": ApprovalDecision.APPROVED.value,
                "bundle_hash": bundle.payload_hash,
                "job_state": JobState.APPROVED.value,
                "channel": approval.channel,
            }
        ):
            raise PromotionConflictError("Approval does not bind the current review bundle.")
        if job.state is not JobState.APPROVED:
            raise PromotionConflictError("Job is not awaiting local promotion.")

        root, base = self._target(job)
        lock = self._project_locks.setdefault(root, asyncio.Lock())
        async with lock:
            try:
                with target_lock(root, self._locks_root):
                    return await self._apply_locked(approval, job, bundle, root, base)
            except TargetConflictError as error:
                raise PromotionConflictError(str(error)) from error
            except TargetUnavailableError as error:
                raise PromotionUnavailableError(str(error)) from error

    async def _apply_locked(
        self,
        approval: ApprovalRecord,
        job: JobRecord,
        bundle: ReviewBundleRecord,
        root: Path,
        base: str,
    ) -> dict[str, object]:
        try:
            await self._worktrees.verify_bound(job)
            await self._live_review.assert_live(job, bundle)
        except (WorktreeConflictError, ApprovalStateError) as error:
            raise PromotionConflictError(
                "Approved worktree changed or lost its binding."
            ) from error
        except (WorktreeUnavailableError, ApprovalUnavailableError) as error:
            raise PromotionUnavailableError("Approved worktree cannot be verified.") from error

        patch, paths = self._frozen_patch(bundle.payload, base)
        try:
            boundary = WriteBoundary(root)
            for path in paths:
                boundary.authorize(path)
        except (WriteBoundaryError, OSError, ValueError) as error:
            raise PromotionConflictError("Approved file path is not safe in the target.") from error

        try:
            source = WriteBoundary(Path(job.worktree_path or ""))
            approved_states = {path: file_state(source, path) for path in set(paths)}
            await self._live_review.assert_live(job, bundle)
        except (
            WriteBoundaryError,
            OSError,
            TargetConflictError,
            ApprovalStateError,
        ) as error:
            raise PromotionConflictError(
                "Approved file content changed before promotion."
            ) from error
        except ApprovalUnavailableError as error:
            raise PromotionUnavailableError("Approved file content cannot be verified.") from error

        await require_clean_target(root, base)
        checked = await apply_patch(root, patch, check=True)
        if checked.returncode != 0:
            raise PromotionConflictError("Approved patch cannot be applied to the target.")
        await require_clean_target(root, base)

        payload = {
            "approval_id": approval.id,
            "bundle_hash": bundle.payload_hash,
            "base_commit": base,
            "file_count": len(bundle.payload["diff"]["files"]),
        }

        def update(state: JobState) -> JobUpdate:
            return JobUpdate(job.id, state, job.runtime, job.model, job.worktree_path)

        try:
            started = self._transitions.transition(
                update(JobState.APPLYING),
                EventCreate(job.id, "job.promotion.started", payload),
                expected_job=job,
            )
        except AtomicTransitionConflictError as error:
            raise PromotionConflictError("Job state changed before promotion.") from error
        except AtomicTransitionError as error:
            raise PromotionUnavailableError("Promotion could not be started durably.") from error

        try:
            await require_clean_target(root, base)
            applied = await apply_patch(root, patch, check=False)
            if applied.returncode != 0:
                raise PromotionReconciliationError("Promotion may be partial. Reconcile manually.")
            actual = await ReadOnlyDiffService(root, base).collect()
            await assert_applied(root, approved_states, actual)
            completed = self._transitions.transition(
                update(JobState.COMPLETED),
                EventCreate(job.id, "job.promotion.completed", payload),
                expected_job=started.job,
            )
        except (
            ProcessRunnerError,
            DiffServiceError,
            TargetConflictError,
            TargetUnavailableError,
            AtomicTransitionError,
            ValueError,
        ) as error:
            raise PromotionReconciliationError(
                "Promotion outcome is uncertain. Reconcile manually."
            ) from error
        return {
            "approval_id": approval.id,
            "job_id": job.id,
            "bundle_hash": bundle.payload_hash,
            "job_state": completed.job.state.value,
            "started_event_id": started.event.id,
            "completed_event_id": completed.event.id,
        }

    @staticmethod
    def _request_hash(body: object) -> str:
        if not isinstance(body, dict) or set(body) != {"bundle_hash"}:
            raise PromotionInputError("Promotion request fields are invalid.")
        value = body["bundle_hash"]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise PromotionInputError("Review bundle hash is invalid.")
        return value

    def _target(self, job: JobRecord) -> tuple[Path, str]:
        snapshot = job.request_snapshot.get("project")
        base = job.request_snapshot.get("repo_head")
        if (
            not isinstance(snapshot, Mapping)
            or not isinstance(base, str)
            or not valid_object_id(base)
        ):
            raise PromotionConflictError("Job project snapshot is invalid.")
        try:
            project = self._projects.get(job.project_id)
        except ProjectNotFoundError as error:
            raise PromotionConflictError("Target project is no longer registered.") from error
        except ProjectRepositoryError as error:
            raise PromotionUnavailableError("Target project is unavailable.") from error
        expected = {
            "id": project.id,
            "root": project.root,
            "permission_mode": project.permission_mode.value,
            "sensitivity": project.sensitivity.value,
            "cloud_allowed": project.cloud_allowed,
        }
        if snapshot != expected or project.permission_mode is not PermissionMode.SANDBOXED_WRITE:
            raise PromotionConflictError("Target project policy changed after submission.")
        return Path(project.root), base.lower()

    @staticmethod
    def _frozen_patch(payload: Mapping[str, object], base: str) -> tuple[bytes, tuple[str, ...]]:
        diff = payload.get("diff")
        if not isinstance(diff, dict) or diff.get("base_commit") != base:
            raise PromotionConflictError("Approved diff does not match the job base.")
        files = diff.get("files")
        if diff.get("truncated") is not False or not isinstance(files, list) or not files:
            raise PromotionConflictError("Approved diff is incomplete or empty.")
        patches: list[str] = []
        paths: list[str] = []
        for item in files:
            if not isinstance(item, dict) or item.get("content_kind") != "text":
                raise PromotionConflictError("Approved diff contains unsupported content.")
            if item.get("status") not in {"added", "modified", "deleted", "renamed"}:
                raise PromotionConflictError("Approved diff contains an unsupported change.")
            patch = item.get("patch")
            if not isinstance(patch, str) or not patch.startswith("diff --git "):
                raise PromotionConflictError("Approved diff lacks a complete patch.")
            path = item.get("path")
            previous = item.get("previous_path")
            if not isinstance(path, str) or (
                previous is not None and not isinstance(previous, str)
            ):
                raise PromotionConflictError("Approved diff contains an invalid path.")
            patches.append(patch)
            paths.append(path)
            if previous is not None:
                paths.append(previous)
        data = "".join(patches).encode("utf-8")
        if len(data) != diff.get("patch_bytes"):
            raise PromotionConflictError("Approved patch size is inconsistent.")
        return data, tuple(paths)
