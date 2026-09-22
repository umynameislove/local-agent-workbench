from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

from db import (
    EventRecord,
    EventRepository,
    EventRepositoryError,
    JobNotFoundError,
    JobRecord,
    JobRepository,
    JobRepositoryError,
    JobValidationError,
    ReviewBundleConflictError,
    ReviewBundleNotFoundError,
    ReviewBundleRecord,
    ReviewBundleRepository,
    ReviewBundleRepositoryError,
    VerificationRecord,
    VerificationRepository,
    VerificationRepositoryError,
)
from diff_service import DiffServiceError, ReadOnlyDiffService
from diff_types import DiffContentKind, DiffStatus, UnifiedDiff, valid_object_id
from engine import JobState, VerificationOutcome
from logging_setup import REDACTED, redact_text
from secret_gate import SecretScanner


class ReviewBundleError(RuntimeError):
    """Base error for sanitized review bundle failures."""


class ReviewBundleMissingError(ReviewBundleError):
    """Raised when the requested job or bundle does not exist."""


class ReviewBundleStateError(ReviewBundleError):
    """Raised when the review evidence no longer matches the worktree."""


class ReviewBundleUnavailableError(ReviewBundleError):
    """Raised when durable review evidence cannot be read or stored."""


class ReviewBundleService:
    """Freeze the exact reviewed diff and verification evidence once per job."""

    def __init__(
        self,
        jobs: JobRepository,
        events: EventRepository,
        verification: VerificationRepository,
        bundles: ReviewBundleRepository,
    ) -> None:
        self._jobs = jobs
        self._events = events
        self._verification = verification
        self._bundles = bundles
        self._scanner = SecretScanner()
        self._locks: dict[str, asyncio.Lock] = {}

    async def create(self, job_id: str) -> dict[str, object]:
        normalized = self._validate_job_id(job_id)
        lock = self._locks.setdefault(normalized, asyncio.Lock())
        async with lock:
            job = self._load_job(normalized)
            try:
                diff = await ReadOnlyDiffService(
                    Path(job.worktree_path or ""), str(job.request_snapshot["repo_head"])
                ).collect()
            except (DiffServiceError, TypeError, ValueError) as error:
                raise ReviewBundleUnavailableError(
                    "Review content could not be collected safely."
                ) from error
            scan = self._scanner.scan(diff)
            if not scan.passed:
                raise ReviewBundleStateError("Review content no longer passes the secret gate.")
            try:
                events = self._events.list(normalized)
                checks = self._verification.list(job_id=normalized)
            except (EventRepositoryError, VerificationRepositoryError) as error:
                raise ReviewBundleUnavailableError("Review evidence is unavailable.") from error
            readiness = next(
                (event for event in reversed(events) if event.event_type == "job.review_ready"),
                None,
            )
            if readiness is None or readiness.payload.get("scan_digest") != scan.digest:
                raise ReviewBundleStateError("Review content changed after the secret scan.")
            payload = self._payload(job, readiness, diff, scan.digest, checks)
            try:
                record = self._bundles.create(job, readiness, payload)
            except ReviewBundleConflictError as error:
                raise ReviewBundleStateError(str(error)) from error
            except ReviewBundleRepositoryError as error:
                raise ReviewBundleUnavailableError("Review bundle could not be stored.") from error
            return self._response(record)

    def read(self, job_id: str) -> dict[str, object]:
        try:
            record = self._bundles.get(self._validate_job_id(job_id))
        except ReviewBundleNotFoundError as error:
            raise ReviewBundleMissingError("Review bundle does not exist.") from error
        except ReviewBundleRepositoryError as error:
            raise ReviewBundleUnavailableError("Review bundle is unavailable.") from error
        return self._response(record)

    def _load_job(self, job_id: str) -> JobRecord:
        try:
            job = self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise ReviewBundleMissingError("Job does not exist.") from error
        except JobRepositoryError as error:
            raise ReviewBundleUnavailableError("Job state is unavailable.") from error
        if job.state is not JobState.REVIEW_READY or job.worktree_path is None:
            raise ReviewBundleStateError("Job is not ready for review bundling.")
        base = job.request_snapshot.get("repo_head")
        if not isinstance(base, str) or not valid_object_id(base):
            raise ReviewBundleStateError("Job repository snapshot is invalid.")
        return job

    @staticmethod
    def _payload(
        job: JobRecord,
        readiness: EventRecord,
        diff: UnifiedDiff,
        diff_digest: str,
        checks: tuple[VerificationRecord, ...],
    ) -> dict[str, object]:
        counts = {status.value: 0 for status in DiffStatus}
        for entry in diff.files:
            counts[entry.status.value] += 1
        risks: list[str] = []
        if not checks:
            risks.append("No verification command was recorded.")
        elif any(check.outcome is not VerificationOutcome.PASSED for check in checks):
            risks.append("At least one verification command did not pass.")
        if any(entry.content_kind is not DiffContentKind.TEXT for entry in diff.files):
            risks.append("Some changed content is summarized rather than shown.")
        if not diff.files:
            risks.append("No file changes were found.")
        snapshot = json.dumps(
            job.request_snapshot,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return {
            "schema_version": 1,
            "job_id": job.id,
            "project_id": job.project_id,
            "runtime": job.runtime.value,
            "model": job.model,
            "scan_event_id": readiness.id,
            "diff_digest": diff_digest,
            "request_snapshot_hash": hashlib.sha256(snapshot.encode("utf-8")).hexdigest(),
            "summary": {
                "task": ReviewBundleService._safe_text(job.request),
                "changed_files": len(diff.files),
                "changes": counts,
            },
            "diff": diff.to_dict(),
            "verification": [
                {
                    "id": check.id,
                    "command_args": ReviewBundleService._safe_arguments(check.command_args),
                    "command_hash": hashlib.sha256(
                        json.dumps(
                            check.command_args, ensure_ascii=False, separators=(",", ":")
                        ).encode("utf-8")
                    ).hexdigest(),
                    "outcome": check.outcome.value,
                    "exit_code": check.exit_code,
                    "duration_ms": check.duration_ms,
                    "output_digest": check.output_digest,
                    "stdout_bytes": check.stdout_bytes,
                    "stderr_bytes": check.stderr_bytes,
                    "created_at": check.created_at,
                }
                for check in checks
            ],
            "risks": risks,
        }

    @staticmethod
    def _safe_text(value: str) -> str:
        if SecretScanner._matches(value):
            return REDACTED
        return redact_text(value)

    @classmethod
    def _safe_arguments(cls, arguments: tuple[str, ...]) -> list[str]:
        sensitive_options = {
            "--api-key",
            "--authorization",
            "--password",
            "--secret",
            "--token",
        }
        displayed: list[str] = []
        redact_next = False
        for argument in arguments:
            option = argument.split("=", 1)[0].casefold()
            if redact_next:
                displayed.append(REDACTED)
                redact_next = False
            elif option in sensitive_options:
                displayed.append(f"{option}={REDACTED}" if "=" in argument else option)
                redact_next = "=" not in argument
            else:
                displayed.append(cls._safe_text(argument))
        return displayed

    @staticmethod
    def _response(record: ReviewBundleRecord) -> dict[str, object]:
        return {
            "job_id": record.job_id,
            "created_at": record.created_at,
            "bundle_hash": record.payload_hash,
            "bundle": record.payload,
        }

    @staticmethod
    def _validate_job_id(value: object) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise ReviewBundleMissingError("Job does not exist.")
        return value
