from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from db import (
    AtomicTransitionConflictError,
    AtomicTransitionError,
    AtomicTransitionRecord,
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
from diff_service import DiffServiceError, ReadOnlyDiffService
from diff_types import DiffContentKind, DiffStatus, UnifiedDiff, valid_object_id
from engine import EventCreate, JobState, JobUpdate

_HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
_PATTERNS = (
    ("private_key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,255}\b")),
    ("openai_key", re.compile(r"\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{16,}\b")),
    ("stripe_live_key", re.compile(r"\b[rs]k_live_[A-Za-z0-9]{16,}\b")),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
    ),
    ("bearer_token", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{16,}")),
    (
        "credential_url",
        re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
    ),
)
_ASSIGNMENT = re.compile(
    r"(?i)\b(?:api[_-]?key|secret|token|password|passwd|client[_-]?secret|authorization)"
    r"\s*[:=]\s*[\"']?([A-Za-z0-9_./+=:@-]{8,})"
)
_PLACEHOLDERS = frozenset(
    {
        "changeme",
        "dummy",
        "example",
        "example-secret",
        "fake-secret",
        "not-a-secret",
        "placeholder",
        "redacted",
        "replace-me",
        "sample",
        "test-secret",
        "test-only",
    }
)
_REFERENCE_PREFIXES = (
    "config.",
    "env.",
    "getpass.",
    "headers.",
    "os.environ",
    "os.getenv",
    "request.",
    "settings.",
)


class SecretGateError(RuntimeError):
    """Base error for sanitized pre review secret gate failures."""


class SecretGateNotFoundError(SecretGateError):
    """Raised when the requested job does not exist."""


class SecretGateConflictError(SecretGateError):
    """Raised when durable job or worktree state cannot be reviewed safely."""


class SecretGateUnavailableError(SecretGateError):
    """Raised when scanning or persistence infrastructure is unavailable."""


class SecretGateBlockedError(SecretGateError):
    """Raised after a blocked scan is recorded without changing job state."""

    def __init__(self, scan: SecretScanResult, event_id: int) -> None:
        super().__init__("Review readiness is blocked by secret scan.")
        self.scan = scan
        self.event_id = event_id


@dataclass(frozen=True)
class SecretFinding:
    rule_id: str
    path: str
    line: int

    def to_dict(self) -> dict[str, object]:
        return {"rule_id": self.rule_id, "path": self.path, "line": self.line}


@dataclass(frozen=True)
class SecretScanResult:
    digest: str
    scanned_files: int
    scanned_lines: int
    findings: tuple[SecretFinding, ...]
    incomplete_paths: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.findings and not self.incomplete_paths

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "passed" if self.passed else "blocked",
            "digest": self.digest,
            "scanned_files": self.scanned_files,
            "scanned_lines": self.scanned_lines,
            "finding_count": len(self.findings),
            "incomplete_count": len(self.incomplete_paths),
            "findings": [finding.to_dict() for finding in self.findings],
            "incomplete_paths": list(self.incomplete_paths),
        }

    def event_payload(self) -> dict[str, object]:
        return {
            "status": "passed" if self.passed else "blocked",
            "scan_digest": self.digest,
            "scanned_files": self.scanned_files,
            "scanned_lines": self.scanned_lines,
            "finding_count": len(self.findings),
            "incomplete_count": len(self.incomplete_paths),
            "rule_ids": sorted({finding.rule_id for finding in self.findings}),
        }


@dataclass(frozen=True)
class ReviewReadiness:
    job_id: str
    state: JobState
    event_id: int
    scan: SecretScanResult

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "state": self.state.value,
            "event_id": self.event_id,
            "scan": self.scan.to_dict(),
        }


class SecretScanner:
    """Inspect only added review lines and return findings without matched values."""

    _maximum_findings = 100

    def scan(self, review: UnifiedDiff) -> SecretScanResult:
        if not isinstance(review, UnifiedDiff):
            raise TypeError("Secret scan requires a UnifiedDiff.")
        findings: list[SecretFinding] = []
        incomplete: list[str] = []
        scanned_files = 0
        scanned_lines = 0
        for entry in review.files:
            if entry.status is DiffStatus.DELETED:
                continue
            if entry.content_kind is not DiffContentKind.TEXT or entry.patch is None:
                incomplete.append(entry.path)
                continue
            scanned_files += 1
            for line_number, content in self._added_lines(entry.patch):
                scanned_lines += 1
                for rule_id in self._matches(content):
                    if len(findings) < self._maximum_findings:
                        findings.append(SecretFinding(rule_id, entry.path, line_number))
        if review.truncated and not incomplete:
            incomplete.append("[review-limit]")
        return SecretScanResult(
            digest=self._digest(review),
            scanned_files=scanned_files,
            scanned_lines=scanned_lines,
            findings=tuple(findings),
            incomplete_paths=tuple(sorted(set(incomplete))),
        )

    @staticmethod
    def _added_lines(patch: str) -> tuple[tuple[int, str], ...]:
        additions: list[tuple[int, str]] = []
        line_number: int | None = None
        for line in patch.splitlines():
            if match := _HUNK.match(line):
                line_number = int(match.group(1))
                continue
            if line_number is None or line.startswith("\\ No newline"):
                continue
            if line.startswith("+"):
                additions.append((line_number, line[1:]))
                line_number += 1
            elif line.startswith("-"):
                continue
            elif line.startswith(" "):
                line_number += 1
            else:
                line_number = None
        return tuple(additions)

    @classmethod
    def _matches(cls, content: str) -> tuple[str, ...]:
        matches = [rule_id for rule_id, pattern in _PATTERNS if pattern.search(content)]
        for assignment in _ASSIGNMENT.finditer(content):
            if not cls._is_placeholder(assignment.group(1)):
                matches.append("credential_assignment")
        return tuple(dict.fromkeys(matches))

    @staticmethod
    def _is_placeholder(value: str) -> bool:
        normalized = value.strip("<>[]{}()\"'").casefold()
        return (
            normalized in _PLACEHOLDERS
            or any(marker in normalized for marker in ("example", "placeholder", "redacted"))
            or normalized.startswith(_REFERENCE_PREFIXES)
            or len(set(normalized)) <= 2
        )

    @staticmethod
    def _digest(review: UnifiedDiff) -> str:
        canonical = json.dumps(
            review.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PreReviewSecretGate:
    """Collect the actual job diff and gate its atomic review ready transition."""

    def __init__(
        self,
        jobs: JobRepository,
        events: EventRepository,
        transitions: AtomicTransitionService,
        *,
        scanner: SecretScanner | None = None,
    ) -> None:
        if not isinstance(jobs, JobRepository) or not isinstance(events, EventRepository):
            raise TypeError("Secret gate requires job and event repositories.")
        if not isinstance(transitions, AtomicTransitionService):
            raise TypeError("Secret gate requires atomic transition persistence.")
        if scanner is not None and not isinstance(scanner, SecretScanner):
            raise TypeError("Secret gate scanner must use SecretScanner.")
        self._jobs = jobs
        self._events = events
        self._transitions = transitions
        self._scanner = scanner or SecretScanner()
        self._locks: dict[str, asyncio.Lock] = {}

    async def evaluate(self, job_id: str) -> ReviewReadiness:
        normalized_id = self._validate_job_id(job_id)
        lock = self._locks.setdefault(normalized_id, asyncio.Lock())
        async with lock:
            job = self._load_job(normalized_id)
            review = await self._collect(job)
            scan = self._scanner.scan(review)
            if not scan.passed:
                if job.state is JobState.REVIEW_READY:
                    raise SecretGateConflictError("Review ready content changed after scanning.")
                event = self._record_blocked(job, scan)
                raise SecretGateBlockedError(scan, event.id)
            transition = self._record_passed(job, scan)
            return ReviewReadiness(
                job_id=transition.job.id,
                state=transition.job.state,
                event_id=transition.event.id,
                scan=scan,
            )

    def _load_job(self, job_id: str) -> JobRecord:
        try:
            job = self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise SecretGateNotFoundError("Job does not exist.") from error
        except JobRepositoryError as error:
            raise SecretGateUnavailableError("Secret gate state is unavailable.") from error
        if job.state not in {JobState.VERIFYING, JobState.REVIEW_READY}:
            raise SecretGateConflictError("Job is not ready for pre review scanning.")
        if job.worktree_path is None:
            raise SecretGateConflictError("Job worktree is unavailable.")
        base_commit = job.request_snapshot.get("repo_head")
        if not isinstance(base_commit, str) or not valid_object_id(base_commit):
            raise SecretGateConflictError("Job repository snapshot is invalid.")
        return job

    async def _collect(self, job: JobRecord) -> UnifiedDiff:
        try:
            return await ReadOnlyDiffService(
                Path(job.worktree_path or ""),
                str(job.request_snapshot["repo_head"]),
            ).collect()
        except (DiffServiceError, KeyError, TypeError, ValueError) as error:
            raise SecretGateUnavailableError(
                "Review content could not be scanned safely."
            ) from error

    def _record_blocked(self, job: JobRecord, scan: SecretScanResult) -> EventRecord:
        event = EventCreate(
            job_id=job.id,
            event_type="job.secret_scan.blocked",
            payload=scan.event_payload(),
            idempotency_key=f"secret-scan-blocked:{scan.digest}",
        )
        try:
            return self._events.append(event)
        except EventRepositoryError as error:
            raise SecretGateUnavailableError(
                "Blocked secret scan could not be recorded."
            ) from error

    def _record_passed(
        self,
        job: JobRecord,
        scan: SecretScanResult,
    ) -> AtomicTransitionRecord:
        update = JobUpdate(
            id=job.id,
            state=JobState.REVIEW_READY,
            runtime=job.runtime,
            model=job.model,
            worktree_path=job.worktree_path,
        )
        event = EventCreate(
            job_id=job.id,
            event_type="job.review_ready",
            payload=scan.event_payload(),
            idempotency_key=f"secret-scan-passed:{scan.digest}",
        )
        try:
            return self._transitions.transition(update, event)
        except AtomicTransitionConflictError as error:
            raise SecretGateConflictError(
                "Review readiness conflicts with durable job state."
            ) from error
        except AtomicTransitionError as error:
            raise SecretGateUnavailableError("Review readiness could not be recorded.") from error

    @staticmethod
    def _validate_job_id(value: object) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise SecretGateNotFoundError("Job does not exist.")
        return value
