from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import stat
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from db import (
    JobNotFoundError,
    JobRepository,
    JobRepositoryError,
    JobValidationError,
    VerificationRecord,
    VerificationRepository,
    VerificationRepositoryError,
    VerificationValidationError,
)
from engine import (
    JobState,
    ProcessRunnerError,
    ProcessTimeoutError,
    VerificationCreate,
    VerificationOutcome,
    run_process,
)


class VerificationError(RuntimeError):
    """Base error for sanitized verification failures."""


class VerificationNotFoundError(VerificationError):
    """Raised when the requested job does not exist."""


class VerificationConflictError(VerificationError):
    """Raised when the job is not ready for focused verification."""


class VerificationUnavailableError(VerificationError):
    """Raised when execution or durable evidence storage is unavailable."""


@dataclass(frozen=True)
class VerificationCommand:
    """Describe one trusted focused command without shell interpretation."""

    argv: tuple[str, ...]
    timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.argv, tuple)
            or not self.argv
            or len(self.argv) > 256
            or any(not isinstance(arg, str) or "\x00" in arg for arg in self.argv)
            or not self.argv[0]
        ):
            raise ValueError("Verification command arguments are invalid.")
        serialized = json.dumps(self.argv, ensure_ascii=False, separators=(",", ":"))
        if len(serialized.encode("utf-8")) > 65_536:
            raise ValueError("Verification command arguments are too large.")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 3_600
        ):
            raise ValueError("Verification timeout must be between zero and one hour.")


class VerificationRunner:
    """Run focused checks in a verified worktree and persist digest only evidence."""

    def __init__(
        self,
        jobs: JobRepository,
        evidence: VerificationRepository,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        if not isinstance(jobs, JobRepository) or not isinstance(evidence, VerificationRepository):
            raise TypeError("Verification requires job and evidence repositories.")
        self._jobs = jobs
        self._evidence = evidence
        self._environment = self._validate_environment(
            self._default_environment() if environment is None else environment
        )
        self._locks: dict[str, asyncio.Lock] = {}

    async def run(
        self,
        job_id: str,
        command: VerificationCommand,
    ) -> VerificationRecord:
        if not isinstance(command, VerificationCommand):
            raise TypeError("Verification requires a VerificationCommand.")
        normalized_job_id = self._validate_job_id(job_id)
        lock = self._locks.setdefault(normalized_job_id, asyncio.Lock())
        async with lock:
            worktree = self._load_worktree(normalized_job_id)
            started = time.monotonic_ns()
            try:
                result = await run_process(
                    command.argv,
                    cwd=worktree,
                    env=self._environment,
                    timeout=command.timeout_seconds,
                )
                stdout = result.stdout
                stderr = result.stderr
                exit_code = result.returncode
                outcome = (
                    VerificationOutcome.PASSED if exit_code == 0 else VerificationOutcome.FAILED
                )
            except ProcessTimeoutError as error:
                stdout = error.stdout
                stderr = error.stderr
                exit_code = None
                outcome = VerificationOutcome.TIMED_OUT
            except ProcessRunnerError as error:
                raise VerificationUnavailableError(
                    "Verification command could not be executed."
                ) from error
            duration_ms = max(0, (time.monotonic_ns() - started + 999_999) // 1_000_000)
            evidence = VerificationCreate(
                job_id=normalized_job_id,
                command_args=command.argv,
                outcome=outcome,
                exit_code=exit_code,
                duration_ms=duration_ms,
                output_digest=self.output_digest(stdout, stderr),
                stdout_bytes=len(stdout),
                stderr_bytes=len(stderr),
            )
            try:
                return self._evidence.record(evidence)
            except (VerificationRepositoryError, VerificationValidationError) as error:
                raise VerificationUnavailableError(
                    "Verification evidence could not be recorded."
                ) from error

    def _load_worktree(self, job_id: str) -> Path:
        try:
            job = self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise VerificationNotFoundError("Job does not exist.") from error
        except JobRepositoryError as error:
            raise VerificationUnavailableError("Job state is unavailable.") from error
        if job.state is not JobState.VERIFYING or job.worktree_path is None:
            raise VerificationConflictError("Job is not ready for verification.")
        path = Path(job.worktree_path)
        try:
            metadata = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise VerificationConflictError("Verification worktree is unavailable.")
            resolved = path.resolve(strict=True)
        except VerificationConflictError:
            raise
        except (OSError, RuntimeError, ValueError) as error:
            raise VerificationConflictError("Verification worktree is unavailable.") from error
        if not path.is_absolute() or path != resolved:
            raise VerificationConflictError("Verification worktree identity changed.")
        return resolved

    @staticmethod
    def output_digest(stdout: bytes, stderr: bytes) -> str:
        if not isinstance(stdout, bytes) or not isinstance(stderr, bytes):
            raise TypeError("Verification output must be bytes.")
        digest = hashlib.sha256()
        for label, value in ((b"stdout", stdout), (b"stderr", stderr)):
            digest.update(label)
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
        return digest.hexdigest()

    @staticmethod
    def _validate_job_id(value: object) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise VerificationNotFoundError("Job does not exist.")
        return value

    @staticmethod
    def _validate_environment(value: Mapping[str, str]) -> dict[str, str]:
        if not isinstance(value, Mapping) or any(
            not isinstance(key, str)
            or not key
            or "=" in key
            or "\x00" in key
            or not isinstance(item, str)
            or "\x00" in item
            for key, item in value.items()
        ):
            raise ValueError("Verification environment is invalid.")
        return dict(value)

    @staticmethod
    def _default_environment() -> dict[str, str]:
        environment = {
            "LANG": os.environ.get("LANG", "C"),
            "PATH": os.environ.get("PATH", os.defpath),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        for key in ("LC_ALL", "TMPDIR"):
            if key in os.environ:
                environment[key] = os.environ[key]
        return environment
