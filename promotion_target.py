from __future__ import annotations

import errno
import fcntl
import hashlib
import os
import stat
from contextlib import contextmanager
from pathlib import Path

from diff_service import DiffServiceError, ReadOnlyDiffService
from diff_types import DiffContentKind, UnifiedDiff
from engine import ProcessRunnerError, run_process
from write_boundary import WriteBoundary, WriteBoundaryError


class TargetConflictError(RuntimeError):
    """The destination no longer matches the approved target."""


class TargetUnavailableError(RuntimeError):
    """The destination or promotion lock cannot be checked safely."""


@contextmanager
def target_lock(root: Path, locks_root: Path):
    """Exclude simultaneous promotions to one project across app processes."""

    descriptor: int | None = None
    try:
        boundary = WriteBoundary(locks_root)
        key = hashlib.sha256(str(root).encode("utf-8")).hexdigest()
        lock_path = boundary.authorize(f"promotion-{key}.lock")
        descriptor = os.open(
            lock_path,
            os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
        ):
            raise TargetUnavailableError("Promotion lock is not private.")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EWOULDBLOCK, errno.EAGAIN}:
                raise TargetConflictError("Another promotion is using this project.") from error
            raise
        yield
    except (OSError, WriteBoundaryError) as error:
        raise TargetUnavailableError("Promotion lock is unavailable.") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


async def require_clean_target(root: Path, base: str) -> None:
    try:
        inspector = ReadOnlyDiffService(root, base)
        current = await inspector.collect()
        status = await inspector.porcelain_status()
    except DiffServiceError as error:
        raise TargetConflictError("Target repository changed or cannot be inspected.") from error
    if current.files or status:
        raise TargetConflictError("Target repository contains uncommitted changes.")


async def assert_applied(
    root: Path,
    approved_states: dict[str, tuple[bool, bytes] | None],
    actual: UnifiedDiff,
) -> None:
    expected = set(approved_states)
    observed = {
        path
        for entry in actual.files
        for path in (entry.path, entry.previous_path)
        if path is not None
    }
    if (
        actual.truncated
        or expected != observed
        or any(entry.content_kind is not DiffContentKind.TEXT for entry in actual.files)
    ):
        raise TargetConflictError("Target differs from approved review.")
    try:
        target = WriteBoundary(root)
        for path in expected:
            if approved_states[path] != file_state(target, path):
                raise TargetConflictError("Target differs from approved review.")
        status = await ReadOnlyDiffService(root, actual.base_commit).porcelain_status()
    except (WriteBoundaryError, OSError, DiffServiceError) as error:
        raise TargetUnavailableError("Target cannot be verified.") from error
    if any(record and record[:2] != b"??" and record[:1] != b" " for record in status.split(b"\0")):
        raise TargetConflictError("Target index changed during promotion.")


def file_state(boundary: WriteBoundary, path: str) -> tuple[bool, bytes] | None:
    target = boundary.authorize(path)
    try:
        descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > 256 * 1024:
            raise TargetConflictError("Target contains an unsupported file.")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(data) != before.st_size or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise TargetConflictError("File changed during verification.")
        return bool(before.st_mode & stat.S_IXUSR), data
    finally:
        os.close(descriptor)


async def apply_patch(root: Path, patch: bytes, *, check: bool):
    arguments = ("apply", *(("--check",) if check else ()), "--whitespace=nowarn", "-")
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C",
        "LC_ALL": "C",
        "TMPDIR": "/tmp",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_LITERAL_PATHSPECS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
    }
    command = (
        "git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "submodule.recurse=false",
        *arguments,
    )
    try:
        return await run_process(command, cwd=root, env=environment, timeout=30, input_bytes=patch)
    except (ProcessRunnerError, ValueError) as error:
        if check:
            raise TargetUnavailableError("Patch preflight is unavailable.") from error
        raise
