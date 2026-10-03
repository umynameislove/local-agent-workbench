from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import string
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

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
from engine import (
    EventCreate,
    JobState,
    JobUpdate,
    PermissionMode,
    ProcessResult,
    ProcessRunnerError,
    run_process,
)
from write_boundary import WriteBoundary, WriteBoundaryError

_WORKTREE_EVENT_TYPE = "job.worktree.created"
_WORKTREE_IDEMPOTENCY_KEY = "worktree-created"


class WorktreeError(RuntimeError):
    """Base error for sanitized worktree failures."""


class WorktreeNotFoundError(WorktreeError):
    """Raised when the requested job does not exist."""


class WorktreeConflictError(WorktreeError):
    """Raised when a safe worktree cannot be created for the job."""


class WorktreeUnavailableError(WorktreeError):
    """Raised when Git or durable storage is unavailable."""


def worktree_identity(path: Path) -> dict[str, int | str] | None:
    """Capture linked worktree identity without retaining a private path."""
    try:
        boundary = WriteBoundary(path)
        root = boundary.root.stat()
        marker = boundary.root / ".git"
        metadata = marker.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            return None
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise WorktreeConflictError("Linked worktree metadata is invalid.")

        def stable(value: os.stat_result) -> tuple[int, ...]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_nlink,
                value.st_size,
                value.st_mtime_ns,
            )

        descriptor = os.open(marker, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            if stable(os.fstat(descriptor)) != stable(metadata):
                raise WorktreeConflictError("Linked worktree metadata changed.")
            content = os.read(descriptor, 16_385)
        finally:
            os.close(descriptor)
        if len(content) > 16_384 or stable(metadata) != stable(marker.lstat()):
            raise WorktreeConflictError("Linked worktree metadata changed.")
        boundary.verify()
        return {
            "device": root.st_dev,
            "inode": root.st_ino,
            "git_device": metadata.st_dev,
            "git_inode": metadata.st_ino,
            "git_digest": hashlib.sha256(content).hexdigest(),
        }
    except (OSError, ValueError, WriteBoundaryError) as error:
        raise WorktreeUnavailableError("Worktree identity cannot be captured.") from error


@dataclass(frozen=True)
class WorktreeBlock:
    job_id: str
    state: JobState
    event_id: int
    branch: str
    base_commit: str
    worktree_key: str

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "state": self.state.value,
            "event_id": self.event_id,
            "worktree": {
                "branch": self.branch,
                "base_commit": self.base_commit,
                "key": self.worktree_key,
            },
        }


@dataclass(frozen=True)
class _WorktreeContract:
    project_root: Path
    base_commit: str
    branch: str
    key: str

    @property
    def payload(self) -> dict[str, str]:
        return {
            "branch": self.branch,
            "base_commit": self.base_commit,
            "worktree_key": self.key,
        }


class WorktreeManager:
    """Create one deterministic linked worktree from immutable job state."""

    def __init__(
        self,
        jobs: JobRepository,
        events: EventRepository,
        transitions: AtomicTransitionService,
        worktrees_root: Path,
    ) -> None:
        if not isinstance(jobs, JobRepository) or not isinstance(events, EventRepository):
            raise TypeError("Worktree management requires job and event repositories.")
        if not isinstance(transitions, AtomicTransitionService):
            raise TypeError("Worktree management requires atomic job persistence.")
        if not isinstance(worktrees_root, Path) or not worktrees_root.is_absolute():
            raise TypeError("Worktree storage must be an absolute Path.")
        self._jobs = jobs
        self._events = events
        self._transitions = transitions
        self._worktrees_root = worktrees_root
        self._locks: dict[str, asyncio.Lock] = {}

    async def create(self, job_id: str) -> WorktreeBlock:
        if (
            not isinstance(job_id, str)
            or not job_id
            or job_id != job_id.strip()
            or "\x00" in job_id
        ):
            raise WorktreeNotFoundError("Job does not exist.")
        lock = self._locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            return await self._create_locked(job_id)

    async def verify_bound(self, job: JobRecord) -> Path:
        """Verify the job still owns its deterministic linked worktree."""

        contract = self._contract(job)
        storage = self._resolve_directory(
            self._worktrees_root, unavailable="Worktree storage is unavailable."
        )
        project = self._resolve_directory(
            contract.project_root, conflict="Project repository is unavailable."
        )
        target = storage / contract.key
        if self._overlaps(project, storage) or job.worktree_path != str(target):
            raise WorktreeConflictError("Job worktree binding is invalid.")
        await self._verify_repository(project, contract.base_commit)
        return await self._verify_worktree(project, target, contract)

    async def verify_removable(self, job: JobRecord, identity: Mapping[str, object]) -> Path:
        """Require the original linked artifact and no unreviewed disposal targets."""
        target = await self.verify_bound(job)
        if worktree_identity(target) != identity:
            raise WorktreeConflictError("Retained worktree identity changed.")
        contract = self._contract(job)
        created = tuple(
            event for event in self._events.list(job.id) if event.event_type == _WORKTREE_EVENT_TYPE
        )
        if len(created) != 1 or created[0].payload != contract.payload:
            raise WorktreeConflictError("Worktree creation evidence is invalid.")
        records = await self._registrations(contract.project_root)
        matching = [record for record in records if record.get("worktree") == os.fsencode(target)]
        if matching != [
            {
                "worktree": os.fsencode(target),
                "HEAD": contract.base_commit.encode("ascii"),
                "branch": f"refs/heads/{contract.branch}".encode("ascii"),
            }
        ]:
            raise WorktreeConflictError("Retained worktree registration changed or is locked.")
        ignored = await self._git(
            ("ls-files", "--others", "--ignored", "--exclude-standard", "-z"), cwd=target
        )
        index = await self._git(("ls-files", "--stage", "-z"), cwd=target)
        if ignored.returncode != 0 or index.returncode != 0:
            raise WorktreeUnavailableError("Retained worktree inventory is unavailable.")
        if ignored.stdout or any(
            record.startswith(b"160000 ") for record in index.stdout.split(b"\0")
        ):
            raise WorktreeConflictError("Worktree contains ignored files or nested repositories.")
        self._disposal_snapshot(target)
        if worktree_identity(target) != identity:
            raise WorktreeConflictError("Retained worktree changed during inspection.")
        return target

    @staticmethod
    def _disposal_snapshot(target: Path) -> dict[str, tuple[int, ...]]:
        """Inspect every disposal entry and detect changes across the final review."""

        def failed_walk(error: OSError) -> None:
            raise WorktreeUnavailableError("Retained worktree cannot be inspected.") from error

        snapshot: dict[str, tuple[int, ...]] = {}
        try:
            device = target.stat().st_dev
            for parent, directories, files in os.walk(
                target, onerror=failed_walk, followlinks=False
            ):
                for name in (*directories, *files):
                    entry = Path(parent) / name
                    if entry == target / ".git":
                        continue
                    metadata = entry.lstat()
                    if (
                        name == ".git"
                        or metadata.st_dev != device
                        or not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode))
                        or (stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1)
                    ):
                        raise WorktreeConflictError("Worktree contains unsafe disposal targets.")
                    snapshot[str(entry.relative_to(target))] = (
                        metadata.st_dev,
                        metadata.st_ino,
                        metadata.st_mode,
                        metadata.st_nlink,
                        metadata.st_size,
                        metadata.st_mtime_ns,
                        metadata.st_ctime_ns,
                    )
        except OSError as error:
            raise WorktreeUnavailableError("Retained worktree cannot be inspected.") from error
        return snapshot

    async def remove_retained(
        self,
        job: JobRecord,
        identity: Mapping[str, object],
        *,
        before_remove: Callable[[], Awaitable[None]],
    ) -> None:
        """Remove only a verified retained tree, never its branch or other metadata."""
        target = await self.verify_removable(job, identity)
        contract = self._contract(job)
        snapshot = self._disposal_snapshot(target)
        await before_remove()
        if worktree_identity(target) != identity or self._disposal_snapshot(target) != snapshot:
            raise WorktreeConflictError("Retained worktree changed before removal.")
        result = await self._git(
            ("worktree", "remove", "--force", "--", str(target)),
            cwd=contract.project_root,
            timeout=60,
        )
        if result.returncode != 0:
            raise WorktreeUnavailableError("Retained worktree removal did not complete.")
        try:
            target.lstat()
        except FileNotFoundError:
            pass
        except OSError as error:
            raise WorktreeUnavailableError("Worktree removal cannot be verified.") from error
        else:
            raise WorktreeConflictError("Worktree removal is incomplete.")
        records = await self._registrations(contract.project_root)
        branch = await self._git(
            ("rev-parse", "--verify", f"refs/heads/{contract.branch}"), cwd=contract.project_root
        )
        if (
            any(record.get("worktree") == os.fsencode(target) for record in records)
            or branch.returncode != 0
            or self._decode_commit(branch.stdout) != contract.base_commit
        ):
            raise WorktreeConflictError("Worktree removal outcome requires attention.")

    async def _registrations(self, project: Path) -> tuple[dict[str, bytes], ...]:
        result = await self._git(("worktree", "list", "--porcelain", "-z"), cwd=project)
        if result.returncode != 0:
            raise WorktreeUnavailableError("Worktree registration is unavailable.")
        records: list[dict[str, bytes]] = []
        record: dict[str, bytes] = {}
        try:
            for field in result.stdout.split(b"\0"):
                if not field:
                    if record:
                        records.append(record)
                        record = {}
                    continue
                key, _, value = field.partition(b" ")
                name = key.decode("ascii")
                if name in record:
                    raise ValueError
                record[name] = value
            if record:
                records.append(record)
        except (UnicodeError, ValueError) as error:
            raise WorktreeConflictError("Worktree registration is invalid.") from error
        return tuple(records)

    async def _create_locked(self, job_id: str) -> WorktreeBlock:
        job = self._load_job(job_id)
        contract = self._contract(job)
        storage = self._resolve_directory(
            self._worktrees_root,
            unavailable="Worktree storage is unavailable.",
        )
        project = self._resolve_directory(
            contract.project_root,
            conflict="Project repository is unavailable.",
        )
        if self._overlaps(project, storage):
            raise WorktreeConflictError("Project and worktree storage must be separate.")
        target = storage / contract.key

        if job.worktree_path is not None:
            return await self._read_existing(job, contract, project, target)
        if job.state is not JobState.QUEUED:
            raise WorktreeConflictError("Job must finish planning before worktree creation.")

        await self._verify_repository(project, contract.base_commit)
        if await self._artifact_exists(project, target, contract.branch):
            return await self._recover_existing(job, contract, project, target)
        head_before, status_before = await self._repository_state(project)
        created = await self._git(
            (
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "filter.lfs.process=",
                "-c",
                "filter.lfs.smudge=",
                "-c",
                "filter.lfs.required=false",
                "-c",
                "submodule.recurse=false",
                "worktree",
                "add",
                "--no-track",
                "-b",
                contract.branch,
                str(target),
                contract.base_commit,
            ),
            cwd=project,
            timeout=60,
        )
        if created.returncode != 0:
            raise WorktreeUnavailableError("Git could not create the worktree.")

        try:
            resolved_target = await self._verify_worktree(project, target, contract)
            head_after, status_after = await self._repository_state(project)
            if head_after != head_before or status_after != status_before:
                raise WorktreeConflictError(
                    "Project working tree changed during worktree creation."
                )
            return self._bind(job, contract, resolved_target)
        except (WorktreeConflictError, WorktreeUnavailableError):
            if not await self._rollback(project, target, contract.branch):
                raise WorktreeUnavailableError("Worktree state requires attention.") from None
            raise

    def _load_job(self, job_id: str) -> JobRecord:
        try:
            return self._jobs.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise WorktreeNotFoundError("Job does not exist.") from error
        except JobRepositoryError as error:
            raise WorktreeUnavailableError("Worktree state is unavailable.") from error

    @staticmethod
    def _contract(job: JobRecord) -> _WorktreeContract:
        snapshot = job.request_snapshot
        project = snapshot.get("project")
        head = snapshot.get("repo_head")
        if not isinstance(project, Mapping):
            raise WorktreeConflictError("Job repository snapshot is invalid.")
        if project.get("id") != job.project_id:
            raise WorktreeConflictError("Job repository snapshot is invalid.")
        if project.get("permission_mode") != PermissionMode.SANDBOXED_WRITE.value:
            raise WorktreeConflictError("Project policy does not allow worktree creation.")
        root = project.get("root")
        if not isinstance(root, str) or not root or "\x00" in root or not Path(root).is_absolute():
            raise WorktreeConflictError("Job repository snapshot is invalid.")
        if (
            not isinstance(head, str)
            or len(head) not in {40, 64}
            or any(character not in string.hexdigits for character in head)
        ):
            raise WorktreeConflictError("Job repository snapshot is invalid.")
        identity = hashlib.sha256(job.id.encode("utf-8")).hexdigest()[:32]
        return _WorktreeContract(
            project_root=Path(root),
            base_commit=head.lower(),
            branch=f"law/job-{identity}",
            key=f"job-{identity}",
        )

    @staticmethod
    def _resolve_directory(
        path: Path,
        *,
        conflict: str | None = None,
        unavailable: str | None = None,
    ) -> Path:
        message = conflict or unavailable or "Directory is unavailable."
        error_type = WorktreeConflictError if conflict is not None else WorktreeUnavailableError
        try:
            metadata = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise error_type(message)
            return path.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise error_type(message) from error

    @staticmethod
    def _overlaps(project: Path, storage: Path) -> bool:
        return (
            project == storage or project.is_relative_to(storage) or storage.is_relative_to(project)
        )

    async def _verify_repository(self, project: Path, base_commit: str) -> None:
        top = await self._git(("rev-parse", "--show-toplevel"), cwd=project)
        if top.returncode != 0:
            raise WorktreeConflictError("Project repository is unavailable.")
        try:
            reported = Path(top.stdout.decode("utf-8").rstrip("\r\n")).resolve(strict=True)
            if not project.samefile(reported):
                raise WorktreeConflictError("Project repository identity changed.")
        except (OSError, RuntimeError, UnicodeError, ValueError) as error:
            raise WorktreeConflictError("Project repository identity changed.") from error
        commit = await self._git(
            ("rev-parse", "--verify", f"{base_commit}^{{commit}}"),
            cwd=project,
        )
        if commit.returncode != 0 or self._decode_commit(commit.stdout) != base_commit:
            raise WorktreeConflictError("Job base commit is unavailable.")

    async def _artifact_exists(
        self,
        project: Path,
        target: Path,
        branch: str,
    ) -> bool:
        try:
            target.lstat()
        except FileNotFoundError:
            target_exists = False
        except OSError as error:
            raise WorktreeUnavailableError("Worktree target cannot be inspected.") from error
        else:
            target_exists = True
        branch_result = await self._git(
            ("show-ref", "--verify", "--quiet", f"refs/heads/{branch}"),
            cwd=project,
        )
        if branch_result.returncode not in {0, 1}:
            raise WorktreeUnavailableError("Worktree branch cannot be inspected.")
        branch_exists = branch_result.returncode == 0
        if target_exists != branch_exists:
            raise WorktreeConflictError("Worktree state requires attention.")
        return target_exists

    async def _recover_existing(
        self,
        job: JobRecord,
        contract: _WorktreeContract,
        project: Path,
        target: Path,
    ) -> WorktreeBlock:
        resolved = await self._verify_worktree(project, target, contract)
        status = await self._git(
            (
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--ignored=matching",
            ),
            cwd=resolved,
        )
        if status.returncode != 0:
            raise WorktreeUnavailableError("Existing worktree cannot be inspected.")
        if status.stdout:
            raise WorktreeConflictError("Existing worktree contains uncommitted changes.")
        return self._bind(job, contract, resolved)

    async def _repository_state(self, project: Path) -> tuple[str, bytes]:
        head = await self._git(("rev-parse", "--verify", "HEAD^{commit}"), cwd=project)
        status = await self._git(
            (
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "status",
                "--porcelain=v1",
                "--untracked-files=no",
            ),
            cwd=project,
        )
        if head.returncode != 0 or status.returncode != 0:
            raise WorktreeUnavailableError("Project working tree cannot be inspected.")
        return self._decode_commit(head.stdout), status.stdout

    async def _verify_worktree(
        self,
        project: Path,
        target: Path,
        contract: _WorktreeContract,
    ) -> Path:
        resolved = self._resolve_directory(
            target,
            conflict="Worktree artifact is invalid.",
        )
        top = await self._git(("rev-parse", "--show-toplevel"), cwd=resolved)
        head = await self._git(("rev-parse", "--verify", "HEAD^{commit}"), cwd=resolved)
        branch = await self._git(("symbolic-ref", "--short", "HEAD"), cwd=resolved)
        source_common = await self._common_git_directory(project)
        target_common = await self._common_git_directory(resolved)
        try:
            reported = Path(top.stdout.decode("utf-8").rstrip("\r\n")).resolve(strict=True)
            branch_name = branch.stdout.decode("utf-8").rstrip("\r\n")
            valid = (
                top.returncode == 0
                and head.returncode == 0
                and branch.returncode == 0
                and resolved.samefile(reported)
                and source_common.samefile(target_common)
                and self._decode_commit(head.stdout) == contract.base_commit
                and branch_name == contract.branch
            )
        except (OSError, RuntimeError, UnicodeError, ValueError):
            valid = False
        if not valid:
            raise WorktreeConflictError("Worktree artifact failed verification.")
        return resolved

    async def _common_git_directory(self, cwd: Path) -> Path:
        result = await self._git(("rev-parse", "--git-common-dir"), cwd=cwd)
        if result.returncode != 0:
            raise WorktreeConflictError("Git worktree identity is invalid.")
        try:
            value = Path(result.stdout.decode("utf-8").rstrip("\r\n"))
            return (value if value.is_absolute() else cwd / value).resolve(strict=True)
        except (OSError, RuntimeError, UnicodeError, ValueError) as error:
            raise WorktreeConflictError("Git worktree identity is invalid.") from error

    def _bind(
        self,
        job: JobRecord,
        contract: _WorktreeContract,
        target: Path,
    ) -> WorktreeBlock:
        update = JobUpdate(
            id=job.id,
            state=job.state,
            runtime=job.runtime,
            model=job.model,
            worktree_path=str(target),
        )
        event = EventCreate(
            job_id=job.id,
            event_type=_WORKTREE_EVENT_TYPE,
            payload=contract.payload,
            idempotency_key=_WORKTREE_IDEMPOTENCY_KEY,
        )
        try:
            result = self._transitions.bind_worktree(update, event)
        except AtomicTransitionConflictError as error:
            raise WorktreeConflictError("Worktree binding conflicts with job state.") from error
        except AtomicTransitionError as error:
            raise WorktreeUnavailableError("Worktree binding could not be recorded.") from error
        return self._block(result.job, result.event, contract)

    async def _read_existing(
        self,
        job: JobRecord,
        contract: _WorktreeContract,
        project: Path,
        target: Path,
    ) -> WorktreeBlock:
        try:
            stored = Path(job.worktree_path or "").resolve(strict=True)
            if not target.resolve(strict=True).samefile(stored):
                raise WorktreeConflictError("Recorded worktree does not match this job.")
        except (OSError, RuntimeError, ValueError) as error:
            raise WorktreeConflictError("Recorded worktree is unavailable.") from error
        await self._verify_worktree(project, stored, contract)
        try:
            records = tuple(
                event
                for event in self._events.list(job.id)
                if event.event_type == _WORKTREE_EVENT_TYPE
            )
        except EventRepositoryError as error:
            raise WorktreeUnavailableError("Worktree event is unavailable.") from error
        if len(records) != 1 or records[0].payload != contract.payload:
            raise WorktreeConflictError("Recorded worktree event is invalid.")
        return self._block(job, records[0], contract)

    @staticmethod
    def _block(job: JobRecord, event: EventRecord, contract: _WorktreeContract) -> WorktreeBlock:
        return WorktreeBlock(
            job_id=job.id,
            state=job.state,
            event_id=event.id,
            branch=contract.branch,
            base_commit=contract.base_commit,
            worktree_key=contract.key,
        )

    async def _rollback(self, project: Path, target: Path, branch: str) -> bool:
        removed = await self._git(
            (
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "worktree",
                "remove",
                "--force",
                str(target),
            ),
            cwd=project,
            timeout=60,
        )
        if removed.returncode != 0:
            return False
        deleted = await self._git(
            ("-c", "core.hooksPath=/dev/null", "branch", "-D", branch),
            cwd=project,
        )
        return deleted.returncode == 0

    @staticmethod
    def _decode_commit(value: bytes) -> str:
        try:
            commit = value.decode("ascii").strip().lower()
        except UnicodeDecodeError as error:
            raise WorktreeConflictError("Git returned an invalid commit.") from error
        if len(commit) not in {40, 64} or any(
            character not in string.hexdigits for character in commit
        ):
            raise WorktreeConflictError("Git returned an invalid commit.")
        return commit

    @staticmethod
    async def _git(
        arguments: tuple[str, ...],
        *,
        cwd: Path,
        timeout: float = 20,
    ) -> ProcessResult:
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C",
            "LC_ALL": "C",
            "TMPDIR": "/tmp",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_ATTR_NOSYSTEM": "1",
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
            return await run_process(command, cwd=cwd, timeout=timeout, env=environment)
        except (ProcessRunnerError, ValueError) as error:
            raise WorktreeUnavailableError("Git operation is unavailable.") from error
