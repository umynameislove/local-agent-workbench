from __future__ import annotations

import os
import stat
from pathlib import Path

from diff_types import (
    DiffChange,
    DiffConflictError,
    DiffContentKind,
    DiffEntry,
    DiffLimitError,
    DiffLimits,
    DiffPathState,
    DiffStatus,
    DiffUnavailableError,
    UnifiedDiff,
    decode_object_id,
    disabled_filter_arguments,
    is_utf8_text,
    valid_object_id,
)
from diff_types import DiffServiceError as DiffServiceError
from engine import ProcessRunnerError, ProcessTimeoutError, run_process
from write_boundary import WriteBoundary, WriteBoundaryError

_ZERO_OBJECT_IDS = frozenset({"0" * 40, "0" * 64})
_REGULAR_MODES = frozenset({"100644", "100755"})


class ReadOnlyDiffService:
    """Create a bounded review diff without changing Git or filesystem state."""

    def __init__(
        self,
        worktree: Path,
        base_commit: str,
        *,
        limits: DiffLimits | None = None,
    ) -> None:
        if not isinstance(worktree, Path):
            raise TypeError("Diff worktree must be a Path.")
        if not worktree.is_absolute():
            raise ValueError("Diff worktree must be absolute.")
        if not isinstance(base_commit, str) or not valid_object_id(base_commit):
            raise ValueError("Diff base commit is invalid.")
        if limits is None:
            limits = DiffLimits()
        if not isinstance(limits, DiffLimits):
            raise TypeError("Diff limits must use DiffLimits.")
        try:
            boundary = WriteBoundary(worktree)
        except (TypeError, ValueError, WriteBoundaryError) as error:
            raise DiffUnavailableError("Diff worktree is unavailable.") from error
        self._boundary = boundary
        self._worktree = boundary.root
        self._base_commit = base_commit.lower()
        self._limits = limits
        self._filter_arguments: tuple[str, ...] = ()

    async def collect(self) -> UnifiedDiff:
        """Return one stable inventory with bounded patches and explicit summaries."""

        await self._verify_repository()
        filter_config = await self._load_filter_arguments()
        before_status = await self._status()
        changes = await self._changes()
        if len(changes) > self._limits.max_files:
            raise DiffLimitError("Changed file count exceeds the review limit.")

        observed: dict[str, DiffPathState | None] = {}
        entries: list[DiffEntry] = []
        patch_bytes = 0
        truncated = False
        for change in changes:
            old_size = await self._old_size(change)
            new_state = self._path_state(change)
            observed[change.path] = new_state
            new_size = None if new_state is None else new_state.size
            self._require_regular_file(change, new_state)

            if self._is_large(old_size, new_size):
                entries.append(
                    DiffEntry.summarized(
                        change,
                        DiffContentKind.LARGE,
                        old_size,
                        new_size,
                        "Large file content omitted.",
                    )
                )
                continue

            if await self._is_binary(change, old_size, new_state):
                entries.append(
                    DiffEntry.summarized(
                        change,
                        DiffContentKind.BINARY,
                        old_size,
                        new_size,
                        "Binary content omitted.",
                    )
                )
                continue

            patch = await self._patch(change)
            encoded_size = len(patch.encode("utf-8"))
            if patch_bytes + encoded_size > self._limits.max_patch_bytes:
                truncated = True
                entries.append(
                    DiffEntry.summarized(
                        change,
                        DiffContentKind.OMITTED,
                        old_size,
                        new_size,
                        "Text patch omitted because the review limit was reached.",
                    )
                )
                continue
            patch_bytes += encoded_size
            entries.append(
                DiffEntry(
                    status=change.status,
                    path=change.path,
                    previous_path=change.previous_path,
                    content_kind=DiffContentKind.TEXT,
                    old_size=old_size,
                    new_size=new_size,
                    patch=patch,
                    summary=None,
                )
            )

        await self._verify_stable(before_status, changes, observed, filter_config)
        return UnifiedDiff(
            base_commit=self._base_commit,
            files=tuple(entries),
            patch_bytes=patch_bytes,
            truncated=truncated,
        )

    async def _verify_repository(self) -> None:
        try:
            self._boundary.verify()
        except WriteBoundaryError as error:
            raise DiffConflictError("Diff worktree identity changed.") from error
        top = await self._git(("rev-parse", "--show-toplevel"))
        head = await self._git(("rev-parse", "--verify", "HEAD^{commit}"))
        if top.returncode != 0 or head.returncode != 0:
            raise DiffUnavailableError("Git repository is unavailable.")
        try:
            reported = Path(top.stdout.decode("utf-8").rstrip("\r\n")).resolve(strict=True)
            current = decode_object_id(head.stdout)
            same_repository = self._worktree.samefile(reported)
        except (OSError, RuntimeError, UnicodeError, ValueError) as error:
            raise DiffConflictError("Git repository identity is invalid.") from error
        if not same_repository:
            raise DiffConflictError("Git repository identity is invalid.")
        if current != self._base_commit:
            raise DiffConflictError("Worktree HEAD no longer matches the job snapshot.")

    async def _status(self) -> bytes:
        result = await self._git(
            (
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--ignored=no",
            )
        )
        if result.returncode != 0:
            raise DiffUnavailableError("Worktree status is unavailable.")
        return result.stdout

    async def _changes(self) -> tuple[DiffChange, ...]:
        tracked = await self._git(
            (
                "diff",
                "--raw",
                "-z",
                "--find-renames=50%",
                "--no-ext-diff",
                "--no-textconv",
                self._base_commit,
                "--",
            )
        )
        untracked = await self._git(("ls-files", "--others", "--exclude-standard", "-z", "--"))
        if tracked.returncode != 0 or untracked.returncode != 0:
            raise DiffUnavailableError("Changed file inventory is unavailable.")
        changes = list(self._parse_raw_changes(tracked.stdout))
        changes.extend(self._parse_untracked(untracked.stdout))
        changes.sort(key=lambda change: (change.path, change.previous_path or ""))
        if len({change.path for change in changes}) != len(changes):
            raise DiffConflictError("Changed file inventory is ambiguous.")
        return tuple(changes)

    def _parse_raw_changes(self, payload: bytes) -> tuple[DiffChange, ...]:
        fields = payload.split(b"\0")
        changes: list[DiffChange] = []
        index = 0
        while index < len(fields) and fields[index]:
            try:
                header = fields[index].decode("ascii")
                parts = header.split()
                if len(parts) != 5 or not parts[0].startswith(":"):
                    raise ValueError
                old_mode = parts[0][1:]
                new_mode, old_object, _new_object, status_token = parts[1:]
                status_code = status_token[0]
                first_path = self._decode_path(fields[index + 1])
            except (IndexError, UnicodeError, ValueError) as error:
                raise DiffConflictError(
                    "Git returned an invalid changed file inventory."
                ) from error
            index += 2

            previous_path: str | None = None
            path = first_path
            if status_code in {"R", "C"}:
                try:
                    path = self._decode_path(fields[index])
                except (IndexError, UnicodeError, ValueError) as error:
                    raise DiffConflictError(
                        "Git returned an invalid changed file inventory."
                    ) from error
                previous_path = first_path
                index += 1
            status = DiffStatus.from_git(status_code)
            changes.append(
                DiffChange(
                    status=status,
                    path=path,
                    previous_path=previous_path,
                    old_mode=old_mode,
                    new_mode=new_mode,
                    old_object=None if old_object in _ZERO_OBJECT_IDS else old_object,
                    tracked=True,
                )
            )
        if any(fields[index:]):
            raise DiffConflictError("Git returned an invalid changed file inventory.")
        return tuple(changes)

    def _parse_untracked(self, payload: bytes) -> tuple[DiffChange, ...]:
        changes: list[DiffChange] = []
        for field in payload.split(b"\0"):
            if not field:
                continue
            path = self._decode_path(field)
            changes.append(
                DiffChange(
                    status=DiffStatus.ADDED,
                    path=path,
                    previous_path=None,
                    old_mode="000000",
                    new_mode="000000",
                    old_object=None,
                    tracked=False,
                )
            )
        return tuple(changes)

    def _decode_path(self, value: bytes) -> str:
        try:
            path = value.decode("utf-8")
            self._boundary.authorize(path)
        except (UnicodeError, TypeError, ValueError, WriteBoundaryError) as error:
            raise DiffConflictError("Changed file path violates the worktree boundary.") from error
        return path

    async def _old_size(self, change: DiffChange) -> int | None:
        if change.old_object is None or change.old_mode == "000000":
            return None
        if change.old_mode not in _REGULAR_MODES:
            raise DiffConflictError("Worktree contains an unsupported file type.")
        result = await self._git(("cat-file", "-s", change.old_object))
        if result.returncode != 0:
            raise DiffUnavailableError("Base file metadata is unavailable.")
        try:
            size = int(result.stdout.decode("ascii").strip())
        except (UnicodeError, ValueError) as error:
            raise DiffConflictError("Git returned invalid base file metadata.") from error
        if size < 0:
            raise DiffConflictError("Git returned invalid base file metadata.")
        return size

    def _path_state(self, change: DiffChange) -> DiffPathState | None:
        if change.status is DiffStatus.DELETED:
            try:
                self._worktree.joinpath(change.path).lstat()
            except FileNotFoundError:
                return None
            except OSError as error:
                raise DiffUnavailableError("Changed file cannot be inspected.") from error
            raise DiffConflictError("Deleted file inventory changed during inspection.")
        try:
            path = self._boundary.authorize(change.path)
            metadata = path.lstat()
        except (OSError, WriteBoundaryError) as error:
            raise DiffConflictError("Changed file violates the worktree boundary.") from error
        return DiffPathState(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            mode=metadata.st_mode,
            size=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
            links=metadata.st_nlink,
        )

    @staticmethod
    def _require_regular_file(change: DiffChange, state: DiffPathState | None) -> None:
        if change.old_mode not in _REGULAR_MODES | {"000000"}:
            raise DiffConflictError("Worktree contains an unsupported file type.")
        if change.tracked and change.new_mode not in _REGULAR_MODES | {"000000"}:
            raise DiffConflictError("Worktree contains an unsupported file type.")
        if state is not None and (not stat.S_ISREG(state.mode) or state.links != 1):
            raise DiffConflictError("Worktree contains an unsupported file type.")

    def _is_large(self, old_size: int | None, new_size: int | None) -> bool:
        return any(
            size is not None and size > self._limits.max_text_file_bytes
            for size in (old_size, new_size)
        )

    async def _is_binary(
        self,
        change: DiffChange,
        old_size: int | None,
        new_state: DiffPathState | None,
    ) -> bool:
        if old_size is not None:
            old = await self._read_blob(change.old_object, old_size)
            if not is_utf8_text(old):
                return True
        if new_state is not None:
            new = self._read_worktree(change.path, new_state)
            if not is_utf8_text(new):
                return True
        if change.tracked:
            paths = (change.path,)
            if change.previous_path is not None:
                paths = (change.previous_path, change.path)
            result = await self._git(
                (
                    "diff",
                    "--numstat",
                    "--no-ext-diff",
                    "--no-textconv",
                    self._base_commit,
                    "--",
                    *paths,
                )
            )
            if result.returncode != 0:
                raise DiffUnavailableError("File classification is unavailable.")
            if any(line.startswith(b"-\t-\t") for line in result.stdout.splitlines()):
                return True
        return False

    async def _read_blob(self, object_id: str | None, expected_size: int) -> bytes:
        if object_id is None:
            raise DiffConflictError("Base file identity is unavailable.")
        result = await self._git(("cat-file", "blob", object_id))
        if result.returncode != 0 or len(result.stdout) != expected_size:
            raise DiffConflictError("Base file changed during inspection.")
        return result.stdout

    def _read_worktree(self, path: str, expected: DiffPathState) -> bytes:
        try:
            target = self._boundary.authorize(path)
            descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except (OSError, WriteBoundaryError) as error:
            raise DiffConflictError("Changed file cannot be read safely.") from error
        try:
            metadata = os.fstat(descriptor)
            observed = DiffPathState(
                device=metadata.st_dev,
                inode=metadata.st_ino,
                mode=metadata.st_mode,
                size=metadata.st_size,
                modified_ns=metadata.st_mtime_ns,
                links=metadata.st_nlink,
            )
            if observed != expected or not stat.S_ISREG(metadata.st_mode):
                raise DiffConflictError("Changed file changed during inspection.")
            chunks: list[bytes] = []
            remaining = self._limits.max_text_file_bytes + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
        finally:
            os.close(descriptor)
        if len(data) != expected.size:
            raise DiffConflictError("Changed file changed during inspection.")
        return data

    async def _patch(self, change: DiffChange) -> str:
        common = (
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            f"--unified={self._limits.context_lines}",
            "--src-prefix=a/",
            "--dst-prefix=b/",
        )
        if change.tracked:
            paths = (change.path,)
            if change.previous_path is not None:
                paths = (change.previous_path, change.path)
            result = await self._git(
                ("diff", *common, "--find-renames=50%", self._base_commit, "--", *paths)
            )
            accepted_codes = {0}
        else:
            result = await self._git(
                ("diff", "--no-index", *common, "--", "/dev/null", change.path)
            )
            accepted_codes = {0, 1}
        if result.returncode not in accepted_codes or result.stderr:
            raise DiffUnavailableError("Unified diff could not be generated.")
        try:
            patch = result.stdout.decode("utf-8")
        except UnicodeDecodeError as error:
            raise DiffConflictError("Unified diff contains unsupported content.") from error
        if "\x00" in patch:
            raise DiffConflictError("Unified diff contains unsupported content.")
        return patch

    async def _verify_stable(
        self,
        before_status: bytes,
        changes: tuple[DiffChange, ...],
        observed: dict[str, DiffPathState | None],
        filter_config: bytes,
    ) -> None:
        if await self._load_filter_arguments() != filter_config:
            raise DiffConflictError("Git configuration changed during diff generation.")
        await self._verify_repository()
        after_status = await self._status()
        after_changes = await self._changes()
        if before_status != after_status or changes != after_changes:
            raise DiffConflictError("Worktree changed during diff generation.")
        for path, expected in observed.items():
            change = next(item for item in changes if item.path == path)
            if self._path_state(change) != expected:
                raise DiffConflictError("Worktree changed during diff generation.")

    async def _load_filter_arguments(self) -> bytes:
        result = await self._git(("config", "--local", "--includes", "--name-only", "-z", "--list"))
        if result.returncode != 0 or result.stderr:
            raise DiffUnavailableError("Git filter configuration is unavailable.")
        try:
            self._filter_arguments = disabled_filter_arguments(result.stdout)
        except (UnicodeError, ValueError) as error:
            raise DiffConflictError("Git filter configuration is invalid.") from error
        return result.stdout

    async def _git(self, arguments: tuple[str, ...]):
        command = (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "diff.external=",
            "-c",
            "diff.trustExitCode=false",
            "-c",
            "submodule.recurse=false",
            *self._filter_arguments,
            *arguments,
        )
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
        try:
            return await run_process(command, cwd=self._worktree, env=environment, timeout=20)
        except (ProcessRunnerError, ProcessTimeoutError, ValueError) as error:
            raise DiffUnavailableError("Git inspection is unavailable.") from error
