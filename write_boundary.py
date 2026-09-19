from __future__ import annotations

import os
import stat
from pathlib import Path, PurePosixPath

_PROTECTED_DIRECTORIES = frozenset({".agents", ".codex", ".git"})


class WriteBoundaryError(RuntimeError):
    """Raised when a filesystem write cannot be authorized safely."""


class WriteBoundary:
    """Authorize canonical write targets below one immutable worktree root."""

    def __init__(
        self,
        worktree: Path,
        allowed_paths: tuple[str, ...] = (".",),
    ) -> None:
        if not isinstance(worktree, Path):
            raise TypeError("Write boundary worktree must be a Path.")
        if not worktree.is_absolute():
            raise ValueError("Write boundary worktree must be absolute.")
        if not isinstance(allowed_paths, tuple) or not allowed_paths:
            raise TypeError("Allowed write paths must be a nonempty tuple.")
        if any(not isinstance(path, str) for path in allowed_paths):
            raise TypeError("Allowed write paths must contain strings.")

        root, metadata = self._load_root(worktree)
        scopes = tuple(self._relative_parts(path, allow_root=True) for path in allowed_paths)
        if len(set(scopes)) != len(scopes):
            raise ValueError("Allowed write paths must be unique.")

        self._root = root
        self._root_identity = (metadata.st_dev, metadata.st_ino)
        self._device = metadata.st_dev
        self._scopes = scopes
        self._writable_root_identities: tuple[tuple[int, int], ...] | None = None

    @property
    def root(self) -> Path:
        return self._root

    @property
    def allowed_paths(self) -> tuple[str, ...]:
        return tuple(
            "." if not scope else PurePosixPath(*scope).as_posix() for scope in self._scopes
        )

    def verify(self) -> None:
        """Fail when the trusted worktree root has disappeared or changed identity."""

        try:
            metadata = self._root.lstat()
        except OSError as error:
            raise WriteBoundaryError("Write boundary root is unavailable.") from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_dev, metadata.st_ino) != self._root_identity
        ):
            raise WriteBoundaryError("Write boundary root identity changed.")

    def authorize(self, candidate: str | Path) -> Path:
        """Return one worktree path only after every boundary check succeeds."""

        target, _ = self._authorize(candidate)
        return target

    def relative_path(self, candidate: str | Path) -> str:
        """Return a safe provider facing path without exposing the worktree root."""

        _, parts = self._authorize(candidate)
        return PurePosixPath(*parts).as_posix()

    def writable_roots(self) -> tuple[Path, ...]:
        """Return verified existing directories suitable for provider sandbox roots."""

        self.verify()
        roots: list[Path] = []
        identities: list[tuple[int, int]] = []
        for scope in self._scopes:
            if not scope:
                roots.append(self._root)
                identities.append(self._root_identity)
                continue
            relative = PurePosixPath(*scope).as_posix()
            target = self.authorize(relative)
            try:
                metadata = target.lstat()
            except OSError as error:
                raise WriteBoundaryError("Allowed write root is unavailable.") from error
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise WriteBoundaryError("Allowed write root must be a directory.")
            roots.append(target)
            identities.append((metadata.st_dev, metadata.st_ino))
        observed = tuple(identities)
        if self._writable_root_identities is None:
            self._writable_root_identities = observed
        elif self._writable_root_identities != observed:
            raise WriteBoundaryError("Allowed write root identity changed.")
        return tuple(roots)

    def _authorize(self, candidate: str | Path) -> tuple[Path, tuple[str, ...]]:
        self.verify()
        parts = self._candidate_parts(candidate)
        if not any(self._within(parts, scope) for scope in self._scopes):
            raise WriteBoundaryError("Write path is outside the allowed scope.")

        target = self._root.joinpath(*parts)
        current = self._root
        for index, part in enumerate(parts):
            current = current / part
            try:
                metadata = current.lstat()
            except FileNotFoundError:
                break
            except OSError as error:
                raise WriteBoundaryError("Write path cannot be inspected safely.") from error

            if stat.S_ISLNK(metadata.st_mode):
                raise WriteBoundaryError("Write path aliases are not allowed.")
            if metadata.st_dev != self._device:
                raise WriteBoundaryError("Write path crosses a filesystem boundary.")
            if index < len(parts) - 1 and not stat.S_ISDIR(metadata.st_mode):
                raise WriteBoundaryError("Write path parent is not a directory.")
            if index == len(parts) - 1:
                if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
                    raise WriteBoundaryError("Write path type is not allowed.")
                if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1:
                    raise WriteBoundaryError("Write path aliases are not allowed.")
        return target, parts

    def _candidate_parts(self, candidate: str | Path) -> tuple[str, ...]:
        if not isinstance(candidate, (str, Path)):
            raise TypeError("Write path must be a string or Path.")
        text = candidate.as_posix() if isinstance(candidate, Path) else candidate
        if Path(text).is_absolute():
            try:
                text = Path(text).relative_to(self._root).as_posix()
            except ValueError as error:
                raise WriteBoundaryError("Write path is outside the worktree.") from error
        return self._relative_parts(text, allow_root=False)

    @staticmethod
    def _within(candidate: tuple[str, ...], scope: tuple[str, ...]) -> bool:
        return not scope or candidate[: len(scope)] == scope

    @staticmethod
    def _relative_parts(value: str, *, allow_root: bool) -> tuple[str, ...]:
        if not value or any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise WriteBoundaryError("Write path is invalid.")
        if "\\" in value or value.startswith("/"):
            raise WriteBoundaryError("Write path must be worktree relative.")
        if len(value) >= 2 and value[0].isalpha() and value[1] == ":":
            raise WriteBoundaryError("Write path must be worktree relative.")
        if value == ".":
            if allow_root:
                return ()
            raise WriteBoundaryError("Write path must identify a target.")

        parts = tuple(value.split("/"))
        if any(part in {"", ".", ".."} for part in parts):
            raise WriteBoundaryError("Write path traversal is not allowed.")
        if any(part.casefold() in _PROTECTED_DIRECTORIES for part in parts):
            raise WriteBoundaryError("Control metadata is outside the write boundary.")
        return parts

    @staticmethod
    def _load_root(worktree: Path) -> tuple[Path, os.stat_result]:
        try:
            metadata = worktree.lstat()
            resolved = worktree.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise WriteBoundaryError("Write boundary root is unavailable.") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise WriteBoundaryError("Write boundary root is unavailable.")
        if resolved != worktree:
            raise WriteBoundaryError("Write boundary root must be canonical.")
        return resolved, metadata
