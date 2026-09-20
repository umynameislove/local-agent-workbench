from __future__ import annotations

import string
from dataclasses import dataclass
from enum import StrEnum

_MAX_FILES = 1_000
_MAX_TEXT_FILE_BYTES = 4 * 1024 * 1024
_MAX_PATCH_BYTES = 8 * 1024 * 1024


class DiffServiceError(RuntimeError):
    """Base error for sanitized read only diff failures."""


class DiffConflictError(DiffServiceError):
    """Raised when repository state cannot produce a trustworthy diff."""


class DiffLimitError(DiffServiceError):
    """Raised when the changed file inventory exceeds a hard review limit."""


class DiffUnavailableError(DiffServiceError):
    """Raised when Git or the worktree cannot be inspected safely."""


class DiffStatus(StrEnum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"
    TYPE_CHANGED = "type_changed"

    @classmethod
    def from_git(cls, code: str) -> DiffStatus:
        values = {
            "A": cls.ADDED,
            "M": cls.MODIFIED,
            "D": cls.DELETED,
            "R": cls.RENAMED,
            "T": cls.TYPE_CHANGED,
        }
        try:
            return values[code]
        except KeyError as error:
            raise DiffConflictError("Worktree contains an unsupported Git state.") from error


class DiffContentKind(StrEnum):
    TEXT = "text"
    BINARY = "binary"
    LARGE = "large"
    OMITTED = "omitted"


@dataclass(frozen=True)
class DiffLimits:
    max_files: int = 100
    max_text_file_bytes: int = 256 * 1024
    max_patch_bytes: int = 1024 * 1024
    context_lines: int = 3

    def __post_init__(self) -> None:
        values = (
            self.max_files,
            self.max_text_file_bytes,
            self.max_patch_bytes,
            self.context_lines,
        )
        if any(type(value) is not int for value in values):
            raise TypeError("Diff limits must be integers.")
        if not 1 <= self.max_files <= _MAX_FILES:
            raise ValueError("Changed file limit is outside the supported range.")
        if not 1 <= self.max_text_file_bytes <= _MAX_TEXT_FILE_BYTES:
            raise ValueError("Text file limit is outside the supported range.")
        if not 1 <= self.max_patch_bytes <= _MAX_PATCH_BYTES:
            raise ValueError("Patch limit is outside the supported range.")
        if not 0 <= self.context_lines <= 10:
            raise ValueError("Diff context is outside the supported range.")


@dataclass(frozen=True)
class DiffEntry:
    status: DiffStatus
    path: str
    previous_path: str | None
    content_kind: DiffContentKind
    old_size: int | None
    new_size: int | None
    patch: str | None
    summary: str | None

    @classmethod
    def summarized(
        cls,
        change: DiffChange,
        content_kind: DiffContentKind,
        old_size: int | None,
        new_size: int | None,
        summary: str,
    ) -> DiffEntry:
        return cls(
            status=change.status,
            path=change.path,
            previous_path=change.previous_path,
            content_kind=content_kind,
            old_size=old_size,
            new_size=new_size,
            patch=None,
            summary=summary,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "path": self.path,
            "previous_path": self.previous_path,
            "content_kind": self.content_kind.value,
            "old_size": self.old_size,
            "new_size": self.new_size,
            "patch": self.patch,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class UnifiedDiff:
    base_commit: str
    files: tuple[DiffEntry, ...]
    patch_bytes: int
    truncated: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "base_commit": self.base_commit,
            "files": [entry.to_dict() for entry in self.files],
            "patch_bytes": self.patch_bytes,
            "truncated": self.truncated,
        }


@dataclass(frozen=True)
class DiffChange:
    status: DiffStatus
    path: str
    previous_path: str | None
    old_mode: str
    new_mode: str
    old_object: str | None
    tracked: bool


@dataclass(frozen=True)
class DiffPathState:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    links: int


def valid_object_id(value: str) -> bool:
    return len(value) in {40, 64} and all(character in string.hexdigits for character in value)


def decode_object_id(value: bytes) -> str:
    try:
        object_id = value.decode("ascii").strip().lower()
    except UnicodeDecodeError as error:
        raise DiffConflictError("Git returned an invalid commit.") from error
    if not valid_object_id(object_id):
        raise DiffConflictError("Git returned an invalid commit.")
    return object_id


def disabled_filter_arguments(payload: bytes) -> tuple[str, ...]:
    names: set[str] = set()
    for field in payload.split(b"\0"):
        if not field:
            continue
        name = field.decode("ascii")
        folded = name.casefold()
        if folded.startswith("filter.") and folded.rsplit(".", 1)[-1] in {
            "clean",
            "smudge",
            "process",
            "required",
        }:
            if len(name) > 256 or any(ord(character) < 33 for character in name):
                raise ValueError("Git filter configuration is invalid.")
            names.add(name.rsplit(".", 1)[0])
    if len(names) > 100:
        raise ValueError("Git filter configuration exceeds the safety limit.")
    return tuple(
        argument
        for name in sorted(names, key=str.casefold)
        for argument in (
            "-c",
            f"{name}.clean=",
            "-c",
            f"{name}.smudge=",
            "-c",
            f"{name}.process=",
            "-c",
            f"{name}.required=false",
        )
    )


def is_utf8_text(data: bytes) -> bool:
    if b"\0" in data:
        return False
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
