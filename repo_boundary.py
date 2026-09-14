"""Repository boundary gate for the public source tree.

The public repository must never carry internal planning material, runtime data
or personal identifiers. This module inspects the files that Git would publish
and reports every violation with an actionable diagnostic.

Two policy families are evaluated independently:

1. Path policy inspects the real location and name of each file.
2. Content policy inspects file contents for personal paths and credential
   markers.

The checker is read only. It performs no network access and never modifies the
files it inspects.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

PATH_POLICY = "path"
CONTENT_POLICY = "content"

EXIT_OK = 0
EXIT_VIOLATIONS = 1
EXIT_USAGE = 2
EXIT_SCAN_ERROR = 3

# Directories that never belong to the reviewed public source surface.
SKIPPED_DIRECTORIES = frozenset(
    {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
    }
)

# Directory names that must not appear anywhere inside the public tree.
FORBIDDEN_DIRECTORY_NAMES: dict[str, str] = {
    "project-control": "internal-project-control",
    "working-local": "runtime-home-content",
    "worktrees": "runtime-worktree",
    ".pmem": "local-memory-store",
}

# File names that must not appear anywhere inside the public tree.
FORBIDDEN_FILE_NAMES: dict[str, str] = {
    "rules.md": "internal-operating-rules",
    "plan.md": "internal-plan",
    "tracking.md": "internal-tracking",
    "config.json": "runtime-configuration",
    "config.yaml": "runtime-configuration",
    ".env": "local-credential-file",
    ".ds_store": "local-artifact",
}

# File suffixes that indicate runtime data, private material or tracking
# workbooks rather than public source.
FORBIDDEN_SUFFIXES: dict[str, str] = {
    ".db": "runtime-database",
    ".sqlite": "runtime-database",
    ".sqlite3": "runtime-database",
    ".log": "runtime-log",
    ".pem": "private-key-material",
    ".key": "private-key-material",
    ".p12": "private-key-material",
    ".pfx": "private-key-material",
    ".xls": "tracking-workbook",
    ".xlsx": "tracking-workbook",
    ".xlsm": "tracking-workbook",
    ".numbers": "tracking-workbook",
    ".gguf": "model-weight",
    ".safetensors": "model-weight",
    ".ckpt": "model-weight",
    ".pt": "model-weight",
}

# Content rules are deliberately anchored so that a normal architectural mention
# of a directory name in documentation never triggers a violation.
CONTENT_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("personal-path-macos", re.compile(r"/Users/[A-Za-z0-9._-]+/")),
    ("personal-path-linux", re.compile(r"/home/[A-Za-z0-9._-]+/")),
    ("personal-path-windows", re.compile(r"[A-Za-z]:\\Users\\[A-Za-z0-9._ -]+")),
    ("private-key-block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    (
        "bearer-credential",
        re.compile(r"(?i)authorization\s*[:=]\s*[\"']?bearer\s+\S+"),
    ),
    (
        "assigned-credential",
        re.compile(
            r"(?i)\b(?:api[_-]?key|secret|password|token|access[_-]?token"
            r"|refresh[_-]?token|private[_-]?key)\b\s*[:=]\s*[\"'][^\"'\s]{8,}[\"']"
        ),
    ),
    ("aws-access-key-id", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("github-token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b")),
    ("provider-api-key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("slack-token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b")),
)

MASK_PREFIX_LENGTH = 3
MASK_MAX_LENGTH = 24


@dataclass(frozen=True)
class Violation:
    """One boundary violation with enough context to act on it."""

    path: str
    policy: str
    rule: str
    detail: str
    line: int | None = None

    def render(self) -> str:
        location = self.path if self.line is None else f"{self.path}:{self.line}"
        return f"{self.policy:<7} {self.rule:<26} {location} {self.detail}"


class ScanError(RuntimeError):
    """Raised when the candidate set cannot be inspected completely."""


def mask(value: str) -> str:
    """Return a short, non reversible excerpt of a sensitive match."""

    trimmed = value.strip()
    if len(trimmed) > MASK_MAX_LENGTH:
        trimmed = trimmed[:MASK_MAX_LENGTH]
    if len(trimmed) <= MASK_PREFIX_LENGTH:
        return "***"
    return f"{trimmed[:MASK_PREFIX_LENGTH]}***"


def _git_listing(root: Path) -> list[Path] | None:
    """Return files Git would publish, or None when Git cannot answer."""

    if not (root / ".git").exists():
        return None
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ],
            capture_output=True,
            check=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ScanError("Git could not enumerate the repository candidate set") from error
    names = completed.stdout.decode("utf-8", errors="replace").split("\0")
    return [root / name for name in names if name]


def _walk_listing(root: Path) -> list[Path]:
    """Return every regular file below root, minus tooling directories."""

    found: list[Path] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if any(part in SKIPPED_DIRECTORIES for part in relative.parts):
            continue
        if not path.is_symlink() and not path.is_file():
            continue
        found.append(path)
    return found


def collect_files(root: Path) -> list[Path]:
    """Return the reviewed file set in a stable order."""

    listing = _git_listing(root)
    if listing is None:
        listing = _walk_listing(root)
    existing = [path for path in listing if path.is_file() or path.is_symlink()]
    return sorted(set(existing), key=lambda path: path.relative_to(root).as_posix())


def check_path(relative_path: str) -> Iterator[Violation]:
    """Yield path policy violations for one repository relative path."""

    parts = relative_path.split("/")
    for part in parts[:-1]:
        rule = FORBIDDEN_DIRECTORY_NAMES.get(part.lower())
        if rule is not None:
            yield Violation(
                path=relative_path,
                policy=PATH_POLICY,
                rule=rule,
                detail=f"directory '{part}' is internal only",
            )
    name = parts[-1]
    rule = FORBIDDEN_FILE_NAMES.get(name.lower())
    if rule is not None:
        yield Violation(
            path=relative_path,
            policy=PATH_POLICY,
            rule=rule,
            detail=f"file name '{name}' is internal only",
        )
    suffix_rule = FORBIDDEN_SUFFIXES.get(Path(name).suffix.lower())
    if suffix_rule is not None:
        yield Violation(
            path=relative_path,
            policy=PATH_POLICY,
            rule=suffix_rule,
            detail=f"suffix '{Path(name).suffix}' is not public source",
        )


def read_text(path: Path) -> str:
    """Return decoded content or fail when a candidate cannot be inspected."""

    try:
        data = path.read_bytes()
    except OSError as error:
        raise ScanError(f"candidate could not be read: {path.name}") from error
    return data.decode("utf-8", errors="replace")


def check_symlink(root: Path, path: Path, relative_path: str) -> Iterator[Violation]:
    """Yield violations for unsafe or uninspectable symbolic links."""

    try:
        target = os.readlink(path)
    except OSError as error:
        raise ScanError(f"symbolic link could not be read: {relative_path}") from error

    if Path(target).is_absolute() or re.match(r"^[A-Za-z]:[\\/]", target):
        yield Violation(
            path=relative_path,
            policy=PATH_POLICY,
            rule="absolute-symlink-target",
            detail="symbolic link target must be repository relative",
        )
        return

    repository_root = root.resolve()
    resolved_target = (path.parent / target).resolve(strict=False)
    if not resolved_target.is_relative_to(repository_root):
        yield Violation(
            path=relative_path,
            policy=PATH_POLICY,
            rule="escaping-symlink-target",
            detail="symbolic link target escapes the repository",
        )
        return

    target_relative = resolved_target.relative_to(repository_root).as_posix()
    for target_violation in check_path(target_relative):
        yield Violation(
            path=relative_path,
            policy=PATH_POLICY,
            rule=f"symlink-{target_violation.rule}",
            detail="symbolic link points to content excluded from the public repository",
        )
    yield from check_content(relative_path, target)


def check_content(relative_path: str, text: str) -> Iterator[Violation]:
    """Yield content policy violations for one file body."""

    for line_number, line in enumerate(text.splitlines(), start=1):
        for rule, pattern in CONTENT_RULES:
            match = pattern.search(line)
            if match is None:
                continue
            yield Violation(
                path=relative_path,
                policy=CONTENT_POLICY,
                rule=rule,
                detail=f"matched '{mask(match.group(0))}'",
                line=line_number,
            )


def scan(root: Path) -> list[Violation]:
    """Return every boundary violation found below root, ordered stably."""

    violations: list[Violation] = []
    for path in collect_files(root):
        relative_path = path.relative_to(root).as_posix()
        path_violations = list(check_path(relative_path))
        violations.extend(path_violations)
        if path_violations:
            continue
        if path.is_symlink():
            violations.extend(check_symlink(root, path, relative_path))
            continue
        text = read_text(path)
        violations.extend(check_content(relative_path, text))
    return sorted(violations, key=lambda item: (item.path, item.rule, item.line or 0))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="repo_boundary",
        description="Verify that the public repository carries no internal or personal content.",
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Repository root to inspect. Defaults to the current directory.",
    )
    arguments = parser.parse_args(argv)

    root = Path(arguments.root).resolve()
    if not root.is_dir():
        print("repository boundary check could not run: root is not a directory", file=sys.stderr)
        return EXIT_USAGE

    try:
        candidates = collect_files(root)
        violations = scan(root)
    except ScanError as error:
        print(f"repository boundary check could not complete: {error}", file=sys.stderr)
        return EXIT_SCAN_ERROR

    reviewed = len(candidates)
    if not violations:
        print(f"repository boundary check passed: {reviewed} files reviewed")
        return EXIT_OK

    print(
        f"repository boundary check failed: {len(violations)} violations "
        f"across {reviewed} reviewed files",
        file=sys.stderr,
    )
    for violation in violations:
        print(f"  {violation.render()}", file=sys.stderr)
    return EXIT_VIOLATIONS


if __name__ == "__main__":
    raise SystemExit(main())
