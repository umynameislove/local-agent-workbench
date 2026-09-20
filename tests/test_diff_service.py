from __future__ import annotations

import json
import shlex
import subprocess
from pathlib import Path

import pytest

from diff_service import (
    DiffConflictError,
    DiffContentKind,
    DiffLimitError,
    DiffLimits,
    DiffStatus,
    DiffUnavailableError,
    ReadOnlyDiffService,
)


def git(repository: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
    ).stdout


def head(repository: Path) -> str:
    return git(repository, "rev-parse", "HEAD").decode("ascii").strip()


def initialize_repository(path: Path, files: dict[str, bytes] | None = None) -> Path:
    path.mkdir()
    git(path, "init", "--quiet")
    for name, content in (files or {"example.txt": b"initial\n"}).items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    git(path, "add", "--all")
    git(
        path,
        "-c",
        "user.name=Test User",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Initial fixture",
    )
    return path.resolve(strict=True)


@pytest.mark.anyio
async def test_collects_stable_text_changes_without_mutating_git_state(tmp_path: Path) -> None:
    repository = initialize_repository(
        tmp_path / "project",
        {
            "modified.txt": b"initial\n",
            "deleted.txt": b"delete me\n",
            "renamed.txt": b"move me\n",
        },
    )
    base = head(repository)
    (repository / "modified.txt").write_text("staged version\n", encoding="utf-8")
    (repository / "deleted.txt").unlink()
    (repository / "renamed.txt").rename(repository / "moved.txt")
    git(repository, "add", "--all")
    (repository / "modified.txt").write_text("working version\n", encoding="utf-8")
    (repository / "untracked.txt").write_text("new file\n", encoding="utf-8")
    status_before = git(repository, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    index_before = (repository / ".git" / "index").read_bytes()

    result = await ReadOnlyDiffService(repository, base).collect()

    entries = {entry.path: entry for entry in result.files}
    assert result.base_commit == base
    assert result.truncated is False
    assert set(entries) == {"deleted.txt", "modified.txt", "moved.txt", "untracked.txt"}
    assert entries["modified.txt"].status is DiffStatus.MODIFIED
    assert "+working version" in (entries["modified.txt"].patch or "")
    assert entries["deleted.txt"].status is DiffStatus.DELETED
    assert "+++ /dev/null" in (entries["deleted.txt"].patch or "")
    assert entries["moved.txt"].status is DiffStatus.RENAMED
    assert entries["moved.txt"].previous_path == "renamed.txt"
    assert entries["untracked.txt"].status is DiffStatus.ADDED
    assert "+new file" in (entries["untracked.txt"].patch or "")
    assert all(entry.content_kind is DiffContentKind.TEXT for entry in result.files)
    assert result.patch_bytes == sum(
        len((entry.patch or "").encode("utf-8")) for entry in result.files
    )
    assert str(repository) not in json.dumps(result.to_dict())
    assert git(repository, "status", "--porcelain=v1", "-z", "--untracked-files=all") == (
        status_before
    )
    assert (repository / ".git" / "index").read_bytes() == index_before
    assert head(repository) == base


@pytest.mark.anyio
async def test_binary_and_large_files_are_summarized_without_content(tmp_path: Path) -> None:
    repository = initialize_repository(
        tmp_path / "project",
        {
            "binary.dat": b"\x00old-private-marker",
            "large.txt": b"small\n",
        },
    )
    (repository / "binary.dat").write_bytes(b"\x00new-private-marker")
    (repository / "large.txt").write_text("sensitive-line\n" * 20, encoding="utf-8")
    (repository / "invalid.dat").write_bytes(b"\xff\xfeprivate-marker")

    result = await ReadOnlyDiffService(
        repository,
        head(repository),
        limits=DiffLimits(max_text_file_bytes=64, max_patch_bytes=2_048),
    ).collect()

    entries = {entry.path: entry for entry in result.files}
    assert entries["binary.dat"].content_kind is DiffContentKind.BINARY
    assert entries["invalid.dat"].content_kind is DiffContentKind.BINARY
    assert entries["large.txt"].content_kind is DiffContentKind.LARGE
    assert all(entry.patch is None for entry in result.files)
    serialized = json.dumps(result.to_dict())
    assert "private-marker" not in serialized
    assert "sensitive-line" not in serialized
    assert result.patch_bytes == 0
    assert result.truncated is False


@pytest.mark.anyio
async def test_patch_budget_omits_complete_files_instead_of_truncating_text(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    (repository / "example.txt").write_text("replacement\n", encoding="utf-8")

    result = await ReadOnlyDiffService(
        repository,
        head(repository),
        limits=DiffLimits(max_text_file_bytes=1_024, max_patch_bytes=1),
    ).collect()

    assert result.truncated is True
    assert result.patch_bytes == 0
    assert result.files[0].content_kind is DiffContentKind.OMITTED
    assert result.files[0].patch is None
    assert result.files[0].summary == "Text patch omitted because the review limit was reached."


@pytest.mark.anyio
async def test_changed_file_limit_fails_closed(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    (repository / "one.txt").write_text("one\n", encoding="utf-8")
    (repository / "two.txt").write_text("two\n", encoding="utf-8")

    service = ReadOnlyDiffService(
        repository,
        head(repository),
        limits=DiffLimits(max_files=1),
    )
    with pytest.raises(DiffLimitError, match="file count"):
        await service.collect()


@pytest.mark.anyio
async def test_snapshot_head_mismatch_fails_before_diff_generation(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    original = head(repository)
    (repository / "example.txt").write_text("second\n", encoding="utf-8")
    git(repository, "add", "example.txt")
    git(
        repository,
        "-c",
        "user.name=Test User",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Second fixture",
    )

    with pytest.raises(DiffConflictError, match="job snapshot"):
        await ReadOnlyDiffService(repository, original).collect()


@pytest.mark.anyio
async def test_symlink_and_control_character_paths_fail_without_target_disclosure(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    outside = tmp_path / "private-customer-folder"
    outside.mkdir()
    (repository / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(DiffConflictError) as symlink_error:
        await ReadOnlyDiffService(repository, head(repository)).collect()
    assert "private-customer-folder" not in str(symlink_error.value)

    (repository / "escape").unlink()
    (repository / "bad\nprivate-name.txt").write_text("content\n", encoding="utf-8")
    with pytest.raises(DiffConflictError) as path_error:
        await ReadOnlyDiffService(repository, head(repository)).collect()
    assert "private-name" not in str(path_error.value)


@pytest.mark.anyio
async def test_external_git_drivers_cannot_execute(tmp_path: Path) -> None:
    repository = initialize_repository(
        tmp_path / "project",
        {
            ".gitattributes": b"*.txt diff=unsafe filter=unsafe\n",
            "example.txt": b"initial\n",
        },
    )
    marker = tmp_path / "external-driver-ran"
    driver = tmp_path / "unsafe-driver.sh"
    driver.write_text(
        f"#!/bin/sh\ntouch {shlex.quote(str(marker))}\nexit 97\n",
        encoding="utf-8",
    )
    driver.chmod(0o755)
    git(repository, "config", "diff.unsafe.command", str(driver))
    git(repository, "config", "diff.unsafe.textconv", str(driver))
    git(repository, "config", "filter.unsafe.clean", str(driver))
    git(repository, "config", "filter.unsafe.smudge", str(driver))
    (repository / "example.txt").write_text("changed\n", encoding="utf-8")

    result = await ReadOnlyDiffService(repository, head(repository)).collect()

    assert result.files[0].content_kind is DiffContentKind.TEXT
    assert marker.exists() is False


@pytest.mark.anyio
async def test_ignored_files_do_not_enter_the_review_inventory(tmp_path: Path) -> None:
    repository = initialize_repository(
        tmp_path / "project",
        {
            ".gitignore": b"ignored.txt\n",
            "example.txt": b"initial\n",
        },
    )
    (repository / "ignored.txt").write_text("private local content\n", encoding="utf-8")

    result = await ReadOnlyDiffService(repository, head(repository)).collect()

    assert result.files == ()
    assert result.patch_bytes == 0


@pytest.mark.anyio
async def test_concurrent_content_change_is_detected(monkeypatch, tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    target = repository / "example.txt"
    target.write_text("first change\n", encoding="utf-8")
    service = ReadOnlyDiffService(repository, head(repository))
    original_patch = service._patch

    async def mutate_after_patch(change):
        patch = await original_patch(change)
        target.write_text("second change with different size\n", encoding="utf-8")
        return patch

    monkeypatch.setattr(service, "_patch", mutate_after_patch)
    with pytest.raises(DiffConflictError, match="changed during diff generation"):
        await service.collect()


def test_constructor_and_limit_validation_fail_closed(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    base = head(repository)
    alias = tmp_path / "repository-alias"
    alias.symlink_to(repository, target_is_directory=True)

    with pytest.raises(ValueError, match="absolute"):
        ReadOnlyDiffService(Path("relative"), base)
    with pytest.raises(ValueError, match="commit"):
        ReadOnlyDiffService(repository, "not-a-commit")
    with pytest.raises(DiffUnavailableError, match="unavailable"):
        ReadOnlyDiffService(alias, base)
    with pytest.raises(ValueError, match="file limit"):
        DiffLimits(max_files=0)
    with pytest.raises(ValueError, match="Text file limit"):
        DiffLimits(max_text_file_bytes=0)
    with pytest.raises(ValueError, match="Patch limit"):
        DiffLimits(max_patch_bytes=0)
