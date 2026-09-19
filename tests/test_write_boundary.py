from __future__ import annotations

import os
from pathlib import Path

import pytest

from write_boundary import WriteBoundary, WriteBoundaryError


def worktree(tmp_path: Path) -> Path:
    root = tmp_path / "worktree"
    root.mkdir()
    return root.resolve(strict=True)


def test_authorizes_only_canonical_targets_below_allowed_paths(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    (root / "src").mkdir()
    existing = root / "src" / "app.py"
    existing.write_text("pass\n", encoding="utf-8")
    boundary = WriteBoundary(root, ("src", "docs"))

    assert boundary.root == root
    assert boundary.allowed_paths == ("src", "docs")
    assert boundary.authorize("src/app.py") == existing
    assert boundary.authorize(existing) == existing
    assert boundary.authorize("docs/new.md") == root / "docs" / "new.md"
    assert boundary.relative_path(existing) == "src/app.py"

    with pytest.raises(WriteBoundaryError, match="allowed scope"):
        boundary.authorize("README.md")
    with pytest.raises(WriteBoundaryError, match="outside the worktree"):
        boundary.authorize(tmp_path / "outside.txt")


@pytest.mark.parametrize(
    "candidate",
    [
        "",
        ".",
        "../outside.txt",
        "src/../outside.txt",
        "/outside.txt",
        "src//app.py",
        "src/./app.py",
        "src/",
        "src\\app.py",
        "C:/outside.txt",
        ".git/config",
        "src/.GIT/config",
        ".agents/policy.toml",
        "src/.CODEX/config.toml",
        "src/control\nname.py",
    ],
)
def test_invalid_or_reserved_paths_fail_closed(tmp_path: Path, candidate: str) -> None:
    boundary = WriteBoundary(worktree(tmp_path))

    with pytest.raises(WriteBoundaryError):
        boundary.authorize(candidate)


def test_root_scope_allows_normal_worktree_paths(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    boundary = WriteBoundary(root)

    assert boundary.allowed_paths == (".",)
    assert boundary.writable_roots() == (root,)
    assert boundary.authorize("nested/new.txt") == root / "nested" / "new.txt"


def test_writable_roots_require_existing_real_directories(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    (root / "src").mkdir()

    assert WriteBoundary(root, ("src",)).writable_roots() == (root / "src",)

    with pytest.raises(WriteBoundaryError, match="unavailable"):
        WriteBoundary(root, ("missing",)).writable_roots()
    (root / "file.txt").write_text("content\n", encoding="utf-8")
    with pytest.raises(WriteBoundaryError, match="directory"):
        WriteBoundary(root, ("file.txt",)).writable_roots()


def test_bound_writable_root_identity_cannot_change(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    source = root / "src"
    source.mkdir()
    boundary = WriteBoundary(root, ("src",))

    assert boundary.writable_roots() == (source,)
    source.rename(root / "original-src")
    source.mkdir()

    with pytest.raises(WriteBoundaryError, match="identity changed"):
        boundary.writable_roots()


def test_symlink_components_are_never_authorized(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    inside = root / "inside"
    inside.mkdir()
    (root / "alias").symlink_to(inside, target_is_directory=True)
    boundary = WriteBoundary(root)

    with pytest.raises(WriteBoundaryError, match="aliases"):
        boundary.authorize("escape/private.txt")
    with pytest.raises(WriteBoundaryError, match="aliases"):
        boundary.authorize("alias/file.txt")
    with pytest.raises(WriteBoundaryError, match="aliases"):
        boundary.authorize("alias")


def test_existing_non_directory_parent_is_rejected(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    (root / "file.txt").write_text("content\n", encoding="utf-8")
    boundary = WriteBoundary(root)

    with pytest.raises(WriteBoundaryError, match="parent is not a directory"):
        boundary.authorize("file.txt/child.txt")


def test_special_files_and_hard_links_are_rejected(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    source = root / "source.txt"
    source.write_text("content\n", encoding="utf-8")
    alias = root / "alias.txt"
    os.link(source, alias)
    fifo = root / "channel"
    os.mkfifo(fifo)
    boundary = WriteBoundary(root)

    with pytest.raises(WriteBoundaryError, match="aliases"):
        boundary.authorize(source)
    with pytest.raises(WriteBoundaryError, match="aliases"):
        boundary.authorize(alias)
    with pytest.raises(WriteBoundaryError, match="type is not allowed"):
        boundary.authorize(fifo)


def test_root_identity_change_is_detected(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    boundary = WriteBoundary(root)
    original = tmp_path / "original"
    root.rename(original)
    root.mkdir()

    with pytest.raises(WriteBoundaryError, match="identity changed"):
        boundary.verify()
    with pytest.raises(WriteBoundaryError, match="identity changed"):
        boundary.authorize("new.txt")


def test_root_and_scope_construction_rejects_unsafe_inputs(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    file_root = tmp_path / "file"
    file_root.write_text("content\n", encoding="utf-8")
    alias_root = tmp_path / "alias"
    alias_root.symlink_to(root, target_is_directory=True)

    with pytest.raises(TypeError):
        WriteBoundary("not-a-path")
    with pytest.raises(ValueError):
        WriteBoundary(Path("relative"))
    with pytest.raises(WriteBoundaryError, match="unavailable"):
        WriteBoundary(tmp_path / "missing")
    with pytest.raises(WriteBoundaryError, match="unavailable"):
        WriteBoundary(file_root)
    with pytest.raises(WriteBoundaryError, match="unavailable"):
        WriteBoundary(alias_root)
    with pytest.raises(TypeError):
        WriteBoundary(root, [])
    with pytest.raises(TypeError):
        WriteBoundary(root, ("src", Path("docs")))
    with pytest.raises(ValueError, match="unique"):
        WriteBoundary(root, ("src", "src"))
    with pytest.raises(WriteBoundaryError):
        WriteBoundary(root, ("../outside",))


def test_failures_do_not_expose_rejected_paths(tmp_path: Path) -> None:
    root = worktree(tmp_path)
    private_marker = "private-customer-name"
    boundary = WriteBoundary(root, ("src",))

    with pytest.raises(WriteBoundaryError) as captured:
        boundary.authorize(f"../{private_marker}/secret.txt")

    assert private_marker not in str(captured.value)
