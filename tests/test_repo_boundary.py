"""Acceptance tests for the repository boundary gate.

Every negative fixture is synthetic and is created inside a temporary directory,
so no forbidden path ever enters the public tree. No test touches the network.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from repo_boundary import (
    EXIT_OK,
    EXIT_SCAN_ERROR,
    EXIT_USAGE,
    EXIT_VIOLATIONS,
    ScanError,
    collect_files,
    main,
    mask,
    scan,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

SYNTHETIC_MACOS_PATH = "/Users/" + "synthetic_user" + "/projects/demo/config.json"
SYNTHETIC_LINUX_PATH = "/home/" + "synthetic_user" + "/projects/demo/config.json"
SYNTHETIC_WINDOWS_PATH = "C:" + chr(92) + "Users" + chr(92) + "synthetic_user"
SYNTHETIC_PRIVATE_KEY = (
    "-----" + "BEGIN RSA PRIVATE KEY-----\n"
    "SYNTHETICKEYMATERIALFORTESTINGONLY0123456789\n"
    "-----" + "END RSA PRIVATE KEY-----\n"
)
SYNTHETIC_CREDENTIAL = "synthetic" + "_credential_value_123456"


def build_clean_repository(root: Path) -> Path:
    """Create a minimal synthetic repository that must always pass."""

    (root / "src").mkdir(parents=True, exist_ok=True)
    (root / "src" / "service.py").write_text(
        'def handler() -> str:\n    return "ready"\n',
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "# Demo\n\nPublic source only.\n",
        encoding="utf-8",
    )
    return root


def rules_triggered(root: Path) -> set[str]:
    return {violation.rule for violation in scan(root)}


def test_public_repository_passes_the_boundary_gate() -> None:
    assert scan(REPOSITORY_ROOT) == []


def test_clean_synthetic_repository_passes(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)

    assert scan(tmp_path) == []


def test_macos_personal_path_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "src" / "settings.py").write_text(
        f'DATA_ROOT = "{SYNTHETIC_MACOS_PATH}"\n',
        encoding="utf-8",
    )

    assert "personal-path-macos" in rules_triggered(tmp_path)


def test_linux_personal_path_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "src" / "settings.py").write_text(
        f'DATA_ROOT = "{SYNTHETIC_LINUX_PATH}"\n',
        encoding="utf-8",
    )

    assert "personal-path-linux" in rules_triggered(tmp_path)


def test_windows_personal_path_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "src" / "settings.py").write_text(
        f"DATA_ROOT = r'{SYNTHETIC_WINDOWS_PATH}'\n",
        encoding="utf-8",
    )

    assert "personal-path-windows" in rules_triggered(tmp_path)


def test_project_control_path_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    control = tmp_path / "project-control"
    control.mkdir()
    (control / "notes.md").write_text("Internal tracking notes.\n", encoding="utf-8")

    assert "internal-project-control" in rules_triggered(tmp_path)


def test_working_local_content_is_rejected_when_present_in_the_tree(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    runtime_home = tmp_path / "working-local"
    runtime_home.mkdir()
    (runtime_home / "notes.md").write_text("Runtime home content.\n", encoding="utf-8")

    assert "runtime-home-content" in rules_triggered(tmp_path)


def test_runtime_database_and_log_files_are_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "state.db").write_text("synthetic database placeholder\n", encoding="utf-8")
    (tmp_path / "server.log").write_text("synthetic log line\n", encoding="utf-8")

    triggered = rules_triggered(tmp_path)

    assert "runtime-database" in triggered
    assert "runtime-log" in triggered


def test_private_key_marker_is_rejected_without_leaking_the_material(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "deploy_notes.md").write_text(SYNTHETIC_PRIVATE_KEY, encoding="utf-8")

    violations = [item for item in scan(tmp_path) if item.rule == "private-key-block"]

    assert violations
    rendered = " ".join(violation.render() for violation in violations)
    assert "SYNTHETICKEYMATERIALFORTESTINGONLY0123456789" not in rendered
    assert "***" in rendered


def test_safe_documentation_mention_is_not_a_false_positive(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "README.md").write_text(
        "# Demo\n\n"
        "Source lives in `working/` and personal runtime state lives in `working-local/`.\n"
        "The plan for the next release is tracked outside this repository.\n"
        "Set AGENT_WORKBENCH_HOME before starting the server.\n",
        encoding="utf-8",
    )

    assert scan(tmp_path) == []


def test_scan_does_not_modify_the_inspected_tree(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "src" / "settings.py").write_text(
        f'DATA_ROOT = "{SYNTHETIC_MACOS_PATH}"\n',
        encoding="utf-8",
    )

    def fingerprint() -> dict[str, tuple[int, str]]:
        return {
            path.relative_to(tmp_path).as_posix(): (
                path.stat().st_mtime_ns,
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            for path in collect_files(tmp_path)
        }

    before = fingerprint()
    scan(tmp_path)
    after = fingerprint()

    assert before == after


def test_scan_result_is_deterministic(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "state.db").write_text("synthetic database placeholder\n", encoding="utf-8")
    (tmp_path / "src" / "settings.py").write_text(
        f'DATA_ROOT = "{SYNTHETIC_LINUX_PATH}"\n',
        encoding="utf-8",
    )

    first = scan(tmp_path)
    second = scan(tmp_path)

    assert first == second
    assert [violation.render() for violation in first] == [
        violation.render() for violation in second
    ]


@pytest.mark.parametrize("relative_path", ["repo_boundary.py", "tests/test_repo_boundary.py"])
def test_checker_files_receive_content_scanning(tmp_path: Path, relative_path: str) -> None:
    build_clean_repository(tmp_path)
    candidate = tmp_path / relative_path
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text(
        "token" + f' = "{SYNTHETIC_CREDENTIAL}"\n',
        encoding="utf-8",
    )

    assert "assigned-credential" in rules_triggered(tmp_path)


def test_absolute_symbolic_link_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    synthetic_target = Path("/") / "Users" / "synthetic_user" / "private" / "data"
    (tmp_path / "private_link").symlink_to(synthetic_target)

    assert "absolute-symlink-target" in rules_triggered(tmp_path)


def test_escaping_symbolic_link_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "outside_link").symlink_to("../outside.txt")

    assert "escaping-symlink-target" in rules_triggered(tmp_path)


def test_safe_relative_symbolic_link_passes(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "service_link.py").symlink_to("src/service.py")

    assert scan(tmp_path) == []


def test_symbolic_link_to_internal_content_is_rejected(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "runtime_link").symlink_to("working-local/config.json")

    assert "symlink-runtime-home-content" in rules_triggered(tmp_path)


def test_git_listing_failure_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / ".git").mkdir()

    def fail_git(*args: object, **kwargs: object) -> None:
        raise subprocess.CalledProcessError(1, "git")

    monkeypatch.setattr(subprocess, "run", fail_git)

    with pytest.raises(ScanError, match="Git could not enumerate"):
        scan(tmp_path)
    assert main([str(tmp_path)]) == EXIT_SCAN_ERROR


def test_unreadable_candidate_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build_clean_repository(tmp_path)
    candidate = tmp_path / "src" / "service.py"
    original_read_bytes = Path.read_bytes

    def fail_candidate(path: Path) -> bytes:
        if path == candidate:
            raise PermissionError("synthetic permission failure")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", fail_candidate)

    with pytest.raises(ScanError, match="candidate could not be read"):
        scan(tmp_path)
    assert main([str(tmp_path)]) == EXIT_SCAN_ERROR


def test_binary_content_cannot_bypass_path_policy(tmp_path: Path) -> None:
    build_clean_repository(tmp_path)
    (tmp_path / "state.db").write_bytes(b"\x00\x01\x02")

    assert "runtime-database" in rules_triggered(tmp_path)


def test_mask_hides_the_original_value() -> None:
    masked = mask("supersecretvalue")

    assert masked == "sup***"
    assert "supersecretvalue" not in masked


def test_cli_exit_codes(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    build_clean_repository(tmp_path)

    assert main([str(tmp_path)]) == EXIT_OK

    (tmp_path / "state.db").write_text("synthetic database placeholder\n", encoding="utf-8")

    assert main([str(tmp_path)]) == EXIT_VIOLATIONS
    assert main([str(tmp_path / "missing")]) == EXIT_USAGE

    captured = capsys.readouterr()
    assert "repository boundary check failed" in captured.err
