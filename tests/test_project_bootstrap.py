from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from db import Database, ProjectAlreadyExistsError, ProjectRepository
from engine import (
    ConfigurationError,
    ProcessRunnerError,
    WorkbenchConfig,
    load_configured_projects,
    resolve_runtime_home,
)


def initialize_git_repository(path: Path) -> Path:
    path.mkdir(parents=True)
    subprocess.run(
        ("git", "init", "--quiet", str(path)),
        check=True,
        capture_output=True,
    )
    return path


def write_example_config(
    runtime: Path,
    root: Path | str,
    *,
    project_id: str = "sample-project",
) -> Path:
    runtime.mkdir(parents=True, exist_ok=True)
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["projects"][0]["id"] = project_id
    config["projects"][0]["root"] = str(root)
    path = runtime / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


@pytest.mark.anyio
async def test_example_config_registers_one_canonical_project_and_safe_metadata(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    repository = initialize_git_repository(tmp_path / "project with spaces")
    relative_root = Path(os.path.relpath(repository, runtime))
    write_example_config(runtime, relative_root)
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    async with api.router.lifespan_context(api):
        stored = api.state.project_repository.get("sample-project")
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/bootstrap")

    assert stored.root == str(repository.resolve())
    assert api.state.projects == (stored,)
    assert response.status_code == 200
    assert response.json()["projects"] == [
        {
            "id": "sample-project",
            "sensitivity": "private",
            "cloud_allowed": False,
            "permission_mode": "sandboxed-write",
        }
    ]
    assert str(tmp_path) not in response.text


@pytest.mark.anyio
async def test_missing_configuration_keeps_bootstrap_available_with_no_projects(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/bootstrap")

    assert api.state.projects == ()
    assert response.json()["projects"] == []
    assert resolve_runtime_home({"AGENT_WORKBENCH_HOME": str(runtime)}).config == (
        runtime / "config.json"
    )


@pytest.mark.anyio
async def test_matching_project_registration_is_idempotent_across_restart(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    repository = initialize_git_repository(tmp_path / "project")
    write_example_config(runtime, repository)

    first_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with first_app.router.lifespan_context(first_app):
        first = first_app.state.projects[0]

    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted_app.router.lifespan_context(restarted_app):
        restarted = restarted_app.state.projects[0]

    assert restarted == first
    assert restarted_app.state.project_repository.list() == (first,)


@pytest.mark.anyio
async def test_changed_project_configuration_fails_without_rewriting_state(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    original_root = initialize_git_repository(tmp_path / "original")
    changed_root = initialize_git_repository(tmp_path / "changed")
    write_example_config(runtime, original_root)

    first_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with first_app.router.lifespan_context(first_app):
        original = first_app.state.projects[0]

    write_example_config(runtime, changed_root)
    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    with pytest.raises(ProjectAlreadyExistsError, match="conflicts") as failure:
        async with restarted_app.router.lifespan_context(restarted_app):
            pass

    assert str(tmp_path) not in str(failure.value)
    stored = ProjectRepository(Database(runtime / "state.db")).get("sample-project")
    assert stored == original


@pytest.mark.anyio
async def test_persisted_root_cannot_be_registered_under_another_identity(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    repository = initialize_git_repository(tmp_path / "project")
    write_example_config(runtime, repository)

    first_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with first_app.router.lifespan_context(first_app):
        original = first_app.state.projects[0]

    write_example_config(runtime, repository, project_id="replacement")
    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    with pytest.raises(ProjectAlreadyExistsError, match="another project"):
        async with restarted_app.router.lifespan_context(restarted_app):
            pass

    assert ProjectRepository(Database(runtime / "state.db")).list() == (original,)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("missing", "does not exist"),
        ("file", "must be a directory"),
        ("directory", "top level of a Git worktree"),
        ("nested", "points inside a Git worktree"),
    ],
)
async def test_invalid_project_roots_fail_before_database_creation(
    tmp_path: Path,
    kind: str,
    message: str,
) -> None:
    runtime = tmp_path / "runtime"
    candidate = tmp_path / "candidate"
    if kind == "file":
        candidate.write_text("not a directory", encoding="utf-8")
    elif kind == "directory":
        candidate.mkdir()
    elif kind == "nested":
        initialize_git_repository(candidate)
        candidate = candidate / "nested"
        candidate.mkdir()
    write_example_config(runtime, candidate)
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    with pytest.raises(ConfigurationError, match=message) as failure:
        async with api.router.lifespan_context(api):
            pass

    assert str(tmp_path) not in str(failure.value)
    assert not (runtime / "state.db").exists()


@pytest.mark.anyio
async def test_git_start_failure_is_actionable_and_does_not_expose_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = tmp_path / "runtime"
    repository = initialize_git_repository(tmp_path / "project")
    config_path = write_example_config(runtime, repository)

    async def fail_git(*args: object, **kwargs: object) -> None:
        raise ProcessRunnerError("synthetic failure")

    monkeypatch.setattr("engine.run_process", fail_git)

    with pytest.raises(ConfigurationError, match="Git could not be started") as failure:
        await load_configured_projects(config_path)

    assert str(repository) not in str(failure.value)


@pytest.mark.parametrize(
    "value",
    [
        {"version": 1, "projects": "invalid"},
        {"version": 1, "projects": [None]},
        {
            "version": 1,
            "projects": [
                {
                    "id": "sample",
                    "root": ".",
                    "sensitivity": "unknown",
                }
            ],
        },
    ],
)
def test_invalid_project_shapes_raise_configuration_errors(value: object) -> None:
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("consultant", [[], {"min_confidence": "invalid"}])
def test_invalid_consultant_shape_or_limits_raise_configuration_error(
    consultant: object,
) -> None:
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["consultant"] = consultant

    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(config)


@pytest.mark.parametrize("field", ["id", "root"])
@pytest.mark.parametrize("value", [7, " padded ", "bad\x00value"])
def test_project_identity_and_root_require_clean_strings(field: str, value: object) -> None:
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["projects"][0][field] = value

    with pytest.raises(ConfigurationError, match="requires an id and a root"):
        WorkbenchConfig.from_dict(config)


@pytest.mark.anyio
async def test_duplicate_canonical_roots_fail_before_database_creation(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    repository = initialize_git_repository(tmp_path / "project")
    path = write_example_config(runtime, repository)
    config = json.loads(path.read_text(encoding="utf-8"))
    duplicate = {**config["projects"][0], "id": "second-project"}
    config["projects"].append(duplicate)
    path.write_text(json.dumps(config), encoding="utf-8")
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    with pytest.raises(ConfigurationError, match="roots must be unique"):
        async with api.router.lifespan_context(api):
            pass

    assert not (runtime / "state.db").exists()


@pytest.mark.anyio
async def test_invalid_json_is_sanitized_and_does_not_create_database(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    config_path = runtime / "config.json"
    config_path.write_text("{invalid", encoding="utf-8")
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    with pytest.raises(ConfigurationError, match="valid JSON") as failure:
        async with api.router.lifespan_context(api):
            pass

    assert str(config_path) not in str(failure.value)
    assert not (runtime / "state.db").exists()
