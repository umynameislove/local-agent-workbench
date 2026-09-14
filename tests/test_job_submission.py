from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from app import create_app
from db import JobRepository
from engine import (
    JobRuntime,
    PermissionMode,
    ProcessResult,
    ProcessRunnerError,
    ProjectConfig,
    Sensitivity,
)


def git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def initialize_repository(path: Path, *, committed: bool = True) -> Path:
    path.mkdir()
    git(path, "init", "--quiet")
    if committed:
        (path / "example.txt").write_text("initial\n", encoding="utf-8")
        git(path, "add", "example.txt")
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
    return path


def commit_change(repository: Path) -> str:
    (repository / "example.txt").write_text("changed\n", encoding="utf-8")
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
        "Change fixture",
    )
    return git(repository, "rev-parse", "HEAD")


def write_config(runtime: Path, repository: Path) -> None:
    runtime.mkdir()
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["projects"][0]["root"] = str(repository)
    (runtime / "config.json").write_text(json.dumps(config), encoding="utf-8")


def ids(*values: str) -> Iterator[str]:
    return iter(values)


@pytest.mark.anyio
async def test_create_job_snapshots_server_observed_state_and_keeps_it_immutable(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    initial_head = git(repository, "rev-parse", "HEAD")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    generated = ids("job-first", "job-second")
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=generated.__next__,
    )
    request_text = "Review the retry path before changing any files."

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            first_response = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": request_text},
            )
            first = api.state.job_repository.get("job-first")
            original_snapshot = first.request_snapshot
            assert git(repository, "rev-parse", "HEAD") == initial_head
            assert git(repository, "status", "--short") == ""
            assert tuple((runtime / "worktrees").iterdir()) == ()

            api.state.project_repository.update(
                ProjectConfig(
                    id="sample-project",
                    root=str(repository),
                    sensitivity=Sensitivity.PUBLIC,
                    cloud_allowed=True,
                    permission_mode=PermissionMode.READ_ONLY,
                )
            )
            changed_head = commit_change(repository)
            second_response = await client.post(
                "/api/jobs",
                json={
                    "project_id": "sample-project",
                    "request": "Summarize the current repository state.",
                    "runtime": "local",
                    "model": "qwen-local",
                },
            )

        stored_first = api.state.job_repository.get("job-first")
        stored_second = api.state.job_repository.get("job-second")

    assert first_response.status_code == 201
    assert first_response.json() == {
        "created_at": first.created_at,
        "id": "job-first",
        "model": None,
        "project_id": "sample-project",
        "request": request_text,
        "runtime": "auto",
        "state": "created",
    }
    assert original_snapshot == {
        "config_version": 1,
        "model": None,
        "policy_version": 1,
        "project": {
            "cloud_allowed": False,
            "id": "sample-project",
            "permission_mode": "sandboxed-write",
            "root": str(repository),
            "sensitivity": "private",
        },
        "prompt_hash": f"sha256:{hashlib.sha256(request_text.encode()).hexdigest()}",
        "repo_head": initial_head,
        "runtime": "auto",
    }
    assert stored_first.request_snapshot == original_snapshot
    assert stored_second.request_snapshot["repo_head"] == changed_head
    assert stored_second.request_snapshot["project"]["sensitivity"] == "public"
    assert stored_second.runtime is JobRuntime.LOCAL
    assert second_response.status_code == 201
    for response in (first_response, second_response):
        assert str(repository) not in response.text
        assert "request_snapshot" not in response.text
        assert "repo_head" not in response.text


@pytest.mark.anyio
async def test_invalid_payloads_fail_before_git_or_database_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    async def unexpected_git(*args: object, **kwargs: object) -> ProcessResult:
        raise AssertionError("Git must not run for invalid input.")

    invalid_payloads = [
        [],
        {},
        {"project_id": "sample-project"},
        {"project_id": 7, "request": "Valid request"},
        {"project_id": " sample-project", "request": "Valid request"},
        {"project_id": "sample-project", "request": " \n "},
        {"project_id": "sample-project", "request": "unsafe\x00request"},
        {"project_id": "sample-project", "request": "x" * 262_145},
        {"project_id": "sample-project", "request": "Valid request", "runtime": 7},
        {
            "project_id": "sample-project",
            "request": "Valid request",
            "runtime": "remote",
        },
        {"project_id": "sample-project", "request": "Valid request", "model": ""},
        {
            "project_id": "sample-project",
            "request": "Valid request",
            "request_snapshot": {"repo_head": "client-controlled"},
        },
    ]

    async with api.router.lifespan_context(api):
        monkeypatch.setattr("job_submission.run_process", unexpected_git)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            responses = [
                await client.post("/api/jobs", json=payload) for payload in invalid_payloads
            ]

        assert api.state.job_repository.list() == ()

    assert all(response.status_code == 422 for response in responses)
    assert all(str(tmp_path) not in response.text for response in responses)


@pytest.mark.anyio
async def test_missing_project_fails_without_git_or_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})

    async def unexpected_git(*args: object, **kwargs: object) -> ProcessResult:
        raise AssertionError("Git must not run for a missing project.")

    async with api.router.lifespan_context(api):
        monkeypatch.setattr("job_submission.run_process", unexpected_git)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/jobs",
                json={"project_id": "missing", "request": "Inspect this project."},
            )

        assert api.state.job_repository.list() == ()

    assert response.status_code == 404
    assert response.json() == {"detail": "Project does not exist."}


@pytest.mark.anyio
async def test_project_persisted_from_removed_config_is_not_active(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)

    configured = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with configured.router.lifespan_context(configured):
        assert configured.state.project_repository.get("sample-project").root == str(repository)

    (runtime / "config.json").unlink()
    restarted = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted.router.lifespan_context(restarted):
        assert restarted.state.projects == ()
        assert restarted.state.project_repository.get("sample-project").root == str(repository)
        transport = httpx.ASGITransport(app=restarted)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Inspect this project."},
            )

        assert restarted.state.job_repository.list() == ()

    assert response.status_code == 404
    assert response.json() == {"detail": "Project does not exist."}


@pytest.mark.anyio
async def test_repository_without_commit_returns_sanitized_conflict(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "empty", committed=False)
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Inspect this project."},
            )

        assert api.state.job_repository.list() == ()

    assert response.status_code == 409
    assert response.json() == {"detail": "Project repository has no readable commit."}
    assert str(repository) not in response.text


@pytest.mark.anyio
async def test_git_failure_and_invalid_output_are_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})

    async def unavailable(*args: object, **kwargs: object) -> ProcessResult:
        raise ProcessRunnerError(f"sensitive path: {repository}")

    async def invalid(*args: object, **kwargs: object) -> ProcessResult:
        return ProcessResult(0, b"not-a-commit\n", str(repository).encode())

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            monkeypatch.setattr("job_submission.run_process", unavailable)
            unavailable_response = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Inspect this project."},
            )
            monkeypatch.setattr("job_submission.run_process", invalid)
            invalid_response = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Inspect this project."},
            )

        assert api.state.job_repository.list() == ()

    assert unavailable_response.status_code == 503
    assert unavailable_response.json() == {"detail": "Git inspection is unavailable."}
    assert invalid_response.status_code == 409
    assert invalid_response.json() == {"detail": "Project repository returned an invalid commit."}
    assert str(repository) not in unavailable_response.text
    assert str(repository) not in invalid_response.text


@pytest.mark.anyio
async def test_identity_conflict_and_storage_failure_do_not_replace_jobs(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    generated = ids("job-fixed", "job-fixed", "job-new")
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=generated.__next__,
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "First request."},
            )
            conflict = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Replacement request."},
            )
            original = api.state.job_repository.get("job-fixed")

            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER reject_new_jobs
                    BEFORE INSERT ON jobs
                    BEGIN
                        SELECT RAISE(ABORT, 'synthetic storage failure');
                    END
                    """
                )
            unavailable = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Another request."},
            )

        assert api.state.job_repository.list() == (original,)

    assert created.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "Job identity already exists."}
    assert unavailable.status_code == 503
    assert unavailable.json() == {"detail": "Job storage is unavailable."}
    assert JobRepository(api.state.database).get("job-fixed") == original
