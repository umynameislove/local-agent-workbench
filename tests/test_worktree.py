from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from engine import JobState


def git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def git_result(repository: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=False,
        capture_output=True,
        text=True,
    )


def initialize_repository(path: Path) -> Path:
    path.mkdir()
    git(path, "init", "--quiet")
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
    (repository / "example.txt").write_text("second commit\n", encoding="utf-8")
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
    return git(repository, "rev-parse", "HEAD")


def write_config(
    runtime: Path,
    repository: Path,
    *,
    permission_mode: str = "sandboxed-write",
) -> None:
    runtime.mkdir()
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["projects"][0]["root"] = str(repository)
    config["projects"][0]["permission_mode"] = permission_mode
    (runtime / "config.json").write_text(json.dumps(config), encoding="utf-8")


def identity_for(job_id: str) -> tuple[str, str]:
    digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:32]
    return f"law/job-{digest}", f"job-{digest}"


def add_unbound_worktree(
    repository: Path,
    runtime: Path,
    job_id: str,
    base_commit: str,
) -> tuple[str, str, Path]:
    branch, key = identity_for(job_id)
    target = runtime / "worktrees" / key
    git(
        repository,
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "worktree",
        "add",
        "--no-track",
        "-b",
        branch,
        str(target),
        base_commit,
    )
    return branch, key, target


async def submit_and_plan(client: httpx.AsyncClient, job_id: str) -> dict[str, object]:
    created = await client.post(
        "/api/jobs",
        json={
            "project_id": "sample-project",
            "request": "Make one isolated and reviewable change.",
        },
    )
    assert created.status_code == 201
    assert created.json()["id"] == job_id
    planned = await client.post(f"/api/jobs/{job_id}/plan")
    assert planned.status_code == 200
    return planned.json()


@pytest.mark.anyio
async def test_worktree_uses_snapshot_commit_and_preserves_dirty_project_tree(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    snapshot_head = git(repository, "rev-parse", "HEAD")
    checkout_hook = repository / ".git" / "hooks" / "post-checkout"
    checkout_hook.write_text("#!/bin/sh\nexit 91\n", encoding="utf-8")
    checkout_hook.chmod(0o755)
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-isolated",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-isolated")
            current_head = commit_change(repository)
            (repository / "example.txt").write_text("staged edit\n", encoding="utf-8")
            git(repository, "add", "example.txt")
            (repository / "example.txt").write_text("dirty edit\n", encoding="utf-8")
            (repository / "untracked.txt").write_text("keep me\n", encoding="utf-8")
            status_before = git(
                repository,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            )
            tracked_before = (repository / "example.txt").read_bytes()
            untracked_before = (repository / "untracked.txt").read_bytes()
            staged_diff_before = git(repository, "diff", "--cached", "--binary")
            unstaged_diff_before = git(repository, "diff", "--binary")

            response = await client.post("/api/jobs/job-isolated/worktree")

        stored = api.state.job_repository.get("job-isolated")
        events = api.state.event_repository.list("job-isolated")

    branch, key = identity_for("job-isolated")
    worktree = Path(stored.worktree_path or "")
    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job-isolated",
        "state": "queued",
        "event_id": events[-1].id,
        "worktree": {
            "branch": branch,
            "base_commit": snapshot_head,
            "key": key,
        },
    }
    assert stored.state is JobState.QUEUED
    assert worktree == (runtime / "worktrees" / key).resolve()
    assert worktree.is_dir()
    assert git(worktree, "rev-parse", "HEAD") == snapshot_head
    assert git(worktree, "symbolic-ref", "--short", "HEAD") == branch
    assert (worktree / "example.txt").read_text(encoding="utf-8") == "initial\n"
    assert git(repository, "rev-parse", "HEAD") == current_head
    assert git(repository, "status", "--porcelain=v1", "--untracked-files=all") == status_before
    assert (repository / "example.txt").read_bytes() == tracked_before
    assert (repository / "untracked.txt").read_bytes() == untracked_before
    assert git(repository, "diff", "--cached", "--binary") == staged_diff_before
    assert git(repository, "diff", "--binary") == unstaged_diff_before
    assert [event.event_type for event in events] == [
        "job.classified",
        "job.planning.started",
        "job.plan.recorded",
        "job.worktree.created",
    ]
    assert events[-1].payload == {
        "branch": branch,
        "base_commit": snapshot_head,
        "worktree_key": key,
    }
    assert str(repository) not in response.text
    assert str(runtime) not in response.text


@pytest.mark.anyio
async def test_concurrent_retries_return_one_durable_worktree(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-retry",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-retry")
            responses = await asyncio.gather(
                *(client.post("/api/jobs/job-retry/worktree") for _ in range(8))
            )
            final_retry = await client.post("/api/jobs/job-retry/worktree")

        stored = api.state.job_repository.get("job-retry")
        events = api.state.event_repository.list("job-retry")

    assert all(response.status_code == 200 for response in responses)
    assert all(response.json() == responses[0].json() for response in responses)
    assert final_retry.json() == responses[0].json()
    assert sum(event.event_type == "job.worktree.created" for event in events) == 1
    assert len(tuple((runtime / "worktrees").iterdir())) == 1
    assert stored.worktree_path is not None


@pytest.mark.anyio
async def test_restart_rebinds_verified_unbound_worktree_without_duplicate(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    snapshot_head = git(repository, "rev-parse", "HEAD")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    first_app = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-recovery",
    )

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-recovery")
        branch, key, target = add_unbound_worktree(
            repository,
            runtime,
            "job-recovery",
            snapshot_head,
        )
        assert first_app.state.job_repository.get("job-recovery").worktree_path is None
        assert len(first_app.state.event_repository.list("job-recovery")) == 3

    main_head = git(repository, "rev-parse", "HEAD")
    main_status = git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted_app.router.lifespan_context(restarted_app):
        transport = httpx.ASGITransport(app=restarted_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            responses = await asyncio.gather(
                *(client.post("/api/jobs/job-recovery/worktree") for _ in range(8))
            )
        rebound = restarted_app.state.job_repository.get("job-recovery")
        rebound_events = restarted_app.state.event_repository.list("job-recovery")

    final_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with final_app.router.lifespan_context(final_app):
        transport = httpx.ASGITransport(app=final_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            final_retry = await client.post("/api/jobs/job-recovery/worktree")
        final_events = final_app.state.event_repository.list("job-recovery")

    assert all(response.status_code == 200 for response in responses)
    assert all(response.json() == responses[0].json() for response in responses)
    assert final_retry.status_code == 200
    assert final_retry.json() == responses[0].json()
    assert rebound.worktree_path == str(target.resolve())
    assert sum(event.event_type == "job.worktree.created" for event in rebound_events) == 1
    assert final_events == rebound_events
    assert tuple((runtime / "worktrees").iterdir()) == (target,)
    worktree_entries = [
        line
        for line in git(repository, "worktree", "list", "--porcelain").splitlines()
        if line.startswith("worktree ")
    ]
    assert worktree_entries.count(f"worktree {target.resolve()}") == 1
    assert (
        git_result(
            repository,
            "show-ref",
            "--verify",
            f"refs/heads/{branch}",
        ).returncode
        == 0
    )
    assert git(repository, "rev-parse", "HEAD") == main_head
    assert git(repository, "status", "--porcelain=v1", "--untracked-files=all") == main_status
    assert str(repository) not in final_retry.text
    assert str(runtime) not in final_retry.text


@pytest.mark.anyio
@pytest.mark.parametrize("mutation", ["dirty", "advanced", "ignored"])
async def test_restart_rejects_changed_unbound_worktree(
    tmp_path: Path,
    mutation: str,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    if mutation == "ignored":
        (repository / ".gitignore").write_text(".local-cache\n", encoding="utf-8")
        git(repository, "add", ".gitignore")
        git(
            repository,
            "-c",
            "user.name=Test User",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "Add ignored fixture path",
        )
    snapshot_head = git(repository, "rev-parse", "HEAD")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    first_app = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-changed",
    )

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-changed")
        branch, _, target = add_unbound_worktree(
            repository,
            runtime,
            "job-changed",
            snapshot_head,
        )
        if mutation == "ignored":
            (target / ".local-cache").write_text(
                "changed after interruption\n",
                encoding="utf-8",
            )
        else:
            (target / "example.txt").write_text(
                "changed after interruption\n",
                encoding="utf-8",
            )
        if mutation == "advanced":
            git(target, "add", "example.txt")
            git(
                target,
                "-c",
                "user.name=Test User",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "Unexpected fixture change",
            )

    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted_app.router.lifespan_context(restarted_app):
        transport = httpx.ASGITransport(app=restarted_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/api/jobs/job-changed/worktree")
        stored = restarted_app.state.job_repository.get("job-changed")
        events = restarted_app.state.event_repository.list("job-changed")

    assert response.status_code == 409
    assert stored.worktree_path is None
    assert len(events) == 3
    assert target.is_dir()
    assert (
        git_result(
            repository,
            "show-ref",
            "--verify",
            f"refs/heads/{branch}",
        ).returncode
        == 0
    )
    assert str(repository) not in response.text
    assert str(runtime) not in response.text


@pytest.mark.anyio
async def test_rebind_failure_preserves_preexisting_worktree_for_inspection(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    snapshot_head = git(repository, "rev-parse", "HEAD")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    first_app = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-rebind-failure",
    )

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-rebind-failure")
        branch, _, target = add_unbound_worktree(
            repository,
            runtime,
            "job-rebind-failure",
            snapshot_head,
        )

    restarted_app = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted_app.router.lifespan_context(restarted_app):
        with sqlite3.connect(restarted_app.state.database.path) as connection:
            connection.execute(
                """
                CREATE TRIGGER reject_recovered_worktree_binding
                BEFORE UPDATE ON jobs
                WHEN NEW.worktree_path IS NOT NULL
                BEGIN
                    SELECT RAISE(ABORT, 'synthetic recovered binding failure');
                END
                """
            )
        transport = httpx.ASGITransport(app=restarted_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/api/jobs/job-rebind-failure/worktree")
        stored = restarted_app.state.job_repository.get("job-rebind-failure")
        events = restarted_app.state.event_repository.list("job-rebind-failure")

    assert response.status_code == 503
    assert response.json() == {"detail": "Worktree binding could not be recorded."}
    assert stored.worktree_path is None
    assert len(events) == 3
    assert target.is_dir()
    assert git(target, "rev-parse", "HEAD") == snapshot_head
    assert (
        git_result(
            repository,
            "show-ref",
            "--verify",
            f"refs/heads/{branch}",
        ).returncode
        == 0
    )
    assert "synthetic" not in response.text


@pytest.mark.anyio
async def test_unplanned_job_cannot_create_a_worktree(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-unplanned",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = await client.post(
                "/api/jobs",
                json={"project_id": "sample-project", "request": "Do not write yet."},
            )
            response = await client.post("/api/jobs/job-unplanned/worktree")

        stored = api.state.job_repository.get("job-unplanned")
        events = api.state.event_repository.list("job-unplanned")

    assert created.status_code == 201
    assert response.status_code == 409
    assert response.json() == {"detail": "Job must finish planning before worktree creation."}
    assert stored.state is JobState.CREATED
    assert stored.worktree_path is None
    assert events == ()
    assert tuple((runtime / "worktrees").iterdir()) == ()


@pytest.mark.anyio
@pytest.mark.parametrize("permission_mode", ["read-only", "never"])
async def test_project_policy_blocks_write_capable_worktrees(
    tmp_path: Path,
    permission_mode: str,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository, permission_mode=permission_mode)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-policy",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-policy")
            response = await client.post("/api/jobs/job-policy/worktree")

        stored = api.state.job_repository.get("job-policy")
        events = api.state.event_repository.list("job-policy")

    assert response.status_code == 409
    assert response.json() == {"detail": "Project policy does not allow worktree creation."}
    assert stored.worktree_path is None
    assert len(events) == 3
    assert tuple((runtime / "worktrees").iterdir()) == ()


@pytest.mark.anyio
@pytest.mark.parametrize("conflict_kind", ["path", "branch"])
async def test_existing_target_or_branch_fails_closed(
    tmp_path: Path,
    conflict_kind: str,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-conflict",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-conflict")
            branch, key = identity_for("job-conflict")
            target = runtime / "worktrees" / key
            if conflict_kind == "path":
                outside = tmp_path / "outside"
                outside.mkdir()
                (outside / "marker.txt").write_text("preserve\n", encoding="utf-8")
                target.symlink_to(outside, target_is_directory=True)
            else:
                git(repository, "branch", branch)

            response = await client.post("/api/jobs/job-conflict/worktree")

        stored = api.state.job_repository.get("job-conflict")
        events = api.state.event_repository.list("job-conflict")

    assert response.status_code == 409
    assert stored.worktree_path is None
    assert len(events) == 3
    if conflict_kind == "path":
        assert target.is_symlink()
        assert (target / "marker.txt").read_text(encoding="utf-8") == "preserve\n"
        assert git_result(repository, "show-ref", "--verify", f"refs/heads/{branch}").returncode
    else:
        assert not target.exists()
        assert git(repository, "rev-parse", branch) == git(repository, "rev-parse", "HEAD")


@pytest.mark.anyio
async def test_symlinked_worktree_storage_is_rejected(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-storage",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-storage")
            storage = runtime / "worktrees"
            storage.rmdir()
            outside = tmp_path / "outside"
            outside.mkdir()
            storage.symlink_to(outside, target_is_directory=True)

            response = await client.post("/api/jobs/job-storage/worktree")

        stored = api.state.job_repository.get("job-storage")

    assert response.status_code == 503
    assert response.json() == {"detail": "Worktree storage is unavailable."}
    assert stored.worktree_path is None
    assert tuple(outside.iterdir()) == ()


@pytest.mark.anyio
async def test_persistence_failure_rolls_back_new_worktree_and_branch(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-rollback",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_and_plan(client, "job-rollback")
            head_before = git(repository, "rev-parse", "HEAD")
            status_before = git(repository, "status", "--porcelain=v1")
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER reject_worktree_binding
                    BEFORE UPDATE ON jobs
                    WHEN NEW.worktree_path IS NOT NULL
                    BEGIN
                        SELECT RAISE(ABORT, 'synthetic worktree binding failure');
                    END
                    """
                )

            response = await client.post("/api/jobs/job-rollback/worktree")

        stored = api.state.job_repository.get("job-rollback")
        events = api.state.event_repository.list("job-rollback")

    branch, key = identity_for("job-rollback")
    assert response.status_code == 503
    assert response.json() == {"detail": "Worktree binding could not be recorded."}
    assert "synthetic" not in response.text
    assert stored.worktree_path is None
    assert len(events) == 3
    assert not (runtime / "worktrees" / key).exists()
    assert git(repository, "branch", "--list", branch) == ""
    assert git(repository, "rev-parse", "HEAD") == head_before
    assert git(repository, "status", "--porcelain=v1") == status_before
