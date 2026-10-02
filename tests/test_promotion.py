from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from engine import (
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProcessResult,
    ProjectConfig,
    RecoveryAction,
    Sensitivity,
)


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()


def fixture(api: object, tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "project"
    project.mkdir()
    git(project, "init", "--quiet")
    (project / "changed.txt").write_text("before\n", encoding="utf-8")
    (project / "removed.txt").write_text("remove me\n", encoding="utf-8")
    git(project, "add", ".")
    git(
        project,
        "-c",
        "user.name=Test User",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Fixture",
    )
    base = git(project, "rev-parse", "HEAD")
    identity = hashlib.sha256(b"job-001").hexdigest()[:32]
    worktree = tmp_path / "runtime" / "worktrees" / f"job-{identity}"
    git(project, "worktree", "add", "--quiet", "-b", f"law/job-{identity}", str(worktree), base)
    api.state.project_repository.create(
        ProjectConfig(
            id="alpha",
            root=str(project),
            sensitivity=Sensitivity.PRIVATE,
            cloud_allowed=False,
            permission_mode=PermissionMode.SANDBOXED_WRITE,
        )
    )
    api.state.job_repository.create(
        JobCreate(
            id="job-001",
            project_id="alpha",
            request="Apply the reviewed local patch.",
            request_snapshot={
                "repo_head": base,
                "project": {
                    "id": "alpha",
                    "root": str(project),
                    "sensitivity": "private",
                    "cloud_allowed": False,
                    "permission_mode": "sandboxed-write",
                },
            },
            state=JobState.VERIFYING,
            runtime=JobRuntime.CODEX,
            model="fixture-model",
            worktree_path=str(worktree),
        )
    )
    (worktree / "changed.txt").write_text("after\n", encoding="utf-8")
    (worktree / "removed.txt").unlink()
    (worktree / "added.txt").write_text("new file\n", encoding="utf-8")
    return project, worktree


async def approved(client: httpx.AsyncClient) -> tuple[str, str]:
    readiness = await client.post("/api/jobs/job-001/review-readiness")
    assert readiness.status_code == 200, readiness.text
    bundled = await client.post("/api/jobs/job-001/review-bundle")
    assert bundled.status_code == 200, bundled.text
    bundle_hash = bundled.json()["bundle_hash"]
    requested = await client.post(
        "/api/jobs/job-001/approval-request", json={"bundle_hash": bundle_hash}
    )
    assert requested.status_code == 200, requested.text
    approval_id = requested.json()["approval_id"]
    decided = await client.post(
        f"/api/approvals/{approval_id}",
        json={"decision": "approved", "bundle_hash": bundle_hash},
    )
    assert decided.status_code == 200, decided.text
    return approval_id, bundle_hash


async def promote(client: httpx.AsyncClient, approval_id: str, bundle_hash: str):
    return await client.post(
        f"/api/approvals/{approval_id}/promote", json={"bundle_hash": bundle_hash}
    )


@pytest.mark.anyio
async def test_promotes_only_approved_patch_without_commit_or_push(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, worktree = fixture(api, tmp_path)
        base = git(project, "rev-parse", "HEAD")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 200, response.text
        assert response.json()["job_state"] == "completed"
        assert api.state.job_repository.get("job-001").state is JobState.COMPLETED
        assert (project / "changed.txt").read_text(encoding="utf-8") == "after\n"
        assert (project / "added.txt").read_text(encoding="utf-8") == "new file\n"
        assert not (project / "removed.txt").exists()
        assert (worktree / "changed.txt").read_text(encoding="utf-8") == "after\n"
        assert git(project, "rev-parse", "HEAD") == base
        assert git(project, "diff", "--cached", "--name-only") == ""
        assert [event.event_type for event in api.state.event_repository.list("job-001")][-2:] == [
            "job.promotion.started",
            "job.promotion.completed",
        ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tamper", ["source", "target", "head", "hash", "branch", "policy", "symlink"]
)
async def test_preflight_rejects_changed_evidence_without_target_write(
    tmp_path: Path, tamper: str
) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, worktree = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            if tamper == "source":
                (worktree / "changed.txt").write_text("different\n", encoding="utf-8")
            elif tamper == "target":
                (project / "changed.txt").write_text("user data\n", encoding="utf-8")
            elif tamper == "head":
                (project / "other.txt").write_text("new commit\n", encoding="utf-8")
                git(project, "add", "other.txt")
                git(
                    project,
                    "-c",
                    "user.name=Test User",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "Concurrent commit",
                )
            elif tamper == "branch":
                git(worktree, "branch", "-m", "rebound-branch")
            elif tamper == "symlink":
                (project / "changed.txt").unlink()
                (project / "changed.txt").symlink_to(worktree / "changed.txt")
            elif tamper == "policy":
                api.state.project_repository.update(
                    ProjectConfig(
                        id="alpha",
                        root=str(project),
                        sensitivity=Sensitivity.PRIVATE,
                        cloud_allowed=False,
                        permission_mode=PermissionMode.READ_ONLY,
                    )
                )
            else:
                bundle_hash = "0" * 64
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409, response.text
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert not (project / "added.txt").exists()
        assert (project / "removed.txt").exists()
        assert not any(
            event.event_type == "job.promotion.started"
            for event in api.state.event_repository.list("job-001")
        )


@pytest.mark.anyio
async def test_failed_apply_requires_reconciliation_and_never_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import promotion_target

    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            original = promotion_target.run_process

            async def fail_apply(argv, **kwargs):
                if "apply" in argv and "--check" not in argv:
                    (project / "changed.txt").write_text("partial\n", encoding="utf-8")
                    return ProcessResult(1, b"", b"failed")
                return await original(argv, **kwargs)

            monkeypatch.setattr(promotion_target, "run_process", fail_apply)
            failed = await promote(client, approval_id, bundle_hash)
            retry = await promote(client, approval_id, bundle_hash)
        assert failed.status_code == retry.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPLYING
        assert (project / "changed.txt").read_text(encoding="utf-8") == "partial\n"
        assert [event.event_type for event in api.state.event_repository.list("job-001")].count(
            "job.promotion.started"
        ) == 1
        assert not any(
            event.event_type == "job.promotion.completed"
            for event in api.state.event_repository.list("job-001")
        )


@pytest.mark.anyio
async def test_durable_start_failure_never_calls_apply(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            with sqlite3.connect(api.state.runtime.state_db) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER refuse_promotion BEFORE INSERT ON events
                    WHEN NEW.event_type = 'job.promotion.started'
                    BEGIN SELECT RAISE(ABORT, 'injected failure'); END
                    """
                )
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 503
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"
        assert not (project / "added.txt").exists()


@pytest.mark.anyio
async def test_staged_index_change_blocks_promotion_even_with_clean_files(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            (project / "changed.txt").write_text("staged content\n", encoding="utf-8")
            git(project, "add", "changed.txt")
            (project / "changed.txt").write_text("before\n", encoding="utf-8")
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert git(project, "diff", "--cached", "--name-only") == "changed.txt"
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"


@pytest.mark.anyio
async def test_ignored_existing_file_cannot_be_overwritten(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            (project / ".git" / "info" / "exclude").write_text("added.txt\n", encoding="utf-8")
            (project / "added.txt").write_text("private existing data\n", encoding="utf-8")
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert (project / "added.txt").read_text(encoding="utf-8") == "private existing data\n"


@pytest.mark.anyio
async def test_concurrent_retry_applies_once(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            results = await asyncio.gather(
                promote(client, approval_id, bundle_hash),
                promote(client, approval_id, bundle_hash),
            )
        assert sorted(response.status_code for response in results) == [200, 409]
        assert (project / "changed.txt").read_text(encoding="utf-8") == "after\n"
        events = api.state.event_repository.list("job-001")
        assert [event.event_type for event in events].count("job.promotion.started") == 1
        assert [event.event_type for event in events].count("job.promotion.completed") == 1


@pytest.mark.anyio
async def test_post_apply_extra_change_is_not_claimed_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import promotion_target

    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            original = promotion_target.run_process

            async def add_extra_file(argv, **kwargs):
                result = await original(argv, **kwargs)
                if "apply" in argv and "--check" not in argv and result.returncode == 0:
                    (project / "unreviewed.txt").write_text("unreviewed\n", encoding="utf-8")
                return result

            monkeypatch.setattr(promotion_target, "run_process", add_extra_file)
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPLYING
        assert (project / "unreviewed.txt").exists()
        assert not any(
            event.event_type == "job.promotion.completed"
            for event in api.state.event_repository.list("job-001")
        )


@pytest.mark.anyio
async def test_completed_event_failure_keeps_reconciliation_state(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            with sqlite3.connect(api.state.runtime.state_db) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER refuse_completion BEFORE INSERT ON events
                    WHEN NEW.event_type = 'job.promotion.completed'
                    BEGIN SELECT RAISE(ABORT, 'injected failure'); END
                    """
                )
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPLYING
        assert (project / "changed.txt").read_text(encoding="utf-8") == "after\n"
        assert not any(
            event.event_type == "job.promotion.completed"
            for event in api.state.event_repository.list("job-001")
        )


@pytest.mark.anyio
async def test_promotion_rejects_cross_origin_client(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            response = await client.post(
                f"/api/approvals/{approval_id}/promote",
                json={"bundle_hash": bundle_hash},
                headers={"origin": "https://untrusted.example"},
            )
        assert response.status_code == 403
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"


@pytest.mark.anyio
async def test_external_project_lock_blocks_before_writing(tmp_path: Path) -> None:
    from promotion_target import target_lock

    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            with target_lock(project, api.state.runtime.cache):
                response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 409
        assert api.state.job_repository.get("job-001").state is JobState.APPROVED
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"


@pytest.mark.anyio
async def test_git_rename_is_promoted_from_reviewed_patch(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, worktree = fixture(api, tmp_path)
        (worktree / "removed.txt").write_text("remove me\n", encoding="utf-8")
        git(worktree, "mv", "removed.txt", "moved.txt")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            response = await promote(client, approval_id, bundle_hash)
        assert response.status_code == 200, response.text
        assert not (project / "removed.txt").exists()
        assert (project / "moved.txt").read_text(encoding="utf-8") == "remove me\n"


@pytest.mark.anyio
async def test_restart_does_not_retry_uncertain_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import promotion_target

    runtime = tmp_path / "runtime"
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            approval_id, bundle_hash = await approved(client)
            original = promotion_target.run_process

            async def fail_apply(argv, **kwargs):
                if "apply" in argv and "--check" not in argv:
                    return ProcessResult(1, b"", b"failed")
                return await original(argv, **kwargs)

            monkeypatch.setattr(promotion_target, "run_process", fail_apply)
            response = await promote(client, approval_id, bundle_hash)
            assert response.status_code == 409
    monkeypatch.setattr(promotion_target, "run_process", original)

    restarted = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted.router.lifespan_context(restarted):
        assert restarted.state.recovery_items[0].action is RecoveryAction.RECONCILE_IN_FLIGHT
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://localhost"
        ) as client:
            retry = await promote(client, approval_id, bundle_hash)
        assert retry.status_code == 409
        assert restarted.state.job_repository.get("job-001").state is JobState.APPLYING
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"


@pytest.mark.anyio
async def test_promote_requires_valid_approved_binding(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        project, _ = fixture(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            readiness = await client.post("/api/jobs/job-001/review-readiness")
            assert readiness.status_code == 200
            bundle = await client.post("/api/jobs/job-001/review-bundle")
            bundle_hash = bundle.json()["bundle_hash"]
            requested = await client.post(
                "/api/jobs/job-001/approval-request", json={"bundle_hash": bundle_hash}
            )
            approval_id = requested.json()["approval_id"]
            pending = await promote(client, approval_id, bundle_hash)
            rejected = await client.post(
                f"/api/approvals/{approval_id}",
                json={"decision": "rejected", "bundle_hash": bundle_hash},
            )
            assert rejected.status_code == 200
            denied = await promote(client, approval_id, bundle_hash)
            invalid = await client.post(
                f"/api/approvals/{approval_id}/promote", json={"unexpected": "value"}
            )
        assert pending.status_code == denied.status_code == 409
        assert invalid.status_code == 422
        assert api.state.job_repository.get("job-001").state is JobState.REJECTED
        assert (project / "changed.txt").read_text(encoding="utf-8") == "before\n"
