from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from test_retention import (
    JOB_ID,
    RETENTION_URL,
    cleanup,
    cleanup_events,
    due,
    scenario,
)
from test_worktree import git

from db import ApprovalValidationError, ApprovalWorkflowConflictError
from engine import EventCreate, JobState, ProcessResult, ProcessTimeoutError
from promotion_target import target_lock
from retention import RetentionConflictError


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tamper",
    [
        "content",
        "extra",
        "ignored",
        "nested",
        "submodule",
        "locked",
        "root",
        "marker",
        "symlink",
        "backlink",
        "binding",
        "missing",
        "head",
        "hardlink",
        "fifo",
    ],
)
async def test_changed_or_unsafe_artifacts_never_create_cleanup_intent(
    tmp_path: Path, tamper: str
) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        if tamper == "content":
            (context.tree / "example.txt").write_text("unreviewed change\n")
        elif tamper == "extra":
            (context.tree / "extra.txt").write_text("keep this\n")
        elif tamper == "ignored":
            (context.project / ".git" / "info" / "exclude").write_text("ignored.txt\n")
            (context.tree / "ignored.txt").write_text("private ignored content\n")
        elif tamper == "nested":
            nested = context.tree / "nested"
            nested.mkdir()
            git(nested, "init", "--quiet")
        elif tamper == "submodule":
            base = git(context.tree, "rev-parse", "HEAD")
            git(context.tree, "update-index", "--add", "--cacheinfo", f"160000,{base},nested")
        elif tamper == "locked":
            git(
                context.project,
                "worktree",
                "lock",
                "--reason",
                "keep for inspection",
                str(context.tree),
            )
        elif tamper in {"root", "symlink"}:
            saved = tmp_path / "saved-tree"
            context.tree.rename(saved)
            if tamper == "symlink":
                context.tree.symlink_to(saved, target_is_directory=True)
            else:
                context.tree.mkdir()
                (context.tree / ".git").write_bytes((saved / ".git").read_bytes())
                (context.tree / "example.txt").write_bytes((saved / "example.txt").read_bytes())
        elif tamper == "marker":
            saved = tmp_path / "saved-marker"
            (context.tree / ".git").rename(saved)
            (context.tree / ".git").write_bytes(saved.read_bytes())
        elif tamper == "backlink":
            admin = Path(git(context.tree, "rev-parse", "--git-dir"))
            (admin / "gitdir").write_text(str(tmp_path / "foreign" / ".git") + "\n")
        elif tamper == "binding":
            with sqlite3.connect(context.api.state.database.path) as connection:
                connection.execute(
                    "UPDATE jobs SET worktree_path = ? WHERE id = ?", (str(context.project), JOB_ID)
                )
        elif tamper == "missing":
            git(context.project, "worktree", "remove", "--force", str(context.tree))
        elif tamper == "head":
            git(context.tree, "add", "example.txt")
            git(
                context.tree,
                "-c",
                "user.name=Test User",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "Changed fixture",
            )
        elif tamper == "hardlink":
            import os

            os.link(context.tree / "example.txt", tmp_path / "linked-content")
        else:
            import os

            os.mkfifo(context.tree / "pipe")
        response = await cleanup(context)
        assert response.status_code == 409, response.text
        assert not cleanup_events(context)
        assert (context.project / "example.txt").read_text() == "initial\n"
        assert git(context.project, "show-ref", "--verify", f"refs/heads/{context.branch}")
        assert str(context.runtime) not in response.text


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["started", "completed"])
async def test_database_audit_failure_never_hides_a_destructive_attempt(
    tmp_path: Path, stage: str
) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        with sqlite3.connect(context.api.state.database.path) as connection:
            connection.execute(
                f"""CREATE TRIGGER reject_cleanup BEFORE INSERT ON events
                WHEN NEW.event_type = 'job.worktree.cleanup.{stage}'
                BEGIN SELECT RAISE(ABORT, 'injected'); END"""
            )
        response = await cleanup(context)
        assert response.status_code == (503 if stage == "started" else 409)
        assert context.tree.exists() is (stage == "started")
        events = cleanup_events(context)
        assert len(events) == (0 if stage == "started" else 1)
        if stage == "completed":
            assert events[0].event_type == "job.worktree.cleanup.started"
            assert (await context.client.get(RETENTION_URL)).json()["status"] == "needs_attention"
            assert (await cleanup(context)).status_code == 409
        with sqlite3.connect(context.api.state.database.path) as connection:
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["nonzero", "timeout", "noop"])
async def test_git_failure_stays_auditable_and_cannot_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    import worktree

    async with scenario(tmp_path) as context:
        due(context)
        original = worktree.run_process
        removals = 0

        async def failed_remove(argv, **kwargs):
            nonlocal removals
            if "remove" in argv:
                removals += 1
                assert argv.count("--force") == 1
                assert kwargs["env"]["GIT_CONFIG_GLOBAL"] == "/dev/null"
                assert "GIT_DIR" not in kwargs["env"]
                if failure == "timeout":
                    raise ProcessTimeoutError("private test output", stderr=b"private")
                return ProcessResult(1 if failure == "nonzero" else 0, b"", b"private")
            return await original(argv, **kwargs)

        monkeypatch.setattr(worktree, "run_process", failed_remove)
        response = await cleanup(context)
        retry = await cleanup(context)
        assert response.status_code == retry.status_code == 409
        assert removals == 1
        assert context.tree.is_dir()
        assert len(cleanup_events(context)) == 1
        assert "private" not in response.text
        assert (await context.client.get(RETENTION_URL)).json()["status"] == "needs_attention"


@pytest.mark.anyio
async def test_interruption_after_intent_survives_restart_without_deletion_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import worktree
    from app import create_app

    async with scenario(tmp_path) as context:
        due(context)
        reached = asyncio.Event()
        original = worktree.run_process

        async def paused_remove(argv, **kwargs):
            if "remove" in argv:
                reached.set()
                await asyncio.Future()
            return await original(argv, **kwargs)

        monkeypatch.setattr(worktree, "run_process", paused_remove)
        pending = asyncio.create_task(cleanup(context))
        await asyncio.wait_for(reached.wait(), 10)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert context.tree.exists()
        assert len(cleanup_events(context)) == 1
    monkeypatch.setattr(worktree, "run_process", original)
    restarted = create_app({"AGENT_WORKBENCH_HOME": str(context.runtime)})
    async with restarted.router.lifespan_context(restarted):
        assert restarted.state.retention_service.read(JOB_ID)["status"] == "needs_attention"
        with pytest.raises(RetentionConflictError, match="uncertain"):
            await restarted.state.retention_service.cleanup(
                JOB_ID, {"retention_event_id": context.event_id}
            )
        assert context.tree.is_dir()
        assert len(cleanup_events(context)) == 1


@pytest.mark.anyio
async def test_cleanup_respects_project_lock_shared_with_promotion(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        with target_lock(context.project, context.api.state.runtime.cache):
            response = await cleanup(context)
        assert response.status_code == 409
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_artifact_reappearing_after_completion_is_never_deleted(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        assert (await cleanup(context)).status_code == 200
        context.tree.mkdir()
        (context.tree / "keep.txt").write_text("new inspection artifact\n")
        data = (await context.client.get(RETENTION_URL)).json()
        assert data["status"] == "needs_attention"
        assert data["completed_event_id"] is not None
        assert data["cleanup_due"] is False
        assert (await cleanup(context)).status_code == 409
        assert (context.tree / "keep.txt").read_text() == "new inspection artifact\n"
        assert len(cleanup_events(context)) == 2


@pytest.mark.anyio
async def test_stale_job_and_non_rejected_audit_guard_cannot_record_intent(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        workflow = context.api.state.approval_workflow_repository
        job = context.api.state.job_repository.get(JOB_ID)
        approval = context.api.state.approval_repository.get(context.approval_id)
        bundle = context.api.state.review_bundle_repository.get(JOB_ID)
        retained = context.api.state.event_repository.get(context.event_id)
        event = EventCreate(JOB_ID, "job.worktree.cleanup.started", {}, "worktree.cleanup.started")
        with pytest.raises(ApprovalValidationError):
            workflow.record_cleanup(
                replace(job, state=JobState.APPROVED), approval, bundle, retained, event
            )
        with pytest.raises(ApprovalWorkflowConflictError, match="changed"):
            workflow.record_cleanup(
                replace(job, model="stale-model"), approval, bundle, retained, event
            )
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_changed_content_during_final_inventory_is_preserved(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        manager = context.api.state.worktree_manager
        original = manager._git
        inventories = 0

        async def changed_inventory(arguments, *, cwd, timeout=20):
            nonlocal inventories
            if arguments[:2] == ("ls-files", "--stage"):
                inventories += 1
                if inventories == 2:
                    (context.tree / "example.txt").write_text("fresh inspection work\n")
            return await original(arguments, cwd=cwd, timeout=timeout)

        manager._git = changed_inventory
        response = await cleanup(context)
        assert response.status_code == 409, response.text
        assert inventories == 2
        assert (context.tree / "example.txt").read_text() == "fresh inspection work\n"
        assert (context.project / "example.txt").read_text() == "initial\n"
        assert len(cleanup_events(context)) == 1
        assert (await context.client.get(RETENTION_URL)).json()["status"] == "needs_attention"


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["ignored", "nested", "content"])
async def test_final_live_review_cannot_dispose_new_inspection_work(
    tmp_path: Path, change: str
) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        review = context.api.state.approval_service
        original = review.assert_live
        inspections = 0

        async def changed_review(job, bundle):
            nonlocal inspections
            await original(job, bundle)
            inspections += 1
            if inspections == 2:
                if change == "ignored":
                    (context.project / ".git" / "info" / "exclude").write_text("ignored.txt\n")
                    (context.tree / "ignored.txt").write_text("fresh inspection work\n")
                elif change == "nested":
                    (context.tree / "nested" / ".git").mkdir(parents=True)
                else:
                    (context.tree / "example.txt").write_text("fresh inspection work\n")

        review.assert_live = changed_review
        response = await cleanup(context)
        assert response.status_code == 409, response.text
        assert inspections == 2
        assert context.tree.is_dir()
        assert (context.project / "example.txt").read_text() == "initial\n"
        assert len(cleanup_events(context)) == 1
        assert (await context.client.get(RETENTION_URL)).json()["status"] == "needs_attention"


@pytest.mark.anyio
@pytest.mark.parametrize("value", [None, "2030-01-01", datetime(2030, 1, 1)])
async def test_invalid_clock_fails_closed(tmp_path: Path, value: object) -> None:
    async with scenario(tmp_path) as context:
        context.api.state.retention_service._clock = lambda: value
        response = await cleanup(context)
        assert response.status_code == 503
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_hash_valid_but_semantically_corrupt_deadline_fails_closed(tmp_path: Path) -> None:
    import hashlib

    async with scenario(tmp_path) as context:
        retained = context.api.state.event_repository.get(context.event_id)
        payload = dict(retained.payload)
        payload["retain_until"] = "2000-01-01T00:00:00.000000Z"
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with sqlite3.connect(context.api.state.database.path) as connection:
            connection.execute("DROP TRIGGER events_prevent_update")
            connection.execute(
                "UPDATE events SET payload = ?, payload_hash = ? WHERE id = ?",
                (canonical, hashlib.sha256(canonical.encode()).hexdigest(), retained.id),
            )
        due(context)
        assert (await cleanup(context)).status_code == 409
        assert context.tree.exists()
        assert not cleanup_events(context)
