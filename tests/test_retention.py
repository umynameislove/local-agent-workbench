from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from test_worktree import git, initialize_repository, write_config

from app import create_app
from engine import JobState, JobUpdate

NOW = datetime(2030, 1, 1, 12, 30, 0, 123456, tzinfo=UTC)
JOB_ID = "retained-job"
RETENTION_URL = f"/api/jobs/{JOB_ID}/retention"
CLEANUP_URL = f"/api/jobs/{JOB_ID}/cleanup"


@asynccontextmanager
async def scenario(tmp_path: Path, *, decision: str = "rejected", days: int = 7):
    project = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, project)
    config = json.loads((runtime / "config.json").read_text())
    config["retention"] = {"rejected_worktree_days": days}
    (runtime / "config.json").write_text(json.dumps(config))
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)}, job_id_factory=lambda: JOB_ID)
    async with api.router.lifespan_context(api):
        api.state.approval_service._clock = lambda: NOW
        api.state.approval_workflow_repository._approvals.clock = lambda: NOW
        api.state.retention_service._clock = lambda: NOW
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            created = await client.post(
                "/api/jobs", json={"project_id": "sample-project", "request": "Make a safe change."}
            )
            assert created.status_code == 201, created.text
            assert (await client.post(f"/api/jobs/{JOB_ID}/plan")).status_code == 200
            assert (await client.post(f"/api/jobs/{JOB_ID}/worktree")).status_code == 200
            job = api.state.job_repository.get(JOB_ID)
            worktree = Path(job.worktree_path)
            branch = git(worktree, "symbolic-ref", "--short", "HEAD")
            (worktree / "example.txt").write_text("reviewed change\n")
            api.state.job_repository.update(
                JobUpdate(job.id, JobState.VERIFYING, job.runtime, job.model, job.worktree_path)
            )
            assert (await client.post(f"/api/jobs/{JOB_ID}/review-readiness")).status_code == 200
            bundled = await client.post(f"/api/jobs/{JOB_ID}/review-bundle")
            assert bundled.status_code == 200, bundled.text
            digest = bundled.json()["bundle_hash"]
            requested = await client.post(
                f"/api/jobs/{JOB_ID}/approval-request", json={"bundle_hash": digest}
            )
            assert requested.status_code == 200, requested.text
            approval_id = requested.json()["approval_id"]
            resolved = await client.post(
                f"/api/approvals/{approval_id}", json={"decision": decision, "bundle_hash": digest}
            )
            assert resolved.status_code == 200, resolved.text
            records = api.state.event_repository.list(JOB_ID)
            retained = next(
                (event for event in records if event.event_type == "job.worktree.retained"), None
            )
            yield SimpleNamespace(
                api=api,
                client=client,
                project=project,
                runtime=runtime,
                tree=worktree,
                branch=branch,
                digest=digest,
                approval_id=approval_id,
                event_id=None if retained is None else retained.id,
                deadline=NOW + timedelta(days=days),
            )


def due(context: SimpleNamespace) -> None:
    context.api.state.retention_service._clock = lambda: context.deadline


async def cleanup(context: SimpleNamespace) -> httpx.Response:
    return await context.client.post(CLEANUP_URL, json={"retention_event_id": context.event_id})


def cleanup_events(context: SimpleNamespace) -> tuple:
    return tuple(
        event
        for event in context.api.state.event_repository.list(JOB_ID)
        if event.event_type.startswith("job.worktree.cleanup.")
    )


@pytest.mark.anyio
async def test_reject_keeps_artifact_and_returns_private_inspection_deadline(
    tmp_path: Path,
) -> None:
    async with scenario(tmp_path, days=2) as context:
        response = await context.client.get(RETENTION_URL)
        data = response.json()
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert data["status"] == "retained"
        assert data["retain_until"] == "2030-01-03T12:30:00.123456Z"
        assert data["retention_event_id"] == context.event_id
        assert data["cleanup_due"] is False
        assert context.tree.is_dir()
        assert (context.tree / "example.txt").read_text() == "reviewed change\n"
        assert (context.project / "example.txt").read_text() == "initial\n"
        assert str(context.runtime) not in response.text
        assert str(context.project) not in response.text
        assert not cleanup_events(context)


@pytest.mark.anyio
@pytest.mark.parametrize("offset", [-1, -86400, -86400 * 30])
async def test_before_deadline_and_backwards_clock_never_dispose(
    tmp_path: Path, offset: int
) -> None:
    async with scenario(tmp_path) as context:
        context.api.state.retention_service._clock = lambda: (
            context.deadline + timedelta(seconds=offset)
        )
        response = await cleanup(context)
        assert response.status_code == 409
        assert context.tree.is_dir()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_due_cleanup_removes_exact_artifact_and_retains_source_branch_and_history(
    tmp_path: Path,
) -> None:
    async with scenario(tmp_path) as context:
        job = context.api.state.job_repository.get(JOB_ID)
        base = git(context.project, "rev-parse", "HEAD")
        (context.project / "example.txt").write_text("user staged edit\n")
        git(context.project, "add", "example.txt")
        (context.project / "example.txt").write_text("user unstaged edit\n")
        before = git(context.project, "diff", "--cached")
        due(context)
        assert (await context.client.get(RETENTION_URL)).json()["status"] == "eligible"
        first = await cleanup(context)
        retry = await cleanup(context)
        assert first.status_code == retry.status_code == 200, first.text
        assert first.json() == retry.json()
        assert first.json()["status"] == "cleaned"
        assert first.json()["cleanup_due"] is False
        assert not context.tree.exists()
        assert str(context.tree) not in git(context.project, "worktree", "list", "--porcelain")
        assert git(context.project, "rev-parse", f"refs/heads/{context.branch}") == base
        assert (context.project / "example.txt").read_text() == "user unstaged edit\n"
        assert git(context.project, "diff", "--cached") == before
        assert git(context.project, "rev-parse", "HEAD") == base
        assert context.api.state.job_repository.get(JOB_ID) == job
        assert context.api.state.review_bundle_repository.get(JOB_ID).payload_hash == context.digest
        events = cleanup_events(context)
        assert [event.event_type for event in events] == [
            "job.worktree.cleanup.started",
            "job.worktree.cleanup.completed",
        ]
        assert events[1].payload["started_event_id"] == events[0].id
        assert str(context.tree) not in json.dumps([event.payload for event in events])


@pytest.mark.anyio
async def test_restart_and_config_change_do_not_dispose_or_rewrite_deadline(tmp_path: Path) -> None:
    async with scenario(tmp_path, days=10) as context:
        before = (await context.client.get(RETENTION_URL)).json()
        config = json.loads((context.runtime / "config.json").read_text())
        config["retention"]["rejected_worktree_days"] = 1
        (context.runtime / "config.json").write_text(json.dumps(config))
    restarted = create_app({"AGENT_WORKBENCH_HOME": str(context.runtime)})
    async with restarted.router.lifespan_context(restarted):
        restarted.state.retention_service._clock = lambda: NOW + timedelta(days=2)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://localhost"
        ) as client:
            response = await client.get(RETENTION_URL)
            assert response.json() == before
            retry = await client.post(
                f"/api/approvals/{context.approval_id}",
                json={"decision": "rejected", "bundle_hash": context.digest},
            )
            assert retry.status_code == 200
            assert (await client.get(RETENTION_URL)).json() == before
        assert context.tree.is_dir()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        [],
        {"retention_event_id": True},
        {"retention_event_id": 0},
        {"retention_event_id": "1"},
        {"retention_event_id": 1.5},
        {"retention_event_id": 1, "force": True},
    ],
)
async def test_cleanup_body_cannot_supply_paths_or_override_policy(
    tmp_path: Path, body: object
) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        response = await context.client.post(CLEANUP_URL, json=body)
        assert response.status_code == 422
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_wrong_displayed_event_and_missing_job_refuse_cleanup(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        wrong = await context.client.post(
            CLEANUP_URL, json={"retention_event_id": context.event_id + 1}
        )
        missing = await context.client.post(
            "/api/jobs/missing/cleanup", json={"retention_event_id": 1}
        )
        assert wrong.status_code == 409
        assert missing.status_code == 404
        assert (await context.client.get("/api/jobs/missing/retention")).status_code == 404
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
@pytest.mark.parametrize("decision", ["approved", "changes_requested"])
async def test_non_rejected_jobs_cannot_be_cleaned(tmp_path: Path, decision: str) -> None:
    async with scenario(tmp_path, decision=decision) as context:
        response = await context.client.post(CLEANUP_URL, json={"retention_event_id": 1})
        assert response.status_code == 409
        assert (await context.client.get(RETENTION_URL)).status_code == 409
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_legacy_rejections_are_retained_indefinitely(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        with sqlite3.connect(context.api.state.database.path) as connection:
            connection.execute("DROP TRIGGER events_prevent_delete")
            connection.execute("DELETE FROM events WHERE id = ?", (context.event_id,))
        due(context)
        data = (await context.client.get(RETENTION_URL)).json()
        assert data["status"] == "legacy_retained"
        assert data["cleanup_due"] is False
        assert data["retain_until"] is None
        assert (await cleanup(context)).status_code == 409
        assert context.tree.exists()
        assert not cleanup_events(context)


@pytest.mark.anyio
async def test_concurrent_calls_share_one_disposal_and_completed_retry(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        due(context)
        responses = await asyncio.gather(*(cleanup(context) for _ in range(4)))
        assert sorted(response.status_code for response in responses) == [200, 409, 409, 409]
        assert len(cleanup_events(context)) == 2
        assert (await cleanup(context)).status_code == 200


@pytest.mark.anyio
@pytest.mark.parametrize("endpoint", [RETENTION_URL, CLEANUP_URL])
@pytest.mark.parametrize("foreign", ["host", "origin", "client"])
async def test_retention_endpoints_require_local_host_client_and_origin(
    tmp_path: Path, endpoint: str, foreign: str
) -> None:
    async with scenario(tmp_path) as context:
        headers = {"origin": "https://foreign.example"} if foreign == "origin" else {}
        transport = httpx.ASGITransport(
            app=context.api, client=("192.0.2.1" if foreign == "client" else "127.0.0.1", 5000)
        )
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://foreign.example" if foreign == "host" else "http://localhost",
        ) as client:
            response = await client.request(
                "GET" if endpoint == RETENTION_URL else "POST",
                endpoint,
                headers=headers,
                json={"retention_event_id": context.event_id},
            )
        assert response.status_code == 403
        assert context.tree.exists()
        assert not cleanup_events(context)
