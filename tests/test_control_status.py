from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from test_workspace import stored_job, workspace

from db import JobRepository, JobRepositoryError, ProjectRepository
from engine import JobCreate, JobRuntime, JobState, ProjectConfig


@pytest.mark.anyio
async def test_control_counts_all_tasks_without_exposing_content(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, client):
        jobs = api.state.job_repository
        for n in range(61):
            jobs.create(stored_job(f"alpha-{n}"))
        jobs.create(
            JobCreate(
                id="needs-review",
                project_id="alpha",
                request="Do not send this secret request.",
                request_snapshot={"token": "fixture credential"},
                runtime=JobRuntime.CLAUDE,
                state=JobState.WAITING_APPROVAL,
            )
        )
        jobs.create(stored_job("other-project", "beta"))
        result = await client.get("/api/control", params={"project_id": "alpha"})
        data = result.json()
        assert result.status_code == 200
        assert data["total"] == 62
        assert data["needs_attention"] == 1
        assert data["states"] == {"created": 61, "waiting_approval": 1}
        assert {item["runtime"]: item["total"] for item in data["runtimes"]} == {
            "auto": 0,
            "claude": 1,
            "codex": 61,
            "local": 0,
        }
        assert data["capabilities"] == {"native_execution": False, "remote_actions": False}
        assert data["quota"] == {"status": "unknown", "remaining_percent": None}
        assert data["basis"] == "recorded_job_states"
        assert data["sensitivity"] == "private"
        for forbidden in ("secret", "credential", "snapshot", "worktree", str(tmp_path)):
            assert forbidden not in result.text
        assert result.headers["cache-control"] == "no-store"
        assert jobs.get("needs-review").state is JobState.WAITING_APPROVAL


@pytest.mark.anyio
async def test_control_empty_scope_removed_scope_and_loopback_boundary(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, client):
        result = await client.get("/api/control?project_id=alpha")
        assert result.json()["total"] == result.json()["needs_attention"] == 0
        assert result.json()["states"] == {}
        ProjectRepository(api.state.database).register(
            ProjectConfig.from_dict({"id": "removed", "root": str(tmp_path / "removed")})
        )
        for project_id in ("removed", "missing", "alpha' OR 1=1"):
            result = await client.get("/api/control", params={"project_id": project_id})
            assert result.status_code == 404
        assert (await client.get("/api/control")).status_code == 422
        for headers in ({"Origin": "https://foreign.invalid"}, {"Host": "foreign.invalid"}):
            result = await client.get("/api/control?project_id=alpha", headers=headers)
            assert result.status_code == 403
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api, client=("192.0.2.8", 1234)),
            base_url="http://localhost",
        ) as remote:
            assert (await remote.get("/api/control?project_id=alpha")).status_code == 403


@pytest.mark.anyio
async def test_control_database_errors_do_not_leak_details(tmp_path: Path, monkeypatch) -> None:
    def broken(*args):
        raise JobRepositoryError("private database path and credential")

    async with workspace(tmp_path) as (_, client):
        monkeypatch.setattr(JobRepository, "count_by_runtime_and_state", broken)
        result = await client.get("/api/control?project_id=alpha")
        assert result.status_code == 503
        assert result.json() == {"detail": "Control status is unavailable."}


@pytest.mark.anyio
async def test_attention_covers_each_actionable_state(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, client):
        for state in JobState:
            api.state.job_repository.create(
                JobCreate(
                    id=state.value,
                    project_id="alpha",
                    request="Fixture",
                    request_snapshot={},
                    runtime=JobRuntime.AUTO,
                    state=state,
                )
            )
        before = api.state.job_repository.list()
        data = (await client.get("/api/control?project_id=alpha")).json()
        assert data["total"] == len(JobState)
        assert data["needs_attention"] == 5
        assert api.state.job_repository.list() == before
        assert "request" not in json.dumps(data)
