from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from test_retention import CLEANUP_URL, JOB_ID, due, scenario

from app import create_app
from db import JobRepository, JobValidationError, ProjectRepository
from engine import EventCreate, JobCreate, JobRuntime, ProjectConfig
from workspace_ui import asset


def repository(path: Path) -> Path:
    path.mkdir()
    commands = [
        ("init", "--quiet"),
        (
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "Workspace fixture",
        ),
    ]
    for command in commands:
        subprocess.run(("git", "-C", str(path), *command), check=True, capture_output=True)
    return path


@asynccontextmanager
async def workspace(tmp_path: Path, *, configured: bool = True):
    runtime = tmp_path / "runtime"
    if configured:
        runtime.mkdir()
        config = json.loads(Path("config.example.json").read_text())
        config["projects"][0]["id"] = "alpha"
        config["projects"][0]["root"] = str(repository(tmp_path / "alpha"))
        second = dict(config["projects"][0], id="beta", root=str(repository(tmp_path / "beta")))
        config["projects"].append(second)
        (runtime / "config.json").write_text(json.dumps(config))
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client,
    ):
        yield api, client


def stored_job(job_id: str, project: str = "alpha") -> JobCreate:
    return JobCreate(
        id=job_id,
        project_id=project,
        request="A task with <script>untrusted text</script>.",
        request_snapshot={"private": "internal context"},
        runtime=JobRuntime.CODEX,
        worktree_path="/runtime/private/worktree",
    )


@pytest.mark.anyio
async def test_workspace_assets_have_local_csp_and_no_external_dependencies(tmp_path: Path) -> None:
    async with workspace(tmp_path, configured=False) as (_, client):
        response = await client.get("/")
        script = await client.get("/workspace.js")
        bootstrap = await client.get("/api/bootstrap")
        unknown = await client.get("/workspace_ui/index.html")
    assert response.status_code == script.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert script.headers["content-type"].startswith("application/javascript")
    assert bootstrap.json()["projects"] == []
    assert unknown.status_code == 404
    style = response.text.split("<style>")[1].split("</style>")[0]
    digest = base64.b64encode(hashlib.sha256(style.encode()).digest()).decode()
    csp = response.headers["content-security-policy"]
    assert f"style-src 'sha256-{digest}'" in csp
    assert "script-src 'self'" in csp
    assert "connect-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "unsafe-inline" not in csp
    assert 'src="/workspace.js"' in response.text
    for value in ("https://", "innerHTML", "localStorage", "sessionStorage", "indexedDB"):
        assert value not in response.text + script.text
    for result in (response, script, bootstrap, unknown):
        assert result.headers["cache-control"] == "no-store"
        assert result.headers["x-content-type-options"] == "nosniff"
        assert result.headers["referrer-policy"] == "no-referrer"
    with pytest.raises(ValueError, match="Unknown workspace"):
        asset("../app.py")


@pytest.mark.anyio
@pytest.mark.parametrize("path", ["/", "/workspace.js", "/api/bootstrap", "/api/jobs", "/docs"])
@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "attacker.invalid"},
        {"Host": "attacker.invalid", "Sec-Fetch-Site": "same-origin"},
        {"Origin": "https://attacker.invalid"},
        {"Origin": "null"},
        {"Sec-Fetch-Site": "cross-site"},
    ],
)
async def test_every_http_view_rejects_foreign_browser_context(
    tmp_path: Path, path: str, headers: dict[str, str]
) -> None:
    async with workspace(tmp_path, configured=False) as (_, client):
        response = await client.get(path, headers=headers)
    assert response.status_code == 403
    assert response.headers["cache-control"] == "no-store"
    assert str(tmp_path) not in response.text


@pytest.mark.anyio
async def test_local_boundary_accepts_local_tools_and_rejects_remote_clients(
    tmp_path: Path,
) -> None:
    async with workspace(tmp_path, configured=False) as (api, client):
        for host in ("127.0.0.1:8765", "localhost", "[::1]:8765"):
            response = await client.get("/api/health", headers={"Host": host})
            assert response.status_code == 200
        same = await client.get("/api/health", headers={"Origin": "http://localhost"})
        assert same.status_code == 200
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api, client=("192.0.2.8", 1234)),
            base_url="http://localhost",
        ) as remote:
            assert (await remote.get("/api/bootstrap")).status_code == 403


@pytest.mark.anyio
async def test_foreign_mutations_cannot_create_jobs_or_worktrees(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, client):
        for path, payload in (
            ("/api/jobs", {"project_id": "alpha", "request": "Must not create"}),
            ("/api/jobs/missing/plan", {}),
            ("/api/jobs/missing/worktree", {}),
        ):
            response = await client.post(
                path, json=payload, headers={"Origin": "https://attacker.invalid"}
            )
            assert response.status_code == 403
        assert api.state.job_repository.list() == ()
        assert list(api.state.runtime.worktrees.iterdir()) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "headers", [{"Origin": "https://attacker.invalid"}, {"Sec-Fetch-Site": "cross-site"}]
)
async def test_existing_review_and_retention_controls_remain_guarded(
    tmp_path: Path, headers: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async with workspace(tmp_path, configured=False) as (api, client):

        def forbidden(*args, **kwargs):
            raise AssertionError("Foreign requests must stop before calling a service.")

        for name, method in (
            ("approval_service", "request"),
            ("approval_service", "decide"),
            ("promotion_service", "promote"),
            ("retention_service", "read"),
            ("retention_service", "cleanup"),
        ):
            monkeypatch.setattr(getattr(api.state, name), method, forbidden)
        for method, path in (
            ("POST", "/api/jobs/missing/approval-request"),
            ("POST", "/api/approvals/missing"),
            ("POST", "/api/approvals/missing/promote"),
            ("GET", "/api/jobs/missing/retention"),
            ("POST", "/api/jobs/missing/cleanup"),
        ):
            response = await client.request(method, path, headers=headers, json={})
            assert response.status_code == 403
            assert response.headers["cache-control"] == "no-store"
        assert api.state.job_repository.list() == ()


@pytest.mark.anyio
async def test_task_views_preserve_rejected_history_after_retention_cleanup(tmp_path: Path) -> None:
    async with scenario(tmp_path) as context:
        before = (await context.client.get(f"/api/jobs/{JOB_ID}")).json()
        assert before["state"] == "rejected"
        assert before["worktree_ready"] is True
        due(context)
        cleaned = await context.client.post(
            CLEANUP_URL, json={"retention_event_id": context.event_id}
        )
        assert cleaned.status_code == 200, cleaned.text
        assert not context.tree.exists()
        after = (await context.client.get(f"/api/jobs/{JOB_ID}")).json()
        assert after == before
        assert str(context.runtime) not in json.dumps(after)
        assert (await context.client.get("/")).status_code == 200
        page = await context.client.get("/api/jobs?project_id=sample-project")
        assert page.json()["jobs"][0]["id"] == JOB_ID
        assert page.json()["jobs"][0]["state"] == "rejected"
        assert (await context.client.get(f"/api/jobs/{JOB_ID}/retention")).json()["status"] == (
            "cleaned"
        )


@pytest.mark.anyio
async def test_task_views_are_project_scoped_bounded_and_omit_internal_context(
    tmp_path: Path,
) -> None:
    async with workspace(tmp_path) as (api, client):
        jobs: JobRepository = api.state.job_repository
        for job_id in ("job-a", "job-b", "job-c"):
            jobs.create(stored_job(job_id))
        jobs.create(stored_job("beta-job", "beta"))
        with sqlite3.connect(api.state.database.path) as connection:
            connection.execute("UPDATE jobs SET created_at = '2026-01-01T00:00:00Z'")
        first = await client.get("/api/jobs", params={"project_id": "alpha", "limit": 2})
        assert [item["id"] for item in first.json()["jobs"]] == ["job-c", "job-b"]
        second = await client.get(
            "/api/jobs",
            params={
                "project_id": "alpha",
                "limit": 2,
                "before_id": first.json()["next_cursor"],
            },
        )
        assert [item["id"] for item in second.json()["jobs"]] == ["job-a"]
        assert second.json()["next_cursor"] is None
        detail = await client.get("/api/jobs/job-a")
        assert detail.json()["request"] == stored_job("job-a").request
        assert detail.json()["worktree_ready"] is True
        assert detail.json()["runtime"] == "codex"
        for response in (first, second, detail):
            assert "request_snapshot" not in response.text
            assert "worktree_path" not in response.text
            assert "/runtime/private/" not in response.text
            assert "internal context" not in response.text
            assert str(tmp_path) not in response.text
        assert "request" not in first.json()["jobs"][0]
        for query in (
            {"project_id": "alpha", "limit": 0},
            {"project_id": "alpha", "limit": 201},
            {"project_id": "alpha", "before_id": "beta-job"},
            {"project_id": "alpha", "before_id": "unknown"},
        ):
            assert (await client.get("/api/jobs", params=query)).status_code == 422
        assert (await client.get("/api/jobs", params={"project_id": "unknown"})).status_code == 404
        assert (await client.get("/api/jobs/missing")).status_code == 404
        inactive = ProjectConfig.from_dict({"id": "inactive", "root": "/workspace/inactive"})
        ProjectRepository(api.state.database).create(inactive)
        jobs.create(stored_job("inactive-job", "inactive"))
        assert (await client.get("/api/jobs/inactive-job")).status_code == 404


@pytest.mark.anyio
async def test_browser_shell_flow_persists_demo_plan_and_one_worktree(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, client):
        created = await client.post(
            "/api/jobs",
            json={
                "project_id": "alpha",
                "request": "Inspect the retry logic.\nKeep the change bounded.",
                "runtime": "codex",
            },
        )
        assert created.status_code == 201
        job_id = created.json()["id"]
        assert (await client.get(f"/api/jobs/{job_id}")).json()["state"] == "created"
        assert api.state.event_repository.list(job_id) == ()
        plan = await client.post(f"/api/jobs/{job_id}/plan")
        repeat_plan = await client.post(f"/api/jobs/{job_id}/plan")
        assert plan.json() == repeat_plan.json()
        assert plan.json()["plan"]["source"] == "demo"
        assert plan.json()["state"] == "queued"
        prepared = await client.post(f"/api/jobs/{job_id}/worktree")
        repeated = await client.post(f"/api/jobs/{job_id}/worktree")
        assert prepared.status_code == repeated.status_code == 200
        assert prepared.json() == repeated.json()
        assert str(tmp_path) not in prepared.text
        detail = await client.get(f"/api/jobs/{job_id}")
        assert detail.json()["worktree_ready"] is True
        assert detail.json()["state"] == "queued"
        records = api.state.event_repository.list(job_id)
        assert len(records) == 4
        assert len(list(api.state.runtime.worktrees.iterdir())) == 1
        stream = api.state.event_stream_service.subscribe(job_id)
        frames = [await anext(stream) for _ in records]
        await stream.aclose()
        assert all(b"event: workbench.event" in frame for frame in frames)
    restarted = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with restarted.router.lifespan_context(restarted):
        job = restarted.state.job_repository.get(job_id)
        assert job.state.value == "queued" and job.worktree_path is not None
        assert len(restarted.state.event_repository.list(job_id)) == 4


@pytest.mark.anyio
async def test_task_view_storage_failure_is_sanitized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with workspace(tmp_path) as (_, client):

        def fail(*args, **kwargs):
            from db import JobRepositoryError

            raise JobRepositoryError("internal context must not escape")

        monkeypatch.setattr(JobRepository, "list_recent", fail)
        monkeypatch.setattr(JobRepository, "get", fail)
        for path in ("/api/jobs?project_id=alpha", "/api/jobs/job-a"):
            response = await client.get(path)
            assert response.status_code == 503
            assert "internal context" not in response.text


@pytest.mark.anyio
async def test_repository_page_validation_and_large_event_identity(tmp_path: Path) -> None:
    async with workspace(tmp_path) as (api, _):
        jobs = api.state.job_repository
        jobs.create(stored_job("job-a"))
        for limit in (True, 0, 201):
            with pytest.raises(JobValidationError):
                jobs.list_recent("alpha", limit=limit)
        with pytest.raises(JobValidationError):
            jobs.list_recent("alpha", before_id=" bad ")
        api.state.event_repository.append(
            EventCreate(job_id="job-a", event_type="job.progress", payload={"text": "Seed"})
        )
        with sqlite3.connect(api.state.database.path) as connection:
            connection.execute(
                """INSERT INTO events (id, job_id, sequence, event_type, payload, payload_hash)
                SELECT ?, job_id, 2, event_type, payload, payload_hash FROM events LIMIT 1""",
                (9007199254740992,),
            )
        record = api.state.event_repository.append(
            EventCreate(
                job_id="job-a",
                event_type="job.progress",
                payload={"text": "你好\nLine two"},
            )
        )
        stream = api.state.event_stream_service.subscribe("job-a")
        frames = [await anext(stream) for _ in range(3)]
        await stream.aclose()
        assert f"id: {record.id}\n".encode() in frames[-1]
        assert record.id > 2**53
