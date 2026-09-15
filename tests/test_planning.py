from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from db import JobRecord
from engine import EventCreate, JobState, JobUpdate


def git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


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


def write_config(runtime: Path, repository: Path) -> None:
    runtime.mkdir()
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    config["projects"][0]["root"] = str(repository)
    (runtime / "config.json").write_text(json.dumps(config), encoding="utf-8")


async def submit_job(client: httpx.AsyncClient, *, job_id: str) -> None:
    response = await client.post(
        "/api/jobs",
        json={
            "project_id": "sample-project",
            "request": "Inspect the retry path and propose a safe change.",
        },
    )
    assert response.status_code == 201
    assert response.json()["id"] == job_id


def update_for(job: JobRecord, state: JobState, *, worktree_path: str | None = None) -> JobUpdate:
    return JobUpdate(
        id=job.id,
        state=state,
        runtime=job.runtime,
        model=job.model,
        worktree_path=worktree_path,
    )


@pytest.mark.anyio
async def test_plan_is_durable_and_does_not_mutate_a_dirty_repository(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_job(client, job_id="job-plan")
            (repository / "example.txt").write_text("local edit\n", encoding="utf-8")
            (repository / "untracked.txt").write_text("keep me\n", encoding="utf-8")
            head_before = git(repository, "rev-parse", "HEAD")
            status_before = git(repository, "status", "--porcelain=v1", "--untracked-files=all")
            tracked_before = (repository / "example.txt").read_bytes()
            untracked_before = (repository / "untracked.txt").read_bytes()

            response = await client.post("/api/jobs/job-plan/plan")

        stored = api.state.job_repository.get("job-plan")
        events = api.state.event_repository.list("job-plan")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-plan"
    assert body["state"] == "queued"
    assert type(body["event_id"]) is int
    assert body["plan"]["source"] == "demo"
    assert body["plan"]["read_only"] is True
    assert body["plan"]["text"].startswith("Demo planning preview.")
    assert stored.state is JobState.QUEUED
    assert stored.worktree_path is None
    assert [event.event_type for event in events] == [
        "job.classified",
        "job.planning.started",
        "job.plan.recorded",
    ]
    assert [event.sequence for event in events] == [1, 2, 3]
    assert events[-1].id == body["event_id"]
    assert events[-1].payload == body["plan"]
    assert git(repository, "rev-parse", "HEAD") == head_before
    assert git(repository, "status", "--porcelain=v1", "--untracked-files=all") == status_before
    assert (repository / "example.txt").read_bytes() == tracked_before
    assert (repository / "untracked.txt").read_bytes() == untracked_before
    assert tuple((runtime / "worktrees").iterdir()) == ()
    assert str(repository) not in response.text


@pytest.mark.anyio
async def test_plan_can_be_read_and_retried_after_restart_without_duplicate_events(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    first_app = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_job(client, job_id="job-plan")
            original = await client.post("/api/jobs/job-plan/plan")

    restarted = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with restarted.router.lifespan_context(restarted):
        transport = httpx.ASGITransport(app=restarted)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            read = await client.get("/api/jobs/job-plan/plan")
            retry = await client.post("/api/jobs/job-plan/plan")
        events = restarted.state.event_repository.list("job-plan")

    assert original.status_code == 200
    assert read.status_code == 200
    assert retry.status_code == 200
    assert read.json() == original.json()
    assert retry.json() == original.json()
    assert len(events) == 3
    assert tuple((runtime / "worktrees").iterdir()) == ()


@pytest.mark.anyio
async def test_missing_or_unplanned_job_returns_sanitized_errors(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            missing_post = await client.post("/api/jobs/missing/plan")
            missing_get = await client.get("/api/jobs/missing/plan")
            invalid_id = await client.post("/api/jobs/%20bad%20/plan")
            await submit_job(client, job_id="job-plan")
            unplanned = await client.get("/api/jobs/job-plan/plan")

        assert api.state.job_repository.get("job-plan").state is JobState.CREATED
        assert api.state.event_repository.list("job-plan") == ()

    assert missing_post.status_code == 404
    assert missing_get.status_code == 404
    assert invalid_id.status_code == 404
    assert unplanned.status_code == 409
    for response in (missing_post, missing_get, invalid_id, unplanned):
        assert str(repository) not in response.text
        assert str(runtime) not in response.text


@pytest.mark.anyio
async def test_planning_rejects_jobs_that_already_have_a_worktree(tmp_path: Path) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_job(client, job_id="job-plan")
            job = api.state.job_repository.get("job-plan")
            existing = runtime / "worktrees" / "existing"
            existing.mkdir()
            marker = existing / "marker.txt"
            marker.write_text("unchanged\n", encoding="utf-8")
            api.state.job_repository.update(
                update_for(job, JobState.CREATED, worktree_path=str(existing))
            )

            response = await client.post("/api/jobs/job-plan/plan")

        stored = api.state.job_repository.get("job-plan")
        assert api.state.event_repository.list("job-plan") == ()

    assert response.status_code == 409
    assert response.json() == {"detail": "Planning cannot use an existing worktree."}
    assert stored.state is JobState.CREATED
    assert stored.worktree_path == str(existing)
    assert marker.read_text(encoding="utf-8") == "unchanged\n"


@pytest.mark.anyio
async def test_demo_planner_does_not_take_over_an_existing_planning_session(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_job(client, job_id="job-plan")
            created = api.state.job_repository.get("job-plan")
            classified = api.state.atomic_transition_service.transition(
                update_for(created, JobState.CLASSIFIED),
                EventCreate(
                    job_id="job-plan",
                    event_type="job.classified",
                    payload={"basis": "external"},
                    idempotency_key="external-classified",
                ),
            ).job
            api.state.atomic_transition_service.transition(
                update_for(classified, JobState.PLANNING),
                EventCreate(
                    job_id="job-plan",
                    event_type="job.planning.started",
                    payload={"source": "native"},
                    idempotency_key="external-plan-started",
                ),
            )

            response = await client.post("/api/jobs/job-plan/plan")

        stored = api.state.job_repository.get("job-plan")
        events = api.state.event_repository.list("job-plan")

    assert response.status_code == 409
    assert response.json() == {"detail": "This planning session is not a demo session."}
    assert stored.state is JobState.PLANNING
    assert [event.event_type for event in events] == [
        "job.classified",
        "job.planning.started",
    ]


@pytest.mark.anyio
async def test_plan_recording_failure_rolls_back_and_retry_resumes_safely(
    tmp_path: Path,
) -> None:
    repository = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    write_config(runtime, repository)
    api = create_app(
        {"AGENT_WORKBENCH_HOME": str(runtime)},
        job_id_factory=lambda: "job-plan",
    )

    async with api.router.lifespan_context(api):
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await submit_job(client, job_id="job-plan")
            status_before = git(repository, "status", "--porcelain=v1", "--untracked-files=all")
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER reject_plan_event
                    BEFORE INSERT ON events
                    WHEN NEW.event_type = 'job.plan.recorded'
                    BEGIN
                        SELECT RAISE(ABORT, 'synthetic planning failure');
                    END
                    """
                )

            failed = await client.post("/api/jobs/job-plan/plan")
            interrupted = api.state.job_repository.get("job-plan")
            interrupted_events = api.state.event_repository.list("job-plan")

            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute("DROP TRIGGER reject_plan_event")
            recovered = await client.post("/api/jobs/job-plan/plan")

        stored = api.state.job_repository.get("job-plan")
        events = api.state.event_repository.list("job-plan")

    assert failed.status_code == 503
    assert failed.json() == {"detail": "Planning could not be recorded."}
    assert "synthetic" not in failed.text
    assert interrupted.state is JobState.PLANNING
    assert [event.event_type for event in interrupted_events] == [
        "job.classified",
        "job.planning.started",
    ]
    assert recovered.status_code == 200
    assert stored.state is JobState.QUEUED
    assert [event.event_type for event in events] == [
        "job.classified",
        "job.planning.started",
        "job.plan.recorded",
    ]
    assert git(repository, "status", "--porcelain=v1", "--untracked-files=all") == status_before
    assert tuple((runtime / "worktrees").iterdir()) == ()
