from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from app import create_app
from db import Database, EventRepository, EventValidationError, JobRepository, ProjectRepository
from engine import (
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
)
from event_stream import EventStreamCursorError, EventStreamService


def project() -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root="/workspace/alpha",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(job_id: str) -> JobCreate:
    return JobCreate(
        id=job_id,
        project_id="alpha",
        request="Follow durable events.",
        request_snapshot={"policy_version": 1},
        state=JobState.RUNNING,
        runtime=JobRuntime.LOCAL,
        model="qwen-local",
        worktree_path=f"/runtime/worktrees/{job_id}",
    )


def event(job_id: str, number: int, *, event_type: str = "job.progress") -> EventCreate:
    return EventCreate(
        job_id=job_id,
        event_type=event_type,
        payload={"message": f"update {number}", "number": number},
        idempotency_key=f"update-{number}",
    )


def initialized_service(
    tmp_path: Path,
    *,
    batch_size: int = 128,
    poll_interval: float = 0.005,
    heartbeat_interval: float = 1.0,
) -> tuple[Database, JobRepository, EventRepository, EventStreamService]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    jobs = JobRepository(database)
    jobs.create(job("job-001"))
    events = EventRepository(database)
    service = EventStreamService(
        jobs,
        events,
        batch_size=batch_size,
        poll_interval=poll_interval,
        heartbeat_interval=heartbeat_interval,
    )
    return database, jobs, events, service


async def take(stream: AsyncIterator[bytes], count: int) -> list[bytes]:
    try:
        return [await asyncio.wait_for(anext(stream), 1) for _ in range(count)]
    finally:
        await stream.aclose()


def frame_data(frame: bytes) -> dict[str, object]:
    line = next(item for item in frame.decode().splitlines() if item.startswith("data: "))
    return json.loads(line.removeprefix("data: "))


@pytest.mark.anyio
async def test_persisted_events_replay_in_job_order_across_batches(tmp_path: Path) -> None:
    _, jobs, events, service = initialized_service(tmp_path, batch_size=2)
    jobs.create(job("job-002"))
    expected = [events.append(event("job-001", number)) for number in range(1, 3)]
    events.append(event("job-002", 1))
    expected.extend(events.append(event("job-001", number)) for number in range(3, 6))

    frames = await take(service.subscribe("job-001"), len(expected))
    delivered = [frame_data(frame) for frame in frames]

    assert [item["id"] for item in delivered] == [record.id for record in expected]
    assert [item["sequence"] for item in delivered] == list(range(1, 6))
    assert all(item["job_id"] == "job-001" for item in delivered)
    assert all(frame.count(b"event: workbench.event\n") == 1 for frame in frames)


@pytest.mark.anyio
async def test_reconnect_from_last_event_id_has_no_gap_or_duplicate(tmp_path: Path) -> None:
    _, _, events, service = initialized_service(tmp_path)
    first = events.append(event("job-001", 1))
    second = events.append(event("job-001", 2))
    initial = await take(service.subscribe("job-001"), 2)

    third = events.append(event("job-001", 3))
    resumed = service.subscribe("job-001", str(second.id))
    fourth = events.append(event("job-001", 4))
    replayed = await take(resumed, 2)

    assert [frame_data(frame)["id"] for frame in initial] == [first.id, second.id]
    assert [frame_data(frame)["id"] for frame in replayed] == [third.id, fourth.id]
    assert {frame_data(frame)["id"] for frame in initial}.isdisjoint(
        frame_data(frame)["id"] for frame in replayed
    )


@pytest.mark.anyio
async def test_subscription_follows_events_persisted_after_waiting_begins(tmp_path: Path) -> None:
    _, _, events, service = initialized_service(tmp_path)
    stream = service.subscribe("job-001")
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0.02)

    stored = events.append(event("job-001", 1))
    frame = await asyncio.wait_for(pending, 1)
    await stream.aclose()

    assert frame_data(frame)["id"] == stored.id
    assert frame_data(frame)["sequence"] == 1


@pytest.mark.anyio
async def test_idle_stream_emits_comment_heartbeat_without_cursor(tmp_path: Path) -> None:
    _, _, _, service = initialized_service(
        tmp_path,
        poll_interval=0.002,
        heartbeat_interval=0.01,
    )

    frame = (await take(service.subscribe("job-001"), 1))[0]

    assert frame == b": keep-alive\n\n"
    assert b"id:" not in frame


@pytest.mark.anyio
async def test_event_fields_cannot_inject_sse_control_lines(tmp_path: Path) -> None:
    _, _, events, service = initialized_service(tmp_path)
    stored = events.append(
        EventCreate(
            job_id="job-001",
            event_type="unsafe\nid: injected",
            payload={"message": "line one\ndata: injected"},
        )
    )

    frame = (await take(service.subscribe("job-001"), 1))[0]
    data = frame_data(frame)
    lines = frame.decode().splitlines()

    assert sum(line.startswith("id: ") for line in lines) == 1
    assert sum(line.startswith("event: ") for line in lines) == 1
    assert sum(line.startswith("data: ") for line in lines) == 1
    assert data["id"] == stored.id
    assert data["event_type"] == "unsafe\nid: injected"
    assert data["payload"] == {"message": "line one\ndata: injected"}


@pytest.mark.anyio
async def test_stream_failure_is_sanitized_and_closes(tmp_path: Path) -> None:
    database, _, _, service = initialized_service(tmp_path)
    stream = service.subscribe("job-001")
    database.path.unlink()

    frame = await anext(stream)

    assert frame == (b'event: workbench.error\ndata: {"code":"event_stream_unavailable"}\n\n')
    assert str(tmp_path).encode() not in frame
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.anyio
async def test_endpoint_rejects_unknown_jobs_and_invalid_cursors(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        api.state.project_repository.create(project())
        api.state.job_repository.create(job("job-001"))
        api.state.job_repository.create(job("job-002"))
        other = api.state.event_repository.append(event("job-002", 1))
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            unknown = await client.get("/api/jobs/missing/events")
            malformed = await client.get(
                "/api/jobs/job-001/events",
                headers={"Last-Event-ID": " 1"},
            )
            missing_cursor = await client.get(
                "/api/jobs/job-001/events",
                headers={"Last-Event-ID": "999"},
            )
            wrong_job = await client.get(
                "/api/jobs/job-001/events",
                headers={"Last-Event-ID": str(other.id)},
            )

    assert unknown.status_code == 404
    assert malformed.status_code == 400
    assert missing_cursor.status_code == 400
    assert wrong_job.status_code == 400
    assert str(tmp_path) not in unknown.text + malformed.text + missing_cursor.text + wrong_job.text


@pytest.mark.anyio
async def test_endpoint_returns_streaming_headers_and_replay_body(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        api.state.project_repository.create(project())
        api.state.job_repository.create(job("job-001"))
        stored = api.state.event_repository.append(event("job-001", 1))
        route = next(
            item
            for item in api.routes
            if getattr(item, "path", None) == "/api/jobs/{job_id}/events"
        )
        request = Request({"type": "http", "app": api, "headers": []})

        response = await route.endpoint(request, "job-001", None)
        assert isinstance(response, StreamingResponse)
        frame = await asyncio.wait_for(anext(response.body_iterator), 1)
        await response.body_iterator.aclose()

    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert frame_data(frame)["id"] == stored.id


@pytest.mark.parametrize("sequence", [-1, True, 1.5])
def test_repository_rejects_invalid_stream_sequence(tmp_path: Path, sequence: object) -> None:
    _, _, events, _ = initialized_service(tmp_path)

    with pytest.raises(EventValidationError, match="sequence cursor"):
        events.list_after("job-001", sequence)


@pytest.mark.parametrize("limit", [0, True, 1_001, 1.5])
def test_repository_rejects_invalid_stream_limit(tmp_path: Path, limit: object) -> None:
    _, _, events, _ = initialized_service(tmp_path)

    with pytest.raises(EventValidationError, match="query limit"):
        events.list_after("job-001", 0, limit=limit)


@pytest.mark.parametrize("cursor", ["0", "+1", "01 ", "١", "9" * 20])
def test_service_rejects_noncanonical_cursor(tmp_path: Path, cursor: str) -> None:
    _, _, _, service = initialized_service(tmp_path)

    with pytest.raises(EventStreamCursorError, match="positive event identifier"):
        service.subscribe("job-001", cursor)
