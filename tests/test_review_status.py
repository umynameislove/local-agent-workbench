from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from test_approval_service import decide, prepare, request, review

from local_agent_workbench.app import create_app
from local_agent_workbench.engine import JobState, JobUpdate

URL = "/api/jobs/job-001/approval"


@asynccontextmanager
async def scenario(tmp_path: Path):
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            yield api, client, root


def durable_state(api: object) -> tuple:
    return (
        api.state.job_repository.get("job-001"),
        api.state.approval_repository.list(job_id="job-001"),
        api.state.event_repository.list("job-001"),
    )


@pytest.mark.anyio
async def test_reading_review_never_creates_or_renews_authority(tmp_path: Path) -> None:
    async with scenario(tmp_path) as (api, client, _):
        before = durable_state(api)
        empty = await client.get(URL)
        assert empty.status_code == 200
        assert empty.json()["approval"] is None
        assert empty.json()["bundle_hash"] is None
        assert empty.json()["review_current"] is False
        assert durable_state(api) == before
        bundle = await review(client)
        before = durable_state(api)
        assert (await client.get(URL)).json()["approval"] is None
        assert durable_state(api) == before
        created = (await request(client, bundle["bundle_hash"])).json()
        before = durable_state(api)
        for _ in range(3):
            response = await client.get(URL)
            assert response.status_code == 200
            body = response.json()
            assert body["job_id"] == "job-001"
            assert body["project_id"] == "alpha"
            assert body["job_state"] == "waiting_approval"
            assert body["review_current"] is True
            assert body["bundle_hash"] == bundle["bundle_hash"]
            assert body["approval"] == {
                "approval_id": created["approval_id"],
                "bundle_hash": bundle["bundle_hash"],
                "expires_at": created["expires_at"],
                "state": "pending",
                "decision": None,
                "decided_at": None,
                "event_id": created["event_id"],
            }
            assert response.headers["cache-control"] == "no-store"
            for private in (str(tmp_path), "worktree_path", "request_snapshot", "local-user"):
                assert private not in response.text
        assert durable_state(api) == before


@pytest.mark.anyio
async def test_pending_read_recovers_same_identity_after_restart(tmp_path: Path) -> None:
    async with scenario(tmp_path) as (_, client, _):
        bundle = await review(client)
        created = (await request(client, bundle["bundle_hash"])).json()
    restarted = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with (
        restarted.router.lifespan_context(restarted),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://localhost"
        ) as client,
    ):
        recovered = (await client.get(URL)).json()["approval"]
        assert recovered["approval_id"] == created["approval_id"]
        assert recovered["expires_at"] == created["expires_at"]
        assert recovered["state"] == "pending"
        assert len(restarted.state.approval_repository.list(job_id="job-001")) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("offset", [timedelta(), timedelta(seconds=1)])
async def test_read_reports_expiry_at_exact_original_deadline(
    tmp_path: Path, offset: timedelta
) -> None:
    async with scenario(tmp_path) as (api, client, _):
        bundle = await review(client)
        created = (await request(client, bundle["bundle_hash"])).json()
        stored = api.state.approval_repository.get(created["approval_id"])
        api.state.approval_service._clock = lambda: stored.expires_at + offset
        before = durable_state(api)
        recovered = (await client.get(URL)).json()["approval"]
        assert recovered["state"] == "expired"
        assert recovered["expires_at"] == created["expires_at"]
        assert recovered["decision"] is None
        assert durable_state(api) == before


@pytest.mark.anyio
@pytest.mark.parametrize("decision", ["approved", "rejected", "changes_requested"])
async def test_read_recovers_committed_decision_without_inspecting_live_files(
    tmp_path: Path, decision: str
) -> None:
    async with scenario(tmp_path) as (api, client, root):
        bundle = await review(client)
        created = (await request(client, bundle["bundle_hash"])).json()
        decided = await decide(client, created["approval_id"], bundle["bundle_hash"], decision)
        assert decided.status_code == 200
        (root / "example.txt").write_text("later work\n")
        before = durable_state(api)
        response = await client.get(URL)
        assert response.status_code == 200
        recovered = response.json()["approval"]
        assert recovered["approval_id"] == created["approval_id"]
        assert recovered["state"] == "decided"
        assert recovered["decision"] == decision
        assert recovered["event_id"] == decided.json()["event_id"]
        assert recovered["decided_at"] == decided.json()["decided_at"]
        assert durable_state(api) == before


@pytest.mark.anyio
async def test_new_round_marks_old_bundle_as_historical_until_explicit_preparation(
    tmp_path: Path,
) -> None:
    async with scenario(tmp_path) as (api, client, root):
        bundle = await review(client)
        created = (await request(client, bundle["bundle_hash"])).json()
        await decide(client, created["approval_id"], bundle["bundle_hash"], "changes_requested")
        (root / "example.txt").write_text("updated change\n")
        stored = api.state.job_repository.get("job-001")
        api.state.job_repository.update(
            JobUpdate(
                stored.id, JobState.VERIFYING, stored.runtime, stored.model, stored.worktree_path
            )
        )
        assert (await client.post("/api/jobs/job-001/review-readiness")).status_code == 200
        before = durable_state(api)
        old = (await client.get(URL)).json()
        assert old["review_current"] is False
        assert old["approval"]["decision"] == "changes_requested"
        assert old["bundle_hash"] == bundle["bundle_hash"]
        assert durable_state(api) == before
        fresh = (await client.post("/api/jobs/job-001/review-bundle")).json()
        new = (await client.get(URL)).json()
        assert new["review_current"] is True
        assert new["approval"] is None
        assert new["bundle_hash"] == fresh["bundle_hash"] != bundle["bundle_hash"]


@pytest.mark.anyio
@pytest.mark.parametrize("corruption", ["binding", "decision", "pending_hash"])
async def test_inconsistent_authority_fails_closed(tmp_path: Path, corruption: str) -> None:
    async with scenario(tmp_path) as (api, client, _):
        bundle = await review(client)
        created = (await request(client, bundle["bundle_hash"])).json()
        if corruption == "decision":
            await decide(client, created["approval_id"], bundle["bundle_hash"], "approved")
        with sqlite3.connect(api.state.database.path) as connection:
            if corruption == "pending_hash":
                connection.execute("DROP TRIGGER approvals_validate_decision")
                connection.execute("UPDATE approvals SET payload_hash = ?", ("0" * 64,))
            else:
                connection.execute("DROP TRIGGER events_prevent_update")
                kind = "approval.decided" if corruption == "decision" else "approval.requested"
                connection.execute("UPDATE events SET payload = '{}' WHERE event_type = ?", (kind,))
        response = await client.get(URL)
        assert response.status_code == (409 if corruption == "pending_hash" else 503)
        assert str(tmp_path) not in response.text


@pytest.mark.anyio
async def test_read_endpoint_retains_local_browser_boundary(tmp_path: Path) -> None:
    async with scenario(tmp_path) as (_, client, _):
        missing = await client.get("/api/jobs/missing/approval")
        assert missing.status_code == 404
        for headers in (
            {"Origin": "https://other.example.invalid"},
            {"Sec-Fetch-Site": "cross-site"},
        ):
            response = await client.get(URL, headers=headers)
            assert response.status_code == 403
            assert response.headers["cache-control"] == "no-store"
