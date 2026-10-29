from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from test_codex_adapter import calls
from test_codex_adapter import fake_codex as fake_codex
from test_job_submission import initialize_repository
from test_team_config import team_configuration

from local_agent_workbench.app import create_app
from local_agent_workbench.codex_accounts import CodexAccountPool, codex_account_slots
from local_agent_workbench.codex_adapter import CodexAdapter
from local_agent_workbench.engine import JobState
from local_agent_workbench.native_execution import NativeExecutionService
from local_agent_workbench.native_store import NativeConflictError
from local_agent_workbench.worktree import worktree_identity


@asynccontextmanager
async def native_app(tmp_path, executable, *, change=None):
    project = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    config = team_configuration()
    config["projects"][0]["root"] = str(project)
    if change:
        change(config)
    (runtime / "config.json").write_text(json.dumps(config))
    slots = codex_account_slots(runtime, ("fixture",))
    slots[0].home.mkdir(parents=True)
    (slots[0].home / "authenticated").touch()

    def factory(attempt):
        assert attempt.account_slot == "fixture"
        return CodexAdapter(
            CodexAccountPool(slots),
            executable=str(executable),
            model=attempt.authorization["model"]["model"],
            login_timeout=2,
            session_timeout=2,
        )

    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)}, native_adapter_factory=factory)
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client,
    ):
        yield api, client, slots[0].home, project


async def prepare(client, prompt="task", **fields):
    response = await client.post(
        "/api/jobs",
        json={
            "project_id": "sample-project",
            "request": prompt,
            "agent_id": "coder",
            **fields,
        },
    )
    assert response.status_code == 201, response.text
    identity = response.json()["id"]
    assert (await client.post(f"/api/jobs/{identity}/plan")).status_code == 200
    assert (await client.post(f"/api/jobs/{identity}/worktree")).status_code == 200
    authorization = (await client.get(f"/api/jobs/{identity}/execution")).json()[
        "authorization_hash"
    ]
    return identity, {"authorization_hash": authorization}


async def settled(api, job_id):
    async with asyncio.timeout(6):
        while True:
            attempt = api.state.native_attempt_repository.get(job_id)
            if attempt is not None and attempt.phase in {"settled", "reconcile"}:
                return attempt
            await asyncio.sleep(0.02)


@pytest.mark.anyio
async def test_native_api_end_to_end_pins_account_and_replays_ledger(tmp_path, fake_codex):
    async with native_app(tmp_path, fake_codex) as (api, client, home, project):
        job_id, body = await prepare(client)
        response = await client.post(f"/api/jobs/{job_id}/execution", json=body)
        assert response.status_code == 202
        attempt = await settled(api, job_id)
        assert attempt.outcome == "completed" and not attempt.held
        assert attempt.session_id == "thread-fixture" and attempt.account_slot == "fixture"
        assert api.state.job_repository.get(job_id).state is JobState.VERIFYING
        assert len(calls(home)) == 1
        assert (project / "example.txt").read_text() == "initial\n"
        events = api.state.event_repository.list(job_id)
        assert any(event.event_type == "runtime.text" for event in events)
        assert events[-1].event_type == "native.settled"
        assert (await client.post(f"/api/jobs/{job_id}/execution", json=body)).status_code == 202
        assert len(calls(home)) == 1
        public = (await client.get(f"/api/jobs/{job_id}/execution")).text
        assert str(home) not in public and "thread-fixture" not in public


@pytest.mark.anyio
async def test_independent_supervisors_cannot_spawn_duplicate_writers(tmp_path, fake_codex):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client, "wait")
        service = api.state.native_execution_service
        second = NativeExecutionService(
            service.jobs,
            service.projects,
            service.teams,
            service.attempts,
            service.worktrees,
            service.runtime_root,
            adapter_factory=service._factory,
        )
        await asyncio.gather(service.start(job_id, body), second.start(job_id, body))
        async with asyncio.timeout(4):
            while not calls(home):
                await asyncio.sleep(0.02)
        assert len(calls(home)) == 1
        attempt = service.attempts.get(job_id)
        await client.post(f"/api/jobs/{job_id}/execution/cancel", json={"attempt_id": attempt.id})
        assert (await settled(api, job_id)).outcome == "cancelled"
        await second.close()


@pytest.mark.anyio
@pytest.mark.parametrize("prompt", ["no-session", "wait", "complete-then-wait"])
async def test_cancellation_during_start_or_running_never_needs_session_id(
    tmp_path, fake_codex, prompt
):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client, prompt)
        await client.post(f"/api/jobs/{job_id}/execution", json=body)
        async with asyncio.timeout(4):
            while not calls(home):
                await asyncio.sleep(0.02)
        attempt = api.state.native_attempt_repository.get(job_id)
        assert attempt.held
        assert api.state.job_repository.get(job_id).state is JobState.RUNNING
        denied = await client.post(
            f"/api/jobs/{job_id}/execution/cancel", json={"attempt_id": "wrong-attempt"}
        )
        assert denied.status_code == 409
        response = await client.post(
            f"/api/jobs/{job_id}/execution/cancel", json={"attempt_id": attempt.id}
        )
        assert response.status_code == 200
        final = await settled(api, job_id)
        assert final.outcome == "cancelled" and not final.held
        assert api.state.job_repository.get(job_id).state is JobState.CANCELLED
        assert len(calls(home)) == 1


@pytest.mark.anyio
async def test_completion_followed_by_nonzero_exit_fails(tmp_path, fake_codex):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client, "complete-then-fail")
        await client.post(f"/api/jobs/{job_id}/execution", json=body)
        attempt = await settled(api, job_id)
        assert attempt.outcome == "failed"
        assert api.state.job_repository.get(job_id).state is JobState.FAILED
        completions = [
            e
            for e in api.state.event_repository.list(job_id)
            if e.event_type == "runtime.completion"
        ]
        assert len(completions) == 1 and completions[0].payload["status"] == "failed"
        assert len(calls(home)) == 1


@pytest.mark.anyio
async def test_revocation_stops_active_writer_and_frozen_policy_stays_immutable(
    tmp_path, fake_codex
):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client, "wait")
        before = api.state.job_repository.get(job_id).request_snapshot
        await client.post(f"/api/jobs/{job_id}/execution", json=body)
        async with asyncio.timeout(4):
            while not calls(home):
                await asyncio.sleep(0.02)
        team = (await client.get("/api/projects/sample-project/team")).json()
        team["team"]["enabled"] = False
        assert (await client.put("/api/projects/sample-project/team", json=team)).status_code == 200
        assert (await settled(api, job_id)).outcome == "cancelled"
        assert api.state.job_repository.get(job_id).request_snapshot == before


@pytest.mark.anyio
async def test_durable_crash_window_blocks_replay_and_holds_global_lane(tmp_path, fake_codex):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client)
        job = api.state.job_repository.get(job_id)
        path = await api.state.worktree_manager.verify_bound(job)
        store = api.state.native_attempt_repository
        attempt, created = store.claim(
            job, api.state.team_repository, "dead-owner", worktree_identity(path)
        )
        assert created and attempt.held
        response = await client.post(f"/api/jobs/{job_id}/execution", json=body)
        assert response.status_code == 202 and response.json()["requires_reconciliation"]
        assert calls(home) == []
        other_id, other_body = await prepare(client)
        assert (
            await client.post(f"/api/jobs/{other_id}/execution", json=other_body)
        ).status_code == 409
        assert calls(home) == []
        assert store.get(job_id).owner == "dead-owner"


@pytest.mark.anyio
async def test_native_disabled_legacy_unbound_and_wrong_hash_cannot_execute(tmp_path, fake_codex):
    async with native_app(
        tmp_path,
        fake_codex,
        change=lambda c: c["agent_system"].update(native_execution_enabled=False),
    ) as (api, client, home, _):
        job_id, body = await prepare(client)
        assert (await client.post(f"/api/jobs/{job_id}/execution", json=body)).status_code == 409
        wrong = {"authorization_hash": "0" * 64}
        assert (await client.post(f"/api/jobs/{job_id}/execution", json=wrong)).status_code == 409
        assert (await client.post(f"/api/jobs/{job_id}/execution", json={})).status_code == 422
        assert api.state.native_attempt_repository.get(job_id) is None and calls(home) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field,value", [("max_steps", 2), ("max_output_tokens", 30), ("document_paths", ["docs"])]
)
async def test_unenforceable_native_policy_never_spawns(tmp_path, fake_codex, field, value):
    def change(config):
        config["agent_system"]["agents"][0][field] = value

    async with native_app(tmp_path, fake_codex, change=change) as (api, client, home, _):
        job_id, body = await prepare(client)
        assert (await client.post(f"/api/jobs/{job_id}/execution", json=body)).status_code == 409
        assert api.state.native_attempt_repository.get(job_id) is None and calls(home) == []


@pytest.mark.anyio
async def test_completion_with_uncertain_process_cleanup_stays_quarantined(
    tmp_path, fake_codex, monkeypatch
):
    async def unknown_cleanup(_process):
        return False

    monkeypatch.setattr(CodexAdapter, "_group_stopped", staticmethod(unknown_cleanup))
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        job_id, body = await prepare(client)
        await client.post(f"/api/jobs/{job_id}/execution", json=body)
        attempt = await settled(api, job_id)
        assert attempt.phase == "reconcile" and attempt.held and attempt.outcome is None
        assert len(calls(home)) == 1
        assert api.state.job_repository.get(job_id).state is JobState.RUNNING
        with pytest.raises(NativeConflictError):
            api.state.native_attempt_repository.check(attempt, api.state.team_repository)


@pytest.mark.anyio
async def test_revocation_during_authentication_prevents_any_launch(tmp_path, fake_codex):
    async with native_app(tmp_path, fake_codex) as (api, client, home, _):
        service = api.state.native_execution_service
        original_factory = service._factory

        def factory(attempt):
            provider = original_factory(attempt)
            authenticated = provider._authenticated

            async def revoke(slot):
                result = await authenticated(slot)
                body = service.teams.get("sample-project").to_dict()
                body["team"]["enabled"] = False
                service.teams.put("sample-project", body)
                return result

            provider._authenticated = revoke
            return provider

        service._factory = factory
        job_id, body = await prepare(client)
        await client.post(f"/api/jobs/{job_id}/execution", json=body)
        attempt = await settled(api, job_id)
        assert attempt.outcome == "cancelled"
        assert calls(home) == []
