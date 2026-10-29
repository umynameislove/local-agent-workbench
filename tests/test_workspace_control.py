from __future__ import annotations

import copy
import json
from contextlib import asynccontextmanager
from dataclasses import replace

import httpx
import pytest
from test_team_config import team_configuration
from test_workspace import repository

from app import create_app
from engine import EventCreate, JobCreate, JobRuntime, JobState, JobUpdate
from native_store import NativeConflictError
from team_store import TeamRepository


@asynccontextmanager
async def control_app(tmp_path):
    config = team_configuration()
    config["projects"][0]["root"] = str(repository(tmp_path / "alpha"))
    config["projects"].append(
        {**config["projects"][0], "id": "beta", "root": str(repository(tmp_path / "beta"))}
    )
    system = config["agent_system"]
    system["agents"].append({**system["agents"][0], "id": "other-coder"})
    system["teams"].append({"project_id": "beta", "agent_ids": ["coder"]})
    system["connections"].append(
        {
            "id": "api",
            "kind": "openrouter",
            "credential_ref": "keychain:private-reference",
        }
    )
    system["models"].append({"id": "reader-model", "connection_id": "api", "model": "vendor/model"})
    system["agents"].append({"id": "reader", "role": "reader", "model_id": "reader-model"})
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "config.json").write_text(json.dumps(config))
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client,
    ):
        yield api, client


def body(settings, **changes):
    return {
        "revision": settings["revision"],
        "agent_ids": settings["team"]["agent_ids"],
        "enabled": settings["team"]["enabled"],
        **changes,
    }


def claim(api, identity="owned"):
    project = api.state.project_repository.get("sample-project")
    binding = api.state.team_repository.snapshot("sample-project", "coder")
    job = api.state.job_repository.create(
        JobCreate(
            identity,
            project.id,
            "Synthetic work",
            {
                "agent_binding": binding,
                "project": {
                    "id": project.id,
                    "root": project.root,
                    "sensitivity": project.sensitivity.value,
                    "cloud_allowed": project.cloud_allowed,
                    "permission_mode": project.permission_mode.value,
                },
            },
            runtime=JobRuntime.CODEX,
        )
    )
    for state in (JobState.CLASSIFIED, JobState.PLANNING, JobState.QUEUED):
        job = api.state.job_repository.update(
            JobUpdate(job.id, state, JobRuntime.CODEX, worktree_path=project.root + "/isolated")
        )
    attempt, _ = api.state.native_attempt_repository.claim(
        job, api.state.team_repository, "fixture-owner", {}
    )
    return attempt


@pytest.mark.anyio
async def test_safe_library_disabled_team_recovery_and_private_fields(tmp_path):
    async with control_app(tmp_path) as (api, client):
        url = "/api/projects/sample-project/settings"
        initial = (await client.get(url)).json()
        profiles = {p["id"]: p for p in initial["profiles"]}
        assert profiles["other-coder"]["eligible"] and profiles["coder"]["executable"]
        assert profiles["reader"]["eligible"] and not profiles["reader"]["executable"]
        text = json.dumps(initial)
        for secret in (
            str(tmp_path),
            "account_slot",
            "credential_ref",
            "private-reference",
            "fixture",
        ):
            assert secret not in text
        saved = await client.post(
            "/api/projects/sample-project/team/members", json=body(initial, enabled=False)
        )
        assert saved.status_code == 200
        disabled = saved.json()
        assert disabled["revision"] == 2
        assert {p["id"] for p in disabled["profiles"]} == set(profiles)
        assert not any(p["executable"] for p in disabled["profiles"])
        restored = await client.post(
            "/api/projects/sample-project/team/members",
            json=body(disabled, enabled=True, agent_ids=["other-coder"]),
        )
        assert restored.status_code == 200
        assert api.state.team_repository.get("beta").team.agent_ids == ("coder",)
    async with api.router.lifespan_context(api):
        team = api.state.team_repository.get("sample-project")
        assert team.revision == 3 and team.team.agent_ids == ("other-coder",)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "extra", ["mode", "paid_api_enabled", "lead_id", "max_active", "per_task_cap_usd", "project_id"]
)
async def test_limited_editor_rejects_protected_fields_atomically(tmp_path, extra):
    async with control_app(tmp_path) as (api, client):
        before = api.state.team_repository.get("sample-project")
        payload = body(before.to_dict()) | {extra: True}
        response = await client.post("/api/projects/sample-project/team/members", json=payload)
        assert response.status_code == 422
        assert api.state.team_repository.get("sample-project") == before


@pytest.mark.anyio
async def test_revision_validation_preserves_policy_and_blocks_replays(tmp_path):
    async with control_app(tmp_path) as (api, client):
        record = api.state.team_repository.get("sample-project")
        original = record.to_dict()
        payload = body(original, agent_ids=["coder", "other-coder"])
        response = await client.post("/api/projects/sample-project/team/members", json=payload)
        assert response.status_code == 200
        expected = copy.deepcopy(original["team"])
        expected["agent_ids"] = payload["agent_ids"]
        assert response.json()["team"] == expected
        assert (
            await client.post("/api/projects/sample-project/team/members", json=payload)
        ).status_code == 409
        for change in ({"revision": True}, {"revision": 999}, {"revision": 0}):
            assert (
                await client.post(
                    "/api/projects/sample-project/team/members",
                    json=body(response.json(), **change),
                )
            ).status_code == 409
        for change in ({"agent_ids": []}, {"agent_ids": ["missing"]}, {"enabled": 1}):
            assert (
                await client.post(
                    "/api/projects/sample-project/team/members",
                    json=body(response.json(), **change),
                )
            ).status_code == 422


@pytest.mark.anyio
async def test_disable_revokes_owned_execution_without_rewriting_history(tmp_path):
    async with control_app(tmp_path) as (api, client):
        attempt = claim(api)
        before = (await client.get("/api/projects/sample-project/activity")).json()
        settings = (await client.get("/api/projects/sample-project/settings")).json()
        saved = await client.post(
            "/api/projects/sample-project/team/members", json=body(settings, enabled=False)
        )
        assert saved.status_code == 200
        current = api.state.native_attempt_repository.get(attempt.job_id)
        assert current.cancel_requested and current.held and current.phase == "admitted"
        with pytest.raises((NativeConflictError, RuntimeError)):
            api.state.native_attempt_repository.check(current, api.state.team_repository)
        after = (await client.get("/api/projects/sample-project/activity")).json()
        assert after["attempts"][0]["profile_id"] == before["attempts"][0]["profile_id"]
        assert after["attempts"][0]["cancel_requested"]
        assert after["attempts"][0]["requires_reconciliation"]
        assert (await client.get("/api/projects/beta/activity")).json()["attempts"] == []


@pytest.mark.anyio
async def test_activity_bounds_attention_and_includes_attempt_outside_recent_page(tmp_path):
    async with control_app(tmp_path) as (api, client):
        attempt = claim(api)
        for number in range(61):
            job = api.state.job_repository.create(
                JobCreate(f"new-{number}", "sample-project", "Test", {})
            )
            api.state.job_repository.update(JobUpdate(job.id, JobState.BLOCKED, JobRuntime.AUTO))
        api.state.event_repository.append(
            EventCreate(attempt.job_id, "native.fixture", {"text": "event"})
        )
        result = await client.get("/api/projects/sample-project/activity")
        assert result.status_code == 200
        data = result.json()
        assert data["attention_total"] == 61 and len(data["attention"]) == 50
        assert data["attention_truncated"] and not data["attempts_truncated"]
        assert data["attempt_total"] == 1 and data["attempts"][0]["job_id"] == attempt.job_id
        for forbidden in (
            "request_snapshot",
            "authorization",
            "worktree_path",
            "account_slot",
            "session_id",
        ):
            assert forbidden not in result.text
        assert api.state.native_attempt_repository.get(attempt.job_id) == attempt
        for suffix in ("settings", "activity"):
            assert (await client.get(f"/api/projects/missing/{suffix}")).status_code == 404
        assert (
            await client.post(
                "/api/projects/sample-project/team/members",
                json={},
                headers={"Origin": "https://foreign.invalid"},
            )
        ).status_code == 403


@pytest.mark.anyio
async def test_removed_library_member_remains_visible_and_can_be_replaced(tmp_path):
    async with control_app(tmp_path) as (api, client):
        teams = api.state.team_repository
        initial = teams.get("sample-project").to_dict()
        initial["team"]["agent_ids"] = ["other-coder"]
        teams.put("sample-project", initial)
        reduced = replace(
            teams.system, agents=tuple(a for a in teams.system.agents if a.id != "other-coder")
        )
        api.state.team_repository = TeamRepository(teams.database, reduced, teams.project_ids)
        api.state.team_repository.seed()
        stored = (await client.get("/api/projects/sample-project/settings")).json()
        missing = next(p for p in stored["profiles"] if p["id"] == "other-coder")
        assert missing["role"] == "unavailable" and missing["model"] is None
        assert not missing["eligible"] and not missing["executable"]
        assert not missing["connection_id"]
        saved = await client.post(
            "/api/projects/sample-project/team/members", json=body(stored, agent_ids=["coder"])
        )
        assert saved.status_code == 200
        assert saved.json()["team"]["agent_ids"] == ["coder"]
        assert all(p["id"] != "other-coder" for p in saved.json()["profiles"])
        assert (
            await client.post(
                "/api/projects/sample-project/team/members",
                json=body(saved.json(), agent_ids=["coder", "other-coder"]),
            )
        ).status_code == 422
