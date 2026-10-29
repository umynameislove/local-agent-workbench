from __future__ import annotations

import asyncio
import json
import sys

import pytest
from test_codex_adapter import fake_codex as fake_codex
from test_native_execution import native_app, prepare, settled
from test_team_config import team_configuration

from local_agent_workbench.engine import ConfigurationError, EventCreate, WorkbenchConfig


def configure_check(config):
    config["verification_commands"] = {
        "sample-project": [
            sys.executable,
            "-c",
            "from pathlib import Path; assert Path('example.txt').read_text() == 'improved\\n'",
        ]
    }


def writing_worker(executable):
    text = executable.read_text().replace(
        "prompt = sys.stdin.read()",
        "prompt = sys.stdin.read()\nif prompt == 'update':\n"
        "    Path('example.txt').write_text('improved\\n')",
    )
    executable.write_text(text)
    return executable


async def execute(api, client):
    job_id, body = await prepare(client, "update")
    assert (await client.post(f"/api/jobs/{job_id}/execution", json=body)).status_code == 202
    assert (await settled(api, job_id)).outcome == "completed"
    return job_id


@pytest.mark.anyio
async def test_complete_ui_api_path_uses_actual_fixture_worktree_and_separate_promotion(
    tmp_path, fake_codex
):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        home,
        project,
    ):
        agents = (await client.get("/api/projects/sample-project/agents")).json()
        assert agents["agents"][0]["id"] == "coder"
        assert str(home) not in json.dumps(agents) and "account_slot" not in json.dumps(agents)
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        assert (await client.post(prefix + "/review-readiness")).status_code == 409
        workflow = (await client.get(prefix + "/workflow")).json()
        assert (
            workflow["agent"]["id"] == "coder"
            and workflow["verification"]["status"] == "not_started"
        )
        body = {"authorization_hash": workflow["verification"]["authorization_hash"]}
        result = await client.post(prefix + "/verification", json=body)
        assert result.status_code == 200, result.text
        assert result.json()["verification"]["status"] == "passed"
        assert (await client.post(prefix + "/verification", json=body)).json()[
            "verification"
        ] == result.json()["verification"]
        assert len(api.state.verification_repository.list(job_id=job_id)) == 1
        assert (await client.post(prefix + "/review-readiness")).status_code == 200
        bundle = (await client.post(prefix + "/review-bundle")).json()
        assert bundle["bundle"]["verification"][0]["outcome"] == "passed"
        request = await client.post(
            prefix + "/approval-request", json={"bundle_hash": bundle["bundle_hash"]}
        )
        assert request.status_code == 200, request.text
        approval = request.json()["approval_id"]
        decision = await client.post(
            f"/api/approvals/{approval}",
            json={"decision": "approved", "bundle_hash": bundle["bundle_hash"]},
        )
        assert decision.status_code == 200, decision.text
        assert (project / "example.txt").read_text() == "initial\n"
        applied = await client.post(
            f"/api/approvals/{approval}/promote", json={"bundle_hash": bundle["bundle_hash"]}
        )
        assert applied.status_code == 200, applied.text
        assert applied.json()["job_state"] == "completed"
        assert (project / "example.txt").read_text() == "improved\n"
        assert (await client.get(prefix + "/approval")).json()["job_state"] == "completed"


@pytest.mark.anyio
async def test_duplicate_verification_has_one_durable_admission(tmp_path, fake_codex):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        workflow = (await client.get(f"/api/jobs/{job_id}/workflow")).json()
        body = {"authorization_hash": workflow["verification"]["authorization_hash"]}
        results = await asyncio.gather(
            *(client.post(f"/api/jobs/{job_id}/verification", json=body) for _ in range(2))
        )
        assert all(r.status_code == 200 for r in results)
        assert len(api.state.verification_repository.list(job_id=job_id)) == 1
        assert (
            sum(
                e.event_type == "workflow.check.started"
                for e in api.state.event_repository.list(job_id)
            )
            == 1
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind", ["missing-command", "failed", "changed-content", "extra-command", "stale-hash"]
)
async def test_verification_boundary_denies_untrusted_or_unproven_results(
    tmp_path, fake_codex, kind
):
    def change(config):
        configure_check(config)
        if kind == "missing-command":
            del config["verification_commands"]
        elif kind == "failed":
            config["verification_commands"]["sample-project"] = [
                sys.executable,
                "-c",
                "raise SystemExit(1)",
            ]
        elif kind == "changed-content":
            config["verification_commands"]["sample-project"] = [
                sys.executable,
                "-c",
                "from pathlib import Path; Path('example.txt').write_text('unchecked')",
            ]

    async with native_app(tmp_path, writing_worker(fake_codex), change=change) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        value = (await client.get(prefix + "/workflow")).json()
        body = {"authorization_hash": value["verification"]["authorization_hash"]}
        if kind == "extra-command":
            body["argv"] = ["untrusted"]
        elif kind == "stale-hash":
            body["authorization_hash"] = "0" * 64
        result = await client.post(prefix + "/verification", json=body)
        assert result.status_code == (200 if kind == "failed" else 409)
        assert (await client.post(prefix + "/review-readiness")).status_code == 409


@pytest.mark.anyio
async def test_verified_content_cannot_change_before_review(tmp_path, fake_codex):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        value = (await client.get(prefix + "/workflow")).json()
        await client.post(
            prefix + "/verification",
            json={"authorization_hash": value["verification"]["authorization_hash"]},
        )
        path = api.state.job_repository.get(job_id).worktree_path
        from pathlib import Path

        (Path(path) / "example.txt").write_text("not verified\n")
        assert (await client.post(prefix + "/review-readiness")).status_code == 409


@pytest.mark.anyio
async def test_review_scan_checks_its_own_collected_content_against_verification(
    tmp_path, fake_codex, monkeypatch
):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        value = (await client.get(prefix + "/workflow")).json()
        assert (
            await client.post(
                prefix + "/verification",
                json={"authorization_hash": value["verification"]["authorization_hash"]},
            )
        ).status_code == 200
        gate = api.state.secret_gate
        original = gate._collect

        async def race(job):
            from pathlib import Path

            (Path(job.worktree_path) / "example.txt").write_text("unverified race\n")
            return await original(job)

        monkeypatch.setattr(gate, "_collect", race)
        assert (await client.post(prefix + "/review-readiness")).status_code == 409
        assert api.state.job_repository.get(job_id).state.value == "verifying"


@pytest.mark.anyio
async def test_changed_command_never_inherits_old_passed_status(tmp_path, fake_codex):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        value = (await client.get(prefix + "/workflow")).json()
        await client.post(
            prefix + "/verification",
            json={"authorization_hash": value["verification"]["authorization_hash"]},
        )
        api.state.task_workflow.commands = {
            "sample-project": (sys.executable, "-c", "raise SystemExit(1)")
        }
        current = (await client.get(prefix + "/workflow")).json()["verification"]
        assert current["status"] == "not_started"
        assert current["authorization_hash"] != value["verification"]["authorization_hash"]
        assert (await client.post(prefix + "/review-readiness")).status_code == 409


@pytest.mark.anyio
async def test_new_command_cannot_bypass_unfinished_check_from_another_key(tmp_path, fake_codex):
    async with native_app(tmp_path, writing_worker(fake_codex), change=configure_check) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        key = (await client.get(prefix + "/workflow")).json()["verification"]["authorization_hash"]
        api.state.event_repository.append(
            EventCreate(
                job_id,
                "workflow.check.started",
                {"authorization_hash": key},
                f"workflow-check:{key}",
            )
        )
        api.state.task_workflow.commands = {"sample-project": (sys.executable, "-c", "pass")}
        current = (await client.get(prefix + "/workflow")).json()["verification"]
        assert (
            await client.post(
                prefix + "/verification", json={"authorization_hash": current["authorization_hash"]}
            )
        ).status_code == 409
        assert not api.state.verification_repository.list(job_id=job_id)
        assert (
            sum(
                e.event_type == "workflow.check.started"
                for e in api.state.event_repository.list(job_id)
            )
            == 1
        )


@pytest.mark.anyio
async def test_explicit_corrected_command_retains_prior_failed_evidence(tmp_path, fake_codex):
    def change(config):
        configure_check(config)
        config["verification_commands"]["sample-project"] = [
            sys.executable,
            "-c",
            "raise SystemExit(1)",
        ]

    async with native_app(tmp_path, writing_worker(fake_codex), change=change) as (
        api,
        client,
        _,
        _,
    ):
        job_id = await execute(api, client)
        prefix = f"/api/jobs/{job_id}"
        first = (await client.get(prefix + "/workflow")).json()["verification"]
        assert (
            await client.post(
                prefix + "/verification", json={"authorization_hash": first["authorization_hash"]}
            )
        ).json()["verification"]["status"] == "failed"
        api.state.task_workflow.commands = {"sample-project": (sys.executable, "-c", "pass")}
        current = (await client.get(prefix + "/workflow")).json()["verification"]
        assert current["status"] == "not_started"
        result = await client.post(
            prefix + "/verification", json={"authorization_hash": current["authorization_hash"]}
        )
        assert result.status_code == 200 and result.json()["verification"]["status"] == "passed"
        assert [r.outcome.value for r in api.state.verification_repository.list(job_id=job_id)] == [
            "failed",
            "passed",
        ]


@pytest.mark.parametrize(
    "commands",
    [
        [],
        {"unknown": ["true"]},
        {"sample-project": "sh"},
        {"sample-project": ["true", None]},
        {"sample-project": []},
    ],
)
def test_verification_configuration_rejects_unknown_scope_or_invalid_argv(commands):
    config = team_configuration()
    config["verification_commands"] = commands
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(config)
