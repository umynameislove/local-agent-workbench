from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
import pytest
from test_approval_service import request, review
from test_phone_control import BOT, PhoneHarness, action_data
from test_secret_gate import initialize_repository, job, project

from local_agent_workbench.app import create_app
from local_agent_workbench.engine import JobState, WorkbenchConfig
from local_agent_workbench.phone_control import PhoneControl
from local_agent_workbench.phone_store import PhoneError


@asynccontextmanager
async def review_phone(tmp_path, original="original\n", content="safe change\n", **policy):
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root, head = initialize_repository(tmp_path / "runtime" / "worktrees" / "project", original)
        api.state.project_repository.create(project(root))
        api.state.job_repository.create(job(root, head, request="Review this safe change."))
        (root / "example.txt").write_text(content)
        config = {
            "version": 2,
            "projects": [{"id": "alpha", "root": str(root)}],
            "phone_control": {
                "enabled": True,
                "bot_id": BOT,
                "owner_id": 123456,
                "projects": ["alpha"],
                "review_content": True,
                **policy,
            },
        }
        api.state.runtime.config.write_text(json.dumps(config))
        api.state.phone_control = PhoneControl(
            api.state, WorkbenchConfig.from_dict(config).phone_control
        )
        h = PhoneHarness(api)
        api.state.approval_service._clock = lambda: datetime.fromtimestamp(h.now[0], UTC)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            yield h, client, root


async def review_card(h, client):
    bundle = await review(client)
    pending = await request(client, bundle["bundle_hash"])
    assert pending.status_code == 200
    await h.send("/start")
    menu = h.drain()[-1]
    await h.send(data=f"task:{h.service.task_alias('job-001')}", message=menu)
    return bundle, pending.json(), h.drain()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "decision,target", [(0, JobState.APPROVED), (1, JobState.RUNNING), (2, JobState.REJECTED)]
)
async def test_phone_decision_uses_frozen_review_and_never_applies(tmp_path, decision, target):
    async with review_phone(tmp_path) as (h, client, root):
        bundle, pending, cards = await review_card(h, client)
        card = cards[-1]
        text = "".join(c["text"] for c in cards)
        assert bundle["bundle_hash"] in text and pending["approval_id"] in text
        assert "Review this safe change." in text and "safe change" in text
        before = (root / "example.txt").read_text()
        update = await h.send(data=action_data(card, decision), message=card)
        await h.service.process(BOT, update)
        await h.send(data=action_data(card, decision), message=card)
        job = h.api.state.job_repository.get("job-001")
        approval = h.api.state.approval_repository.get(pending["approval_id"])
        assert job.state is target and approval.actor == "telegram:123456"
        assert approval.channel == "telegram"
        assert (root / "example.txt").read_text() == before
        events = h.api.state.event_repository.list(job.id)
        assert (
            next(e.payload["bundle_hash"] for e in events if e.event_type == "approval.decided")
            == bundle["bundle_hash"]
        )
        assert sum(e.event_type == "approval.decided" for e in events) == 1
        assert not any(e.event_type.startswith("promotion.") for e in events)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "boundary", ["expiry", "original_expiry", "changed_file", "different_message", "other_decision"]
)
async def test_stale_phone_review_cannot_authorize_work(tmp_path, boundary):
    async with review_phone(tmp_path) as (h, client, root):
        bundle, pending, cards = await review_card(h, client)
        card = cards[-1]
        if boundary == "expiry":
            h.now[0] += 120
        elif boundary == "original_expiry":
            h.now[0] += 901
        elif boundary == "changed_file":
            (root / "example.txt").write_text("another change\n")
        elif boundary == "different_message":
            card["message_id"] += 1
        else:
            response = await client.post(
                f"/api/approvals/{pending['approval_id']}",
                json={"decision": "rejected", "bundle_hash": bundle["bundle_hash"]},
            )
            assert response.status_code == 200
        await h.send(data=action_data(card), message=card)
        approval = h.api.state.approval_repository.get(pending["approval_id"])
        assert (
            approval.decision is None
            if boundary != "other_decision"
            else approval.decision.value == "rejected"
        )


@pytest.mark.anyio
async def test_decision_commit_recovers_after_crash_and_original_expiry(tmp_path, monkeypatch):
    async with review_phone(tmp_path) as (h, client, _):
        _, pending, cards = await review_card(h, client)
        update = h.update(data=action_data(cards[-1]), message=cards[-1])
        finish = h.service.store.finish_action
        monkeypatch.setattr(
            h.service.store, "finish_action", lambda *a: (_ for _ in ()).throw(SystemExit("crash"))
        )
        with pytest.raises(SystemExit):
            await h.service.process(BOT, update)
        monkeypatch.setattr(h.service.store, "finish_action", finish)
        h.now[0] += 901
        restarted = PhoneControl(h.api.state, h.service.policy, clock=lambda: h.now[0])
        await restarted.process(BOT, update)
        assert (
            h.api.state.approval_repository.get(pending["approval_id"]).decision.value == "approved"
        )
        assert (
            sum(
                e.event_type == "approval.decided"
                for e in h.api.state.event_repository.list("job-001")
            )
            == 1
        )


@pytest.mark.anyio
async def test_full_review_must_be_delivered_before_any_decision(tmp_path):
    async with review_phone(tmp_path) as (h, client, root):
        (root / "example.txt").write_text("bounded change\n" * 220)
        bundle = await review(client)
        pending = (await request(client, bundle["bundle_hash"])).json()
        parts, action, project, kind = h.service.review(
            h.api.state.job_repository.get("job-001"),
            h.api.state.approval_service.read("job-001"),
            h.now[0],
        )
        assert len(parts) > 1
        with h.service.store._write_connection() as c:
            token = h.service.store.add_action(c, *action)
            h.service.store.queue(
                c,
                "partial",
                parts,
                action[2],
                project=project,
                kind=kind,
                token=token,
                subject=action[1],
            )
            ids = [r[0] for r in c.execute("SELECT id FROM phone_outbox ORDER BY id")]
            c.execute("UPDATE phone_outbox SET phase='sending' WHERE id=?", (ids[-1],))
        h.service.store.acknowledge(ids[-1], 999)
        with pytest.raises(PhoneError, match="incomplete"):
            h.service.store.begin(token, "approved", 999, h.now[0])
        assert h.api.state.approval_repository.get(pending["approval_id"]).decision is None


@pytest.mark.anyio
@pytest.mark.parametrize("location", ["deleted", "context"])
async def test_secrets_in_old_diff_content_block_the_entire_phone_review(tmp_path, location):
    secret = "sk-" + "synthetic" * 5
    original = secret + "\noriginal\n"
    content = "removed unsafe credential\n" if location == "deleted" else secret + "\nchanged\n"
    async with review_phone(tmp_path, original=original, content=content) as (h, client, _):
        bundle = await review(client)
        pending = await request(client, bundle["bundle_hash"])
        assert pending.status_code == 200
        job = h.api.state.job_repository.get("job-001")
        with pytest.raises(PhoneError, match="Sensitive"):
            h.service.review(job, h.api.state.approval_service.read(job.id), h.now[0])
        await h.send("/start")
        menu = h.drain()[-1]
        await h.send(data=f"task:{h.service.task_alias(job.id)}", message=menu)
        cards = h.drain()
        assert all(secret not in c["text"] and c["reply_markup"] is None for c in cards)


@pytest.mark.anyio
async def test_phone_review_is_off_until_content_egress_is_enabled(tmp_path):
    async with review_phone(tmp_path, review_content=False) as (h, client, _):
        _, _, cards = await review_card(h, client)
        assert cards[-1]["reply_markup"] is None
        assert "Mac" in cards[-1]["text"] and "safe change" not in cards[-1]["text"]


@pytest.mark.anyio
async def test_browser_and_phone_conflicting_decisions_commit_only_one(tmp_path):
    async with review_phone(tmp_path) as (h, client, _):
        bundle, pending, cards = await review_card(h, client)
        await asyncio.gather(
            h.send(data=action_data(cards[-1]), message=cards[-1]),
            client.post(
                f"/api/approvals/{pending['approval_id']}",
                json={"decision": "rejected", "bundle_hash": bundle["bundle_hash"]},
            ),
        )
        events = h.api.state.event_repository.list("job-001")
        assert sum(e.event_type == "approval.decided" for e in events) == 1
