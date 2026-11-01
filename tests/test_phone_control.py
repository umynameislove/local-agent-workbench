from __future__ import annotations

import asyncio
import copy
import json
import time
from contextlib import asynccontextmanager

import pytest
from test_telegram_gateway import OWNER, TOKEN
from test_workspace_control import claim, control_app

from local_agent_workbench.db import EventRepository
from local_agent_workbench.engine import JobState, WorkbenchConfig
from local_agent_workbench.phone_control import PhoneControl
from local_agent_workbench.phone_notifications import PhoneNotifications

BOT = int(TOKEN.split(":")[0])


class PhoneHarness:
    def __init__(self, api):
        self.api, self.now = api, [int(time.time())]
        self.service = api.state.phone_control
        self.service.clock = lambda: self.now[0]
        self.identity, self.message_id, self.sent = 0, 100, []

    def update(self, text=None, *, data=None, message=None, reply=None):
        self.identity += 1
        chat = {"id": OWNER, "type": "private"}
        if data is not None:
            return {
                "update_id": self.identity,
                "callback_query": {
                    "id": str(self.identity),
                    "from": {"id": OWNER, "is_bot": False},
                    "data": data,
                    "message": message,
                },
            }
        return {
            "update_id": self.identity,
            "message": {
                "message_id": 1000 + self.identity,
                "date": self.now[0],
                "from": {"id": OWNER, "is_bot": False},
                "chat": chat,
                "text": text,
                **({"reply_to_message": reply} if reply else {}),
            },
        }

    async def send(self, *args, **kwargs):
        update = self.update(*args, **kwargs)
        await self.service.process(BOT, update)
        return update

    def drain(self):
        result = []
        for _ in range(30):
            self.now[0] += 1
            delivery = PhoneNotifications(self.service).next(BOT, OWNER)
            if delivery is None:
                break
            self.message_id += 1
            self.service.store.acknowledge(delivery["delivery_id"], self.message_id)
            item = {
                "message_id": self.message_id,
                "date": self.now[0],
                "from": {"id": BOT, "is_bot": True},
                "chat": {"id": OWNER, "type": "private"},
                "text": delivery["text"],
                "reply_markup": delivery.get("reply_markup"),
            }
            self.sent.append(item)
            result.append(item)
        return result

    async def confirmation(self, text="Fix the bounded test task."):
        await self.send("/start")
        menu = self.drain()[-1]
        await self.send(data="compose:1:1", message=menu)
        prompt = self.drain()[-1]
        await self.send(text, reply=prompt)
        return self.drain()[-1]


@asynccontextmanager
async def phone_app(tmp_path, **settings):
    tmp_path.mkdir(parents=True, exist_ok=True)
    async with control_app(tmp_path) as (api, client):
        path = api.state.runtime.config
        config = json.loads(path.read_text())
        config["phone_control"] = {
            "enabled": True,
            "bot_id": BOT,
            "owner_id": OWNER,
            "projects": ["sample-project", "beta"],
            **settings,
        }
        path.write_text(json.dumps(config))
        policy = WorkbenchConfig.load(path).phone_control
        api.state.phone_control = PhoneControl(api.state, policy)
        yield PhoneHarness(api), client


def action_data(message, index=0):
    return message["reply_markup"]["inline_keyboard"][0][index]["callback_data"]


@pytest.mark.anyio
async def test_phone_task_flow_duplicate_taps_and_restart_create_exactly_one_job(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        update = h.update(data=action_data(card), message=card)
        await asyncio.gather(h.service.process(BOT, update), h.service.process(BOT, update))
        await h.send(data=action_data(card), message=card)
        jobs = h.api.state.job_repository.list()
        assert len(jobs) == 1 and jobs[0].state is JobState.CREATED
        job = jobs[0]
        assert job.request == "Fix the bounded test task."
        assert job.request_snapshot["agent_binding"]["profile"]["id"] == "coder"
        assert h.api.state.native_attempt_repository.get(job.id) is None
        assert [e.event_type for e in h.api.state.event_repository.list(job.id)] == [
            "phone.task.created"
        ]
        restarted = PhoneControl(h.api.state, h.service.policy, clock=lambda: h.now[0])
        await restarted.process(BOT, update)
        assert len(h.api.state.job_repository.list()) == 1


@pytest.mark.anyio
async def test_domain_commit_before_phone_result_recovers_without_second_mutation(
    tmp_path, monkeypatch
):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        update = h.update(data=action_data(card), message=card)
        original = h.service.store.finish_action
        monkeypatch.setattr(
            h.service.store, "finish_action", lambda *a: (_ for _ in ()).throw(SystemExit("crash"))
        )
        with pytest.raises(SystemExit):
            await h.service.process(BOT, update)
        job = h.api.state.job_repository.list()[0]
        monkeypatch.setattr(h.service.store, "finish_action", original)
        h.now[0] += 300
        restarted = PhoneControl(h.api.state, h.service.policy, clock=lambda: h.now[0])
        await restarted.process(BOT, update)
        assert len(h.api.state.job_repository.list()) == 1
        assert len(h.api.state.event_repository.list(job.id)) == 1
        with restarted.store._connection() as c:
            assert (
                c.execute("SELECT phase FROM phone_actions WHERE kind='submit'").fetchone()[0]
                == "done"
            )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "boundary",
    [
        "expiry",
        "wrong_owner",
        "group",
        "wrong_bot",
        "wrong_message",
        "revoked_agent",
        "policy_changed",
    ],
)
async def test_confirmation_fails_closed_when_authority_changes(tmp_path, boundary):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        update = h.update(data=action_data(card), message=copy.deepcopy(card))
        callback = update["callback_query"]
        if boundary == "expiry":
            h.now[0] += 120
        elif boundary == "wrong_owner":
            callback["from"]["id"] += 1
        elif boundary == "group":
            callback["message"]["chat"]["type"] = "group"
        elif boundary == "wrong_bot":
            callback["message"]["from"]["id"] += 1
        elif boundary == "wrong_message":
            callback["message"]["message_id"] += 100
        elif boundary == "revoked_agent":
            teams = h.api.state.team_repository
            value = teams.get("sample-project").to_dict()
            value["team"]["enabled"] = False
            teams.put("sample-project", value)
        else:
            config = json.loads(h.api.state.runtime.config.read_text())
            config["phone_control"]["enabled"] = False
            h.api.state.runtime.config.write_text(json.dumps(config))
        try:
            await h.service.process(BOT, update)
        except RuntimeError:
            assert boundary == "policy_changed"
        assert h.api.state.job_repository.list() == ()


@pytest.mark.anyio
async def test_cancellation_targets_one_attempt_and_never_reports_termination(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        attempt = claim(h.api)
        await h.send("/start")
        menu = h.drain()[-1]
        await h.send(data=f"task:{h.service.task_alias(attempt.job_id)}", message=menu)
        card = h.drain()[-1]
        await h.send(data=action_data(card), message=card)
        await h.send(data=action_data(card), message=card)
        current = h.api.state.native_attempt_repository.get(attempt.job_id)
        assert current.cancel_requested and current.held and current.phase == "admitted"
        records = h.api.state.event_repository.list(attempt.job_id)
        assert sum(e.event_type == "native.cancel.requested" for e in records) == 1
        cancellation = next(e for e in records if e.event_type == "native.cancel.requested")
        assert cancellation.payload["actor"] == f"telegram:{OWNER}"
        assert cancellation.payload["channel"] == "telegram"
        assert any("Cancellation requested" in m["text"] for m in h.drain())


@pytest.mark.anyio
async def test_one_prompt_cannot_prepare_two_distinct_requests(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        await h.send("/start")
        menu = h.drain()[-1]
        await h.send(data="compose:1:1", message=menu)
        prompt = h.drain()[-1]
        await h.send("First request", reply=prompt)
        await h.send("Second request", reply=prompt)
        with h.service.store._connection() as c:
            assert (
                c.execute("SELECT COUNT(*) FROM phone_actions WHERE kind='submit'").fetchone()[0]
                == 1
            )
        assert h.api.state.job_repository.list() == ()


@pytest.mark.anyio
async def test_simultaneous_identical_replies_cannot_prepare_two_confirmations(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        await h.send("/start")
        menu = h.drain()[-1]
        await h.send(data="compose:1:1", message=menu)
        prompt = h.drain()[-1]
        first = h.update("One intent", reply=prompt)
        second = h.update("One intent", reply=prompt)
        await asyncio.gather(h.service.process(BOT, first), h.service.process(BOT, second))
        cards = h.drain()
        assert len(cards) == 1
        await h.send(data=action_data(cards[0]), message=cards[0])
        assert len(h.api.state.job_repository.list()) == 1


@pytest.mark.anyio
async def test_phone_bridge_is_disabled_by_default_and_rejects_foreign_origins(tmp_path):
    async with control_app(tmp_path) as (_, client):
        assert (
            await client.post("/api/phone/connect", json={"bot_id": BOT, "owner_id": OWNER})
        ).status_code == 403
        assert (
            await client.post(
                "/api/phone/connect",
                json={"bot_id": BOT, "owner_id": OWNER},
                headers={"Origin": "https://foreign.invalid"},
            )
        ).status_code == 403
    async with phone_app(tmp_path / "second") as (h, client):
        connect = await client.post("/api/phone/connect", json={"bot_id": BOT, "owner_id": OWNER})
        assert connect.status_code == 200
        assert "account_slot" not in connect.text and "token" not in connect.text
        assert (
            await client.post(
                "/api/phone/updates",
                json={"bot_id": BOT, "update": h.update("/start"), "extra": True},
            )
        ).status_code == 422


@pytest.mark.anyio
async def test_transaction_failure_rolls_back_task_and_audit(tmp_path, monkeypatch):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        monkeypatch.setattr(
            EventRepository, "_append", lambda *a: (_ for _ in ()).throw(RuntimeError("synthetic"))
        )
        await h.send(data=action_data(card), message=card)
        assert h.api.state.job_repository.list() == ()
