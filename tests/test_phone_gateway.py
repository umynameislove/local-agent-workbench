from __future__ import annotations

import copy
import json
from urllib.parse import urlsplit

import pytest
from anyio import from_thread, to_thread
from test_phone_control import BOT, OWNER, action_data, phone_app
from test_telegram_gateway import TOKEN

from local_agent_workbench.phone_gateway import PhoneGateway
from local_agent_workbench.telegram_gateway import BotPollerLock, GatewayError


class FakePhoneTransport:
    def __init__(self, client):
        self.client, self.batch, self.sent, self.calls = client, [], [], []
        self.send_error = False
        self.ack_error = False

    async def local(self, path, payload):
        response = await self.client.post(path, json=payload)
        if response.status_code != 200:
            raise GatewayError("Synthetic local policy failure.")
        if path.endswith("/ack") and self.ack_error:
            raise GatewayError("Synthetic lost acknowledgement.")
        return response.json()

    def __call__(self, request, timeout):
        payload = json.loads(request.data)
        path = urlsplit(request.full_url).path
        self.calls.append((path.rsplit("/", 1)[-1], payload, timeout))
        if request.full_url.startswith("http://127.0.0.1"):
            return from_thread.run(self.local, path, payload)
        if path.endswith("getMe"):
            result = {"id": BOT, "is_bot": True}
        elif path.endswith("getUpdates"):
            result = copy.deepcopy(self.batch)
        elif path.endswith("answerCallbackQuery"):
            result = True
        elif path.endswith("sendMessage"):
            if self.send_error:
                raise GatewayError("Synthetic uncertain send.")
            result = {
                "message_id": 500 + len(self.sent),
                "date": 1,
                "from": {"id": BOT, "is_bot": True},
                "chat": {"id": OWNER, "type": "private"},
                **payload,
            }
            self.sent.append(result)
        else:
            raise AssertionError("Unexpected external operation")
        return {"ok": True, "result": result}


async def connect(h, transport):
    return await to_thread.run_sync(
        lambda: PhoneGateway(
            TOKEN,
            OWNER,
            h.service.policy.projects,
            runtime=h.api.state.runtime.root,
            read_json=transport,
            clock=lambda: h.now[0],
            allow_review_content=h.service.policy.review_content,
        )
    )


async def poll(h, bot, transport, update):
    transport.batch = [update]
    h.now[0] += 1
    await to_thread.run_sync(bot.poll_once)


@pytest.mark.anyio
async def test_fake_bot_to_real_backend_creates_one_task_without_execution(tmp_path):
    async with phone_app(tmp_path) as (h, client):
        transport = FakePhoneTransport(client)
        bot = await connect(h, transport)
        try:
            await poll(h, bot, transport, h.update("/start"))
            menu = transport.sent[-1]
            await poll(h, bot, transport, h.update(data="compose:1:1", message=menu))
            prompt = transport.sent[-1]
            await poll(h, bot, transport, h.update("Small bounded task.", reply=prompt))
            confirmation = transport.sent[-1]
            update = h.update(data=action_data(confirmation), message=confirmation)
            await poll(h, bot, transport, update)
            await poll(h, bot, transport, update)
            assert len(h.api.state.job_repository.list()) == 1
            assert "created" in transport.sent[-1]["text"]
            assert bot.offset == update["update_id"] + 1
            assert all(
                c[1]["protect_content"] and not c[1].get("allow_paid_broadcast", False)
                for c in transport.calls
                if c[0] == "sendMessage"
            )
            assert not any(c[0] in {"start", "apply", "promote"} for c in transport.calls)
        finally:
            bot.close()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["send", "ack"])
async def test_send_or_ack_loss_never_causes_resend_after_gateway_restart(tmp_path, failure):
    async with phone_app(tmp_path) as (h, client):
        transport = FakePhoneTransport(client)
        bot = await connect(h, transport)
        update = h.update("/start")
        transport.send_error, transport.ack_error = failure == "send", failure == "ack"
        try:
            with pytest.raises(GatewayError):
                await poll(h, bot, transport, update)
        finally:
            bot.close()
        sends_before = sum(c[0] == "sendMessage" for c in transport.calls)
        transport.send_error = transport.ack_error = False
        restarted = await connect(h, transport)
        try:
            await poll(h, restarted, transport, update)
            assert sum(c[0] == "sendMessage" for c in transport.calls) == sends_before
        finally:
            restarted.close()


@pytest.mark.anyio
async def test_status_and_control_poller_share_one_bot_lock(tmp_path):
    async with phone_app(tmp_path) as (h, client):
        lock = BotPollerLock(h.api.state.runtime.root, BOT)
        try:
            with pytest.raises(GatewayError, match="could not start"):
                await connect(h, FakePhoneTransport(client))
        finally:
            lock.close()
        bot = await connect(h, FakePhoneTransport(client))
        try:
            with pytest.raises(GatewayError):
                BotPollerLock(h.api.state.runtime.root, BOT)
        finally:
            bot.close()


@pytest.mark.anyio
async def test_gateway_requires_matching_backend_content_authorization(tmp_path):
    async with phone_app(tmp_path, review_content=True) as (h, client):
        transport = FakePhoneTransport(client)
        with pytest.raises(GatewayError):
            await to_thread.run_sync(
                lambda: PhoneGateway(
                    TOKEN,
                    OWNER,
                    h.service.policy.projects,
                    runtime=h.api.state.runtime.root,
                    read_json=transport,
                )
            )
        bot = await connect(h, transport)
        bot.close()
