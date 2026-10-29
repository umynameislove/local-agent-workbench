from __future__ import annotations

import json
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request

import pytest
from anyio import from_thread, to_thread
from test_workspace import stored_job, workspace

from local_agent_workbench.telegram_gateway import (
    GatewayError,
    TelegramGateway,
    _NoRedirect,
    _read_json,
    main,
)

TOKEN = "".join(("123456789", ":", "x" * 30))
OWNER = 123456


def update(identity: int = 1, text: str = "/status") -> dict:
    return {
        "update_id": identity,
        "message": {
            "from": {"id": OWNER, "is_bot": False},
            "chat": {"id": OWNER, "type": "private"},
            "date": 1000,
            "text": text,
        },
    }


def status(project: str = "alpha", **changes) -> dict:
    return {
        "project_id": project,
        "sensitivity": "private",
        "basis": "recorded_job_states",
        "total": 8,
        "needs_attention": 2,
        **changes,
    }


def gateway(batch=None, local=None, send_error=False):
    calls = []

    def read(request, timeout):
        calls.append((request, timeout))
        if request.full_url.startswith("http://127.0.0.1"):
            return status() if local is None else local
        if request.full_url.endswith("getUpdates"):
            return {"ok": True, "result": [update()] if batch is None else batch}
        if send_error:
            raise GatewayError("Connection lost")
        return {"ok": True, "result": {}}

    return TelegramGateway(TOKEN, OWNER, ["alpha"], read_json=read, clock=lambda: 1000), calls


def test_authorized_poll_sends_only_counts_and_confirms_without_repeating():
    bot, calls = gateway()
    bot.poll_once()
    bot.poll_once()
    sends = [json.loads(req.data) for req, _ in calls if req.full_url.endswith("sendMessage")]
    assert len(sends) == 1
    assert sends[0]["chat_id"] == OWNER
    assert "8 stored tasks, 2 need attention" in sends[0]["text"]
    assert "alpha" not in sends[0]["text"]
    polls = [json.loads(req.data) for req, _ in calls if req.full_url.endswith("getUpdates")]
    assert polls[1]["offset"] == 2
    assert polls[0]["allowed_updates"] == ["message"]
    assert all(
        req.full_url.startswith(("https://api.telegram.org/bot", "http://127.0.0.1:"))
        for req, _ in calls
    )


@pytest.mark.parametrize(
    "change",
    [
        {"from": {"id": OWNER + 1, "is_bot": False}},
        {"from": {"id": str(OWNER), "is_bot": False}},
        {"from": {"id": OWNER, "is_bot": True}},
        {"chat": {"id": OWNER + 1, "type": "private"}},
        {"chat": {"id": OWNER, "type": "group"}},
        {"date": 879},
        {"date": 1031},
        {"date": True},
        {"forward_origin": {}},
        {"forward_date": 1000},
        {"via_bot": {}},
        {"sender_chat": {}},
        {"text": "/run rm everything"},
        {"text": "/approve"},
        {"text": "/status alpha"},
        {"text": "/status@anotherbot"},
        {"text": {"unexpected": "object"}},
    ],
)
def test_unauthorized_or_unsupported_commands_do_not_read_or_send(change):
    item = deepcopy(update())
    item["message"].update(change)
    bot, calls = gateway([item])
    bot.poll_once()
    assert len(calls) == 1


def test_edited_messages_and_help_have_no_operational_authority():
    edited = update()
    edited["edited_message"] = edited.pop("message")
    bot, calls = gateway([edited, update(2, "/help")])
    bot.poll_once()
    assert len(calls) == 2
    assert "cannot run, stop or approve" in json.loads(calls[-1][0].data)["text"]


@pytest.mark.parametrize("policy", ["internal", "restricted", "unknown", None])
def test_local_only_and_unknown_policy_never_export_counts(policy):
    bot, calls = gateway(local=status(sensitivity=policy))
    bot.poll_once()
    assert "8 stored" not in json.loads(calls[-1][0].data)["text"]


@pytest.mark.parametrize(
    "changes",
    [
        {"total": True},
        {"total": -1},
        {"needs_attention": 9},
        {"project_id": "beta"},
        {"basis": "estimated"},
        {"total": "secret"},
    ],
)
def test_invalid_status_is_not_forwarded(changes):
    bot, calls = gateway(local=status(**changes))
    bot.poll_once()
    assert json.loads(calls[-1][0].data)["text"].startswith("Local status unavailable.")


def test_uncertain_send_and_malformed_batches_stop_without_retry():
    bot, calls = gateway(send_error=True)
    with pytest.raises(GatewayError):
        bot.poll_once()
    assert sum(req.full_url.endswith("sendMessage") for req, _ in calls) == 1
    for batch in ({}, [None], [{"update_id": True}], [{"update_id": -1}]):
        bot, calls = gateway(batch)
        with pytest.raises(GatewayError):
            bot.poll_once()
        assert len(calls) == 1


def test_token_and_network_failures_are_sanitized(monkeypatch):
    class Opener:
        def open(self, *args, **kwargs):
            raise URLError("https://api.telegram.org/bot" + TOKEN)

    monkeypatch.setattr(
        "local_agent_workbench.telegram_gateway.build_opener", lambda *args: Opener()
    )
    with pytest.raises(GatewayError) as error:
        _read_json(Request("https://api.telegram.org/bot" + TOKEN), 1)
    assert TOKEN not in str(error.value)
    with pytest.raises(GatewayError):
        _NoRedirect().redirect_request(None, None, 302, None, None, "https://foreign.invalid")


@pytest.mark.parametrize("body", [b"not json", b"x" * 262145])
def test_transport_bounds_untrusted_responses_and_disables_proxies(monkeypatch, body):
    handlers = []

    class Reply(BytesIO):
        pass

    class Opener:
        def open(self, request, timeout):
            assert timeout == 5
            return Reply(body)

    def build(*items):
        handlers.extend(items)
        return Opener()

    monkeypatch.setattr("local_agent_workbench.telegram_gateway.build_opener", build)
    with pytest.raises(GatewayError):
        _read_json(Request("http://127.0.0.1:8765/api/control"), 5)
    assert handlers[0].proxies == {}
    assert isinstance(handlers[1], _NoRedirect)


@pytest.mark.parametrize(
    "values",
    [
        {"token": "bad"},
        {"owner_id": True},
        {"owner_id": -1},
        {"port": 0},
        {"port": True},
        {"projects": []},
        {"projects": ["alpha", "alpha"]},
        {"projects": "alpha"},
    ],
)
def test_invalid_configuration_fails_before_network(values):
    with pytest.raises(GatewayError):
        TelegramGateway(**{"token": TOKEN, "owner_id": OWNER, "projects": ["alpha"], **values})


def test_cli_requires_explicit_egress_and_interactive_token(monkeypatch):
    with pytest.raises(SystemExit):
        main(["--owner-id", str(OWNER), "--project", "alpha"])
    monkeypatch.setattr("sys.stdin", BytesIO())
    with pytest.raises(SystemExit):
        main(["--owner-id", str(OWNER), "--project", "alpha", "--allow-metadata-egress"])


@pytest.mark.anyio
async def test_bot_to_local_api_reads_real_storage_without_mutating_it(tmp_path: Path):
    async with workspace(tmp_path) as (api, client):
        api.state.job_repository.create(stored_job("durable-task"))
        before = api.state.job_repository.list()
        sent = []

        def read(request, timeout):
            if request.full_url.startswith("http://127.0.0.1"):
                parsed = urlsplit(request.full_url)
                return from_thread.run(client.get, parsed.path + "?" + parsed.query).json()
            if request.full_url.endswith("getUpdates"):
                return {"ok": True, "result": [update()]}
            sent.append(json.loads(request.data))
            return {"ok": True, "result": {}}

        bot = TelegramGateway(TOKEN, OWNER, ["alpha"], read_json=read, clock=lambda: 1000)
        await to_thread.run_sync(bot.poll_once)
        assert "1 stored tasks, 0 need attention" in sent[0]["text"]
        assert "untrusted" not in json.dumps(sent)
        assert api.state.job_repository.list() == before
