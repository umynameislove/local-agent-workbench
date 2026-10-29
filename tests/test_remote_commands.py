from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest
from test_telegram_gateway import OWNER, TOKEN, status, update

from local_agent_workbench.db import MIGRATIONS, Database
from local_agent_workbench.remote_commands import RemoteJournal, RemoteJournalError
from local_agent_workbench.telegram_gateway import GatewayError, TelegramGateway

CHANNEL = TOKEN.split(":")[0]


def journal(tmp_path):
    db = Database(tmp_path / "remote.db")
    db.initialize()
    return RemoteJournal(db)


def bot(store, *, batch=None, error=False, local=None, clock=None, response=None, owner=OWNER):
    calls = []

    def read(request, timeout):
        body = json.loads(request.data) if request.data else None
        calls.append((request.full_url.rsplit("/", 1)[-1], body))
        if request.full_url.startswith("http://127.0.0.1"):
            return status() if local is None else local
        if request.full_url.endswith("getMe"):
            return {"ok": True, "result": {"id": int(CHANNEL), "is_bot": True}}
        if request.full_url.endswith("getUpdates"):
            return {"ok": True, "result": [update()] if batch is None else batch}
        if error:
            raise GatewayError("Synthetic lost acknowledgement")
        return {
            "ok": True,
            "result": {"message_id": 7, "chat": {"id": owner, "type": "private"}}
            if response is None
            else response,
        }

    return TelegramGateway(
        TOKEN, owner, ["alpha"], read_json=read, clock=clock or (lambda: 1000), journal=store
    ), calls


def test_restart_duplicate_receipt_has_one_result_and_send(tmp_path):
    store = journal(tmp_path)
    first, calls = bot(store)
    first.poll_once()
    second, more = bot(RemoteJournal(Database(store.database.path)))
    second.poll_once()
    assert sum(name == "sendMessage" for name, _ in calls + more) == 1
    record = store.get(CHANNEL, 1)
    assert record["phase"] == "sent" and record["message_id"] == 7
    assert record["expires_at"] == 1120
    assert TOKEN not in json.dumps(record) and "payload" not in record


def test_same_identity_with_changed_payload_conflicts_without_second_send(tmp_path):
    store = journal(tmp_path)
    first, calls = bot(store)
    first.poll_once()
    changed, more = bot(store, batch=[update(text="/help")])
    with pytest.raises(GatewayError):
        changed.poll_once()
    assert not any(name == "sendMessage" for name, _ in more)
    assert store.get(CHANNEL, 1)["phase"] == "sent"
    assert first.offset == 2


@pytest.mark.parametrize(
    "response",
    [{}, {"message_id": True}, {"message_id": 8, "chat": {"id": OWNER + 1, "type": "private"}}],
)
def test_malformed_success_is_uncertain_and_never_resent(tmp_path, response):
    store = journal(tmp_path)
    first, _ = bot(store, response=response)
    with pytest.raises(GatewayError):
        first.poll_once()
    second, calls = bot(store)
    second.poll_once()
    assert store.get(CHANNEL, 1)["phase"] == "uncertain"
    assert not any(name == "sendMessage" for name, _ in calls)


def test_lost_reply_and_crash_after_claim_never_resend(tmp_path):
    store = journal(tmp_path)
    first, _ = bot(store, error=True)
    with pytest.raises(GatewayError):
        first.poll_once()
    scope = RemoteJournal.scope(OWNER, ("alpha",), 8765)
    store.receive(CHANNEL, update(2), scope, 1000)
    store.complete(CHANNEL, 2, "Previously prepared counts")
    assert store.claim(CHANNEL, 2, scope, 1000)
    second, calls = bot(store, batch=[update(), update(2)])
    second.poll_once()
    assert store.get(CHANNEL, 1)["phase"] == store.get(CHANNEL, 2)["phase"] == "uncertain"
    assert not any(name == "sendMessage" for name, _ in calls)


def test_concurrent_claim_has_one_winner_and_preserves_scope(tmp_path):
    store = journal(tmp_path)
    scope = RemoteJournal.scope(OWNER, ("alpha",), 8765)
    store.receive(CHANNEL, update(), scope, 1000)
    store.complete(CHANNEL, 1, "Counts")
    with ThreadPoolExecutor(max_workers=2) as workers:
        claims = list(
            workers.map(
                lambda _: RemoteJournal(Database(store.database.path)).claim(
                    CHANNEL, 1, scope, 1000
                ),
                range(2),
            )
        )
    assert sum(item is not None for item in claims) == 1
    with sqlite3.connect(store.database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE remote_receipts SET actor=42")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM remote_receipts")


@pytest.mark.parametrize("kind", ["scope", "expiry", "revoked-policy"])
def test_pending_reply_cannot_outlive_authority(tmp_path, kind):
    store = journal(tmp_path)
    scope = RemoteJournal.scope(OWNER, ("other" if kind == "scope" else "alpha",), 8765)
    store.receive(CHANNEL, update(), scope, 1000)
    store.complete(CHANNEL, 1, "Counts to withhold", egress=("alpha",))
    app, calls = bot(
        store,
        local=status(sensitivity="restricted") if kind == "revoked-policy" else None,
        clock=lambda: 1121 if kind == "expiry" else 1000,
    )
    app.poll_once()
    assert store.get(CHANNEL, 1)["phase"] == "expired"
    assert not any(name == "sendMessage" for name, _ in calls)


def test_offset_advances_only_after_durable_receipt_and_idle_reset_accepts_lower_id(
    tmp_path, monkeypatch
):
    store = journal(tmp_path)
    now = [1000]
    app, calls = bot(store, batch=[update(90)], clock=lambda: now[0])
    original = store.receive
    monkeypatch.setattr(
        store, "receive", lambda *a: (_ for _ in ()).throw(RemoteJournalError("Write failed"))
    )
    with pytest.raises(GatewayError):
        app.poll_once()
    assert app.offset == 0
    monkeypatch.setattr(store, "receive", original)
    app.poll_once()
    assert app.offset == 91
    now[0] += 7 * 86400
    fresh = update(2)
    fresh["message"]["date"] = now[0]
    app._read_json = bot(store, batch=[fresh], clock=lambda: now[0])[0]._read_json
    app.poll_once()
    assert store.get(CHANNEL, 2)["phase"] == "sent" and app.offset == 3


def test_migration_preserves_native_tables_and_journal_requests_extra_durability(tmp_path):
    old = Database(tmp_path / "old.db", migrations=MIGRATIONS[:13])
    assert old.initialize() == 13
    current = Database(old.path)
    assert current.initialize() == 14
    with RemoteJournal(current)._connection() as connection:
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM native_attempts").fetchone()[0] == 0


def test_unauthorized_callbacks_and_mutations_are_durably_ignored(tmp_path):
    store = journal(tmp_path)
    items = [
        update(1, "/run task"),
        update(2),
        {"update_id": 3, "callback_query": {"data": "approve"}},
    ]
    items[1]["message"]["from"]["id"] += 1
    app, calls = bot(store, batch=deepcopy(items))
    app.poll_once()
    assert not any(name == "sendMessage" for name, _ in calls)
    assert all(store.get(CHANNEL, n)["phase"] == "ignored" for n in (1, 2, 3))


@pytest.mark.parametrize("command", ["/help", "/status"])
def test_local_only_projects_still_receive_content_free_help_or_withheld_notice(tmp_path, command):
    store = journal(tmp_path)
    app, calls = bot(store, batch=[update(text=command)], local=status(sensitivity="restricted"))
    app.poll_once()
    sends = [body for name, body in calls if name == "sendMessage"]
    assert len(sends) == 1 and "8 stored tasks" not in sends[0]["text"]
    assert store.get(CHANNEL, 1)["phase"] == "sent"
