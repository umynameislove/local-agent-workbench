from __future__ import annotations

import hashlib
from contextlib import contextmanager

from .db import DatabaseError, _Repository
from .team_store import encode


class RemoteJournalError(DatabaseError):
    """Remote receipt or reply state needs operator reconciliation."""


class RemoteJournal(_Repository):
    """Persist receipt identity and one conservative send attempt, never credentials."""

    error_type = RemoteJournalError
    storage_name = "Remote command"

    @contextmanager
    def _connection(self):
        with super()._connection() as connection:
            connection.execute("PRAGMA synchronous = EXTRA")
            yield connection

    @staticmethod
    def scope(owner: int, projects: tuple[str, ...], port: int) -> str:
        return hashlib.sha256(
            encode({"owner": owner, "projects": projects, "port": port}).encode()
        ).hexdigest()

    def get(self, channel: str, update_id: int) -> dict | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM remote_receipts WHERE channel=? AND update_id=?",
                (channel, update_id),
            ).fetchone()
            return None if row is None else dict(row)

    def receive(self, channel: str, update: dict, scope: str, now: int) -> tuple[dict, bool]:
        identity = update.get("update_id")
        if (
            not isinstance(channel, str)
            or not channel.isdecimal()
            or len(channel) > 20
            or type(identity) is not int
            or not 0 <= identity < 2**63
            or type(now) is not int
            or not isinstance(scope, str)
            or len(scope) != 64
        ):
            raise RemoteJournalError("Remote receipt identity is invalid.")
        try:
            payload = encode(update).encode()
        except (ValueError, TypeError, OverflowError):
            raise RemoteJournalError("Remote receipt is invalid.") from None
        if len(payload) > 262144:
            raise RemoteJournalError("Remote receipt is too large.")
        digest = hashlib.sha256(payload).hexdigest()
        message = update.get("message", {})
        message = message if isinstance(message, dict) else {}
        sender, chat = message.get("from", {}), message.get("chat", {})
        sender = sender if isinstance(sender, dict) else {}
        chat = chat if isinstance(chat, dict) else {}
        actor, chat_id, issued = sender.get("id"), chat.get("id"), message.get("date")

        def safe(value):
            return value if type(value) is int and -(2**63) < value < 2**63 else None

        actor, chat_id, issued = safe(actor), safe(chat_id), safe(issued)
        expires = now if issued is None else min(2**63 - 1, issued + 120)
        with self._write_connection() as connection:
            row = connection.execute(
                "SELECT * FROM remote_receipts WHERE channel=? AND update_id=?", (channel, identity)
            ).fetchone()
            if row is not None:
                if row["payload_hash"] != digest:
                    raise RemoteJournalError(
                        "Remote update identity was reused with different content."
                    )
                return dict(row), False
            connection.execute(
                """INSERT INTO remote_receipts
                (channel,update_id,payload_hash,scope_hash,actor,chat,issued_at,expires_at,phase)
                VALUES (?,?,?,?,?,?,?,?,'received')""",
                (channel, identity, digest, scope, actor, chat_id, issued, expires),
            )
        return self.get(channel, identity), True

    def complete(
        self, channel: str, update_id: int, reply: str | None, *, egress: tuple[str, ...] = ()
    ) -> dict:
        if reply is not None and (
            not isinstance(reply, str) or not reply or len(reply.encode()) > 16384
        ):
            raise RemoteJournalError("Remote reply is invalid.")
        if (
            not isinstance(egress, tuple)
            or len(egress) > 8
            or any(not isinstance(p, str) or not p or len(p) > 200 for p in egress)
        ):
            raise RemoteJournalError("Remote reply egress scope is invalid.")
        encoded = None if reply is None else encode({"text": reply, "projects": egress})
        if encoded is not None and len(encoded.encode()) > 16384:
            raise RemoteJournalError("Remote reply is too large.")
        with self._write_connection() as connection:
            row = connection.execute(
                "SELECT phase FROM remote_receipts WHERE channel=? AND update_id=?",
                (channel, update_id),
            ).fetchone()
            if row is None or row["phase"] != "received":
                raise RemoteJournalError(
                    "Remote result is already recorded or needs reconciliation."
                )
            connection.execute(
                "UPDATE remote_receipts SET phase=?,reply=? WHERE channel=? AND update_id=?",
                ("ignored" if reply is None else "ready", encoded, channel, update_id),
            )
        return self.get(channel, update_id)

    def claim(self, channel: str, update_id: int, scope: str, now: int) -> dict | None:
        with self._write_connection() as connection:
            row = connection.execute(
                "SELECT * FROM remote_receipts WHERE channel=? AND update_id=?",
                (channel, update_id),
            ).fetchone()
            if row is None or row["phase"] != "ready":
                return None
            if row["scope_hash"] != scope or now > row["expires_at"]:
                connection.execute(
                    "UPDATE remote_receipts SET phase='expired' WHERE channel=? AND update_id=?",
                    (channel, update_id),
                )
                return None
            connection.execute(
                "UPDATE remote_receipts SET phase='sending' WHERE channel=? AND update_id=?",
                (channel, update_id),
            )
        return self.get(channel, update_id)

    def finish(self, channel: str, update_id: int, message_id: int | None) -> None:
        if message_id is not None and (type(message_id) is not int or not 0 < message_id < 2**63):
            raise RemoteJournalError("Remote message identity is invalid.")
        with self._write_connection() as connection:
            row = connection.execute(
                "SELECT phase FROM remote_receipts WHERE channel=? AND update_id=?",
                (channel, update_id),
            ).fetchone()
            if row is None or row["phase"] != "sending":
                raise RemoteJournalError("Remote send result needs reconciliation.")
            connection.execute(
                "UPDATE remote_receipts SET phase=?,message_id=? WHERE channel=? AND update_id=?",
                ("uncertain" if message_id is None else "sent", message_id, channel, update_id),
            )

    def recover(self, channel: str) -> int:
        """A sending record from another process is never automatically sent again."""
        with self._write_connection() as connection:
            connection.execute(
                "UPDATE remote_receipts SET phase='uncertain' WHERE channel=? AND phase='sending'",
                (channel,),
            )
            return connection.execute(
                "SELECT COUNT(*) FROM remote_receipts "
                "WHERE channel=? AND phase IN ('received','uncertain')",
                (channel,),
            ).fetchone()[0]
