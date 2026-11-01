"""Durable phone intentions and conservative delivery, independent of bot tokens."""

from __future__ import annotations

import hashlib
import json
import re
import secrets

from .db import DatabaseError, _Repository
from .secret_gate import SecretScanner
from .team_store import encode


class PhoneError(DatabaseError):
    """Phone state cannot safely authorize the requested action."""


class PhoneStore(_Repository):
    error_type = PhoneError
    storage_name = "Phone control"

    def __init__(self, database, policy):
        super().__init__(database)
        self.policy = policy

    @property
    def scope(self):
        return self.policy.scope

    def cursor(self, connection):
        connection.execute(
            "INSERT OR IGNORE INTO phone_cursors(scope_hash) VALUES (?)", (self.scope,)
        )
        return connection.execute(
            "SELECT * FROM phone_cursors WHERE scope_hash=?", (self.scope,)
        ).fetchone()

    def action(self, token, connection=None):
        if not isinstance(token, str) or not re.fullmatch(r"[a-f0-9]{32}", token):
            raise PhoneError("Action is unavailable. Open the current task again.")
        if connection is None:
            with self._connection() as active:
                return self.action(token, active)
        row = connection.execute(
            "SELECT * FROM phone_actions WHERE token=? AND scope_hash=?", (token, self.scope)
        ).fetchone()
        if (
            row is None
            or hashlib.sha256(row["payload"].encode()).hexdigest() != row["payload_hash"]
        ):
            raise PhoneError("Action is unavailable. Open the current task again.")
        return dict(row) | {"data": json.loads(row["payload"])}

    def add_action(self, connection, kind, data, expires):
        token = secrets.token_hex(16)
        payload = encode(data)
        if len(payload.encode()) > 524288:
            raise PhoneError("Action is too large. Continue on the Mac.")
        connection.execute(
            "INSERT INTO phone_actions(token,scope_hash,kind,payload,payload_hash,expires_at) "
            "VALUES (?,?,?,?,?,?)",
            (
                token,
                self.scope,
                kind,
                payload,
                hashlib.sha256(payload.encode()).hexdigest(),
                expires,
            ),
        )
        return token

    def begin(self, token, choice, message_id, now):
        with self._write_connection() as connection:
            row = self.action(token, connection)
            delivered = connection.execute(
                "SELECT group_key FROM phone_outbox "
                "WHERE scope_hash=? AND action_token=? AND message_id=? AND phase='sent'",
                (self.scope, token, message_id),
            ).fetchone()
            if (
                delivered is None
                or connection.execute(
                    "SELECT 1 FROM phone_outbox "
                    "WHERE scope_hash=? AND group_key=? AND phase!='sent'",
                    (self.scope, delivered["group_key"]),
                ).fetchone()
            ):
                raise PhoneError("Review delivery is incomplete. Open the task again.")
            if row["phase"] == "ready":
                if now >= row["expires_at"]:
                    raise PhoneError("Action expired. Open the current task again.")
                connection.execute(
                    "UPDATE phone_actions SET phase='running',choice=? WHERE token=?",
                    (choice, token),
                )
            elif row["choice"] != choice:
                raise PhoneError("Another choice is already recorded for this action.")
            return self.action(token, connection)

    def finish_action(self, connection, token, outcome):
        row = self.action(token, connection)
        if row["phase"] == "done":
            if row["outcome"] != outcome:
                raise PhoneError("Action outcome conflicts with stored state.")
            return
        connection.execute(
            "UPDATE phone_actions SET phase='done',outcome=? WHERE token=? AND phase='running'",
            (outcome, token),
        )

    def queue(
        self,
        connection,
        key,
        packets,
        expires,
        *,
        project=None,
        kind="reply",
        subject=None,
        token=None,
    ):
        if not 1 <= len(packets) <= 8:
            raise PhoneError("Reply is too large. Continue on the Mac.")
        for part, packet in enumerate(packets):
            if (
                set(packet) - {"text", "reply_markup"}
                or not isinstance(packet.get("text"), str)
                or not 1 <= len(packet["text"].encode("utf-16-le")) // 2 <= 4000
                or SecretScanner.text_findings(packet["text"])
            ):
                raise PhoneError("Reply is invalid.")
            payload = encode(packet)
            connection.execute(
                """INSERT INTO phone_outbox(scope_hash,source_key,group_key,part,project_id,
                kind,subject,packet,packet_hash,action_token,expires_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scope_hash,source_key) DO NOTHING""",
                (
                    self.scope,
                    f"{key}:{part}",
                    key,
                    part,
                    project,
                    kind,
                    encode(subject or {}),
                    payload,
                    hashlib.sha256(payload.encode()).hexdigest(),
                    token,
                    expires,
                ),
            )

    @staticmethod
    def complete_receipt(connection, channel, identity):
        connection.execute(
            "UPDATE remote_receipts SET phase='ignored' "
            "WHERE channel=? AND update_id=? AND phase='received'",
            (channel, identity),
        )

    def receipt_pending(self, connection, identity):
        row = connection.execute(
            "SELECT phase,scope_hash FROM remote_receipts WHERE channel=? AND update_id=?",
            (str(self.policy.bot_id), identity),
        ).fetchone()
        return row is not None and row["phase"] == "received" and row["scope_hash"] == self.scope

    def sent_prompt(self, message_id):
        with self._connection() as connection:
            row = connection.execute(
                "SELECT action_token FROM phone_outbox "
                "WHERE scope_hash=? AND message_id=? AND phase='sent'",
                (self.scope, message_id),
            ).fetchone()
            return None if row is None else row["action_token"]

    def acknowledge(self, identity, message_id):
        if message_id is not None and (type(message_id) is not int or not 0 < message_id < 2**63):
            raise PhoneError("Delivery acknowledgement is invalid.")
        with self._write_connection() as connection:
            row = connection.execute(
                "SELECT phase,message_id FROM phone_outbox WHERE id=? AND scope_hash=?",
                (identity, self.scope),
            ).fetchone()
            if row is None:
                raise PhoneError("Delivery does not exist.")
            phase = "sent" if message_id is not None else "uncertain"
            if row["phase"] == phase and row["message_id"] == message_id:
                return
            if row["phase"] != "sending":
                raise PhoneError("Delivery requires reconciliation.")
            connection.execute(
                "UPDATE phone_outbox SET phase=?,message_id=? WHERE id=?",
                (phase, message_id, identity),
            )

    def recover(self):
        with self._write_connection() as connection:
            connection.execute(
                "UPDATE phone_outbox SET phase='uncertain' WHERE scope_hash=? AND phase='sending'",
                (self.scope,),
            )

    def delivery(self, now, eligible):
        with self._write_connection() as connection:
            cursor = self.cursor(connection)
            if now - cursor["last_send"] < 1:
                return None
            for row in connection.execute(
                "SELECT * FROM phone_outbox WHERE scope_hash=? AND phase='ready' "
                "ORDER BY CASE kind WHEN 'notice' THEN 1 ELSE 0 END,id LIMIT 100",
                (self.scope,),
            ).fetchall():
                row = dict(row)
                if hashlib.sha256(row["packet"].encode()).hexdigest() != row["packet_hash"]:
                    raise PhoneError("Stored delivery is unreadable.")
                if now >= row["expires_at"] or not eligible(connection, row):
                    connection.execute(
                        "UPDATE phone_outbox SET phase='dropped' WHERE id=?", (row["id"],)
                    )
                    continue
                if row["kind"] == "notice" and (cursor["muted"] or self.policy.quiet(now)):
                    continue
                earlier = connection.execute(
                    "SELECT phase FROM phone_outbox WHERE scope_hash=? AND group_key=? AND part<?",
                    (self.scope, row["group_key"], row["part"]),
                ).fetchall()
                if any(r["phase"] != "sent" for r in earlier):
                    continue
                connection.execute(
                    "UPDATE phone_outbox SET phase='sending' WHERE id=?", (row["id"],)
                )
                connection.execute(
                    "UPDATE phone_cursors SET last_send=? WHERE scope_hash=?", (now, self.scope)
                )
                return {
                    "delivery_id": row["id"],
                    "chat_id": self.policy.owner_id,
                    **json.loads(row["packet"]),
                }
        return None
