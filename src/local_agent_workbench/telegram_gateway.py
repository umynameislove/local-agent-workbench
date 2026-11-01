from __future__ import annotations

import argparse
import fcntl
import getpass
import json
import os
import re
import stat
import sys
import time
from collections.abc import Callable, Sequence
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .db import Database, DatabaseError
from .engine import ConfigurationError, resolve_runtime_home
from .remote_commands import RemoteJournal, RemoteJournalError


class GatewayError(RuntimeError):
    """A sanitized transport or control status failure."""


class BotPollerLock:
    """Keep status and control pollers exclusive within the same runtime home."""

    def __init__(self, runtime, bot_id):
        self.handle = None
        if not str(bot_id).isdecimal() or len(str(bot_id)) > 20:
            raise GatewayError("Bot identity is invalid.")
        try:
            descriptor = os.open(
                runtime / f"telegram-{bot_id}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
            )
            self.handle = os.fdopen(descriptor, "a")
            info = os.fstat(self.handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise GatewayError("Gateway lock is invalid.")
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, GatewayError):
            self.close()
            raise GatewayError(
                "Another poller is active or the gateway lock is unavailable."
            ) from None

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise GatewayError("Redirects are not permitted.")


def _read_json(request: Request, timeout: int) -> object:
    try:
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        with opener.open(request, timeout=timeout) as response:
            body = response.read(262145)
        if len(body) > 262144:
            raise ValueError
        return json.loads(body)
    except (OSError, URLError, ValueError, GatewayError):
        raise GatewayError("Transport failed. No message was retried.") from None


class TelegramGateway:
    """An explicitly enabled, single owner status reader. No execution authority."""

    def __init__(
        self,
        token: str,
        owner_id: int,
        projects: Sequence[str],
        *,
        port: int = 8765,
        read_json: Callable[[Request, int], object] = _read_json,
        clock: Callable[[], float] = time.time,
        journal: RemoteJournal | None = None,
    ) -> None:
        if not re.fullmatch(r"[0-9]{5,20}:[A-Za-z0-9_-]{20,128}", token):
            raise GatewayError("Bot token format is invalid.")
        if type(owner_id) is not int or not 0 < owner_id < 2**52:
            raise GatewayError("A positive numeric owner ID is required.")
        if type(port) is not int or not 1 <= port <= 65535:
            raise GatewayError("Local port is invalid.")
        if (
            isinstance(projects, str)
            or not 1 <= len(projects) <= 8
            or any(
                not isinstance(item, str) or not item.strip() or len(item) > 200
                for item in projects
            )
            or len(set(projects)) != len(projects)
        ):
            raise GatewayError("Select one to eight distinct projects explicitly.")
        self._token = token
        self.owner_id, self.projects, self.port = owner_id, tuple(projects), port
        self._read_json, self._clock = read_json, clock
        self.offset = 0
        self._last_update = self._clock()
        self._journal = journal
        self._scope = RemoteJournal.scope(owner_id, self.projects, port)
        self._channel = None
        if journal is not None:
            bot = self._telegram("getMe", {})
            if (
                not isinstance(bot, dict)
                or type(bot.get("id")) is not int
                or bot["id"] != int(token.split(":", 1)[0])
                or bot.get("is_bot") is not True
            ):
                raise GatewayError("Bot identity could not be verified.")
            self._channel = str(bot["id"])
            journal.recover(self._channel)

    def _telegram(self, method: str, payload: dict[str, object]) -> object:
        request = Request(
            f"https://api.telegram.org/bot{self._token}/{method}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        response = self._read_json(request, 35)
        if not isinstance(response, dict) or response.get("ok") is not True:
            raise GatewayError("Telegram rejected the request. No message was retried.")
        return response.get("result")

    def status(self) -> str:
        self._egress_projects = []
        lines = ["Local Agent Workbench", "Recorded task states, not live agent health."]
        for index, project in enumerate(self.projects, start=1):
            query = urlencode({"project_id": project})
            data = self._read_json(Request(f"http://127.0.0.1:{self.port}/api/control?{query}"), 5)
            if not isinstance(data, dict) or data.get("project_id") != project:
                raise GatewayError("Local status is unavailable.")
            sensitivity = data.get("sensitivity")
            if not isinstance(sensitivity, str):
                raise GatewayError("Project policy is unknown. Status withheld.")
            if sensitivity in {"internal", "restricted"}:
                lines.append(f"Project {index}: local only. Status withheld.")
                continue
            if sensitivity not in {"public", "private"}:
                raise GatewayError("Project policy is unknown. Status withheld.")
            total, attention = data.get("total"), data.get("needs_attention")
            if (
                data.get("basis") != "recorded_job_states"
                or any(
                    type(value) is not int or not 0 <= value <= 2**53
                    for value in (total, attention)
                )
                or attention > total
            ):
                raise GatewayError("Local status is invalid. Status withheld.")
            lines.append(f"Project {index}: {total} stored tasks, {attention} need attention.")
            self._egress_projects.append(project)
        lines.append("Quota unknown. Run, stop and approval commands are unavailable.")
        return "\n".join(lines)

    def _command(self, update: dict[str, object]) -> str | None:
        message = update.get("message")
        if not isinstance(message, dict) or any(
            key in message for key in ("forward_origin", "forward_date", "via_bot", "sender_chat")
        ):
            return None
        sender, chat, date = message.get("from"), message.get("chat"), message.get("date")
        if not isinstance(sender, dict) or not isinstance(chat, dict):
            return None
        if type(sender.get("id")) is not int or type(chat.get("id")) is not int:
            return None
        if sender["id"] != self.owner_id or chat["id"] != self.owner_id:
            return None
        if chat.get("type") != "private" or sender.get("is_bot") is not False:
            return None
        if type(date) is not int or not -30 <= self._clock() - date <= 120:
            return None
        text = message.get("text")
        return text if text in ("/start", "/help", "/status") else None

    def poll_once(self) -> None:
        if self._clock() - self._last_update >= 6 * 86400:
            self.offset = 0
        updates = self._telegram(
            "getUpdates",
            {
                "offset": self.offset,
                "timeout": 25,
                "limit": 20,
                "allowed_updates": ["message"],
            },
        )
        if not isinstance(updates, list) or len(updates) > 20:
            raise GatewayError("Telegram update batch is invalid.")
        for update in updates:
            if not isinstance(update, dict) or type(update.get("update_id")) is not int:
                raise GatewayError("Telegram update identity is invalid.")
            update_id = update["update_id"]
            if not 0 <= update_id < 2**63 - 1:
                raise GatewayError("Telegram update identity is invalid.")
            if update_id < self.offset and self._journal is None:
                continue
            if self._journal is not None:
                self._durable_update(update)
                self.offset = max(self.offset, update_id + 1)
                self._last_update = self._clock()
                continue
            self.offset = update_id + 1
            command = self._command(update)
            if command is None:
                continue
            if command == "/status":
                try:
                    text = self.status()
                except GatewayError:
                    text = (
                        "Local status unavailable. Check the Mac and backend. No task was changed."
                    )
            else:
                text = (
                    "Use /status for counts in your selected projects. "
                    "This bot cannot run, stop or approve tasks."
                )
            self._telegram("sendMessage", {"chat_id": self.owner_id, "text": text})

    def _reply(self, command: str) -> str:
        self._egress_projects = []
        if command != "/status":
            return (
                "Use /status for counts in your selected projects. "
                "This bot cannot run, stop or approve tasks."
            )
        try:
            return self.status()
        except GatewayError:
            self._egress_projects = []
            return "Local status unavailable. Check the Mac and backend. No task was changed."

    def _durable_update(self, update: dict) -> None:
        journal, channel = self._journal, self._channel
        assert journal is not None and channel is not None
        try:
            receipt, new = journal.receive(channel, update, self._scope, int(self._clock()))
            identity = update["update_id"]
            if new:
                command = self._command(update)
                receipt = journal.complete(
                    channel,
                    identity,
                    None if command is None else self._reply(command),
                    egress=tuple(self._egress_projects) if command is not None else (),
                )
            if receipt["phase"] != "ready":
                return
            # Recheck egress before a stored reply can leave this process.
            permitted = self._command(update) is not None
            try:
                reply = json.loads(receipt["reply"])
                if (
                    set(reply) != {"text", "projects"}
                    or not isinstance(reply["text"], str)
                    or not isinstance(reply["projects"], list)
                    or any(p not in self.projects for p in reply["projects"])
                ):
                    raise ValueError
            except (TypeError, ValueError):
                raise GatewayError("Stored reply scope is invalid.") from None
            for project in reply["projects"]:
                query = urlencode({"project_id": project})
                current = self._read_json(
                    Request(f"http://127.0.0.1:{self.port}/api/control?{query}"), 5
                )
                permitted = (
                    permitted
                    and isinstance(current, dict)
                    and current.get("project_id") == project
                    and current.get("sensitivity") in {"public", "private"}
                )
            receipt = journal.claim(
                channel, identity, self._scope if permitted else "revoked", int(self._clock())
            )
            if receipt is None:
                return
            try:
                message = self._telegram(
                    "sendMessage", {"chat_id": self.owner_id, "text": reply["text"]}
                )
                if (
                    not isinstance(message, dict)
                    or type(message.get("message_id")) is not int
                    or not isinstance(message.get("chat"), dict)
                    or message["chat"].get("id") != self.owner_id
                    or message["chat"].get("type") != "private"
                ):
                    raise GatewayError("Telegram send outcome is unconfirmed.")
                journal.finish(channel, identity, message["message_id"])
            except (GatewayError, RemoteJournalError):
                if journal.get(channel, identity)["phase"] == "sending":
                    journal.finish(channel, identity, None)
                raise GatewayError(
                    "Reply outcome needs reconciliation. It will not be resent."
                ) from None
        except RemoteJournalError:
            raise GatewayError(
                "Remote command state needs reconciliation. No command was replayed."
            ) from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read local task counts from one private Telegram chat."
    )
    parser.add_argument("--owner-id", type=int, required=True)
    parser.add_argument("--project", action="append", required=True)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--enable-control",
        action="store_true",
        help="Use the explicitly configured phone pilot instead of the status reader.",
    )
    parser.add_argument(
        "--allow-review-content",
        action="store_true",
        help="Allow complete bounded frozen reviews to Telegram, matching local policy.",
    )
    parser.add_argument(
        "--allow-metadata-egress",
        action="store_true",
        required=True,
        help="Allow selected project task metadata to Telegram. Status mode sends only counts.",
    )
    args = parser.parse_args(argv)
    if not sys.stdin.isatty():
        parser.error("Use an interactive terminal. Never pass tokens in arguments or a pipe.")
    print(
        "Telegram receives selected project metadata. Phone control also receives task requests"
        " and, only when enabled, frozen review content. Keep the Mac awake."
    )
    gateway, poller = None, None
    try:
        runtime = resolve_runtime_home()
        runtime.bootstrap()
        database = Database(runtime.state_db)
        database.initialize()
        token = getpass.getpass("Bot token (not stored): ")
        if args.enable_control:
            from .phone_gateway import PhoneGateway

            gateway = PhoneGateway(
                token,
                args.owner_id,
                args.project,
                port=args.port,
                runtime=runtime.root,
                allow_review_content=args.allow_review_content,
            )
            print(
                "Phone control started. Creation and approval never start or apply work"
                " automatically."
            )
        else:
            if args.allow_review_content:
                raise GatewayError("Review content requires phone control mode.")
            poller = BotPollerLock(runtime.root, token.split(":", 1)[0])
            gateway = TelegramGateway(
                token, args.owner_id, args.project, port=args.port, journal=RemoteJournal(database)
            )
            print("Status reader started. No execution commands are enabled.")
        while True:
            gateway.poll_once()
    except (KeyboardInterrupt, EOFError):
        return 0
    except (GatewayError, DatabaseError, ConfigurationError):
        print(
            "Status reader stopped after an error. Check configuration and connectivity.",
            file=sys.stderr,
        )
        return 1
    finally:
        if poller is not None:
            poller.close()
        if gateway is not None and hasattr(gateway, "close"):
            gateway.close()


if __name__ == "__main__":
    raise SystemExit(main())
