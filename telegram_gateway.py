from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
import time
from collections.abc import Callable, Sequence
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class GatewayError(RuntimeError):
    """A sanitized transport or control status failure."""


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
            if update_id < 0:
                raise GatewayError("Telegram update identity is invalid.")
            if update_id < self.offset:
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read local task counts from one private Telegram chat."
    )
    parser.add_argument("--owner-id", type=int, required=True)
    parser.add_argument("--project", action="append", required=True)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--allow-metadata-egress",
        action="store_true",
        required=True,
        help="Allow selected project counts to Telegram. No task content is sent.",
    )
    args = parser.parse_args(argv)
    if not sys.stdin.isatty():
        parser.error("Use an interactive terminal. Never pass tokens in arguments or a pipe.")
    print("Telegram will receive aggregate counts for the selected projects. Keep the Mac awake.")
    try:
        gateway = TelegramGateway(
            getpass.getpass("Bot token (not stored): "), args.owner_id, args.project, port=args.port
        )
        print("Status reader started. Press Ctrl+C to stop. No execution commands are enabled.")
        while True:
            gateway.poll_once()
    except (KeyboardInterrupt, EOFError):
        return 0
    except GatewayError:
        print(
            "Status reader stopped after an error. Check configuration and connectivity.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
