"""Explicit Telegram authority and content boundaries, disabled by default."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class PhonePolicy:
    enabled: bool = False
    owner_id: int | None = None
    bot_id: int | None = None
    projects: tuple[str, ...] = ()
    notifications: bool = False
    review_content: bool = False
    timezone: str = "UTC"
    quiet_start: str = "22:00"
    quiet_end: str = "08:00"

    @classmethod
    def from_dict(cls, value: object) -> PhonePolicy:
        if not isinstance(value, dict) or set(value) - set(cls.__dataclass_fields__):
            raise ValueError("Phone control fields are invalid.")
        body = {**asdict(cls()), **value}
        for flag in ("enabled", "notifications", "review_content"):
            if type(body[flag]) is not bool:
                raise ValueError("Phone control flags must be booleans.")
        for field in ("owner_id", "bot_id"):
            identity = body[field]
            if identity is not None and (type(identity) is not int or not 0 < identity < 2**52):
                raise ValueError("Phone control requires positive numeric identities.")
        projects = body["projects"]
        if (
            not isinstance(projects, (list, tuple))
            or len(projects) > 8
            or any(
                not isinstance(p, str) or not p or p != p.strip() or len(p) > 200 for p in projects
            )
            or len(set(projects)) != len(projects)
        ):
            raise ValueError("Phone control projects are invalid.")
        body["projects"] = tuple(projects)
        if body["enabled"] and (not projects or not body["owner_id"] or not body["bot_id"]):
            raise ValueError("Phone control needs an owner, bot and selected projects.")
        try:
            ZoneInfo(body["timezone"])
        except (ZoneInfoNotFoundError, TypeError, ValueError) as error:
            raise ValueError("Phone control timezone is invalid.") from error
        for field in ("quiet_start", "quiet_end"):
            if not isinstance(body[field], str) or not re.fullmatch(
                r"(?:[01]\d|2[0-3]):[0-5]\d", body[field]
            ):
                raise ValueError("Quiet hours must use HH:MM.")
        return cls(**body)

    @property
    def scope(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()

    def quiet(self, timestamp: int) -> bool:
        now = (
            datetime.fromtimestamp(timestamp, UTC)
            .astimezone(ZoneInfo(self.timezone))
            .strftime("%H:%M")
        )
        start, end = self.quiet_start, self.quiet_end
        return (
            start <= now < end
            if start < end
            else now >= start or now < end
            if start > end
            else False
        )

    def owner_message(self, update: dict, now: int) -> dict | None:
        if set(update) - {"update_id", "message", "callback_query"} or ("message" in update) == (
            "callback_query" in update
        ):
            return None
        callback = update.get("callback_query")
        message = callback.get("message") if isinstance(callback, dict) else update.get("message")
        if not isinstance(message, dict) or any(
            k in message
            for k in ("forward_origin", "forward_date", "via_bot", "sender_chat", "edit_date")
        ):
            return None
        sender = callback.get("from") if isinstance(callback, dict) else message.get("from")
        chat = message.get("chat")
        if not isinstance(sender, dict) or not isinstance(chat, dict):
            return None
        if (
            type(sender.get("id")) is not int
            or sender["id"] != self.owner_id
            or sender.get("is_bot") is not False
        ):
            return None
        if (
            type(chat.get("id")) is not int
            or chat["id"] != self.owner_id
            or chat.get("type") != "private"
        ):
            return None
        if type(message.get("message_id")) is not int or message["message_id"] <= 0:
            return None
        if callback is not None:
            author = message.get("from", {})
            if not isinstance(author, dict):
                return None
            if (
                author.get("id") != self.bot_id
                or author.get("is_bot") is not True
                or not isinstance(callback.get("id"), str)
            ):
                return None
            return message
        issued = message.get("date")
        return message if type(issued) is int and -30 <= now - issued < 120 else None
