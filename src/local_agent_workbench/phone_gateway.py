"""Explicitly activated transport for one configured phone owner and bot."""

from __future__ import annotations

import json
from urllib.request import Request

from .telegram_gateway import BotPollerLock, GatewayError, TelegramGateway


class PhoneGateway(TelegramGateway):
    def __init__(self, *args, runtime, allow_review_content=False, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = None
        bot = self._telegram("getMe", {})
        if (
            not isinstance(bot, dict)
            or type(bot.get("id")) is not int
            or bot["id"] != int(self._token.split(":")[0])
            or bot.get("is_bot") is not True
        ):
            raise GatewayError("Bot identity is invalid.")
        self.bot_id = bot["id"]
        try:
            self._lock = BotPollerLock(runtime, self.bot_id)
            configured = self._local("connect", self.identity)
            if (
                not isinstance(configured, dict)
                or configured.get("owner_id") != self.owner_id
                or configured.get("bot_id") != self.bot_id
                or configured.get("projects") != list(self.projects)
                or configured.get("review_content") is not allow_review_content
            ):
                raise GatewayError("Gateway scope differs from backend policy.")
        except (OSError, GatewayError):
            self.close()
            raise GatewayError("Gateway could not start. Check policy and other pollers.") from None

    @property
    def identity(self):
        return {"bot_id": self.bot_id, "owner_id": self.owner_id}

    def _local(self, operation, payload):
        request = Request(
            f"http://127.0.0.1:{self.port}/api/phone/{operation}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        return self._read_json(request, 30)

    def close(self):
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def poll_once(self):
        if self._clock() - self._last_update >= 6 * 86400:
            self.offset = 0
        updates = self._telegram(
            "getUpdates",
            {
                "offset": self.offset,
                "timeout": 1,
                "limit": 20,
                "allowed_updates": ["message", "callback_query"],
            },
        )
        if not isinstance(updates, list) or len(updates) > 20:
            raise GatewayError("Update batch is invalid.")
        for update in updates:
            if (
                not isinstance(update, dict)
                or type(update.get("update_id")) is not int
                or not 0 <= update["update_id"] < 2**63 - 1
            ):
                raise GatewayError("Update identity is invalid.")
            response = self._local("updates", {"bot_id": self.bot_id, "update": update})
            if response != {"accepted": True}:
                raise GatewayError("Command receipt is unconfirmed. Check stored state.")
            self.offset = max(self.offset, update["update_id"] + 1)
            self._last_update = self._clock()
            callback = update.get("callback_query")
            if (
                isinstance(callback, dict)
                and isinstance(callback.get("from"), dict)
                and callback["from"].get("id") == self.owner_id
                and isinstance(callback.get("id"), str)
            ):
                self._telegram("answerCallbackQuery", {"callback_query_id": callback.get("id")})
        response = self._local("next", self.identity)
        if not isinstance(response, dict) or set(response) != {"delivery"}:
            raise GatewayError("Delivery state is invalid.")
        delivery = response["delivery"]
        if delivery is None:
            return
        if (
            not isinstance(delivery, dict)
            or set(delivery) - {"delivery_id", "chat_id", "text", "reply_markup"}
            or type(delivery.get("delivery_id")) is not int
            or type(delivery.get("chat_id")) is not int
            or delivery["chat_id"] != self.owner_id
            or not isinstance(delivery.get("text"), str)
        ):
            raise GatewayError("Delivery target is invalid.")
        identity = delivery.pop("delivery_id")
        try:
            message = self._telegram(
                "sendMessage",
                {
                    **delivery,
                    "protect_content": True,
                    "link_preview_options": {"is_disabled": True},
                },
            )
            if (
                not isinstance(message, dict)
                or type(message.get("message_id")) is not int
                or message["message_id"] <= 0
                or message.get("chat", {}).get("id") != self.owner_id
                or message.get("chat", {}).get("type") != "private"
            ):
                raise GatewayError("Telegram delivery outcome is unconfirmed.")
        except GatewayError:
            self._local("ack", {**self.identity, "delivery_id": identity, "message_id": None})
            raise GatewayError("Delivery outcome is uncertain. It will not be resent.") from None
        result = self._local(
            "ack", {**self.identity, "delivery_id": identity, "message_id": message["message_id"]}
        )
        if result != {"accepted": True}:
            raise GatewayError("Delivery acknowledgement is unconfirmed. It will not be resent.")
