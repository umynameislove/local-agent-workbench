from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from engine import JobRuntime, RuntimeEvent, RuntimeEventKind
from write_boundary import WriteBoundary, WriteBoundaryError


class CodexNativeProtocolError(RuntimeError):
    """Report an identity violation without retaining provider output."""


class CodexEventTranslator:
    """Translate bounded Codex JSONL messages into shared runtime events."""

    def __init__(
        self,
        job_id: str,
        worktree: Path,
        *,
        first_sequence: int = 1,
        expected_session_id: str | None = None,
        max_line_bytes: int = 1_048_576,
        allowed_write_paths: tuple[str, ...] = (".",),
    ) -> None:
        if not isinstance(job_id, str) or not job_id.strip() or "\x00" in job_id:
            raise ValueError("Codex event job identity is invalid.")
        if not isinstance(worktree, Path) or not worktree.is_absolute():
            raise ValueError("Codex event worktree must be an absolute Path.")
        if type(first_sequence) is not int or first_sequence < 1:
            raise ValueError("Codex first event sequence must be positive.")
        if expected_session_id is not None and (
            not isinstance(expected_session_id, str)
            or not expected_session_id
            or "\x00" in expected_session_id
        ):
            raise ValueError("Expected Codex session identity is invalid.")
        if type(max_line_bytes) is not int or max_line_bytes < 1:
            raise ValueError("Codex stream limit must be positive.")
        self._job_id = job_id
        self._write_boundary = WriteBoundary(worktree, allowed_write_paths)
        self._next_sequence = first_sequence
        self._session_id: str | None = None
        self._expected_session_id = expected_session_id
        self._max_line_bytes = max_line_bytes
        self._completion_status: str | None = None

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def next_sequence(self) -> int:
        return self._next_sequence

    @property
    def completion_status(self) -> str | None:
        return self._completion_status

    def feed(self, line: bytes) -> tuple[RuntimeEvent, ...]:
        if not isinstance(line, bytes):
            raise TypeError("Codex stream line must be bytes.")
        if self._completion_status is not None:
            return ()
        if not line.strip():
            return ()
        if len(line) > self._max_line_bytes:
            return self.error("codex_message_too_large", "Codex stream message was too large.")
        try:
            value = self._decode(line)
        except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError):
            return self.error("codex_invalid_event", "Codex emitted an invalid event.")
        event_type = value.get("type")
        if event_type == "thread.started":
            self._thread_started(value.get("thread_id"))
            return ()
        if self._session_id is None:
            raise CodexNativeProtocolError("Codex emitted an event before its session identity.")
        if event_type == "turn.completed":
            return (*self._usage(value.get("usage")), *self.complete("completed"))
        if event_type == "turn.failed":
            return (
                *self.error("codex_turn_failed", "Codex turn failed."),
                *self.complete("failed"),
            )
        if event_type == "error":
            return (
                *self.error("codex_provider_error", "Codex reported an error."),
                *self.complete("failed"),
            )
        if event_type not in ("item.started", "item.updated", "item.completed"):
            return ()
        item = value.get("item")
        if not isinstance(item, Mapping):
            return self.error("codex_invalid_event", "Codex emitted an invalid event.")
        return self._item(event_type, item)

    def error(self, code: str, message: str) -> tuple[RuntimeEvent, ...]:
        return (
            self._event(
                RuntimeEventKind.ERROR,
                {"code": code, "message": message, "retryable": False},
            ),
        )

    def complete(self, status: str) -> tuple[RuntimeEvent, ...]:
        if self._completion_status is not None:
            return ()
        event = self._event(RuntimeEventKind.COMPLETION, {"status": status})
        self._completion_status = status
        return (event,)

    def _event(
        self,
        kind: RuntimeEventKind,
        payload: Mapping[str, str | int | bool | None],
    ) -> RuntimeEvent:
        event = RuntimeEvent(
            self._job_id,
            self._next_sequence,
            JobRuntime.CODEX,
            datetime.now(UTC),
            kind,
            payload,
        )
        self._next_sequence += 1
        return event

    def _thread_started(self, value: object) -> None:
        if not isinstance(value, str) or not value or "\x00" in value:
            raise CodexNativeProtocolError("Codex session identity is invalid.")
        if self._expected_session_id is not None and value != self._expected_session_id:
            raise CodexNativeProtocolError("Codex session identity changed unexpectedly.")
        if self._session_id is not None and value != self._session_id:
            raise CodexNativeProtocolError("Codex session identity changed unexpectedly.")
        self._session_id = value

    def _item(self, event_type: str, item: Mapping[str, Any]) -> tuple[RuntimeEvent, ...]:
        item_type = item.get("type")
        completed = event_type == "item.completed"
        text = self._safe_text(item.get("text"))
        if completed and item_type == "agent_message":
            return (
                (self._event(RuntimeEventKind.TEXT, {"text": text}),)
                if text is not None
                else self.error("codex_invalid_event", "Codex emitted an invalid event.")
            )
        if completed and item_type == "reasoning":
            return (
                (self._event(RuntimeEventKind.PLAN, {"text": text}),)
                if text is not None
                else self.error("codex_invalid_event", "Codex emitted an invalid event.")
            )
        if item_type == "todo_list":
            return self._todo_list(item)
        if event_type != "item.updated" and item_type in (
            "command_execution",
            "mcp_tool_call",
            "web_search",
        ):
            call_id = item.get("id")
            if not isinstance(call_id, str) or not call_id or "\x00" in call_id:
                call_id = f"codex-call-{self._next_sequence}"
            status = "completed" if completed else "started"
            if completed and item.get("status") == "failed":
                status = "failed"
            return (
                self._event(
                    RuntimeEventKind.TOOL,
                    {"call_id": call_id, "name": str(item_type), "status": status},
                ),
            )
        if completed and item_type == "file_change" and item.get("status") == "failed":
            return self.error("codex_file_change_failed", "Codex could not apply a file change.")
        if completed and item_type == "file_change":
            return (
                self._file_changes(item)
                if item.get("status") == "completed"
                else self.error("codex_invalid_event", "Codex emitted an invalid event.")
            )
        return ()

    def _todo_list(self, item: Mapping[str, Any]) -> tuple[RuntimeEvent, ...]:
        items = item.get("items")
        if not isinstance(items, list):
            return self.error("codex_invalid_event", "Codex emitted an invalid event.")
        lines: list[str] = []
        for entry in items:
            if not isinstance(entry, Mapping) or type(entry.get("completed")) is not bool:
                return self.error("codex_invalid_event", "Codex emitted an invalid event.")
            text = self._safe_text(entry.get("text"))
            if text is None:
                return self.error("codex_invalid_event", "Codex emitted an invalid event.")
            marker = "done" if entry["completed"] else "pending"
            lines.append(f"{marker}: {text}")
        if not lines:
            return ()
        return (self._event(RuntimeEventKind.PLAN, {"text": "\n".join(lines)}),)

    @staticmethod
    def _safe_text(value: object) -> str | None:
        return value if isinstance(value, str) and "\x00" not in value else None

    def _usage(self, usage: object) -> tuple[RuntimeEvent, ...]:
        if not isinstance(usage, Mapping):
            return ()

        def count(name: str) -> int | None:
            value = usage.get(name)
            return value if type(value) is int and value >= 0 else None

        return (
            self._event(
                RuntimeEventKind.USAGE,
                {
                    "input_tokens": count("input_tokens"),
                    "output_tokens": count("output_tokens"),
                    "cost_usd": None,
                },
            ),
        )

    def _file_changes(self, item: Mapping[str, Any]) -> tuple[RuntimeEvent, ...]:
        changes = item.get("changes")
        if not isinstance(changes, list):
            changes = [item]
        actions = {
            "add": "created",
            "create": "created",
            "delete": "deleted",
            "modify": "modified",
            "update": "modified",
        }
        normalized: list[tuple[str, str]] = []
        for change in changes:
            if not isinstance(change, Mapping):
                raise CodexNativeProtocolError("Codex file change violated write policy.")
            requested_action = change.get("kind") or change.get("action")
            action = actions.get(requested_action) if isinstance(requested_action, str) else None
            if action is None:
                raise CodexNativeProtocolError("Codex file change violated write policy.")
            path = self._relative_path(change.get("path"))
            normalized.append((path, action))
        if not normalized:
            raise CodexNativeProtocolError("Codex file change violated write policy.")
        return tuple(
            self._event(RuntimeEventKind.FILE, {"path": path, "action": action})
            for path, action in normalized
        )

    def _relative_path(self, value: object) -> str:
        if not isinstance(value, str):
            raise CodexNativeProtocolError("Codex file change violated write policy.")
        try:
            return self._write_boundary.relative_path(value)
        except (TypeError, ValueError, WriteBoundaryError) as error:
            raise CodexNativeProtocolError("Codex file change violated write policy.") from error

    @staticmethod
    def _decode(line: bytes) -> dict[str, Any]:
        def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError
                result[key] = value
            return result

        def reject_constant(_value: str) -> None:
            raise ValueError

        value = json.loads(
            line.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
        if not isinstance(value, dict):
            raise ValueError
        return value
