"""Structured logging with safe defaults for Local Agent Workbench."""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Mapping, Set
from datetime import UTC, datetime
from typing import Any

LOGGER_NAME = "local_agent_workbench"
REDACTED = "[REDACTED]"
REDACTED_PATH = "[REDACTED_PATH]"

DEFAULT_SENSITIVE_FIELDS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "credential",
        "env",
        "environment",
        "file_content",
        "file_contents",
        "password",
        "private_key",
        "prompt",
        "refresh_token",
        "secret",
        "task_payload",
        "token",
    }
)

_MANAGED_HANDLER_ATTRIBUTE = "_local_agent_workbench_managed"
_PRIVATE_KEY_PATTERN = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)
_AUTHORIZATION_PATTERN = re.compile(
    r"(?i)\bauthorization\s*[:=]\s*(?:(?:bearer|basic)\s+)?[^\s,;]+"
)
_COOKIE_PATTERN = re.compile(r"(?i)\b(?P<field>cookie|set-cookie)\s*:\s*[^\r\n]+")
_BEARER_PATTERN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(?P<field>api[_ -]?key|authorization|password|secret|credential|cookie|"
    r"access[_ -]?token|refresh[_ -]?token|private[_ -]?key|token|prompt|"
    r"task[_ -]?payload|environment|env|file[_ -]?contents?)\b"
    r"(?P<separator>\s*[:=]\s*)"
    r"(?:(?P<quote>[\"'])(?P<quoted>.*?)(?P=quote)|(?P<bare>[^\s,;&]+))"
)
_PERSONAL_PATH_PATTERNS = (
    re.compile(r"/Users/[^/\s]+(?:/[^\s\"']*)?"),
    re.compile(r"/home/[^/\s]+(?:/[^\s\"']*)?"),
    re.compile(r"(?i)[A-Z]:\\Users\\[^\\\s]+(?:\\[^\s\"']*)?"),
)


def _normalize_field(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def _sensitive_field_names(custom_fields: Set[str] | None = None) -> frozenset[str]:
    fields = set(DEFAULT_SENSITIVE_FIELDS)
    if custom_fields:
        fields.update(custom_fields)
    return frozenset(_normalize_field(field) for field in fields)


def redact_text(value: str) -> str:
    """Redact credentials and personal home prefixes embedded in free text."""

    redacted = _PRIVATE_KEY_PATTERN.sub(REDACTED, value)
    redacted = _AUTHORIZATION_PATTERN.sub(f"Authorization: {REDACTED}", redacted)
    redacted = _COOKIE_PATTERN.sub(lambda match: f"{match.group('field')}: {REDACTED}", redacted)
    redacted = _BEARER_PATTERN.sub(REDACTED, redacted)

    def replace_assignment(match: re.Match[str]) -> str:
        return f"{match.group('field')}{match.group('separator')}{REDACTED}"

    redacted = _ASSIGNMENT_PATTERN.sub(replace_assignment, redacted)
    for pattern in _PERSONAL_PATH_PATTERNS:
        redacted = pattern.sub(REDACTED_PATH, redacted)
    return redacted


def redact(value: Any, *, sensitive_fields: Set[str] | None = None) -> Any:
    """Return a JSON compatible redacted copy without mutating the input."""

    names = _sensitive_field_names(sensitive_fields)
    return _redact(value, names=names, ancestors=frozenset())


def _redact(value: Any, *, names: frozenset[str], ancestors: frozenset[int]) -> Any:
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, bytes):
        return redact_text(value.decode("utf-8", errors="replace"))

    if isinstance(value, Mapping):
        identity = id(value)
        if identity in ancestors:
            return "[CIRCULAR]"
        next_ancestors = ancestors | {identity}
        result: dict[str, Any] = {}
        for key, item in value.items():
            output_key = str(key)
            if _normalize_field(key) in names:
                result[output_key] = REDACTED
            else:
                result[output_key] = _redact(item, names=names, ancestors=next_ancestors)
        return result

    if isinstance(value, list | tuple):
        identity = id(value)
        if identity in ancestors:
            return "[CIRCULAR]"
        next_ancestors = ancestors | {identity}
        return [_redact(item, names=names, ancestors=next_ancestors) for item in value]

    if isinstance(value, Set):
        identity = id(value)
        if identity in ancestors:
            return "[CIRCULAR]"
        next_ancestors = ancestors | {identity}
        ordered = sorted(value, key=repr)
        return [_redact(item, names=names, ancestors=next_ancestors) for item in ordered]

    return redact_text(str(value))


class JsonLogFormatter(logging.Formatter):
    """Format records as stable JSON after redacting all public fields."""

    def __init__(self, *, sensitive_fields: Set[str] | None = None) -> None:
        super().__init__()
        self.sensitive_fields = frozenset(sensitive_fields or ())

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", "log.message")
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "event": redact_text(str(event)),
            "message": redact_text(record.getMessage()),
        }

        if hasattr(record, "context"):
            payload["context"] = redact(record.context, sensitive_fields=self.sensitive_fields)
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))

        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def configure_logging(
    *,
    level: int = logging.INFO,
    handler: logging.Handler | None = None,
    sensitive_fields: Set[str] | None = None,
) -> logging.Logger:
    """Configure one managed handler and accept rotation capable handlers."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    managed = [
        existing
        for existing in logger.handlers
        if getattr(existing, _MANAGED_HANDLER_ATTRIBUTE, False)
    ]
    selected = handler or (managed[0] if managed else logging.StreamHandler(sys.stderr))

    for existing in managed:
        if existing is not selected:
            logger.removeHandler(existing)

    setattr(selected, _MANAGED_HANDLER_ATTRIBUTE, True)
    selected.setLevel(level)
    preserve_formatter = (
        handler is None
        and sensitive_fields is None
        and isinstance(selected.formatter, JsonLogFormatter)
    )
    if not preserve_formatter:
        selected.setFormatter(JsonLogFormatter(sensitive_fields=sensitive_fields))
    if selected not in logger.handlers:
        logger.addHandler(selected)

    return logger
