import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from engine import JobRuntime, RuntimeEvent, RuntimeEventKind

PAYLOADS = {
    "text": {"text": ""},
    "plan": {"text": "Inspect then implement"},
    "tool": {"call_id": "call", "name": "test", "status": "started"},
    "file": {"path": "src/app.py", "action": "modified"},
    "usage": {"input_tokens": None, "output_tokens": 0, "cost_usd": None},
    "question": {"question_id": "question", "text": "Proceed?"},
    "error": {"code": "unavailable", "message": "Runtime unavailable", "retryable": True},
    "completion": {"status": "completed"},
}


def envelope(kind="text"):
    return {
        "job_id": "job",
        "sequence": 1,
        "runtime": "local",
        "timestamp": "2026-09-08T10:00:00+07:00",
        "kind": kind,
        "payload": dict(PAYLOADS[kind]),
    }


@pytest.mark.parametrize("kind", PAYLOADS)
def test_every_kind_roundtrips_through_json(kind):
    event = RuntimeEvent.from_dict(envelope(kind))
    assert event.timestamp == datetime(2026, 9, 8, 3, tzinfo=UTC)
    assert event.kind is RuntimeEventKind(kind)
    assert RuntimeEvent.from_dict(json.loads(json.dumps(event.to_dict()))) == event


@pytest.mark.parametrize("field", list(envelope()))
def test_missing_envelope_fields_are_rejected(field):
    value = envelope()
    del value[field]
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_id", " "),
        ("job_id", " job"),
        ("sequence", True),
        ("sequence", 0),
        ("sequence", 1.5),
        ("runtime", "auto"),
        ("runtime", "unknown"),
        ("timestamp", "2026-09-08T10:00:00"),
        ("timestamp", None),
        ("kind", "other"),
        ("payload", []),
    ],
)
def test_invalid_envelope_is_rejected(field, value):
    item = envelope()
    item[field] = value
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(item)


@pytest.mark.parametrize("kind", PAYLOADS)
def test_extra_and_missing_payload_fields_are_rejected(kind):
    value = envelope(kind)
    value["payload"]["raw_provider_output"] = "unexpected"
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(value)
    value["payload"] = {}
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("input_tokens", True),
        ("input_tokens", -1),
        ("output_tokens", "10"),
        ("cost_usd", 0.1),
        ("cost_usd", "NaN"),
        ("cost_usd", "Infinity"),
        ("cost_usd", "-1"),
        ("cost_usd", "invalid"),
    ],
)
def test_invalid_usage_is_rejected(field, value):
    item = envelope("usage")
    item["payload"][field] = value
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(item)


def test_usage_preserves_unknown_and_exact_cost():
    item = envelope("usage")
    item["payload"]["cost_usd"] = "0.000001"
    assert RuntimeEvent.from_dict(item).to_dict()["payload"] == item["payload"]


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("tool", "status", "unknown"),
        ("file", "action", "execute"),
        ("completion", "status", "started"),
        ("error", "retryable", 1),
        ("question", "text", ""),
        ("plan", "text", None),
        ("text", "text", "\x00"),
    ],
)
def test_kind_specific_validation(kind, field, value):
    item = envelope(kind)
    item["payload"][field] = value
    with pytest.raises(ValueError):
        RuntimeEvent.from_dict(item)


def test_payload_is_detached_and_immutable():
    item = envelope()
    event = RuntimeEvent.from_dict(item)
    item["payload"]["text"] = "changed"
    assert event.payload["text"] == ""
    with pytest.raises(TypeError):
        event.payload["text"] = "changed"
    exported = event.to_dict()
    exported["payload"]["text"] = "changed"
    assert event.payload["text"] == ""


def test_direct_constructor_normalizes_timezone():
    event = RuntimeEvent(
        "job",
        1,
        JobRuntime.CODEX,
        datetime(2026, 9, 8, tzinfo=timezone(timedelta(hours=7))),
        RuntimeEventKind.TEXT,
        {"text": "hello"},
    )
    assert event.timestamp.tzinfo is UTC
