import json
from datetime import UTC

import pytest

from engine import AdapterSession, JobRuntime, RuntimeEventKind, RuntimeEventStreamNormalizer

PAYLOADS = {
    "text": {"text": ""},
    "plan": {"text": "inspect"},
    "tool": {"call_id": "call", "name": "test", "status": "started"},
    "file": {"path": "src/app.py", "action": "modified"},
    "usage": {"input_tokens": 1, "output_tokens": None, "cost_usd": "0.0001"},
    "question": {"question_id": "question", "text": "Proceed?"},
    "error": {"code": "unavailable", "message": "Unavailable", "retryable": True},
    "completion": {"status": "completed"},
}


def message(kind="text", payload=None):
    if payload is None:
        payload = {"text": "hello"}
    return json.dumps({"kind": kind, "payload": payload}, ensure_ascii=False).encode()


def normalizer(**kwargs):
    session = AdapterSession("job", JobRuntime.CODEX, "session")
    return RuntimeEventStreamNormalizer(session, **kwargs)


@pytest.mark.parametrize("kind,payload", PAYLOADS.items())
def test_every_event_kind_is_normalized(kind, payload):
    event = normalizer().feed(message(kind, payload) + b"\n")[0]

    assert event.kind is RuntimeEventKind(kind)
    assert dict(event.payload) == payload
    assert event.sequence == 1


def test_fragmented_multibyte_and_crlf_messages_preserve_order():
    stream = normalizer()
    content = (
        b"\r\n".join(
            (
                message("plan", {"text": "Kiểm tra"}),
                b"   ",
                message("text", {"text": "xong"}),
            )
        )
        + b"\r\n"
    )

    events = []
    for byte in content:
        events.extend(stream.feed(bytes([byte])))

    assert [event.sequence for event in events] == [1, 2]
    assert [event.kind for event in events] == [RuntimeEventKind.PLAN, RuntimeEventKind.TEXT]
    assert [event.payload["text"] for event in events] == ["Kiểm tra", "xong"]
    assert all(event.job_id == "job" for event in events)
    assert all(event.runtime is JobRuntime.CODEX for event in events)
    assert all(event.timestamp.tzinfo is UTC for event in events)
    assert stream.finish() == ()


def test_multiple_messages_and_final_unterminated_line_are_flushed():
    stream = normalizer(first_sequence=7)
    events = stream.feed(message() + b"\n" + message("plan", {"text": "next"})[:10])
    assert [event.sequence for event in events] == [7]

    remainder = message("plan", {"text": "next"})[10:]
    final = stream.feed(remainder)
    assert final == ()
    final = stream.finish()
    assert [event.sequence for event in final] == [8]
    assert final[0].payload == {"text": "next"}
    assert stream.finish() == ()


def test_malformed_line_becomes_sanitized_error_and_later_message_survives():
    stream = normalizer()
    secret = b'{"token":"must-not-leak"'
    events = stream.feed(message() + b"\n" + secret + b"\n" + message() + b"\n")

    assert [event.sequence for event in events] == [1, 2, 3]
    assert [event.kind for event in events] == [
        RuntimeEventKind.TEXT,
        RuntimeEventKind.ERROR,
        RuntimeEventKind.TEXT,
    ]
    error = events[1]
    assert error.payload == {
        "code": "stream_invalid_json",
        "message": "Provider stream contained invalid JSON.",
        "retryable": False,
    }
    assert "must-not-leak" not in json.dumps(error.to_dict())


@pytest.mark.parametrize(
    "raw,code",
    [
        (b"[]", "stream_invalid_event"),
        (b'{"kind":"text","payload":{"text":"ok"},"extra":true}', "stream_invalid_event"),
        (b'{"kind":"other","payload":{"text":"ok"}}', "stream_invalid_event"),
        (b'{"kind":"text","payload":{}}', "stream_invalid_event"),
        (b'{"kind":"text","payload":{"text":"a","text":"b"}}', "stream_invalid_json"),
        (b'{"kind":"usage","payload":{"input_tokens":NaN}}', "stream_invalid_json"),
        (b"[" * 2_000 + b"]" * 2_000, "stream_invalid_event"),
    ],
)
def test_invalid_message_contract_is_recoverable(raw, code):
    stream = normalizer()
    first = stream.feed(raw + b"\n")
    second = stream.feed(message() + b"\n")

    assert first[0].kind is RuntimeEventKind.ERROR
    assert first[0].payload["code"] == code
    assert second[0].kind is RuntimeEventKind.TEXT
    assert [first[0].sequence, second[0].sequence] == [1, 2]


def test_invalid_utf8_isolated_to_its_line():
    stream = normalizer()
    events = stream.feed(b"\xff\xfe\n" + message("plan", {"text": "safe"}) + b"\n")

    assert [event.kind for event in events] == [RuntimeEventKind.ERROR, RuntimeEventKind.PLAN]
    assert events[0].payload["code"] == "stream_invalid_encoding"
    assert events[1].payload["text"] == "safe"


def test_oversized_fragment_emits_once_and_resynchronizes_at_newline():
    valid = message()
    stream = normalizer(max_line_bytes=len(valid))

    first = stream.feed(b"x" * (len(valid) + 1))
    assert len(first) == 1
    assert first[0].payload["code"] == "stream_message_too_large"
    assert stream.feed(b"discarded") == ()

    recovered = stream.feed(b"\n" + valid + b"\n")
    assert len(recovered) == 1
    assert recovered[0].kind is RuntimeEventKind.TEXT
    assert recovered[0].sequence == 2


def test_complete_oversized_line_does_not_hide_following_message():
    valid = message()
    stream = normalizer(max_line_bytes=len(valid))
    events = stream.feed(b"x" * (len(valid) + 1) + b"\n" + valid + b"\n")

    assert [event.kind for event in events] == [RuntimeEventKind.ERROR, RuntimeEventKind.TEXT]
    assert events[0].payload["code"] == "stream_message_too_large"


def test_invalid_chunk_and_closed_stream_do_not_advance_sequence():
    stream = normalizer()
    assert stream.feed(b"") == ()
    with pytest.raises(TypeError):
        stream.feed(bytearray(message()))
    event = stream.feed(message() + b"\n")[0]
    assert event.sequence == 1

    assert stream.finish() == ()
    with pytest.raises(RuntimeError):
        stream.feed(message())


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("session", None, TypeError),
        ("first_sequence", 0, ValueError),
        ("first_sequence", True, ValueError),
        ("max_line_bytes", 0, ValueError),
        ("max_line_bytes", 1.5, ValueError),
    ],
)
def test_constructor_rejects_invalid_boundaries(field, value, error):
    session = AdapterSession("job", JobRuntime.LOCAL, "session")
    kwargs = {field: value}
    if field != "session":
        kwargs["session"] = session
    with pytest.raises(error):
        RuntimeEventStreamNormalizer(**kwargs)


def test_finish_handles_whitespace_and_invalid_final_encoding():
    whitespace = normalizer()
    assert whitespace.feed(b" \r") == ()
    assert whitespace.finish() == ()

    invalid = normalizer()
    assert invalid.feed(b"\xe2\x82") == ()
    event = invalid.finish()[0]
    assert event.kind is RuntimeEventKind.ERROR
    assert event.payload["code"] == "stream_invalid_encoding"
