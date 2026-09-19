from __future__ import annotations

import json
from pathlib import Path

import pytest

from codex_events import CodexEventTranslator, CodexNativeProtocolError
from engine import JobRuntime, RuntimeEventKind


def line(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False) + "\n").encode()


def translator(tmp_path: Path, **kwargs) -> CodexEventTranslator:
    return CodexEventTranslator("job", tmp_path, **kwargs)


def active_translator(tmp_path: Path, **kwargs) -> CodexEventTranslator:
    stream = translator(tmp_path, **kwargs)
    session_id = kwargs.get("expected_session_id") or "thread-1"
    stream.feed(line({"type": "thread.started", "thread_id": session_id}))
    return stream


def test_native_turn_becomes_ordered_shared_events(tmp_path: Path) -> None:
    stream = translator(tmp_path)

    assert stream.feed(line({"type": "thread.started", "thread_id": "thread-1"})) == ()
    plan = stream.feed(
        line(
            {
                "type": "item.completed",
                "item": {"id": "reason", "type": "reasoning", "text": "Inspect first"},
            }
        )
    )
    text = stream.feed(
        line(
            {
                "type": "item.completed",
                "item": {"id": "message", "type": "agent_message", "text": "Done"},
            }
        )
    )
    end = stream.feed(
        line(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 11, "cached_input_tokens": 7, "output_tokens": 3},
            }
        )
    )
    events = (*plan, *text, *end)

    assert stream.session_id == "thread-1"
    assert [event.sequence for event in events] == [1, 2, 3, 4]
    assert [event.kind for event in events] == [
        RuntimeEventKind.PLAN,
        RuntimeEventKind.TEXT,
        RuntimeEventKind.USAGE,
        RuntimeEventKind.COMPLETION,
    ]
    assert events[2].payload == {
        "input_tokens": 11,
        "output_tokens": 3,
        "cost_usd": None,
    }
    assert events[3].payload == {"status": "completed"}
    assert all(event.job_id == "job" for event in events)
    assert all(event.runtime is JobRuntime.CODEX for event in events)


def test_tool_events_hide_native_command_and_output(tmp_path: Path) -> None:
    stream = active_translator(tmp_path)
    private_marker = "private-command-value"
    started = stream.feed(
        line(
            {
                "type": "item.started",
                "item": {
                    "id": "call-1",
                    "type": "command_execution",
                    "command": f"print {private_marker}",
                    "aggregated_output": private_marker,
                },
            }
        )
    )[0]
    completed = stream.feed(
        line(
            {
                "type": "item.completed",
                "item": {
                    "id": "call-1",
                    "type": "command_execution",
                    "status": "completed",
                    "aggregated_output": private_marker,
                },
            }
        )
    )[0]

    assert started.payload == {
        "call_id": "call-1",
        "name": "command_execution",
        "status": "started",
    }
    assert completed.payload["status"] == "completed"
    assert private_marker not in json.dumps([started.to_dict(), completed.to_dict()])


def test_failed_tool_and_missing_call_identity_are_normalized(tmp_path: Path) -> None:
    event = active_translator(tmp_path).feed(
        line(
            {
                "type": "item.completed",
                "item": {"type": "mcp_tool_call", "status": "failed"},
            }
        )
    )[0]

    assert event.kind is RuntimeEventKind.TOOL
    assert event.payload == {
        "call_id": "codex-call-1",
        "name": "mcp_tool_call",
        "status": "failed",
    }


def test_file_changes_keep_only_safe_worktree_relative_paths(tmp_path: Path) -> None:
    worktree = tmp_path / "repo"
    worktree.mkdir()
    outside = tmp_path / "outside.txt"
    events = active_translator(worktree).feed(
        line(
            {
                "type": "item.completed",
                "item": {
                    "type": "file_change",
                    "status": "completed",
                    "changes": [
                        {"path": "src/new.py", "kind": "add"},
                        {"path": str(worktree / "README.md"), "kind": "update"},
                        {"path": "old.py", "kind": "delete"},
                        {"path": "../escape", "kind": "update"},
                        {"path": str(outside), "kind": "update"},
                        {"path": "ignored", "kind": "unknown"},
                    ],
                },
            }
        )
    )

    assert [dict(event.payload) for event in events] == [
        {"path": "src/new.py", "action": "created"},
        {"path": "README.md", "action": "modified"},
        {"path": "old.py", "action": "deleted"},
    ]


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json\n",
        b'{"type":"error","type":"turn.completed"}\n',
        b'{"type":NaN}\n',
        b"[]\n",
        b"\xff\n",
    ],
)
def test_malformed_native_lines_become_sanitized_errors(tmp_path: Path, raw: bytes) -> None:
    private_marker = b"must-not-leak"
    event = translator(tmp_path).feed(raw + private_marker)[0]

    assert event.kind is RuntimeEventKind.ERROR
    assert event.payload == {
        "code": "codex_invalid_event",
        "message": "Codex emitted an invalid event.",
        "retryable": False,
    }
    assert private_marker.decode() not in json.dumps(event.to_dict())


def test_oversized_line_is_recoverable_and_does_not_expose_content(tmp_path: Path) -> None:
    stream = translator(tmp_path, max_line_bytes=8)

    error = stream.feed(b"private-value\n")[0]

    assert error.payload["code"] == "codex_message_too_large"
    assert "private-value" not in json.dumps(error.to_dict())


def test_resume_rejects_changed_native_session_identity(tmp_path: Path) -> None:
    stream = translator(tmp_path, expected_session_id="thread-1")

    with pytest.raises(CodexNativeProtocolError, match="changed unexpectedly"):
        stream.feed(line({"type": "thread.started", "thread_id": "thread-2"}))


def test_turn_failure_is_sanitized_and_completed_once(tmp_path: Path) -> None:
    stream = active_translator(tmp_path)
    first = stream.feed(
        line({"type": "turn.failed", "error": {"message": "private provider detail"}})
    )
    duplicate = stream.complete("failed")

    assert [event.kind for event in first] == [RuntimeEventKind.ERROR, RuntimeEventKind.COMPLETION]
    assert first[-1].payload == {"status": "failed"}
    assert "private provider detail" not in json.dumps([event.to_dict() for event in first])
    assert duplicate == ()


def test_unknown_events_and_invalid_item_shape_do_not_break_later_events(tmp_path: Path) -> None:
    stream = active_translator(tmp_path)

    assert stream.feed(line({"type": "future.event", "private": "ignored"})) == ()
    error = stream.feed(line({"type": "item.completed", "item": "bad"}))[0]
    text = stream.feed(
        line({"type": "item.completed", "item": {"type": "agent_message", "text": "ok"}})
    )[0]

    assert error.kind is RuntimeEventKind.ERROR
    assert text.kind is RuntimeEventKind.TEXT
    assert [error.sequence, text.sequence] == [1, 2]


def test_invalid_usage_counts_remain_truthfully_unknown(tmp_path: Path) -> None:
    events = active_translator(tmp_path).feed(
        line(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": True, "output_tokens": -1},
            }
        )
    )

    assert events[0].payload == {
        "input_tokens": None,
        "output_tokens": None,
        "cost_usd": None,
    }


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"job_id": ""}, ValueError),
        ({"worktree": Path("relative")}, ValueError),
        ({"first_sequence": 0}, ValueError),
        ({"expected_session_id": ""}, ValueError),
        ({"max_line_bytes": 0}, ValueError),
    ],
)
def test_translator_rejects_invalid_boundaries(tmp_path: Path, kwargs, error) -> None:
    values = {"job_id": "job", "worktree": tmp_path, **kwargs}

    with pytest.raises(error):
        CodexEventTranslator(**values)


def test_feed_requires_bytes(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="must be bytes"):
        translator(tmp_path).feed("not-bytes")


def test_native_events_cannot_precede_session_identity(tmp_path: Path) -> None:
    with pytest.raises(CodexNativeProtocolError, match="before its session identity"):
        translator(tmp_path).feed(line({"type": "turn.started"}))


def test_todo_updates_become_plan_snapshots(tmp_path: Path) -> None:
    event = active_translator(tmp_path).feed(
        line(
            {
                "type": "item.updated",
                "item": {
                    "id": "todo-1",
                    "type": "todo_list",
                    "items": [
                        {"text": "Inspect", "completed": True},
                        {"text": "Verify", "completed": False},
                    ],
                },
            }
        )
    )[0]

    assert event.kind is RuntimeEventKind.PLAN
    assert event.payload == {"text": "done: Inspect\npending: Verify"}


def test_failed_file_change_does_not_claim_a_mutation(tmp_path: Path) -> None:
    event = active_translator(tmp_path).feed(
        line(
            {
                "type": "item.completed",
                "item": {
                    "id": "change-1",
                    "type": "file_change",
                    "status": "failed",
                    "changes": [{"path": "app.py", "kind": "update"}],
                },
            }
        )
    )[0]

    assert event.kind is RuntimeEventKind.ERROR
    assert event.payload["code"] == "codex_file_change_failed"


def test_top_level_provider_error_is_terminal(tmp_path: Path) -> None:
    stream = active_translator(tmp_path)
    events = stream.feed(line({"type": "error", "message": "private native detail"}))
    trailing_failure = stream.feed(
        line({"type": "turn.failed", "error": {"message": "later private detail"}})
    )

    assert [event.kind for event in events] == [RuntimeEventKind.ERROR, RuntimeEventKind.COMPLETION]
    assert events[-1].payload == {"status": "failed"}
    assert "private native detail" not in json.dumps([event.to_dict() for event in events])
    assert trailing_failure == ()
    assert stream.next_sequence == 3
