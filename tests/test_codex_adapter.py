from __future__ import annotations

import asyncio
import json
import sys
import textwrap
from pathlib import Path

import pytest
from provider_conformance import ProviderConformanceSubject, ProviderContractTests

from codex_accounts import CodexAccountPool, codex_account_slots
from codex_adapter import CodexAdapter
from engine import (
    AdapterError,
    AdapterSession,
    AdapterStart,
    AdapterUnsupportedError,
    JobRuntime,
    ProviderCapability,
    ProviderHealthState,
    RuntimeEventKind,
    StreamingProviderAdapter,
)


@pytest.fixture
def fake_codex(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-codex"
    executable.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import json
            import os
            import sys
            import time
            from pathlib import Path

            home = Path(os.environ["CODEX_HOME"])
            command = sys.argv[1:]
            if command == ["login", "status"]:
                raise SystemExit(0 if (home / "authenticated").is_file() else 1)
            if not command or command[0] != "exec":
                raise SystemExit(2)

            prompt = sys.stdin.read()
            record = {{
                "args": command[1:],
                "parent_secret": os.environ.get("PRIVATE_PARENT_SECRET"),
                "prompt": prompt,
            }}
            with (home / "calls.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, separators=(",", ":")) + "\\n")

            thread_id = "thread-" + home.name
            print(json.dumps({{"type": "thread.started", "thread_id": thread_id}}), flush=True)
            if prompt == "wait":
                time.sleep(30)
            if prompt == "fail":
                print("private provider failure detail", file=sys.stderr, flush=True)
                raise SystemExit(9)
            if prompt == "provider-fail":
                provider_error = {{"type": "error", "message": "private native detail"}}
                print(json.dumps(provider_error), flush=True)
                print(
                    json.dumps(
                        {{"type": "turn.failed", "error": {{"message": "private detail"}}}}
                    ),
                    flush=True,
                )
                raise SystemExit(9)
            if prompt.startswith("file:"):
                change = {{
                    "type": "item.completed",
                    "item": {{
                        "id": "change-1",
                        "type": "file_change",
                        "status": "completed",
                        "changes": [{{"path": prompt[5:], "kind": "update"}}],
                    }},
                }}
                print(json.dumps(change), flush=True)
                print(
                    json.dumps(
                        {{
                            "type": "turn.completed",
                            "usage": {{"input_tokens": 7, "output_tokens": 2}},
                        }}
                    ),
                    flush=True,
                )
                raise SystemExit(0)

            started = {{
                "type": "item.started",
                "item": {{"id": "call-1", "type": "command_execution"}},
            }}
            completed = {{
                "type": "item.completed",
                "item": {{"id": "call-1", "type": "command_execution", "status": "completed"}},
            }}
            message = {{
                "type": "item.completed",
                "item": {{"id": "message-1", "type": "agent_message", "text": "done:" + prompt}},
            }}
            finished = {{
                "type": "turn.completed",
                "usage": {{"input_tokens": 7, "output_tokens": 2}},
            }}
            for event in (started, completed, message, finished):
                print(json.dumps(event), flush=True)
            """
        ),
        encoding="utf-8",
    )
    executable.chmod(0o700)
    return executable


def authenticated_accounts(tmp_path: Path, names=("one", "two", "three")) -> CodexAccountPool:
    slots = codex_account_slots(tmp_path / "runtime", tuple(names))
    for slot in slots:
        slot.home.mkdir(parents=True)
        (slot.home / "authenticated").touch()
    return CodexAccountPool(slots)


def adapter(tmp_path: Path, executable: Path, names=("one", "two", "three")) -> CodexAdapter:
    return CodexAdapter(
        authenticated_accounts(tmp_path, names),
        executable=str(executable),
        model="gpt-test",
        login_timeout=2,
        session_timeout=2,
    )


async def collect(provider: CodexAdapter, session: AdapterSession):
    return [event async for event in provider.stream(session)]


def calls(slot_home: Path) -> list[dict[str, object]]:
    path = slot_home / "calls.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class TestCodexProviderConformance(ProviderContractTests):
    @pytest.fixture
    def subject(self, tmp_path: Path, fake_codex: Path) -> ProviderConformanceSubject:
        accounts = authenticated_accounts(tmp_path, ("contract",))
        return ProviderConformanceSubject(
            factory=lambda: CodexAdapter(
                accounts,
                executable=str(fake_codex),
                login_timeout=2,
                session_timeout=2,
            ),
            runtime=JobRuntime.CODEX,
            accepted_message="Continue",
            required_capability=ProviderCapability.FILES,
        )


@pytest.mark.anyio
async def test_scoped_task_streams_normalized_events(tmp_path: Path, fake_codex: Path) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    streaming: StreamingProviderAdapter = provider

    session = await provider.start(AdapterStart("job", "inspect", tmp_path))
    events = [event async for event in streaming.stream(session)]

    assert session == AdapterSession("job", JobRuntime.CODEX, "thread-primary")
    assert [event.kind for event in events] == [
        RuntimeEventKind.TOOL,
        RuntimeEventKind.TOOL,
        RuntimeEventKind.TEXT,
        RuntimeEventKind.USAGE,
        RuntimeEventKind.COMPLETION,
    ]
    assert [event.sequence for event in events] == [1, 2, 3, 4, 5]
    assert events[2].payload == {"text": "done:inspect"}
    assert events[3].payload == {
        "input_tokens": 7,
        "output_tokens": 2,
        "cost_usd": None,
    }
    assert events[-1].payload == {"status": "completed"}


@pytest.mark.anyio
async def test_file_events_enforce_the_adapter_write_boundary(
    tmp_path: Path,
    fake_codex: Path,
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    (tmp_path / "src").mkdir()
    allowed = await provider.start(
        AdapterStart(
            "allowed",
            "file:src/app.py",
            tmp_path,
            allowed_write_paths=("src",),
        )
    )
    allowed_events = await collect(provider, allowed)

    assert [event.kind for event in allowed_events] == [
        RuntimeEventKind.FILE,
        RuntimeEventKind.USAGE,
        RuntimeEventKind.COMPLETION,
    ]
    assert allowed_events[0].payload == {"path": "src/app.py", "action": "modified"}

    denied = await provider.start(
        AdapterStart(
            "denied",
            "file:../private-outside.txt",
            tmp_path,
            allowed_write_paths=("src",),
        )
    )
    denied_events = await collect(provider, denied)
    serialized = json.dumps([event.to_dict() for event in denied_events])

    assert [event.kind for event in denied_events] == [
        RuntimeEventKind.ERROR,
        RuntimeEventKind.COMPLETION,
    ]
    assert denied_events[0].payload["code"] == "codex_adapter_failed"
    assert "private-outside" not in serialized


@pytest.mark.anyio
async def test_start_uses_safe_flags_stdin_and_filtered_environment(
    tmp_path: Path,
    fake_codex: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRIVATE_PARENT_SECRET", "must-not-leak")
    pool = authenticated_accounts(tmp_path, ("primary",))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        model="gpt-test",
        login_timeout=2,
        session_timeout=2,
    )

    session = await provider.start(AdapterStart("job", "private request", tmp_path))
    await collect(provider, session)
    record = calls(pool.slots[0].home)[0]
    arguments = record["args"]

    assert record["prompt"] == "private request"
    assert "private request" not in arguments
    assert record["parent_secret"] is None
    assert "--json" in arguments
    assert "--ignore-user-config" in arguments
    assert "--ignore-rules" in arguments
    assert "--strict-config" in arguments
    assert arguments[arguments.index("--sandbox") + 1] == "workspace-write"
    assert 'approval_policy="never"' in arguments
    assert "sandbox_workspace_write.network_access=false" in arguments
    assert "sandbox_workspace_write.exclude_tmpdir_env_var=true" in arguments
    assert "sandbox_workspace_write.exclude_slash_tmp=true" in arguments
    assert "allow_login_shell=false" in arguments
    assert 'web_search="disabled"' in arguments
    assert "features.apps=false" in arguments
    assert "features.hooks=false" in arguments
    assert "agents.enabled=false" in arguments
    assert "--approve-for-me" not in arguments
    assert "--dangerously-bypass-approvals-and-sandbox" not in arguments
    assert arguments[arguments.index("--model") + 1] == "gpt-test"
    assert arguments[-1] == "-"


@pytest.mark.anyio
async def test_explicit_allowed_directories_become_the_only_codex_workspace_roots(
    tmp_path: Path,
    fake_codex: Path,
) -> None:
    source = tmp_path / "src"
    docs = tmp_path / "docs"
    source.mkdir()
    docs.mkdir()
    pool = authenticated_accounts(tmp_path, ("primary",))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    session = await provider.start(
        AdapterStart(
            "job",
            "task",
            tmp_path,
            allowed_write_paths=("src", "docs"),
        )
    )
    await collect(provider, session)
    arguments = calls(pool.slots[0].home)[0]["args"]

    assert arguments[arguments.index("--cd") + 1] == str(source)
    assert arguments[arguments.index("--add-dir") + 1] == str(docs)
    assert str(tmp_path) not in (
        arguments[arguments.index("--cd") + 1],
        arguments[arguments.index("--add-dir") + 1],
    )


@pytest.mark.anyio
async def test_replaced_allowed_directory_blocks_followup_before_process_start(
    tmp_path: Path,
    fake_codex: Path,
) -> None:
    source = tmp_path / "src"
    source.mkdir()
    pool = authenticated_accounts(tmp_path, ("primary",))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )
    session = await provider.start(
        AdapterStart("job", "first", tmp_path, allowed_write_paths=("src",))
    )
    await collect(provider, session)
    source.rename(tmp_path / "original-src")
    source.mkdir()

    with pytest.raises(AdapterError, match="write boundary is unavailable"):
        await provider.send(session, "second")

    assert len(calls(pool.slots[0].home)) == 1


@pytest.mark.anyio
async def test_three_accounts_rotate_once_per_new_job(tmp_path: Path, fake_codex: Path) -> None:
    pool = authenticated_accounts(tmp_path)
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    sessions = []
    for index in range(4):
        session = await provider.start(AdapterStart(f"job-{index}", f"task-{index}", tmp_path))
        await collect(provider, session)
        sessions.append(session)

    assert [session.session_id for session in sessions] == [
        "thread-one",
        "thread-two",
        "thread-three",
        "thread-one",
    ]
    assert [len(calls(slot.home)) for slot in pool.slots] == [2, 1, 1]


@pytest.mark.anyio
async def test_unavailable_account_is_skipped_and_health_is_degraded(
    tmp_path: Path, fake_codex: Path
) -> None:
    pool = authenticated_accounts(tmp_path, ("one", "two", "three"))
    (pool.slots[1].home / "authenticated").unlink()
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    assert (await provider.health()).state is ProviderHealthState.DEGRADED
    first = await provider.start(AdapterStart("first", "task", tmp_path))
    await collect(provider, first)
    second = await provider.start(AdapterStart("second", "task", tmp_path))
    await collect(provider, second)

    assert [first.session_id, second.session_id] == ["thread-one", "thread-three"]
    assert calls(pool.slots[1].home) == []


@pytest.mark.anyio
async def test_health_is_available_only_when_every_slot_is_authenticated(
    tmp_path: Path, fake_codex: Path
) -> None:
    pool = authenticated_accounts(tmp_path, ("one", "two"))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    assert (await provider.health()).state is ProviderHealthState.AVAILABLE
    for slot in pool.slots:
        (slot.home / "authenticated").unlink()
    assert (await provider.health()).state is ProviderHealthState.UNAVAILABLE


@pytest.mark.anyio
async def test_followup_stays_on_original_account_and_preserves_sequence(
    tmp_path: Path, fake_codex: Path
) -> None:
    pool = authenticated_accounts(tmp_path, ("one", "two"))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )
    session = await provider.start(AdapterStart("job", "first", tmp_path))
    first = await collect(provider, session)

    await provider.send(session, "second")
    second = await collect(provider, session)

    assert first[-1].sequence == 5
    assert second[0].sequence == 6
    assert second[-1].sequence == 10
    assert second[2].payload == {"text": "done:second"}
    assert len(calls(pool.slots[0].home)) == 2
    assert calls(pool.slots[1].home) == []
    resume_arguments = calls(pool.slots[0].home)[1]["args"]
    resume_index = resume_arguments.index("resume")
    assert resume_arguments[resume_index + 1] == session.session_id
    assert resume_arguments[-1] == "-"
    assert resume_arguments[resume_arguments.index("--sandbox") + 1] == "workspace-write"
    assert "--ignore-rules" in resume_arguments
    assert "--strict-config" in resume_arguments
    assert resume_arguments[resume_arguments.index("--cd") + 1] == str(tmp_path)
    assert 'approval_policy="never"' in resume_arguments
    assert "sandbox_workspace_write.network_access=false" in resume_arguments
    assert "sandbox_workspace_write.exclude_tmpdir_env_var=true" in resume_arguments
    assert "sandbox_workspace_write.exclude_slash_tmp=true" in resume_arguments
    assert "allow_login_shell=false" in resume_arguments
    assert 'web_search="disabled"' in resume_arguments
    assert "features.apps=false" in resume_arguments
    assert "features.hooks=false" in resume_arguments
    assert "agents.enabled=false" in resume_arguments
    assert "--approve-for-me" not in resume_arguments
    assert "--dangerously-bypass-approvals-and-sandbox" not in resume_arguments


@pytest.mark.anyio
async def test_cancel_terminates_active_process_and_emits_once(
    tmp_path: Path, fake_codex: Path
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "wait", tmp_path))

    await provider.cancel(session)
    await provider.cancel(session)
    events = await collect(provider, session)

    assert [event.kind for event in events] == [RuntimeEventKind.COMPLETION]
    assert events[0].payload == {"status": "cancelled"}
    with pytest.raises(AdapterError, match="ended"):
        await provider.send(session, "continue")


@pytest.mark.anyio
async def test_cancel_is_not_blocked_by_a_queued_followup(tmp_path: Path, fake_codex: Path) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "wait", tmp_path))
    followup = asyncio.create_task(provider.send(session, "continue"))
    await asyncio.sleep(0.05)

    await asyncio.wait_for(provider.cancel(session), 2)

    with pytest.raises(AdapterError, match="ended"):
        await followup
    events = await collect(provider, session)
    assert events[-1].payload == {"status": "cancelled"}


@pytest.mark.anyio
async def test_cancel_falls_back_when_process_group_signal_is_denied(
    tmp_path: Path,
    fake_codex: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "wait", tmp_path))

    def deny_group_signal(_process_group: int, _signal_number: int) -> None:
        raise PermissionError

    monkeypatch.setattr("codex_adapter.os.killpg", deny_group_signal)
    await provider.cancel(session)
    events = await collect(provider, session)

    assert events[-1].payload == {"status": "cancelled"}


@pytest.mark.anyio
async def test_nonzero_exit_and_stderr_are_sanitized(tmp_path: Path, fake_codex: Path) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "fail", tmp_path))

    events = await collect(provider, session)
    serialized = json.dumps([event.to_dict() for event in events])

    assert [event.kind for event in events] == [RuntimeEventKind.ERROR, RuntimeEventKind.COMPLETION]
    assert events[0].payload["code"] == "codex_process_failed"
    assert events[-1].payload == {"status": "failed"}
    assert "private provider failure detail" not in serialized
    assert str(tmp_path) not in serialized


@pytest.mark.anyio
async def test_terminal_provider_error_is_not_followed_by_duplicate_events(
    tmp_path: Path, fake_codex: Path
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "provider-fail", tmp_path))

    events = await collect(provider, session)
    serialized = json.dumps([event.to_dict() for event in events])

    assert [event.kind for event in events] == [
        RuntimeEventKind.ERROR,
        RuntimeEventKind.COMPLETION,
    ]
    assert events[0].payload["code"] == "codex_provider_error"
    assert events[-1].payload == {"status": "failed"}
    assert "private native detail" not in serialized
    assert "private detail" not in serialized


@pytest.mark.anyio
async def test_no_authenticated_account_fails_without_starting_task(
    tmp_path: Path, fake_codex: Path
) -> None:
    slots = codex_account_slots(tmp_path / "runtime", ("one", "two"))
    for slot in slots:
        slot.home.mkdir(parents=True)
    provider = CodexAdapter(
        CodexAccountPool(slots),
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    with pytest.raises(AdapterError, match="No authenticated Codex account"):
        await provider.start(AdapterStart("job", "task", tmp_path))

    assert all(calls(slot.home) == [] for slot in slots)


@pytest.mark.anyio
async def test_stream_is_single_consumer_and_restart_resume_is_explicitly_unsupported(
    tmp_path: Path, fake_codex: Path
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "task", tmp_path))
    await collect(provider, session)

    with pytest.raises(AdapterError, match="stream is unavailable"):
        await collect(provider, session)
    with pytest.raises(AdapterUnsupportedError, match="durable account binding"):
        await provider.resume(session)


@pytest.mark.anyio
async def test_duplicate_job_and_invalid_worktree_fail_closed(
    tmp_path: Path, fake_codex: Path
) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "task", tmp_path))

    with pytest.raises(AdapterError, match="already started"):
        await provider.start(AdapterStart("job", "again", tmp_path))
    with pytest.raises(AdapterError, match="write boundary is unavailable"):
        await provider.start(AdapterStart("other", "task", tmp_path / "missing"))
    with pytest.raises(AdapterError, match="surrounding whitespace"):
        await provider.start(AdapterStart(" spaced ", "task", tmp_path))
    await collect(provider, session)


@pytest.mark.anyio
async def test_symlinked_worktree_is_rejected_before_account_use(
    tmp_path: Path,
    fake_codex: Path,
) -> None:
    pool = authenticated_accounts(tmp_path, ("primary",))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )
    real = tmp_path / "real-worktree"
    real.mkdir()
    alias = tmp_path / "worktree-alias"
    alias.symlink_to(real, target_is_directory=True)

    with pytest.raises(AdapterError, match="write boundary is unavailable"):
        await provider.start(AdapterStart("job", "task", alias))

    assert calls(pool.slots[0].home) == []


@pytest.mark.anyio
async def test_missing_allowed_directory_is_rejected_before_account_use(
    tmp_path: Path,
    fake_codex: Path,
) -> None:
    pool = authenticated_accounts(tmp_path, ("primary",))
    provider = CodexAdapter(
        pool,
        executable=str(fake_codex),
        login_timeout=2,
        session_timeout=2,
    )

    with pytest.raises(AdapterError, match="write boundary is unavailable"):
        await provider.start(
            AdapterStart("job", "task", tmp_path, allowed_write_paths=("missing",))
        )

    assert calls(pool.slots[0].home) == []


@pytest.mark.anyio
async def test_foreign_identity_cannot_read_stream(tmp_path: Path, fake_codex: Path) -> None:
    provider = adapter(tmp_path, fake_codex, ("primary",))
    session = await provider.start(AdapterStart("job", "task", tmp_path))
    foreign = AdapterSession("foreign", JobRuntime.CODEX, session.session_id)

    with pytest.raises(AdapterError, match="Unknown Codex session"):
        await collect(provider, foreign)
    await collect(provider, session)


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"accounts": None}, TypeError),
        ({"executable": ""}, ValueError),
        ({"model": ""}, ValueError),
        ({"login_timeout": 0}, ValueError),
        ({"session_timeout": float("nan")}, ValueError),
        ({"max_line_bytes": 0}, ValueError),
    ],
)
def test_adapter_rejects_invalid_construction(tmp_path: Path, kwargs, error) -> None:
    values = {"accounts": authenticated_accounts(tmp_path, ("one",)), **kwargs}

    with pytest.raises(error):
        CodexAdapter(**values)
