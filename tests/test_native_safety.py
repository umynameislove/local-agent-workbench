from __future__ import annotations

import asyncio
import sys
import textwrap

import pytest
from test_codex_adapter import adapter, authenticated_accounts, collect
from test_codex_adapter import fake_codex as fake_codex

from local_agent_workbench.codex_adapter import CodexAdapter
from local_agent_workbench.engine import AdapterStart, ProcessResult


@pytest.mark.anyio
@pytest.mark.parametrize(
    "stdout,stderr,code,accepted",
    [
        (b"", b"Logged in using ChatGPT\n", 0, True),
        (b"Logged in using ChatGPT\n", b"", 0, True),
        (b"", b"", 0, False),
        (b"", b"Logged in using an API key", 0, False),
        (b"Logged in using ChatGPT", b"Unexpected account detail", 0, False),
        (b"", b"Logged in using ChatGPT", 1, False),
        (b"\xff", b"", 0, False),
    ],
)
async def test_authentication_requires_explicit_chatgpt_mode(
    tmp_path, monkeypatch, stdout, stderr, code, accepted
):
    accounts = authenticated_accounts(tmp_path, ("fixture",))
    provider = CodexAdapter(accounts)

    async def probe(command, **kwargs):
        assert command == ("codex", "login", "status")
        assert kwargs["env"]["CODEX_HOME"] == str(accounts.slots[0].home)
        return ProcessResult(code, stdout, stderr)

    monkeypatch.setattr("local_agent_workbench.codex_adapter.run_process", probe)
    assert await provider._authenticated(accounts.slots[0]) is accepted


@pytest.mark.anyio
async def test_completion_is_delayed_until_process_exit(tmp_path, fake_codex):
    provider = adapter(tmp_path, fake_codex, ("fixture",))
    session = await provider.start(AdapterStart("job", "complete-then-wait", tmp_path))
    reader = asyncio.create_task(collect(provider, session))
    await asyncio.sleep(0.1)
    assert not reader.done() and not provider.stopped("job")
    await provider.cancel(session)
    events = await reader
    assert events[-1].payload == {"status": "cancelled"}
    assert provider.stopped("job")


@pytest.mark.anyio
async def test_stop_reaps_owned_children_after_cli_leader_exits(tmp_path):
    executable = tmp_path / "child-fixture"
    executable.write_text(
        textwrap.dedent(f"""\
        #!{sys.executable}
        import json, os, signal, sys, time
        if sys.argv[1:] == ["login", "status"]:
            print("Logged in using ChatGPT", file=sys.stderr)
            raise SystemExit(0)
        if os.fork() == 0:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(30)
            os._exit(0)
        print(json.dumps({{"type":"thread.started","thread_id":"fixture-thread"}}), flush=True)
        print(json.dumps({{"type":"turn.completed","usage":{{}}}}), flush=True)
        os._exit(0)
    """)
    )
    executable.chmod(0o700)
    provider = adapter(tmp_path, executable, ("fixture",))
    session = await provider.start(AdapterStart("job", "task", tmp_path))
    async with asyncio.timeout(5):
        await provider.stop_job("job")
        events = await collect(provider, session)
    assert events[-1].payload == {"status": "cancelled"}
    # Zombie reaping may lag. Unknown group liveness cannot be called stopped.
    assert provider.stopped("job") == (provider.outcome("job") is not None)
