import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from engine import ProcessRunnerError, ProcessTimeoutError, run_process


def test_arguments_are_literal_and_exit_output_is_preserved(tmp_path: Path):
    marker = tmp_path / "injected"
    argument = f"; touch {marker}; $(echo unsafe)"
    result = asyncio.run(
        run_process(
            (
                sys.executable,
                "-c",
                "import sys; print(sys.argv[1]); print('error', file=sys.stderr); sys.exit(7)",
                argument,
            ),
            cwd=tmp_path,
        )
    )
    assert result.returncode == 7
    assert result.stdout.decode().strip() == argument
    assert result.stderr == b"error\n"
    assert not marker.exists()


def test_stdin_is_closed(tmp_path: Path):
    result = asyncio.run(
        run_process(
            (sys.executable, "-c", "import sys; print(len(sys.stdin.read()))"),
            cwd=tmp_path,
        )
    )
    assert result.stdout == b"0\n"


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf")])
def test_invalid_timeout_rejected(tmp_path: Path, timeout):
    with pytest.raises(ValueError):
        asyncio.run(run_process((sys.executable,), cwd=tmp_path, timeout=timeout))


@pytest.mark.parametrize("argv", ["echo hello", (), ("",), ("echo", None), ("echo\x00",)])
def test_unstructured_arguments_rejected(tmp_path: Path, argv):
    with pytest.raises(ValueError):
        asyncio.run(run_process(argv, cwd=tmp_path))


def test_spawn_failure_is_sanitized(tmp_path: Path):
    with pytest.raises(ProcessRunnerError) as error:
        asyncio.run(run_process((str(tmp_path / "missing"),), cwd=tmp_path))
    assert str(tmp_path) not in str(error.value)


@pytest.mark.parametrize("cancel", [False, True])
def test_timeout_and_cancellation_kill_group_and_reap_parent(tmp_path: Path, cancel):
    marker = tmp_path / "pids.json"
    program = (
        "import json,os,subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        "open(sys.argv[1],'w').write(json.dumps([os.getpid(),child.pid])); time.sleep(30)"
    )

    async def scenario():
        task = asyncio.create_task(
            run_process(
                (sys.executable, "-c", program, str(marker)),
                cwd=tmp_path,
                timeout=10 if cancel else 0.5,
            )
        )
        if cancel:
            for _ in range(200):
                if marker.exists() and marker.stat().st_size:
                    break
                await asyncio.sleep(0.01)
            assert marker.exists()
            task.cancel()
            asyncio.get_running_loop().call_soon(task.cancel)
        with pytest.raises(asyncio.CancelledError if cancel else ProcessTimeoutError):
            await asyncio.wait_for(task, 5)

    asyncio.run(scenario())
    parent, child = json.loads(marker.read_text())
    with pytest.raises(ChildProcessError):
        os.waitpid(parent, os.WNOHANG)
    with pytest.raises(ProcessLookupError):
        os.kill(parent, 0)
    # A killed descendant may briefly remain a zombie until its system reaper runs.
    import subprocess

    state = subprocess.run(["ps", "-o", "stat=", "-p", str(child)], capture_output=True)
    assert not state.stdout.strip() or state.stdout.strip().startswith(b"Z")
