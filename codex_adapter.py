from __future__ import annotations

import asyncio
import math
import os
import signal
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from codex_accounts import CodexAccountPool, CodexAccountSlot
from codex_events import CodexEventTranslator, CodexNativeProtocolError
from engine import (
    AdapterError,
    AdapterSession,
    AdapterStart,
    AdapterUnsupportedError,
    CapabilitySupport,
    JobRuntime,
    ProcessRunnerError,
    ProviderCapabilities,
    ProviderCapability,
    ProviderHealth,
    ProviderHealthState,
    RuntimeEvent,
    run_process,
)
from write_boundary import WriteBoundary, WriteBoundaryError

_STREAM_END = object()
_PASSTHROUGH_ENVIRONMENT = (
    "HOME",
    "LANG",
    "LC_ALL",
    "LOGNAME",
    "PATH",
    "SHELL",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TERM",
    "TMPDIR",
    "USER",
    "__CF_USER_TEXT_ENCODING",
)


@dataclass
class _Turn:
    translator: CodexEventTranslator
    queue: asyncio.Queue[RuntimeEvent | object] = field(default_factory=asyncio.Queue)
    task: asyncio.Task[None] | None = None
    process: asyncio.subprocess.Process | None = None
    claimed: bool = False
    cancel_requested: bool = False
    spawned: asyncio.Future[None] | None = None


@dataclass
class _SessionState:
    job_id: str
    account: CodexAccountSlot
    worktree: Path
    write_boundary: WriteBoundary
    writable_roots: tuple[Path, ...]
    session: AdapterSession | None = None
    next_sequence: int = 1
    turn: _Turn | None = None
    cancelled: bool = False
    failed: bool = False
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class CodexAdapter:
    """Run the official Codex CLI with isolated subscription account state."""

    def __init__(
        self,
        accounts: CodexAccountPool,
        *,
        executable: str = "codex",
        model: str | None = None,
        login_timeout: float = 10.0,
        session_timeout: float = 30.0,
        max_line_bytes: int = 1_048_576,
    ) -> None:
        if not isinstance(accounts, CodexAccountPool):
            raise TypeError("Codex adapter requires an account pool.")
        if not isinstance(executable, str) or not executable or "\x00" in executable:
            raise ValueError("Codex executable is invalid.")
        if model is not None and (
            not isinstance(model, str) or not model.strip() or "\x00" in model
        ):
            raise ValueError("Codex model is invalid.")
        for value, field_name in (
            (login_timeout, "login timeout"),
            (session_timeout, "session timeout"),
        ):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"Codex {field_name} must be finite and positive.")
        if type(max_line_bytes) is not int or max_line_bytes < 1:
            raise ValueError("Codex stream limit must be a positive integer.")
        self._accounts = accounts
        self._executable = executable
        self._model = model
        self._login_timeout = float(login_timeout)
        self._session_timeout = float(session_timeout)
        self._max_line_bytes = max_line_bytes
        self._capabilities = ProviderCapabilities(
            JobRuntime.CODEX,
            {
                ProviderCapability.PLAN: CapabilitySupport.SUPPORTED,
                ProviderCapability.TOOLS: CapabilitySupport.SUPPORTED,
                ProviderCapability.FILES: CapabilitySupport.SUPPORTED,
                ProviderCapability.STREAMING: CapabilitySupport.SUPPORTED,
                ProviderCapability.COST_REPORTING: CapabilitySupport.UNSUPPORTED,
            },
        )
        self._states: dict[str, _SessionState] = {}
        self._reserved_jobs: set[str] = set()
        self._registry_lock = asyncio.Lock()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    def _environment(self, slot: CodexAccountSlot) -> dict[str, str]:
        environment = {
            key: os.environ[key] for key in _PASSTHROUGH_ENVIRONMENT if key in os.environ
        }
        environment.setdefault("PATH", os.defpath)
        environment["CODEX_HOME"] = str(slot.home)
        return environment

    async def _authenticated(self, slot: CodexAccountSlot) -> bool:
        if not slot.home.is_dir() or slot.home.is_symlink():
            return False
        try:
            result = await run_process(
                (self._executable, "login", "status"),
                cwd=slot.home,
                env=self._environment(slot),
                timeout=self._login_timeout,
            )
        except ProcessRunnerError:
            return False
        return result.returncode == 0

    async def health(self) -> ProviderHealth:
        observations = await asyncio.gather(
            *(self._authenticated(slot) for slot in self._accounts.slots)
        )
        available = sum(observations)
        state = (
            ProviderHealthState.AVAILABLE
            if available == len(observations)
            else ProviderHealthState.DEGRADED
            if available
            else ProviderHealthState.UNAVAILABLE
        )
        return ProviderHealth(state, datetime.now(UTC))

    async def start(self, request: AdapterStart) -> AdapterSession:
        if not isinstance(request, AdapterStart):
            raise AdapterError("Codex start requires AdapterStart.")
        if request.job_id != request.job_id.strip():
            raise AdapterError("Codex job identity must not have surrounding whitespace.")
        self.capabilities.require(request.required)
        try:
            write_boundary = WriteBoundary(request.worktree, request.allowed_write_paths)
            writable_roots = write_boundary.writable_roots()
        except (TypeError, ValueError, WriteBoundaryError):
            raise AdapterError("Codex write boundary is unavailable.") from None
        worktree = write_boundary.root
        async with self._registry_lock:
            if request.job_id in self._states or request.job_id in self._reserved_jobs:
                raise AdapterError("Codex job already started.")
            self._reserved_jobs.add(request.job_id)
        state: _SessionState | None = None
        try:
            account = await self._accounts.acquire(self._authenticated)
            state = _SessionState(
                request.job_id,
                account,
                worktree,
                write_boundary,
                writable_roots,
            )
            turn = self._launch(state, self._start_command(state), request.request)
            await self._await_spawn(turn)
            session = await self._await_session(state, turn)
            async with self._registry_lock:
                self._states[request.job_id] = state
            return session
        except asyncio.CancelledError:
            if state is not None and state.turn is not None:
                state.turn.cancel_requested = True
                await self._stop(state.turn)
            raise
        finally:
            async with self._registry_lock:
                self._reserved_jobs.discard(request.job_id)

    async def send(self, session: AdapterSession, message: str) -> None:
        state = self._check(session)
        if not isinstance(message, str) or not message.strip() or "\x00" in message:
            raise AdapterError("Codex message is invalid.")
        async with state.operation_lock:
            if state.cancelled or state.failed:
                raise AdapterError("Codex session has ended.")
            if state.turn is not None and state.turn.task is not None:
                await state.turn.task
            if state.cancelled or state.failed:
                raise AdapterError("Codex session has ended.")
            turn = self._launch(state, self._resume_command(state), message)
            try:
                await self._await_spawn(turn)
            except asyncio.CancelledError:
                turn.cancel_requested = True
                await self._stop(turn)
                raise

    async def cancel(self, session: AdapterSession) -> None:
        state = self._check(session)
        if state.cancelled:
            return
        state.cancelled = True
        if state.turn is not None:
            state.turn.cancel_requested = True
            await self._stop(state.turn)

    async def resume(self, session: AdapterSession) -> AdapterSession:
        self._check(session)
        raise AdapterUnsupportedError(
            "Codex resume after application restart requires durable account binding."
        )

    async def stream(self, session: AdapterSession) -> AsyncIterator[RuntimeEvent]:
        state = self._check(session)
        turn = state.turn
        if turn is None or turn.claimed:
            raise AdapterError("Codex turn stream is unavailable.")
        turn.claimed = True
        while True:
            item = await turn.queue.get()
            if item is _STREAM_END:
                return
            if not isinstance(item, RuntimeEvent):
                raise AdapterError("Codex emitted an invalid internal event.")
            yield item

    def _check(self, session: AdapterSession) -> _SessionState:
        if not isinstance(session, AdapterSession):
            raise AdapterError("Unknown Codex session.")
        state = self._states.get(session.job_id)
        if state is None or state.session != session or session.runtime is not JobRuntime.CODEX:
            raise AdapterError("Unknown Codex session.")
        return state

    def _start_command(self, state: _SessionState) -> tuple[str, ...]:
        command = [
            self._executable,
            "exec",
            "--json",
            "--color",
            "never",
            "--ignore-user-config",
            "--ignore-rules",
            "--strict-config",
            "--sandbox",
            "workspace-write",
            "--config",
            'approval_policy="never"',
            "--config",
            "sandbox_workspace_write.network_access=false",
            "--config",
            "sandbox_workspace_write.exclude_tmpdir_env_var=true",
            "--config",
            "sandbox_workspace_write.exclude_slash_tmp=true",
            "--config",
            "allow_login_shell=false",
            "--config",
            'web_search="disabled"',
            "--config",
            "features.apps=false",
            "--config",
            "features.hooks=false",
            "--config",
            "agents.enabled=false",
            "--cd",
            str(state.writable_roots[0]),
        ]
        for root in state.writable_roots[1:]:
            command.extend(("--add-dir", str(root)))
        if self._model is not None:
            command.extend(("--model", self._model))
        command.append("-")
        return tuple(command)

    def _resume_command(self, state: _SessionState) -> tuple[str, ...]:
        assert state.session is not None
        command = [
            self._executable,
            "exec",
            "--json",
            "--color",
            "never",
            "--ignore-user-config",
            "--ignore-rules",
            "--strict-config",
            "--sandbox",
            "workspace-write",
            "--config",
            'approval_policy="never"',
            "--config",
            "sandbox_workspace_write.network_access=false",
            "--config",
            "sandbox_workspace_write.exclude_tmpdir_env_var=true",
            "--config",
            "sandbox_workspace_write.exclude_slash_tmp=true",
            "--config",
            "allow_login_shell=false",
            "--config",
            'web_search="disabled"',
            "--config",
            "features.apps=false",
            "--config",
            "features.hooks=false",
            "--config",
            "agents.enabled=false",
            "--cd",
            str(state.writable_roots[0]),
        ]
        for root in state.writable_roots[1:]:
            command.extend(("--add-dir", str(root)))
        if self._model is not None:
            command.extend(("--model", self._model))
        command.extend(("resume", state.session.session_id, "-"))
        return tuple(command)

    def _launch(self, state: _SessionState, command: tuple[str, ...], prompt: str) -> _Turn:
        try:
            writable_roots = state.write_boundary.writable_roots()
        except WriteBoundaryError:
            raise AdapterError("Codex write boundary is unavailable.") from None
        if writable_roots != state.writable_roots:
            raise AdapterError("Codex write boundary is unavailable.")
        translator = CodexEventTranslator(
            state.job_id,
            state.worktree,
            first_sequence=state.next_sequence,
            expected_session_id=None if state.session is None else state.session.session_id,
            max_line_bytes=self._max_line_bytes,
            allowed_write_paths=state.write_boundary.allowed_paths,
        )
        turn = _Turn(translator, spawned=asyncio.get_running_loop().create_future())
        state.turn = turn
        turn.task = asyncio.create_task(self._run_turn(state, turn, command, prompt))
        return turn

    async def _await_spawn(self, turn: _Turn) -> None:
        assert turn.spawned is not None
        try:
            await asyncio.wait_for(asyncio.shield(turn.spawned), self._session_timeout)
        except TimeoutError:
            turn.cancel_requested = True
            await self._stop(turn)
            raise AdapterError("Codex process did not start in time.") from None

    async def _await_session(self, state: _SessionState, turn: _Turn) -> AdapterSession:
        deadline = asyncio.get_running_loop().time() + self._session_timeout
        while state.session is None:
            if turn.task is not None and turn.task.done():
                break
            if asyncio.get_running_loop().time() >= deadline:
                turn.cancel_requested = True
                await self._stop(turn)
                raise AdapterError("Codex did not provide a session identity.")
            await asyncio.sleep(0.01)
        if state.session is None:
            raise AdapterError("Codex did not provide a session identity.")
        return state.session

    async def _run_turn(
        self,
        state: _SessionState,
        turn: _Turn,
        command: tuple[str, ...],
        prompt: str,
    ) -> None:
        stderr_task: asyncio.Task[None] | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=state.worktree,
                env=self._environment(state.account),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                limit=self._max_line_bytes + 1,
            )
            turn.process = process
            if turn.spawned is not None and not turn.spawned.done():
                turn.spawned.set_result(None)
            assert process.stdin is not None
            process.stdin.write(prompt.encode("utf-8"))
            await process.stdin.drain()
            process.stdin.close()
            with suppress(BrokenPipeError, ConnectionResetError):
                await process.stdin.wait_closed()
            assert process.stderr is not None
            stderr_task = asyncio.create_task(self._discard(process.stderr))
            if turn.cancel_requested:
                await self._terminate(process)
            assert process.stdout is not None
            while line := await process.stdout.readline():
                self._publish(turn, turn.translator.feed(line))
                if turn.translator.session_id is not None and state.session is None:
                    state.session = AdapterSession(
                        state.job_id,
                        JobRuntime.CODEX,
                        turn.translator.session_id,
                    )
            returncode = await process.wait()
            if turn.cancel_requested:
                self._publish(turn, turn.translator.complete("cancelled"))
            elif returncode != 0 and turn.translator.completion_status is None:
                self._publish(
                    turn,
                    turn.translator.error("codex_process_failed", "Codex task failed."),
                )
                self._publish(turn, turn.translator.complete("failed"))
            elif turn.translator.completion_status is None:
                self._publish(
                    turn,
                    turn.translator.error(
                        "codex_incomplete_stream",
                        "Codex stream ended without completion.",
                    ),
                )
                self._publish(turn, turn.translator.complete("failed"))
            state.failed = turn.translator.completion_status == "failed"
        except asyncio.CancelledError:
            if turn.process is not None:
                await self._terminate(turn.process)
            raise
        except (OSError, UnicodeError, ValueError, CodexNativeProtocolError):
            if turn.spawned is not None and not turn.spawned.done():
                turn.spawned.set_exception(AdapterError("Codex process could not be started."))
            self._publish(
                turn,
                turn.translator.error("codex_adapter_failed", "Codex adapter failed safely."),
            )
            self._publish(turn, turn.translator.complete("failed"))
            state.failed = True
            if turn.process is not None:
                await self._terminate(turn.process)
        finally:
            state.next_sequence = turn.translator.next_sequence
            if stderr_task is not None:
                await stderr_task
            if turn.spawned is not None and not turn.spawned.done():
                turn.spawned.set_exception(AdapterError("Codex process could not be started."))
            await turn.queue.put(_STREAM_END)

    @staticmethod
    def _publish(turn: _Turn, events: tuple[RuntimeEvent, ...]) -> None:
        for event in events:
            turn.queue.put_nowait(event)

    async def _stop(self, turn: _Turn) -> None:
        if turn.process is not None and turn.process.returncode is None:
            await self._terminate(turn.process)
        if turn.task is not None:
            await turn.task

    @staticmethod
    async def _discard(reader: asyncio.StreamReader) -> None:
        while await reader.read(65_536):
            pass

    @staticmethod
    async def _terminate(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        CodexAdapter._signal_process(process, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), 2.0)
        except TimeoutError:
            CodexAdapter._signal_process(process, signal.SIGKILL)
            await process.wait()

    @staticmethod
    def _signal_process(process: asyncio.subprocess.Process, signal_number: int) -> None:
        try:
            os.killpg(process.pid, signal_number)
        except ProcessLookupError:
            return
        except PermissionError:
            with suppress(ProcessLookupError, PermissionError):
                process.send_signal(signal_number)
