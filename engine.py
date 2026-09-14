from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

APP_VERSION = "0.1.0"
RUNTIME_ENV = "AGENT_WORKBENCH_HOME"
CONFIG_FILENAME = "config.json"
CONFIG_VERSION = 1
CONSULTANT_MODEL = "deepseek/deepseek-v4-flash-0731"
CONSULTANT_RECOMMENDATIONS = frozenset({"codex", "claude", "local", "ask_user"})


class ProcessRunnerError(RuntimeError):
    """Report process failures without exposing command arguments or output."""


class ProcessTimeoutError(ProcessRunnerError):
    """Raised after a timed out process has been terminated and reaped."""


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes


async def run_process(
    argv: tuple[str, ...],
    *,
    cwd: Path,
    timeout: float = 60.0,
) -> ProcessResult:
    """Run structured arguments on POSIX and reap the process group on cancellation.

    Output is captured for bounded commands, not continuous provider streams.
    Nonzero exit codes are returned to the caller as ordinary results.
    """
    if os.name != "posix":
        raise ProcessRunnerError("Process groups require a POSIX platform.")
    if (
        not isinstance(argv, tuple)
        or not argv
        or any(not isinstance(arg, str) or "\x00" in arg for arg in argv)
        or not argv[0]
    ):
        raise ValueError("Process arguments must be a nonempty tuple of strings.")
    if not isinstance(cwd, Path) or not cwd.is_absolute() or not cwd.is_dir():
        raise ValueError("Process working directory must be an existing absolute Path.")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Process timeout must be finite and positive.")

    async def reap(process: asyncio.subprocess.Process) -> None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()

    async def finish(task: asyncio.Task) -> object:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        return task.result()

    spawn = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
    )
    try:
        process = await asyncio.shield(spawn)
    except asyncio.CancelledError:
        try:
            process = await finish(spawn)
        except OSError:
            raise asyncio.CancelledError from None
        await finish(asyncio.create_task(reap(process)))
        raise
    except OSError:
        raise ProcessRunnerError("Process could not be started.") from None

    communication = asyncio.create_task(process.communicate())
    try:
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout)
        return ProcessResult(process.returncode, stdout, stderr)
    except (asyncio.CancelledError, TimeoutError) as error:
        await finish(asyncio.create_task(reap(process)))
        await finish(communication)
        if isinstance(error, asyncio.CancelledError):
            raise
        raise ProcessTimeoutError("Process timed out.") from None


class ConfigurationError(ValueError):
    """Raised when public or local configuration violates a hard contract."""


class Sensitivity(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    INTERNAL = "internal"
    RESTRICTED = "restricted"


class PermissionMode(StrEnum):
    READ_ONLY = "read-only"
    SANDBOXED_WRITE = "sandboxed-write"
    NEVER = "never"


class JobState(StrEnum):
    CREATED = "created"
    CLASSIFIED = "classified"
    PLANNING = "planning"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    WAITING_APPROVAL = "waiting_approval"
    VERIFYING = "verifying"
    REVIEW_READY = "review_ready"
    APPROVED = "approved"
    REJECTED = "rejected"
    APPLYING = "applying"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class ForbiddenJobTransitionError(ValueError):
    """Raised when a job attempts to leave its defined lifecycle."""

    def __init__(self, current_state: JobState, target_state: JobState) -> None:
        self.current_state = current_state
        self.target_state = target_state
        super().__init__(
            f"Job cannot transition from {current_state.value} to {target_state.value}."
        )


_STOP_STATES = frozenset(
    {
        JobState.FAILED,
        JobState.BLOCKED,
        JobState.CANCELLED,
    }
)


def _with_stop_states(*states: JobState) -> frozenset[JobState]:
    return frozenset(states) | _STOP_STATES


ALLOWED_JOB_TRANSITIONS: Mapping[JobState, frozenset[JobState]] = MappingProxyType(
    {
        JobState.CREATED: _with_stop_states(JobState.CLASSIFIED),
        JobState.CLASSIFIED: _with_stop_states(JobState.PLANNING),
        JobState.PLANNING: _with_stop_states(
            JobState.QUEUED,
            JobState.WAITING_INPUT,
        ),
        JobState.QUEUED: _with_stop_states(JobState.RUNNING),
        JobState.RUNNING: _with_stop_states(
            JobState.WAITING_INPUT,
            JobState.WAITING_APPROVAL,
            JobState.VERIFYING,
        ),
        JobState.WAITING_INPUT: _with_stop_states(
            JobState.PLANNING,
            JobState.QUEUED,
            JobState.RUNNING,
        ),
        JobState.WAITING_APPROVAL: _with_stop_states(
            JobState.RUNNING,
            JobState.APPROVED,
            JobState.REJECTED,
        ),
        JobState.VERIFYING: _with_stop_states(
            JobState.RUNNING,
            JobState.REVIEW_READY,
        ),
        JobState.REVIEW_READY: _with_stop_states(
            JobState.RUNNING,
            JobState.WAITING_APPROVAL,
            JobState.APPROVED,
            JobState.REJECTED,
        ),
        JobState.APPROVED: _with_stop_states(JobState.APPLYING),
        JobState.APPLYING: _with_stop_states(JobState.COMPLETED),
        JobState.COMPLETED: frozenset(),
        JobState.REJECTED: frozenset(),
        JobState.FAILED: frozenset(),
        JobState.BLOCKED: frozenset(),
        JobState.CANCELLED: frozenset(),
    }
)

TERMINAL_JOB_STATES = frozenset(
    state for state, transitions in ALLOWED_JOB_TRANSITIONS.items() if not transitions
)


def validate_job_transition(current_state: JobState, target_state: JobState) -> None:
    """Require a transition declared by the durable job lifecycle."""

    if not isinstance(current_state, JobState) or not isinstance(target_state, JobState):
        raise TypeError("Job transition states must use JobState values.")
    if target_state not in ALLOWED_JOB_TRANSITIONS[current_state]:
        raise ForbiddenJobTransitionError(current_state, target_state)


class JobRuntime(StrEnum):
    AUTO = "auto"
    CLAUDE = "claude"
    CODEX = "codex"
    LOCAL = "local"


class ProviderCapability(StrEnum):
    PLAN = "plan"
    TOOLS = "tools"
    FILES = "files"
    STREAMING = "streaming"
    COST_REPORTING = "cost_reporting"


class CapabilitySupport(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class ProviderCapabilityError(ValueError):
    """Raised when a runtime cannot satisfy required capabilities."""

    def __init__(self, runtime: JobRuntime, missing: tuple[ProviderCapability, ...]) -> None:
        self.runtime = runtime
        self.missing = missing
        super().__init__(
            f"Runtime {runtime.value} lacks confirmed capabilities: "
            + ", ".join(capability.value for capability in missing)
        )


@dataclass(frozen=True)
class ProviderCapabilities:
    """Describe adapter capabilities without granting execution permission."""

    runtime: JobRuntime
    support: Mapping[ProviderCapability, CapabilitySupport]

    def __post_init__(self) -> None:
        if not isinstance(self.runtime, JobRuntime) or self.runtime is JobRuntime.AUTO:
            raise ValueError("Capabilities require a concrete runtime.")
        if not isinstance(self.support, Mapping):
            raise TypeError("Capability support must be a mapping.")
        snapshot = dict(self.support)
        if any(
            not isinstance(key, ProviderCapability) or not isinstance(value, CapabilitySupport)
            for key, value in snapshot.items()
        ):
            raise TypeError("Capability entries must use typed capability and support values.")
        object.__setattr__(
            self,
            "support",
            MappingProxyType(
                {
                    capability: snapshot.get(capability, CapabilitySupport.UNKNOWN)
                    for capability in ProviderCapability
                }
            ),
        )

    def require(self, required: frozenset[ProviderCapability]) -> None:
        """Reject unmet requirements before the caller invokes an adapter."""
        if not isinstance(required, frozenset) or any(
            not isinstance(capability, ProviderCapability) for capability in required
        ):
            raise TypeError("Requirements must be a frozenset of ProviderCapability values.")
        missing = tuple(
            capability
            for capability in ProviderCapability
            if capability in required
            and self.support[capability] is not CapabilitySupport.SUPPORTED
        )
        if missing:
            raise ProviderCapabilityError(self.runtime, missing)


class AdapterError(RuntimeError):
    """Report a sanitized adapter failure without raw provider output."""


class AdapterUnsupportedError(AdapterError):
    """Report an operation that the adapter cannot perform."""


class ProviderHealthState(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    RATE_LIMITED = "rate_limited"
    DEGRADED = "degraded"


@dataclass(frozen=True)
class ProviderHealth:
    """Represent one truthful, time bounded provider health observation."""

    state: ProviderHealthState
    observed_at: datetime
    reset_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ProviderHealthState):
            raise ValueError("Provider health state must use ProviderHealthState.")
        if not isinstance(self.observed_at, datetime) or self.observed_at.utcoffset() is None:
            raise ValueError("Provider health observation time must include a timezone.")
        observed_at = self.observed_at.astimezone(UTC)
        reset_at = self.reset_at
        if reset_at is not None:
            if not isinstance(reset_at, datetime) or reset_at.utcoffset() is None:
                raise ValueError("Provider health reset time must include a timezone.")
            reset_at = reset_at.astimezone(UTC)
            if reset_at <= observed_at:
                raise ValueError("Provider health reset time must follow its observation.")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "reset_at", reset_at)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "state": self.state.value,
            "observed_at": self.observed_at.isoformat().replace("+00:00", "Z"),
            "reset_at": (
                None if self.reset_at is None else self.reset_at.isoformat().replace("+00:00", "Z")
            ),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProviderHealth:
        if not isinstance(value, Mapping) or value.keys() != {
            "state",
            "observed_at",
            "reset_at",
        }:
            raise ValueError("Provider health fields are invalid.")
        try:
            observed_at = value["observed_at"]
            reset_at = value["reset_at"]
            if not isinstance(observed_at, str):
                raise ValueError
            if reset_at is not None and not isinstance(reset_at, str):
                raise ValueError
            return cls(
                state=ProviderHealthState(value["state"]),
                observed_at=datetime.fromisoformat(observed_at),
                reset_at=None if reset_at is None else datetime.fromisoformat(reset_at),
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError("Provider health is invalid.") from error


def _adapter_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"Adapter {field} must be nonempty text without null characters.")


@dataclass(frozen=True)
class AdapterSession:
    job_id: str
    runtime: JobRuntime
    session_id: str

    def __post_init__(self) -> None:
        _adapter_text(self.job_id, "job_id")
        _adapter_text(self.session_id, "session_id")
        if not isinstance(self.runtime, JobRuntime) or self.runtime is JobRuntime.AUTO:
            raise ValueError("Adapter sessions require a concrete runtime.")


@dataclass(frozen=True)
class AdapterStart:
    job_id: str
    request: str
    worktree: Path
    required: frozenset[ProviderCapability] = frozenset()

    def __post_init__(self) -> None:
        _adapter_text(self.job_id, "job_id")
        _adapter_text(self.request, "request")
        if not isinstance(self.worktree, Path) or not self.worktree.is_absolute():
            raise ValueError("Adapter worktree must be an absolute Path.")
        if not isinstance(self.required, frozenset) or any(
            not isinstance(item, ProviderCapability) for item in self.required
        ):
            raise TypeError("Adapter requirements must be typed immutable capabilities.")


class ProviderAdapter(Protocol):
    """Async control boundary; implementations keep provider SDK types private.

    Session methods must reject mismatched runtime or job identity. Unsupported
    operations raise AdapterUnsupportedError. Implementations translate provider
    failures to sanitized AdapterError and preserve task cancellation.
    """

    @property
    def capabilities(self) -> ProviderCapabilities: ...

    async def start(self, request: AdapterStart) -> AdapterSession:
        """Check required capabilities before starting work and return its session."""
        ...

    async def send(self, session: AdapterSession, message: str) -> None:
        """Accept input for the existing session; acknowledgement is not completion."""
        ...

    async def cancel(self, session: AdapterSession) -> None:
        """Request cancellation; repeated requests must be safe."""
        ...

    async def health(self) -> ProviderHealth:
        """Return a timestamped observation without inferring quota information."""
        ...

    async def resume(self, session: AdapterSession) -> AdapterSession:
        """Reconnect the same durable session without silently starting a new job."""
        ...


class RuntimeEventKind(StrEnum):
    TEXT = "text"
    PLAN = "plan"
    TOOL = "tool"
    FILE = "file"
    USAGE = "usage"
    QUESTION = "question"
    ERROR = "error"
    COMPLETION = "completion"


@dataclass(frozen=True)
class RuntimeEvent:
    """Normalized adapter output, distinct from the persisted event ledger."""

    job_id: str
    sequence: int
    runtime: JobRuntime
    timestamp: datetime
    kind: RuntimeEventKind
    payload: Mapping[str, str | int | bool | None]

    def __post_init__(self) -> None:
        _adapter_text(self.job_id, "job_id")
        if self.job_id != self.job_id.strip():
            raise ValueError("Event job identity must not have surrounding whitespace.")
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError("Event sequence must be a positive integer.")
        if not isinstance(self.runtime, JobRuntime) or self.runtime is JobRuntime.AUTO:
            raise ValueError("Event runtime must be concrete.")
        if not isinstance(self.timestamp, datetime) or self.timestamp.utcoffset() is None:
            raise ValueError("Event timestamp must include a timezone.")
        if not isinstance(self.kind, RuntimeEventKind):
            raise ValueError("Event kind must use RuntimeEventKind.")
        if not isinstance(self.payload, Mapping):
            raise ValueError("Event payload must be a mapping.")
        payload = dict(self.payload)
        fields = {
            RuntimeEventKind.TEXT: {"text"},
            RuntimeEventKind.PLAN: {"text"},
            RuntimeEventKind.TOOL: {"call_id", "name", "status"},
            RuntimeEventKind.FILE: {"path", "action"},
            RuntimeEventKind.USAGE: {"input_tokens", "output_tokens", "cost_usd"},
            RuntimeEventKind.QUESTION: {"question_id", "text"},
            RuntimeEventKind.ERROR: {"code", "message", "retryable"},
            RuntimeEventKind.COMPLETION: {"status"},
        }
        if payload.keys() != fields[self.kind]:
            raise ValueError("Event payload fields do not match its kind.")
        if self.kind is RuntimeEventKind.USAGE:
            for field in ("input_tokens", "output_tokens"):
                value = payload[field]
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError("Event token counts must be nonnegative integers or null.")
            cost = payload["cost_usd"]
            if cost is not None:
                if not isinstance(cost, str):
                    raise ValueError("Event cost must be a decimal string or null.")
                try:
                    amount = Decimal(cost)
                except ArithmeticError as error:
                    raise ValueError("Event cost is invalid.") from error
                if not amount.is_finite() or amount < 0:
                    raise ValueError("Event cost must be finite and nonnegative.")
        else:
            for field, value in payload.items():
                if field == "retryable":
                    if type(value) is not bool:
                        raise ValueError("Event retryable must be a boolean.")
                elif field == "text" and self.kind is RuntimeEventKind.TEXT:
                    if not isinstance(value, str) or "\x00" in value:
                        raise ValueError("Event text must be a string without null characters.")
                else:
                    _adapter_text(value, field)
        choices = {
            RuntimeEventKind.TOOL: ("status", {"started", "completed", "failed"}),
            RuntimeEventKind.FILE: ("action", {"created", "modified", "deleted"}),
            RuntimeEventKind.COMPLETION: ("status", {"completed", "failed", "cancelled"}),
        }
        if self.kind in choices:
            field, allowed = choices[self.kind]
            if payload[field] not in allowed:
                raise ValueError("Event payload status or action is invalid.")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))
        object.__setattr__(self, "payload", MappingProxyType(payload))

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "sequence": self.sequence,
            "runtime": self.runtime.value,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "kind": self.kind.value,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> RuntimeEvent:
        if not isinstance(value, Mapping) or value.keys() != {
            "job_id",
            "sequence",
            "runtime",
            "timestamp",
            "kind",
            "payload",
        }:
            raise ValueError("Event envelope fields are invalid.")
        try:
            timestamp = value["timestamp"]
            if not isinstance(timestamp, str):
                raise ValueError("Event timestamp must be an ISO timestamp string.")
            return cls(
                job_id=value["job_id"],
                sequence=value["sequence"],
                runtime=JobRuntime(value["runtime"]),
                timestamp=datetime.fromisoformat(timestamp),
                kind=RuntimeEventKind(value["kind"]),
                payload=value["payload"],
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError("Runtime event is invalid.") from error


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Provider stream JSON contains a duplicate field.")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Provider stream JSON constant is invalid: {value}")


class RuntimeEventStreamNormalizer:
    """Convert bounded UTF-8 JSON lines into ordered runtime events.

    Each provider line contains only ``kind`` and ``payload``. Job identity,
    runtime, sequence and observation time remain owned by the application.
    Malformed input becomes a sanitized error event so later lines can proceed.
    """

    def __init__(
        self,
        session: AdapterSession,
        *,
        first_sequence: int = 1,
        max_line_bytes: int = 1_048_576,
    ) -> None:
        if not isinstance(session, AdapterSession):
            raise TypeError("Stream normalization requires an AdapterSession.")
        if type(first_sequence) is not int or first_sequence < 1:
            raise ValueError("First event sequence must be a positive integer.")
        if type(max_line_bytes) is not int or max_line_bytes < 1:
            raise ValueError("Maximum stream line size must be a positive integer.")
        self._session = session
        self._next_sequence = first_sequence
        self._max_line_bytes = max_line_bytes
        self._buffer = bytearray()
        self._discarding_oversized_line = False
        self._finished = False

    def _event(
        self,
        kind: RuntimeEventKind,
        payload: Mapping[str, str | int | bool | None],
    ) -> RuntimeEvent:
        event = RuntimeEvent(
            job_id=self._session.job_id,
            sequence=self._next_sequence,
            runtime=self._session.runtime,
            timestamp=datetime.now(UTC),
            kind=kind,
            payload=payload,
        )
        self._next_sequence += 1
        return event

    def _error(self, code: str, message: str) -> RuntimeEvent:
        return self._event(
            RuntimeEventKind.ERROR,
            {"code": code, "message": message, "retryable": False},
        )

    def _parse_line(self, line: bytes) -> RuntimeEvent:
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError:
            return self._error(
                "stream_invalid_encoding",
                "Provider stream contained invalid UTF-8.",
            )
        try:
            value = json.loads(
                text,
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, RecursionError, ValueError):
            return self._error(
                "stream_invalid_json",
                "Provider stream contained invalid JSON.",
            )
        if not isinstance(value, dict) or value.keys() != {"kind", "payload"}:
            return self._error(
                "stream_invalid_event",
                "Provider stream message violated the event contract.",
            )
        try:
            return self._event(RuntimeEventKind(value["kind"]), value["payload"])
        except (TypeError, ValueError):
            return self._error(
                "stream_invalid_event",
                "Provider stream message violated the event contract.",
            )

    def feed(self, chunk: bytes) -> tuple[RuntimeEvent, ...]:
        """Consume one byte chunk and return every complete event in order."""
        if self._finished:
            raise RuntimeError("Provider stream has already finished.")
        if not isinstance(chunk, bytes):
            raise TypeError("Provider stream chunks must be bytes.")
        if not chunk:
            return ()
        self._buffer.extend(chunk)
        events: list[RuntimeEvent] = []
        while True:
            newline = self._buffer.find(b"\n")
            if newline < 0:
                if len(self._buffer) > self._max_line_bytes:
                    self._buffer.clear()
                    if not self._discarding_oversized_line:
                        self._discarding_oversized_line = True
                        events.append(
                            self._error(
                                "stream_message_too_large",
                                "Provider stream message exceeded the configured size limit.",
                            )
                        )
                break
            line = bytes(self._buffer[:newline])
            del self._buffer[: newline + 1]
            if self._discarding_oversized_line:
                self._discarding_oversized_line = False
                continue
            if line.endswith(b"\r"):
                line = line[:-1]
            if len(line) > self._max_line_bytes:
                events.append(
                    self._error(
                        "stream_message_too_large",
                        "Provider stream message exceeded the configured size limit.",
                    )
                )
            elif line.strip():
                events.append(self._parse_line(line))
        return tuple(events)

    def finish(self) -> tuple[RuntimeEvent, ...]:
        """Flush one final unterminated line and close the normalizer."""
        if self._finished:
            return ()
        self._finished = True
        if self._discarding_oversized_line:
            self._buffer.clear()
            return ()
        line = bytes(self._buffer)
        self._buffer.clear()
        if line.endswith(b"\r"):
            line = line[:-1]
        if not line.strip():
            return ()
        return (self._parse_line(line),)


class FakeProvider:
    """Deterministic in memory demo adapter; never executes tools or writes files."""

    def __init__(self) -> None:
        self._capabilities = ProviderCapabilities(
            JobRuntime.LOCAL,
            {
                ProviderCapability.PLAN: CapabilitySupport.SUPPORTED,
                ProviderCapability.TOOLS: CapabilitySupport.UNSUPPORTED,
                ProviderCapability.FILES: CapabilitySupport.UNSUPPORTED,
                ProviderCapability.STREAMING: CapabilitySupport.UNSUPPORTED,
                ProviderCapability.COST_REPORTING: CapabilitySupport.UNSUPPORTED,
            },
        )
        self._sessions: dict[str, AdapterSession] = {}
        self._events: dict[str, tuple[RuntimeEvent, ...]] = {}
        self._finished: set[str] = set()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    def _check(self, session: AdapterSession) -> None:
        if not isinstance(session, AdapterSession) or self._sessions.get(session.job_id) != session:
            raise AdapterError("Unknown demo session.")

    def _emit(self, session: AdapterSession, kind: RuntimeEventKind, payload: Mapping) -> None:
        events = self._events[session.job_id]
        event = RuntimeEvent(
            session.job_id,
            len(events) + 1,
            session.runtime,
            datetime(2000, 1, 1, tzinfo=UTC),
            kind,
            payload,
        )
        self._events[session.job_id] = (*events, event)

    async def start(self, request: AdapterStart) -> AdapterSession:
        if not isinstance(request, AdapterStart):
            raise AdapterError("Demo start requires AdapterStart.")
        if request.job_id != request.job_id.strip():
            raise AdapterError("Demo job identity must not have surrounding whitespace.")
        self.capabilities.require(request.required)
        if request.job_id in self._sessions:
            raise AdapterError("Demo job already started.")
        session = AdapterSession(request.job_id, JobRuntime.LOCAL, f"fake:{request.job_id}")
        self._sessions[request.job_id] = session
        self._events[request.job_id] = ()
        for kind, payload in (
            (
                RuntimeEventKind.PLAN,
                {"text": "Demo plan: inspect, propose, verify, request review."},
            ),
            (
                RuntimeEventKind.TEXT,
                {"text": "Demo write proposal: add a greeting. No files changed."},
            ),
            (
                RuntimeEventKind.TEXT,
                {"text": "Demo verification: simulated pass. No tests executed."},
            ),
            (
                RuntimeEventKind.QUESTION,
                {
                    "question_id": "demo-review",
                    "text": "Demo review: send approve or reject. No real changes will be applied.",
                },
            ),
        ):
            self._emit(session, kind, payload)
        return session

    def events(self, session: AdapterSession) -> tuple[RuntimeEvent, ...]:
        """Return a replayable snapshot; this is not the production streaming contract."""
        self._check(session)
        return self._events[session.job_id]

    async def send(self, session: AdapterSession, message: str) -> None:
        self._check(session)
        if session.job_id in self._finished:
            raise AdapterError("Demo session has ended.")
        if message not in ("approve", "reject"):
            raise AdapterError("Demo input must be approve or reject.")
        self._emit(
            session,
            RuntimeEventKind.COMPLETION,
            {"status": "completed" if message == "approve" else "cancelled"},
        )
        self._finished.add(session.job_id)

    async def cancel(self, session: AdapterSession) -> None:
        self._check(session)
        if session.job_id not in self._finished:
            self._emit(session, RuntimeEventKind.COMPLETION, {"status": "cancelled"})
            self._finished.add(session.job_id)

    async def health(self) -> ProviderHealth:
        return ProviderHealth(
            ProviderHealthState.AVAILABLE,
            datetime(2000, 1, 1, tzinfo=UTC),
        )

    async def resume(self, session: AdapterSession) -> AdapterSession:
        self._check(session)
        raise AdapterUnsupportedError("Demo sessions do not support durable resume.")


class RecoveryAction(StrEnum):
    SAFE_RESUME = "safe_resume"
    WAIT_FOR_INPUT = "wait_for_input"
    WAIT_FOR_APPROVAL = "wait_for_approval"
    READY_FOR_REVIEW = "ready_for_review"
    READY_TO_APPLY = "ready_to_apply"
    RECONCILE_IN_FLIGHT = "reconcile_in_flight"
    NEEDS_ATTENTION = "needs_attention"


class RecoveryIssue(StrEnum):
    WORKTREE_REQUIRED = "worktree_required"
    WORKTREE_UNAVAILABLE = "worktree_unavailable"
    WORKTREE_OUTSIDE_RUNTIME = "worktree_outside_runtime"
    APPROVAL_REQUIRED = "approval_required"
    MULTIPLE_PENDING_APPROVALS = "multiple_pending_approvals"
    APPROVAL_EXPIRED = "approval_expired"
    UNEXPECTED_PENDING_APPROVAL = "unexpected_pending_approval"


class ApprovalDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    CHANGES_REQUESTED = "changes_requested"


class PlannerItemKind(StrEnum):
    DEADLINE = "deadline"
    BLOCKER = "blocker"
    WATCHER = "watcher"


@dataclass(frozen=True)
class RuntimeHome:
    root: Path

    @property
    def config(self) -> Path:
        return self.root / CONFIG_FILENAME

    @property
    def state_db(self) -> Path:
        return self.root / "state.db"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def cache(self) -> Path:
        return self.root / "cache"

    @property
    def worktrees(self) -> Path:
        return self.root / "worktrees"

    def bootstrap(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (self.logs, self.cache, self.worktrees):
            directory.mkdir(exist_ok=True)


def _platform_default_home(home: Path | None = None) -> Path:
    user_home = (home or Path.home()).expanduser()
    if sys.platform == "darwin":
        return user_home / "Library" / "Application Support" / "LocalAgentWorkbench"
    data_home = os.environ.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home).expanduser() / "local-agent-workbench"
    return user_home / ".local" / "share" / "local-agent-workbench"


def resolve_runtime_home(
    env: Mapping[str, str] | None = None,
    *,
    cwd: Path | None = None,
    user_home: Path | None = None,
) -> RuntimeHome:
    values = os.environ if env is None else env
    configured = values.get(RUNTIME_ENV, "").strip()
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = (cwd or Path.cwd()) / candidate
    else:
        candidate = _platform_default_home(user_home)
    return RuntimeHome(candidate.resolve())


@dataclass(frozen=True)
class ProjectConfig:
    id: str
    root: str
    sensitivity: Sensitivity
    cloud_allowed: bool
    permission_mode: PermissionMode

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ProjectConfig:
        if not isinstance(value, Mapping):
            raise ConfigurationError("Each project must be an object.")
        project_id = value.get("id")
        root = value.get("root")
        if any(
            not isinstance(item, str) or not item or item != item.strip() or "\x00" in item
            for item in (project_id, root)
        ):
            raise ConfigurationError("Each project requires an id and a root value.")
        try:
            sensitivity = Sensitivity(value.get("sensitivity", Sensitivity.PRIVATE))
            permission_mode = PermissionMode(
                value.get("permission_mode", PermissionMode.SANDBOXED_WRITE)
            )
        except ValueError:
            raise ConfigurationError("Project sensitivity or permission mode is invalid.") from None
        cloud_allowed = value.get("cloud_allowed", False)
        if not isinstance(cloud_allowed, bool):
            raise ConfigurationError("Project cloud_allowed must be a boolean.")
        if sensitivity in {Sensitivity.INTERNAL, Sensitivity.RESTRICTED} and cloud_allowed:
            raise ConfigurationError("Internal and restricted projects default to cloud disabled.")
        return cls(
            id=project_id,
            root=root,
            sensitivity=sensitivity,
            cloud_allowed=cloud_allowed,
            permission_mode=permission_mode,
        )


@dataclass(frozen=True)
class JobCreate:
    id: str
    project_id: str
    request: str
    request_snapshot: Mapping[str, Any]
    state: JobState = JobState.CREATED
    runtime: JobRuntime = JobRuntime.AUTO
    model: str | None = None
    worktree_path: str | None = None


@dataclass(frozen=True)
class JobUpdate:
    id: str
    state: JobState
    runtime: JobRuntime
    model: str | None = None
    worktree_path: str | None = None


@dataclass(frozen=True)
class EventCreate:
    job_id: str
    event_type: str
    payload: Mapping[str, Any]
    idempotency_key: str | None = None


@dataclass(frozen=True)
class ApprovalCreate:
    id: str
    job_id: str
    payload: Mapping[str, Any]
    expires_at: datetime


@dataclass(frozen=True)
class ApprovalResolution:
    decision: ApprovalDecision
    actor: str
    channel: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class PlannerItemCreate:
    id: str
    kind: PlannerItemKind
    title: str
    project_id: str | None = None
    details: str | None = None
    due_at: datetime | None = None
    source: str | None = None
    source_key: str | None = None


@dataclass(frozen=True)
class UsageCreate:
    provider: str
    job_id: str | None = None
    model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: Decimal | None = None
    latency_ms: int | None = None
    quota_limit: Decimal | None = None
    quota_remaining: Decimal | None = None
    quota_unit: str | None = None
    quota_reset_at: datetime | None = None
    rate_limited: bool | None = None


@dataclass(frozen=True)
class MemoryReferenceCreate:
    projmem_record_id: str
    job_id: str
    event_id: int | None = None


@dataclass(frozen=True)
class ConsultantConfig:
    enabled: bool = False
    model: str = CONSULTANT_MODEL
    min_confidence: float = 0.8
    monthly_hard_cap_usd: float = 5.0
    warning_usd: float = 4.0
    per_job_cap_usd: float = 0.1

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ConsultantConfig:
        if not isinstance(value, Mapping):
            raise ConfigurationError("consultant must be an object.")
        enabled = value.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ConfigurationError("Consultant enabled must be a boolean.")
        try:
            config = cls(
                enabled=enabled,
                model=str(value.get("model", CONSULTANT_MODEL)),
                min_confidence=float(value.get("min_confidence", 0.8)),
                monthly_hard_cap_usd=float(value.get("monthly_hard_cap_usd", 5.0)),
                warning_usd=float(value.get("warning_usd", 4.0)),
                per_job_cap_usd=float(value.get("per_job_cap_usd", 0.1)),
            )
        except (TypeError, ValueError):
            raise ConfigurationError("Consultant limits must be numeric.") from None
        if config.model != CONSULTANT_MODEL:
            raise ConfigurationError(f"V1 consultant model must be {CONSULTANT_MODEL}.")
        if not 0.0 <= config.min_confidence <= 1.0:
            raise ConfigurationError("Consultant min_confidence must be between 0 and 1.")
        if not 0.0 <= config.warning_usd <= config.monthly_hard_cap_usd:
            raise ConfigurationError("Consultant warning must not exceed the monthly hard cap.")
        if not 0.0 < config.per_job_cap_usd <= config.monthly_hard_cap_usd:
            raise ConfigurationError("Consultant job cap must be positive and within monthly cap.")
        return config


@dataclass(frozen=True)
class WorkbenchConfig:
    version: int
    projects: tuple[ProjectConfig, ...]
    providers: Mapping[str, bool]
    consultant: ConsultantConfig

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> WorkbenchConfig:
        if not isinstance(value, Mapping):
            raise ConfigurationError("Config root must be an object.")
        if not isinstance(value.get("version"), int) or isinstance(value.get("version"), bool):
            raise ConfigurationError("Config version must be an integer.")
        if value.get("version") != CONFIG_VERSION:
            raise ConfigurationError("Unsupported config version.")
        project_values = value.get("projects", [])
        if not isinstance(project_values, list):
            raise ConfigurationError("projects must be an array.")
        projects = tuple(ProjectConfig.from_dict(item) for item in project_values)
        if not projects:
            raise ConfigurationError("At least one project is required.")
        if len({project.id for project in projects}) != len(projects):
            raise ConfigurationError("Project ids must be unique.")
        providers = value.get("providers", {})
        if not isinstance(providers, Mapping):
            raise ConfigurationError("providers must be an object.")
        normalized_providers: dict[str, bool] = {}
        for key, enabled in providers.items():
            if not isinstance(enabled, bool):
                raise ConfigurationError("Provider flags must be booleans.")
            normalized_providers[str(key)] = enabled
        return cls(
            version=CONFIG_VERSION,
            projects=projects,
            providers=normalized_providers,
            consultant=ConsultantConfig.from_dict(value.get("consultant", {})),
        )

    @classmethod
    def load(cls, path: Path) -> WorkbenchConfig:
        try:
            with path.open(encoding="utf-8") as handle:
                value = json.load(handle)
        except FileNotFoundError:
            raise ConfigurationError("Configuration file does not exist.") from None
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise ConfigurationError(
                "Configuration file could not be read as valid JSON."
            ) from None
        if not isinstance(value, Mapping):
            raise ConfigurationError("Config root must be an object.")
        return cls.from_dict(value)


async def load_configured_projects(path: Path) -> tuple[ProjectConfig, ...]:
    """Load configured projects and require each path to be an exact Git root."""

    try:
        path.lstat()
    except FileNotFoundError:
        return ()
    except OSError:
        raise ConfigurationError("Configuration file could not be inspected.") from None
    config = WorkbenchConfig.load(path)
    try:
        base = path.parent.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ConfigurationError("Configuration directory could not be resolved.") from None
    projects: list[ProjectConfig] = []
    for project in config.projects:
        try:
            candidate = Path(project.root).expanduser()
            if not candidate.is_absolute():
                candidate = base / candidate
            root = candidate.resolve(strict=True)
        except (OSError, RuntimeError, ValueError):
            raise ConfigurationError("Configured project root does not exist.") from None
        if not root.is_dir():
            raise ConfigurationError("Configured project root must be a directory.")
        try:
            result = await run_process(
                ("git", "rev-parse", "--show-toplevel"),
                cwd=root,
                timeout=10,
            )
        except ProcessRunnerError:
            raise ConfigurationError(
                "Git could not be started while validating the configured project root."
            ) from None
        if result.returncode != 0:
            raise ConfigurationError(
                "Configured project root must be the top level of a Git worktree."
            )
        try:
            reported = result.stdout.decode("utf-8").rstrip("\r\n")
            git_root = Path(reported).resolve(strict=True)
            same_root = bool(reported) and root.samefile(git_root)
        except (OSError, RuntimeError, UnicodeError, ValueError):
            raise ConfigurationError("Git returned an invalid project root.") from None
        if not same_root:
            raise ConfigurationError(
                "Configured project root points inside a Git worktree. Use its top level directory."
            )
        projects.append(replace(project, root=str(git_root)))
    if len({project.root for project in projects}) != len(projects):
        raise ConfigurationError("Project roots must be unique.")
    return tuple(projects)


def validate_consultant_advice(
    value: Mapping[str, Any],
    *,
    known_task_ids: set[str],
    known_evidence_ids: set[str],
    min_confidence: float = 0.8,
) -> dict[str, Any]:
    required = {
        "recommendation",
        "task_ids",
        "evidence_ids",
        "assumptions",
        "unknowns",
        "confidence",
    }
    if set(value) != required:
        raise ConfigurationError("Consultant advice has missing or unknown fields.")
    recommendation = value["recommendation"]
    if recommendation not in CONSULTANT_RECOMMENDATIONS:
        raise ConfigurationError("Consultant recommendation is invalid.")
    for field in ("task_ids", "evidence_ids", "assumptions", "unknowns"):
        if not isinstance(value[field], list) or not all(
            isinstance(item, str) for item in value[field]
        ):
            raise ConfigurationError(f"Consultant {field} must be a string array.")
    if not set(value["task_ids"]).issubset(known_task_ids):
        raise ConfigurationError("Consultant referenced an unknown task id.")
    if not set(value["evidence_ids"]).issubset(known_evidence_ids):
        raise ConfigurationError("Consultant referenced an unknown evidence id.")
    confidence = float(value["confidence"])
    if confidence < min_confidence or confidence > 1.0:
        raise ConfigurationError("Consultant confidence is outside the accepted range.")
    return dict(value)
