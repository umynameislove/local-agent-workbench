from __future__ import annotations

import inspect
from datetime import UTC, datetime
from pathlib import Path
from typing import get_type_hints

import pytest

from engine import (
    AdapterError,
    AdapterSession,
    AdapterStart,
    AdapterUnsupportedError,
    CapabilitySupport,
    JobRuntime,
    ProviderAdapter,
    ProviderCapabilities,
    ProviderCapability,
    ProviderCapabilityError,
    ProviderHealth,
    ProviderHealthState,
)


class FakeAdapter:
    def __init__(self) -> None:
        self.capabilities = ProviderCapabilities(
            JobRuntime.LOCAL,
            {
                ProviderCapability.PLAN: CapabilitySupport.SUPPORTED,
            },
        )
        self.session: AdapterSession | None = None
        self.messages: list[str] = []
        self.cancelled = False

    def check(self, session: AdapterSession) -> None:
        if self.session != session:
            raise AdapterError("Session does not belong to this adapter.")

    async def start(self, request: AdapterStart) -> AdapterSession:
        self.capabilities.require(request.required)
        if self.session is not None:
            raise AdapterError("Session already started.")
        self.session = AdapterSession(request.job_id, JobRuntime.LOCAL, "opaque-session")
        return self.session

    async def send(self, session: AdapterSession, message: str) -> None:
        self.check(session)
        if self.cancelled:
            raise AdapterError("Session was cancelled.")
        self.messages.append(message)

    async def cancel(self, session: AdapterSession) -> None:
        self.check(session)
        self.cancelled = True

    async def health(self) -> ProviderHealth:
        return ProviderHealth(
            ProviderHealthState.AVAILABLE,
            datetime(2000, 1, 1, tzinfo=UTC),
        )

    async def resume(self, session: AdapterSession) -> AdapterSession:
        self.check(session)
        raise AdapterUnsupportedError("Session resume is unavailable.")


@pytest.mark.anyio
async def test_shared_protocol_controls_adapter_without_sdk_types(tmp_path: Path) -> None:
    fake = FakeAdapter()
    adapter: ProviderAdapter = fake
    assert (await adapter.health()).state is ProviderHealthState.AVAILABLE
    session = await adapter.start(AdapterStart("job", "Plan work", tmp_path))
    await adapter.send(session, "Continue")
    assert fake.messages == ["Continue"]
    with pytest.raises(AdapterUnsupportedError):
        await adapter.resume(session)
    assert fake.session == session
    await adapter.cancel(session)
    await adapter.cancel(session)
    with pytest.raises(AdapterError):
        await adapter.send(session, "Too late")


@pytest.mark.anyio
async def test_rejected_capability_has_no_start_effect(tmp_path: Path) -> None:
    fake = FakeAdapter()
    with pytest.raises(ProviderCapabilityError):
        await fake.start(
            AdapterStart("job", "Edit files", tmp_path, frozenset({ProviderCapability.FILES}))
        )
    assert fake.session is None


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["send", "cancel", "resume"])
async def test_wrong_session_is_rejected(tmp_path: Path, method: str) -> None:
    fake = FakeAdapter()
    await fake.start(AdapterStart("job", "Plan", tmp_path))
    wrong = AdapterSession("other-job", JobRuntime.CODEX, "opaque-session")
    with pytest.raises(AdapterError):
        if method == "send":
            await fake.send(wrong, "message")
        else:
            await getattr(fake, method)(wrong)
    assert not fake.cancelled
    assert fake.messages == []


def test_protocol_signatures_are_async_and_use_shared_contracts() -> None:
    expected = {
        "start": {"request": AdapterStart, "return": AdapterSession},
        "send": {"session": AdapterSession, "message": str, "return": type(None)},
        "cancel": {"session": AdapterSession, "return": type(None)},
        "health": {"return": ProviderHealth},
        "resume": {"session": AdapterSession, "return": AdapterSession},
    }
    for name, hints in expected.items():
        assert inspect.iscoroutinefunction(getattr(ProviderAdapter, name))
        assert get_type_hints(getattr(ProviderAdapter, name)) == hints
        assert get_type_hints(getattr(FakeAdapter, name)) == hints


@pytest.mark.anyio
async def test_resumable_adapter_preserves_durable_identity(tmp_path: Path) -> None:
    class ResumableAdapter(FakeAdapter):
        async def resume(self, session: AdapterSession) -> AdapterSession:
            self.check(session)
            return session

    original = ResumableAdapter()
    session = await original.start(AdapterStart("job", "Plan", tmp_path))
    reconnected = ResumableAdapter()
    reconnected.session = session
    adapter: ProviderAdapter = reconnected
    assert await adapter.resume(session) == session
    await adapter.send(session, "Continue from checkpoint")
    assert reconnected.messages == ["Continue from checkpoint"]


@pytest.mark.parametrize("runtime", [JobRuntime.AUTO, "local", None])
def test_sessions_reject_unresolved_runtime(runtime) -> None:
    with pytest.raises(ValueError):
        AdapterSession("job", runtime, "session")


@pytest.mark.parametrize("value", ["", "  ", "bad\x00text", None])
def test_empty_or_invalid_identity_and_request_are_rejected(tmp_path: Path, value) -> None:
    with pytest.raises(ValueError):
        AdapterStart("job", value, tmp_path)
    with pytest.raises(ValueError):
        AdapterSession(value, JobRuntime.LOCAL, "session")


def test_start_rejects_relative_paths_and_mutable_requirements(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        AdapterStart("job", "request", Path("relative"))
    with pytest.raises(TypeError):
        AdapterStart("job", "request", tmp_path, {ProviderCapability.PLAN})
