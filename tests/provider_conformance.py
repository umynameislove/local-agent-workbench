from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from engine import (
    AdapterError,
    AdapterSession,
    AdapterStart,
    AdapterUnsupportedError,
    CapabilitySupport,
    JobRuntime,
    ProviderAdapter,
    ProviderCapability,
    ProviderHealth,
)


@dataclass(frozen=True)
class ProviderConformanceSubject:
    """Supply provider specific inputs to the shared lifecycle contract."""

    factory: Callable[[], ProviderAdapter]
    runtime: JobRuntime
    accepted_message: str
    required_capability: ProviderCapability

    def __post_init__(self) -> None:
        if not callable(self.factory):
            raise TypeError("Conformance subject factory must be callable.")
        if not isinstance(self.runtime, JobRuntime) or self.runtime is JobRuntime.AUTO:
            raise ValueError("Conformance subject runtime must be concrete.")
        if not self.accepted_message or self.accepted_message != self.accepted_message.strip():
            raise ValueError("Conformance subject message is invalid.")
        if not isinstance(self.required_capability, ProviderCapability):
            raise TypeError("Conformance subject capability is invalid.")


class ProviderContractTests:
    """Lifecycle assertions inherited unchanged by every provider test class."""

    @pytest.fixture
    def subject(self) -> ProviderConformanceSubject:
        raise NotImplementedError

    @staticmethod
    def request(
        subject: ProviderConformanceSubject,
        tmp_path: Path,
        *,
        job_id: str = "contract-job",
    ) -> AdapterStart:
        return AdapterStart(
            job_id,
            "Verify the provider lifecycle contract.",
            tmp_path,
            frozenset({subject.required_capability}),
        )

    def test_capabilities_are_total_typed_and_immutable(
        self,
        subject: ProviderConformanceSubject,
    ) -> None:
        adapter = subject.factory()
        capabilities = adapter.capabilities

        assert capabilities.runtime is subject.runtime
        assert set(capabilities.support) == set(ProviderCapability)
        assert all(isinstance(value, CapabilitySupport) for value in capabilities.support.values())
        assert capabilities.support[subject.required_capability] is CapabilitySupport.SUPPORTED
        with pytest.raises(TypeError):
            capabilities.support[subject.required_capability] = CapabilitySupport.UNKNOWN

    @pytest.mark.anyio
    async def test_health_uses_the_shared_roundtrip_contract(
        self,
        subject: ProviderConformanceSubject,
    ) -> None:
        adapter = subject.factory()

        health = await adapter.health()

        assert isinstance(health, ProviderHealth)
        assert ProviderHealth.from_dict(health.to_dict()) == health

    @pytest.mark.anyio
    async def test_start_and_send_preserve_adapter_identity(
        self,
        subject: ProviderConformanceSubject,
        tmp_path: Path,
    ) -> None:
        adapter = subject.factory()

        session = await adapter.start(self.request(subject, tmp_path))
        await adapter.send(session, subject.accepted_message)

        assert isinstance(session, AdapterSession)
        assert session.job_id == "contract-job"
        assert session.runtime is subject.runtime
        assert session.session_id

    @pytest.mark.anyio
    @pytest.mark.parametrize("operation", ["send", "cancel", "resume"])
    async def test_session_operations_reject_foreign_job_identity(
        self,
        subject: ProviderConformanceSubject,
        tmp_path: Path,
        operation: str,
    ) -> None:
        adapter = subject.factory()
        session = await adapter.start(self.request(subject, tmp_path))
        foreign = AdapterSession("foreign-job", subject.runtime, session.session_id)

        with pytest.raises(AdapterError):
            if operation == "send":
                await adapter.send(foreign, subject.accepted_message)
            else:
                await getattr(adapter, operation)(foreign)

    @pytest.mark.anyio
    @pytest.mark.parametrize("operation", ["send", "cancel", "resume"])
    async def test_session_operations_reject_foreign_runtime_identity(
        self,
        subject: ProviderConformanceSubject,
        tmp_path: Path,
        operation: str,
    ) -> None:
        adapter = subject.factory()
        session = await adapter.start(self.request(subject, tmp_path))
        other_runtime = next(
            runtime
            for runtime in (JobRuntime.LOCAL, JobRuntime.CODEX, JobRuntime.CLAUDE)
            if runtime is not subject.runtime
        )
        foreign = AdapterSession(session.job_id, other_runtime, session.session_id)

        with pytest.raises(AdapterError):
            if operation == "send":
                await adapter.send(foreign, subject.accepted_message)
            else:
                await getattr(adapter, operation)(foreign)

    @pytest.mark.anyio
    async def test_cancel_request_is_repeatable(
        self,
        subject: ProviderConformanceSubject,
        tmp_path: Path,
    ) -> None:
        adapter = subject.factory()
        session = await adapter.start(self.request(subject, tmp_path))

        await adapter.cancel(session)
        await adapter.cancel(session)

    @pytest.mark.anyio
    async def test_resume_preserves_identity_or_is_explicitly_unsupported(
        self,
        subject: ProviderConformanceSubject,
        tmp_path: Path,
    ) -> None:
        adapter = subject.factory()
        session = await adapter.start(self.request(subject, tmp_path))

        try:
            resumed = await adapter.resume(session)
        except AdapterUnsupportedError:
            return

        assert resumed == session
