from __future__ import annotations

import pytest
from provider_conformance import ProviderConformanceSubject, ProviderContractTests

from engine import FakeProvider, JobRuntime, ProviderCapability


class TestFakeProviderConformance(ProviderContractTests):
    @pytest.fixture
    def subject(self) -> ProviderConformanceSubject:
        return ProviderConformanceSubject(
            factory=FakeProvider,
            runtime=JobRuntime.LOCAL,
            accepted_message="approve",
            required_capability=ProviderCapability.PLAN,
        )
