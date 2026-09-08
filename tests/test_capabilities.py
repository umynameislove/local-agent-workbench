from dataclasses import FrozenInstanceError

import pytest

from engine import (
    CapabilitySupport,
    JobRuntime,
    ProviderCapabilities,
    ProviderCapability,
    ProviderCapabilityError,
)


@pytest.mark.parametrize("runtime", [JobRuntime.CLAUDE, JobRuntime.CODEX, JobRuntime.LOCAL])
@pytest.mark.parametrize("capability", list(ProviderCapability))
@pytest.mark.parametrize("support", list(CapabilitySupport))
def test_only_confirmed_support_passes(runtime, capability, support) -> None:
    descriptor = ProviderCapabilities(runtime, {capability: support})
    required = frozenset({capability})
    if support is CapabilitySupport.SUPPORTED:
        descriptor.require(required)
    else:
        with pytest.raises(ProviderCapabilityError) as rejected:
            descriptor.require(required)
        assert rejected.value.runtime is runtime
        assert rejected.value.missing == (capability,)


def test_missing_declarations_are_unknown_and_rejection_is_deterministic() -> None:
    descriptor = ProviderCapabilities(
        JobRuntime.LOCAL,
        {
            ProviderCapability.PLAN: CapabilitySupport.SUPPORTED,
            ProviderCapability.TOOLS: CapabilitySupport.UNSUPPORTED,
        },
    )
    assert descriptor.support[ProviderCapability.FILES] is CapabilitySupport.UNKNOWN
    descriptor.require(frozenset())
    with pytest.raises(ProviderCapabilityError) as rejected:
        descriptor.require(frozenset(ProviderCapability))
    assert rejected.value.missing == tuple(
        c for c in ProviderCapability if c is not ProviderCapability.PLAN
    )


def test_descriptor_snapshots_input_and_cannot_be_mutated() -> None:
    original = {ProviderCapability.TOOLS: CapabilitySupport.UNSUPPORTED}
    descriptor = ProviderCapabilities(JobRuntime.CODEX, original)
    original[ProviderCapability.TOOLS] = CapabilitySupport.SUPPORTED
    with pytest.raises(ProviderCapabilityError):
        descriptor.require(frozenset({ProviderCapability.TOOLS}))
    with pytest.raises(TypeError):
        descriptor.support[ProviderCapability.TOOLS] = CapabilitySupport.SUPPORTED
    with pytest.raises(FrozenInstanceError):
        descriptor.runtime = JobRuntime.LOCAL


@pytest.mark.parametrize("runtime", [JobRuntime.AUTO, "codex", None])
def test_descriptor_requires_concrete_typed_runtime(runtime) -> None:
    with pytest.raises(ValueError):
        ProviderCapabilities(runtime, {})


@pytest.mark.parametrize(
    "support",
    [
        None,
        [],
        {"tools": CapabilitySupport.SUPPORTED},
        {ProviderCapability.TOOLS: True},
        {ProviderCapability.TOOLS: "supported"},
    ],
)
def test_invalid_declarations_are_rejected(support) -> None:
    with pytest.raises(TypeError):
        ProviderCapabilities(JobRuntime.LOCAL, support)


@pytest.mark.parametrize("required", [None, [], {ProviderCapability.PLAN}, frozenset({"plan"})])
def test_invalid_requirements_cannot_bypass_gate(required) -> None:
    with pytest.raises(TypeError):
        ProviderCapabilities(JobRuntime.LOCAL, {}).require(required)


def test_capability_gate_prevents_execution_for_inference_only_adapter() -> None:
    executed = []
    descriptor = ProviderCapabilities(
        JobRuntime.LOCAL,
        {
            ProviderCapability.PLAN: CapabilitySupport.SUPPORTED,
            ProviderCapability.TOOLS: CapabilitySupport.UNSUPPORTED,
        },
    )

    def dispatch(required):
        descriptor.require(required)
        executed.append("called")

    with pytest.raises(ProviderCapabilityError):
        dispatch(frozenset({ProviderCapability.PLAN, ProviderCapability.TOOLS}))
    assert executed == []
    dispatch(frozenset({ProviderCapability.PLAN}))
    assert executed == ["called"]
