import json
from pathlib import Path

import pytest

from engine import (
    AdapterError,
    AdapterHealth,
    AdapterSession,
    AdapterStart,
    AdapterUnsupportedError,
    FakeProvider,
    JobRuntime,
    ProviderAdapter,
    ProviderCapability,
    ProviderCapabilityError,
    RuntimeEvent,
)


@pytest.mark.anyio
async def test_golden_events_are_deterministic_and_have_no_file_effects(tmp_path: Path):
    expected = [
        ("plan", {"text": "Demo plan: inspect, propose, verify, request review."}),
        ("text", {"text": "Demo write proposal: add a greeting. No files changed."}),
        ("text", {"text": "Demo verification: simulated pass. No tests executed."}),
        (
            "question",
            {
                "question_id": "demo-review",
                "text": "Demo review: send approve or reject. No real changes will be applied.",
            },
        ),
    ]
    runs = []
    for _ in range(2):
        provider = FakeProvider()
        adapter: ProviderAdapter = provider
        assert await adapter.health() is AdapterHealth.READY
        session = await adapter.start(AdapterStart("job", "Demo", tmp_path))
        events = [event.to_dict() for event in provider.events(session)]
        assert events == [
            {
                "job_id": "job",
                "sequence": index,
                "runtime": "local",
                "timestamp": "2000-01-01T00:00:00Z",
                "kind": kind,
                "payload": payload,
            }
            for index, (kind, payload) in enumerate(expected, 1)
        ]
        assert [RuntimeEvent.from_dict(item) for item in json.loads(json.dumps(events))] == list(
            provider.events(session)
        )
        runs.append(events)
    assert runs[0] == runs[1]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
@pytest.mark.parametrize("answer,status", [("approve", "completed"), ("reject", "cancelled")])
async def test_demo_response_finishes_once(tmp_path: Path, answer, status):
    provider = FakeProvider()
    session = await provider.start(AdapterStart("job", "Demo", tmp_path))
    before = provider.events(session)
    await provider.send(session, answer)
    await provider.cancel(session)
    with pytest.raises(AdapterError):
        await provider.send(session, answer)
    assert len(before) == 4
    assert len(provider.events(session)) == 5
    assert provider.events(session)[-1].payload == {"status": status}
    assert provider.events(session)[-1].sequence == 5


@pytest.mark.anyio
async def test_cancel_repeat_and_job_isolation(tmp_path: Path):
    provider = FakeProvider()
    first = await provider.start(AdapterStart("first", "Demo", tmp_path))
    second = await provider.start(AdapterStart("second", "Demo", tmp_path))
    await provider.cancel(first)
    await provider.cancel(first)
    assert len(provider.events(first)) == 5
    assert len(provider.events(second)) == 4
    with pytest.raises(AdapterError):
        await provider.start(AdapterStart("second", "Demo", tmp_path))
    with pytest.raises(AdapterUnsupportedError):
        await provider.resume(second)
    assert len(provider.events(second)) == 4


@pytest.mark.anyio
async def test_rejected_requirements_and_inputs_have_no_event_effect(tmp_path: Path):
    provider = FakeProvider()
    with pytest.raises(ProviderCapabilityError):
        await provider.start(
            AdapterStart("job", "Demo", tmp_path, frozenset({ProviderCapability.TOOLS}))
        )
    session = await provider.start(AdapterStart("job", "Demo", tmp_path))
    before = provider.events(session)
    with pytest.raises(AdapterError):
        await provider.send(session, "unexpected")
    assert provider.events(session) == before
    wrong = AdapterSession("job", JobRuntime.CODEX, session.session_id)
    for method in (provider.cancel, provider.resume):
        with pytest.raises(AdapterError):
            await method(wrong)
    with pytest.raises(AdapterError):
        provider.events(wrong)
    assert provider.events(session) == before
