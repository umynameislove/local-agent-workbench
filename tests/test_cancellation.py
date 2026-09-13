from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cancellation import (
    CancellationConflictError,
    CancellationPersistenceError,
    CancellationProviderError,
    CancellationService,
)
from db import AtomicTransitionService, Database, EventRepository, JobRepository, ProjectRepository
from engine import (
    AdapterError,
    AdapterSession,
    AdapterStart,
    CapabilitySupport,
    FakeProvider,
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    ProviderCapabilities,
    ProviderCapability,
    RuntimeEvent,
    RuntimeEventKind,
    Sensitivity,
)


class CancelAdapter:
    def __init__(
        self,
        *,
        runtime: JobRuntime = JobRuntime.LOCAL,
        fail: bool = False,
    ) -> None:
        self.capabilities = ProviderCapabilities(
            runtime,
            {ProviderCapability.PLAN: CapabilitySupport.SUPPORTED},
        )
        self.fail = fail
        self.cancelled: list[AdapterSession] = []

    async def cancel(self, session: AdapterSession) -> None:
        self.cancelled.append(session)
        if self.fail:
            raise AdapterError("private provider failure")


def initialized_service(
    tmp_path: Path,
    *,
    state: JobState = JobState.RUNNING,
):
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(
        ProjectConfig(
            id="alpha",
            root="/workspace/alpha",
            sensitivity=Sensitivity.PRIVATE,
            cloud_allowed=False,
            permission_mode=PermissionMode.SANDBOXED_WRITE,
        )
    )
    worktree = tmp_path / "worktrees" / "job-001"
    worktree.mkdir(parents=True)
    (worktree / "proposal.txt").write_text("retain me")
    jobs = JobRepository(database)
    jobs.create(
        JobCreate(
            id="job-001",
            project_id="alpha",
            request="Cancel provider work safely.",
            request_snapshot={"policy_version": 1},
            state=state,
            runtime=JobRuntime.LOCAL,
            model="qwen-local",
            worktree_path=str(worktree),
        )
    )
    events = EventRepository(database)
    service = CancellationService(jobs, AtomicTransitionService(database))
    session = AdapterSession("job-001", JobRuntime.LOCAL, "opaque-session")
    return database, service, jobs, events, session, worktree


def cancelled_event(session: AdapterSession) -> RuntimeEvent:
    return RuntimeEvent(
        job_id=session.job_id,
        sequence=1,
        runtime=session.runtime,
        timestamp=datetime(2026, 9, 13, tzinfo=UTC),
        kind=RuntimeEventKind.COMPLETION,
        payload={"status": "cancelled"},
    )


@pytest.mark.anyio
async def test_confirmed_cancel_records_one_terminal_event_and_retains_worktree(
    tmp_path: Path,
) -> None:
    _, service, jobs, events, session, worktree = initialized_service(tmp_path)
    adapter = CancelAdapter()
    original = jobs.get("job-001")

    await service.request(adapter, session)

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()
    result = service.confirm_cancelled(session, cancelled_event(session))

    assert adapter.cancelled == [session]
    assert result.job.state is JobState.CANCELLED
    assert result.job.runtime is JobRuntime.LOCAL
    assert result.job.model == "qwen-local"
    assert result.job.worktree_path == str(worktree)
    assert result.event.event_type == "job.cancelled"
    assert result.event.payload == {"state": "cancelled"}
    assert result.event.idempotency_key == "job-cancel"
    assert jobs.get("job-001") == result.job
    assert events.list("job-001") == (result.event,)
    assert (worktree / "proposal.txt").read_text() == "retain me"


@pytest.mark.anyio
async def test_fake_provider_cancel_reaches_runtime_and_durable_ledger(tmp_path: Path) -> None:
    _, service, jobs, events, _, worktree = initialized_service(tmp_path)
    provider = FakeProvider()
    session = await provider.start(AdapterStart("job-001", "Demo", worktree))

    await service.request(provider, session)
    result = service.confirm_cancelled(session, provider.events(session)[-1])

    assert provider.events(session)[-1].kind is RuntimeEventKind.COMPLETION
    assert provider.events(session)[-1].payload == {"status": "cancelled"}
    assert jobs.get("job-001").state is JobState.CANCELLED
    assert events.list("job-001") == (result.event,)
    assert worktree.exists()


@pytest.mark.anyio
async def test_retry_returns_the_committed_result_without_recontacting_provider(
    tmp_path: Path,
) -> None:
    _, service, _, events, session, _ = initialized_service(tmp_path)
    adapter = CancelAdapter()

    await service.request(adapter, session)
    event = cancelled_event(session)
    first = service.confirm_cancelled(session, event)
    retry = service.confirm_cancelled(session, event)
    await service.request(adapter, session)

    assert retry == first
    assert adapter.cancelled == [session]
    assert events.list("job-001") == (first.event,)


@pytest.mark.anyio
async def test_concurrent_confirmation_records_one_event(tmp_path: Path) -> None:
    _, service, jobs, events, session, _ = initialized_service(tmp_path)
    adapter = CancelAdapter()
    await service.request(adapter, session)
    event = cancelled_event(session)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.map(
            lambda _: service.confirm_cancelled(session, event),
            range(2),
        )

    assert first == second
    assert adapter.cancelled == [session]
    assert jobs.get("job-001").state is JobState.CANCELLED
    assert events.list("job-001") == (first.event,)


@pytest.mark.anyio
async def test_provider_failure_leaves_durable_state_unchanged(tmp_path: Path) -> None:
    _, service, jobs, events, session, worktree = initialized_service(tmp_path)
    original = jobs.get("job-001")
    adapter = CancelAdapter(fail=True)

    with pytest.raises(CancellationProviderError) as failure:
        await service.request(adapter, session)

    assert "private provider failure" not in str(failure.value)
    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()
    assert worktree.exists()


@pytest.mark.anyio
async def test_persistence_failure_rolls_back_confirmed_terminal_state(
    tmp_path: Path,
) -> None:
    database, service, jobs, events, session, worktree = initialized_service(tmp_path)
    original = jobs.get("job-001")
    adapter = CancelAdapter()
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_cancel_event
            BEFORE INSERT ON events
            WHEN NEW.event_type = 'job.cancelled'
            BEGIN
                SELECT RAISE(ABORT, 'simulated cancellation persistence failure');
            END
            """
        )

    await service.request(adapter, session)
    with pytest.raises(CancellationPersistenceError, match="recorded durably"):
        service.confirm_cancelled(session, cancelled_event(session))

    assert adapter.cancelled == [session]
    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()
    assert worktree.exists()


@pytest.mark.anyio
async def test_session_runtime_mismatch_has_no_side_effect(tmp_path: Path) -> None:
    _, service, jobs, events, _, _ = initialized_service(tmp_path)
    original = jobs.get("job-001")
    adapter = CancelAdapter(runtime=JobRuntime.CODEX)
    session = AdapterSession("job-001", JobRuntime.CODEX, "wrong-runtime")

    with pytest.raises(CancellationConflictError, match="does not own"):
        await service.request(adapter, session)

    assert adapter.cancelled == []
    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


@pytest.mark.anyio
async def test_adapter_runtime_mismatch_has_no_side_effect(tmp_path: Path) -> None:
    _, service, jobs, events, session, _ = initialized_service(tmp_path)
    original = jobs.get("job-001")
    adapter = CancelAdapter(runtime=JobRuntime.CODEX)

    with pytest.raises(CancellationConflictError, match="adapter and session"):
        await service.request(adapter, session)

    assert adapter.cancelled == []
    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state",
    [JobState.COMPLETED, JobState.REJECTED, JobState.FAILED, JobState.BLOCKED],
)
async def test_terminal_job_is_not_sent_to_provider(tmp_path: Path, state: JobState) -> None:
    _, service, jobs, events, session, worktree = initialized_service(tmp_path, state=state)
    adapter = CancelAdapter()

    with pytest.raises(CancellationConflictError, match="Terminal job"):
        await service.request(adapter, session)

    assert adapter.cancelled == []
    assert jobs.get("job-001").state is state
    assert events.list("job-001") == ()
    assert worktree.exists()


def test_cancelled_state_without_event_fails_closed(tmp_path: Path) -> None:
    _, service, jobs, events, session, worktree = initialized_service(
        tmp_path,
        state=JobState.CANCELLED,
    )

    with pytest.raises(CancellationConflictError, match="durable job state"):
        service.confirm_cancelled(session, cancelled_event(session))

    assert jobs.get("job-001").state is JobState.CANCELLED
    assert events.list("job-001") == ()
    assert worktree.exists()


@pytest.mark.parametrize("dependency", [None, object()])
def test_service_rejects_invalid_repository_dependencies(dependency) -> None:
    with pytest.raises(TypeError):
        CancellationService(dependency, dependency)


@pytest.mark.anyio
async def test_cancel_rejects_invalid_adapter_and_session(tmp_path: Path) -> None:
    _, service, jobs, events, session, _ = initialized_service(tmp_path)
    original = jobs.get("job-001")

    with pytest.raises(TypeError, match="ProviderCapabilities"):
        await service.request(object(), session)
    with pytest.raises(TypeError, match="AdapterSession"):
        await service.request(CancelAdapter(), object())

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()


def test_confirmation_rejects_invalid_or_nonterminal_events(tmp_path: Path) -> None:
    _, service, jobs, events, session, _ = initialized_service(tmp_path)
    original = jobs.get("job-001")
    completed = RuntimeEvent(
        job_id=session.job_id,
        sequence=1,
        runtime=session.runtime,
        timestamp=datetime(2026, 9, 13, tzinfo=UTC),
        kind=RuntimeEventKind.COMPLETION,
        payload={"status": "completed"},
    )
    wrong_job = AdapterSession("other-job", JobRuntime.LOCAL, "other-session")

    with pytest.raises(TypeError, match="AdapterSession"):
        service.confirm_cancelled(object(), cancelled_event(session))
    with pytest.raises(TypeError, match="RuntimeEvent"):
        service.confirm_cancelled(session, object())
    with pytest.raises(CancellationConflictError, match="do not match"):
        service.confirm_cancelled(wrong_job, cancelled_event(session))
    with pytest.raises(CancellationConflictError, match="does not confirm"):
        service.confirm_cancelled(session, completed)

    assert jobs.get("job-001") == original
    assert events.list("job-001") == ()
