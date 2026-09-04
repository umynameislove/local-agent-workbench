from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app import create_app
from db import (
    ApprovalRecord,
    ApprovalRepository,
    Database,
    JobRepository,
    ProjectRepository,
    RecoveryService,
    RecoveryServiceError,
)
from engine import (
    TERMINAL_JOB_STATES,
    ApprovalCreate,
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    RecoveryAction,
    RecoveryIssue,
    Sensitivity,
)

NOW = datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)
EXPECTED_ACTIONS = {
    JobState.CREATED: RecoveryAction.SAFE_RESUME,
    JobState.CLASSIFIED: RecoveryAction.SAFE_RESUME,
    JobState.PLANNING: RecoveryAction.SAFE_RESUME,
    JobState.QUEUED: RecoveryAction.SAFE_RESUME,
    JobState.RUNNING: RecoveryAction.RECONCILE_IN_FLIGHT,
    JobState.WAITING_INPUT: RecoveryAction.WAIT_FOR_INPUT,
    JobState.WAITING_APPROVAL: RecoveryAction.WAIT_FOR_APPROVAL,
    JobState.VERIFYING: RecoveryAction.SAFE_RESUME,
    JobState.REVIEW_READY: RecoveryAction.READY_FOR_REVIEW,
    JobState.APPROVED: RecoveryAction.READY_TO_APPLY,
    JobState.APPLYING: RecoveryAction.RECONCILE_IN_FLIGHT,
}


def project() -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root="/workspace/alpha",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(
    state: JobState,
    *,
    job_id: str = "job-001",
    worktree_path: str | None = None,
) -> JobCreate:
    return JobCreate(
        id=job_id,
        project_id="alpha",
        request="Recover durable work after the local process restarts.",
        request_snapshot={"policy_version": 1},
        state=state,
        runtime=JobRuntime.CODEX,
        model="gpt-5.6-codex",
        worktree_path=worktree_path,
    )


def approval(
    *,
    approval_id: str = "approval-001",
    expires_at: datetime = NOW + timedelta(hours=1),
) -> ApprovalCreate:
    return ApprovalCreate(
        id=approval_id,
        job_id="job-001",
        payload={"action": "apply_patch", "diff_hash": "abc123"},
        expires_at=expires_at,
    )


def initialized_storage(tmp_path: Path) -> tuple[Database, Path]:
    runtime = tmp_path / "runtime"
    worktrees = runtime / "worktrees"
    worktrees.mkdir(parents=True)
    database = Database(runtime / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    return database, worktrees


def add_pending_approval(
    database: Database,
    value: ApprovalCreate | None = None,
) -> ApprovalRecord:
    return ApprovalRepository(database, clock=lambda: NOW).create(value or approval())


@pytest.mark.parametrize("state", tuple(EXPECTED_ACTIONS))
def test_every_nonterminal_state_has_an_explicit_recovery_action(
    tmp_path: Path,
    state: JobState,
) -> None:
    database, worktrees = initialized_storage(tmp_path)
    active_worktree = worktrees / "job-001"
    active_worktree.mkdir()
    JobRepository(database).create(job(state, worktree_path=str(active_worktree)))
    if state is JobState.WAITING_APPROVAL:
        expected_approval = add_pending_approval(database)
    else:
        expected_approval = None

    recovered = RecoveryService(database, worktrees, clock=lambda: NOW).load()

    assert set(EXPECTED_ACTIONS) == set(JobState) - TERMINAL_JOB_STATES
    assert len(recovered) == 1
    assert recovered[0].action is EXPECTED_ACTIONS[state]
    assert recovered[0].worktree == active_worktree.resolve()
    assert recovered[0].approval == expected_approval
    assert recovered[0].issue is None


def test_terminal_jobs_are_not_recovered(tmp_path: Path) -> None:
    database, worktrees = initialized_storage(tmp_path)
    jobs = JobRepository(database)
    for state in TERMINAL_JOB_STATES:
        jobs.create(job(state, job_id=f"job-{state.value}"))

    assert RecoveryService(database, worktrees, clock=lambda: NOW).load() == ()


@pytest.mark.parametrize(
    "state",
    [
        JobState.CREATED,
        JobState.CLASSIFIED,
        JobState.PLANNING,
        JobState.QUEUED,
        JobState.WAITING_INPUT,
    ],
)
def test_preparation_and_input_states_do_not_invent_a_worktree(
    tmp_path: Path,
    state: JobState,
) -> None:
    database, worktrees = initialized_storage(tmp_path)
    JobRepository(database).create(job(state))

    recovered = RecoveryService(database, worktrees, clock=lambda: NOW).load()[0]

    assert recovered.action is EXPECTED_ACTIONS[state]
    assert recovered.worktree is None
    assert recovered.issue is None


def test_waiting_approval_restores_the_same_approval_and_worktree_after_restart(
    tmp_path: Path,
) -> None:
    database, worktrees = initialized_storage(tmp_path)
    active_worktree = worktrees / "job-001"
    active_worktree.mkdir()
    expected_job = JobRepository(database).create(
        job(JobState.WAITING_APPROVAL, worktree_path=str(active_worktree))
    )
    expected_approval = add_pending_approval(database)

    restarted = Database(database.path)
    restarted.initialize()
    first = RecoveryService(restarted, worktrees, clock=lambda: NOW).load()
    second = RecoveryService(restarted, worktrees, clock=lambda: NOW).load()

    assert first == second
    assert first[0].job == expected_job
    assert first[0].approval == expected_approval
    assert first[0].worktree == active_worktree.resolve()
    assert JobRepository(restarted).get(expected_job.id) == expected_job
    assert ApprovalRepository(restarted).get(expected_approval.id) == expected_approval


def test_new_python_process_recovers_waiting_approval(tmp_path: Path) -> None:
    database, worktrees = initialized_storage(tmp_path)
    active_worktree = worktrees / "job-001"
    active_worktree.mkdir()
    JobRepository(database).create(
        job(JobState.WAITING_APPROVAL, worktree_path=str(active_worktree))
    )
    add_pending_approval(database)
    program = """
import json
import sys
from pathlib import Path
from db import Database, RecoveryService

item = RecoveryService(Database(Path(sys.argv[1])), Path(sys.argv[2])).load()[0]
print(json.dumps({
    "action": item.action.value,
    "approval": item.approval.id if item.approval else None,
    "job": item.job.id,
    "worktree": str(item.worktree),
}))
"""

    completed = subprocess.run(
        [sys.executable, "-c", program, str(database.path), str(worktrees)],
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {
        "action": "wait_for_approval",
        "approval": "approval-001",
        "job": "job-001",
        "worktree": str(active_worktree.resolve()),
    }


@pytest.mark.anyio
async def test_application_restart_loads_recovery_state(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    environment = {"AGENT_WORKBENCH_HOME": str(runtime)}
    first_app = create_app(environment)
    async with first_app.router.lifespan_context(first_app):
        first_app.state.project_repository.create(project())
        active_worktree = runtime / "worktrees" / "job-001"
        active_worktree.mkdir()
        first_app.state.job_repository.create(
            job(JobState.WAITING_APPROVAL, worktree_path=str(active_worktree))
        )
        expected_approval = first_app.state.approval_repository.create(approval())

    restarted_app = create_app(environment)
    async with restarted_app.router.lifespan_context(restarted_app):
        recovered = restarted_app.state.recovery_items

    assert len(recovered) == 1
    assert recovered[0].action is RecoveryAction.WAIT_FOR_APPROVAL
    assert recovered[0].approval == expected_approval
    assert recovered[0].worktree == active_worktree.resolve()


@pytest.mark.parametrize(
    ("setup", "issue"),
    [
        ("missing", RecoveryIssue.APPROVAL_REQUIRED),
        ("multiple", RecoveryIssue.MULTIPLE_PENDING_APPROVALS),
        ("expired", RecoveryIssue.APPROVAL_EXPIRED),
    ],
)
def test_invalid_waiting_approval_requires_attention(
    tmp_path: Path,
    setup: str,
    issue: RecoveryIssue,
) -> None:
    database, worktrees = initialized_storage(tmp_path)
    active_worktree = worktrees / "job-001"
    active_worktree.mkdir()
    JobRepository(database).create(
        job(JobState.WAITING_APPROVAL, worktree_path=str(active_worktree))
    )
    if setup in {"multiple", "expired"}:
        add_pending_approval(database, approval(expires_at=NOW + timedelta(seconds=1)))
    if setup == "multiple":
        add_pending_approval(database, approval(approval_id="approval-002"))

    def clock() -> datetime:
        return NOW + timedelta(seconds=2) if setup == "expired" else NOW

    recovered = RecoveryService(database, worktrees, clock=clock).load()[0]

    assert recovered.action is RecoveryAction.NEEDS_ATTENTION
    assert recovered.issue is issue
    assert recovered.worktree is None


def test_pending_approval_on_other_state_requires_attention(tmp_path: Path) -> None:
    database, worktrees = initialized_storage(tmp_path)
    active_worktree = worktrees / "job-001"
    active_worktree.mkdir()
    JobRepository(database).create(job(JobState.RUNNING, worktree_path=str(active_worktree)))
    add_pending_approval(database)

    recovered = RecoveryService(database, worktrees, clock=lambda: NOW).load()[0]

    assert recovered.action is RecoveryAction.NEEDS_ATTENTION
    assert recovered.issue is RecoveryIssue.UNEXPECTED_PENDING_APPROVAL


@pytest.mark.parametrize(
    ("state", "path_kind", "issue"),
    [
        (JobState.RUNNING, "missing", RecoveryIssue.WORKTREE_REQUIRED),
        (JobState.RUNNING, "unavailable", RecoveryIssue.WORKTREE_UNAVAILABLE),
        (JobState.RUNNING, "file", RecoveryIssue.WORKTREE_UNAVAILABLE),
        (JobState.RUNNING, "relative", RecoveryIssue.WORKTREE_OUTSIDE_RUNTIME),
        (JobState.RUNNING, "outside", RecoveryIssue.WORKTREE_OUTSIDE_RUNTIME),
        (JobState.RUNNING, "symlink_escape", RecoveryIssue.WORKTREE_OUTSIDE_RUNTIME),
    ],
)
def test_unsafe_worktree_requires_attention(
    tmp_path: Path,
    state: JobState,
    path_kind: str,
    issue: RecoveryIssue,
) -> None:
    database, worktrees = initialized_storage(tmp_path)
    if path_kind == "missing":
        stored_path = None
    elif path_kind == "unavailable":
        stored_path = str(worktrees / "missing")
    elif path_kind == "file":
        file_path = worktrees / "not-a-worktree"
        file_path.touch()
        stored_path = str(file_path)
    elif path_kind == "relative":
        stored_path = "relative/worktree"
    else:
        outside = tmp_path / "outside"
        outside.mkdir()
        if path_kind == "symlink_escape":
            symlink = worktrees / "escaped-worktree"
            symlink.symlink_to(outside, target_is_directory=True)
            stored_path = str(symlink)
        else:
            stored_path = str(outside)
    JobRepository(database).create(job(state, worktree_path=stored_path))

    recovered = RecoveryService(database, worktrees, clock=lambda: NOW).load()[0]

    assert recovered.action is RecoveryAction.NEEDS_ATTENTION
    assert recovered.issue is issue
    assert recovered.worktree is None


def test_corrupt_storage_and_invalid_runtime_inputs_fail_safely(tmp_path: Path) -> None:
    database, worktrees = initialized_storage(tmp_path)
    JobRepository(database).create(job(JobState.CREATED))
    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE jobs SET state = 'unknown' WHERE id = 'job-001'")

    with pytest.raises(RecoveryServiceError) as corrupt:
        RecoveryService(database, worktrees, clock=lambda: NOW).load()
    with pytest.raises(RecoveryServiceError, match="worktree storage"):
        RecoveryService(database, tmp_path / "missing", clock=lambda: NOW).load()
    with pytest.raises(RecoveryServiceError, match="clock"):
        RecoveryService(database, worktrees, clock=lambda: datetime(2030, 1, 2)).load()

    assert str(tmp_path) not in str(corrupt.value)
