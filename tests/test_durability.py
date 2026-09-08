from __future__ import annotations

import shutil
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from db import (
    ApprovalAlreadyExistsError,
    ApprovalRepository,
    AtomicTransitionService,
    BackupService,
    Database,
    EventRepository,
    JobAlreadyExistsError,
    JobRepository,
    ProjectRepository,
    RecoveryService,
)
from engine import (
    ApprovalCreate,
    ApprovalDecision,
    ApprovalResolution,
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    RecoveryAction,
    Sensitivity,
)

CRASH_PROGRAM = """
import os
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from db import Database, JobRepository, AtomicTransitionService, ApprovalRepository
from engine import (
    JobCreate, JobUpdate, JobState, JobRuntime, EventCreate,
    ApprovalCreate, ApprovalResolution, ApprovalDecision,
)

database = Database(Path(sys.argv[1]))
operation, boundary = sys.argv[2:4]
original_connect = sqlite3.connect
def connect(*args, **kwargs):
    connection = original_connect(*args, **kwargs)
    if boundary == 'before_commit':
        def trace(statement):
            if statement.strip().upper() == 'COMMIT':
                os._exit(73)
        connection.set_trace_callback(trace)
    return connection
sqlite3.connect = connect

if operation == 'create':
    JobRepository(database).create(JobCreate('new', 'project', 'Durable request', {}))
elif operation == 'transition':
    AtomicTransitionService(database).transition(
        JobUpdate('job', JobState.CLASSIFIED, JobRuntime.AUTO),
        EventCreate('job', 'classified', {'result': 'ready'}, 'transition-once'),
    )
elif operation == 'approval':
    ApprovalRepository(database).create(ApprovalCreate(
        'approval', 'job', {'action': 'review'}, datetime.now(UTC) + timedelta(hours=1),
    ))
elif operation == 'decision':
    ApprovalRepository(database).decide('approval', ApprovalResolution(
        ApprovalDecision.APPROVED, 'reviewer', 'local', {'action': 'review'},
    ))
else:
    raise ValueError('Unknown operation')
os._exit(74)
"""


def storage(tmp_path: Path, mode: str) -> Database:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(
        ProjectConfig(
            "project",
            "/workspace/sample",
            Sensitivity.PRIVATE,
            False,
            PermissionMode.SANDBOXED_WRITE,
        )
    )
    JobRepository(database).create(JobCreate("job", "project", "Durable request", {}))
    with sqlite3.connect(database.path) as connection:
        assert connection.execute(f"PRAGMA journal_mode={mode}").fetchone()[0] == mode.lower()
    return database


def pending() -> ApprovalCreate:
    return ApprovalCreate(
        "approval", "job", {"action": "review"}, datetime.now(UTC) + timedelta(hours=1)
    )


def integrity(database: Database) -> None:
    with sqlite3.connect(database.path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute(
            "SELECT COUNT(*) FROM approvals a LEFT JOIN jobs j ON a.job_id=j.id WHERE j.id IS NULL"
        ).fetchone() == (0,)


@pytest.mark.parametrize("mode", ["DELETE", "WAL"])
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
@pytest.mark.parametrize("operation", ["create", "transition", "approval", "decision"])
def test_process_crash_preserves_transaction_boundaries(
    tmp_path: Path,
    mode: str,
    boundary: str,
    operation: str,
) -> None:
    database = storage(tmp_path, mode)
    if operation == "decision":
        ApprovalRepository(database).create(pending())
    process = subprocess.run(
        [sys.executable, "-c", CRASH_PROGRAM, str(database.path), operation, boundary],
        capture_output=True,
        text=True,
        timeout=10,
    )
    committed = boundary == "after_commit"
    assert process.returncode == (74 if committed else 73), process.stderr
    restarted = Database(database.path)
    integrity(restarted)
    jobs = JobRepository(restarted)
    approvals = ApprovalRepository(restarted)
    if operation == "create":
        assert len(jobs.list()) == (2 if committed else 1)
        if committed:
            with pytest.raises(JobAlreadyExistsError):
                jobs.create(JobCreate("new", "project", "Durable request", {}))
        else:
            jobs.create(JobCreate("new", "project", "Durable request", {}))
        assert len(jobs.list()) == 2
    elif operation == "transition":
        assert jobs.get("job").state is (JobState.CLASSIFIED if committed else JobState.CREATED)
        assert len(EventRepository(restarted).list("job")) == int(committed)
        service = AtomicTransitionService(restarted)
        update = JobUpdate("job", JobState.CLASSIFIED, JobRuntime.AUTO)
        event = EventCreate("job", "classified", {"result": "ready"}, "transition-once")
        assert service.transition(update, event) == service.transition(update, event)
        assert len(EventRepository(restarted).list("job")) == 1
    elif operation == "approval":
        assert len(approvals.list()) == int(committed)
        if committed:
            with pytest.raises(ApprovalAlreadyExistsError):
                approvals.create(pending())
        else:
            approvals.create(pending())
        assert len(approvals.list()) == 1
    else:
        assert approvals.get("approval").decision is (
            ApprovalDecision.APPROVED if committed else None
        )
        resolution = ApprovalResolution(
            ApprovalDecision.APPROVED,
            "reviewer",
            "local",
            {"action": "review"},
        )
        assert approvals.decide("approval", resolution) == approvals.decide("approval", resolution)
        assert len(approvals.list()) == 1
    integrity(restarted)


@pytest.mark.parametrize("mode", ["DELETE", "WAL"])
def test_interrupted_recovery_and_backup_preserve_pending_approval(
    tmp_path: Path,
    mode: str,
) -> None:
    database = storage(tmp_path, mode)
    worktrees = tmp_path / "worktrees"
    active = worktrees / "job"
    active.mkdir(parents=True)
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            "UPDATE jobs SET state='waiting_approval', worktree_path=? WHERE id='job'",
            (str(active),),
        )
    expected = ApprovalRepository(database).create(pending())
    program = """
import os, sys
from pathlib import Path
from db import Database, RecoveryService
service = RecoveryService(Database(Path(sys.argv[1])), Path(sys.argv[2]))
original = service._classify
def interrupt(*args, **kwargs):
    original(*args, **kwargs)
    os._exit(75)
service._classify = interrupt
service.load()
"""
    process = subprocess.run(
        [sys.executable, "-c", program, str(database.path), str(worktrees)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert process.returncode == 75, process.stderr
    snapshot = BackupService(database).create(tmp_path / "backup.db")
    restored_path = tmp_path / "restored.db"
    shutil.copyfile(snapshot, restored_path)
    restored = Database(restored_path)
    recovery = RecoveryService(restored, worktrees)
    first = recovery.load()
    assert first == recovery.load()
    assert first[0].action is RecoveryAction.WAIT_FOR_APPROVAL
    assert first[0].approval == expected
    assert first[0].worktree == active.resolve()
    assert ApprovalRepository(database).get("approval") == expected
    assert len(ApprovalRepository(restored).list()) == 1
    integrity(restored)
