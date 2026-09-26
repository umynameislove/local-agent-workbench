from __future__ import annotations

import asyncio
import sqlite3
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    JobRepository,
    ProjectRepository,
    VerificationNotFoundError,
    VerificationRepository,
    VerificationRepositoryError,
    VerificationValidationError,
)
from engine import (
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
    VerificationCreate,
    VerificationOutcome,
)
from verification import (
    VerificationCommand,
    VerificationConflictError,
    VerificationRunner,
)
from verification import (
    VerificationNotFoundError as RunnerNotFoundError,
)


def project(root: Path) -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root=str(root),
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(worktree: Path, **overrides: object) -> JobCreate:
    values: dict[str, object] = {
        "id": "job-001",
        "project_id": "alpha",
        "request": "Run focused verification.",
        "request_snapshot": {"policy_version": 1},
        "state": JobState.VERIFYING,
        "runtime": JobRuntime.CODEX,
        "model": "gpt-codex",
        "worktree_path": str(worktree),
    }
    values.update(overrides)
    return JobCreate(**values)


def initialized_runner(
    tmp_path: Path,
) -> tuple[Database, JobRepository, VerificationRepository, VerificationRunner, Path]:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project(worktree))
    jobs = JobRepository(database)
    jobs.create(job(worktree))
    evidence = VerificationRepository(database)
    return database, jobs, evidence, VerificationRunner(jobs, evidence, environment={}), worktree


def test_version_nine_database_adds_verification_evidence_without_losing_jobs(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:9])
    assert legacy.initialize() == 9
    ProjectRepository(legacy).create(project(worktree))
    expected_job = JobRepository(legacy).create(job(worktree))

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION == 12
    assert JobRepository(upgraded).get(expected_job.id) == expected_job
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name LIKE 'verification_runs%'
            ORDER BY type, name
            """
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert objects == [
        ("index", "verification_runs_job_created_idx"),
        ("table", "verification_runs"),
        ("trigger", "verification_runs_prevent_delete"),
        ("trigger", "verification_runs_prevent_update"),
    ]


@pytest.mark.anyio
async def test_pass_and_failure_evidence_preserve_exact_arguments_and_digests(
    tmp_path: Path,
) -> None:
    database, _, repository, runner, _ = initialized_runner(tmp_path)
    literal = "; echo not-a-shell"
    passing = VerificationCommand(
        (
            sys.executable,
            "-c",
            "import sys; print(sys.argv[1]); print('note', file=sys.stderr)",
            literal,
        )
    )
    failing = VerificationCommand((sys.executable, "-c", "import sys; print('bad'); sys.exit(7)"))

    passed = await runner.run("job-001", passing)
    failed = await runner.run("job-001", failing)

    assert passed.command_args == passing.argv
    assert passed.outcome is VerificationOutcome.PASSED
    assert passed.exit_code == 0
    assert passed.duration_ms >= 0
    assert passed.stdout_bytes == len(f"{literal}\n".encode())
    assert passed.stderr_bytes == len(b"note\n")
    assert passed.output_digest == runner.output_digest(
        f"{literal}\n".encode(),
        b"note\n",
    )
    assert failed.outcome is VerificationOutcome.FAILED
    assert failed.exit_code == 7
    assert failed.output_digest == runner.output_digest(b"bad\n", b"")
    assert repository.list(job_id="job-001") == (passed, failed)
    assert VerificationRepository(Database(database.path)).get(passed.id) == passed
    database_bytes = database.path.read_bytes()
    assert literal.encode() in database_bytes
    assert b"note\n" not in database_bytes
    assert b"bad\n" not in database_bytes


@pytest.mark.anyio
async def test_timeout_persists_partial_output_digest_and_no_exit_code(tmp_path: Path) -> None:
    _, _, repository, runner, _ = initialized_runner(tmp_path)
    command = VerificationCommand(
        (
            sys.executable,
            "-c",
            "import sys,time; print('before-timeout', flush=True); time.sleep(30)",
        ),
        timeout_seconds=0.1,
    )

    stored = await runner.run("job-001", command)

    assert stored.outcome is VerificationOutcome.TIMED_OUT
    assert stored.exit_code is None
    assert stored.duration_ms >= 100
    assert stored.stdout_bytes == len(b"before-timeout\n")
    assert stored.stderr_bytes == 0
    assert stored.output_digest == runner.output_digest(b"before-timeout\n", b"")
    assert repository.get(stored.id) == stored


@pytest.mark.anyio
async def test_cancelled_verification_does_not_record_incomplete_evidence(tmp_path: Path) -> None:
    _, _, repository, runner, _ = initialized_runner(tmp_path)
    task = asyncio.create_task(
        runner.run(
            "job-001",
            VerificationCommand(
                (sys.executable, "-c", "import time; time.sleep(30)"),
                timeout_seconds=60,
            ),
        )
    )
    await asyncio.sleep(0.1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert repository.list(job_id="job-001") == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state, worktree_value",
    [
        (JobState.RUNNING, "valid"),
        (JobState.VERIFYING, None),
        (JobState.VERIFYING, "missing"),
    ],
)
async def test_runner_rejects_jobs_without_a_ready_verified_worktree(
    tmp_path: Path,
    state: JobState,
    worktree_value: str | None,
) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project(worktree))
    jobs = JobRepository(database)
    selected = worktree if worktree_value == "valid" else tmp_path / "missing"
    jobs.create(
        job(
            worktree,
            state=state,
            worktree_path=None if worktree_value is None else str(selected),
        )
    )
    runner = VerificationRunner(jobs, VerificationRepository(database), environment={})

    with pytest.raises(VerificationConflictError):
        await runner.run("job-001", VerificationCommand((sys.executable, "-c", "pass")))


@pytest.mark.anyio
async def test_unknown_job_is_sanitized(tmp_path: Path) -> None:
    _, _, _, runner, _ = initialized_runner(tmp_path)

    with pytest.raises(RunnerNotFoundError, match="does not exist") as error:
        await runner.run("missing-job", VerificationCommand((sys.executable, "-c", "pass")))

    assert str(tmp_path) not in str(error.value)


def test_repository_rejects_inconsistent_or_oversized_evidence(tmp_path: Path) -> None:
    _, _, repository, _, _ = initialized_runner(tmp_path)
    valid = VerificationCreate(
        job_id="job-001",
        command_args=("pytest", "tests/test_unit.py"),
        outcome=VerificationOutcome.PASSED,
        exit_code=0,
        duration_ms=12,
        output_digest="a" * 64,
        stdout_bytes=10,
        stderr_bytes=0,
    )

    for invalid in (
        replace(valid, command_args=()),
        replace(valid, command_args=("",)),
        replace(valid, command_args=("pytest", "bad\x00arg")),
        replace(valid, outcome=VerificationOutcome.FAILED, exit_code=0),
        replace(valid, outcome=VerificationOutcome.TIMED_OUT, exit_code=1),
        replace(valid, duration_ms=-1),
        replace(valid, stdout_bytes=True),
        replace(valid, output_digest="A" * 64),
    ):
        with pytest.raises(VerificationValidationError):
            repository.record(invalid)

    with pytest.raises(VerificationValidationError, match="does not exist"):
        repository.record(replace(valid, job_id="missing-job"))
    with pytest.raises(VerificationNotFoundError):
        repository.get(999)


def test_verification_evidence_is_immutable_and_corruption_fails_closed(tmp_path: Path) -> None:
    database, _, repository, _, _ = initialized_runner(tmp_path)
    stored = repository.record(
        VerificationCreate(
            job_id="job-001",
            command_args=("pytest",),
            outcome=VerificationOutcome.PASSED,
            exit_code=0,
            duration_ms=1,
            output_digest="a" * 64,
            stdout_bytes=0,
            stderr_bytes=0,
        )
    )
    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE verification_runs SET duration_ms = 2 WHERE id = ?",
                (stored.id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM verification_runs WHERE id = ?", (stored.id,))
        connection.execute("DROP TRIGGER verification_runs_prevent_update")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE verification_runs SET output_digest = 'invalid' WHERE id = ?",
            (stored.id,),
        )

    with pytest.raises(VerificationRepositoryError, match="Stored verification"):
        repository.get(stored.id)


@pytest.mark.parametrize(
    "invalid",
    [
        (),
        ("",),
        ("pytest\x00",),
        tuple("x" for _ in range(257)),
    ],
)
def test_command_rejects_invalid_arguments(invalid: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="arguments|many"):
        VerificationCommand(invalid)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), 3_601, True])
def test_command_rejects_invalid_timeout(timeout: object) -> None:
    with pytest.raises(ValueError, match="timeout"):
        VerificationCommand(("pytest",), timeout_seconds=timeout)


def test_digest_framing_distinguishes_stream_boundaries() -> None:
    assert VerificationRunner.output_digest(b"ab", b"c") != VerificationRunner.output_digest(
        b"a", b"bc"
    )
