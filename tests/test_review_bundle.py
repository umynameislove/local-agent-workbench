from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    AtomicTransitionService,
    Database,
    EventRepository,
    JobRepository,
    ProjectRepository,
    ReviewBundleRepository,
    VerificationRepository,
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
from review_bundle import (
    ReviewBundleMissingError,
    ReviewBundleService,
    ReviewBundleStateError,
    ReviewBundleUnavailableError,
)
from secret_gate import PreReviewSecretGate


def git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def worktree_fixture(path: Path) -> tuple[Path, str]:
    path.mkdir()
    git(path, "init", "--quiet")
    (path / "example.txt").write_text("original\n", encoding="utf-8")
    git(path, "add", "example.txt")
    git(
        path,
        "-c",
        "user.name=Test User",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Initial fixture",
    )
    return path.resolve(strict=True), git(path, "rev-parse", "HEAD")


def project(root: Path) -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root=str(root),
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job(worktree: Path, base: str, *, request: str = "Review this safe change.") -> JobCreate:
    return JobCreate(
        id="job-001",
        project_id="alpha",
        request=request,
        request_snapshot={"repo_head": base, "policy_version": 1},
        state=JobState.VERIFYING,
        runtime=JobRuntime.CODEX,
        model="test-model",
        worktree_path=str(worktree),
    )


def initialized(
    tmp_path: Path,
    *,
    request: str = "Review this safe change.",
) -> tuple[Database, PreReviewSecretGate, ReviewBundleService, Path]:
    worktree, base = worktree_fixture(tmp_path / "worktree")
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project(worktree))
    jobs = JobRepository(database)
    jobs.create(job(worktree, base, request=request))
    events = EventRepository(database)
    verification = VerificationRepository(database)
    bundles = ReviewBundleRepository(database)
    gate = PreReviewSecretGate(jobs, events, AtomicTransitionService(database))
    service = ReviewBundleService(jobs, events, verification, bundles)
    return database, gate, service, worktree


def record_check(database: Database, outcome: VerificationOutcome) -> None:
    exit_code = 0 if outcome is VerificationOutcome.PASSED else 1
    VerificationRepository(database).record(
        VerificationCreate(
            job_id="job-001",
            command_args=("pytest", "tests/test_example.py"),
            outcome=outcome,
            exit_code=exit_code,
            duration_ms=12,
            output_digest="a" * 64,
            stdout_bytes=8,
            stderr_bytes=0,
        )
    )


def test_schema_ten_upgrade_adds_immutable_review_storage(tmp_path: Path) -> None:
    worktree, base = worktree_fixture(tmp_path / "worktree")
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:10])
    assert legacy.initialize() == 10
    ProjectRepository(legacy).create(project(worktree))
    JobRepository(legacy).create(job(worktree, base))

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION == 11
    assert JobRepository(upgraded).get("job-001").request_snapshot["repo_head"] == base
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'review_bundles%'"
            )
        }
    assert names == {
        "review_bundles",
        "review_bundles_prevent_update",
        "review_bundles_prevent_delete",
    }


@pytest.mark.anyio
async def test_bundle_freezes_diff_evidence_hash_and_risks_across_restart(
    tmp_path: Path,
) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    record_check(database, VerificationOutcome.PASSED)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    readiness = await gate.evaluate("job-001")

    created = await service.create("job-001")
    retry = await service.create("job-001")
    restarted = ReviewBundleService(
        JobRepository(Database(database.path)),
        EventRepository(Database(database.path)),
        VerificationRepository(Database(database.path)),
        ReviewBundleRepository(Database(database.path)),
    )
    stored = restarted.read("job-001")

    assert created == retry == stored
    bundle = created["bundle"]
    assert bundle["schema_version"] == 1
    assert bundle["summary"]["changed_files"] == 1
    assert bundle["summary"]["changes"]["modified"] == 1
    assert bundle["diff"]["files"][0]["patch"].endswith("+safe change\n")
    assert bundle["diff_digest"] == readiness.scan.digest
    assert bundle["scan_event_id"] == readiness.event_id
    assert bundle["runtime"] == "codex"
    assert bundle["model"] == "test-model"
    assert bundle["verification"][0]["command_args"] == ["pytest", "tests/test_example.py"]
    assert bundle["verification"][0]["output_digest"] == "a" * 64
    assert bundle["risks"] == []
    canonical = json.dumps(bundle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    assert created["bundle_hash"] == hashlib.sha256(canonical.encode()).hexdigest()
    with sqlite3.connect(database.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_bundles").fetchone() == (1,)


@pytest.mark.anyio
async def test_bundle_rejects_changes_before_and_after_snapshot(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    target = worktree / "example.txt"
    target.write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")
    target.write_text("different safe change\n", encoding="utf-8")

    with pytest.raises(ReviewBundleStateError, match="changed after the secret scan"):
        await service.create("job-001")
    with pytest.raises(ReviewBundleMissingError):
        service.read("job-001")

    target.write_text("safe change\n", encoding="utf-8")
    frozen = await service.create("job-001")
    target.write_text("another safe change\n", encoding="utf-8")
    with pytest.raises(ReviewBundleStateError):
        await service.create("job-001")
    assert service.read("job-001") == frozen
    assert len(ReviewBundleRepository(database).get("job-001").payload["diff"]["files"]) == 1


@pytest.mark.anyio
async def test_new_verification_evidence_cannot_rewrite_displayed_bundle(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")
    frozen = await service.create("job-001")
    record_check(database, VerificationOutcome.PASSED)

    with pytest.raises(ReviewBundleStateError, match="already frozen"):
        await service.create("job-001")

    assert service.read("job-001") == frozen


@pytest.mark.anyio
async def test_failed_verification_is_an_explicit_risk(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    record_check(database, VerificationOutcome.FAILED)
    (worktree / "example.txt").unlink()
    (worktree / "new.txt").write_text("safe new content\n", encoding="utf-8")
    await gate.evaluate("job-001")

    bundle = (await service.create("job-001"))["bundle"]

    assert bundle["summary"]["changes"]["deleted"] == 1
    assert bundle["verification"][0]["outcome"] == "failed"
    assert "At least one verification command did not pass." in bundle["risks"]


@pytest.mark.anyio
async def test_database_triggers_and_hash_check_protect_frozen_bundle(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")
    await service.create("job-001")
    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE review_bundles SET payload = '{}' WHERE job_id = 'job-001'")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM review_bundles WHERE job_id = 'job-001'")
        connection.execute("DROP TRIGGER review_bundles_prevent_update")
        connection.execute(
            "UPDATE review_bundles SET payload = json_set(payload, '$.model', 'tampered')"
        )
    with pytest.raises(ReviewBundleUnavailableError):
        service.read("job-001")


@pytest.mark.anyio
async def test_storage_failure_leaves_review_state_and_events_untouched(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_bundle_insert
            BEFORE INSERT ON review_bundles
            BEGIN
                SELECT RAISE(ABORT, 'synthetic storage failure');
            END
            """
        )

    with pytest.raises(ReviewBundleUnavailableError):
        await service.create("job-001")

    assert JobRepository(database).get("job-001").state is JobState.REVIEW_READY
    assert [event.event_type for event in EventRepository(database).list("job-001")] == [
        "job.review_ready"
    ]
    with sqlite3.connect(database.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_bundles").fetchone() == (0,)


@pytest.mark.anyio
async def test_bundle_creation_is_idempotent_across_services(tmp_path: Path) -> None:
    database, gate, service, worktree = initialized(tmp_path)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")
    second = ReviewBundleService(
        JobRepository(database),
        EventRepository(database),
        VerificationRepository(database),
        ReviewBundleRepository(database),
    )

    first, retry = await asyncio.gather(service.create("job-001"), second.create("job-001"))

    assert first == retry
    with sqlite3.connect(database.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_bundles").fetchone() == (1,)


@pytest.mark.anyio
async def test_task_and_command_credentials_are_not_copied_into_bundle(tmp_path: Path) -> None:
    token = "ghp_" + "x" * 36
    sample_value = "sample" + "-private-value"
    database, gate, service, worktree = initialized(tmp_path, request=f"Use {token} safely.")
    VerificationRepository(database).record(
        VerificationCreate(
            job_id="job-001",
            command_args=("check", "--password", sample_value, f"--token={token}"),
            outcome=VerificationOutcome.PASSED,
            exit_code=0,
            duration_ms=1,
            output_digest="b" * 64,
            stdout_bytes=0,
            stderr_bytes=0,
        )
    )
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    await gate.evaluate("job-001")

    bundle = (await service.create("job-001"))["bundle"]
    serialized = json.dumps(bundle)

    assert token not in serialized
    assert sample_value not in serialized
    assert bundle["summary"]["task"] == "[REDACTED]"
    assert bundle["verification"][0]["command_args"] == [
        "check",
        "--password",
        "[REDACTED]",
        "--token=[REDACTED]",
    ]


@pytest.mark.anyio
async def test_api_returns_frozen_bundle_without_caching_or_private_paths(tmp_path: Path) -> None:
    worktree, base = worktree_fixture(tmp_path / "worktree")
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        api.state.project_repository.create(project(worktree))
        api.state.job_repository.create(job(worktree, base))
        (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            missing = await client.get("/api/jobs/job-001/review-bundle")
            not_ready = await client.post("/api/jobs/job-001/review-bundle")
            await client.post("/api/jobs/job-001/review-readiness")
            created = await client.post("/api/jobs/job-001/review-bundle")
            read = await client.get("/api/jobs/job-001/review-bundle")

    assert missing.status_code == 404
    assert not_ready.status_code == 409
    assert created.status_code == read.status_code == 200
    assert created.json() == read.json()
    assert read.headers["Cache-Control"] == "no-store"
    assert str(worktree) not in read.text
    assert read.json()["bundle"]["risks"] == ["No verification command was recorded."]
