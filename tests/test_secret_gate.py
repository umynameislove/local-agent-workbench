from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest

from app import create_app
from db import AtomicTransitionService, Database, EventRepository, JobRepository, ProjectRepository
from diff_types import DiffContentKind, DiffEntry, DiffStatus, UnifiedDiff
from engine import (
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
)
from secret_gate import (
    PreReviewSecretGate,
    SecretGateBlockedError,
    SecretGateConflictError,
    SecretGateNotFoundError,
    SecretGateUnavailableError,
    SecretScanner,
)


def git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def initialize_repository(path: Path, content: str = "initial\n") -> tuple[Path, str]:
    path.mkdir()
    git(path, "init", "--quiet")
    (path / "example.txt").write_text(content, encoding="utf-8")
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


def job(worktree: Path, base: str, **overrides: object) -> JobCreate:
    values: dict[str, object] = {
        "id": "job-001",
        "project_id": "alpha",
        "request": "Prepare a safe review.",
        "request_snapshot": {"repo_head": base},
        "state": JobState.VERIFYING,
        "runtime": JobRuntime.CODEX,
        "model": "gpt-codex",
        "worktree_path": str(worktree),
    }
    values.update(overrides)
    return JobCreate(**values)


def initialized_gate(
    tmp_path: Path,
    *,
    committed_content: str = "initial\n",
) -> tuple[Database, PreReviewSecretGate, JobRepository, EventRepository, Path]:
    worktree, base = initialize_repository(tmp_path / "worktree", committed_content)
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project(worktree))
    jobs = JobRepository(database)
    jobs.create(job(worktree, base))
    events = EventRepository(database)
    gate = PreReviewSecretGate(jobs, events, AtomicTransitionService(database))
    return database, gate, jobs, events, worktree


def review_with(content: str, *, kind: DiffContentKind = DiffContentKind.TEXT) -> UnifiedDiff:
    patch = None
    summary = "Content omitted."
    if kind is DiffContentKind.TEXT:
        patch = f"diff --git a/file.txt b/file.txt\n@@ -0,0 +1 @@\n+{content}\n"
        summary = None
    return UnifiedDiff(
        base_commit="a" * 40,
        files=(
            DiffEntry(
                status=DiffStatus.ADDED,
                path="file.txt",
                previous_path=None,
                content_kind=kind,
                old_size=None,
                new_size=len(content.encode()),
                patch=patch,
                summary=summary,
            ),
        ),
        patch_bytes=0 if patch is None else len(patch.encode()),
        truncated=kind is DiffContentKind.OMITTED,
    )


@pytest.mark.parametrize(
    ("rule_id", "content"),
    [
        ("private_key", "-----BEGIN " + "PRIVATE KEY-----"),
        ("aws_access_key", "AKIA" + "A" * 16),
        ("github_token", "ghp_" + "a" * 36),
        ("openai_key", "sk-" + "a" * 32),
        ("google_api_key", "AIza" + "a" * 35),
        ("slack_token", "xoxb-" + "1" * 16),
        ("stripe_live_key", "sk_" + "live_" + "a" * 24),
        ("jwt", "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12),
        ("bearer_token", "Bearer " + "a" * 24),
        ("credential_url", "postgres://user:" + "private-value@localhost/db"),
        ("credential_assignment", "password = " + "correct-horse-battery-staple"),
    ],
)
def test_known_secret_families_are_detected_without_retaining_values(
    rule_id: str,
    content: str,
) -> None:
    result = SecretScanner().scan(review_with(content))

    assert result.passed is False
    assert rule_id in {finding.rule_id for finding in result.findings}
    serialized = json.dumps(result.to_dict())
    assert content not in serialized


@pytest.mark.parametrize(
    "content",
    [
        "token = placeholder",
        "api_key = REDACTED",
        "password = changeme",
        "authorization = xxxxxxxxxxxxxxxx",
        "secret = os.environ.get",
        "api_key = settings.openai_api_key",
        "password = getpass.getpass",
        "sha256 = 67a51a2d18f681b6f1b8c76e6f4e35a0",
        "Documentation mentions secrets without assigning a value.",
    ],
)
def test_placeholders_and_public_identifiers_do_not_create_false_findings(
    content: str,
) -> None:
    result = SecretScanner().scan(review_with(content))

    assert result.passed is True
    assert result.findings == ()


@pytest.mark.parametrize(
    "kind",
    [DiffContentKind.BINARY, DiffContentKind.LARGE, DiffContentKind.OMITTED],
)
def test_unscannable_added_content_fails_closed(kind: DiffContentKind) -> None:
    result = SecretScanner().scan(review_with("opaque", kind=kind))

    assert result.passed is False
    assert result.findings == ()
    assert result.incomplete_paths == ("file.txt",)


@pytest.mark.anyio
async def test_secret_blocks_review_ready_and_persists_only_sanitized_evidence(
    tmp_path: Path,
) -> None:
    database, gate, jobs, events, worktree = initialized_gate(tmp_path)
    secret = "ghp_" + "q" * 36
    (worktree / "credentials.txt").write_text(secret + "\n", encoding="utf-8")

    with pytest.raises(SecretGateBlockedError) as error:
        await gate.evaluate("job-001")

    assert jobs.get("job-001").state is JobState.VERIFYING
    assert error.value.scan.findings[0].rule_id == "github_token"
    assert error.value.scan.findings[0].path == "credentials.txt"
    assert error.value.scan.findings[0].line == 1
    stored = events.list("job-001")
    assert len(stored) == 1
    assert stored[0].id == error.value.event_id
    assert stored[0].event_type == "job.secret_scan.blocked"
    assert stored[0].payload["finding_count"] == 1
    assert stored[0].payload["rule_ids"] == ["github_token"]
    assert secret not in json.dumps(stored[0].payload)
    assert secret.encode() not in database.path.read_bytes()


@pytest.mark.anyio
async def test_clean_scan_atomically_enters_review_ready_and_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    database, gate, jobs, events, worktree = initialized_gate(tmp_path)
    (worktree / "example.txt").write_text("safe improvement\n", encoding="utf-8")

    first = await gate.evaluate("job-001")
    restarted = PreReviewSecretGate(
        JobRepository(Database(database.path)),
        EventRepository(Database(database.path)),
        AtomicTransitionService(Database(database.path)),
    )
    retry = await restarted.evaluate("job-001")

    assert first == retry
    assert first.state is JobState.REVIEW_READY
    assert first.scan.passed is True
    assert first.scan.scanned_files == 1
    assert first.scan.scanned_lines == 1
    assert jobs.get("job-001").state is JobState.REVIEW_READY
    assert events.list("job-001") == (events.get(first.event_id),)
    assert events.get(first.event_id).event_type == "job.review_ready"


@pytest.mark.anyio
async def test_removing_a_committed_secret_is_not_blocked(tmp_path: Path) -> None:
    old_secret = "AKIA" + "Z" * 16
    _, gate, jobs, _, worktree = initialized_gate(
        tmp_path,
        committed_content=old_secret + "\n",
    )
    (worktree / "example.txt").write_text("removed unsafe credential\n", encoding="utf-8")

    result = await gate.evaluate("job-001")

    assert result.scan.passed is True
    assert jobs.get("job-001").state is JobState.REVIEW_READY


@pytest.mark.anyio
async def test_binary_addition_blocks_without_changing_job_state(tmp_path: Path) -> None:
    _, gate, jobs, events, worktree = initialized_gate(tmp_path)
    (worktree / "archive.bin").write_bytes(b"\x00opaque")

    with pytest.raises(SecretGateBlockedError) as error:
        await gate.evaluate("job-001")

    assert error.value.scan.incomplete_paths == ("archive.bin",)
    assert jobs.get("job-001").state is JobState.VERIFYING
    assert events.list("job-001")[0].payload["incomplete_count"] == 1


@pytest.mark.anyio
async def test_invalid_job_states_and_snapshot_fail_without_events(tmp_path: Path) -> None:
    _, gate, jobs, events, worktree = initialized_gate(tmp_path)
    current = jobs.get("job-001")
    jobs.update(
        JobUpdate(
            id=current.id,
            state=JobState.RUNNING,
            runtime=current.runtime,
            model=current.model,
            worktree_path=current.worktree_path,
        )
    )

    with pytest.raises(SecretGateConflictError):
        await gate.evaluate("job-001")
    with pytest.raises(SecretGateNotFoundError):
        await gate.evaluate("missing")

    assert worktree.is_dir()
    assert events.list("job-001") == ()


@pytest.mark.anyio
async def test_api_reports_sanitized_block_then_allows_clean_retry(tmp_path: Path) -> None:
    worktree, base = initialize_repository(tmp_path / "worktree")
    runtime = tmp_path / "runtime"
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)})
    secret = "sk-" + "z" * 32

    async with api.router.lifespan_context(api):
        api.state.project_repository.create(project(worktree))
        api.state.job_repository.create(job(worktree, base))
        (worktree / "proposal.txt").write_text(f"api_key={secret}\n", encoding="utf-8")
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            blocked = await client.post("/api/jobs/job-001/review-readiness")
            (worktree / "proposal.txt").write_text("safe=true\n", encoding="utf-8")
            passed = await client.post("/api/jobs/job-001/review-readiness")

        stored = api.state.job_repository.get("job-001")
        event_types = [event.event_type for event in api.state.event_repository.list("job-001")]

    assert blocked.status_code == 409
    assert blocked.json()["detail"]["scan"]["finding_count"] >= 1
    assert secret not in blocked.text
    assert str(worktree) not in blocked.text
    assert passed.status_code == 200
    assert passed.json()["state"] == "review_ready"
    assert passed.json()["scan"]["status"] == "passed"
    assert stored.state is JobState.REVIEW_READY
    assert event_types == ["job.secret_scan.blocked", "job.review_ready"]


@pytest.mark.anyio
async def test_storage_failure_never_promotes_the_job(tmp_path: Path) -> None:
    database, gate, jobs, events, worktree = initialized_gate(tmp_path)
    (worktree / "example.txt").write_text("safe change\n", encoding="utf-8")
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_review_ready
            BEFORE INSERT ON events
            WHEN NEW.event_type = 'job.review_ready'
            BEGIN
                SELECT RAISE(ABORT, 'synthetic storage failure');
            END
            """
        )

    with pytest.raises(SecretGateUnavailableError):
        await gate.evaluate("job-001")

    assert jobs.get("job-001").state is JobState.VERIFYING
    assert events.list("job-001") == ()
