from __future__ import annotations

import asyncio
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app import create_app
from approval_service import ApprovalService
from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    ApprovalRepository,
    ApprovalValidationError,
    ApprovalWorkflowRepository,
    AtomicTransitionService,
    Database,
    EventRepository,
    JobRepository,
    Migration,
    MigrationApplyError,
    ProjectRepository,
    ReviewBundleRepository,
    VerificationRepository,
)
from engine import (
    ApprovalCreate,
    ApprovalDecision,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    ProjectConfig,
    RecoveryAction,
    Sensitivity,
)
from review_bundle import ReviewBundleService
from secret_gate import PreReviewSecretGate


def git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def prepare(api: object, tmp_path: Path) -> Path:
    root = tmp_path / "runtime" / "worktrees" / "project"
    root.mkdir(parents=True)
    git(root, "init", "--quiet")
    (root / "example.txt").write_text("original\n", encoding="utf-8")
    git(root, "add", "example.txt")
    git(
        root,
        "-c",
        "user.name=Test User",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Initial fixture",
    )
    api.state.project_repository.create(
        ProjectConfig(
            id="alpha",
            root=str(root),
            sensitivity=Sensitivity.PRIVATE,
            cloud_allowed=False,
            permission_mode=PermissionMode.SANDBOXED_WRITE,
        )
    )
    api.state.job_repository.create(
        JobCreate(
            id="job-001",
            project_id="alpha",
            request="Review this safe change.",
            request_snapshot={"repo_head": git(root, "rev-parse", "HEAD")},
            state=JobState.VERIFYING,
            runtime=JobRuntime.CODEX,
            model="test-model",
            worktree_path=str(root),
        )
    )
    (root / "example.txt").write_text("safe change\n", encoding="utf-8")
    return root


async def review(client: httpx.AsyncClient) -> dict[str, object]:
    readiness = await client.post("/api/jobs/job-001/review-readiness")
    assert readiness.status_code == 200
    response = await client.post("/api/jobs/job-001/review-bundle")
    assert response.status_code == 200
    return response.json()


async def request(client: httpx.AsyncClient, bundle_hash: str) -> httpx.Response:
    return await client.post(
        "/api/jobs/job-001/approval-request", json={"bundle_hash": bundle_hash}
    )


async def decide(
    client: httpx.AsyncClient, approval_id: str, bundle_hash: str, decision: str
) -> httpx.Response:
    return await client.post(
        f"/api/approvals/{approval_id}",
        json={"decision": decision, "bundle_hash": bundle_hash},
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("decision", "target"),
    [
        ("approved", JobState.APPROVED),
        ("rejected", JobState.REJECTED),
        ("changes_requested", JobState.RUNNING),
    ],
)
async def test_all_decisions_commit_one_event_and_replay_after_restart(
    tmp_path: Path, decision: str, target: JobState
) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            first_request, retry_request = await asyncio.gather(
                request(client, bundle["bundle_hash"]),
                request(client, bundle["bundle_hash"]),
            )
            assert first_request.status_code == retry_request.status_code == 200
            assert first_request.json() == retry_request.json()
            approval_id = first_request.json()["approval_id"]
            first = await decide(client, approval_id, bundle["bundle_hash"], decision)
            retry = await decide(client, approval_id, bundle["bundle_hash"], decision)
            conflict = await decide(
                client,
                approval_id,
                bundle["bundle_hash"],
                "rejected" if decision != "rejected" else "approved",
            )

        assert first.status_code == retry.status_code == 200
        assert first.json() == retry.json()
        assert first.json()["job_state"] == target.value
        assert first.headers["Cache-Control"] == "no-store"
        assert str(root) not in first.text
        assert conflict.status_code == 409
        assert api.state.job_repository.get("job-001").state is target
        assert api.state.approval_repository.get(approval_id).decision is ApprovalDecision(decision)
        events = api.state.event_repository.list("job-001")
        assert [event.event_type for event in events].count("approval.requested") == 1
        assert [event.event_type for event in events].count("approval.decided") == 1
        if target is not JobState.REJECTED:
            assert api.state.recovery_service.load()[0].job.state is target

    restarted = Database(tmp_path / "runtime" / "state.db")
    assert restarted.initialize() == LATEST_SCHEMA_VERSION
    assert ApprovalRepository(restarted).get(approval_id).decision is ApprovalDecision(decision)
    assert [event.event_type for event in EventRepository(restarted).list("job-001")].count(
        "approval.decided"
    ) == 1


@pytest.mark.anyio
async def test_changed_content_and_request_input_fail_closed(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            digest = bundle["bundle_hash"]
            with pytest.raises(ApprovalValidationError):
                api.state.approval_workflow_repository.request(
                    ApprovalCreate(
                        "invalid-binding",
                        "job-001",
                        {"job_id": "job-001", "bundle_hash": "0" * 64},
                        datetime.now(UTC) + timedelta(minutes=15),
                    ),
                    api.state.job_repository.get("job-001"),
                    api.state.review_bundle_repository.get("job-001"),
                )
            wrong = "0" * 64 if digest != "0" * 64 else "1" * 64
            assert (await request(client, wrong)).status_code == 409
            malformed = await client.post(
                "/api/jobs/job-001/approval-request",
                json={"bundle_hash": digest, "extra": "not allowed"},
            )
            assert malformed.status_code == 422
            foreign_origin = await client.post(
                "/api/jobs/job-001/approval-request",
                headers={"Origin": "https://other.example.invalid"},
                json={"bundle_hash": digest},
            )
            assert foreign_origin.status_code == 403
            remote_transport = httpx.ASGITransport(app=api, client=("192.0.2.9", 1234))
            async with httpx.AsyncClient(
                transport=remote_transport, base_url="http://localhost"
            ) as remote:
                assert (await request(remote, digest)).status_code == 403
            alien_host = await client.post(
                "http://other.example.invalid/api/jobs/job-001/approval-request",
                json={"bundle_hash": digest},
            )
            assert alien_host.status_code == 403
            (root / "example.txt").write_text("different change\n", encoding="utf-8")
            assert (await request(client, digest)).status_code == 409
            assert api.state.job_repository.get("job-001").state is JobState.REVIEW_READY

            (root / "example.txt").write_text("safe change\n", encoding="utf-8")
            approval_id = (await request(client, digest)).json()["approval_id"]
            (root / "example.txt").write_text("different change\n", encoding="utf-8")
            assert (await decide(client, approval_id, digest, "approved")).status_code == 409
            assert api.state.approval_repository.get(approval_id).decision is None
            assert api.state.job_repository.get("job-001").state is JobState.WAITING_APPROVAL
            (root / "example.txt").write_text("safe change\n", encoding="utf-8")
            assert (await decide(client, approval_id, wrong, "approved")).status_code == 409
            assert (await decide(client, approval_id, digest, "invalid")).status_code == 422
            extra = await client.post(
                f"/api/approvals/{approval_id}",
                json={"decision": "approved", "bundle_hash": digest, "actor": "other"},
            )
            assert extra.status_code == 422
            assert (await decide(client, approval_id, digest, "approved")).status_code == 200


@pytest.mark.anyio
async def test_pending_approval_survives_application_restart(tmp_path: Path) -> None:
    environment = {"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")}
    first = create_app(environment)
    async with first.router.lifespan_context(first):
        prepare(first, tmp_path)
        transport = httpx.ASGITransport(app=first)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            pending = (await request(client, bundle["bundle_hash"])).json()

    restarted = create_app(environment)
    async with restarted.router.lifespan_context(restarted):
        recovered = restarted.state.recovery_items
        assert len(recovered) == 1
        assert recovered[0].action is RecoveryAction.WAIT_FOR_APPROVAL
        assert recovered[0].approval.id == pending["approval_id"]
        transport = httpx.ASGITransport(app=restarted)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            decided = await decide(
                client, pending["approval_id"], bundle["bundle_hash"], "approved"
            )
        assert decided.status_code == 200
        assert restarted.state.job_repository.get("job-001").state is JobState.APPROVED


@pytest.mark.anyio
async def test_request_changes_keeps_old_bundle_and_allows_new_round(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            old = await review(client)
            first_id = (await request(client, old["bundle_hash"])).json()["approval_id"]
            first_decision = await decide(client, first_id, old["bundle_hash"], "changes_requested")
            assert first_decision.status_code == 200
            current = api.state.job_repository.get("job-001")
            api.state.job_repository.update(
                JobUpdate(
                    current.id,
                    JobState.VERIFYING,
                    current.runtime,
                    current.model,
                    current.worktree_path,
                )
            )
            refreshed = await client.post("/api/jobs/job-001/review-readiness")
            assert refreshed.status_code == 200
            assert (await request(client, old["bundle_hash"])).status_code == 409
            new = await review(client)
            assert new["bundle_hash"] != old["bundle_hash"]
            assert new["bundle"]["diff_digest"] == old["bundle"]["diff_digest"]
            assert new["bundle"]["scan_event_id"] > old["bundle"]["scan_event_id"]
            assert (await client.get("/api/jobs/job-001/review-bundle")).json() == new
            old_record = api.state.review_bundle_repository.get_for_event(
                "job-001", old["bundle"]["scan_event_id"]
            )
            assert old_record.payload_hash == old["bundle_hash"]
            second_id = (await request(client, new["bundle_hash"])).json()["approval_id"]
            assert second_id != first_id
            assert (
                await decide(client, second_id, new["bundle_hash"], "changes_requested")
            ).status_code == 200
            current = api.state.job_repository.get("job-001")
            api.state.job_repository.update(
                JobUpdate(
                    current.id,
                    JobState.VERIFYING,
                    current.runtime,
                    current.model,
                    current.worktree_path,
                )
            )
            (root / "example.txt").write_text("revised safe change\n", encoding="utf-8")
            third = await review(client)
            assert third["bundle"]["diff_digest"] != old["bundle"]["diff_digest"]
            assert third["bundle"]["scan_event_id"] > new["bundle"]["scan_event_id"]
            third_id = (await request(client, third["bundle_hash"])).json()["approval_id"]
            assert (
                await decide(client, third_id, third["bundle_hash"], "approved")
            ).status_code == 200
            old_retry = await decide(client, first_id, old["bundle_hash"], "changes_requested")
            assert old_retry.status_code == 200
            assert old_retry.json() == first_decision.json()
            assert api.state.job_repository.get("job-001").state is JobState.APPROVED
            with sqlite3.connect(api.state.database.path) as connection:
                assert connection.execute("SELECT COUNT(*) FROM review_bundles").fetchone() == (3,)


@pytest.mark.anyio
async def test_expiry_and_failed_event_insert_roll_back_every_state(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        prepare(api, tmp_path)
        now = datetime(2030, 1, 1, tzinfo=UTC)

        def clock() -> datetime:
            return now

        workflow = ApprovalWorkflowRepository(api.state.database, clock=clock)
        service = ApprovalService(
            api.state.job_repository,
            api.state.approval_repository,
            api.state.review_bundle_repository,
            workflow,
            clock=clock,
        )
        api.state.approval_service = service
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            digest = bundle["bundle_hash"]
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER fail_request BEFORE INSERT ON events
                    WHEN NEW.event_type = 'approval.requested'
                    BEGIN SELECT RAISE(ABORT, 'test failure'); END
                    """
                )
            assert (await request(client, digest)).status_code == 503
            assert api.state.job_repository.get("job-001").state is JobState.REVIEW_READY
            assert api.state.approval_repository.list(job_id="job-001") == ()
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute("DROP TRIGGER fail_request")
            approval_id = (await request(client, digest)).json()["approval_id"]
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER fail_decision BEFORE INSERT ON events
                    WHEN NEW.event_type = 'approval.decided'
                    BEGIN SELECT RAISE(ABORT, 'test failure'); END
                    """
                )
            assert (await decide(client, approval_id, digest, "approved")).status_code == 503
            assert api.state.job_repository.get("job-001").state is JobState.WAITING_APPROVAL
            assert api.state.approval_repository.get(approval_id).decision is None
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute("DROP TRIGGER fail_decision")
            service._clock = lambda: now + timedelta(minutes=16)
            workflow._approvals.clock = service._clock
            assert (await decide(client, approval_id, digest, "approved")).status_code == 409
            assert api.state.approval_repository.get(approval_id).decision is None
            assert [event.event_type for event in api.state.event_repository.list("job-001")].count(
                "approval.decided"
            ) == 0


@pytest.mark.anyio
async def test_concurrent_decision_retries_create_one_event(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        prepare(api, tmp_path)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            approval_id = (await request(client, bundle["bundle_hash"])).json()["approval_id"]
            first, second = await asyncio.gather(
                decide(client, approval_id, bundle["bundle_hash"], "approved"),
                decide(client, approval_id, bundle["bundle_hash"], "approved"),
            )
        assert first.status_code == second.status_code == 200
        assert first.json() == second.json()
        events = api.state.event_repository.list("job-001")
        assert [event.event_type for event in events].count("approval.decided") == 1


@pytest.mark.anyio
async def test_trusted_channels_share_the_same_decision_service(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        prepare(api, tmp_path)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            bundle = await review(client)
            approval_id = (await request(client, bundle["bundle_hash"])).json()["approval_id"]
            body = {"decision": "approved", "bundle_hash": bundle["bundle_hash"]}
            recorded = await api.state.approval_service.decide(
                approval_id, body, actor="operator-01", channel="desktop"
            )
            retry = await api.state.approval_service.decide(
                approval_id, body, actor="operator-01", channel="desktop"
            )
            api_retry = await decide(client, approval_id, bundle["bundle_hash"], "approved")
        assert recorded == retry
        assert api_retry.status_code == 409
        assert api.state.approval_repository.get(approval_id).channel == "desktop"
        assert [event.event_type for event in api.state.event_repository.list("job-001")].count(
            "approval.decided"
        ) == 1


@pytest.mark.anyio
async def test_schema_eleven_bundle_survives_revision_migration(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:11])
    assert legacy.initialize() == 11
    stub = SimpleNamespace(
        state=SimpleNamespace(
            project_repository=ProjectRepository(legacy),
            job_repository=JobRepository(legacy),
        )
    )
    prepare(stub, tmp_path)
    jobs = JobRepository(legacy)
    events = EventRepository(legacy)
    gate = PreReviewSecretGate(jobs, events, AtomicTransitionService(legacy))
    await gate.evaluate("job-001")
    bundle = await ReviewBundleService(
        jobs,
        events,
        VerificationRepository(legacy),
        ReviewBundleRepository(legacy),
    ).create("job-001")
    interrupted = Migration(
        version=12,
        name="interrupted_review_upgrade",
        statements=MIGRATIONS[11].statements[:3] + ("INSERT INTO missing_table VALUES (1)",),
    )
    with pytest.raises(MigrationApplyError):
        Database(path, migrations=MIGRATIONS[:11] + (interrupted,)).initialize()
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT version FROM schema_version").fetchone() == (11,)
        assert connection.execute("SELECT COUNT(*) FROM review_bundles").fetchone() == (1,)
    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION == 12
    assert ReviewBundleRepository(upgraded).get("job-001").payload_hash == bundle["bundle_hash"]
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE review_bundles SET payload_hash = ?", ("0" * 64,))
