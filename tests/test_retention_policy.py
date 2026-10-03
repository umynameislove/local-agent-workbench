from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from test_approval_service import decide, prepare, request, review

from app import create_app
from approval_service import ApprovalService
from db import ApprovalWorkflowRepository
from engine import ConfigurationError, JobState, RetentionConfig, WorkbenchConfig


@pytest.mark.parametrize("value", [0, -1, 366, True, False, 1.5, "7", None])
def test_retention_period_requires_positive_bounded_whole_days(value: object) -> None:
    with pytest.raises(ConfigurationError):
        RetentionConfig.from_dict({"rejected_worktree_days": value})


@pytest.mark.parametrize("value", [[], None, 7, {"days": 7}, {"enabled": True}])
def test_retention_config_rejects_unknown_or_non_object_fields(value: object) -> None:
    with pytest.raises(ConfigurationError):
        RetentionConfig.from_dict(value)


def test_old_config_defaults_and_explicit_retention_remain_compatible() -> None:
    base = {"version": 1, "projects": [{"id": "alpha", "root": "./project"}]}
    assert WorkbenchConfig.from_dict(base).retention.rejected_worktree_days == 7
    assert (
        WorkbenchConfig.from_dict(
            {**base, "retention": {"rejected_worktree_days": 31}}
        ).retention.rejected_worktree_days
        == 31
    )
    assert RetentionConfig.from_dict({}).rejected_worktree_days == 7


def fixed_workflow(api: object, now: datetime, days: int = 7) -> ApprovalWorkflowRepository:
    workflow = ApprovalWorkflowRepository(
        api.state.database, clock=lambda: now, retention=RetentionConfig(days)
    )
    api.state.approval_workflow_repository = workflow
    api.state.approval_service = ApprovalService(
        api.state.job_repository,
        api.state.approval_repository,
        api.state.review_bundle_repository,
        workflow,
        clock=lambda: now,
    )
    return workflow


@pytest.mark.anyio
async def test_rejection_freezes_policy_atomically_and_retry_does_not_reset_it(
    tmp_path: Path,
) -> None:
    now = datetime(2030, 1, 1, 10, 30, 0, 123456, tzinfo=UTC)
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        workflow = fixed_workflow(api, now, 3)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            digest = (await review(client))["bundle_hash"]
            approval_id = (await request(client, digest)).json()["approval_id"]
            first = await decide(client, approval_id, digest, "rejected")
            workflow._retention = RetentionConfig(1)
            retry = await decide(client, approval_id, digest, "rejected")
        assert first.status_code == retry.status_code == 200
        assert first.json() == retry.json()
        assert root.exists()
        assert (root / "example.txt").read_text() == "safe change\n"
        assert api.state.job_repository.get("job-001").state is JobState.REJECTED
        policy = tuple(
            event
            for event in api.state.event_repository.list("job-001")
            if event.event_type == "job.worktree.retained"
        )
        assert len(policy) == 1
        assert policy[0].payload == {
            "approval_id": approval_id,
            "bundle_hash": digest,
            "retention_days": 3,
            "retain_until": "2030-01-04T10:30:00.123456Z",
            "worktree_identity": None,
        }
        assert not any(
            "cleanup" in event.event_type for event in api.state.event_repository.list("job-001")
        )
        assert policy[0].id < first.json()["event_id"]
    restarted = ApprovalWorkflowRepository(api.state.database, retention=RetentionConfig(30))
    assert restarted.get_decision_event(approval_id, "job-001").id == first.json()["event_id"]
    assert api.state.event_repository.get(policy[0].id) == policy[0]


@pytest.mark.anyio
async def test_retention_event_failure_rolls_back_entire_rejection(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        fixed_workflow(api, now)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            digest = (await review(client))["bundle_hash"]
            approval_id = (await request(client, digest)).json()["approval_id"]
            with sqlite3.connect(api.state.database.path) as connection:
                connection.execute(
                    """CREATE TRIGGER block_retention BEFORE INSERT ON events
                    WHEN NEW.event_type = 'job.worktree.retained'
                    BEGIN SELECT RAISE(ABORT, 'injected'); END"""
                )
            failed = await decide(client, approval_id, digest, "rejected")
        assert failed.status_code == 503
        assert api.state.approval_repository.get(approval_id).decision is None
        assert api.state.job_repository.get("job-001").state is JobState.WAITING_APPROVAL
        assert root.exists()
        assert all(
            event.event_type not in {"approval.decided", "job.worktree.retained"}
            for event in api.state.event_repository.list("job-001")
        )


@pytest.mark.anyio
@pytest.mark.parametrize("decision", ["approved", "changes_requested"])
async def test_other_decisions_do_not_create_retention_policy(
    tmp_path: Path, decision: str
) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})
    async with api.router.lifespan_context(api):
        root = prepare(api, tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            digest = (await review(client))["bundle_hash"]
            approval_id = (await request(client, digest)).json()["approval_id"]
            assert (await decide(client, approval_id, digest, decision)).status_code == 200
        assert root.exists()
        assert all(
            event.event_type != "job.worktree.retained"
            for event in api.state.event_repository.list("job-001")
        )


def test_policy_can_represent_365_days_and_frozen_value_cannot_be_changed() -> None:
    policy = RetentionConfig(365)
    assert timedelta(days=policy.rejected_worktree_days).days == 365
    with pytest.raises(AttributeError):
        policy.rejected_worktree_days = 1
