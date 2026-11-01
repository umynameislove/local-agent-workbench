from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from test_phone_control import BOT, OWNER, phone_app
from test_phone_review import review_card, review_phone
from test_planner import planner_item
from test_workspace_control import claim

from local_agent_workbench.engine import EventCreate, JobCreate, JobState, JobUpdate, Sensitivity
from local_agent_workbench.phone_notifications import PhoneNotifications
from local_agent_workbench.phone_policy import PhonePolicy
from local_agent_workbench.phone_store import PhoneError


def record(h, state, project="sample-project", identity="notice-job"):
    job = h.api.state.job_repository.create(
        JobCreate(identity, project, "Private request stays local.", {}, state=state)
    )
    h.api.state.event_repository.append(EventCreate(job.id, "fixture.state", {}))
    return job


@pytest.mark.anyio
async def test_notifications_deduplicate_across_projection_rewind_and_restart(tmp_path):
    async with phone_app(tmp_path, notifications=True, quiet_start="00:00", quiet_end="00:00") as (
        h,
        _,
    ):
        job = record(h, JobState.BLOCKED)
        cards = h.drain()
        assert len(cards) == 1 and "Blocked" in cards[0]["text"]
        assert job.request not in cards[0]["text"] and job.project_id not in cards[0]["text"]
        with h.service.store._write_connection() as c:
            c.execute("UPDATE phone_cursors SET event_id=0")
        h.service.store.recover()
        assert h.drain() == []
        h.api.state.event_repository.append(EventCreate(job.id, "fixture.another", {}))
        assert h.drain() == []
        with h.service.store._connection() as c:
            assert (
                c.execute("SELECT COUNT(*) FROM phone_outbox WHERE kind='notice'").fetchone()[0]
                == 1
            )


@pytest.mark.anyio
async def test_quiet_hours_defer_notices_but_not_owner_replies_and_mute_persists(
    tmp_path, monkeypatch
):
    async with phone_app(tmp_path, notifications=True, timezone="Asia/Ho_Chi_Minh") as (h, _):
        quiet = [True]
        monkeypatch.setattr(PhonePolicy, "quiet", lambda *args: quiet[0])
        record(h, JobState.BLOCKED)
        assert h.drain() == []
        await h.send("/mute")
        assert "muted" in h.drain()[0]["text"]
        h.service.store.recover()
        h.now[0] += 3600
        quiet[0] = False
        assert h.drain() == []
        await h.send("/unmute")
        cards = h.drain()
        assert "Notifications enabled" in cards[0]["text"]
        assert sum("Blocked" in c["text"] for c in cards) == 1


@pytest.mark.anyio
async def test_delayed_notice_is_dropped_when_job_or_project_policy_changes(tmp_path):
    async with phone_app(tmp_path, notifications=True) as (h, _):
        job = record(h, JobState.BLOCKED)
        notices = PhoneNotifications(h.service)
        notices.project(h.now[0])
        h.api.state.job_repository.update(JobUpdate(job.id, JobState.QUEUED, job.runtime))
        assert h.drain() == []
        second = record(h, JobState.FAILED, identity="another")
        notices.project(h.now[0])
        project = h.api.state.project_repository.get(second.project_id).to_config()
        h.api.state.project_repository.update(
            replace(project, sensitivity=Sensitivity.RESTRICTED, cloud_allowed=False)
        )
        assert h.drain() == []


@pytest.mark.anyio
async def test_approval_notification_expires_without_extending_original_authority(tmp_path):
    async with review_phone(tmp_path, notifications=True) as (h, client, _):
        h.service.policy = replace(h.service.policy, quiet_start="00:00", quiet_end="00:00")
        h.service.store.policy = h.service.policy
        config = json.loads(h.api.state.runtime.config.read_text())
        config["phone_control"].update(quiet_start="00:00", quiet_end="00:00")
        h.api.state.runtime.config.write_text(json.dumps(config))
        _, pending, _ = await review_card(h, client)
        original = h.api.state.approval_repository.get(pending["approval_id"]).expires_at
        notices = PhoneNotifications(h.service)
        notices.project(h.now[0])
        h.now[0] += 901
        assert h.drain() == []
        assert h.api.state.approval_repository.get(pending["approval_id"]).expires_at == original


@pytest.mark.anyio
async def test_uncertain_delivery_is_never_retried_and_rate_limit_is_shared(tmp_path):
    async with phone_app(tmp_path, notifications=True, quiet_start="00:00", quiet_end="00:00") as (
        h,
        _,
    ):
        record(h, JobState.BLOCKED)
        await h.send("/start")
        notices = PhoneNotifications(h.service)
        first = notices.next(BOT, OWNER)
        assert "Choose" in first["text"]
        h.service.store.acknowledge(first["delivery_id"], 200)
        assert notices.next(BOT, OWNER) is None
        h.now[0] += 1
        second = notices.next(BOT, OWNER)
        assert "Blocked" in second["text"]
        h.service.store.recover()
        h.now[0] += 1
        assert notices.next(BOT, OWNER) is None
        with h.service.store._connection() as c:
            assert (
                c.execute(
                    "SELECT phase FROM phone_outbox WHERE id=?", (second["delivery_id"],)
                ).fetchone()[0]
                == "uncertain"
            )
        with pytest.raises(PhoneError):
            h.service.store.acknowledge(second["delivery_id"], 201)


@pytest.mark.anyio
async def test_reconciliation_notice_targets_current_attempt_and_is_suppressed_after_settlement(
    tmp_path,
):
    async with phone_app(tmp_path, notifications=True, quiet_start="00:00", quiet_end="00:00") as (
        h,
        _,
    ):
        attempt = claim(h.api)
        attempts = h.api.state.native_attempt_repository
        attempts.quarantine(attempt)
        assert len(h.drain()) == 1
        with h.service.store._write_connection() as c:
            c.execute("UPDATE phone_cursors SET event_id=0")
        assert h.drain() == []
        attempts.finish(attempt, "cancelled")
        assert h.drain() == []


@pytest.mark.anyio
async def test_outbox_does_not_store_secret_content(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        secret = "sk-" + "synthetic" * 5
        with pytest.raises(PhoneError), h.service.store._write_connection() as c:
            h.service.store.queue(c, "bad", [{"text": secret}], h.now[0] + 120)
        with h.service.store._connection() as c:
            assert c.execute("SELECT COUNT(*) FROM phone_outbox").fetchone()[0] == 0


@pytest.mark.anyio
async def test_urgent_deadline_is_content_free_and_deduplicated(tmp_path):
    async with phone_app(tmp_path, notifications=True, quiet_start="00:00", quiet_end="00:00") as (
        h,
        _,
    ):
        due = datetime.fromtimestamp(h.now[0] + 600, UTC)
        h.api.state.planner_repository.create(planner_item(project_id="sample-project", due_at=due))
        cards = h.drain()
        assert len(cards) == 1 and "deadline" in cards[0]["text"]
        assert "reviewed deliverable" not in cards[0]["text"]
        assert h.drain() == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state,label",
    [
        (JobState.WAITING_INPUT, "Input needed"),
        (JobState.FAILED, "Failed"),
        (JobState.COMPLETED, "Completed"),
    ],
)
async def test_action_completion_and_error_notifications_expose_only_state(tmp_path, state, label):
    async with phone_app(tmp_path, notifications=True, quiet_start="00:00", quiet_end="00:00") as (
        h,
        _,
    ):
        job = record(h, state)
        cards = h.drain()
        assert len(cards) == 1 and label in cards[0]["text"]
        assert job.request not in cards[0]["text"]


@pytest.mark.parametrize(
    "zone,start,end,at,expected",
    [
        ("Asia/Ho_Chi_Minh", "22:00", "08:00", "2026-11-01T15:00:00+00:00", True),
        ("Asia/Ho_Chi_Minh", "22:00", "08:00", "2026-11-02T01:00:00+00:00", False),
        ("America/New_York", "01:00", "03:00", "2026-11-01T06:30:00+00:00", True),
        ("UTC", "09:00", "10:00", "2026-11-01T09:00:00+00:00", True),
        ("UTC", "09:00", "09:00", "2026-11-01T09:00:00+00:00", False),
    ],
)
def test_quiet_hours_respect_timezone_boundaries_and_clock_change(zone, start, end, at, expected):
    policy = PhonePolicy.from_dict({"timezone": zone, "quiet_start": start, "quiet_end": end})
    assert policy.quiet(int(datetime.fromisoformat(at).timestamp())) is expected
