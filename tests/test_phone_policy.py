from __future__ import annotations

import copy
import sqlite3

import pytest
from test_phone_control import BOT, OWNER, action_data, phone_app
from test_team_config import team_configuration

from local_agent_workbench.db import DatabaseError
from local_agent_workbench.engine import ConfigurationError, WorkbenchConfig
from local_agent_workbench.phone_policy import PhonePolicy


@pytest.mark.parametrize(
    "change",
    [
        {"enabled": "yes"},
        {"owner_id": True},
        {"bot_id": "123"},
        {"owner_id": 0},
        {"projects": "alpha"},
        {"projects": ["alpha", "alpha"]},
        {"projects": [" "]},
        {"timezone": "missing/timezone"},
        {"quiet_start": "25:00"},
        {"quiet_end": None},
        {"review_content": 1},
        {"notifications": []},
        {"api_key": "example"},
        {"enabled": True},
    ],
)
def test_invalid_phone_policy_never_infers_authority(change):
    with pytest.raises(ValueError):
        PhonePolicy.from_dict(change)


@pytest.mark.parametrize(
    "version,project,sensitivity",
    [
        (1, "sample-project", "private"),
        (2, "missing", "private"),
        (2, "sample-project", "restricted"),
        (2, "sample-project", "internal"),
    ],
)
def test_phone_configuration_requires_explicit_safe_project_scope(version, project, sensitivity):
    config = team_configuration()
    config["version"] = version
    config["agent_system"] = {}
    config["projects"][0].update(sensitivity=sensitivity, cloud_allowed=False)
    config["phone_control"] = {
        "enabled": True,
        "bot_id": BOT,
        "owner_id": OWNER,
        "projects": [project],
    }
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(config)


@pytest.mark.anyio
@pytest.mark.parametrize("owner", [None, True, "123456", OWNER + 1])
async def test_bridge_rejects_missing_or_coerced_owner_identity(tmp_path, owner):
    async with phone_app(tmp_path) as (_, client):
        identity = {"bot_id": BOT, "owner_id": owner}
        assert (await client.post("/api/phone/connect", json=identity)).status_code == 403
        assert (await client.post("/api/phone/next", json=identity)).status_code == 409


@pytest.mark.anyio
@pytest.mark.parametrize(
    "boundary",
    [
        "forward",
        "edited",
        "old",
        "future",
        "sender_bot",
        "sender_type",
        "other_chat",
        "unsupported",
    ],
)
async def test_unauthorized_updates_have_no_reply_or_mutation(tmp_path, boundary):
    async with phone_app(tmp_path) as (h, _):
        update = h.update("/start")
        message = update["message"]
        if boundary == "forward":
            message["forward_origin"] = {}
        elif boundary == "edited":
            update["edited_message"] = message
        elif boundary == "old":
            message["date"] -= 120
        elif boundary == "future":
            message["date"] += 31
        elif boundary == "sender_bot":
            message["from"]["is_bot"] = True
        elif boundary == "sender_type":
            message["from"] = []
        elif boundary == "other_chat":
            message["chat"]["id"] += 1
        else:
            update = {"update_id": update["update_id"], "channel_post": message}
        await h.service.process(BOT, update)
        assert h.drain() == [] and h.api.state.job_repository.list() == ()


@pytest.mark.anyio
async def test_callback_receipt_records_clicker_without_inventing_click_time(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        update = await h.send(data=action_data(card), message=card)
        receipt = h.service.journal.get(str(BOT), update["update_id"])
        assert receipt["actor"] == receipt["chat"] == OWNER
        assert receipt["issued_at"] is None
        altered = copy.deepcopy(update)
        altered["callback_query"]["data"] += "changed"
        with pytest.raises(DatabaseError, match="different content"):
            await h.service.process(BOT, altered)
        assert len(h.api.state.job_repository.list()) == 1


@pytest.mark.anyio
async def test_action_and_delivery_contracts_cannot_be_rewritten(tmp_path):
    async with phone_app(tmp_path) as (h, _):
        card = await h.confirmation()
        token = action_data(card).split(":")[-1]
        with h.service.store._connection() as c:
            for sql, arguments in [
                ("UPDATE phone_actions SET payload='{}' WHERE token=?", (token,)),
                ("UPDATE phone_actions SET expires_at=expires_at+1 WHERE token=?", (token,)),
                ("UPDATE phone_outbox SET packet='{}' WHERE action_token=?", (token,)),
                ("UPDATE phone_outbox SET phase='ready' WHERE action_token=?", (token,)),
            ]:
                with pytest.raises(sqlite3.IntegrityError):
                    c.execute(sql, arguments)
