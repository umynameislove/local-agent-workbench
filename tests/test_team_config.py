from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from local_agent_workbench.engine import ConfigurationError, WorkbenchConfig
from local_agent_workbench.team_config import AgentSystem, TeamConfigError, migrate_config


def team_configuration() -> dict:
    return {
        "version": 2,
        "projects": [{"id": "sample-project", "root": "./project", "cloud_allowed": True}],
        "agent_system": {
            "version": 1,
            "native_execution_enabled": True,
            "connections": [
                {"id": "native", "kind": "codex", "enabled": True, "account_slot": "fixture"}
            ],
            "models": [
                {
                    "id": "coding",
                    "connection_id": "native",
                    "model": "test-model",
                    "verified": True,
                    "capabilities": ["tools", "files", "streaming"],
                }
            ],
            "agents": [
                {
                    "id": "coder",
                    "model_id": "coding",
                    "role": "implementer",
                    "tools": ["native_workspace"],
                    "allowed_write_paths": ["."],
                }
            ],
            "teams": [{"project_id": "sample-project", "agent_ids": ["coder"]}],
        },
    }


def test_version_two_round_trip_and_defaults():
    config = WorkbenchConfig.from_dict(team_configuration())
    system = config.agent_system
    assert AgentSystem.from_dict(system.to_dict()) == system
    assert system.max_active_agents == 4 and system.max_heavy_local == 1
    assert not system.teams[0].paid_api_enabled
    assert config.version == 2
    empty = AgentSystem.from_dict({})
    assert not empty.native_execution_enabled and not empty.teams


def test_legacy_migration_does_not_infer_accounts_or_paid_authority():
    raw = json.loads(Path("config.example.json").read_text())
    raw["consultant"]["enabled"] = True
    before = copy.deepcopy(raw)
    assert WorkbenchConfig.from_dict(raw).agent_system == AgentSystem()
    migrated = migrate_config(raw)
    assert raw == before
    result = WorkbenchConfig.from_dict(migrated)
    assert result.version == 2 and not result.consultant.enabled
    assert result.agent_system == AgentSystem()
    with pytest.raises(TeamConfigError):
        migrate_config(migrated)


@pytest.mark.parametrize("sensitivity", ["public", "private", "internal", "restricted"])
def test_project_sensitivity_and_cloud_permission(sensitivity):
    raw = team_configuration()
    raw["projects"][0].update(sensitivity=sensitivity, cloud_allowed=False)
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)
    raw["projects"][0]["cloud_allowed"] = True
    if sensitivity in {"internal", "restricted"}:
        with pytest.raises(ConfigurationError):
            WorkbenchConfig.from_dict(raw)
    else:
        assert WorkbenchConfig.from_dict(raw).projects[0].cloud_allowed


@pytest.mark.parametrize(
    "group,field,value",
    [
        (None, "version", True),
        (None, "native_execution_enabled", 1),
        (None, "max_active_agents", 0),
        (None, "max_heavy_local", 2),
        (None, "monthly_cap_usd", float("nan")),
        (None, "execution_timeout_seconds", True),
        ("connections", "kind", []),
        ("connections", "account_slot", "../account"),
        ("connections", "credential_ref", "raw-secret"),
        ("connections", "enabled", "yes"),
        ("models", "connection_id", "missing"),
        ("models", "capabilities", ["execute_everything"]),
        ("agents", "model_id", "missing"),
        ("agents", "role", []),
        ("agents", "allowed_write_paths", ["../secret"]),
        ("agents", "allowed_write_paths", [".git"]),
        ("agents", "allowed_write_paths", ["/workspace"]),
        ("teams", "agent_ids", ["missing"]),
        ("teams", "agent_ids", ["coder", "coder"]),
        ("teams", "mode", "magical"),
        ("teams", "mode", []),
        ("teams", "max_active", 5),
        ("teams", "per_task_cap_usd", 6),
        ("teams", "lead_id", "coder"),
        ("teams", "project_id", "other-project"),
    ],
)
def test_invalid_configuration_fails_closed(group, field, value):
    raw = team_configuration()
    target = raw["agent_system"] if group is None else raw["agent_system"][group][0]
    target[field] = value
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)


@pytest.mark.parametrize("group", ["connections", "models", "agents", "teams"])
def test_duplicate_ids_and_unknown_fields_are_rejected(group):
    raw = team_configuration()
    raw["agent_system"][group].append(copy.deepcopy(raw["agent_system"][group][0]))
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)
    raw = team_configuration()
    raw["agent_system"][group][0]["unknown"] = True
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)


def test_connection_and_model_are_not_execution_permissions():
    raw = team_configuration()
    raw["agent_system"]["connections"][0]["enabled"] = False
    system = WorkbenchConfig.from_dict(raw).agent_system
    with pytest.raises(TeamConfigError):
        system.binding(system.teams[0], "coder", require_available=True)
    raw["agent_system"]["connections"][0]["enabled"] = True
    raw["agent_system"]["models"][0]["verified"] = False
    system = WorkbenchConfig.from_dict(raw).agent_system
    with pytest.raises(TeamConfigError):
        system.binding(system.teams[0], "coder", require_available=True)


def test_api_catalog_cannot_inherit_native_harness_and_advice_has_no_tools():
    raw = team_configuration()
    source = raw["agent_system"]["connections"][0]
    source.update(kind="openrouter", account_slot=None, credential_ref="keychain:fixture")
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)
    profile = raw["agent_system"]["agents"][0]
    profile.update(role="advice", tools=[], allowed_write_paths=[])
    system = WorkbenchConfig.from_dict(raw).agent_system
    with pytest.raises(TeamConfigError, match="Paid API"):
        system.binding(system.teams[0], "coder", require_available=True)
    profile["tools"] = ["document.read"]
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)


def test_legacy_cannot_hide_new_fields_and_version_two_is_strict():
    raw = team_configuration()
    raw["version"] = 1
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)
    raw["version"] = 2
    raw["extra"] = True
    with pytest.raises(ConfigurationError):
        WorkbenchConfig.from_dict(raw)
