from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any

_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,191}\Z")
_KINDS = {"codex", "claude", "local", "openrouter"}
_ROLES = {"lead", "reader", "implementer", "reviewer", "local_assistant", "advice"}
_TOOLS = {"native_workspace", "document.search", "document.read"}
_CAPABILITIES = {"plan", "tools", "files", "streaming", "structured_output"}


class TeamConfigError(ValueError):
    """Configuration does not describe a bounded project team."""


def _object(value: object, fields: set[str], required: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) - fields or required - set(value):
        raise TeamConfigError("Team configuration fields are invalid.")
    return value


def _id(value: object) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise TeamConfigError("Team configuration identity is invalid.")
    return value


def _bool(value: object) -> bool:
    if type(value) is not bool:
        raise TeamConfigError("Team configuration flag must be a boolean.")
    return value


def _integer(value: object, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise TeamConfigError("Team configuration limit is invalid.")
    return value


def _money(value: object) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise TeamConfigError("Team budget must be finite and positive.")
    return float(value)


def _items(value: object) -> tuple:
    if not isinstance(value, list) or len(value) > 128:
        raise TeamConfigError("Team configuration list is invalid.")
    return tuple(value)


def _ids(value: object) -> tuple[str, ...]:
    result = tuple(_id(item) for item in _items(value))
    if len(set(result)) != len(result):
        raise TeamConfigError("Team configuration identities must be unique.")
    return result


def _paths(value: object) -> tuple[str, ...]:
    paths = tuple("." if item == "." else _id(item) for item in _items(value))
    if len(set(paths)) != len(paths):
        raise TeamConfigError("Agent paths must be unique.")
    for path in paths:
        parts = PurePosixPath(path).parts
        if (
            path.startswith("/")
            or ".." in parts
            or any(part in {".git", ".codex", ".agents"} for part in parts)
        ):
            raise TeamConfigError("Agent paths must remain inside the project.")
        if path != "." and (not parts or PurePosixPath(path).as_posix() != path):
            raise TeamConfigError("Agent paths must be canonical relative paths.")
    return paths


@dataclass(frozen=True)
class Connection:
    id: str
    kind: str
    enabled: bool = False
    account_slot: str | None = None
    credential_ref: str | None = None

    @classmethod
    def from_dict(cls, value: object) -> Connection:
        data = _object(value, set(cls.__dataclass_fields__), {"id", "kind"})
        identity = _id(data["id"])
        kind = data["kind"]
        if not isinstance(kind, str) or kind not in _KINDS:
            raise TeamConfigError("Connection kind is unsupported.")
        slot = data.get("account_slot")
        if slot is not None and (
            not isinstance(slot, str) or re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", slot) is None
        ):
            raise TeamConfigError("Native account slot is invalid.")
        ref = data.get("credential_ref")
        if ref is not None and (
            not isinstance(ref, str)
            or not ref.startswith("keychain:")
            or _ID.fullmatch(ref) is None
        ):
            raise TeamConfigError("Only a Keychain reference may identify a credential.")
        if (slot is not None and kind not in {"codex", "claude"}) or (
            ref is not None and kind != "openrouter"
        ):
            raise TeamConfigError("Connection credential type does not match its source.")
        return cls(identity, kind, _bool(data.get("enabled", False)), slot, ref)


@dataclass(frozen=True)
class Model:
    id: str
    connection_id: str
    model: str | None = None
    verified: bool = False
    capabilities: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: object) -> Model:
        data = _object(value, set(cls.__dataclass_fields__), {"id", "connection_id"})
        capabilities = _ids(data.get("capabilities", []))
        if set(capabilities) - _CAPABILITIES:
            raise TeamConfigError("Model capability is unsupported.")
        model = data.get("model")
        return cls(
            _id(data["id"]),
            _id(data["connection_id"]),
            None if model is None else _id(model),
            _bool(data.get("verified", False)),
            capabilities,
        )


@dataclass(frozen=True)
class AgentProfile:
    id: str
    role: str
    model_id: str
    tools: tuple[str, ...] = ()
    allowed_write_paths: tuple[str, ...] = ()
    document_paths: tuple[str, ...] = ()
    max_steps: int | None = None
    max_output_tokens: int | None = None

    @classmethod
    def from_dict(cls, value: object) -> AgentProfile:
        data = _object(value, set(cls.__dataclass_fields__), {"id", "role", "model_id"})
        role = data["role"]
        tools = _ids(data.get("tools", []))
        if not isinstance(role, str) or role not in _ROLES or set(tools) - _TOOLS:
            raise TeamConfigError("Agent role or tools are unsupported.")
        if (role == "advice" and tools) or (
            role == "reader" and set(tools) - {"document.search", "document.read"}
        ):
            raise TeamConfigError("Agent tools exceed the role boundary.")
        writes = _paths(data.get("allowed_write_paths", []))
        if writes and (role != "implementer" or "native_workspace" not in tools):
            raise TeamConfigError("Only a native implementer may request workspace writes.")
        return cls(
            _id(data["id"]),
            role,
            _id(data["model_id"]),
            tools,
            writes,
            _paths(data.get("document_paths", [])),
            None if data.get("max_steps") is None else _integer(data["max_steps"], 128),
            None
            if data.get("max_output_tokens") is None
            else _integer(data["max_output_tokens"], 131072),
        )


@dataclass(frozen=True)
class ProjectTeam:
    project_id: str
    agent_ids: tuple[str, ...]
    mode: str = "manual"
    lead_id: str | None = None
    enabled: bool = True
    paid_api_enabled: bool = False
    max_active: int = 1
    per_task_cap_usd: float = 0.1

    @classmethod
    def from_dict(cls, value: object) -> ProjectTeam:
        data = _object(value, set(cls.__dataclass_fields__), {"project_id", "agent_ids"})
        agents = _ids(data["agent_ids"])
        mode = data.get("mode", "manual")
        if not agents or not isinstance(mode, str) or mode not in {"manual", "auto"}:
            raise TeamConfigError("Project team or mode is invalid.")
        lead = data.get("lead_id")
        if lead is not None:
            lead = _id(lead)
            if lead not in agents:
                raise TeamConfigError("Team Lead must belong to its project team.")
        if data.get("mode") == "auto" and lead is None:
            raise TeamConfigError("An automatic team requires a Lead.")
        return cls(
            _id(data["project_id"]),
            agents,
            data.get("mode", "manual"),
            lead,
            _bool(data.get("enabled", True)),
            _bool(data.get("paid_api_enabled", False)),
            _integer(data.get("max_active", 1), 32),
            _money(data.get("per_task_cap_usd", 0.1)),
        )

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["agent_ids"] = list(self.agent_ids)
        return result


@dataclass(frozen=True)
class AgentSystem:
    version: int = 1
    connections: tuple[Connection, ...] = ()
    models: tuple[Model, ...] = ()
    agents: tuple[AgentProfile, ...] = ()
    teams: tuple[ProjectTeam, ...] = ()
    native_execution_enabled: bool = False
    max_active_agents: int = 4
    max_heavy_local: int = 1
    monthly_cap_usd: float = 5.0
    execution_timeout_seconds: int = 300

    @classmethod
    def from_dict(cls, value: object) -> AgentSystem:
        data = _object(value, set(cls.__dataclass_fields__), set())
        if type(data.get("version", 1)) is not int or data.get("version", 1) != 1:
            raise TeamConfigError("Agent schema version is unsupported.")
        groups = []
        for name, parser in (
            ("connections", Connection),
            ("models", Model),
            ("agents", AgentProfile),
            ("teams", ProjectTeam),
        ):
            group = tuple(parser.from_dict(item) for item in _items(data.get(name, [])))
            identities = [item.project_id if name == "teams" else item.id for item in group]
            if len(set(identities)) != len(identities):
                raise TeamConfigError("Agent configuration contains duplicate identities.")
            groups.append(group)
        system = cls(
            1,
            *groups,
            _bool(data.get("native_execution_enabled", False)),
            _integer(data.get("max_active_agents", 4), 32),
            _integer(data.get("max_heavy_local", 1), 1),
            _money(data.get("monthly_cap_usd", 5.0)),
            _integer(data.get("execution_timeout_seconds", 300), 3600),
        )
        system._validate_references()
        return system

    def _validate_references(self) -> None:
        connections = {item.id: item for item in self.connections}
        models = {item.id: item for item in self.models}
        for model in self.models:
            if model.connection_id not in connections:
                raise TeamConfigError("Model connection does not exist.")
            if connections[model.connection_id].kind == "openrouter" and model.model is None:
                raise TeamConfigError("An API model requires an exact model identity.")
        for agent in self.agents:
            if agent.model_id not in models:
                raise TeamConfigError("Agent model does not exist.")
            model = models[agent.model_id]
            connection = connections[model.connection_id]
            if "native_workspace" in agent.tools and connection.kind not in {"codex", "claude"}:
                raise TeamConfigError("API models cannot inherit a native harness.")
            if agent.role == "local_assistant" and connection.kind != "local":
                raise TeamConfigError("Local assistant must use a local connection.")
        for team in self.teams:
            self.validate_team(team)

    def validate_team(self, team: ProjectTeam) -> None:
        agents = {item.id: item for item in self.agents}
        if set(team.agent_ids) - set(agents) or team.max_active > self.max_active_agents:
            raise TeamConfigError("Project team references or execution limit are invalid.")
        if team.per_task_cap_usd > self.monthly_cap_usd:
            raise TeamConfigError("Task cap exceeds the application API cap.")
        if team.lead_id is not None and agents[team.lead_id].role != "lead":
            raise TeamConfigError("Selected Lead has the wrong role.")

    def validate_project(
        self, team: ProjectTeam, sensitivity: str, cloud_allowed: bool, permission_mode: str
    ) -> None:
        self.validate_team(team)
        for identity in team.agent_ids:
            binding = self.binding(team, identity)
            if binding["connection"]["kind"] != "local" and (
                sensitivity in {"internal", "restricted"} or not cloud_allowed
            ):
                raise TeamConfigError("Project policy prohibits this cloud profile.")
            if binding["profile"]["allowed_write_paths"] and permission_mode != "sandboxed-write":
                raise TeamConfigError("Project policy prohibits this writable profile.")

    def binding(
        self, team: ProjectTeam, agent_id: str, *, require_available: bool = False
    ) -> dict[str, Any]:
        if agent_id not in team.agent_ids:
            raise TeamConfigError("Agent does not belong to this project team.")
        profile = next((item for item in self.agents if item.id == agent_id), None)
        if profile is None:
            raise TeamConfigError("Agent profile does not exist.")
        model = next(item for item in self.models if item.id == profile.model_id)
        connection = next(item for item in self.connections if item.id == model.connection_id)
        if require_available and (
            not team.enabled
            or not connection.enabled
            or (profile.role != "advice" and not model.verified)
        ):
            raise TeamConfigError("Selected source is disabled or its capability is unverified.")
        if require_available and connection.kind == "openrouter" and not team.paid_api_enabled:
            raise TeamConfigError("Paid API use is disabled for this project.")
        import json

        return json.loads(
            json.dumps(
                {
                    "team": team.to_dict(),
                    "profile": asdict(profile),
                    "model": asdict(model),
                    "connection": asdict(connection),
                }
            )
        )

    def to_dict(self) -> dict[str, Any]:
        import json

        return json.loads(json.dumps(asdict(self)))


def migrate_config(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return an explicit version two copy without activating legacy access."""
    import copy

    if type(value.get("version")) is not int or value["version"] != 1:
        raise TeamConfigError("Only a version one configuration may be migrated.")
    result = copy.deepcopy(dict(value))
    if "agent_system" in result:
        raise TeamConfigError("Legacy configuration cannot contain agent settings.")
    result["version"] = 2
    result["agent_system"] = AgentSystem().to_dict()
    if "consultant" in result:
        result["consultant"]["enabled"] = False
    return result
