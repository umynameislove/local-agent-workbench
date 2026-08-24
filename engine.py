from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

APP_VERSION = "0.1.0"
RUNTIME_ENV = "AGENT_WORKBENCH_HOME"
CONSULTANT_MODEL = "deepseek/deepseek-v4-flash-0731"
CONSULTANT_RECOMMENDATIONS = frozenset({"codex", "claude", "local", "ask_user"})


class ConfigurationError(ValueError):
    """Raised when public or local configuration violates a hard contract."""


class Sensitivity(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    INTERNAL = "internal"
    RESTRICTED = "restricted"


class PermissionMode(StrEnum):
    READ_ONLY = "read-only"
    SANDBOXED_WRITE = "sandboxed-write"
    NEVER = "never"


class JobState(StrEnum):
    CREATED = "created"
    CLASSIFIED = "classified"
    PLANNING = "planning"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    WAITING_APPROVAL = "waiting_approval"
    VERIFYING = "verifying"
    REVIEW_READY = "review_ready"
    APPROVED = "approved"
    REJECTED = "rejected"
    APPLYING = "applying"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class JobRuntime(StrEnum):
    AUTO = "auto"
    CLAUDE = "claude"
    CODEX = "codex"
    LOCAL = "local"


@dataclass(frozen=True)
class RuntimeHome:
    root: Path

    @property
    def state_db(self) -> Path:
        return self.root / "state.db"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def cache(self) -> Path:
        return self.root / "cache"

    @property
    def worktrees(self) -> Path:
        return self.root / "worktrees"

    def bootstrap(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (self.logs, self.cache, self.worktrees):
            directory.mkdir(exist_ok=True)


def _platform_default_home(home: Path | None = None) -> Path:
    user_home = (home or Path.home()).expanduser()
    if sys.platform == "darwin":
        return user_home / "Library" / "Application Support" / "LocalAgentWorkbench"
    data_home = os.environ.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home).expanduser() / "local-agent-workbench"
    return user_home / ".local" / "share" / "local-agent-workbench"


def resolve_runtime_home(
    env: Mapping[str, str] | None = None,
    *,
    cwd: Path | None = None,
    user_home: Path | None = None,
) -> RuntimeHome:
    values = os.environ if env is None else env
    configured = values.get(RUNTIME_ENV, "").strip()
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = (cwd or Path.cwd()) / candidate
    else:
        candidate = _platform_default_home(user_home)
    return RuntimeHome(candidate.resolve())


@dataclass(frozen=True)
class ProjectConfig:
    id: str
    root: str
    sensitivity: Sensitivity
    cloud_allowed: bool
    permission_mode: PermissionMode

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ProjectConfig:
        project_id = str(value.get("id", "")).strip()
        root = str(value.get("root", "")).strip()
        if not project_id or not root:
            raise ConfigurationError("Each project requires an id and a root value.")
        sensitivity = Sensitivity(value.get("sensitivity", Sensitivity.PRIVATE))
        cloud_allowed = value.get("cloud_allowed", False)
        if not isinstance(cloud_allowed, bool):
            raise ConfigurationError("Project cloud_allowed must be a boolean.")
        if sensitivity in {Sensitivity.INTERNAL, Sensitivity.RESTRICTED} and cloud_allowed:
            raise ConfigurationError("Internal and restricted projects default to cloud disabled.")
        return cls(
            id=project_id,
            root=root,
            sensitivity=sensitivity,
            cloud_allowed=cloud_allowed,
            permission_mode=PermissionMode(
                value.get("permission_mode", PermissionMode.SANDBOXED_WRITE)
            ),
        )


@dataclass(frozen=True)
class JobCreate:
    id: str
    project_id: str
    request: str
    request_snapshot: Mapping[str, Any]
    state: JobState = JobState.CREATED
    runtime: JobRuntime = JobRuntime.AUTO
    model: str | None = None
    worktree_path: str | None = None


@dataclass(frozen=True)
class JobUpdate:
    id: str
    state: JobState
    runtime: JobRuntime
    model: str | None = None
    worktree_path: str | None = None


@dataclass(frozen=True)
class ConsultantConfig:
    enabled: bool = False
    model: str = CONSULTANT_MODEL
    min_confidence: float = 0.8
    monthly_hard_cap_usd: float = 5.0
    warning_usd: float = 4.0
    per_job_cap_usd: float = 0.1

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ConsultantConfig:
        enabled = value.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ConfigurationError("Consultant enabled must be a boolean.")
        config = cls(
            enabled=enabled,
            model=str(value.get("model", CONSULTANT_MODEL)),
            min_confidence=float(value.get("min_confidence", 0.8)),
            monthly_hard_cap_usd=float(value.get("monthly_hard_cap_usd", 5.0)),
            warning_usd=float(value.get("warning_usd", 4.0)),
            per_job_cap_usd=float(value.get("per_job_cap_usd", 0.1)),
        )
        if config.model != CONSULTANT_MODEL:
            raise ConfigurationError(f"V1 consultant model must be {CONSULTANT_MODEL}.")
        if not 0.0 <= config.min_confidence <= 1.0:
            raise ConfigurationError("Consultant min_confidence must be between 0 and 1.")
        if not 0.0 <= config.warning_usd <= config.monthly_hard_cap_usd:
            raise ConfigurationError("Consultant warning must not exceed the monthly hard cap.")
        if not 0.0 < config.per_job_cap_usd <= config.monthly_hard_cap_usd:
            raise ConfigurationError("Consultant job cap must be positive and within monthly cap.")
        return config


@dataclass(frozen=True)
class WorkbenchConfig:
    version: int
    projects: tuple[ProjectConfig, ...]
    providers: Mapping[str, bool]
    consultant: ConsultantConfig

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> WorkbenchConfig:
        if not isinstance(value.get("version"), int) or isinstance(value.get("version"), bool):
            raise ConfigurationError("Config version must be an integer.")
        if value.get("version") != 1:
            raise ConfigurationError("Unsupported config version.")
        projects = tuple(ProjectConfig.from_dict(item) for item in value.get("projects", []))
        if not projects:
            raise ConfigurationError("At least one project is required.")
        if len({project.id for project in projects}) != len(projects):
            raise ConfigurationError("Project ids must be unique.")
        providers = value.get("providers", {})
        if not isinstance(providers, Mapping):
            raise ConfigurationError("providers must be an object.")
        normalized_providers: dict[str, bool] = {}
        for key, enabled in providers.items():
            if not isinstance(enabled, bool):
                raise ConfigurationError("Provider flags must be booleans.")
            normalized_providers[str(key)] = enabled
        return cls(
            version=1,
            projects=projects,
            providers=normalized_providers,
            consultant=ConsultantConfig.from_dict(value.get("consultant", {})),
        )

    @classmethod
    def load(cls, path: Path) -> WorkbenchConfig:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, Mapping):
            raise ConfigurationError("Config root must be an object.")
        return cls.from_dict(value)


def validate_consultant_advice(
    value: Mapping[str, Any],
    *,
    known_task_ids: set[str],
    known_evidence_ids: set[str],
    min_confidence: float = 0.8,
) -> dict[str, Any]:
    required = {
        "recommendation",
        "task_ids",
        "evidence_ids",
        "assumptions",
        "unknowns",
        "confidence",
    }
    if set(value) != required:
        raise ConfigurationError("Consultant advice has missing or unknown fields.")
    recommendation = value["recommendation"]
    if recommendation not in CONSULTANT_RECOMMENDATIONS:
        raise ConfigurationError("Consultant recommendation is invalid.")
    for field in ("task_ids", "evidence_ids", "assumptions", "unknowns"):
        if not isinstance(value[field], list) or not all(
            isinstance(item, str) for item in value[field]
        ):
            raise ConfigurationError(f"Consultant {field} must be a string array.")
    if not set(value["task_ids"]).issubset(known_task_ids):
        raise ConfigurationError("Consultant referenced an unknown task id.")
    if not set(value["evidence_ids"]).issubset(known_evidence_ids):
        raise ConfigurationError("Consultant referenced an unknown evidence id.")
    confidence = float(value["confidence"])
    if confidence < min_confidence or confidence > 1.0:
        raise ConfigurationError("Consultant confidence is outside the accepted range.")
    return dict(value)
