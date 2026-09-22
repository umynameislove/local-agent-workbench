from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from app import create_app, main
from db import (
    LATEST_SCHEMA_VERSION,
    ApprovalRepository,
    AtomicTransitionService,
    Database,
    EventRepository,
    JobRepository,
    MemoryReferenceRepository,
    PlannerRepository,
    ProjectRepository,
    RecoveryService,
    UsageRepository,
    VerificationRepository,
)
from engine import (
    CONSULTANT_MODEL,
    ConfigurationError,
    WorkbenchConfig,
    resolve_runtime_home,
    validate_consultant_advice,
)
from event_stream import EventStreamService
from secret_gate import PreReviewSecretGate
from verification import VerificationRunner
from worktree import WorktreeManager


def test_runtime_home_resolves_relative_path_and_bootstraps_outside_source(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    runtime = resolve_runtime_home({"AGENT_WORKBENCH_HOME": "../runtime"}, cwd=source)
    runtime.bootstrap()

    assert runtime.root == tmp_path / "runtime"
    assert {item.name for item in runtime.root.iterdir()} == {"logs", "cache", "worktrees"}
    assert list(source.iterdir()) == []


def test_runtime_home_uses_platform_location_when_env_is_missing(tmp_path: Path) -> None:
    runtime = resolve_runtime_home({}, user_home=tmp_path)

    assert runtime.root.is_absolute()
    assert not runtime.root.is_relative_to(Path.cwd())


def test_public_example_config_is_valid() -> None:
    config = WorkbenchConfig.load(Path("config.example.json"))

    assert config.version == 1
    assert config.projects[0].cloud_allowed is False
    assert config.consultant.model == CONSULTANT_MODEL
    assert config.consultant.enabled is False


def test_restricted_project_cannot_enable_cloud() -> None:
    with pytest.raises(ConfigurationError, match="cloud disabled"):
        WorkbenchConfig.from_dict(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "private",
                        "root": "./repo",
                        "sensitivity": "restricted",
                        "cloud_allowed": True,
                    }
                ],
            }
        )


def test_consultant_advice_rejects_unknown_references_and_low_confidence() -> None:
    advice = {
        "recommendation": "codex",
        "task_ids": ["TASK-404"],
        "evidence_ids": ["MEM-001"],
        "assumptions": [],
        "unknowns": [],
        "confidence": 0.7,
    }
    with pytest.raises(ConfigurationError):
        validate_consultant_advice(
            advice,
            known_task_ids={"TASK-001"},
            known_evidence_ids={"MEM-001"},
        )


@pytest.mark.anyio
async def test_health_and_bootstrap_contracts(tmp_path: Path) -> None:
    api = create_app({"AGENT_WORKBENCH_HOME": str(tmp_path / "runtime")})

    async with api.router.lifespan_context(api):
        assert isinstance(api.state.project_repository, ProjectRepository)
        assert isinstance(api.state.job_repository, JobRepository)
        assert isinstance(api.state.event_repository, EventRepository)
        assert isinstance(api.state.event_stream_service, EventStreamService)
        assert isinstance(api.state.approval_repository, ApprovalRepository)
        assert isinstance(api.state.planner_repository, PlannerRepository)
        assert isinstance(api.state.usage_repository, UsageRepository)
        assert isinstance(api.state.memory_reference_repository, MemoryReferenceRepository)
        assert isinstance(api.state.verification_repository, VerificationRepository)
        assert isinstance(api.state.atomic_transition_service, AtomicTransitionService)
        assert isinstance(api.state.secret_gate, PreReviewSecretGate)
        assert isinstance(api.state.worktree_manager, WorktreeManager)
        assert isinstance(api.state.verification_runner, VerificationRunner)
        assert isinstance(api.state.recovery_service, RecoveryService)
        assert api.state.recovery_items == ()
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/api/health")
            bootstrap = await client.get("/api/bootstrap")

    assert health.status_code == 200
    assert health.json()["runtime_home_configured"] is True
    assert health.json()["storage"] == {
        "root_ready": True,
        "logs_ready": True,
        "cache_ready": True,
        "worktrees_ready": True,
    }
    assert health.json()["database"] == {
        "status": "ready",
        "schema_version": LATEST_SCHEMA_VERSION,
    }
    assert (tmp_path / "runtime" / "state.db").is_file()
    assert bootstrap.json()["runtimes"] == ["auto", "claude", "codex", "local"]
    assert bootstrap.json()["consultant"]["can_execute"] is False


def test_example_contains_no_secret_fields() -> None:
    config = json.loads(Path("config.example.json").read_text(encoding="utf-8"))
    serialized = json.dumps(config).lower()

    assert "api_key" not in serialized
    assert "token" not in serialized


def test_string_boolean_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="boolean"):
        WorkbenchConfig.from_dict(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "sample",
                        "root": "./repo",
                        "cloud_allowed": "false",
                    }
                ],
            }
        )


def test_backup_command_creates_a_verified_snapshot(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    Database(runtime / "state.db").initialize()
    destination = tmp_path / "snapshot.db"
    result = subprocess.run(
        [sys.executable, "app.py", "backup", str(destination)],
        env={**os.environ, "AGENT_WORKBENCH_HOME": str(runtime)},
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "Database backup created and verified.\n"
    assert result.stderr == ""
    with sqlite3.connect(destination) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("SELECT version FROM schema_version").fetchone() == (
            LATEST_SCHEMA_VERSION,
        )


def test_backup_command_does_not_bootstrap_missing_source(tmp_path: Path) -> None:
    runtime = tmp_path / "missing-runtime"
    destination = tmp_path / "snapshot.db"
    result = subprocess.run(
        [sys.executable, "app.py", "backup", str(destination)],
        env={**os.environ, "AGENT_WORKBENCH_HOME": str(runtime)},
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "Backup failed" in result.stderr
    assert str(tmp_path) not in result.stderr
    assert not runtime.exists()
    assert not destination.exists()


def test_default_command_still_starts_local_server(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import Mock

    start_server = Mock()
    monkeypatch.setattr("uvicorn.run", start_server)

    main([])

    start_server.assert_called_once_with("app:app", host="127.0.0.1", port=8765, reload=False)
