from __future__ import annotations

import copy
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from test_job_submission import initialize_repository
from test_team_config import team_configuration

from local_agent_workbench.app import create_app
from local_agent_workbench.db import LATEST_SCHEMA_VERSION, MIGRATIONS, Database, ProjectRepository
from local_agent_workbench.engine import WorkbenchConfig
from local_agent_workbench.team_config import TeamConfigError
from local_agent_workbench.team_store import (
    TeamConflictError,
    TeamNotFoundError,
    TeamRepository,
    TeamStoreError,
)


def team_repository(tmp_path):
    configuration = WorkbenchConfig.from_dict(team_configuration())
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(configuration.projects[0])
    teams = TeamRepository(database, configuration.agent_system, frozenset({"sample-project"}))
    teams.seed()
    return teams


def test_team_updates_survive_seeding_and_database_restart(tmp_path):
    teams = team_repository(tmp_path)
    initial = teams.get("sample-project")
    change = initial.to_dict()
    change["team"]["enabled"] = False
    updated = teams.put("sample-project", change)
    teams.seed()
    restarted = TeamRepository(Database(teams.database.path), teams.system, teams.project_ids)
    assert restarted.get("sample-project") == updated
    assert initial.revision == 1 and updated.revision == 2
    assert not updated.team.enabled
    with pytest.raises(TeamConflictError):
        teams.put("sample-project", change)


def test_two_database_connections_cas_exactly_one_update(tmp_path):
    teams = team_repository(tmp_path)
    body = teams.get("sample-project").to_dict()

    def update(_):
        other = TeamRepository(Database(teams.database.path), teams.system, teams.project_ids)
        try:
            return other.put("sample-project", body).revision
        except TeamConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert set(executor.map(update, range(2))) == {2, "conflict"}


@pytest.mark.parametrize("mutation", ["cross-project", "unknown-profile", "extra", "revision"])
def test_invalid_team_write_is_atomic(tmp_path, mutation):
    teams = team_repository(tmp_path)
    body = teams.get("sample-project").to_dict()
    original = copy.deepcopy(body)
    if mutation == "cross-project":
        body["team"]["project_id"] = "other"
    elif mutation == "unknown-profile":
        body["team"]["agent_ids"] = ["unknown"]
    elif mutation == "extra":
        body["secret"] = "not-a-credential"
    else:
        body["revision"] = True
    with pytest.raises((TeamConfigError, TeamNotFoundError)):
        teams.put("sample-project", body)
    assert teams.get("sample-project").to_dict() == original


def test_corruption_is_detected_and_hidden_project_is_not_readable(tmp_path):
    teams = team_repository(tmp_path)
    with pytest.raises(TeamNotFoundError):
        teams.get("other")
    with sqlite3.connect(teams.database.path) as connection:
        connection.execute("UPDATE project_teams SET payload_hash = ?", ("0" * 64,))
    with pytest.raises(TeamStoreError, match="invalid"):
        teams.get("sample-project")


def test_schema_upgrade_preserves_old_projects_and_rolls_back_failed_migration(tmp_path):
    database = Database(tmp_path / "state.db", migrations=MIGRATIONS[:12])
    assert database.initialize() == 12
    old = WorkbenchConfig.from_dict(team_configuration()).projects[0]
    ProjectRepository(database).create(old)
    latest = Database(database.path)
    assert latest.initialize() == LATEST_SCHEMA_VERSION
    assert ProjectRepository(latest).get(old.id).to_config() == old


@pytest.mark.anyio
async def test_api_team_scope_snapshot_and_restart(tmp_path):
    project = initialize_repository(tmp_path / "project")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    config = team_configuration()
    config["projects"][0]["root"] = str(project)
    (runtime / "config.json").write_text(json.dumps(config))
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime)}, job_id_factory=lambda: "job-team")
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client,
    ):
        team = await client.get("/api/projects/sample-project/team")
        assert team.status_code == 200
        assert str(project) not in team.text and "account_slot" not in team.text
        response = await client.post(
            "/api/jobs",
            json={
                "project_id": "sample-project",
                "request": "Read this fixture",
                "agent_id": "coder",
            },
        )
        assert response.status_code == 201
        job = api.state.job_repository.get("job-team")
        frozen = copy.deepcopy(job.request_snapshot)
        assert frozen["agent_binding"]["revision"] == 1
        assert job.runtime.value == "codex" and job.model == "test-model"
        update = team.json()
        update["team"]["enabled"] = False
        saved = await client.put("/api/projects/sample-project/team", json=update)
        assert saved.status_code == 200
        assert (
            await client.put("/api/projects/sample-project/team", json=update)
        ).status_code == 409
        assert (await client.get("/api/projects/missing/team")).status_code == 404
        assert (
            await client.put(
                "/api/projects/sample-project/team",
                json=update,
                headers={"Origin": "https://example.invalid"},
            )
        ).status_code == 403
        assert api.state.job_repository.get(job.id).request_snapshot == frozen
        with (
            api.state.team_repository._connection() as connection,
            pytest.raises(TeamConfigError),
        ):
            api.state.team_repository.authorize(connection, job)
    async with api.router.lifespan_context(api):
        assert api.state.team_repository.get("sample-project").revision == 2
        assert not api.state.team_repository.get("sample-project").team.enabled
