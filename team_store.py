from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from db import DatabaseError, JobRecord, ProjectRepository, _Repository
from team_config import AgentSystem, ProjectTeam, TeamConfigError


class TeamStoreError(DatabaseError):
    """Project team storage is unavailable or invalid."""


class TeamConflictError(TeamStoreError):
    """Project team revision or execution authority changed."""


class TeamNotFoundError(TeamStoreError):
    """Project team does not exist in the current configuration."""


def encode(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


@dataclass(frozen=True)
class TeamRecord:
    revision: int
    team: ProjectTeam

    def to_dict(self) -> dict[str, object]:
        return {"revision": self.revision, "team": self.team.to_dict()}


class TeamRepository(_Repository):
    error_type = TeamStoreError
    storage_name = "Project team"

    def __init__(self, database, system: AgentSystem, project_ids: frozenset[str]) -> None:
        super().__init__(database)
        self.system = system
        self.project_ids = project_ids

    def seed(self) -> None:
        """Initialize missing teams without overwriting operator edits on restart."""
        with self._write_connection() as connection:
            for team in self.system.teams:
                self._validate(connection, team)
                payload = encode(team.to_dict())
                connection.execute(
                    """INSERT INTO project_teams(project_id, revision, payload, payload_hash)
                    VALUES (?, 1, ?, ?) ON CONFLICT(project_id) DO NOTHING""",
                    (team.project_id, payload, hashlib.sha256(payload.encode()).hexdigest()),
                )

    def get(self, project_id: str) -> TeamRecord:
        with self._connection() as connection:
            return self._get(connection, project_id)

    def _get(self, connection, project_id: str) -> TeamRecord:
        if project_id not in self.project_ids:
            raise TeamNotFoundError("Project team does not exist.")
        row = connection.execute(
            "SELECT revision, payload, payload_hash FROM project_teams WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        if row is None:
            raise TeamNotFoundError("Project team does not exist.")
        try:
            if hashlib.sha256(row["payload"].encode()).hexdigest() != row["payload_hash"]:
                raise ValueError
            team = ProjectTeam.from_dict(json.loads(row["payload"]))
            if (
                team.project_id != project_id
                or type(row["revision"]) is not int
                or row["revision"] < 1
            ):
                raise ValueError
        except (ValueError, TypeError, KeyError) as error:
            raise TeamStoreError("Stored project team is invalid.") from error
        return TeamRecord(row["revision"], team)

    def _validate(self, connection, team: ProjectTeam) -> None:
        if team.project_id not in self.project_ids:
            raise TeamNotFoundError("Project team does not exist.")
        row = connection.execute(
            "SELECT * FROM projects WHERE id = ?", (team.project_id,)
        ).fetchone()
        if row is None:
            raise TeamNotFoundError("Project team does not exist.")
        project = ProjectRepository._to_record(row)
        self.system.validate_project(
            team, project.sensitivity.value, project.cloud_allowed, project.permission_mode.value
        )

    def put(self, project_id: str, payload: object) -> TeamRecord:
        if not isinstance(payload, Mapping) or set(payload) != {"revision", "team"}:
            raise TeamConfigError("Team update fields are invalid.")
        revision = payload["revision"]
        if type(revision) is not int or revision < 0:
            raise TeamConfigError("Team revision must be a nonnegative integer.")
        team = ProjectTeam.from_dict(payload["team"])
        if team.project_id != project_id:
            raise TeamConfigError("Team project does not match its route.")
        with self._write_connection() as connection:
            self._validate(connection, team)
            row = connection.execute(
                "SELECT revision FROM project_teams WHERE project_id = ?", (project_id,)
            ).fetchone()
            observed = 0 if row is None else row["revision"]
            if observed != revision:
                raise TeamConflictError("Project team changed. Reload before saving.")
            encoded = encode(team.to_dict())
            connection.execute(
                """INSERT INTO project_teams(project_id, revision, payload, payload_hash)
                VALUES (?, ?, ?, ?) ON CONFLICT(project_id) DO UPDATE SET
                revision = excluded.revision, payload = excluded.payload,
                payload_hash = excluded.payload_hash,
                updated_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')""",
                (project_id, revision + 1, encoded, hashlib.sha256(encoded.encode()).hexdigest()),
            )
            for attempt in connection.execute(
                """SELECT id, authorization FROM native_attempts
                WHERE held = 1 AND job_id IN (SELECT id FROM jobs WHERE project_id = ?)""",
                (project_id,),
            ).fetchall():
                profile = json.loads(attempt["authorization"])["profile"]["id"]
                if not team.enabled or profile not in team.agent_ids:
                    connection.execute(
                        "UPDATE native_attempts SET cancel_requested = 1 WHERE id = ?",
                        (attempt["id"],),
                    )
        return TeamRecord(revision + 1, team)

    def snapshot(self, project_id: str, agent_id: str) -> dict[str, object]:
        with self._connection() as connection:
            record = self._get(connection, project_id)
            self._validate(connection, record.team)
            binding = self.system.binding(record.team, agent_id, require_available=True)
            if record.team.mode != "manual":
                raise TeamConfigError("Automatic team execution is not connected yet.")
            return {"revision": record.revision, **binding}

    def authorize(self, connection, job: JobRecord) -> dict:
        """Keep frozen authority but honor current revocation before native admission."""
        frozen = job.request_snapshot.get("agent_binding")
        if not isinstance(frozen, dict) or set(frozen) != {
            "revision",
            "team",
            "profile",
            "model",
            "connection",
        }:
            raise TeamConflictError("Job has no frozen agent authorization.")
        record = self._get(connection, job.project_id)
        self._validate(connection, record.team)
        agent_id = frozen["profile"].get("id")
        current = self.system.binding(record.team, agent_id, require_available=True)
        if any(current[key] != frozen[key] for key in ("profile", "model", "connection")):
            raise TeamConflictError("Agent source or policy changed after submission.")
        project = connection.execute(
            "SELECT * FROM projects WHERE id = ?", (job.project_id,)
        ).fetchone()
        project = ProjectRepository._to_record(project)
        expected = {
            "id": project.id,
            "root": project.root,
            "sensitivity": project.sensitivity.value,
            "cloud_allowed": project.cloud_allowed,
            "permission_mode": project.permission_mode.value,
        }
        if job.request_snapshot.get("project") != expected:
            raise TeamConflictError("Project policy changed after submission.")
        return frozen
