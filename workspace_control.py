"""Safe project projections and deliberately limited team controls."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from textwrap import shorten

from db import ProjectRepository
from native_execution import NativeExecutionService
from team_config import TeamConfigError
from team_store import TeamConflictError, TeamNotFoundError, TeamStoreError


class WorkspaceControl:
    def __init__(self, state) -> None:
        self.state = state
        self.teams = state.team_repository

    def _project(self, identity):
        if identity not in self.teams.project_ids:
            raise TeamNotFoundError("Project does not exist.")
        return ProjectRepository(self.teams.database).get(identity)

    def settings(self, project_id: str) -> dict:
        project = self._project(project_id)
        system = self.teams.system
        try:
            record = self.teams.get(project_id)
        except TeamNotFoundError:
            return {"project_id": project_id, "team": None, "profiles": []}
        profiles = []
        for profile in system.agents:
            candidate = replace(record.team, agent_ids=(profile.id,), lead_id=None, mode="manual")
            eligible, reason = True, "Available in the configured library."
            try:
                system.validate_project(
                    candidate,
                    project.sensitivity.value,
                    project.cloud_allowed,
                    project.permission_mode.value,
                )
            except TeamConfigError:
                eligible, reason = False, "Project policy prohibits this profile."
            binding = system.binding(candidate, profile.id)
            executable = False
            if eligible:
                try:
                    NativeExecutionService._require_supported(
                        system.binding(candidate, profile.id, require_available=True)
                    )
                    executable = (
                        record.team.enabled
                        and record.team.mode == "manual"
                        and system.native_execution_enabled
                    )
                    reason = "Native Codex supported; authentication is not checked here."
                    if record.team.mode != "manual":
                        reason = "Auto Team execution is not connected."
                except (TeamConfigError, RuntimeError):
                    reason = "Execution is unavailable for this source, role or configuration."
            if not eligible and profile.id not in record.team.agent_ids:
                continue
            source, model = binding["connection"], binding["model"]
            profiles.append(
                {
                    "id": profile.id,
                    "role": profile.role,
                    "model_id": model["id"],
                    "model": model["model"],
                    "connection_id": source["id"],
                    "runtime": source["kind"],
                    "connection_enabled": source["enabled"],
                    "model_verified": model["verified"],
                    "eligible": eligible,
                    "executable": executable,
                    "reason": reason,
                }
            )
        known = {profile.id for profile in system.agents}
        for identity in record.team.agent_ids:
            if identity not in known:
                profiles.append(
                    {
                        "id": identity,
                        "role": "unavailable",
                        "model_id": "",
                        "model": None,
                        "connection_id": "",
                        "runtime": "unavailable",
                        "connection_enabled": False,
                        "model_verified": False,
                        "eligible": False,
                        "executable": False,
                        "reason": "Profile removed. Remove this member to replace it.",
                    }
                )
        return {"project_id": project_id, **record.to_dict(), "profiles": profiles}

    def save_members(self, project_id: str, payload: object) -> dict:
        self._project(project_id)
        if not isinstance(payload, Mapping) or set(payload) != {"revision", "agent_ids", "enabled"}:
            raise TeamConfigError("Team controls accept only revision, members and enablement.")
        record = self.teams.get(project_id)
        if type(payload["revision"]) is not int or payload["revision"] != record.revision:
            raise TeamConflictError("Project team changed. Reload before saving.")
        body = record.to_dict()
        body["revision"] = payload["revision"]
        body["team"].update(agent_ids=payload["agent_ids"], enabled=payload["enabled"])
        self.teams.put(project_id, body)
        return self.settings(project_id)

    def activity(self, project_id: str) -> dict:
        self._project(project_id)
        attention_states = (
            "waiting_input",
            "waiting_approval",
            "review_ready",
            "blocked",
            "failed",
        )
        placeholders = ",".join("?" for _ in attention_states)
        with self.teams._connection() as connection:
            connection.execute("BEGIN")
            attention_total = connection.execute(
                f"SELECT COUNT(*) FROM jobs WHERE project_id=? AND state IN ({placeholders})",
                (project_id, *attention_states),
            ).fetchone()[0]
            attention = connection.execute(
                f"SELECT id, request, state, updated_at FROM jobs WHERE project_id=? "
                f"AND state IN ({placeholders}) ORDER BY updated_at DESC, id DESC LIMIT 50",
                (project_id, *attention_states),
            ).fetchall()
            total = connection.execute(
                "SELECT COUNT(*) FROM native_attempts a JOIN jobs j ON j.id=a.job_id "
                "WHERE j.project_id=?",
                (project_id,),
            ).fetchone()[0]
            attempts = connection.execute(
                """SELECT a.id, a.job_id, a.authorization, a.phase, a.cancel_requested,
                a.outcome, a.created_at, j.request,
                COALESCE((SELECT MAX(e.created_at) FROM events e WHERE e.job_id=j.id),
                    a.created_at) AS last_event_at
                FROM native_attempts a JOIN jobs j ON j.id=a.job_id
                WHERE j.project_id=? ORDER BY a.held DESC, a.created_at DESC, a.id DESC LIMIT 50""",
                (project_id,),
            ).fetchall()
        rows = []
        for row in attempts:
            try:
                binding = json.loads(row["authorization"])
                profile_id, role = binding["profile"]["id"], binding["profile"]["role"]
                runtime, model = binding["connection"]["kind"], binding["model"]["model"]
            except (ValueError, KeyError, TypeError) as error:
                raise TeamStoreError("Stored attempt identity is unreadable.") from error
            rows.append(
                {
                    "attempt_id": row["id"],
                    "job_id": row["job_id"],
                    "project_id": project_id,
                    "title": shorten(row["request"], width=100, placeholder="…"),
                    "profile_id": profile_id,
                    "role": role,
                    "runtime": runtime,
                    "model": model,
                    "requires_reconciliation": row["phase"] != "settled"
                    and self.state.native_execution_service.read(row["job_id"])[
                        "requires_reconciliation"
                    ],
                    "phase": row["phase"],
                    "cancel_requested": bool(row["cancel_requested"]),
                    "outcome": row["outcome"],
                    "created_at": row["created_at"],
                    "last_event_at": row["last_event_at"],
                }
            )
        return {
            "project_id": project_id,
            "observed_at": datetime.now(UTC).isoformat(),
            "basis": "recorded_native_attempts",
            "attempts": rows,
            "attempt_total": total,
            "attempts_truncated": total > len(rows),
            "attention_total": attention_total,
            "attention_truncated": attention_total > len(attention),
            "attention": [
                {
                    "job_id": row["id"],
                    "project_id": project_id,
                    "state": row["state"],
                    "title": shorten(row["request"], width=100, placeholder="…"),
                    "updated_at": row["updated_at"],
                }
                for row in attention
            ],
        }
