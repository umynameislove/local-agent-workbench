from __future__ import annotations

import hashlib
from collections.abc import Mapping

from .db import EventRepository, JobRepository
from .diff_service import ReadOnlyDiffService
from .engine import EventCreate, JobState, VerificationOutcome
from .native_execution import NativeExecutionService
from .team_config import TeamConfigError
from .team_store import TeamStoreError, encode
from .verification import VerificationCommand


class WorkflowConflictError(RuntimeError):
    """A workflow action needs fresh state or explicit reconciliation."""


class TaskWorkflow:
    """Expose explicit task actions without accepting browser supplied commands."""

    def __init__(self, app, commands) -> None:
        self.app, self.commands = app, commands

    def agents(self, project_id: str) -> dict:
        teams = self.app.state.team_repository
        record = teams.get(project_id)
        profiles = []
        for agent_id in record.team.agent_ids:
            try:
                binding = teams.snapshot(project_id, agent_id)
                NativeExecutionService._require_supported(binding)
            except (TeamConfigError, TeamStoreError, RuntimeError):
                continue
            profiles.append(
                {
                    "id": agent_id,
                    "role": binding["profile"]["role"],
                    "runtime": binding["connection"]["kind"],
                    "model": binding["model"]["model"],
                    "write_paths": binding["profile"]["allowed_write_paths"],
                }
            )
        return {"project_id": project_id, "revision": record.revision, "agents": profiles}

    def _job(self, job_id):
        self.app.state.native_execution_service.read(job_id)
        return self.app.state.job_repository.get(job_id)

    def _key(self, job) -> str:
        return hashlib.sha256(
            encode(
                {
                    "job_id": job.id,
                    "snapshot": job.request_snapshot,
                    "command": self.commands.get(job.project_id),
                }
            ).encode()
        ).hexdigest()

    def _checks(self, job):
        key = self._key(job)
        events = [
            e
            for e in self.app.state.event_repository.list(job.id)
            if e.payload.get("authorization_hash") == key
        ]
        return (
            next((e for e in events if e.event_type == "workflow.check.started"), None),
            next((e for e in events if e.event_type == "workflow.check.finished"), None),
        )

    def read(self, job_id: str) -> dict:
        job = self._job(job_id)
        binding = job.request_snapshot.get("agent_binding")
        profile = (
            None
            if not binding
            else {
                "id": binding["profile"]["id"],
                "role": binding["profile"]["role"],
                "runtime": binding["connection"]["kind"],
                "model": binding["model"]["model"],
                "write_paths": binding["profile"]["allowed_write_paths"],
            }
        )
        started, finished = self._checks(job)
        return {
            "job_id": job.id,
            "project_id": job.project_id,
            "job_state": job.state.value,
            "agent": profile,
            "execution": self.app.state.native_execution_service.read(job_id),
            "verification": {
                "authorization_hash": self._key(job),
                "command": list(self.commands.get(job.project_id, ())),
                "status": finished.payload["status"]
                if finished
                else "uncertain"
                if started
                else "not_started",
                "result_id": None if finished is None else finished.id,
            },
        }

    async def _fingerprint(self, job) -> str:
        path = await self.app.state.worktree_manager.verify_bound(job)
        diff = await ReadOnlyDiffService(path, job.request_snapshot["repo_head"]).collect()
        if not diff.files or diff.truncated or any(f.patch is None for f in diff.files):
            raise WorkflowConflictError("Verification requires a complete changed text inventory.")
        return hashlib.sha256(encode(diff.to_dict()).encode()).hexdigest()

    async def require_verified(self, job_id: str) -> str | None:
        if self.app.state.native_attempt_repository.get(job_id) is None:
            return
        job = self._job(job_id)
        started, finished = self._checks(job)
        if (
            not started
            or not finished
            or finished.payload.get("status") != "passed"
            or started.payload.get("authorization_hash") != self._key(job)
            or not isinstance(finished.payload.get("diff_hash"), str)
        ):
            raise WorkflowConflictError("Current worktree has no matching successful verification.")
        await self.app.state.worktree_manager.verify_bound(job)
        return finished.payload["diff_hash"]

    async def verify(self, job_id: str, body: object) -> dict:
        job = self._job(job_id)
        key = self._key(job)
        if (
            not isinstance(body, Mapping)
            or set(body) != {"authorization_hash"}
            or body["authorization_hash"] != key
        ):
            raise WorkflowConflictError(
                "Verification requires the displayed command authorization."
            )
        command = self.commands.get(job.project_id)
        if not command:
            raise WorkflowConflictError(
                "No trusted verification command is configured for this project."
            )
        if self._checks(job)[0] is not None:
            return self.read(job.id)
        attempt = self.app.state.native_attempt_repository.get(job.id)
        if (
            job.state is not JobState.VERIFYING
            or not attempt
            or attempt.held
            or attempt.outcome != "completed"
        ):
            raise WorkflowConflictError(
                "Native execution must finish successfully before verification."
            )
        before = await self._fingerprint(job)
        repository = self.app.state.event_repository
        event = EventCreate(
            job.id,
            "workflow.check.started",
            {"authorization_hash": key},
            f"workflow-check:{key}",
        )
        payload, digest, identity = EventRepository._validate_event(event)
        with repository._write_connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM events WHERE job_id=? AND idempotency_key=?",
                (job.id, f"workflow-check:{key}"),
            ).fetchone()
            if row:
                return self.read(job.id)
            pending = connection.execute(
                """SELECT 1 FROM events AS started
                WHERE started.job_id=? AND started.event_type='workflow.check.started'
                AND NOT EXISTS (SELECT 1 FROM events AS finished
                    WHERE finished.job_id=started.job_id
                    AND finished.event_type='workflow.check.finished'
                    AND json_extract(finished.payload, '$.authorization_hash') =
                        json_extract(started.payload, '$.authorization_hash'))""",
                (job.id,),
            ).fetchone()
            if pending:
                raise WorkflowConflictError("An earlier verification needs reconciliation.")
            current = JobRepository._to_record(JobRepository._select_by_id(connection, job.id))
            if current != job:
                raise WorkflowConflictError("Task changed before verification.")
            EventRepository._append(connection, event, payload, digest, identity)
        status = "failed"
        try:
            result = await self.app.state.verification_runner.run(
                job.id, VerificationCommand(tuple(command))
            )
            after = await self._fingerprint(self._job(job.id))
            if before != after:
                raise WorkflowConflictError(
                    "Worktree changed while verification ran. Reconcile before review."
                )
            if result.outcome is VerificationOutcome.PASSED:
                status = "passed"
        finally:
            repository.append(
                EventCreate(
                    job.id,
                    "workflow.check.finished",
                    {"status": status, "diff_hash": before, "authorization_hash": key},
                    f"workflow-check-finished:{key}",
                )
            )
        return self.read(job.id)
