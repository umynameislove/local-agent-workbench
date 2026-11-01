from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import uuid4

from .db import DatabaseError, EventRepository, JobRecord, JobRepository, _Repository
from .engine import EventCreate, JobRuntime, JobState, JobUpdate, validate_job_transition
from .team_store import TeamConflictError, TeamRepository, encode


class NativeStoreError(DatabaseError):
    """Native execution state cannot be read or persisted safely."""


class NativeConflictError(NativeStoreError):
    """Execution authority or durable ownership conflicts with the request."""


@dataclass(frozen=True)
class NativeAttempt:
    id: str
    job_id: str
    owner: str
    account_slot: str
    worktree_path: str
    worktree_identity: dict
    authorization: dict
    phase: str
    held: bool
    cancel_requested: bool
    session_id: str | None
    outcome: str | None

    def public(self) -> dict[str, object]:
        return {
            "attempt_id": self.id,
            "job_id": self.job_id,
            "phase": self.phase,
            "cancel_requested": self.cancel_requested,
            "outcome": self.outcome,
        }


class NativeAttemptRepository(_Repository):
    error_type = NativeStoreError
    storage_name = "Native execution"

    @staticmethod
    def _record(row) -> NativeAttempt:
        try:
            return NativeAttempt(
                row["id"],
                row["job_id"],
                row["owner"],
                row["account_slot"],
                row["worktree_path"],
                json.loads(row["worktree_identity"]),
                json.loads(row["authorization"]),
                row["phase"],
                bool(row["held"]),
                bool(row["cancel_requested"]),
                row["session_id"],
                row["outcome"],
            )
        except (TypeError, ValueError, KeyError) as error:
            raise NativeStoreError("Stored native attempt is invalid.") from error

    def get(self, job_id: str) -> NativeAttempt | None:
        with self._connection() as connection:
            return self._get(connection, job_id)

    def _get(self, connection, job_id: str) -> NativeAttempt | None:
        row = connection.execute(
            "SELECT * FROM native_attempts WHERE job_id = ?", (job_id,)
        ).fetchone()
        return None if row is None else self._record(row)

    @staticmethod
    def _event(connection, job_id: str, event_type: str, payload: dict, key: str) -> None:
        event = EventCreate(job_id, event_type, payload, key)
        encoded, digest, identity = EventRepository._validate_event(event)
        EventRepository._append(connection, event, encoded, digest, identity)

    @staticmethod
    def _transition(connection, job: JobRecord, state: JobState) -> None:
        validate_job_transition(job.state, state)
        JobRepository._update(
            connection,
            JobUpdate(
                job.id,
                state,
                job.runtime,
                job.model,
                job.worktree_path,
            ),
        )

    def claim(
        self,
        expected: JobRecord,
        teams: TeamRepository,
        owner: str,
        worktree_identity: dict,
    ) -> tuple[NativeAttempt, bool]:
        with self._write_connection() as connection:
            existing = self._get(connection, expected.id)
            if existing is not None:
                return existing, False
            row = JobRepository._select_by_id(connection, expected.id)
            if row is None or (job := JobRepository._to_record(row)) != expected:
                raise NativeConflictError("Job changed before native admission.")
            if job.state is not JobState.QUEUED or job.runtime is not JobRuntime.CODEX:
                raise NativeConflictError("Only a queued Codex job may start.")
            authorization = teams.authorize(connection, job)
            if connection.execute("SELECT 1 FROM native_attempts WHERE held = 1").fetchone():
                raise NativeConflictError(
                    "The native execution lane is occupied or needs reconciliation."
                )
            attempt_id = f"attempt-{uuid4().hex}"
            connection.execute(
                """INSERT INTO native_attempts
                (id,job_id,owner,account_slot,worktree_path,worktree_identity,authorization,phase,held)
                VALUES (?,?,?,?,?,?,?,'admitted',1)""",
                (
                    attempt_id,
                    job.id,
                    owner,
                    authorization["connection"]["account_slot"],
                    job.worktree_path,
                    encode(worktree_identity),
                    encode(authorization),
                ),
            )
            self._transition(connection, job, JobState.RUNNING)
            self._event(
                connection,
                job.id,
                "native.admitted",
                {"attempt_id": attempt_id},
                f"{attempt_id}:admitted",
            )
            return self._get(connection, job.id), True

    def check(self, attempt: NativeAttempt, teams: TeamRepository) -> NativeAttempt:
        with self._connection() as connection:
            current = self._owned(connection, attempt)
            job = JobRepository._to_record(JobRepository._select_by_id(connection, attempt.job_id))
            if (
                current.phase == "reconcile"
                or current.cancel_requested
                or job.state is not JobState.RUNNING
            ):
                raise NativeConflictError("Native execution is stopped or needs reconciliation.")
            try:
                teams.authorize(connection, job)
            except (TeamConflictError, ValueError) as error:
                raise NativeConflictError("Native execution authority was revoked.") from error
            return current

    def bind(self, attempt: NativeAttempt, session_id: str) -> None:
        if not isinstance(session_id, str) or not session_id or len(session_id) > 256:
            raise NativeConflictError("Native session identity is invalid.")
        with self._write_connection() as connection:
            current = self._owned(connection, attempt)
            if current.session_id not in {None, session_id}:
                raise NativeConflictError("Native session identity changed.")
            connection.execute(
                "UPDATE native_attempts SET session_id = ?, phase = 'running' WHERE id = ?",
                (session_id, attempt.id),
            )
            self._event(
                connection,
                attempt.job_id,
                "native.session.bound",
                {"attempt_id": attempt.id},
                f"{attempt.id}:session",
            )

    def append(self, attempt: NativeAttempt, event) -> None:
        if event.job_id != attempt.job_id or event.runtime is not JobRuntime.CODEX:
            raise NativeConflictError("Native event belongs to another attempt.")
        with self._write_connection() as connection:
            self._owned(connection, attempt)
            payload = dict(event.payload)
            payload.update(attempt_id=attempt.id, runtime=event.runtime.value)
            self._event(
                connection,
                attempt.job_id,
                f"runtime.{event.kind.value}",
                payload,
                f"{attempt.id}:event:{event.sequence}",
            )

    def cancel(
        self, job_id: str, attempt_id: str, *, actor: str | None = None, channel: str = "local"
    ) -> NativeAttempt:
        if actor is not None and (not isinstance(actor, str) or not 1 <= len(actor) <= 200):
            raise NativeConflictError("Cancellation actor is invalid.")
        if channel not in {"local", "telegram"}:
            raise NativeConflictError("Cancellation channel is invalid.")
        with self._write_connection() as connection:
            current = self._get(connection, job_id)
            if current is None or current.id != attempt_id:
                raise NativeConflictError("Native attempt does not match the cancellation.")
            if current.phase != "settled" and not current.cancel_requested:
                connection.execute(
                    "UPDATE native_attempts SET cancel_requested = 1 WHERE id = ?", (attempt_id,)
                )
                self._event(
                    connection,
                    job_id,
                    "native.cancel.requested",
                    {
                        "attempt_id": attempt_id,
                        **({"actor": actor, "channel": channel} if actor else {}),
                    },
                    f"{attempt_id}:cancel",
                )
            return self._get(connection, job_id)

    def finish(self, attempt: NativeAttempt, outcome: str) -> None:
        target = {
            "completed": JobState.VERIFYING,
            "failed": JobState.FAILED,
            "cancelled": JobState.CANCELLED,
        }.get(outcome)
        if target is None:
            raise NativeConflictError("Native outcome is invalid.")
        with self._write_connection() as connection:
            current = self._owned(connection, attempt)
            if current.cancel_requested:
                outcome, target = "cancelled", JobState.CANCELLED
            job = JobRepository._to_record(JobRepository._select_by_id(connection, attempt.job_id))
            self._transition(connection, job, target)
            connection.execute(
                "UPDATE native_attempts SET phase='settled', held=0, outcome=? WHERE id=?",
                (outcome, attempt.id),
            )
            self._event(
                connection,
                job.id,
                "native.settled",
                {"attempt_id": attempt.id, "outcome": outcome, "state": target.value},
                f"{attempt.id}:settled",
            )

    def quarantine(self, attempt: NativeAttempt) -> None:
        with self._write_connection() as connection:
            self._owned(connection, attempt)
            connection.execute(
                "UPDATE native_attempts SET phase='reconcile' WHERE id=?", (attempt.id,)
            )
            self._event(
                connection,
                attempt.job_id,
                "native.reconciliation.required",
                {"attempt_id": attempt.id},
                f"{attempt.id}:reconcile",
            )

    def _owned(self, connection, attempt: NativeAttempt) -> NativeAttempt:
        current = self._get(connection, attempt.job_id)
        if (
            current is None
            or current.id != attempt.id
            or current.owner != attempt.owner
            or not current.held
        ):
            raise NativeConflictError("Native attempt ownership is unavailable.")
        return current
