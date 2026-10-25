from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from codex_accounts import CodexAccountPool, codex_account_slots
from codex_adapter import CodexAdapter
from db import JobNotFoundError, JobRepository, ProjectRepository
from engine import AdapterStart, JobRuntime, ProviderCapability, RuntimeEventKind
from native_store import (
    NativeAttempt,
    NativeAttemptRepository,
    NativeConflictError,
    NativeStoreError,
)
from team_config import AgentSystem, TeamConfigError
from team_store import TeamRepository, TeamStoreError, encode
from worktree import WorktreeManager, worktree_identity


class NativeExecutionService:
    """Own one durable native writer without automatic restart or account reassignment."""

    def __init__(
        self,
        jobs: JobRepository,
        projects: ProjectRepository,
        teams: TeamRepository,
        attempts: NativeAttemptRepository,
        worktrees: WorktreeManager,
        runtime_root: Path,
        *,
        adapter_factory: Callable | None = None,
    ) -> None:
        self.jobs, self.projects, self.teams = jobs, projects, teams
        self.attempts, self.worktrees, self.runtime_root = attempts, worktrees, runtime_root
        self.system: AgentSystem = teams.system
        self._factory = adapter_factory or self._adapter
        self._owner = uuid4().hex
        self._tasks: dict[str, asyncio.Task] = {}
        self._adapters: dict[str, object] = {}

    @staticmethod
    def authorization_hash(job) -> str:
        return hashlib.sha256(
            encode(
                {
                    "job_id": job.id,
                    "request_snapshot": job.request_snapshot,
                    "request": job.request,
                }
            ).encode()
        ).hexdigest()

    def _job(self, job_id: str):
        job = self.jobs.get(job_id)
        if job.project_id not in self.teams.project_ids:
            raise JobNotFoundError("Job does not exist.")
        return job

    def read(self, job_id: str) -> dict[str, object]:
        job = self._job(job_id)
        attempt = self.attempts.get(job_id)
        result: dict[str, object] = {
            "job_id": job.id,
            "enabled": self.system.native_execution_enabled,
            "authorization_hash": self.authorization_hash(job),
            "attempt": None if attempt is None else attempt.public(),
            "supervised_here": job.id in self._tasks and not self._tasks[job.id].done(),
            "requires_reconciliation": bool(
                attempt
                and attempt.held
                and (
                    attempt.phase == "reconcile"
                    or attempt.owner != self._owner
                    or job.id not in self._tasks
                    or self._tasks[job.id].done()
                )
            ),
        }
        return result

    async def start(self, job_id: str, payload: object) -> dict[str, object]:
        job = self._job(job_id)
        if not isinstance(payload, Mapping) or set(payload) != {"authorization_hash"}:
            raise TeamConfigError("Native start requires the current authorization hash.")
        if payload["authorization_hash"] != self.authorization_hash(job):
            raise NativeConflictError("Native authorization changed. Reload before starting.")
        if self.attempts.get(job_id) is not None:
            return self.read(job_id)
        if not self.system.native_execution_enabled:
            raise NativeConflictError("Native execution is disabled in local configuration.")
        with self.attempts._connection() as connection:
            binding = self.teams.authorize(connection, job)
        self._require_supported(binding)
        path = await self.worktrees.verify_bound(job)
        identity = worktree_identity(path)
        if identity is None:
            raise NativeConflictError("Native execution requires an isolated linked worktree.")
        attempt, admitted = self.attempts.claim(job, self.teams, self._owner, identity)
        if admitted:
            task = asyncio.create_task(self._run(attempt, job))
            self._tasks[job.id] = task
        return self.read(job_id)

    @staticmethod
    def _require_supported(binding: dict) -> None:
        profile, model, source = binding["profile"], binding["model"], binding["connection"]
        if (
            source["kind"] != "codex"
            or not source["account_slot"]
            or profile["role"] != "implementer"
            or profile["tools"] != ["native_workspace"]
            or not profile["allowed_write_paths"]
            or profile["document_paths"]
            or profile["max_steps"] is not None
            or profile["max_output_tokens"] is not None
            or not {"tools", "files", "streaming"} <= set(model["capabilities"])
            or not model["verified"]
        ):
            raise NativeConflictError(
                "This agent policy is not supported by the native supervisor."
            )

    def _adapter(self, attempt: NativeAttempt):
        slots = codex_account_slots(self.runtime_root, (attempt.account_slot,))
        adapter = CodexAdapter(
            CodexAccountPool(slots), model=attempt.authorization["model"]["model"]
        )
        return adapter

    async def _consume(self, attempt: NativeAttempt, job, adapter) -> str:
        self.attempts.check(attempt, self.teams)
        compatibility = await adapter.check_compatibility()
        if compatibility is not True:
            raise NativeConflictError("The installed native CLI is incompatible.")
        self.attempts.check(attempt, self.teams)
        path = await self.worktrees.verify_bound(self.jobs.get(job.id))
        if worktree_identity(path) != attempt.worktree_identity:
            raise NativeConflictError("Native worktree identity changed before launch.")
        self.attempts.check(attempt, self.teams)
        session = await adapter.start(
            AdapterStart(
                job.id,
                job.request,
                path,
                frozenset(
                    {
                        ProviderCapability.TOOLS,
                        ProviderCapability.FILES,
                        ProviderCapability.STREAMING,
                    }
                ),
                tuple(attempt.authorization["profile"]["allowed_write_paths"]),
            )
        )
        if session.job_id != job.id or session.runtime is not JobRuntime.CODEX:
            raise NativeConflictError("Native session identity is invalid.")
        self.attempts.bind(attempt, session.session_id)
        completion = None
        sequence = 0
        async for event in adapter.stream(session):
            if event.sequence != sequence + 1:
                raise NativeConflictError("Native event sequence is invalid.")
            sequence = event.sequence
            self.attempts.check(attempt, self.teams)
            self.attempts.append(attempt, event)
            if event.kind is RuntimeEventKind.COMPLETION:
                if completion is not None:
                    raise NativeConflictError("Native stream contains duplicate completion.")
                completion = event.payload["status"]
        if (
            not adapter.stopped(job.id)
            or adapter.outcome(job.id) != completion
            or completion is None
        ):
            raise NativeConflictError("Native termination is not confirmed.")
        return completion

    async def _run(self, attempt: NativeAttempt, job) -> None:
        adapter = None
        consumption = None
        outcome = "failed"
        stopped = False
        try:
            adapter = self._factory(attempt)
            if isinstance(adapter, CodexAdapter):

                def guard() -> None:
                    self.attempts.check(attempt, self.teams)
                    if worktree_identity(Path(attempt.worktree_path)) != attempt.worktree_identity:
                        raise NativeConflictError("Native worktree identity changed before launch.")

                adapter.install_start_guard(guard)
            self._adapters[job.id] = adapter
            consumption = asyncio.create_task(self._consume(attempt, job, adapter))
            deadline = asyncio.get_running_loop().time() + self.system.execution_timeout_seconds
            while not consumption.done():
                await asyncio.wait({consumption}, timeout=0.05)
                if consumption.done():
                    break
                self.attempts.check(attempt, self.teams)
                if asyncio.get_running_loop().time() >= deadline:
                    raise NativeConflictError("Native execution exceeded its time limit.")
            outcome = await consumption
            stopped = adapter.stopped(job.id)
        except asyncio.CancelledError:
            outcome = "cancelled"
        except Exception:
            outcome = "failed"
        finally:
            if consumption is not None and not consumption.done():
                consumption.cancel()
            if consumption is not None:
                with suppress(Exception, asyncio.CancelledError):
                    await consumption
            if adapter is not None:
                try:
                    stopped = await adapter.stop_job(job.id)
                except Exception:
                    stopped = False
            else:
                stopped = True
            try:
                if stopped:
                    self.attempts.finish(attempt, outcome)
                else:
                    self.attempts.quarantine(attempt)
            except (NativeStoreError, TeamStoreError, ValueError):
                with suppress(NativeStoreError):
                    self.attempts.quarantine(attempt)
            self._adapters.pop(job.id, None)
            self._tasks.pop(job.id, None)

    async def cancel(self, job_id: str, payload: object) -> dict[str, object]:
        self._job(job_id)
        if not isinstance(payload, Mapping) or set(payload) != {"attempt_id"}:
            raise TeamConfigError("Cancellation requires a native attempt identity.")
        self.attempts.cancel(job_id, payload["attempt_id"])
        return self.read(job_id)

    async def close(self) -> None:
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
