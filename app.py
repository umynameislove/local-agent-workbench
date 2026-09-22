from __future__ import annotations

import argparse
import os
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Body, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from db import (
    ApprovalRepository,
    AtomicTransitionService,
    BackupService,
    BackupServiceError,
    Database,
    EventRepository,
    EventRepositoryError,
    JobNotFoundError,
    JobRepository,
    JobRepositoryError,
    JobValidationError,
    MemoryReferenceRepository,
    PlannerRepository,
    ProjectRepository,
    RecoveryService,
    ReviewBundleRepository,
    UsageRepository,
    VerificationRepository,
)
from engine import (
    APP_VERSION,
    RUNTIME_ENV,
    JobRuntime,
    RuntimeHome,
    load_configured_projects,
    resolve_runtime_home,
)
from event_stream import EventStreamCursorError, EventStreamService
from job_submission import (
    JobSubmissionConflictError,
    JobSubmissionNotFoundError,
    JobSubmissionService,
    JobSubmissionUnavailableError,
    JobSubmissionValidationError,
)
from logging_setup import configure_logging
from planning import (
    PlanningConflictError,
    PlanningNotFoundError,
    PlanningUnavailableError,
    ReadOnlyPlanningService,
)
from review_bundle import (
    ReviewBundleMissingError,
    ReviewBundleService,
    ReviewBundleStateError,
    ReviewBundleUnavailableError,
)
from secret_gate import (
    PreReviewSecretGate,
    SecretGateBlockedError,
    SecretGateConflictError,
    SecretGateNotFoundError,
    SecretGateUnavailableError,
)
from verification import VerificationRunner
from worktree import (
    WorktreeConflictError,
    WorktreeManager,
    WorktreeNotFoundError,
    WorktreeUnavailableError,
)


def create_app(
    env: Mapping[str, str] | None = None,
    *,
    cwd: Path | None = None,
    user_home: Path | None = None,
    job_id_factory: Callable[[], str] | None = None,
) -> FastAPI:
    runtime: RuntimeHome = resolve_runtime_home(env, cwd=cwd, user_home=user_home)
    runtime_home_configured = bool(
        (os.environ if env is None else env).get(RUNTIME_ENV, "").strip()
    )
    logger = configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime.bootstrap()
        configured_projects = await load_configured_projects(runtime.config)
        database = Database(runtime.state_db)
        schema_version = database.initialize()
        app.state.runtime = runtime
        app.state.database = database
        app.state.project_repository = ProjectRepository(database)
        app.state.projects = tuple(
            app.state.project_repository.register(project) for project in configured_projects
        )
        app.state.job_repository = JobRepository(database)
        app.state.job_submission_service = JobSubmissionService(
            app.state.project_repository,
            app.state.job_repository,
            active_project_ids=frozenset(project.id for project in app.state.projects),
            id_factory=job_id_factory,
        )
        app.state.event_repository = EventRepository(database)
        app.state.event_stream_service = EventStreamService(
            app.state.job_repository,
            app.state.event_repository,
        )
        app.state.approval_repository = ApprovalRepository(database)
        app.state.planner_repository = PlannerRepository(database)
        app.state.usage_repository = UsageRepository(database)
        app.state.memory_reference_repository = MemoryReferenceRepository(database)
        app.state.verification_repository = VerificationRepository(database)
        app.state.review_bundle_repository = ReviewBundleRepository(database)
        app.state.atomic_transition_service = AtomicTransitionService(database)
        app.state.secret_gate = PreReviewSecretGate(
            app.state.job_repository,
            app.state.event_repository,
            app.state.atomic_transition_service,
        )
        app.state.review_bundle_service = ReviewBundleService(
            app.state.job_repository,
            app.state.event_repository,
            app.state.verification_repository,
            app.state.review_bundle_repository,
        )
        app.state.planning_service = ReadOnlyPlanningService(
            app.state.job_repository,
            app.state.event_repository,
            app.state.atomic_transition_service,
        )
        app.state.worktree_manager = WorktreeManager(
            app.state.job_repository,
            app.state.event_repository,
            app.state.atomic_transition_service,
            runtime.worktrees,
        )
        app.state.verification_runner = VerificationRunner(
            app.state.job_repository,
            app.state.verification_repository,
        )
        app.state.recovery_service = RecoveryService(database, runtime.worktrees)
        app.state.recovery_items = app.state.recovery_service.load()
        app.state.schema_version = schema_version
        logger.info(
            "Runtime storage is ready.",
            extra={
                "event": "runtime.bootstrap.completed",
                "context": {
                    "runtime_home_configured": runtime_home_configured,
                    "schema_version": schema_version,
                    "recovery_jobs": len(app.state.recovery_items),
                },
            },
        )
        yield

    api = FastAPI(title="Local Agent Workbench", version=APP_VERSION, lifespan=lifespan)

    @api.get("/api/health")
    async def health(request: Request) -> dict[str, object]:
        active: RuntimeHome = request.app.state.runtime
        return {
            "status": "ok",
            "version": APP_VERSION,
            "runtime_home_configured": runtime_home_configured,
            "storage": {
                "root_ready": active.root.is_dir(),
                "logs_ready": active.logs.is_dir(),
                "cache_ready": active.cache.is_dir(),
                "worktrees_ready": active.worktrees.is_dir(),
            },
            "database": {
                "status": "ready",
                "schema_version": request.app.state.schema_version,
            },
        }

    @api.get("/api/bootstrap")
    async def bootstrap(request: Request) -> dict[str, object]:
        return {
            "version": APP_VERSION,
            "windows": ["workspace", "planner"],
            "runtimes": [runtime.value for runtime in JobRuntime],
            "projects": [
                {
                    "id": project.id,
                    "sensitivity": project.sensitivity.value,
                    "cloud_allowed": project.cloud_allowed,
                    "permission_mode": project.permission_mode.value,
                }
                for project in request.app.state.projects
            ],
            "consultant": {
                "role": "advice-only",
                "can_execute": False,
                "can_approve": False,
            },
        }

    @api.get("/api/jobs/{job_id}/events")
    async def job_events(
        request: Request,
        job_id: str,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        service: EventStreamService = request.app.state.event_stream_service
        try:
            body = service.subscribe(job_id, last_event_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise HTTPException(status_code=404, detail="Job does not exist.") from error
        except EventStreamCursorError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        except (EventRepositoryError, JobRepositoryError) as error:
            raise HTTPException(status_code=503, detail="Event stream is unavailable.") from error
        return StreamingResponse(
            body,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @api.post("/api/jobs", status_code=201)
    async def create_job(
        request: Request,
        payload: Annotated[object, Body()],
    ) -> dict[str, object]:
        service: JobSubmissionService = request.app.state.job_submission_service
        try:
            job = await service.submit(payload)
        except JobSubmissionValidationError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except JobSubmissionNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except JobSubmissionConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except JobSubmissionUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        return {
            "created_at": job.created_at,
            "id": job.id,
            "model": job.model,
            "project_id": job.project_id,
            "request": job.request,
            "runtime": job.runtime.value,
            "state": job.state.value,
        }

    @api.post("/api/jobs/{job_id}/plan")
    async def run_plan(request: Request, job_id: str) -> dict[str, object]:
        service: ReadOnlyPlanningService = request.app.state.planning_service
        try:
            return service.run(job_id).to_dict()
        except PlanningNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except PlanningConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except PlanningUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.get("/api/jobs/{job_id}/plan")
    async def read_plan(request: Request, job_id: str) -> dict[str, object]:
        service: ReadOnlyPlanningService = request.app.state.planning_service
        try:
            return service.read(job_id).to_dict()
        except PlanningNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except PlanningConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except PlanningUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/jobs/{job_id}/worktree")
    async def create_worktree(request: Request, job_id: str) -> dict[str, object]:
        service: WorktreeManager = request.app.state.worktree_manager
        try:
            return (await service.create(job_id)).to_dict()
        except WorktreeNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except WorktreeConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except WorktreeUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/jobs/{job_id}/review-readiness")
    async def review_readiness(request: Request, job_id: str) -> dict[str, object]:
        service: PreReviewSecretGate = request.app.state.secret_gate
        try:
            return (await service.evaluate(job_id)).to_dict()
        except SecretGateNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except SecretGateBlockedError as error:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": str(error),
                    "event_id": error.event_id,
                    "scan": error.scan.to_dict(),
                },
            ) from error
        except SecretGateConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except SecretGateUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/jobs/{job_id}/review-bundle")
    async def create_review_bundle(
        request: Request, response: Response, job_id: str
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: ReviewBundleService = request.app.state.review_bundle_service
        try:
            return await service.create(job_id)
        except ReviewBundleMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ReviewBundleStateError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ReviewBundleUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.get("/api/jobs/{job_id}/review-bundle")
    async def read_review_bundle(
        request: Request, response: Response, job_id: str
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: ReviewBundleService = request.app.state.review_bundle_service
        try:
            return service.read(job_id)
        except ReviewBundleMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ReviewBundleUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    return api


app = create_app()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the local agent workbench.")
    commands = parser.add_subparsers(dest="command")
    backup = commands.add_parser("backup", help="Create a verified SQLite backup.")
    backup.add_argument("destination", type=Path, help="A new file outside source repositories.")
    arguments = parser.parse_args(argv)
    if arguments.command == "backup":
        try:
            runtime = resolve_runtime_home()
            BackupService(Database(runtime.state_db)).create(arguments.destination)
        except (BackupServiceError, OSError, ValueError):
            parser.exit(1, "Backup failed. Check storage, destination and database integrity.\n")
        print("Database backup created and verified.")
        return

    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8765, reload=False)


if __name__ == "__main__":
    main()
