from __future__ import annotations

import argparse
import ipaddress
import os
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from textwrap import shorten
from typing import Annotated

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from approval_service import (
    ApprovalInputError,
    ApprovalMissingError,
    ApprovalService,
    ApprovalStateError,
    ApprovalUnavailableError,
)
from db import (
    ApprovalRepository,
    ApprovalWorkflowRepository,
    AtomicTransitionService,
    BackupService,
    BackupServiceError,
    Database,
    EventRepository,
    EventRepositoryError,
    JobNotFoundError,
    JobRecord,
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
    RetentionConfig,
    RuntimeHome,
    load_configured_projects,
    load_optional_config,
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
from promotion import (
    PromotionConflictError,
    PromotionInputError,
    PromotionMissingError,
    PromotionReconciliationError,
    PromotionService,
    PromotionUnavailableError,
)
from retention import (
    RetentionConflictError,
    RetentionInputError,
    RetentionMissingError,
    RetentionService,
    RetentionUnavailableError,
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
from workspace_ui import asset
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
        configuration = load_optional_config(runtime.config)
        configured_projects = (
            ()
            if configuration is None
            else await load_configured_projects(runtime.config, config=configuration)
        )
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
        app.state.approval_workflow_repository = ApprovalWorkflowRepository(
            database, retention=configuration.retention if configuration else RetentionConfig()
        )
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
        app.state.approval_service = ApprovalService(
            app.state.job_repository,
            app.state.approval_repository,
            app.state.review_bundle_repository,
            app.state.approval_workflow_repository,
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
        app.state.promotion_service = PromotionService(
            app.state.project_repository,
            app.state.job_repository,
            app.state.approval_repository,
            app.state.approval_workflow_repository,
            app.state.review_bundle_repository,
            app.state.atomic_transition_service,
            app.state.worktree_manager,
            app.state.approval_service,
            runtime.cache,
        )
        app.state.retention_service = RetentionService(
            app.state.job_repository,
            app.state.approval_repository,
            app.state.approval_workflow_repository,
            app.state.review_bundle_repository,
            app.state.event_repository,
            app.state.worktree_manager,
            app.state.approval_service,
            runtime.cache,
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

    @api.middleware("http")
    async def local_boundary(request: Request, call_next: Callable) -> Response:
        try:
            _check_local_origin(request)
        except HTTPException as error:
            response = JSONResponse({"detail": error.detail}, status_code=error.status_code)
        else:
            response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @api.get("/", include_in_schema=False)
    async def workspace() -> Response:
        return asset("index.html")

    @api.get("/workspace.js", include_in_schema=False)
    async def workspace_script() -> Response:
        return asset("workspace.js")

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

    @api.get("/api/jobs")
    async def list_jobs(
        request: Request,
        project_id: str,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        before_id: str | None = None,
    ) -> dict[str, object]:
        if project_id not in {project.id for project in request.app.state.projects}:
            raise HTTPException(status_code=404, detail="Project does not exist.")
        try:
            jobs = request.app.state.job_repository.list_recent(
                project_id, limit=limit, before_id=before_id
            )
        except JobValidationError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except JobRepositoryError as error:
            raise HTTPException(status_code=503, detail="Task list is unavailable.") from error
        return {
            "jobs": [_workspace_job(job, summary=True) for job in jobs],
            "next_cursor": jobs[-1].id if len(jobs) == limit else None,
        }

    @api.get("/api/jobs/{job_id}")
    async def read_job(request: Request, job_id: str) -> dict[str, object]:
        try:
            job = request.app.state.job_repository.get(job_id)
        except (JobNotFoundError, JobValidationError) as error:
            raise HTTPException(status_code=404, detail="Job does not exist.") from error
        except JobRepositoryError as error:
            raise HTTPException(status_code=503, detail="Task detail is unavailable.") from error
        if job.project_id not in {project.id for project in request.app.state.projects}:
            raise HTTPException(status_code=404, detail="Job does not exist.")
        return _workspace_job(job)

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

    @api.post("/api/jobs/{job_id}/approval-request")
    async def request_approval(
        request: Request,
        response: Response,
        job_id: str,
        payload: Annotated[object, Body()],
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: ApprovalService = request.app.state.approval_service
        try:
            return await service.request(job_id, payload)
        except ApprovalInputError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ApprovalMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ApprovalStateError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ApprovalUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/approvals/{approval_id}")
    async def decide_approval(
        request: Request,
        response: Response,
        approval_id: str,
        payload: Annotated[object, Body()],
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: ApprovalService = request.app.state.approval_service
        try:
            return await service.decide(approval_id, payload)
        except ApprovalInputError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ApprovalMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ApprovalStateError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ApprovalUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/approvals/{approval_id}/promote")
    async def promote_approval(
        request: Request,
        response: Response,
        approval_id: str,
        payload: Annotated[object, Body()],
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: PromotionService = request.app.state.promotion_service
        try:
            return await service.promote(approval_id, payload)
        except PromotionInputError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except PromotionMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except (PromotionConflictError, PromotionReconciliationError) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except PromotionUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.get("/api/jobs/{job_id}/retention")
    async def read_retention(
        request: Request, response: Response, job_id: str
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: RetentionService = request.app.state.retention_service
        try:
            return service.read(job_id)
        except RetentionMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except RetentionConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except RetentionUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @api.post("/api/jobs/{job_id}/cleanup")
    async def cleanup_worktree(
        request: Request, response: Response, job_id: str, payload: Annotated[object, Body()]
    ) -> dict[str, object]:
        response.headers["Cache-Control"] = "no-store"
        service: RetentionService = request.app.state.retention_service
        try:
            return await service.cleanup(job_id, payload)
        except RetentionInputError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except RetentionMissingError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except RetentionConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except RetentionUnavailableError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    return api


def _workspace_job(job: JobRecord, *, summary: bool = False) -> dict[str, object]:
    result: dict[str, object] = {
        "id": job.id,
        "project_id": job.project_id,
        "title": shorten(job.request, width=100, placeholder="…"),
        "state": job.state.value,
        "runtime": job.runtime.value,
        "model": job.model,
        "worktree_ready": job.worktree_path is not None,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    if not summary:
        result["request"] = job.request
    return result


def _check_local_origin(request: Request) -> None:
    if request.url.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise HTTPException(status_code=403, detail="Workbench requires a local host.")
    try:
        client = ipaddress.ip_address(request.client.host) if request.client else None
    except ValueError:
        client = None
    if client is None or not client.is_loopback:
        raise HTTPException(status_code=403, detail="Workbench requires a local client.")
    origin = request.headers.get("origin")
    if origin is not None and origin != f"{request.url.scheme}://{request.headers.get('host')}":
        raise HTTPException(status_code=403, detail="Cross origin requests are not allowed.")
    if request.headers.get("sec-fetch-site") == "cross-site":
        raise HTTPException(status_code=403, detail="Cross site requests are not allowed.")


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
