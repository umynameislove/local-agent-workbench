from __future__ import annotations

import argparse
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request

from db import (
    ApprovalRepository,
    AtomicTransitionService,
    BackupService,
    BackupServiceError,
    Database,
    EventRepository,
    JobRepository,
    MemoryReferenceRepository,
    PlannerRepository,
    ProjectRepository,
    RecoveryService,
    UsageRepository,
)
from engine import APP_VERSION, RUNTIME_ENV, JobRuntime, RuntimeHome, resolve_runtime_home
from logging_setup import configure_logging


def create_app(
    env: Mapping[str, str] | None = None,
    *,
    cwd: Path | None = None,
    user_home: Path | None = None,
) -> FastAPI:
    runtime: RuntimeHome = resolve_runtime_home(env, cwd=cwd, user_home=user_home)
    runtime_home_configured = bool(
        (os.environ if env is None else env).get(RUNTIME_ENV, "").strip()
    )
    logger = configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime.bootstrap()
        database = Database(runtime.state_db)
        schema_version = database.initialize()
        app.state.runtime = runtime
        app.state.database = database
        app.state.project_repository = ProjectRepository(database)
        app.state.job_repository = JobRepository(database)
        app.state.event_repository = EventRepository(database)
        app.state.approval_repository = ApprovalRepository(database)
        app.state.planner_repository = PlannerRepository(database)
        app.state.usage_repository = UsageRepository(database)
        app.state.memory_reference_repository = MemoryReferenceRepository(database)
        app.state.atomic_transition_service = AtomicTransitionService(database)
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
    async def bootstrap() -> dict[str, object]:
        return {
            "version": APP_VERSION,
            "windows": ["workspace", "planner"],
            "runtimes": [runtime.value for runtime in JobRuntime],
            "consultant": {
                "role": "advice-only",
                "can_execute": False,
                "can_approve": False,
            },
        }

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
