from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request

from engine import APP_VERSION, RUNTIME_ENV, RuntimeHome, resolve_runtime_home
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
        app.state.runtime = runtime
        logger.info(
            "Runtime storage is ready.",
            extra={
                "event": "runtime.bootstrap.completed",
                "context": {"runtime_home_configured": runtime_home_configured},
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
            "database": {"status": "not_initialized"},
        }

    @api.get("/api/bootstrap")
    async def bootstrap() -> dict[str, object]:
        return {
            "version": APP_VERSION,
            "windows": ["workspace", "planner"],
            "runtimes": ["auto", "claude", "codex", "local"],
            "consultant": {
                "role": "advice-only",
                "can_execute": False,
                "can_approve": False,
            },
        }

    return api


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8765, reload=False)


if __name__ == "__main__":
    main()
