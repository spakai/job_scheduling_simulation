"""Role-specific ASGI composition for durable production processes."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from job_visibility.api import create_app
from job_visibility.chaos import fault_injector_from_env
from job_visibility.config import database_config_from_env, scheduler_tuning_from_env
from job_visibility.edr_store import DurableVisibilityReader
from job_visibility.observability import HealthService
from job_visibility.persistence import build_database_sessions
from job_visibility.scheduler import SchedulerService

_SHARED_PATHS = {"/health/live", "/health/ready", "/metrics", "/openapi.json", "/docs", "/redoc"}


def _retain_paths(app: FastAPI, prefixes: tuple[str, ...]) -> FastAPI:
    app.router.routes = [
        route
        for route in app.router.routes
        if getattr(route, "path", "") in _SHARED_PATHS
        or any(getattr(route, "path", "").startswith(prefix) for prefix in prefixes)
    ]
    app.openapi_schema = None
    return app


def create_scheduler_app() -> FastAPI:
    database = build_database_sessions(database_config_from_env("SCHEDULER"), role="scheduler-api")
    tuning = scheduler_tuning_from_env()
    fault_injector = fault_injector_from_env()
    scheduler = SchedulerService(
        database.session_factory,
        claim_lease_seconds=tuning.claim_lease_seconds,
        fault_injector=fault_injector,
    )
    health = HealthService(database.session_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            database.dispose()

    app = create_app(
        scheduler=scheduler,
        health=health,
        lifespan=lifespan,
        max_payload_bytes=int(os.getenv("API_MAX_PAYLOAD_BYTES", "65536")),
        visibility_base_url=os.getenv("VISIBILITY_API_PUBLIC_URL", "http://localhost:8001"),
    )
    app.title = "Job Scheduler API"
    return _retain_paths(app, ("/scheduler/",))


def create_visibility_app() -> FastAPI:
    database = build_database_sessions(database_config_from_env("EDR"), role="visibility-api")
    reader = DurableVisibilityReader(
        database.session_factory,
        fault_injector=fault_injector_from_env(),
    )
    health = HealthService(edr=database.session_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            database.dispose()

    app = create_app(durable_reader=reader, health=health, lifespan=lifespan)
    app.title = "Job Visibility API"
    return _retain_paths(app, ("/scheduled-jobs", "/edr-lifecycle", "/reconciliation-runs"))


def main() -> None:
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Durable role-specific HTTP API")
    parser.add_argument("role", choices=("scheduler", "visibility"))
    parser.add_argument("--host", default=os.getenv("API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()
    port = args.port or int(
        os.getenv(
            "SCHEDULER_API_PORT" if args.role == "scheduler" else "VISIBILITY_API_PORT", "8000"
        )
    )
    factory = create_scheduler_app if args.role == "scheduler" else create_visibility_app
    uvicorn.run(factory(), host=args.host, port=port)


if __name__ == "__main__":
    main()
