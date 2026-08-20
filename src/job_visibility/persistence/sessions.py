"""Independent SQLAlchemy resources for scheduler and EDR database roles."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.config import AppConfig, DatabaseConfig


@dataclass(frozen=True, slots=True)
class DatabaseSessions:
    """An engine and transaction factory owned by one database role."""

    engine: Engine
    session_factory: sessionmaker[Session]

    def dispose(self) -> None:
        self.engine.dispose()


@dataclass(frozen=True, slots=True)
class DurableSessions:
    """Both database roles, kept distinct even when hosted on one server."""

    scheduler: DatabaseSessions
    edr: DatabaseSessions

    def dispose(self) -> None:
        self.scheduler.dispose()
        self.edr.dispose()


def _build_database(config: DatabaseConfig, *, role: str) -> DatabaseSessions:
    engine = create_engine(
        config.url.get_secret_value(),
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout_seconds,
        pool_pre_ping=True,
        connect_args={"application_name": f"job-visibility-{role}"},
    )
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    return DatabaseSessions(engine=engine, session_factory=factory)


def build_durable_sessions(config: AppConfig) -> DurableSessions:
    """Build role-specific pools without opening a database connection."""
    return DurableSessions(
        scheduler=_build_database(config.scheduler_database, role="scheduler"),
        edr=_build_database(config.edr_database, role="edr"),
    )
