from sqlalchemy.pool import QueuePool

from job_visibility.config import AppConfig
from job_visibility.persistence import build_durable_sessions


def test_builds_independent_role_specific_pools_without_connecting() -> None:
    config = AppConfig.from_env(
        {
            "SCHEDULER_DATABASE_URL": "postgresql+psycopg://scheduler:secret@db/scheduler",
            "EDR_DATABASE_URL": "postgresql+psycopg://edr:secret@db/edr",
        }
    )

    sessions = build_durable_sessions(config)
    try:
        assert sessions.scheduler.engine is not sessions.edr.engine
        assert sessions.scheduler.session_factory.kw["bind"] is sessions.scheduler.engine
        assert sessions.edr.session_factory.kw["bind"] is sessions.edr.engine
        assert isinstance(sessions.scheduler.engine.pool, QueuePool)
        assert sessions.scheduler.engine.pool.size() == config.scheduler_database.pool_size
        assert sessions.scheduler.engine.url.database == "scheduler"
        assert sessions.edr.engine.url.database == "edr"
        assert str(sessions.scheduler.engine.url).find("secret") == -1
    finally:
        sessions.dispose()
