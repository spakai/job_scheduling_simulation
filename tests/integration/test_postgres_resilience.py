from __future__ import annotations

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError


@pytest.mark.integration
@pytest.mark.postgres
def test_authorities_use_independent_endpoints(
    scheduler_engine: Engine, edr_engine: Engine
) -> None:
    assert (
        scheduler_engine.url.host,
        scheduler_engine.url.port,
        scheduler_engine.url.database,
    ) != (
        edr_engine.url.host,
        edr_engine.url.port,
        edr_engine.url.database,
    )
    with scheduler_engine.connect() as connection:
        assert connection.execute(text("SELECT current_database()")).scalar_one() == "scheduler"
    with edr_engine.connect() as connection:
        assert connection.execute(text("SELECT current_database()")).scalar_one() == "edr"


@pytest.mark.integration
@pytest.mark.postgres
def test_statement_timeout_cancels_work_and_connection_recovers(scheduler_engine: Engine) -> None:
    with scheduler_engine.connect() as connection:
        with pytest.raises(DBAPIError), connection.begin():
            connection.execute(text("SET LOCAL statement_timeout = '50ms'"))
            connection.execute(text("SELECT pg_sleep(1)"))
        assert connection.execute(text("SELECT 1")).scalar_one() == 1


@pytest.mark.integration
@pytest.mark.postgres
def test_lock_timeout_cancels_waiter_without_releasing_owner(scheduler_engine: Engine) -> None:
    with scheduler_engine.connect() as owner, scheduler_engine.connect() as waiter:
        owner_tx = owner.begin()
        owner.execute(text("SELECT pg_advisory_xact_lock(3003)"))
        try:
            with pytest.raises(DBAPIError), waiter.begin():
                waiter.execute(text("SET LOCAL lock_timeout = '50ms'"))
                waiter.execute(text("SELECT pg_advisory_xact_lock(3003)"))
            assert owner.execute(text("SELECT 1")).scalar_one() == 1
        finally:
            owner_tx.rollback()
