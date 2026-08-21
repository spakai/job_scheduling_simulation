from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from job_visibility.testing import ToxiproxyClient, poll_until


@pytest.fixture(autouse=True)
def require_postgres_runtime() -> None:
    if not any(os.getenv(name) == "1" for name in ("RUN_POSTGRES_TESTS", "RUN_RESILIENCE_TESTS")):
        pytest.skip("set RUN_POSTGRES_TESTS=1 to run resource-pressure integration tests")


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.chaos
@pytest.mark.resource
def test_net_01_postgres_latency_is_observable_and_recovers(
    toxiproxy: ToxiproxyClient,
) -> None:
    url = os.getenv(
        "SCHEDULER_CHAOS_DATABASE_URL",
        "postgresql+psycopg://scheduler_owner:scheduler-local@localhost:15432/scheduler",
    )
    engine = create_engine(url, pool_pre_ping=True)
    toxic = f"net-01-{uuid4()}"
    try:
        toxiproxy.add_toxic(
            "scheduler-postgres",
            toxic,
            "latency",
            attributes={"latency": 200, "jitter": 25},
        )
        started = time.monotonic()
        with engine.connect() as connection:
            assert connection.execute(text("SELECT 1")).scalar_one() == 1
        assert time.monotonic() - started >= 0.15
        toxiproxy.remove_toxic("scheduler-postgres", toxic)
        poll_until(
            lambda: _select_one(engine),
            lambda value: value == 1,
            description="scheduler PostgreSQL proxy to recover",
            timeout_seconds=10,
            interval_seconds=0.1,
        )
    finally:
        toxiproxy.remove_toxic("scheduler-postgres", toxic)
        engine.dispose()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.chaos
@pytest.mark.resource
def test_net_03_pool_saturation_is_bounded_and_recovers() -> None:
    url = os.getenv(
        "SCHEDULER_DATABASE_URL",
        "postgresql+psycopg://scheduler_owner:scheduler-local@localhost:5432/scheduler",
    )
    engine = create_engine(url, pool_size=1, max_overflow=0, pool_timeout=0.1)
    try:
        with engine.connect() as held:
            assert held.execute(text("SELECT 1")).scalar_one() == 1
            started = time.monotonic()
            with pytest.raises(PoolTimeoutError), engine.connect():
                pass
            assert time.monotonic() - started < 1
        with engine.connect() as recovered:
            assert recovered.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()


def _select_one(engine: object) -> int:
    with engine.connect() as connection:  # type: ignore[attr-defined]
        return connection.execute(text("SELECT 1")).scalar_one()
