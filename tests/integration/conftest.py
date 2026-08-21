from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine

from job_visibility.testing import ToxiproxyClient


def _required_url(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is required for infrastructure tests")
    return value


@pytest.fixture(scope="session")
def scheduler_engine() -> Iterator[Engine]:
    if os.getenv("RUN_POSTGRES_TESTS") != "1" and os.getenv("RUN_RESILIENCE_TESTS") != "1":
        pytest.skip("set RUN_POSTGRES_TESTS=1 to run PostgreSQL integration tests")
    engine = create_engine(_required_url("SCHEDULER_DATABASE_URL"), pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def edr_engine() -> Iterator[Engine]:
    if os.getenv("RUN_POSTGRES_TESTS") != "1" and os.getenv("RUN_RESILIENCE_TESTS") != "1":
        pytest.skip("set RUN_POSTGRES_TESTS=1 to run PostgreSQL integration tests")
    engine = create_engine(_required_url("EDR_DATABASE_URL"), pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def toxiproxy() -> Iterator[ToxiproxyClient]:
    if not any(
        os.getenv(name) == "1"
        for name in ("RUN_CASSANDRA_TESTS", "RUN_RESILIENCE_TESTS", "RUN_CHAOS_TESTS")
    ):
        pytest.skip("set RUN_CHAOS_TESTS=1 to run proxy chaos tests")
    with ToxiproxyClient(os.getenv("TOXIPROXY_URL", "http://localhost:8474")) as client:
        client.reset()
        try:
            yield client
        finally:
            client.reset()
