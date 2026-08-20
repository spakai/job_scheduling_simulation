from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.edr_store import KafkaEdrIngress
from job_visibility.model import Event, EventType
from job_visibility.outbox import ConfluentBrokerProducer, OutboxPublisher
from job_visibility.scheduler import JobSubmission, SchedulerService
from job_visibility.testing import ComposeOutageController, poll_until


def _controller() -> ComposeOutageController:
    if os.getenv("RUN_OUTAGE_TESTS") != "1":
        pytest.skip("set RUN_OUTAGE_TESTS=1 to run container outage tests")
    return ComposeOutageController(
        os.getenv("JOB_VISIBILITY_COMPOSE_PROJECT", "job-visibility-resilience")
    )


def _scalar(engine: Engine, query: str) -> object:
    with engine.connect() as connection:
        return connection.execute(text(query)).scalar_one()


def _sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def _delete_scheduler_job(engine: Engine, job_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM scheduler_attempts WHERE job_id=:job"), {"job": job_id}
        )
        connection.execute(
            text("DELETE FROM scheduler_outbox WHERE message_key=:job"), {"job": job_id}
        )
        connection.execute(text("DELETE FROM scheduler_jobs WHERE job_id=:job"), {"job": job_id})


@pytest.mark.integration
@pytest.mark.e2e
def test_scheduler_and_edr_database_outages_are_independent(
    scheduler_engine: Engine, edr_engine: Engine
) -> None:
    controller = _controller()
    controller.stop("scheduler-postgres")
    try:
        assert _scalar(edr_engine, "SELECT 1") == 1
        with pytest.raises(DBAPIError):
            _scalar(scheduler_engine, "SELECT 1")
    finally:
        controller.start("scheduler-postgres")
    assert (
        poll_until(
            lambda: _safe_scalar(scheduler_engine),
            lambda value: value == 1,
            description="scheduler database recovery",
            timeout_seconds=30,
            retry_exceptions=(DBAPIError,),
        )
        == 1
    )

    controller.stop("edr-postgres")
    try:
        assert _scalar(scheduler_engine, "SELECT 1") == 1
        with pytest.raises(DBAPIError):
            _scalar(edr_engine, "SELECT 1")
    finally:
        controller.start("edr-postgres")
    assert (
        poll_until(
            lambda: _safe_scalar(edr_engine),
            lambda value: value == 1,
            description="EDR database recovery",
            timeout_seconds=30,
            retry_exceptions=(DBAPIError,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_kafka_outage_preserves_and_drains_scheduler_outbox(
    scheduler_engine: Engine, edr_engine: Engine
) -> None:
    controller = _controller()
    producer = _producer(delivery_timeout_ms=1_500)
    job_id = f"r-kafka-02-{uuid4()}"
    scheduler = SchedulerService(_sessions(scheduler_engine))
    scheduler.submit(JobSubmission(job_id, job_id, "PRINT", datetime.now(UTC), {"message": "x"}))
    publisher = OutboxPublisher(
        _sessions(scheduler_engine),
        producer,
        owner="outage-test",
        retry_initial=0.01,
        retry_max=0.01,
    )
    controller.stop("kafka")
    try:
        assert publisher.run_once() >= 2
        with scheduler_engine.connect() as connection:
            assert (
                connection.execute(
                    text("""SELECT count(*) FROM scheduler_outbox
                WHERE message_key=:job AND published_at IS NULL"""),
                    {"job": job_id},
                ).scalar_one()
                >= 2
            )
    finally:
        controller.start("kafka")

    try:
        with scheduler_engine.begin() as connection:
            connection.execute(
                text("""UPDATE scheduler_outbox SET next_attempt_at=clock_timestamp()
                WHERE message_key=:job AND published_at IS NULL"""),
                {"job": job_id},
            )
        poll_until(
            publisher.run_once,
            lambda _: _unpublished(scheduler_engine, job_id) == 0,
            description="outbox to drain after Kafka recovery",
            timeout_seconds=30,
            interval_seconds=0.25,
        )
        persisted = poll_until(
            lambda: _persisted_events(edr_engine, job_id),
            lambda count: count >= 2,
            description="Kafka Connect catch-up after broker recovery",
            timeout_seconds=60,
            interval_seconds=0.25,
        )
        assert persisted >= 2
    finally:
        producer.close(5)
        _delete_scheduler_job(scheduler_engine, job_id)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_edr_outage_buffers_in_kafka_and_catches_up(edr_engine: Engine) -> None:
    controller = _controller()
    producer = _producer()
    event_id = f"r-sink-01-{uuid4()}"
    job_id = f"job-{event_id}"
    now = datetime.now(UTC)
    controller.stop("edr-postgres")
    try:
        KafkaEdrIngress(producer, topic="job-lifecycle-edr.v1").publish(
            Event(event_id, EventType.JOB_CREATED, now, now, job_id)
        )
        with pytest.raises(DBAPIError):
            _scalar(edr_engine, "SELECT 1")
    finally:
        controller.start("edr-postgres")
    try:
        assert poll_until(
            lambda: _event_exists(edr_engine, event_id),
            bool,
            description="sink to catch up after EDR database recovery",
            timeout_seconds=45,
            interval_seconds=0.25,
            retry_exceptions=(DBAPIError,),
        )
    finally:
        producer.close(5)


def _producer(*, delivery_timeout_ms: int = 10_000) -> ConfluentBrokerProducer:
    return ConfluentBrokerProducer(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        socket_timeout_ms=min(1_000, delivery_timeout_ms),
        request_timeout_ms=min(1_000, delivery_timeout_ms),
        delivery_timeout_ms=delivery_timeout_ms,
        metadata_timeout_ms=min(1_000, delivery_timeout_ms),
    )


def _safe_scalar(engine: Engine) -> object:
    return _scalar(engine, "SELECT 1")


def _unpublished(engine: Engine, job_id: str) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text("""SELECT count(*) FROM scheduler_outbox
            WHERE message_key=:job AND published_at IS NULL"""),
            {"job": job_id},
        ).scalar_one()


def _persisted_events(engine: Engine, job_id: str) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT count(*) FROM edr_events WHERE job_id=:job"), {"job": job_id}
        ).scalar_one()


def _event_exists(engine: Engine, event_id: str) -> bool:
    with engine.connect() as connection:
        return bool(
            connection.execute(
                text("SELECT count(*) FROM edr_events WHERE event_id=:event"),
                {"event": event_id},
            ).scalar_one()
        )
