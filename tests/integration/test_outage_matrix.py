from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, Producer
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


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_poison_record_reaches_dlq_and_subsequent_record_progresses(edr_engine: Engine) -> None:
    """SINK-02: a converter failure must not wedge the connector partition."""
    poison_key = f"r-sink-02-poison-{uuid4()}"
    valid_event_id = f"r-sink-02-valid-{uuid4()}"
    valid_job_id = f"job-{valid_event_id}"
    consumer = _dlq_consumer()
    raw_producer = Producer({"bootstrap.servers": _bootstrap_servers()})
    producer = _producer()
    now = datetime.now(UTC)
    try:
        raw_producer.produce(
            "job-lifecycle-edr.v1",
            key=poison_key.encode(),
            value=b"not-schema-registry-framed-json",
        )
        assert raw_producer.flush(10) == 0
        KafkaEdrIngress(producer, topic="job-lifecycle-edr.v1").publish(
            Event(valid_event_id, EventType.JOB_CREATED, now, now, valid_job_id)
        )

        assert _wait_for_dlq_key(consumer, poison_key) == poison_key
        assert poll_until(
            lambda: _event_exists(edr_engine, valid_event_id),
            bool,
            description="valid EDR after poison record",
            timeout_seconds=45,
            interval_seconds=0.25,
        )
    finally:
        consumer.close()
        producer.close(5)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_event_identity_collision_is_immutable_and_partition_continues(
    edr_engine: Engine,
) -> None:
    """SINK-03: a conflicting event ID is quarantined without mutating authority."""
    collision_id = f"r-sink-03-collision-{uuid4()}"
    collision_job_id = f"job-{collision_id}"
    following_id = f"r-sink-03-following-{uuid4()}"
    following_job_id = f"job-{following_id}"
    consumer = _dlq_consumer()
    producer = _producer()
    ingress = KafkaEdrIngress(producer, topic="job-lifecycle-edr.v1")
    now = datetime.now(UTC)
    try:
        ingress.publish(Event(collision_id, EventType.JOB_CREATED, now, now, collision_job_id))
        assert poll_until(
            lambda: _event_type(edr_engine, collision_id),
            lambda value: value == EventType.JOB_CREATED.value,
            description="original collision candidate to persist",
            timeout_seconds=30,
            interval_seconds=0.25,
        )

        ingress.publish(Event(collision_id, EventType.JOB_CANCELLED, now, now, collision_job_id))
        ingress.publish(Event(following_id, EventType.JOB_CREATED, now, now, following_job_id))

        assert _wait_for_dlq_key(consumer, collision_job_id) == collision_job_id
        assert _event_type(edr_engine, collision_id) == EventType.JOB_CREATED.value
        assert poll_until(
            lambda: _event_exists(edr_engine, following_id),
            bool,
            description="valid EDR after identity collision",
            timeout_seconds=45,
            interval_seconds=0.25,
        )
    finally:
        consumer.close()
        producer.close(5)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_connect_restart_replays_buffered_record(edr_engine: Engine) -> None:
    """Restart matrix: Connect resumes from Kafka after a process restart."""
    controller = _controller()
    event_id = f"r-restart-connect-{uuid4()}"
    job_id = f"job-{event_id}"
    producer = _producer()
    now = datetime.now(UTC)
    controller.stop("kafka-connect")
    try:
        KafkaEdrIngress(producer, topic="job-lifecycle-edr.v1").publish(
            Event(event_id, EventType.JOB_CREATED, now, now, job_id)
        )
        assert not _event_exists(edr_engine, event_id)
    finally:
        controller.start("kafka-connect")
    try:
        assert poll_until(
            lambda: _event_exists(edr_engine, event_id),
            bool,
            description="Connect to replay after restart",
            timeout_seconds=60,
            interval_seconds=0.25,
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


def _bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _dlq_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": _bootstrap_servers(),
            "group.id": f"resilience-dlq-{uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.getenv("KAFKA_EDR_DLQ_TOPIC", "job-lifecycle-edr-dlq.v1")])

    def assigned() -> list[object]:
        consumer.poll(0.1)
        return consumer.assignment()

    # Force assignment before producing so "latest" cannot skip the test record.
    poll_until(
        assigned,
        bool,
        description="DLQ consumer partition assignment",
        timeout_seconds=15,
        interval_seconds=0.1,
    )
    return consumer


def _wait_for_dlq_key(consumer: Consumer, expected_key: str) -> str:
    def receive() -> str | None:
        message = consumer.poll(0.25)
        if message is None:
            return None
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(message.error())
        key = message.key()
        return key.decode() if key else ""

    return poll_until(
        receive,
        lambda key: key == expected_key,
        description=f"DLQ record with key {expected_key}",
        timeout_seconds=45,
        interval_seconds=0.01,
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


def _event_type(engine: Engine, event_id: str) -> str | None:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT event_type FROM edr_events WHERE event_id=:event"),
            {"event": event_id},
        ).scalar_one_or_none()
