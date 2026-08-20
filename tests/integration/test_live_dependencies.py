from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from cassandra import DriverException
from cassandra.cluster import NoHostAvailable
from sqlalchemy import Engine, text

from job_visibility.config import CassandraConfig
from job_visibility.edr_store import KafkaEdrIngress
from job_visibility.model import Event, EventType
from job_visibility.outbox import ConfluentBrokerProducer
from job_visibility.scheduler import CassandraDriverClient
from job_visibility.testing import ToxiproxyClient, poll_until


@pytest.mark.integration
@pytest.mark.cassandra
def test_real_cassandra_read_times_out_through_proxy_and_recovers(
    toxiproxy: ToxiproxyClient,
) -> None:
    config = CassandraConfig(
        contact_points=(os.getenv("CASSANDRA_HOST", "localhost"),),
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        request_timeout_ms=250,
        execution_timeout_ms=2_000,
        max_record_count=16,
        page_size=16,
    )
    client = CassandraDriverClient(config, username="worker", password="worker-local")
    toxic_name = f"read-timeout-{uuid4()}"
    try:
        assert len(client.select_records(dataset_id="local-v1", record_count=4, seed=42)) == 4
        toxiproxy.add_toxic(
            "cassandra",
            toxic_name,
            "timeout",
            stream="downstream",
            attributes={"timeout": 0},
        )
        with pytest.raises((DriverException, NoHostAvailable)):
            client.select_records(dataset_id="local-v1", record_count=4, seed=42)
        toxiproxy.remove_toxic("cassandra", toxic_name)
        recovered = poll_until(
            lambda: client.select_records(dataset_id="local-v1", record_count=4, seed=42),
            lambda rows: len(rows) == 4,
            description="Cassandra reads to recover after proxy timeout",
            timeout_seconds=10,
            interval_seconds=0.25,
        )
        assert len(recovered) == 4
    finally:
        toxiproxy.remove_toxic("cassandra", toxic_name)
        client.close()


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.e2e
def test_real_kafka_connect_path_persists_canonical_edr(edr_engine: Engine) -> None:
    if os.getenv("RUN_RESILIENCE_TESTS") != "1":
        pytest.skip("set RUN_RESILIENCE_TESTS=1 to run the full Kafka path")
    event_id = f"r-kafka-path-{uuid4()}"
    job_id = f"job-{event_id}"
    now = datetime.now(UTC)
    producer = ConfluentBrokerProducer(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        delivery_timeout_ms=10_000,
    )
    try:
        KafkaEdrIngress(producer, topic="job-lifecycle-edr.v1").publish(
            Event(event_id, EventType.JOB_CREATED, now, now, job_id)
        )
        row = poll_until(
            lambda: _event_row(edr_engine, event_id),
            lambda value: value is not None,
            description=f"Kafka Connect to persist {event_id}",
            timeout_seconds=30,
            interval_seconds=0.25,
        )
        assert row.event_id == event_id
        assert row.job_id == job_id
        assert row.kafka_topic == "job-lifecycle-edr.v1"
    finally:
        producer.close(5)


def _event_row(engine: Engine, event_id: str) -> object | None:
    with engine.connect() as connection:
        return connection.execute(
            text("""SELECT event_id,job_id,kafka_topic FROM edr_events
            WHERE event_id=:event_id"""),
            {"event_id": event_id},
        ).first()
