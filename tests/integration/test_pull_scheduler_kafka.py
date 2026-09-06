from __future__ import annotations

import os
from uuid import uuid4

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


def test_same_owner_key_stays_on_one_partition() -> None:
    if os.getenv("RUN_PULL_KAFKA_TESTS") != "1":
        pytest.skip("set RUN_PULL_KAFKA_TESTS=1 to run pull-worker Kafka tests")

    from confluent_kafka import Producer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_WORK_TOPIC", "job-requests.v1")
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    partitions: list[int] = []
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error:
            errors.append(str(error))
        else:
            partitions.append(message.partition())  # type: ignore[attr-defined]

    owner = f"subscriber:integration-{uuid4()}"
    for index in range(20):
        producer.produce(topic, key=owner.encode(), value=str(index).encode(), callback=delivered)
    assert producer.flush(15) == 0
    assert errors == []
    assert len(partitions) == 20
    assert len(set(partitions)) == 1
