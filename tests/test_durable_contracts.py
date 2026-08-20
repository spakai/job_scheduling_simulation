import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from job_visibility.api import create_app
from job_visibility.edr_store import KafkaEdrIngress, event_from_wire
from job_visibility.model import Event, EventType
from job_visibility.outbox import BrokerCoordinate, canonical_edr

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


class RecordingProducer:
    def __init__(self) -> None:
        self.records: list[dict[str, str]] = []

    def publish(self, *, topic: str, key: str, value: str) -> BrokerCoordinate:
        self.records.append({"topic": topic, "key": key, "value": value})
        return BrokerCoordinate(1, 2)

    def close(self, timeout: float) -> None:
        pass


def test_canonical_wire_round_trips_into_domain_event() -> None:
    original = Event("event-1", EventType.JOB_CREATED, NOW, NOW, "job-1")
    wire, _ = canonical_edr(original)
    envelope = json.loads(wire)

    restored = event_from_wire(envelope["canonicalPayload"])

    assert restored == original


def test_kafka_ingress_uses_job_id_as_key() -> None:
    producer = RecordingProducer()
    ingress = KafkaEdrIngress(producer, topic="events")

    ingress.publish(Event("event-1", EventType.JOB_CREATED, NOW, NOW, "job-1"))

    assert producer.records[0]["key"] == "job-1"
    assert producer.records[0]["topic"] == "events"


def test_api_exposes_health_and_metrics() -> None:
    client = TestClient(create_app())

    assert client.get("/health/live").json() == {"status": "alive"}
    assert client.get("/health/ready").json()["mode"] == "simulation"
    assert "job_visibility" in client.get("/metrics").text
