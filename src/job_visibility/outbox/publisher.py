from __future__ import annotations

import json
import random
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID, uuid4

from prometheus_client import Counter, Gauge
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

PUBLISHED = Counter("job_visibility_outbox_published_total", "Published lifecycle EDRs")
FAILED = Counter("job_visibility_outbox_failed_total", "Failed lifecycle EDR publications")
BACKLOG = Gauge("job_visibility_outbox_leased", "Outbox records leased in the last batch")


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    event_id: str
    topic: str
    message_key: str
    payload: str
    attempts: int
    lease_token: UUID


@dataclass(frozen=True, slots=True)
class BrokerCoordinate:
    partition: int
    offset: int


class BrokerProducer(Protocol):
    def publish(self, *, topic: str, key: str, value: str) -> BrokerCoordinate: ...
    def close(self, timeout: float) -> None: ...


class ConfluentBrokerProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        *,
        schema_registry_url: str | None = None,
        schema_subject: str = "job-lifecycle-edr.v1-value",
    ) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 30_000,
            }
        )
        self._serializer = None
        if schema_registry_url:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.json_schema import JSONSerializer

            registry = SchemaRegistryClient({"url": schema_registry_url})
            schema = registry.get_latest_version(schema_subject).schema.schema_str
            self._serializer = JSONSerializer(
                schema,
                registry,
                conf={"auto.register.schemas": False, "use.latest.version": True},
            )

    def publish(self, *, topic: str, key: str, value: str) -> BrokerCoordinate:
        delivered: list[object] = []
        errors: list[Exception] = []

        def callback(error: Exception | None, message: object) -> None:
            (errors if error else delivered).append(error or message)

        encoded: bytes = value.encode()
        if self._serializer is not None:
            from confluent_kafka.serialization import MessageField, SerializationContext

            encoded = self._serializer(
                json.loads(value), SerializationContext(topic, MessageField.VALUE)
            )
        self._producer.produce(topic, key=key.encode(), value=encoded, callback=callback)
        self._producer.flush(30)
        if errors or not delivered:
            raise RuntimeError(str(errors[0]) if errors else "Kafka acknowledgement timed out")
        message = delivered[0]
        return BrokerCoordinate(message.partition(), message.offset())  # type: ignore[attr-defined]

    def close(self, timeout: float) -> None:
        self._producer.flush(timeout)


class OutboxPublisher:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        producer: BrokerProducer,
        *,
        owner: str,
        batch_size: int = 100,
        lease_seconds: int = 30,
        retry_initial: float = 1,
        retry_max: float = 60,
        random_source: random.Random | None = None,
    ) -> None:
        self.sessions, self.producer, self.owner = sessions, producer, owner
        self.batch_size, self.lease_seconds = batch_size, lease_seconds
        self.retry_initial, self.retry_max = retry_initial, retry_max
        self.random = random_source or random.Random()
        self.stopping = False

    def run_once(self) -> int:
        if self.stopping:
            return 0
        records = self._lease()
        BACKLOG.set(len(records))
        for record in records:
            try:
                coordinate = self.producer.publish(
                    topic=record.topic, key=record.message_key, value=record.payload
                )
                self._published(record, coordinate)
                PUBLISHED.inc()
            except Exception as exc:
                self._failed(record, type(exc).__name__)
                FAILED.inc()
        return len(records)

    def stop(self, timeout: float = 10) -> None:
        self.stopping = True
        self.producer.close(timeout)

    def _lease(self) -> list[OutboxRecord]:
        token = uuid4()
        with self.sessions.begin() as session:
            rows = session.execute(
                text("""WITH due AS (
                SELECT event_id FROM scheduler_outbox WHERE published_at IS NULL
                  AND next_attempt_at <= clock_timestamp()
                  AND (lease_expires_at IS NULL OR lease_expires_at < clock_timestamp())
                ORDER BY next_attempt_at, created_at FOR UPDATE SKIP LOCKED LIMIT :limit)
              UPDATE scheduler_outbox o SET lease_owner=:owner, lease_token=:token,
                lease_expires_at=clock_timestamp()+make_interval(secs=>:lease)
              FROM due WHERE o.event_id=due.event_id
              RETURNING o.event_id,o.topic,o.message_key,o.canonical_payload,
                        o.publish_attempts,o.lease_token"""),
                {
                    "limit": self.batch_size,
                    "owner": self.owner,
                    "token": token,
                    "lease": self.lease_seconds,
                },
            ).mappings()
            return [
                OutboxRecord(
                    r["event_id"],
                    r["topic"],
                    r["message_key"],
                    r["canonical_payload"],
                    r["publish_attempts"],
                    UUID(str(r["lease_token"])),
                )
                for r in rows
            ]

    def _published(self, record: OutboxRecord, coordinate: BrokerCoordinate) -> None:
        with self.sessions.begin() as session:
            session.execute(
                text("""UPDATE scheduler_outbox SET published_at=clock_timestamp(),
                kafka_partition=:partition,kafka_offset=:offset,lease_owner=NULL,lease_token=NULL,
                lease_expires_at=NULL,last_error=NULL WHERE event_id=:id AND lease_token=:token"""),
                {
                    "id": record.event_id,
                    "token": record.lease_token,
                    "partition": coordinate.partition,
                    "offset": coordinate.offset,
                },
            )

    def _failed(self, record: OutboxRecord, error: str) -> None:
        ceiling = min(self.retry_max, self.retry_initial * 2**record.attempts)
        delay = self.random.uniform(ceiling / 2, ceiling)
        with self.sessions.begin() as session:
            session.execute(
                text("""UPDATE scheduler_outbox SET publish_attempts=publish_attempts+1,
                last_error=:error,next_attempt_at=clock_timestamp()+make_interval(secs=>:delay),
                lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL
                WHERE event_id=:id AND lease_token=:token"""),
                {
                    "id": record.event_id,
                    "token": record.lease_token,
                    "error": error[:200],
                    "delay": delay,
                },
            )
