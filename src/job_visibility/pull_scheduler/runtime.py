from __future__ import annotations

import json
import logging
import signal
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from job_visibility.model import Event, EventType
from job_visibility.outbox.serialization import canonical_edr
from job_visibility.scheduler.handlers import HandlerError, fibonacci

from .config import PullSchedulerConfig
from .contracts import JobResult, PullJobRequest
from .primitives import OwnerGate, TokenBucket

LOGGER = logging.getLogger(__name__)


class PullJobProcessor:
    """Small built-in handler set for the first runnable pull-worker slice."""

    def execute(self, job: PullJobRequest) -> dict[str, Any]:
        payload = job.payload or {}
        if job.job_type == "PRINT":
            message = str(payload.get("message", ""))
            LOGGER.info("pull print job", extra={"job_id": job.job_id, "owner_id": job.owner_id})
            return {"messageLength": len(message)}
        if job.job_type == "FIBONACCI":
            limit = payload.get("limit", 10_000)
            if not isinstance(limit, int) or isinstance(limit, bool) or not 0 <= limit <= 10_000:
                raise HandlerError(
                    "INVALID_FIBONACCI_LIMIT",
                    "limit must be an integer from 0 to 10000",
                    retryable=False,
                )
            values: list[int] = []
            index = 0
            while True:
                value = fibonacci(index)
                if value > limit:
                    break
                values.append(value)
                index += 1
            return {"count": len(values), "lastValue": values[-1] if values else None}
        raise HandlerError("UNKNOWN_JOB_TYPE", "no handler is registered", retryable=False)


class ConfluentPullWorker:
    """Sequential Kafka worker with transactional output and manual source offsets.

    Sequential dispatch is intentional for the first slice: it gives correct partition and
    owner ordering while the later concurrent dispatcher can build on the tested primitives.
    """

    def __init__(
        self, config: PullSchedulerConfig, processor: PullJobProcessor | None = None
    ) -> None:
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.json_schema import JSONSerializer

        self.config = config
        self.processor = processor or PullJobProcessor()
        self.rate_limiter = TokenBucket(config.rate_limit_tps, config.rate_limit_burst)
        self.owner_gate = OwnerGate()
        self.stopping = False
        self.paused = False
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.consumer_group,
                "client.id": config.instance_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "client.id": config.instance_id,
                "transactional.id": config.transactional_id,
                "enable.idempotence": True,
                "acks": "all",
                "transaction.timeout.ms": config.transaction_timeout_ms,
            }
        )
        registry = SchemaRegistryClient({"url": config.schema_registry_url})
        schema = registry.get_latest_version(
            f"{config.lifecycle_topic}-value"
        ).schema.schema_str
        self.edr_serializer = JSONSerializer(
            schema,
            registry,
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        self.producer.init_transactions(config.transaction_timeout_ms / 1_000)
        self.consumer.subscribe([config.work_topic])

    def stop(self) -> None:
        self.stopping = True

    def run_forever(self) -> None:
        while not self.stopping:
            try:
                self.run_once()
            except Exception:
                LOGGER.exception("pull worker iteration failed")

    def run_once(self) -> bool:
        if self.paused:
            self.consumer.poll(0)
            try:
                self.producer.list_topics(timeout=self.config.poll_timeout_seconds)
            except Exception:
                time.sleep(min(1.0, self.config.poll_timeout_seconds))
                return False
            assignments = self.consumer.assignment()
            if assignments:
                self.consumer.resume(assignments)
            self.paused = False

        message = self.consumer.poll(self.config.poll_timeout_seconds)
        if message is None:
            return False
        if message.error():
            raise RuntimeError(str(message.error()))
        if not self.rate_limiter.try_acquire():
            self._pause_and_rewind(message)
            return False

        key = message.key().decode("utf-8") if message.key() else ""
        try:
            job = PullJobRequest.model_validate_json(message.value())
            if key != job.owner_id:
                raise ValueError("Kafka key must exactly equal ownerId")
        except (ValidationError, ValueError, UnicodeDecodeError) as exc:
            self._commit_failure(message, key, None, "INVALID_REQUEST", str(exc))
            return True

        if not self.owner_gate.acquire(job.owner_id):
            raise RuntimeError("owner gate invariant violated in sequential worker")
        try:
            try:
                summary = self.processor.execute(job)
                self._commit_success(message, job, summary)
            except HandlerError as exc:
                self._commit_handler_failure(message, job, exc)
        finally:
            self.owner_gate.release(job.owner_id)
        return True

    def close(self) -> None:
        self.consumer.close()
        self.producer.flush(10)

    def _commit_success(self, message: Any, job: PullJobRequest, summary: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        result = JobResult(
            jobId=job.job_id,
            ownerId=job.owner_id,
            correlationId=job.correlation_id,
            attempt=job.attempt,
            status="SUCCEEDED",
            completedAt=now,
            result=summary,
        )
        event = self._event(job, EventType.JOB_EXECUTION_SUCCEEDED, now, result_code="SUCCESS")
        self._transaction(message, [(self.config.result_topic, job.job_id, result.wire())], event)

    def _commit_handler_failure(self, message: Any, job: PullJobRequest, exc: HandlerError) -> None:
        now = datetime.now(UTC)
        exhausted = not exc.retryable or job.attempt >= job.max_attempts
        status = "FAILED" if not exhausted else "RETRIES_EXHAUSTED"
        result = JobResult(
            jobId=job.job_id,
            ownerId=job.owner_id,
            correlationId=job.correlation_id,
            attempt=job.attempt,
            status=status,
            completedAt=now,
            errorCode=exc.code,
            retryable=exc.retryable,
        )
        outputs: list[tuple[str, str, dict[str, Any]]] = [
            (self.config.result_topic, job.job_id, result.wire())
        ]
        if exc.retryable and not exhausted:
            retry = job.model_copy(update={"attempt": job.attempt + 1})
            outputs.append((self.config.work_topic, job.owner_id, retry.wire()))
        else:
            outputs.append(
                (
                    self.config.dlq_topic,
                    job.owner_id,
                    self._dlq(message, job.wire(), exc.code, str(exc)),
                )
            )
        event_type = (
            EventType.JOB_RETRIES_EXHAUSTED
            if exhausted
            else EventType.JOB_EXECUTION_FAILED
        )
        event = self._event(job, event_type, now, error_code=exc.code, retryable=exc.retryable)
        self._transaction(message, outputs, event)

    def _commit_failure(
        self,
        message: Any,
        key: str,
        job: PullJobRequest | None,
        code: str,
        error: str,
    ) -> None:
        payload = self._dlq(message, self._decoded_value(message), code, error)
        self._transaction(message, [(self.config.dlq_topic, key or "invalid", payload)], None)

    def _transaction(
        self,
        message: Any,
        outputs: list[tuple[str, str, dict[str, Any]]],
        event: Event | None,
    ) -> None:
        from confluent_kafka import TopicPartition

        try:
            self.producer.begin_transaction()
            for topic, key, value in outputs:
                self.producer.produce(topic, key=key.encode(), value=self._json(value))
            if event is not None:
                from confluent_kafka.serialization import MessageField, SerializationContext

                wire, _ = canonical_edr(event)
                self.producer.produce(
                    self.config.lifecycle_topic,
                    key=event.job_id.encode(),
                    value=self.edr_serializer(
                        json.loads(wire),
                        SerializationContext(self.config.lifecycle_topic, MessageField.VALUE),
                    ),
                )
            self.producer.send_offsets_to_transaction(
                [TopicPartition(message.topic(), message.partition(), message.offset() + 1)],
                self.consumer.consumer_group_metadata(),
            )
            self.producer.commit_transaction(self.config.transaction_timeout_ms / 1_000)
        except Exception:
            try:
                self.producer.abort_transaction(self.config.transaction_timeout_ms / 1_000)
            finally:
                self._pause_and_rewind(message)
            raise

    def _pause_and_rewind(self, message: Any) -> None:
        from confluent_kafka import TopicPartition

        self.consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
        assignments = self.consumer.assignment()
        if assignments:
            self.consumer.pause(assignments)
        self.paused = True

    @staticmethod
    def _json(value: dict[str, Any]) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    @staticmethod
    def _decoded_value(message: Any) -> Any:
        try:
            return json.loads(message.value())
        except Exception:
            return {"rawValueUnavailable": True}

    @staticmethod
    def _dlq(message: Any, original: Any, code: str, error: str) -> dict[str, Any]:
        return {
            "schemaVersion": 1,
            "dlqRecordId": str(uuid4()),
            "originalTopic": message.topic(),
            "originalPartition": message.partition(),
            "originalOffset": message.offset(),
            "originalKey": (
                message.key().decode("utf-8", errors="replace") if message.key() else None
            ),
            "originalValue": original,
            "failureCode": code,
            "errorSummary": error[:500],
            "failedAt": datetime.now(UTC).isoformat(),
        }

    @staticmethod
    def _event(job: PullJobRequest, event_type: EventType, now: datetime, **values: Any) -> Event:
        return Event(
            event_id=str(uuid4()),
            event_type=event_type,
            event_time=now,
            ingestion_time=now,
            job_id=job.job_id,
            correlation_id=job.correlation_id,
            job_type=job.job_type,
            attempt_number=job.attempt,
            max_attempts=job.max_attempts,
            **values,
        )


def run_pull_worker(config: PullSchedulerConfig) -> None:
    worker = ConfluentPullWorker(config)

    def stop(_signum: int, _frame: object) -> None:
        worker.stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        worker.run_forever()
    finally:
        worker.close()
