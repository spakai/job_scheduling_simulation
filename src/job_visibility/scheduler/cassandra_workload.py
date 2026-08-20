from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from job_visibility.config import CassandraConfig

from .handlers import HandlerError, fibonacci
from .models import ClaimedJob, HandlerResult


@dataclass(frozen=True, slots=True)
class WorkloadRecord:
    dataset_id: str
    bucket: int
    record_id: int
    input_number: int
    checksum: int


class CassandraWorkloadClient(Protocol):
    """Driver boundary; implementations reconcile unknown writes by operation marker."""

    def select_records(
        self, *, dataset_id: str, record_count: int, seed: int
    ) -> Sequence[WorkloadRecord]: ...

    def apply_once(
        self, *, record: WorkloadRecord, operation_id: UUID, fibonacci_result: str
    ) -> tuple[bool, int]: ...


def stable_operation_id(job: ClaimedJob) -> UUID:
    return uuid5(NAMESPACE_URL, f"job-visibility:cassandra-fib-update:v1:{job.job_id}")


def build_cassandra_handler(
    client: CassandraWorkloadClient,
    config: CassandraConfig,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[ClaimedJob], HandlerResult]:
    def execute(job: ClaimedJob) -> HandlerResult:
        payload = job.payload or {}
        dataset_id = payload.get("datasetId")
        record_count = payload.get("recordCount")
        seed = payload.get("seed")
        delay_ms = payload.get("processingDelayMs", 0)
        if not isinstance(dataset_id, str) or not dataset_id:
            raise HandlerError("INVALID_DATASET", "datasetId is required", retryable=False)
        if (
            not isinstance(record_count, int)
            or isinstance(record_count, bool)
            or not 1 <= record_count <= config.max_record_count
        ):
            raise HandlerError(
                "INVALID_RECORD_COUNT", "recordCount exceeds configured bounds", retryable=False
            )
        if not isinstance(seed, int) or isinstance(seed, bool):
            raise HandlerError("INVALID_SEED", "seed must be an integer", retryable=False)
        if (
            not isinstance(delay_ms, int)
            or isinstance(delay_ms, bool)
            or not 0 <= delay_ms <= config.max_processing_delay_ms
        ):
            raise HandlerError(
                "INVALID_PROCESSING_DELAY",
                "processing delay exceeds configured bounds",
                retryable=False,
            )

        records = list(
            client.select_records(dataset_id=dataset_id, record_count=record_count, seed=seed)
        )
        if len(records) != record_count:
            raise HandlerError(
                "RECORD_COUNT_MISMATCH", "dataset returned an incomplete selection", retryable=False
            )
        selected = max(records, key=lambda item: (item.input_number, -item.bucket, -item.record_id))
        if selected.input_number > config.max_input_number:
            raise HandlerError(
                "INPUT_BOUND_EXCEEDED", "selected input exceeds configured bounds", retryable=False
            )
        result = str(fibonacci(selected.input_number))
        if delay_ms:
            sleep(delay_ms / 1000)
        operation_id = stable_operation_id(job)
        try:
            applied, checksum = client.apply_once(
                record=selected, operation_id=operation_id, fibonacci_result=result
            )
        except Exception as exc:
            raise HandlerError(
                "CASSANDRA_OPERATION_FAILED", "Cassandra operation failed", retryable=True
            ) from exc
        if not applied:
            raise HandlerError(
                "CASSANDRA_CONDITIONAL_CONFLICT", "conditional update conflicted", retryable=True
            )
        summary = {
            "rows": len(records),
            "maximum": selected.input_number,
            "bucket": selected.bucket,
            "recordId": selected.record_id,
            "operationId": str(operation_id),
            "previousChecksum": selected.checksum,
            "checksum": checksum,
            "delayMs": delay_ms,
            "resultHash": hashlib.sha256(result.encode()).hexdigest(),
        }
        # Round-trip enforces a compact JSON-compatible summary before persistence.
        return HandlerResult(json.loads(json.dumps(summary, separators=(",", ":"))))

    return execute
