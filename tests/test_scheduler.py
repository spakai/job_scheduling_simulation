from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from job_visibility.model import Event, EventType
from job_visibility.outbox import canonical_edr
from job_visibility.scheduler import (
    ClaimedJob,
    HandlerError,
    HandlerRegistry,
    JobSubmission,
    WorkloadRecord,
    build_cassandra_handler,
    fibonacci,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def claimed(job_type: str, payload: dict[str, object]) -> ClaimedJob:
    return ClaimedJob(
        job_id="job-1",
        correlation_id="correlation-1",
        job_type=job_type,
        payload=payload,
        payload_reference=None,
        scheduled_at=NOW,
        attempt_number=1,
        max_attempts=3,
        claimed_by="worker-1",
        claim_token=UUID("00000000-0000-0000-0000-000000000001"),
        claim_expires_at=NOW + timedelta(minutes=1),
    )


def test_fibonacci_handler_is_bounded_and_returns_expected_summary() -> None:
    result = HandlerRegistry().execute(claimed("FIBONACCI", {"limit": 10_000}))

    assert fibonacci(20) == 6765
    assert result.summary["lastValue"] == 6765
    assert result.summary["count"] == 21
    assert len(result.summary["resultHash"]) == 64


@pytest.mark.parametrize("limit", [-1, 10_001, 1.5, True])
def test_fibonacci_handler_rejects_invalid_or_unbounded_input(limit: object) -> None:
    with pytest.raises(HandlerError, match="limit") as caught:
        HandlerRegistry().execute(claimed("FIBONACCI", {"limit": limit}))

    assert caught.value.retryable is False


def test_submission_requires_exactly_one_payload_source() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        JobSubmission("job-1", "correlation-1", "PRINT", NOW, {}, "payload://1")


def test_canonical_edr_is_stable_and_hashed() -> None:
    value = Event(
        event_id="event-1",
        event_type=EventType.JOB_CREATED,
        event_time=NOW,
        ingestion_time=NOW,
        job_id="job-1",
    )

    first = canonical_edr(value)
    second = canonical_edr(value)

    assert first == second
    assert '"canonicalPayload":"{' in first[0]
    assert f'"payloadHash":"{first[1]}"' in first[0]
    assert len(first[1]) == 64


def test_cassandra_handler_selects_deterministic_maximum_and_uses_stable_operation() -> None:
    class Client:
        calls: list[object] = []

        def select_records(self, **values: object) -> list[WorkloadRecord]:
            self.calls.append(values)
            return [
                WorkloadRecord("local-v1", 1, 9, 20, 4),
                WorkloadRecord("local-v1", 0, 8, 20, 7),
            ]

        def apply_once(self, **values: object) -> tuple[bool, int]:
            self.calls.append(values)
            return True, 8

    from job_visibility.config import CassandraConfig

    client = Client()
    handler = build_cassandra_handler(
        client, CassandraConfig(max_record_count=2, page_size=2), sleep=lambda _: None
    )
    job = claimed(
        "CASSANDRA_FIB_UPDATE",
        {"datasetId": "local-v1", "recordCount": 2, "seed": 42, "processingDelayMs": 1},
    )

    first = handler(job)
    second = handler(job)

    assert first.summary["recordId"] == 8
    assert first.summary["checksum"] == 8
    assert first.summary["operationId"] == second.summary["operationId"]
