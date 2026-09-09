from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from job_visibility.config import ConfigurationError
from job_visibility.pull_scheduler import (
    BackpressureController,
    BackpressureState,
    ContiguousOffsetTracker,
    OwnerGate,
    PullJobRequest,
    TokenBucket,
    pull_scheduler_config_from_env,
)


def request(**changes: object) -> PullJobRequest:
    values: dict[str, object] = {
        "jobId": "job-1",
        "ownerId": "subscriber:123",
        "ownerType": "SUBSCRIBER",
        "correlationId": "correlation-1",
        "jobType": "PRINT",
        "requestedAt": datetime(2026, 9, 6, tzinfo=UTC),
        "payload": {"message": "hello"},
    }
    return PullJobRequest(**(values | changes))


def test_request_uses_namespaced_owner_contract() -> None:
    assert request().owner_id == "subscriber:123"
    assert request(ownerId="group:9", ownerType="GROUP").wire()["ownerId"] == "group:9"

    with pytest.raises(ValidationError, match="subscriber:"):
        request(ownerId="group:9")
    with pytest.raises(ValidationError, match="timezone-aware"):
        request(requestedAt=datetime(2026, 9, 6))


def test_request_requires_exactly_one_payload_source() -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        request(payload=None)
    with pytest.raises(ValidationError, match="exactly one"):
        request(payloadReference="ref")


def test_contiguous_offsets_never_commit_across_a_gap() -> None:
    tracker = ContiguousOffsetTracker(100)

    assert tracker.complete(100) == 101
    assert tracker.complete(102) == 101
    assert tracker.complete(101) == 103


def test_owner_gate_allows_only_one_active_job_per_owner() -> None:
    gate = OwnerGate()

    assert gate.acquire("subscriber:1") is True
    assert gate.acquire("subscriber:1") is False
    assert gate.acquire("subscriber:2") is True
    gate.release("subscriber:1")
    assert gate.acquire("subscriber:1") is True


def test_token_bucket_enforces_burst_and_refill() -> None:
    now = [0.0]
    bucket = TokenBucket(2, 2, clock=lambda: now[0])

    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    now[0] = 0.5
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_backpressure_requires_cooldown_and_recovery_threshold() -> None:
    now = [0.0]
    controller = BackpressureController(
        recovery_threshold=2,
        initial_backoff_seconds=1,
        max_backoff_seconds=4,
        clock=lambda: now[0],
    )

    assert controller.failure() is BackpressureState.PAUSED
    assert controller.should_probe() is False
    now[0] = 1
    assert controller.should_probe() is True
    assert controller.success() is BackpressureState.PROBING
    assert controller.should_probe() is True
    assert controller.success() is BackpressureState.RUNNING


def test_pull_config_is_database_free_and_validates_topics() -> None:
    config = pull_scheduler_config_from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "PULL_WORKER_INSTANCE_ID": "pod-1",
            "KAFKA_TRANSACTIONAL_ID": "worker-pod-1",
            "PULL_WORKER_TPS": "12.5",
            "PULL_WORKER_BURST": "20",
        }
    )

    assert config.consumer_group == "job-workers-v1"
    assert config.rate_limit_tps == 12.5
    assert config.transactional_id == "worker-pod-1"

    with pytest.raises(ConfigurationError, match="invalid pull scheduler"):
        pull_scheduler_config_from_env(
            {
                "PULL_WORKER_INSTANCE_ID": "pod-1",
                "KAFKA_WORK_TOPIC": "same",
                "KAFKA_RESULT_TOPIC": "same",
            }
        )
