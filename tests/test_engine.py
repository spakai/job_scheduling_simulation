from datetime import UTC, datetime, timedelta

import pytest

from job_visibility.engine import VersionConflictError, VisibilityEngine
from job_visibility.model import Event, EventType, VisibilityConfig

UTC = UTC
NOW = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)


def event(event_id: str, kind: EventType, **values: object) -> Event:
    return Event(
        event_id=event_id,
        event_type=kind,
        event_time=NOW,
        ingestion_time=NOW,
        job_id="job-1",
        **values,
    )


def test_exact_duplicate_does_not_increment_version() -> None:
    engine = VisibilityEngine()
    created = event("one", EventType.JOB_CREATED)
    engine.apply(created)
    engine.apply(created)
    assert engine.get("job-1", NOW)["version"] == 1


def test_success_is_not_regressed_by_late_started() -> None:
    engine = VisibilityEngine()
    engine.apply(event("success", EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1))
    engine.apply(event("start", EventType.JOB_EXECUTION_STARTED, attempt_number=1))
    response = engine.get("job-1", NOW)
    assert response["status"] == "SUCCEEDED"
    assert response["startedAt"] is not None


def test_acknowledgement_does_not_imply_retrieval() -> None:
    engine = VisibilityEngine()
    engine.apply(
        event(
            "ack",
            EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED,
            scheduled_at=NOW,
        )
    )
    response = engine.get("job-1", NOW + timedelta(seconds=61))
    assert response["recordedStatus"] == "SCHEDULED"
    assert response["schedulerRetrievedAt"] is None
    assert response["delay"]["classification"] == "POSSIBLE_BATCH_BACKLOG"


def test_overdue_is_derived_without_erasing_recorded_state() -> None:
    engine = VisibilityEngine(VisibilityConfig(grace_period_seconds=60))
    engine.apply(
        event(
            "ack",
            EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED,
            scheduled_at=NOW,
        )
    )
    response = engine.get("job-1", NOW + timedelta(seconds=61))
    assert response["status"] == "OVERDUE"
    assert response["recordedStatus"] == "SCHEDULED"


def test_optimistic_concurrency_rejects_stale_writer() -> None:
    engine = VisibilityEngine()
    engine.apply(event("one", EventType.JOB_CREATED), expected_version=0)
    with pytest.raises(VersionConflictError):
        engine.apply(event("two", EventType.JOB_SCHEDULER_SUBMISSION_REQUESTED), expected_version=0)
