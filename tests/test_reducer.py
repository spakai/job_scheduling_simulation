from datetime import UTC, datetime, timedelta

import pytest

from job_visibility.engine import VersionConflictError, reduce_visibility
from job_visibility.model import Event, EventType, JobVisibility, Status
from job_visibility.persistence import InMemoryVisibilityRepository

NOW = datetime(2026, 8, 20, 8, tzinfo=UTC)


def event(event_id: str, event_type: EventType, **changes: object) -> Event:
    values = {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": NOW,
        "ingestion_time": NOW + timedelta(seconds=1),
        "job_id": "job-1",
        "attempt_number": 1,
    }
    values.update(changes)
    return Event(**values)  # type: ignore[arg-type]


def test_reducer_is_deterministic_and_does_not_mutate_input() -> None:
    current = JobVisibility(job_id="job-1", recorded_status=Status.SCHEDULED, version=3)
    incoming = event("start-1", EventType.JOB_EXECUTION_STARTED)

    first = reduce_visibility(current, incoming)
    second = reduce_visibility(current, incoming)

    assert first == second
    assert current.recorded_status is Status.SCHEDULED
    assert current.version == 3
    assert first.job.recorded_status is Status.RUNNING
    assert first.job.version == 4


def test_reducer_handles_semantic_duplicate_without_incrementing_version() -> None:
    succeeded = reduce_visibility(None, event("success-1", EventType.JOB_EXECUTION_SUCCEEDED))
    duplicate = reduce_visibility(
        succeeded.job,
        event("success-2", EventType.JOB_EXECUTION_SUCCEEDED),
    )

    assert duplicate.decision.decision == "SEMANTIC_DUPLICATE"
    assert duplicate.job.version == succeeded.job.version
    assert len(duplicate.job.decisions) == 2


def test_in_memory_repository_is_copy_isolated_and_checks_versions() -> None:
    repository = InMemoryVisibilityRepository()
    job = JobVisibility(job_id="job-1", version=1)
    repository.save(job, expected_version=0)

    loaded = repository.require("job-1")
    loaded.recorded_status = Status.RUNNING

    assert repository.require("job-1").recorded_status is Status.UNKNOWN
    assert repository.job_ids() == ["job-1"]
    with pytest.raises(VersionConflictError):
        repository.save(loaded, expected_version=0)
