from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

UTC = UTC


class EventType(StrEnum):
    JOB_CREATED = "JOB_CREATED"
    JOB_SCHEDULER_SUBMISSION_REQUESTED = "JOB_SCHEDULER_SUBMISSION_REQUESTED"
    JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED = "JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED"
    JOB_SCHEDULER_SUBMISSION_FAILED = "JOB_SCHEDULER_SUBMISSION_FAILED"
    JOB_SCHEDULER_ITEM_RETRIEVED = "JOB_SCHEDULER_ITEM_RETRIEVED"
    JOB_EXECUTION_STARTED = "JOB_EXECUTION_STARTED"
    JOB_EXECUTION_SUCCEEDED = "JOB_EXECUTION_SUCCEEDED"
    JOB_EXECUTION_FAILED = "JOB_EXECUTION_FAILED"
    JOB_RETRY_REQUESTED = "JOB_RETRY_REQUESTED"
    JOB_RETRY_ACKNOWLEDGED = "JOB_RETRY_ACKNOWLEDGED"
    JOB_RETRY_REJECTED = "JOB_RETRY_REJECTED"
    JOB_RETRIES_EXHAUSTED = "JOB_RETRIES_EXHAUSTED"
    JOB_CANCELLED = "JOB_CANCELLED"


class Status(StrEnum):
    CREATED = "CREATED"
    PENDING_SUBMISSION = "PENDING_SUBMISSION"
    SCHEDULED = "SCHEDULED"
    AWAITING_EXECUTION = "AWAITING_EXECUTION"
    RUNNING = "RUNNING"
    RETRY_PENDING = "RETRY_PENDING"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    OVERDUE = "OVERDUE"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class DelayClassification(StrEnum):
    NOT_YET_ELIGIBLE = "NOT_YET_ELIGIBLE"
    AWAITING_POLL_WINDOW = "AWAITING_POLL_WINDOW"
    POSSIBLE_BATCH_BACKLOG = "POSSIBLE_BATCH_BACKLOG"
    RETRIEVED_AWAITING_WORKER = "RETRIEVED_AWAITING_WORKER"
    EXECUTION_OVERDUE = "EXECUTION_OVERDUE"
    EDR_DELIVERY_DELAY = "EDR_DELIVERY_DELAY"
    CAUSE_UNKNOWN = "CAUSE_UNKNOWN"


TERMINAL_STATUSES = {
    Status.SUCCEEDED,
    Status.FAILED,
    Status.RETRIES_EXHAUSTED,
    Status.CANCELLED,
}


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class Event:
    event_id: str
    event_type: EventType
    event_time: datetime
    ingestion_time: datetime
    job_id: str
    correlation_id: str = ""
    job_type: str = "GENERIC"
    source_system: str = "simulation"
    scheduler_reference: str | None = None
    scheduled_at: datetime | None = None
    attempt_number: int = 0
    retryable: bool | None = None
    max_attempts: int = 3
    next_retry_at: datetime | None = None
    result_code: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    trace_id: str | None = None
    payload_reference: str | None = None
    poll_id: str | None = None
    poll_time: datetime | None = None
    batch_position: int | None = None
    batch_limit: int | None = None

    def __post_init__(self) -> None:
        if self.attempt_number < 0:
            raise ValueError("attempt_number cannot be negative")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["event_type"] = self.event_type.value
        for key in ("event_time", "ingestion_time", "scheduled_at", "next_retry_at", "poll_time"):
            result[key] = iso(result[key])
        return result


@dataclass(slots=True)
class Attempt:
    attempt_number: int
    status: str = "UNKNOWN"
    retrieved_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retryable: bool | None = None
    error_code: str | None = None
    result_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attemptNumber": self.attempt_number,
            "status": self.status,
            "retrievedAt": iso(self.retrieved_at),
            "startedAt": iso(self.started_at),
            "completedAt": iso(self.completed_at),
            "retryable": self.retryable,
            "errorCode": self.error_code,
            "resultCode": self.result_code,
        }


@dataclass(slots=True)
class Finding:
    code: str
    first_observed_at: datetime
    message: str
    active: bool = True
    resolved_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "firstObservedAt": iso(self.first_observed_at),
            "active": self.active,
            "resolvedAt": iso(self.resolved_at),
        }


@dataclass(slots=True)
class ProjectionDecision:
    event_id: str
    decision: str
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JobVisibility:
    job_id: str
    correlation_id: str = ""
    job_type: str = "GENERIC"
    recorded_status: Status = Status.UNKNOWN
    created_at: datetime | None = None
    submission_requested_at: datetime | None = None
    scheduler_acknowledged_at: datetime | None = None
    scheduled_at: datetime | None = None
    scheduler_retrieved_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    scheduler_reference: str | None = None
    max_attempts: int = 3
    next_retry_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error_code: str | None = None
    retry_requested: bool = False
    retry_acknowledged: bool = False
    retry_eligible: bool = False
    retry_requested_at: datetime | None = None
    cancellation_violation: bool = False
    retries_exhausted_violation: bool = False
    incomplete_lifecycle: bool = False
    last_event_time: datetime | None = None
    data_as_of: datetime | None = None
    version: int = 0
    attempts: dict[int, Attempt] = field(default_factory=dict)
    findings: dict[str, Finding] = field(default_factory=dict)
    decisions: list[ProjectionDecision] = field(default_factory=list)

    @property
    def completed_attempts(self) -> int:
        return sum(
            1 for attempt in self.attempts.values() if attempt.status in {"SUCCEEDED", "FAILED"}
        )

    @property
    def next_attempt_number(self) -> int | None:
        if self.recorded_status in TERMINAL_STATUSES and not self.retry_acknowledged:
            return None
        if self.next_retry_at is not None or self.retry_requested:
            return max(self.attempts, default=0) + 1
        return None


@dataclass(slots=True)
class VisibilityConfig:
    poll_interval_seconds: int = 60
    max_items_per_poll: int = 100
    worker_capacity: int = 20
    grace_period_seconds: int = 600
    running_timeout_seconds: int = 1800
    retry_acknowledgement_timeout_seconds: int = 120
    submission_acknowledgement_timeout_seconds: int = 120
    eventual_consistency_target_seconds: int = 10


@dataclass(slots=True)
class PollRecord:
    poll_id: str
    poll_time: datetime
    eligible_count: int
    retrieved_job_ids: list[str]
    batch_limit: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "pollId": self.poll_id,
            "pollTime": iso(self.poll_time),
            "eligibleCount": self.eligible_count,
            "retrievedJobIds": self.retrieved_job_ids,
            "batchLimit": self.batch_limit,
        }
