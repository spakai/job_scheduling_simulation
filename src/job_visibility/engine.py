from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from threading import RLock
from typing import Any

from job_visibility.model import (
    TERMINAL_STATUSES,
    Attempt,
    DelayClassification,
    Event,
    EventType,
    Finding,
    JobVisibility,
    ProjectionDecision,
    Status,
    VisibilityConfig,
    iso,
)


class JobNotFoundError(KeyError):
    pass


class VersionConflictError(RuntimeError):
    pass


class VisibilityEngine:
    """Thread-safe in-memory EDR store and visibility projector.

    Projection rules are independent of wall-clock time. A persistent repository can
    replace the dictionaries without changing the domain behavior.
    """

    def __init__(self, config: VisibilityConfig | None = None) -> None:
        self.config = config or VisibilityConfig()
        self._jobs: dict[str, JobVisibility] = {}
        self._event_ids: set[str] = set()
        self._events: list[Event] = []
        self._lock = RLock()

    @property
    def events(self) -> tuple[Event, ...]:
        return tuple(self._events)

    def job_ids(self) -> list[str]:
        return sorted(self._jobs)

    def apply(self, event: Event, expected_version: int | None = None) -> ProjectionDecision:
        with self._lock:
            if event.event_id in self._event_ids:
                job = self._jobs.get(event.job_id)
                decision = ProjectionDecision(event.event_id, "EXACT_DUPLICATE", "eventId seen")
                if job is not None:
                    job.decisions.append(decision)
                return decision

            current = self._jobs.get(event.job_id)
            if expected_version is not None:
                actual = current.version if current else 0
                if expected_version != actual:
                    raise VersionConflictError(
                        f"job {event.job_id}: expected version {expected_version}, actual {actual}"
                    )

            self._event_ids.add(event.event_id)
            self._events.append(event)
            job = current or JobVisibility(job_id=event.job_id)
            self._jobs[event.job_id] = job

            if self._is_semantic_duplicate(job, event):
                decision = ProjectionDecision(
                    event.event_id,
                    "SEMANTIC_DUPLICATE",
                    "same outcome already observed for job attempt",
                )
                job.decisions.append(decision)
                job.last_event_time = self._latest(job.last_event_time, event.event_time)
                job.data_as_of = self._latest(job.data_as_of, event.ingestion_time)
                return decision

            decision = self._project(job, event)
            job.decisions.append(decision)
            if decision.decision != "IGNORED":
                job.version += 1
            job.last_event_time = self._latest(job.last_event_time, event.event_time)
            job.data_as_of = self._latest(job.data_as_of, event.ingestion_time)
            return decision

    def apply_with_retry(
        self, event: Event, expected_version: int, retries: int = 3
    ) -> ProjectionDecision:
        """Model an optimistic writer that reloads and reapplies after a conflict."""
        for number in range(retries + 1):
            try:
                return self.apply(event, expected_version=expected_version)
            except VersionConflictError:
                if number == retries:
                    raise
                expected_version = self._jobs[event.job_id].version
        raise AssertionError("unreachable")

    def _project(self, job: JobVisibility, event: Event) -> ProjectionDecision:
        job.correlation_id = job.correlation_id or event.correlation_id
        job.job_type = event.job_type or job.job_type
        job.max_attempts = max(job.max_attempts, event.max_attempts)
        job.scheduled_at = job.scheduled_at or event.scheduled_at
        if event.scheduler_reference:
            job.scheduler_reference = event.scheduler_reference

        handler = getattr(self, f"_on_{event.event_type.value.lower()}")
        changed, reason = handler(job, event)
        return ProjectionDecision(event.event_id, "APPLIED" if changed else "IGNORED", reason)

    def _on_job_created(self, job: JobVisibility, event: Event) -> tuple[bool, str | None]:
        job.created_at = self._earliest(job.created_at, event.event_time)
        if job.recorded_status is Status.UNKNOWN:
            job.recorded_status = Status.CREATED
        return True, None

    def _on_job_scheduler_submission_requested(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        job.submission_requested_at = self._earliest(job.submission_requested_at, event.event_time)
        if job.recorded_status not in TERMINAL_STATUSES and job.recorded_status not in {
            Status.RUNNING,
            Status.RETRY_PENDING,
            Status.RETRY_SCHEDULED,
        }:
            job.recorded_status = Status.PENDING_SUBMISSION
        return True, None

    def _on_job_scheduler_submission_acknowledged(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        job.scheduler_acknowledged_at = self._earliest(
            job.scheduler_acknowledged_at, event.event_time
        )
        if job.recorded_status not in TERMINAL_STATUSES and job.recorded_status not in {
            Status.RUNNING,
            Status.RETRY_PENDING,
            Status.RETRY_SCHEDULED,
            Status.AWAITING_EXECUTION,
        }:
            job.recorded_status = Status.SCHEDULED
        self._resolve(job, "SCHEDULER_ACK_TIMEOUT", event.ingestion_time)
        return True, None

    def _on_job_scheduler_submission_failed(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        if job.recorded_status in TERMINAL_STATUSES:
            return False, "terminal state preserved"
        job.recorded_status = Status.FAILED
        job.completed_at = self._latest(job.completed_at, event.event_time)
        return True, None

    def _on_job_scheduler_item_retrieved(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        attempt = self._attempt(job, event)
        retrieved_at = event.poll_time or event.event_time
        attempt.retrieved_at = self._earliest(attempt.retrieved_at, retrieved_at)
        if attempt.status == "UNKNOWN":
            attempt.status = "RETRIEVED"
        job.scheduler_retrieved_at = self._earliest(job.scheduler_retrieved_at, retrieved_at)
        if (
            job.recorded_status not in TERMINAL_STATUSES
            and job.recorded_status is not Status.RUNNING
        ):
            job.recorded_status = Status.AWAITING_EXECUTION
        return True, None

    def _on_job_execution_started(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        attempt = self._attempt(job, event)
        if attempt.retrieved_at is None:
            job.incomplete_lifecycle = True
        attempt.started_at = self._earliest(attempt.started_at, event.event_time)
        job.started_at = self._earliest(job.started_at, event.event_time)
        if attempt.status == "SUCCEEDED" or job.recorded_status is Status.SUCCEEDED:
            return True, "startedAt backfilled; successful terminal state preserved"
        if attempt.status == "FAILED" and job.recorded_status is Status.FAILED:
            return True, "startedAt backfilled; failed terminal state preserved"
        attempt.status = "RUNNING"
        if job.recorded_status is Status.CANCELLED:
            job.cancellation_violation = True
            self._finding(
                job,
                "EXECUTED_AFTER_CANCEL",
                event.ingestion_time,
                "Execution was observed after cancellation.",
            )
        if job.recorded_status is Status.RETRIES_EXHAUSTED:
            job.retries_exhausted_violation = True
            self._finding(
                job,
                "EXECUTION_AFTER_RETRY_EXHAUSTION",
                event.ingestion_time,
                "Execution was observed after retries were exhausted.",
            )
        job.recorded_status = Status.RUNNING
        return True, None

    def _on_job_execution_succeeded(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        attempt = self._attempt(job, event)
        if attempt.status == "FAILED" or (
            job.recorded_status in TERMINAL_STATUSES
            and job.recorded_status not in {Status.SUCCEEDED, Status.CANCELLED}
        ):
            self._terminal_conflict(job, event)
            return True, "conflicting terminal outcome retained; prior terminal state preserved"
        if attempt.started_at is None:
            job.incomplete_lifecycle = True
        if job.recorded_status is Status.CANCELLED:
            job.cancellation_violation = True
            self._finding(
                job,
                "EXECUTED_AFTER_CANCEL",
                event.ingestion_time,
                "Successful execution was observed after cancellation.",
            )
        attempt.status = "SUCCEEDED"
        attempt.completed_at = self._latest(attempt.completed_at, event.event_time)
        attempt.result_code = event.result_code or "SUCCESS"
        job.completed_at = self._latest(job.completed_at, event.event_time)
        job.recorded_status = Status.SUCCEEDED
        job.retry_eligible = False
        job.next_retry_at = None
        return True, None

    def _on_job_execution_failed(self, job: JobVisibility, event: Event) -> tuple[bool, str | None]:
        attempt = self._attempt(job, event)
        if attempt.status == "SUCCEEDED" or job.recorded_status is Status.SUCCEEDED:
            self._terminal_conflict(job, event)
            return True, "conflicting late failure retained; success preserved"
        if job.recorded_status in TERMINAL_STATUSES and job.recorded_status not in {
            Status.CANCELLED,
        }:
            self._terminal_conflict(job, event)
            return True, "terminal state preserved"
        if attempt.started_at is None:
            job.incomplete_lifecycle = True
        attempt.status = "FAILED"
        attempt.completed_at = self._latest(attempt.completed_at, event.event_time)
        attempt.retryable = event.retryable
        attempt.error_code = event.error_code
        job.completed_at = self._latest(job.completed_at, event.event_time)
        job.last_failure_at = self._latest(job.last_failure_at, event.event_time)
        job.last_error_code = event.error_code
        job.retry_eligible = bool(event.retryable)
        job.retry_requested = False
        job.retry_acknowledged = False
        job.next_retry_at = None
        job.recorded_status = Status.RETRY_PENDING if event.retryable else Status.FAILED
        return True, None

    def _on_job_retry_requested(self, job: JobVisibility, event: Event) -> tuple[bool, str | None]:
        if job.recorded_status in TERMINAL_STATUSES:
            return False, "terminal state preserved"
        job.retry_requested = True
        job.retry_acknowledged = False
        job.retry_requested_at = self._latest(job.retry_requested_at, event.event_time)
        job.next_retry_at = None
        job.recorded_status = Status.RETRY_PENDING
        return True, None

    def _on_job_retry_acknowledged(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        job.retry_requested = True
        job.retry_acknowledged = True
        job.next_retry_at = event.next_retry_at or job.next_retry_at
        self._resolve(job, "RETRY_ACK_TIMEOUT", event.ingestion_time)
        if (
            job.recorded_status not in TERMINAL_STATUSES
            and job.recorded_status is not Status.RUNNING
        ):
            job.recorded_status = Status.RETRY_SCHEDULED
        return True, None

    def _on_job_retry_rejected(self, job: JobVisibility, event: Event) -> tuple[bool, str | None]:
        if job.recorded_status in TERMINAL_STATUSES:
            return False, "terminal state preserved"
        job.retry_requested = True
        job.retry_acknowledged = False
        job.retry_eligible = False
        job.next_retry_at = None
        job.recorded_status = Status.FAILED
        return True, None

    def _on_job_retries_exhausted(
        self, job: JobVisibility, event: Event
    ) -> tuple[bool, str | None]:
        if job.recorded_status is Status.SUCCEEDED:
            return False, "successful terminal state preserved"
        job.recorded_status = Status.RETRIES_EXHAUSTED
        job.retry_eligible = False
        job.retry_acknowledged = False
        job.next_retry_at = None
        job.completed_at = self._latest(job.completed_at, event.event_time)
        return True, None

    def _on_job_cancelled(self, job: JobVisibility, event: Event) -> tuple[bool, str | None]:
        if job.recorded_status in TERMINAL_STATUSES:
            return False, "existing terminal state preserved"
        job.recorded_status = Status.CANCELLED
        job.completed_at = self._latest(job.completed_at, event.event_time)
        return True, None

    def reconcile(self, now: datetime) -> list[Finding]:
        observed: list[Finding] = []
        for job in self._jobs.values():
            if (
                job.recorded_status is Status.PENDING_SUBMISSION
                and job.submission_requested_at
                and now - job.submission_requested_at
                > timedelta(seconds=self.config.submission_acknowledgement_timeout_seconds)
            ):
                observed.append(
                    self._finding(
                        job,
                        "SCHEDULER_ACK_TIMEOUT",
                        now,
                        "No scheduler acknowledgement was observed before the timeout.",
                    )
                )
            if (
                job.recorded_status is Status.RUNNING
                and job.started_at
                and now - job.started_at > timedelta(seconds=self.config.running_timeout_seconds)
            ):
                observed.append(
                    self._finding(
                        job,
                        "RUNNING_TIMEOUT",
                        now,
                        "Execution has remained running beyond the configured timeout.",
                    )
                )
            if (
                job.retry_requested
                and not job.retry_acknowledged
                and job.retry_requested_at
                and now - job.retry_requested_at
                > timedelta(seconds=self.config.retry_acknowledgement_timeout_seconds)
            ):
                observed.append(
                    self._finding(
                        job,
                        "RETRY_ACK_TIMEOUT",
                        now,
                        "No retry acknowledgement was observed before the timeout.",
                    )
                )
            if self._is_overdue(job, now):
                observed.append(
                    self._finding(
                        job,
                        "EXECUTION_START_OVERDUE",
                        now,
                        "No execution start was observed before the business grace period expired.",
                    )
                )
            attempts = sorted(job.attempts)
            if attempts and attempts != list(range(1, max(attempts) + 1)):
                observed.append(
                    self._finding(
                        job,
                        "ATTEMPT_SEQUENCE_GAP",
                        now,
                        "Observed attempt numbers contain a gap; "
                        "missing attempts were not inferred.",
                    )
                )
        return observed

    def get(self, job_id: str, now: datetime) -> dict[str, Any]:
        with self._lock:
            if job_id not in self._jobs:
                raise JobNotFoundError(job_id)
            job = deepcopy(self._jobs[job_id])
        return self._response(job, now)

    def attempts(self, job_id: str) -> list[dict[str, Any]]:
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)
        return [
            self._jobs[job_id].attempts[key].to_dict()
            for key in sorted(self._jobs[job_id].attempts)
        ]

    def search(
        self,
        now: datetime,
        *,
        status: Status | None = None,
        correlation_id: str | None = None,
        scheduled_from: datetime | None = None,
        scheduled_to: datetime | None = None,
    ) -> list[dict[str, Any]]:
        responses = [self.get(job_id, now) for job_id in self.job_ids()]
        if status:
            responses = [item for item in responses if item["status"] == status.value]
        if correlation_id:
            responses = [item for item in responses if item["correlationId"] == correlation_id]
        if scheduled_from:
            responses = [
                item
                for item in responses
                if self._jobs[item["jobId"]].scheduled_at
                and self._jobs[item["jobId"]].scheduled_at >= scheduled_from
            ]
        if scheduled_to:
            responses = [
                item
                for item in responses
                if self._jobs[item["jobId"]].scheduled_at
                and self._jobs[item["jobId"]].scheduled_at <= scheduled_to
            ]
        return responses

    def snapshot(self, job_id: str) -> JobVisibility:
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)
        return deepcopy(self._jobs[job_id])

    def _response(self, job: JobVisibility, now: datetime) -> dict[str, Any]:
        status = self._derived_status(job, now)
        delay_classification = self._delay_classification(job, now, status)
        start_delay = None
        if job.scheduled_at:
            endpoint = job.started_at or now
            start_delay = max(0, int((endpoint - job.scheduled_at).total_seconds()))
        post_retrieval = None
        if job.scheduler_retrieved_at:
            endpoint = job.started_at or now
            post_retrieval = max(0, int((endpoint - job.scheduler_retrieved_at).total_seconds()))
        possibly_stale = bool(
            job.data_as_of
            and now - job.data_as_of
            > timedelta(seconds=self.config.eventual_consistency_target_seconds)
        )
        processing_delay = None
        if job.data_as_of and job.last_event_time:
            processing_delay = max(0, int((job.data_as_of - job.last_event_time).total_seconds()))
        return {
            "jobId": job.job_id,
            "correlationId": job.correlation_id,
            "jobType": job.job_type,
            "status": status.value,
            "recordedStatus": job.recorded_status.value,
            "createdAt": iso(job.created_at),
            "submissionRequestedAt": iso(job.submission_requested_at),
            "schedulerAcknowledgedAt": iso(job.scheduler_acknowledged_at),
            "scheduledAt": iso(job.scheduled_at),
            "schedulerRetrievedAt": iso(job.scheduler_retrieved_at),
            "startedAt": iso(job.started_at),
            "completedAt": iso(job.completed_at),
            "schedulerReference": job.scheduler_reference,
            "retry": {
                "completedAttempts": job.completed_attempts,
                "nextAttemptNumber": job.next_attempt_number,
                "maxAttempts": job.max_attempts,
                "eligible": job.retry_eligible,
                "requested": job.retry_requested,
                "schedulerAcknowledged": job.retry_acknowledged,
                "lastFailureAt": iso(job.last_failure_at),
                "lastErrorCode": job.last_error_code,
                "nextRetryAt": iso(job.next_retry_at),
            },
            "delay": {
                "classification": delay_classification.value if delay_classification else None,
                "startDelaySeconds": start_delay,
                "postRetrievalDelaySeconds": post_retrieval,
                "overdueDurationSeconds": self._overdue_duration(job, now),
            },
            "incompleteLifecycle": job.incomplete_lifecycle,
            "cancellationViolation": job.cancellation_violation,
            "retriesExhaustedViolation": job.retries_exhausted_violation,
            "freshness": {
                "dataAsOf": iso(job.data_as_of),
                "processingDelaySeconds": processing_delay,
                "possiblyStale": possibly_stale,
            },
            "dataAsOf": iso(job.data_as_of),
            "version": job.version,
            "reconciliationFindings": [finding.to_dict() for finding in job.findings.values()],
        }

    def _derived_status(self, job: JobVisibility, now: datetime) -> Status:
        if job.recorded_status in TERMINAL_STATUSES or job.recorded_status in {
            Status.RUNNING,
            Status.RETRY_PENDING,
            Status.RETRY_SCHEDULED,
        }:
            return job.recorded_status
        if (
            job.recorded_status in {Status.SCHEDULED, Status.AWAITING_EXECUTION}
            and job.scheduled_at
        ):
            if self._is_overdue(job, now):
                return Status.OVERDUE
            if now >= job.scheduled_at:
                return Status.AWAITING_EXECUTION
        return job.recorded_status

    def _delay_classification(
        self, job: JobVisibility, now: datetime, status: Status
    ) -> DelayClassification | None:
        if not job.scheduled_at or job.started_at:
            return None
        if now < job.scheduled_at:
            return DelayClassification.NOT_YET_ELIGIBLE
        if status is Status.OVERDUE:
            return DelayClassification.EXECUTION_OVERDUE
        if job.scheduler_retrieved_at:
            return DelayClassification.RETRIEVED_AWAITING_WORKER
        first_poll = job.scheduled_at + timedelta(seconds=self.config.poll_interval_seconds)
        if now < first_poll:
            return DelayClassification.AWAITING_POLL_WINDOW
        return DelayClassification.POSSIBLE_BATCH_BACKLOG

    def _is_overdue(self, job: JobVisibility, now: datetime) -> bool:
        return bool(
            job.scheduled_at
            and not job.started_at
            and job.recorded_status in {Status.SCHEDULED, Status.AWAITING_EXECUTION}
            and now > job.scheduled_at + timedelta(seconds=self.config.grace_period_seconds)
        )

    def _overdue_duration(self, job: JobVisibility, now: datetime) -> int:
        if not self._is_overdue(job, now) or not job.scheduled_at:
            return 0
        threshold = job.scheduled_at + timedelta(seconds=self.config.grace_period_seconds)
        return max(0, int((now - threshold).total_seconds()))

    def _attempt(self, job: JobVisibility, event: Event) -> Attempt:
        number = event.attempt_number or 1
        if number > 1 and any(missing not in job.attempts for missing in range(1, number)):
            job.incomplete_lifecycle = True
            self._finding(
                job,
                "ATTEMPT_SEQUENCE_GAP",
                event.ingestion_time,
                "Observed attempt number has no complete preceding sequence.",
            )
        return job.attempts.setdefault(number, Attempt(attempt_number=number))

    @staticmethod
    def _is_semantic_duplicate(job: JobVisibility, event: Event) -> bool:
        number = event.attempt_number or 1
        attempt = job.attempts.get(number)
        if not attempt:
            return False
        return (
            event.event_type is EventType.JOB_EXECUTION_SUCCEEDED and attempt.status == "SUCCEEDED"
        ) or (event.event_type is EventType.JOB_EXECUTION_FAILED and attempt.status == "FAILED")

    def _terminal_conflict(self, job: JobVisibility, event: Event) -> None:
        job.incomplete_lifecycle = True
        self._finding(
            job,
            "CONFLICTING_TERMINAL_OUTCOME",
            event.ingestion_time,
            "Conflicting terminal execution outcomes were observed.",
        )

    @staticmethod
    def _finding(job: JobVisibility, code: str, observed_at: datetime, message: str) -> Finding:
        existing = job.findings.get(code)
        if existing:
            existing.active = True
            existing.resolved_at = None
            return existing
        finding = Finding(code, observed_at, message)
        job.findings[code] = finding
        return finding

    @staticmethod
    def _resolve(job: JobVisibility, code: str, resolved_at: datetime) -> None:
        finding = job.findings.get(code)
        if finding and finding.active:
            finding.active = False
            finding.resolved_at = resolved_at

    @staticmethod
    def _earliest(left: datetime | None, right: datetime | None) -> datetime | None:
        if left is None:
            return right
        if right is None:
            return left
        return min(left, right)

    @staticmethod
    def _latest(left: datetime | None, right: datetime | None) -> datetime | None:
        if left is None:
            return right
        if right is None:
            return left
        return max(left, right)
