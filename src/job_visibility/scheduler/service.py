from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.chaos import Checkpoint, FaultContext, FaultInjector, NoOpFaultInjector
from job_visibility.model import Event, EventType
from job_visibility.outbox import canonical_edr

from .handlers import HandlerError, HandlerRegistry
from .models import ClaimedJob, HandlerResult, JobSubmission


class StaleClaimError(RuntimeError):
    pass


class SubmissionDecision(StrEnum):
    CREATED = "CREATED"
    IDENTICAL = "IDENTICAL"
    CONFLICT = "CONFLICT"


class SchedulerService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        topic: str = "job-lifecycle-edr.v1",
        claim_lease_seconds: int = 60,
        retry_delay: Callable[[int], timedelta] | None = None,
        handlers: HandlerRegistry | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self.sessions = sessions
        self.topic = topic
        self.claim_lease_seconds = claim_lease_seconds
        self.retry_delay = retry_delay or (lambda attempt: timedelta(seconds=min(300, 2**attempt)))
        self.handlers = handlers or HandlerRegistry()
        self.fault_injector = fault_injector or NoOpFaultInjector()

    def submit(self, value: JobSubmission) -> bool:
        return self.submit_checked(value) is SubmissionDecision.CREATED

    def submit_checked(self, value: JobSubmission) -> SubmissionDecision:
        context = FaultContext(job_id=value.job_id, correlation_id=value.correlation_id)
        with self.sessions() as session, session.begin():
            inserted = session.execute(
                text("""
                INSERT INTO scheduler_jobs
                  (job_id, correlation_id, job_type, payload, payload_reference, scheduled_at,
                   available_at, status, max_attempts)
                VALUES (:job_id, :correlation_id, :job_type, CAST(:payload AS jsonb),
                        :payload_reference, :scheduled_at, :scheduled_at, 'PENDING', :max_attempts)
                ON CONFLICT (job_id) DO NOTHING RETURNING created_at
                """),
                {
                    "job_id": value.job_id,
                    "correlation_id": value.correlation_id,
                    "job_type": value.job_type,
                    "payload": json.dumps(value.payload) if value.payload is not None else None,
                    "payload_reference": value.payload_reference,
                    "scheduled_at": value.scheduled_at,
                    "max_attempts": value.max_attempts,
                },
            ).scalar_one_or_none()
            if inserted is None:
                existing = (
                    session.execute(
                        text("""SELECT correlation_id,job_type,payload,payload_reference,
                    scheduled_at,max_attempts FROM scheduler_jobs WHERE job_id=:job_id"""),
                        {"job_id": value.job_id},
                    )
                    .mappings()
                    .one()
                )
                expected_payload = value.payload if value.payload is not None else None
                identical = (
                    existing["correlation_id"] == value.correlation_id
                    and existing["job_type"] == value.job_type
                    and existing["payload"] == expected_payload
                    and existing["payload_reference"] == value.payload_reference
                    and existing["scheduled_at"] == value.scheduled_at
                    and existing["max_attempts"] == value.max_attempts
                )
                return SubmissionDecision.IDENTICAL if identical else SubmissionDecision.CONFLICT
            common = self._event_values(value, inserted)
            self._add_event(session, EventType.JOB_CREATED, **common)
            self._add_event(session, EventType.JOB_SCHEDULER_SUBMISSION_REQUESTED, **common)
            self.fault_injector.inject(Checkpoint.SCHEDULER_BEFORE_COMMIT, context)
        self.fault_injector.inject(Checkpoint.SCHEDULER_AFTER_COMMIT, context)
        return SubmissionDecision.CREATED

    def claim_due(self, *, owner: str, limit: int) -> list[ClaimedJob]:
        if limit < 1:
            return []
        with self.sessions.begin() as session:
            rows = (
                session.execute(
                    text("""
                WITH due AS (
                  SELECT job_id FROM scheduler_jobs
                  WHERE status IN ('PENDING', 'RETRY_WAIT') AND available_at <= clock_timestamp()
                  ORDER BY available_at, job_id FOR UPDATE SKIP LOCKED LIMIT :limit
                )
                UPDATE scheduler_jobs AS job SET
                  status='CLAIMED', attempt_number=job.attempt_number + 1,
                  claimed_by=:owner, claim_token=gen_random_uuid(),
                  claimed_at=clock_timestamp(),
                  claim_expires_at=clock_timestamp() + make_interval(secs => :lease),
                  updated_at=clock_timestamp(), version=version + 1
                FROM due WHERE job.job_id=due.job_id
                RETURNING job.*
                """),
                    {"owner": owner, "limit": limit, "lease": self.claim_lease_seconds},
                )
                .mappings()
                .all()
            )
            claimed = [self._claimed(row) for row in rows]
            for job in claimed:
                session.execute(
                    text("""
                    INSERT INTO scheduler_attempts
                      (job_id, attempt_number, claim_token, claimed_by, claimed_at)
                    VALUES (:job_id, :attempt, :token, :owner, clock_timestamp())
                    """),
                    {
                        "job_id": job.job_id,
                        "attempt": job.attempt_number,
                        "token": job.claim_token,
                        "owner": owner,
                    },
                )
                self._add_event(
                    session,
                    EventType.JOB_SCHEDULER_ITEM_RETRIEVED,
                    job_id=job.job_id,
                    correlation_id=job.correlation_id,
                    job_type=job.job_type,
                    event_time=job.claim_expires_at - timedelta(seconds=self.claim_lease_seconds),
                    scheduled_at=job.scheduled_at,
                    attempt_number=job.attempt_number,
                    max_attempts=job.max_attempts,
                )
            return claimed

    def start(self, job: ClaimedJob) -> None:
        with self.sessions.begin() as session:
            started_at = self._fenced_update(
                session,
                job,
                """status='RUNNING', updated_at=clock_timestamp(), version=version + 1""",
                returning="updated_at",
            )
            session.execute(
                text(
                    """UPDATE scheduler_attempts SET started_at=:started
                    WHERE job_id=:job_id AND attempt_number=:attempt"""
                ),
                {"started": started_at, "job_id": job.job_id, "attempt": job.attempt_number},
            )
            self._add_job_event(session, job, EventType.JOB_EXECUTION_STARTED, started_at)

    def heartbeat(self, job: ClaimedJob) -> datetime:
        with self.sessions.begin() as session:
            return self._fenced_update(
                session,
                job,
                """claim_expires_at=clock_timestamp() + make_interval(secs => :lease),
                updated_at=clock_timestamp()""",
                {"lease": self.claim_lease_seconds},
                returning="claim_expires_at",
            )

    def succeed(self, job: ClaimedJob, result: HandlerResult) -> None:
        with self.sessions.begin() as session:
            completed = self._fenced_update(
                session,
                job,
                """status='SUCCEEDED', claimed_by=NULL, claim_token=NULL, claimed_at=NULL,
                   claim_expires_at=NULL, updated_at=clock_timestamp(), version=version + 1""",
                returning="updated_at",
            )
            session.execute(
                text("""UPDATE scheduler_attempts SET completed_at=:completed, outcome='SUCCEEDED',
                     result_summary=CAST(:summary AS jsonb)
                     WHERE job_id=:job_id AND attempt_number=:attempt"""),
                {
                    "completed": completed,
                    "summary": json.dumps(result.summary),
                    "job_id": job.job_id,
                    "attempt": job.attempt_number,
                },
            )
            self._add_job_event(
                session, job, EventType.JOB_EXECUTION_SUCCEEDED, completed, result_code="SUCCESS"
            )

    def fail(self, job: ClaimedJob, failure: HandlerError) -> None:
        retry = failure.retryable and job.attempt_number < job.max_attempts
        status = "RETRY_WAIT" if retry else ("RETRIES_EXHAUSTED" if failure.retryable else "FAILED")
        with self.sessions.begin() as session:
            now = session.execute(text("SELECT clock_timestamp()"), {}).scalar_one()
            available = now + self.retry_delay(job.attempt_number) if retry else now
            completed = self._fenced_update(
                session,
                job,
                """status=:status, available_at=:available, claimed_by=NULL, claim_token=NULL,
                   claimed_at=NULL, claim_expires_at=NULL, updated_at=clock_timestamp(),
                   version=version + 1""",
                {"status": status, "available": available},
                returning="updated_at",
            )
            session.execute(
                text("""UPDATE scheduler_attempts SET completed_at=:completed, outcome='FAILED',
                     retryable=:retryable, error_code=:code, next_retry_at=:next_retry
                     WHERE job_id=:job_id AND attempt_number=:attempt"""),
                {
                    "completed": completed,
                    "retryable": failure.retryable,
                    "code": failure.code,
                    "next_retry": available if retry else None,
                    "job_id": job.job_id,
                    "attempt": job.attempt_number,
                },
            )
            self._add_job_event(
                session,
                job,
                EventType.JOB_EXECUTION_FAILED,
                completed,
                retryable=failure.retryable,
                error_code=failure.code,
            )
            if retry:
                self._add_job_event(
                    session, job, EventType.JOB_RETRY_REQUESTED, completed, retryable=True
                )
                self._add_job_event(
                    session,
                    job,
                    EventType.JOB_RETRY_ACKNOWLEDGED,
                    completed,
                    retryable=True,
                    next_retry_at=available,
                )
            elif failure.retryable:
                self._add_job_event(
                    session, job, EventType.JOB_RETRIES_EXHAUSTED, completed, retryable=False
                )

    def execute(self, job: ClaimedJob) -> HandlerResult | None:
        self.start(job)
        try:
            result = self.handlers.execute(job)
        except HandlerError as exc:
            self.fail(job, exc)
            return None
        except Exception:
            self.fail(
                job, HandlerError("UNEXPECTED_HANDLER_ERROR", "handler failed", retryable=True)
            )
            return None
        self.fault_injector.inject(
            Checkpoint.WORKER_BEFORE_COMPLETE,
            FaultContext(job_id=job.job_id, correlation_id=job.correlation_id),
        )
        self.succeed(job, result)
        return result

    def recover_expired_claims(self, *, limit: int = 100) -> int:
        with self.sessions.begin() as session:
            return len(
                session.execute(
                    text("""
                WITH expired AS (
                  SELECT job_id FROM scheduler_jobs WHERE status IN ('CLAIMED','RUNNING')
                    AND claim_expires_at < clock_timestamp()
                  ORDER BY claim_expires_at FOR UPDATE SKIP LOCKED LIMIT :limit
                )
                UPDATE scheduler_jobs j SET status=CASE WHEN attempt_number < max_attempts
                    THEN 'RETRY_WAIT' ELSE 'RETRIES_EXHAUSTED' END,
                  available_at=clock_timestamp(), claimed_by=NULL, claim_token=NULL,
                  claimed_at=NULL, claim_expires_at=NULL, updated_at=clock_timestamp(),
                  version=version+1
                FROM expired WHERE j.job_id=expired.job_id RETURNING j.job_id
            """),
                    {"limit": limit},
                ).all()
            )

    def _fenced_update(
        self,
        session: Session,
        job: ClaimedJob,
        assignments: str,
        values: dict[str, Any] | None = None,
        *,
        returning: str,
    ) -> Any:
        result = session.execute(
            text(f"""UPDATE scheduler_jobs SET {assignments}
            WHERE job_id=:job_id AND attempt_number=:attempt AND claimed_by=:owner
              AND claim_token=:token AND claim_expires_at >= clock_timestamp()
            RETURNING {returning}"""),
            {
                "job_id": job.job_id,
                "attempt": job.attempt_number,
                "owner": job.claimed_by,
                "token": job.claim_token,
            }
            | (values or {}),
        ).scalar_one_or_none()
        if result is None:
            raise StaleClaimError(f"claim is no longer current for job {job.job_id}")
        return result

    def _add_job_event(
        self,
        session: Session,
        job: ClaimedJob,
        event_type: EventType,
        event_time: datetime,
        **values: Any,
    ) -> None:
        self._add_event(
            session,
            event_type,
            job_id=job.job_id,
            correlation_id=job.correlation_id,
            job_type=job.job_type,
            event_time=event_time,
            scheduled_at=job.scheduled_at,
            attempt_number=job.attempt_number,
            max_attempts=job.max_attempts,
            **values,
        )

    def _add_event(self, session: Session, event_type: EventType, **values: Any) -> None:
        event = Event(
            event_id=str(uuid4()),
            event_type=event_type,
            ingestion_time=values["event_time"],
            **values,
        )
        payload, digest = canonical_edr(event)
        session.execute(
            text("""INSERT INTO scheduler_outbox
            (event_id, topic, message_key, schema_version, canonical_payload, payload_hash)
            VALUES (:id,:topic,:key,1,:payload,:hash)"""),
            {
                "id": event.event_id,
                "topic": self.topic,
                "key": event.job_id,
                "payload": payload,
                "hash": digest,
            },
        )

    @staticmethod
    def _event_values(value: JobSubmission, timestamp: datetime) -> dict[str, Any]:
        return {
            "job_id": value.job_id,
            "correlation_id": value.correlation_id,
            "job_type": value.job_type,
            "event_time": timestamp,
            "scheduled_at": value.scheduled_at,
            "attempt_number": 0,
            "max_attempts": value.max_attempts,
        }

    @staticmethod
    def _claimed(row: Any) -> ClaimedJob:
        return ClaimedJob(
            job_id=row["job_id"],
            correlation_id=row["correlation_id"],
            job_type=row["job_type"],
            payload=row["payload"],
            payload_reference=row["payload_reference"],
            scheduled_at=row["scheduled_at"],
            attempt_number=row["attempt_number"],
            max_attempts=row["max_attempts"],
            claimed_by=row["claimed_by"],
            claim_token=UUID(str(row["claim_token"])),
            claim_expires_at=row["claim_expires_at"],
        )
