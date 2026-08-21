from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

from prometheus_client import Counter
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.chaos import Checkpoint, FaultContext, FaultInjector, NoOpFaultInjector
from job_visibility.engine import VisibilityEngine
from job_visibility.model import Event, EventType

PROJECTED = Counter("job_visibility_projected_events_total", "Durably projected EDRs")
PROJECTION_FAILURES = Counter("job_visibility_projection_failures_total", "Projection failures")


def event_from_wire(value: str | dict[str, Any]) -> Event:
    data = json.loads(value) if isinstance(value, str) else value
    return Event(
        event_id=data["eventId"],
        event_type=EventType(data["eventType"]),
        event_time=datetime.fromisoformat(data["eventTime"].replace("Z", "+00:00")),
        ingestion_time=datetime.fromisoformat(data["ingestionTime"].replace("Z", "+00:00")),
        job_id=data["jobId"],
        correlation_id=data.get("correlationId") or "",
        job_type=data.get("jobType") or "GENERIC",
        scheduled_at=_time(data.get("scheduledAt")),
        attempt_number=data.get("attemptNumber", 0),
        max_attempts=data.get("maxAttempts", 3),
        retryable=data.get("retryable"),
        next_retry_at=_time(data.get("nextRetryAt")),
        result_code=data.get("resultCode"),
        error_code=data.get("errorCode"),
    )


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


class ProjectionWorker:
    """Projects immutable journal rows; each job is rebuilt from its authoritative EDRs."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        batch_size: int = 500,
        before_commit: Callable[[list[str]], None] | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self.sessions, self.batch_size = sessions, batch_size
        self.before_commit = before_commit
        self.fault_injector = fault_injector or NoOpFaultInjector()

    def run_once(self) -> int:
        with self.sessions.begin() as session:
            pending = (
                session.execute(
                    text("""SELECT e.event_id,e.job_id FROM edr_events e
                LEFT JOIN projected_events p ON p.event_id=e.event_id WHERE p.event_id IS NULL
                ORDER BY e.persisted_at,e.event_id FOR UPDATE OF e SKIP LOCKED LIMIT :limit"""),
                    {"limit": self.batch_size},
                )
                .mappings()
                .all()
            )
            for item in pending:
                context = FaultContext(job_id=item["job_id"], event_id=item["event_id"])
                self.fault_injector.inject(Checkpoint.PROJECTOR_BEFORE_APPLY, context)
                self._project_job(session, item["job_id"])
                session.execute(
                    text("""INSERT INTO projected_events(event_id,job_id)
                    VALUES (:event_id,:job_id) ON CONFLICT DO NOTHING"""),
                    dict(item),
                )
                self.fault_injector.inject(Checkpoint.PROJECTOR_AFTER_APPLY, context)
                PROJECTED.inc()
            if pending and self.before_commit is not None:
                self.before_commit([item["event_id"] for item in pending])
            return len(pending)

    def rebuild(self) -> int:
        with self.sessions.begin() as session:
            job_ids = (
                session.execute(text("SELECT DISTINCT job_id FROM edr_events ORDER BY job_id"))
                .scalars()
                .all()
            )
            for job_id in job_ids:
                self._project_job(session, job_id)
            return len(job_ids)

    def _project_job(self, session: Session, job_id: str) -> None:
        rows = session.execute(
            text("""SELECT canonical_payload FROM edr_events
            WHERE job_id=:job_id ORDER BY persisted_at,event_id"""),
            {"job_id": job_id},
        ).scalars()
        engine = VisibilityEngine()
        for payload in rows:
            wire = payload if isinstance(payload, dict) else json.loads(payload)
            engine.apply(event_from_wire(wire.get("canonicalPayload", wire)))
        snapshot = engine.snapshot(job_id)
        response = engine.get(job_id, snapshot.data_as_of)
        session.execute(
            text("""INSERT INTO job_visibility
            (job_id,correlation_id,job_type,recorded_status,projection,data_as_of,version)
            VALUES (:job_id,:correlation,:job_type,:status,CAST(:projection AS jsonb),
                    :data_as_of,:version)
            ON CONFLICT(job_id) DO UPDATE SET correlation_id=excluded.correlation_id,
              job_type=excluded.job_type,recorded_status=excluded.recorded_status,
              projection=excluded.projection,data_as_of=excluded.data_as_of,version=excluded.version"""),
            {
                "job_id": job_id,
                "correlation": snapshot.correlation_id,
                "job_type": snapshot.job_type,
                "status": snapshot.recorded_status.value,
                "projection": json.dumps(response),
                "data_as_of": snapshot.data_as_of,
                "version": snapshot.version,
            },
        )
        session.execute(text("DELETE FROM job_attempts WHERE job_id=:job_id"), {"job_id": job_id})
        for attempt in snapshot.attempts.values():
            session.execute(
                text("""INSERT INTO job_attempts(job_id,attempt_number,projection)
                VALUES (:job_id,:number,CAST(:projection AS jsonb))"""),
                {
                    "job_id": job_id,
                    "number": attempt.attempt_number,
                    "projection": json.dumps(attempt.to_dict()),
                },
            )
        for decision in snapshot.decisions:
            session.execute(
                text("""INSERT INTO projection_decisions(event_id,job_id,decision,reason)
                VALUES (:event_id,:job_id,:decision,:reason) ON CONFLICT(event_id) DO NOTHING"""),
                {
                    "event_id": decision.event_id,
                    "job_id": job_id,
                    "decision": decision.decision,
                    "reason": decision.reason,
                },
            )
        for finding in snapshot.findings.values():
            session.execute(
                text("""INSERT INTO reconciliation_findings
                (job_id,code,message,first_observed_at,active,resolved_at)
                VALUES (:job_id,:code,:message,:first_observed,:active,:resolved)
                ON CONFLICT(job_id,code,first_observed_at) DO UPDATE SET
                  active=excluded.active,resolved_at=excluded.resolved_at"""),
                {
                    "job_id": job_id,
                    "code": finding.code,
                    "message": finding.message,
                    "first_observed": finding.first_observed_at,
                    "active": finding.active,
                    "resolved": finding.resolved_at,
                },
            )
