from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.chaos import Checkpoint, FaultContext, FaultInjector, NoOpFaultInjector
from job_visibility.engine import JobNotFoundError


class DurableVisibilityReader:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self.sessions = sessions
        self.fault_injector = fault_injector or NoOpFaultInjector()

    def get(self, job_id: str) -> dict[str, Any]:
        self.fault_injector.inject(Checkpoint.VISIBILITY_BEFORE_QUERY, FaultContext(job_id=job_id))
        with self.sessions() as session:
            value = session.execute(
                text("SELECT projection FROM job_visibility WHERE job_id=:id"), {"id": job_id}
            ).scalar_one_or_none()
        if value is None:
            raise JobNotFoundError(job_id)
        return value

    def attempts(self, job_id: str) -> list[dict[str, Any]]:
        self.fault_injector.inject(Checkpoint.VISIBILITY_BEFORE_QUERY, FaultContext(job_id=job_id))
        with self.sessions() as session:
            exists = session.execute(
                text("SELECT 1 FROM job_visibility WHERE job_id=:id"), {"id": job_id}
            ).scalar_one_or_none()
            if exists is None:
                raise JobNotFoundError(job_id)
            return list(
                session.execute(
                    text("""SELECT projection FROM job_attempts
                WHERE job_id=:id ORDER BY attempt_number"""),
                    {"id": job_id},
                ).scalars()
            )

    def search(
        self, *, status: str | None = None, correlation_id: str | None = None
    ) -> list[dict[str, Any]]:
        self.fault_injector.inject(
            Checkpoint.VISIBILITY_BEFORE_QUERY,
            FaultContext(correlation_id=correlation_id),
        )
        clauses, values = [], {}
        if status:
            clauses.append("recorded_status=:status")
            values["status"] = status
        if correlation_id:
            clauses.append("correlation_id=:correlation")
            values["correlation"] = correlation_id
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.sessions() as session:
            return list(
                session.execute(
                    text("SELECT projection FROM job_visibility" + where + " ORDER BY job_id"),
                    values,
                ).scalars()
            )

    def findings(self) -> list[dict[str, Any]]:
        self.fault_injector.inject(Checkpoint.VISIBILITY_BEFORE_QUERY)
        with self.sessions() as session:
            rows = session.execute(
                text("""SELECT job_id,code,message,first_observed_at,
                active,resolved_at FROM reconciliation_findings
                ORDER BY first_observed_at""")
            ).mappings()
            return [dict(row) for row in rows]
