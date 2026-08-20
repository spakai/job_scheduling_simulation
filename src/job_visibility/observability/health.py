from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


class HealthService:
    def __init__(
        self,
        scheduler: sessionmaker[Session] | None = None,
        edr: sessionmaker[Session] | None = None,
    ) -> None:
        self.scheduler, self.edr = scheduler, edr

    def check(self) -> tuple[bool, dict[str, Any]]:
        checks: dict[str, Any] = {}
        if self.scheduler:
            checks["schedulerDatabase"] = self._query(self.scheduler, "SELECT 1")
            checks["outbox"] = self._scalar(
                self.scheduler,
                """SELECT json_build_object(
                'count',count(*),'oldest',min(created_at)) FROM scheduler_outbox
                WHERE published_at IS NULL""",
            )
        if self.edr:
            checks["edrDatabase"] = self._query(self.edr, "SELECT 1")
            checks["projection"] = self._scalar(
                self.edr,
                """SELECT json_build_object(
                'count',count(*),'oldest',min(e.persisted_at)) FROM edr_events e
                LEFT JOIN projected_events p ON p.event_id=e.event_id WHERE p.event_id IS NULL""",
            )
        healthy = all(value is not False for value in checks.values())
        return healthy, {"status": "ready" if healthy else "degraded", "checks": checks}

    @staticmethod
    def _query(factory: sessionmaker[Session], query: str) -> bool:
        try:
            with factory() as session:
                session.execute(text(query)).scalar_one()
            return True
        except Exception:
            return False

    @staticmethod
    def _scalar(factory: sessionmaker[Session], query: str) -> Any:
        try:
            with factory() as session:
                value = session.execute(text(query)).scalar_one()
            if isinstance(value, dict):
                return {
                    key: item.isoformat() if isinstance(item, datetime) else item
                    for key, item in value.items()
                }
            return value
        except Exception:
            return False
