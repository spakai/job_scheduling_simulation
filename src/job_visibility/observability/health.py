from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


class HealthService:
    def __init__(
        self,
        scheduler: sessionmaker[Session] | None = None,
        edr: sessionmaker[Session] | None = None,
        *,
        oldest_due_seconds: int = 120,
        oldest_outbox_seconds: int = 60,
        oldest_unprojected_seconds: int = 60,
        connector_status: Callable[[], dict[str, Any]] | None = None,
        dlq_growth: Callable[[], int] | None = None,
        dlq_growth_limit: int = 0,
    ) -> None:
        self.scheduler, self.edr = scheduler, edr
        self.oldest_due_seconds = oldest_due_seconds
        self.oldest_outbox_seconds = oldest_outbox_seconds
        self.oldest_unprojected_seconds = oldest_unprojected_seconds
        self.connector_status = connector_status
        self.dlq_growth = dlq_growth
        self.dlq_growth_limit = dlq_growth_limit

    def check(self) -> tuple[bool, dict[str, Any]]:
        checks: dict[str, Any] = {}
        if self.scheduler:
            checks["schedulerDatabase"] = self._query(self.scheduler, "SELECT 1")
            checks["dueJobs"] = self._queue(
                self.scheduler,
                """SELECT json_build_object(
                'count',count(*),'oldest',min(available_at)) FROM scheduler_jobs
                WHERE status IN ('PENDING','RETRY_WAIT') AND available_at <= clock_timestamp()""",
                self.oldest_due_seconds,
            )
            checks["outbox"] = self._queue(
                self.scheduler,
                """SELECT json_build_object(
                'count',count(*),'oldest',min(created_at)) FROM scheduler_outbox
                WHERE published_at IS NULL""",
                self.oldest_outbox_seconds,
            )
        if self.edr:
            checks["edrDatabase"] = self._query(self.edr, "SELECT 1")
            checks["projection"] = self._queue(
                self.edr,
                """SELECT json_build_object(
                'count',count(*),'oldest',min(e.persisted_at)) FROM edr_events e
                LEFT JOIN projected_events p ON p.event_id=e.event_id WHERE p.event_id IS NULL""",
                self.oldest_unprojected_seconds,
            )
        if self.connector_status:
            checks["connector"] = self._dependency(self.connector_status)
        if self.dlq_growth:
            checks["dlq"] = self._dlq(self.dlq_growth)
        healthy = all(
            value is not False and not (isinstance(value, dict) and value.get("healthy") is False)
            for value in checks.values()
        )
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

    @classmethod
    def _queue(
        cls, factory: sessionmaker[Session], query: str, threshold_seconds: int
    ) -> dict[str, Any] | bool:
        value = cls._scalar(factory, query)
        if value is False:
            return False
        return cls.evaluate_queue(value, threshold_seconds=threshold_seconds)

    @staticmethod
    def evaluate_queue(value: dict[str, Any], *, threshold_seconds: int) -> dict[str, Any]:
        oldest = value.get("oldest")
        if isinstance(oldest, str):
            oldest = datetime.fromisoformat(oldest.replace("Z", "+00:00"))
        age = max(0.0, (datetime.now(UTC) - oldest).total_seconds()) if oldest else 0.0
        return value | {
            "ageSeconds": round(age, 3),
            "thresholdSeconds": threshold_seconds,
            "healthy": age <= threshold_seconds,
        }

    @staticmethod
    def _dependency(check: Callable[[], dict[str, Any]]) -> dict[str, Any] | bool:
        try:
            value = check()
            tasks = value.get("tasks", [])
            running = value.get("connector", {}).get("state") == "RUNNING" and all(
                task.get("state") == "RUNNING" for task in tasks
            )
            return value | {"healthy": running}
        except Exception:
            return False

    def _dlq(self, check: Callable[[], int]) -> dict[str, Any] | bool:
        try:
            growth = check()
            return {
                "growth": growth,
                "growthLimit": self.dlq_growth_limit,
                "healthy": growth <= self.dlq_growth_limit,
            }
        except Exception:
            return False
