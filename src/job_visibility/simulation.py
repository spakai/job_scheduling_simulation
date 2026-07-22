from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import count

from job_visibility.engine import VisibilityEngine
from job_visibility.model import Event, EventType, PollRecord, VisibilityConfig

UTC = UTC


@dataclass(slots=True)
class VirtualClock:
    current: datetime

    def now(self) -> datetime:
        return self.current

    def advance(self, *, seconds: int = 0, minutes: int = 0) -> datetime:
        self.current += timedelta(seconds=seconds, minutes=minutes)
        return self.current

    def set(self, value: datetime) -> None:
        if value < self.current:
            raise ValueError("virtual clock cannot move backwards")
        self.current = value


class EventFactory:
    def __init__(self, clock: VirtualClock) -> None:
        self.clock = clock
        self._ids = count(1)

    def make(
        self,
        event_type: EventType,
        job_id: str,
        *,
        event_time: datetime | None = None,
        ingestion_time: datetime | None = None,
        **values: object,
    ) -> Event:
        event_time = event_time or self.clock.now()
        ingestion_time = ingestion_time or self.clock.now()
        return Event(
            event_id=str(values.pop("event_id", f"evt-{next(self._ids):05d}")),
            event_type=event_type,
            event_time=event_time,
            ingestion_time=ingestion_time,
            job_id=job_id,
            **values,
        )


@dataclass(slots=True)
class ScheduledItem:
    job_id: str
    scheduled_at: datetime
    correlation_id: str = ""
    job_type: str = "GENERIC"
    scheduler_reference: str | None = None
    attempt_number: int = 1
    retrieved: bool = False


class PollingScheduler:
    def __init__(
        self,
        engine: VisibilityEngine,
        clock: VirtualClock,
        factory: EventFactory,
        config: VisibilityConfig | None = None,
    ) -> None:
        self.engine = engine
        self.clock = clock
        self.factory = factory
        self.config = config or engine.config
        self.items: list[ScheduledItem] = []
        self.polls: list[PollRecord] = []
        self._poll_number = count(1)

    def enqueue(self, item: ScheduledItem) -> None:
        self.items.append(item)

    def poll(
        self,
        *,
        start: bool = True,
        complete: bool = False,
        retrieve_count: int | None = None,
        emit_retrieval: bool = True,
        poller_id: str = "poller-1",
    ) -> PollRecord:
        now = self.clock.now()
        eligible = [item for item in self.items if not item.retrieved and item.scheduled_at <= now]
        eligible.sort(key=lambda item: (item.scheduled_at, item.job_id))
        limit = min(
            self.config.max_items_per_poll, retrieve_count or self.config.max_items_per_poll
        )
        selected = eligible[:limit]
        poll_id = f"{poller_id}-poll-{next(self._poll_number):04d}"
        record = PollRecord(poll_id, now, len(eligible), [item.job_id for item in selected], limit)
        self.polls.append(record)
        for position, item in enumerate(selected, start=1):
            item.retrieved = True
            common = {
                "scheduled_at": item.scheduled_at,
                "attempt_number": item.attempt_number,
                "correlation_id": item.correlation_id,
                "job_type": item.job_type,
                "scheduler_reference": item.scheduler_reference,
            }
            if emit_retrieval:
                self.engine.apply(
                    self.factory.make(
                        EventType.JOB_SCHEDULER_ITEM_RETRIEVED,
                        item.job_id,
                        poll_id=poll_id,
                        poll_time=now,
                        batch_position=position,
                        batch_limit=limit,
                        **common,
                    )
                )
            if start and position <= self.config.worker_capacity:
                self.engine.apply(
                    self.factory.make(EventType.JOB_EXECUTION_STARTED, item.job_id, **common)
                )
                if complete:
                    self.engine.apply(
                        self.factory.make(
                            EventType.JOB_EXECUTION_SUCCEEDED,
                            item.job_id,
                            result_code="SUCCESS",
                            **common,
                        )
                    )
        return record


class Transport:
    """Small deterministic EDR transport supporting the specification's fault vocabulary."""

    def __init__(self) -> None:
        self.pending: list[Event] = []
        self.dropped: list[Event] = []

    def publish(self, *events: Event) -> None:
        self.pending.extend(events)

    def duplicate(self, index: int) -> None:
        self.pending.insert(index + 1, self.pending[index])

    def drop(self, index: int) -> None:
        self.dropped.append(self.pending.pop(index))

    def reorder(self, order: Iterable[int]) -> None:
        original = list(self.pending)
        self.pending = [original[index] for index in order]

    def burst_deliver(self, engine: VisibilityEngine) -> list[Event]:
        delivered = list(self.pending)
        for event in delivered:
            engine.apply(event)
        self.pending.clear()
        return delivered
