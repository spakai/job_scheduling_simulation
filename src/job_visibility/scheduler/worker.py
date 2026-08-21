"""Bounded polling loop for durable scheduled jobs."""

from __future__ import annotations

import time
from collections.abc import Callable

from job_visibility.chaos import Checkpoint, FaultContext, FaultInjector, NoOpFaultInjector

from .service import SchedulerService


class SchedulerWorker:
    def __init__(
        self,
        scheduler: SchedulerService,
        *,
        owner: str,
        batch_size: int = 100,
        recovery_batch_size: int = 100,
        poll_interval_seconds: float = 0.5,
        wait: Callable[[float], None] = time.sleep,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if not owner:
            raise ValueError("scheduler worker owner is required")
        if batch_size < 1 or recovery_batch_size < 1 or poll_interval_seconds <= 0:
            raise ValueError("scheduler worker bounds must be positive")
        self.scheduler = scheduler
        self.owner = owner
        self.batch_size = batch_size
        self.recovery_batch_size = recovery_batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.wait = wait
        self.fault_injector = fault_injector or NoOpFaultInjector()
        self._stopping = False

    def run_once(self) -> int:
        self.scheduler.recover_expired_claims(limit=self.recovery_batch_size)
        claimed = self.scheduler.claim_due(owner=self.owner, limit=self.batch_size)
        for job in claimed:
            self.fault_injector.inject(
                Checkpoint.WORKER_AFTER_CLAIM,
                FaultContext(
                    job_id=getattr(job, "job_id", None),
                    correlation_id=getattr(job, "correlation_id", None),
                ),
            )
            if self._stopping:
                break
            self.scheduler.execute(job)
        return len(claimed)

    def run_forever(self) -> None:
        while not self._stopping:
            if self.run_once() == 0:
                self.wait(self.poll_interval_seconds)

    def stop(self) -> None:
        self._stopping = True
