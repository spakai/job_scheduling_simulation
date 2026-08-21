from __future__ import annotations

from job_visibility.scheduler import SchedulerWorker


class StubScheduler:
    def __init__(self, jobs: list[object] | None = None) -> None:
        self.jobs = jobs or []
        self.recovery_limits: list[int] = []
        self.claims: list[tuple[str, int]] = []
        self.executed: list[object] = []

    def recover_expired_claims(self, *, limit: int) -> int:
        self.recovery_limits.append(limit)
        return 0

    def claim_due(self, *, owner: str, limit: int) -> list[object]:
        self.claims.append((owner, limit))
        jobs, self.jobs = self.jobs, []
        return jobs

    def execute(self, job: object) -> None:
        self.executed.append(job)


def test_worker_recovers_claims_and_executes_one_bounded_batch() -> None:
    scheduler = StubScheduler(["one", "two"])
    worker = SchedulerWorker(
        scheduler,  # type: ignore[arg-type]
        owner="worker-1",
        batch_size=2,
        recovery_batch_size=7,
    )

    assert worker.run_once() == 2
    assert scheduler.recovery_limits == [7]
    assert scheduler.claims == [("worker-1", 2)]
    assert scheduler.executed == ["one", "two"]


def test_worker_waits_when_idle_and_stops_without_another_claim() -> None:
    scheduler = StubScheduler()
    waits: list[float] = []
    worker: SchedulerWorker

    def wait(seconds: float) -> None:
        waits.append(seconds)
        worker.stop()

    worker = SchedulerWorker(
        scheduler,  # type: ignore[arg-type]
        owner="worker-1",
        poll_interval_seconds=0.25,
        wait=wait,
    )
    worker.run_forever()

    assert waits == [0.25]
    assert scheduler.claims == [("worker-1", 100)]
