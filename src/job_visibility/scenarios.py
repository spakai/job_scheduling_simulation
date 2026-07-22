from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from job_visibility.engine import VersionConflictError, VisibilityEngine
from job_visibility.model import Event, EventType, VisibilityConfig, iso
from job_visibility.simulation import EventFactory, PollingScheduler, ScheduledItem, VirtualClock

UTC = UTC
BASE = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)


@dataclass(slots=True)
class AssertionResult:
    name: str
    passed: bool
    expected: Any = None
    actual: Any = None


@dataclass(slots=True)
class ScenarioResult:
    scenario_id: str
    scenario_name: str
    input_events: list[dict[str, Any]]
    delivery_order: list[str]
    faults_injected: list[str]
    final_visibility_record: dict[str, Any]
    api_response: dict[str, Any]
    reconciliation_findings: list[dict[str, Any]]
    assertions: list[AssertionResult]
    result: str
    poll_records: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["scenarioId"] = value.pop("scenario_id")
        value["scenarioName"] = value.pop("scenario_name")
        value["inputEvents"] = value.pop("input_events")
        value["deliveryOrder"] = value.pop("delivery_order")
        value["faultsInjected"] = value.pop("faults_injected")
        value["finalVisibilityRecord"] = value.pop("final_visibility_record")
        value["apiResponse"] = value.pop("api_response")
        value["reconciliationFindings"] = value.pop("reconciliation_findings")
        value["pollRecords"] = value.pop("poll_records")
        return value


class Check:
    def __init__(self) -> None:
        self.items: list[AssertionResult] = []

    def equal(self, name: str, actual: Any, expected: Any) -> None:
        self.items.append(AssertionResult(name, actual == expected, expected, actual))

    def true(self, name: str, actual: Any) -> None:
        self.items.append(AssertionResult(name, bool(actual), True, actual))


def _context(
    config: VisibilityConfig | None = None,
) -> tuple[VirtualClock, EventFactory, VisibilityEngine]:
    clock = VirtualClock(BASE)
    engine = VisibilityEngine(config)
    return clock, EventFactory(clock), engine


def _event(factory: EventFactory, kind: EventType, job_id: str = "job-123", **kwargs: Any) -> Event:
    return factory.make(
        kind,
        job_id,
        correlation_id="subscription-456:RENEW:2026-07-23",
        job_type="RENEW_SUBSCRIPTION",
        max_attempts=3,
        **kwargs,
    )


def _finish(
    scenario_id: str,
    name: str,
    engine: VisibilityEngine,
    clock: VirtualClock,
    checks: Check,
    events: list[Event],
    faults: list[str] | None = None,
    polls: list[dict[str, Any]] | None = None,
) -> ScenarioResult:
    response = engine.get("job-123", clock.now())
    return ScenarioResult(
        scenario_id,
        name,
        [event.to_dict() for event in events],
        [event.event_id for event in events],
        faults or [],
        response,
        response,
        response["reconciliationFindings"],
        checks.items,
        "PASS" if all(item.passed for item in checks.items) else "FAIL",
        polls,
    )


def _apply(engine: VisibilityEngine, events: list[Event]) -> None:
    for event in events:
        engine.apply(event)


def hp01() -> ScenarioResult:
    clock, factory, engine = _context()
    scheduled = BASE + timedelta(days=1)
    events = [
        _event(factory, EventType.JOB_CREATED, scheduled_at=scheduled),
        _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_REQUESTED, scheduled_at=scheduled),
        _event(
            factory,
            EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED,
            scheduled_at=scheduled,
            scheduler_reference="scheduler-789",
        ),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "SCHEDULED")
    check.equal("completed attempts", api["retry"]["completedAttempts"], 0)
    check.true("scheduler reference present", api["schedulerReference"])
    check.true("freshness present", api["dataAsOf"])
    return _finish("HP-01", "Job created and scheduled successfully", engine, clock, check, events)


def hp02() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_CREATED, scheduled_at=BASE),
        _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED, scheduled_at=BASE),
        _event(factory, EventType.JOB_EXECUTION_STARTED, scheduled_at=BASE, attempt_number=1),
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, scheduled_at=BASE, attempt_number=1),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "SUCCEEDED")
    check.equal("attempt count", api["retry"]["completedAttempts"], 1)
    check.true("startedAt populated", api["startedAt"])
    check.true("completedAt populated", api["completedAt"])
    return _finish("HP-02", "Job executes successfully", engine, clock, check, events)


def hp03() -> ScenarioResult:
    clock, factory, engine = _context()
    retry_at = BASE + timedelta(minutes=5)
    events = [
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=1),
        _event(
            factory,
            EventType.JOB_EXECUTION_FAILED,
            attempt_number=1,
            retryable=True,
            error_code="UPSTREAM_TIMEOUT",
        ),
        _event(factory, EventType.JOB_RETRY_REQUESTED, attempt_number=2),
        _event(
            factory,
            EventType.JOB_RETRY_ACKNOWLEDGED,
            attempt_number=2,
            next_retry_at=retry_at,
        ),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "RETRY_SCHEDULED")
    check.equal("completed attempts", api["retry"]["completedAttempts"], 1)
    check.equal("next attempt", api["retry"]["nextAttemptNumber"], 2)
    check.true("retry acknowledged", api["retry"]["schedulerAcknowledged"])
    return _finish("HP-03", "Job fails and retry is scheduled", engine, clock, check, events)


def hp04() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=1),
        _event(factory, EventType.JOB_EXECUTION_FAILED, attempt_number=1, retryable=True),
        _event(
            factory,
            EventType.JOB_RETRY_ACKNOWLEDGED,
            attempt_number=2,
            next_retry_at=BASE,
        ),
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=2),
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=2),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    attempts = engine.attempts("job-123")
    check = Check()
    check.equal("status", api["status"], "SUCCEEDED")
    check.equal("completed attempts", api["retry"]["completedAttempts"], 2)
    check.equal("attempt outcomes", [item["status"] for item in attempts], ["FAILED", "SUCCEEDED"])
    return _finish("HP-04", "Retry succeeds", engine, clock, check, events)


def hp05() -> ScenarioResult:
    clock, factory, engine = _context()
    events: list[Event] = []
    for attempt in range(1, 4):
        events.extend(
            [
                _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=attempt),
                _event(
                    factory,
                    EventType.JOB_EXECUTION_FAILED,
                    attempt_number=attempt,
                    retryable=attempt < 3,
                ),
            ]
        )
    events.append(_event(factory, EventType.JOB_RETRIES_EXHAUSTED, attempt_number=3))
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "RETRIES_EXHAUSTED")
    check.equal("completed attempts", api["retry"]["completedAttempts"], 3)
    check.equal("next retry null", api["retry"]["nextRetryAt"], None)
    return _finish("HP-05", "Retry exhausted", engine, clock, check, events)


def ch01() -> ScenarioResult:
    clock, factory, engine = _context()
    created = _event(factory, EventType.JOB_CREATED)
    events = [created, created]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "CREATED")
    check.equal("version unchanged by duplicate", api["version"], 1)
    return _finish("CH-01", "Duplicate JOB_CREATED", engine, clock, check, events, ["DUPLICATE"])


def ch02() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1),
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("status", api["status"], "SUCCEEDED")
    check.equal("attempt count", api["retry"]["completedAttempts"], 1)
    check.equal("version unchanged by semantic duplicate", api["version"], 1)
    return _finish(
        "CH-02", "Semantic duplicate execution outcome", engine, clock, check, events, ["DUPLICATE"]
    )


def ch03() -> ScenarioResult:
    clock, factory, engine = _context()
    success = _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1)
    started = _event(
        factory,
        EventType.JOB_EXECUTION_STARTED,
        attempt_number=1,
        event_time=BASE - timedelta(seconds=5),
    )
    events = [success, started]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("terminal state stable", api["status"], "SUCCEEDED")
    check.true("startedAt backfilled", api["startedAt"])
    return _finish(
        "CH-03", "Out-of-order STARTED after SUCCEEDED", engine, clock, check, events, ["REORDER"]
    )


def ch04() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_CREATED),
        _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_REQUESTED),
    ]
    _apply(engine, events)
    clock.advance(minutes=3)
    engine.reconcile(clock.now())
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("not scheduled", api["status"], "PENDING_SUBMISSION")
    check.true(
        "ack timeout finding",
        any(f["code"] == "SCHEDULER_ACK_TIMEOUT" for f in api["reconciliationFindings"]),
    )
    return _finish(
        "CH-04", "Scheduler acknowledgement missing", engine, clock, check, events, ["DROP"]
    )


def ch05() -> ScenarioResult:
    clock, factory, engine = _context()
    scheduled = BASE
    events = [
        _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED, scheduled_at=scheduled)
    ]
    _apply(engine, events)
    clock.advance(minutes=11)
    engine.reconcile(clock.now())
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("overdue", api["status"], "OVERDUE")
    check.equal("recorded scheduler state", api["recordedStatus"], "SCHEDULED")
    return _finish(
        "CH-05", "Acknowledged but execution EDR missing", engine, clock, check, events, ["DROP"]
    )


def ch06() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [_event(factory, EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED, scheduled_at=BASE)]
    _apply(engine, events)
    clock.advance(minutes=5)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("within grace", api["status"], "AWAITING_EXECUTION")
    return _finish(
        "CH-06", "Execution delayed within grace period", engine, clock, check, events, ["DELAY"]
    )


def ch07() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_EXECUTION_FAILED, attempt_number=1, retryable=True),
        _event(factory, EventType.JOB_RETRY_REQUESTED, attempt_number=2),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("retry pending", api["status"], "RETRY_PENDING")
    check.equal("not acknowledged", api["retry"]["schedulerAcknowledged"], False)
    check.equal("next retry unknown", api["retry"]["nextRetryAt"], None)
    return _finish("CH-07", "Retry acknowledgement missing", engine, clock, check, events, ["DROP"])


def ch08() -> ScenarioResult:
    clock, factory, engine = _context()
    requested = _event(factory, EventType.JOB_RETRY_REQUESTED, attempt_number=2)
    engine.apply(requested)
    clock.advance(minutes=3)
    engine.reconcile(clock.now())
    ack = _event(
        factory, EventType.JOB_RETRY_ACKNOWLEDGED, attempt_number=2, next_retry_at=clock.now()
    )
    engine.apply(ack)
    events = [requested, ack]
    api = engine.get("job-123", clock.now())
    finding = next(f for f in api["reconciliationFindings"] if f["code"] == "RETRY_ACK_TIMEOUT")
    check = Check()
    check.equal("late ack schedules retry", api["status"], "RETRY_SCHEDULED")
    check.equal("historical timeout resolved", finding["active"], False)
    return _finish(
        "CH-08", "Retry acknowledgement arrives late", engine, clock, check, events, ["DELAY"]
    )


def ch09() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=2),
        _event(factory, EventType.JOB_RETRY_ACKNOWLEDGED, attempt_number=2, next_retry_at=BASE),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("late ack does not regress running", api["status"], "RUNNING")
    return _finish(
        "CH-09",
        "Retry executes before acknowledgement EDR",
        engine,
        clock,
        check,
        events,
        ["REORDER"],
    )


def ch10() -> ScenarioResult:
    clock, factory, engine = _context()
    event_time = BASE
    clock.advance(minutes=10)
    event = _event(
        factory,
        EventType.JOB_EXECUTION_STARTED,
        attempt_number=1,
        event_time=event_time,
        ingestion_time=clock.now(),
    )
    engine.apply(event)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("state from event semantics", api["status"], "RUNNING")
    check.equal("dataAsOf uses ingestion", api["dataAsOf"], iso(clock.now()))
    check.equal("processing delay observable", api["freshness"]["processingDelaySeconds"], 600)
    return _finish("CH-10", "EDR delivery delay", engine, clock, check, [event], ["DELAY"])


def ch11() -> ScenarioResult:
    clock, factory, engine = _context()
    event = _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1)
    engine.apply(event)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("success despite dropped start", api["status"], "SUCCEEDED")
    check.equal("start unknown", api["startedAt"], None)
    check.true("incomplete lifecycle", api["incompleteLifecycle"])
    return _finish("CH-11", "Execution start EDR dropped", engine, clock, check, [event], ["DROP"])


def ch12() -> ScenarioResult:
    clock, factory, engine = _context()
    event = _event(factory, EventType.JOB_EXECUTION_FAILED, attempt_number=2, retryable=True)
    engine.apply(event)
    engine.reconcile(clock.now())
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("does not infer attempt one", api["retry"]["completedAttempts"], 1)
    check.true(
        "gap finding",
        any(f["code"] == "ATTEMPT_SEQUENCE_GAP" for f in api["reconciliationFindings"]),
    )
    return _finish("CH-12", "Invalid attempt number", engine, clock, check, [event])


def ch13() -> ScenarioResult:
    clock, factory, engine = _context()
    first = _event(factory, EventType.JOB_CREATED)
    second = _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_REQUESTED)
    third = _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED)
    engine.apply(first, expected_version=0)
    engine.apply(second, expected_version=1)
    conflict_observed = False
    try:
        engine.apply(third, expected_version=1)
    except VersionConflictError:
        conflict_observed = True
        engine.apply_with_retry(third, expected_version=1)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.true("optimistic conflict observed", conflict_observed)
    check.equal("event reapplied", api["status"], "SCHEDULED")
    check.equal("no event lost", len(engine.events), 3)
    return _finish(
        "CH-13",
        "Concurrent job updates",
        engine,
        clock,
        check,
        [first, second, third],
        ["CONCURRENT_UPDATE"],
    )


def ch14() -> ScenarioResult:
    clock, factory, engine = _context()
    event = _event(factory, EventType.JOB_CREATED)
    engine.apply(event)
    clock.advance(seconds=30)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.true("stale snapshot exposed", api["freshness"]["possiblyStale"])
    check.true("dataAsOf exposed", api["dataAsOf"])
    return _finish("CH-14", "Stale API replica", engine, clock, check, [event], ["STALE_READ"])


def ch15() -> ScenarioResult:
    clock, factory, engine = _context()
    ack = _event(factory, EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED, scheduled_at=BASE)
    engine.apply(ack)
    clock.advance(minutes=11)
    engine.reconcile(clock.now())
    start = _event(factory, EventType.JOB_EXECUTION_STARTED, scheduled_at=BASE, attempt_number=1)
    success = _event(
        factory, EventType.JOB_EXECUTION_SUCCEEDED, scheduled_at=BASE, attempt_number=1
    )
    _apply(engine, [start, success])
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("late success wins", api["status"], "SUCCEEDED")
    check.true(
        "overdue finding retained",
        any(f["code"] == "EXECUTION_START_OVERDUE" for f in api["reconciliationFindings"]),
    )
    return _finish(
        "CH-15",
        "Job succeeds after overdue",
        engine,
        clock,
        check,
        [ack, start, success],
        ["DELAY"],
    )


def ch16() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1),
        _event(factory, EventType.JOB_EXECUTION_FAILED, attempt_number=1, retryable=False),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("success preserved", api["status"], "SUCCEEDED")
    check.true(
        "conflict finding",
        any(f["code"] == "CONFLICTING_TERMINAL_OUTCOME" for f in api["reconciliationFindings"]),
    )
    return _finish("CH-16", "Late failure after success", engine, clock, check, events, ["REORDER"])


def ch17() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_CANCELLED),
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=1),
        _event(factory, EventType.JOB_EXECUTION_SUCCEEDED, attempt_number=1),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("observed success wins", api["status"], "SUCCEEDED")
    check.true("cancellation violation", api["cancellationViolation"])
    return _finish("CH-17", "Cancelled job executes later", engine, clock, check, events)


def ch18() -> ScenarioResult:
    clock, factory, engine = _context()
    events = [
        _event(factory, EventType.JOB_RETRIES_EXHAUSTED, attempt_number=3),
        _event(factory, EventType.JOB_EXECUTION_STARTED, attempt_number=4),
    ]
    _apply(engine, events)
    api = engine.get("job-123", clock.now())
    check = Check()
    check.equal("observed execution wins", api["status"], "RUNNING")
    check.true("exhaustion violation", api["retriesExhaustedViolation"])
    return _finish("CH-18", "Attempt starts after retry exhaustion", engine, clock, check, events)


def polling_scenario(number: int) -> ScenarioResult:
    scenario_id = f"POLL-{number:02d}"
    config = VisibilityConfig(max_items_per_poll=3, worker_capacity=3, grace_period_seconds=180)
    clock, factory, engine = _context(config)
    scheduler = PollingScheduler(engine, clock, factory, config)
    checks = Check()
    faults: list[str] = []
    events_before = len(engine.events)

    def add_job(job_id: str, scheduled_at: datetime) -> None:
        engine.apply(
            _event(factory, EventType.JOB_CREATED, job_id=job_id, scheduled_at=scheduled_at)
        )
        engine.apply(
            _event(
                factory,
                EventType.JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED,
                job_id=job_id,
                scheduled_at=scheduled_at,
            )
        )
        scheduler.enqueue(ScheduledItem(job_id, scheduled_at))

    if number == 1:
        add_job("job-123", BASE + timedelta(seconds=59))
        clock.advance(seconds=60)
        record = scheduler.poll()
        checks.equal("retrieved at next poll", record.retrieved_job_ids, ["job-123"])
        name = "Eligible just before a poll"
    elif number == 2:
        add_job("job-123", BASE + timedelta(seconds=1))
        scheduler.poll()
        api = engine.get("job-123", clock.now())
        checks.equal("not overdue before next poll", api["status"], "SCHEDULED")
        clock.advance(seconds=60)
        record = scheduler.poll()
        checks.equal("retrieved on following poll", record.retrieved_job_ids, ["job-123"])
        name = "Eligible just after a poll"
    elif number in {3, 4, 5}:
        job_count = {3: 3, 4: 4, 5: 10}[number]
        for index in range(job_count):
            job_id = "job-123" if index == 0 else f"job-{index + 1:03d}"
            add_job(job_id, BASE)
        expected_polls = (job_count + 2) // 3
        for _ in range(expected_polls):
            scheduler.poll(start=False)
            clock.advance(minutes=1)
        retrieved = [job for poll in scheduler.polls for job in poll.retrieved_job_ids]
        checks.equal("all jobs retrieved once", len(retrieved), len(set(retrieved)))
        checks.equal("all eligible jobs retrieved", len(retrieved), job_count)
        name = {
            3: "Exactly X eligible jobs",
            4: "X plus one eligible jobs",
            5: "Multi-batch backlog",
        }[number]
    elif number == 6:
        add_job("job-123", BASE)
        clock.advance(minutes=2)
        faults.append("SKIPPED_POLL")
        engine.reconcile(clock.now())
        checks.equal(
            "job remains evidence-based",
            engine.get("job-123", clock.now())["recordedStatus"],
            "SCHEDULED",
        )
        name = "Poll skipped"
    elif number == 7:
        for index in range(3):
            add_job("job-123" if index == 0 else f"job-{index}", BASE)
        record = scheduler.poll(start=False, retrieve_count=2)
        checks.equal("partial batch visible", len(record.retrieved_job_ids), 2)
        checks.equal("eligible count visible", record.eligible_count, 3)
        name = "Scheduler retrieves fewer than X"
    elif number == 8:
        add_job("job-123", BASE)
        scheduler.poll(start=False)
        api = engine.get("job-123", clock.now())
        checks.equal("retrieved awaiting execution", api["status"], "AWAITING_EXECUTION")
        checks.equal(
            "worker delay classified", api["delay"]["classification"], "RETRIEVED_AWAITING_WORKER"
        )
        name = "Retrieved but worker unavailable"
    elif number == 9:
        for index in range(6):
            add_job("job-123" if index == 0 else f"job-{index}", BASE)
        first = scheduler.poll(start=False, poller_id="a")
        second = scheduler.poll(start=False, poller_id="b")
        combined = first.retrieved_job_ids + second.retrieved_job_ids
        checks.equal("no double retrieval", len(combined), len(set(combined)))
        checks.equal("both pollers claim full work", len(combined), 6)
        name = "Multiple concurrent pollers"
    elif number == 10:
        add_job("job-123", BASE)
        scheduler.poll(start=True, complete=True, emit_retrieval=False)
        api = engine.get("job-123", clock.now())
        checks.equal("execution outcome preserved", api["status"], "SUCCEEDED")
        checks.equal("retrieval remains unknown", api["schedulerRetrievedAt"], None)
        checks.true("incomplete lifecycle", api["incompleteLifecycle"])
        faults.append("DROP_RETRIEVAL_EDR")
        name = "Retrieval EDR missing"
    elif number == 11:
        add_job("job-123", BASE)
        start = _event(
            factory, EventType.JOB_EXECUTION_STARTED, scheduled_at=BASE, attempt_number=1
        )
        retrieval = _event(
            factory,
            EventType.JOB_SCHEDULER_ITEM_RETRIEVED,
            scheduled_at=BASE,
            attempt_number=1,
            event_time=BASE - timedelta(seconds=1),
        )
        _apply(engine, [start, retrieval])
        checks.equal(
            "late retrieval does not regress",
            engine.get("job-123", clock.now())["status"],
            "RUNNING",
        )
        faults.append("DELAY_RETRIEVAL_EDR")
        name = "Retrieval EDR delayed"
    elif number == 12:
        add_job("job-123", BASE + timedelta(hours=1))
        clock.advance(seconds=30)
        api = engine.get("job-123", clock.now())
        checks.true("stale visibility exposed", api["freshness"]["possiblyStale"])
        faults.append("STALE_READ")
        name = "Stale visibility data"
    else:
        raise ValueError(number)

    events = list(engine.events[events_before:])
    return _finish(
        scenario_id,
        name,
        engine,
        clock,
        checks,
        events,
        faults,
        [poll.to_dict() for poll in scheduler.polls],
    )


SCENARIOS: dict[str, Callable[[], ScenarioResult]] = {
    "HP-01": hp01,
    "HP-02": hp02,
    "HP-03": hp03,
    "HP-04": hp04,
    "HP-05": hp05,
    "CH-01": ch01,
    "CH-02": ch02,
    "CH-03": ch03,
    "CH-04": ch04,
    "CH-05": ch05,
    "CH-06": ch06,
    "CH-07": ch07,
    "CH-08": ch08,
    "CH-09": ch09,
    "CH-10": ch10,
    "CH-11": ch11,
    "CH-12": ch12,
    "CH-13": ch13,
    "CH-14": ch14,
    "CH-15": ch15,
    "CH-16": ch16,
    "CH-17": ch17,
    "CH-18": ch18,
    **{f"POLL-{number:02d}": (lambda n=number: polling_scenario(n)) for number in range(1, 13)},
}


CI_SCENARIOS = [
    "HP-01",
    "HP-02",
    "HP-03",
    "HP-04",
    "CH-01",
    "CH-03",
    "CH-04",
    "CH-05",
    "CH-07",
    "CH-09",
    "CH-13",
    "CH-15",
    "CH-16",
    "POLL-02",
    "POLL-04",
    "POLL-08",
    "POLL-09",
    "POLL-10",
]
