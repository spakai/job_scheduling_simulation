from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.edr_store import ProjectionWorker
from job_visibility.model import Event, EventType
from job_visibility.outbox import BrokerCoordinate, OutboxPublisher, canonical_edr
from job_visibility.scheduler import (
    HandlerError,
    JobSubmission,
    SchedulerService,
    StaleClaimError,
)
from job_visibility.scheduler.models import HandlerResult


def _sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def _delete_scheduler_job(engine: Engine, job_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM scheduler_attempts WHERE job_id=:job"), {"job": job_id}
        )
        connection.execute(
            text("DELETE FROM scheduler_outbox WHERE message_key=:job"), {"job": job_id}
        )
        connection.execute(text("DELETE FROM scheduler_jobs WHERE job_id=:job"), {"job": job_id})


@pytest.mark.integration
@pytest.mark.postgres
def test_concurrent_pollers_claim_disjoint_jobs(scheduler_engine: Engine) -> None:
    prefix = f"r-persist-02-{uuid4()}"
    factory = _sessions(scheduler_engine)
    service = SchedulerService(factory)
    for number in range(8):
        service.submit(
            JobSubmission(
                f"{prefix}-{number}",
                prefix,
                "PRINT",
                datetime.now(UTC),
                {"message": str(number)},
            )
        )
    barrier = Barrier(2)

    def claim(owner: str) -> list[str]:
        barrier.wait(timeout=5)
        return [job.job_id for job in service.claim_due(owner=owner, limit=5)]

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(claim, "poller-a")
            second = pool.submit(claim, "poller-b")
            claimed = first.result(timeout=10) + second.result(timeout=10)

        assert len(claimed) == 8
        assert len(set(claimed)) == 8
        with scheduler_engine.connect() as connection:
            retrievals = connection.execute(
                text("""SELECT count(*) FROM scheduler_outbox
                WHERE message_key LIKE :prefix
                  AND canonical_payload LIKE '%JOB_SCHEDULER_ITEM_RETRIEVED%'"""),
                {"prefix": f"{prefix}%"},
            ).scalar_one()
        assert retrievals == 8
    finally:
        with scheduler_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM scheduler_attempts WHERE job_id LIKE :prefix"),
                {"prefix": f"{prefix}%"},
            )
            connection.execute(
                text("DELETE FROM scheduler_outbox WHERE message_key LIKE :prefix"),
                {"prefix": f"{prefix}%"},
            )
            connection.execute(
                text("DELETE FROM scheduler_jobs WHERE job_id LIKE :prefix"),
                {"prefix": f"{prefix}%"},
            )


@pytest.mark.integration
@pytest.mark.postgres
def test_publisher_crash_after_ack_leaves_republishable_outbox(scheduler_engine: Engine) -> None:
    event_id = f"r-kafka-01-{uuid4()}"

    class Producer:
        deliveries = 0

        def publish(self, **_: str) -> BrokerCoordinate:
            self.deliveries += 1
            return BrokerCoordinate(0, self.deliveries)

        def close(self, timeout: float) -> None:
            pass

    with scheduler_engine.begin() as connection:
        connection.execute(
            text("""INSERT INTO scheduler_outbox
            (event_id,topic,message_key,schema_version,canonical_payload,payload_hash)
            VALUES (:id,'events',:id,1,'{}',repeat('a',64))"""),
            {"id": event_id},
        )
    producer = Producer()
    crashed = False

    def crash_once(*_: object) -> None:
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("test crash boundary")

    try:
        first = OutboxPublisher(
            _sessions(scheduler_engine),
            producer,
            owner="publisher-a",
            retry_initial=0.001,
            retry_max=0.001,
            after_ack=crash_once,
        )
        assert first.run_once() == 1
        with scheduler_engine.begin() as connection:
            row = connection.execute(
                text(
                    "SELECT published_at,publish_attempts FROM scheduler_outbox WHERE event_id=:id"
                ),
                {"id": event_id},
            ).one()
            assert row.published_at is None
            assert row.publish_attempts == 1
            connection.execute(
                text(
                    """UPDATE scheduler_outbox SET next_attempt_at=clock_timestamp()
                    WHERE event_id=:id"""
                ),
                {"id": event_id},
            )

        second = OutboxPublisher(
            _sessions(scheduler_engine), producer, owner="publisher-b", after_ack=crash_once
        )
        assert second.run_once() == 1
        with scheduler_engine.connect() as connection:
            assert connection.execute(
                text("SELECT published_at IS NOT NULL FROM scheduler_outbox WHERE event_id=:id"),
                {"id": event_id},
            ).scalar_one()
        assert producer.deliveries == 2
    finally:
        with scheduler_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM scheduler_outbox WHERE event_id=:id"), {"id": event_id}
            )


@pytest.mark.integration
@pytest.mark.postgres
def test_sustained_retryable_failure_stops_at_exact_attempt_limit(
    scheduler_engine: Engine,
) -> None:
    job_id = f"r-worker-03-{uuid4()}"
    service = SchedulerService(
        _sessions(scheduler_engine), retry_delay=lambda _: timedelta(seconds=0)
    )
    service.submit(JobSubmission(job_id, job_id, "PRINT", datetime.now(UTC), {"message": "x"}))
    failure = HandlerError("DEPENDENCY_UNAVAILABLE", "dependency unavailable", retryable=True)
    try:
        for attempt in range(1, 4):
            claimed = service.claim_due(owner=f"worker-{attempt}", limit=1)
            assert len(claimed) == 1
            service.start(claimed[0])
            service.fail(claimed[0], failure)

        assert service.claim_due(owner="worker-4", limit=1) == []
        with scheduler_engine.connect() as connection:
            row = connection.execute(
                text("""SELECT status,attempt_number,claimed_by,claim_token
                FROM scheduler_jobs WHERE job_id=:job"""),
                {"job": job_id},
            ).one()
            assert tuple(row) == ("RETRIES_EXHAUSTED", 3, None, None)
            assert (
                connection.execute(
                    text("SELECT count(*) FROM scheduler_attempts WHERE job_id=:job"),
                    {"job": job_id},
                ).scalar_one()
                == 3
            )
    finally:
        _delete_scheduler_job(scheduler_engine, job_id)


@pytest.mark.integration
@pytest.mark.postgres
def test_expired_worker_cannot_commit_over_recovered_claim(scheduler_engine: Engine) -> None:
    job_id = f"r-worker-04-{uuid4()}"
    service = SchedulerService(
        _sessions(scheduler_engine),
        claim_lease_seconds=1,
        retry_delay=lambda _: timedelta(seconds=0),
    )
    service.submit(JobSubmission(job_id, job_id, "PRINT", datetime.now(UTC), {"message": "x"}))
    old = service.claim_due(owner="old-worker", limit=1)[0]
    service.start(old)
    try:
        with scheduler_engine.begin() as connection:
            connection.execute(
                text("""UPDATE scheduler_jobs SET claim_expires_at=clock_timestamp()-interval '1s'
                WHERE job_id=:job"""),
                {"job": job_id},
            )
        assert service.recover_expired_claims() >= 1
        replacements = service.claim_due(owner="replacement-worker", limit=100)
        replacement = next(job for job in replacements if job.job_id == job_id)

        with pytest.raises(StaleClaimError):
            service.succeed(old, HandlerResult({"stale": True}))
        service.start(replacement)
        service.succeed(replacement, HandlerResult({"recovered": True}))

        with scheduler_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT status FROM scheduler_jobs WHERE job_id=:job"), {"job": job_id}
                ).scalar_one()
                == "SUCCEEDED"
            )
    finally:
        _delete_scheduler_job(scheduler_engine, job_id)


@pytest.mark.integration
@pytest.mark.postgres
def test_projection_crash_rolls_back_and_replay_commits_once(edr_engine: Engine) -> None:
    event_id = f"r-proj-01-{uuid4()}"
    job_id = f"job-{event_id}"
    now = datetime.now(UTC)
    wire, digest = canonical_edr(Event(event_id, EventType.JOB_CREATED, now, now, job_id))
    envelope = json.loads(wire)
    with edr_engine.begin() as connection:
        connection.execute(
            text("""INSERT INTO edr_events
            (event_id,schema_version,event_type,event_time,ingestion_time,job_id,
             attempt_number,max_attempts,canonical_payload,payload_hash,
             kafka_topic,kafka_partition,kafka_offset)
            VALUES (:id,1,'JOB_CREATED',:now,:now,:job,0,3,CAST(:payload AS jsonb),:hash,
                    'events',0,:offset)"""),
            {
                "id": event_id,
                "now": now,
                "job": job_id,
                "payload": envelope["canonicalPayload"],
                "hash": digest,
                "offset": int(uuid4().int % 9_000_000_000),
            },
        )

    def crash(_: list[str]) -> None:
        raise RuntimeError("test pre-commit crash")

    factory = _sessions(edr_engine)
    with pytest.raises(RuntimeError, match="pre-commit"):
        ProjectionWorker(factory, before_commit=crash).run_once()
    with edr_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM projected_events WHERE event_id=:id"), {"id": event_id}
            ).scalar_one()
            == 0
        )

    assert ProjectionWorker(factory).run_once() >= 1
    with edr_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM projected_events WHERE event_id=:id"), {"id": event_id}
            ).scalar_one()
            == 1
        )
