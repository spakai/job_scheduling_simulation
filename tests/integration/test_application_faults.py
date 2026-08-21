from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from job_visibility.chaos import (
    Checkpoint,
    ConfiguredFaultInjector,
    FaultAction,
    FaultRule,
    SyntheticFault,
)
from job_visibility.scheduler import JobSubmission, SchedulerService, SubmissionDecision


def _sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def _submission(job_id: str) -> JobSubmission:
    return JobSubmission(job_id, "chaos-correlation", "PRINT", datetime.now(UTC), {"message": "x"})


def _counts(engine: Engine, job_id: str) -> tuple[int, int]:
    with engine.connect() as connection:
        jobs = connection.execute(
            text("SELECT count(*) FROM scheduler_jobs WHERE job_id=:job"), {"job": job_id}
        ).scalar_one()
        outbox = connection.execute(
            text("SELECT count(*) FROM scheduler_outbox WHERE message_key=:job"), {"job": job_id}
        ).scalar_one()
    return jobs, outbox


def _delete(engine: Engine, job_id: str) -> None:
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
@pytest.mark.application_fault
def test_app_01_exception_before_commit_leaves_no_partial_state(scheduler_engine: Engine) -> None:
    job_id = f"chaos-app-01-{uuid4()}"
    injector = ConfiguredFaultInjector(
        "APP-01",
        [
            FaultRule(
                checkpoint=Checkpoint.SCHEDULER_BEFORE_COMMIT,
                action=FaultAction.RAISE,
                job_id=job_id,
            )
        ],
    )
    service = SchedulerService(_sessions(scheduler_engine), fault_injector=injector)
    try:
        with pytest.raises(SyntheticFault):
            service.submit_checked(_submission(job_id))
        assert _counts(scheduler_engine, job_id) == (0, 0)
    finally:
        _delete(scheduler_engine, job_id)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.application_fault
def test_app_02_exception_after_commit_replays_idempotently(scheduler_engine: Engine) -> None:
    job_id = f"chaos-app-02-{uuid4()}"
    injector = ConfiguredFaultInjector(
        "APP-02",
        [
            FaultRule(
                checkpoint=Checkpoint.SCHEDULER_AFTER_COMMIT,
                action=FaultAction.RAISE,
                job_id=job_id,
            )
        ],
    )
    service = SchedulerService(_sessions(scheduler_engine), fault_injector=injector)
    submission = _submission(job_id)
    try:
        with pytest.raises(SyntheticFault):
            service.submit_checked(submission)
        assert _counts(scheduler_engine, job_id) == (1, 2)
        assert service.submit_checked(submission) is SubmissionDecision.IDENTICAL
        assert _counts(scheduler_engine, job_id) == (1, 2)
    finally:
        _delete(scheduler_engine, job_id)
