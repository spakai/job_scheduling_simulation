from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from job_visibility.api import create_app
from job_visibility.production_api import create_scheduler_app, create_visibility_app
from job_visibility.scheduler import SubmissionDecision


class StubScheduler:
    def __init__(self, decision: SubmissionDecision) -> None:
        self.decision = decision

    def submit_checked(self, _: object) -> SubmissionDecision:
        return self.decision


def _submission(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "jobId": "job-004",
        "correlationId": "order-004",
        "jobType": "FIBONACCI",
        "scheduledAt": datetime.now(UTC).isoformat(),
        "payload": {"limit": 100},
        "maxAttempts": 3,
    }
    value.update(changes)
    return value


def test_submission_contract_distinguishes_create_replay_and_conflict() -> None:
    created = TestClient(create_app(scheduler=StubScheduler(SubmissionDecision.CREATED)))
    replay = TestClient(create_app(scheduler=StubScheduler(SubmissionDecision.IDENTICAL)))
    conflict = TestClient(create_app(scheduler=StubScheduler(SubmissionDecision.CONFLICT)))

    assert created.post("/scheduler/jobs", json=_submission()).status_code == 201
    replay_response = replay.post("/scheduler/jobs", json=_submission())
    assert replay_response.status_code == 200
    assert replay_response.json()["created"] is False
    assert conflict.post("/scheduler/jobs", json=_submission()).status_code == 409


def test_submission_rejects_unknown_type_and_large_payload() -> None:
    client = TestClient(
        create_app(scheduler=StubScheduler(SubmissionDecision.CREATED), max_payload_bytes=10)
    )

    assert client.post("/scheduler/jobs", json=_submission(jobType="UNKNOWN")).status_code == 422
    assert (
        client.post(
            "/scheduler/jobs", json=_submission(payload={"message": "too large"})
        ).status_code
        == 413
    )


def test_production_factories_mount_only_role_routes(monkeypatch: object) -> None:
    monkeypatch.setenv(  # type: ignore[attr-defined]
        "SCHEDULER_DATABASE_URL", "postgresql+psycopg://scheduler:secret@db/scheduler"
    )
    scheduler_app = create_scheduler_app()
    scheduler_paths = {route.path for route in scheduler_app.routes}
    assert "/scheduler/jobs" in scheduler_paths
    assert "/scheduled-jobs" not in scheduler_paths
    assert "/edrs" not in scheduler_paths

    monkeypatch.delenv("SCHEDULER_DATABASE_URL")  # type: ignore[attr-defined]
    monkeypatch.setenv(  # type: ignore[attr-defined]
        "EDR_DATABASE_URL", "postgresql+psycopg://reader:secret@db/edr"
    )
    visibility_app = create_visibility_app()
    visibility_paths = {route.path for route in visibility_app.routes}
    assert "/scheduled-jobs" in visibility_paths
    assert "/scheduler/jobs" not in visibility_paths
    assert "/edrs" not in visibility_paths
