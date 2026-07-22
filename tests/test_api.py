from datetime import UTC, datetime

from fastapi.testclient import TestClient

from job_visibility.api import create_app
from job_visibility.engine import VisibilityEngine


def test_edr_ingestion_and_retrieval() -> None:
    client = TestClient(create_app(VisibilityEngine()))
    now = datetime.now(UTC).isoformat()
    payload = {
        "eventId": "evt-1",
        "eventType": "JOB_CREATED",
        "eventTime": now,
        "ingestionTime": now,
        "jobId": "job-1",
        "correlationId": "subscription-1:RENEW:2026-07-23",
        "jobType": "RENEW_SUBSCRIPTION",
        "scheduledAt": "2026-07-23T00:00:00Z",
    }

    accepted = client.post("/edrs", json=payload)
    retrieved = client.get("/scheduled-jobs/job-1")

    assert accepted.status_code == 202
    assert accepted.json()["decision"] == "APPLIED"
    assert retrieved.status_code == 200
    assert retrieved.json()["status"] == "CREATED"
    assert retrieved.json()["dataAsOf"] is not None


def test_missing_job_response_preserves_qualified_meaning() -> None:
    client = TestClient(create_app(VisibilityEngine()))
    response = client.get("/scheduled-jobs/not-observed")

    assert response.status_code == 404
    assert response.json()["code"] == "JOB_VISIBILITY_RECORD_NOT_FOUND"
    assert "does not prove" in response.json()["meaning"]
