from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from job_visibility.engine import JobNotFoundError, VisibilityEngine
from job_visibility.model import Event, EventType, Status


class EventInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventId")
    event_type: EventType = Field(alias="eventType")
    event_time: datetime = Field(alias="eventTime")
    ingestion_time: datetime = Field(alias="ingestionTime")
    job_id: str = Field(alias="jobId")
    correlation_id: str = Field(default="", alias="correlationId")
    job_type: str = Field(default="GENERIC", alias="jobType")
    source_system: str = Field(default="api", alias="sourceSystem")
    scheduler_reference: str | None = Field(default=None, alias="schedulerReference")
    scheduled_at: datetime | None = Field(default=None, alias="scheduledAt")
    attempt_number: int = Field(default=0, alias="attemptNumber")
    retryable: bool | None = None
    max_attempts: int = Field(default=3, alias="maxAttempts")
    next_retry_at: datetime | None = Field(default=None, alias="nextRetryAt")
    result_code: str | None = Field(default=None, alias="resultCode")
    error_code: str | None = Field(default=None, alias="errorCode")
    error_message: str | None = Field(default=None, alias="errorMessage")
    trace_id: str | None = Field(default=None, alias="traceId")
    payload_reference: str | None = Field(default=None, alias="payloadReference")
    poll_id: str | None = Field(default=None, alias="pollId")
    poll_time: datetime | None = Field(default=None, alias="pollTime")
    batch_position: int | None = Field(default=None, alias="batchPosition")
    batch_limit: int | None = Field(default=None, alias="batchLimit")

    def to_event(self) -> Event:
        return Event(**self.model_dump())


def create_app(engine: VisibilityEngine | None = None) -> FastAPI:
    visibility = engine or VisibilityEngine()
    app = FastAPI(title="Scheduled Job Visibility Simulation", version="0.1.0")
    app.state.visibility_engine = visibility

    @app.post("/edrs", status_code=202)
    def ingest_edr(value: EventInput) -> dict[str, str | None]:
        decision = visibility.apply(value.to_event())
        return {
            "eventId": decision.event_id,
            "decision": decision.decision,
            "reason": decision.reason,
        }

    def not_found() -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={
                "code": "JOB_VISIBILITY_RECORD_NOT_FOUND",
                "message": "No job visibility record was found for the supplied job ID.",
                "meaning": (
                    "The absence of a visibility record does not prove that the external "
                    "scheduler has no such job."
                ),
            },
        )

    @app.get("/scheduled-jobs/{job_id}", response_model=None)
    def retrieve_job(job_id: str) -> dict | JSONResponse:
        try:
            return visibility.get(job_id, datetime.now(UTC))
        except JobNotFoundError:
            return not_found()

    @app.get("/scheduled-jobs/{job_id}/attempts", response_model=None)
    def retrieve_attempts(job_id: str) -> dict[str, object] | JSONResponse:
        try:
            return {"jobId": job_id, "attempts": visibility.attempts(job_id)}
        except JobNotFoundError:
            return not_found()

    @app.get("/scheduled-jobs")
    def search_jobs(
        status: Status | None = None,
        correlation_id: Annotated[str | None, Query(alias="correlationId")] = None,
        scheduled_from: Annotated[datetime | None, Query(alias="scheduledFrom")] = None,
        scheduled_to: Annotated[datetime | None, Query(alias="scheduledTo")] = None,
    ) -> dict[str, object]:
        jobs = visibility.search(
            datetime.now(UTC),
            status=status,
            correlation_id=correlation_id,
            scheduled_from=scheduled_from,
            scheduled_to=scheduled_to,
        )
        return {"items": jobs, "count": len(jobs)}

    @app.post("/reconciliation-runs")
    def reconcile() -> dict[str, object]:
        findings = visibility.reconcile(datetime.now(UTC))
        return {"findings": [finding.to_dict() for finding in findings], "count": len(findings)}

    return app


app = create_app()
