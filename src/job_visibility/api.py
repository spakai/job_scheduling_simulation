from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import SQLAlchemyError

from job_visibility.edr_store import DurableVisibilityReader, KafkaEdrIngress
from job_visibility.engine import JobNotFoundError, VisibilityEngine
from job_visibility.model import Event, EventType, Status, classify_event
from job_visibility.observability import HealthService
from job_visibility.scheduler import JobSubmission, SchedulerService, SubmissionDecision


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


class JobSubmissionInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    correlation_id: str = Field(alias="correlationId")
    job_type: str = Field(alias="jobType")
    scheduled_at: datetime = Field(alias="scheduledAt")
    payload: dict[str, object] | None = Field(default_factory=dict)
    payload_reference: str | None = Field(default=None, alias="payloadReference")
    max_attempts: int = Field(default=3, ge=1, alias="maxAttempts")


def create_app(
    engine: VisibilityEngine | None = None,
    scheduler: SchedulerService | None = None,
    durable_reader: DurableVisibilityReader | None = None,
    edr_ingress: KafkaEdrIngress | None = None,
    health: HealthService | None = None,
    *,
    lifespan: Any = None,
    max_payload_bytes: int = 65_536,
    visibility_base_url: str = "",
) -> FastAPI:
    visibility = engine or VisibilityEngine()
    app = FastAPI(title="Scheduled Job Visibility Simulation", version="0.1.0", lifespan=lifespan)
    app.state.visibility_engine = visibility
    app.state.scheduler_service = scheduler

    @app.get("/health/live")
    def liveness() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready", response_model=None)
    def readiness() -> dict[str, object] | JSONResponse:
        if health is None:
            return {"status": "ready", "mode": "simulation"}
        ready, detail = health.check()
        return detail if ready else JSONResponse(status_code=503, content=detail)

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    if scheduler is not None:

        @app.post("/scheduler/jobs", status_code=201)
        def submit_job(value: JobSubmissionInput) -> JSONResponse:
            if value.job_type not in {"PRINT", "FIBONACCI"}:
                return JSONResponse(
                    status_code=422,
                    content={"code": "UNSUPPORTED_JOB_TYPE", "jobType": value.job_type},
                )
            payload_size = len(json.dumps(value.payload, separators=(",", ":")).encode())
            if payload_size > max_payload_bytes:
                return JSONResponse(
                    status_code=413,
                    content={"code": "JOB_PAYLOAD_TOO_LARGE", "limitBytes": max_payload_bytes},
                )
            try:
                decision = scheduler.submit_checked(JobSubmission(**value.model_dump()))
            except SQLAlchemyError:
                return JSONResponse(
                    status_code=503,
                    content={"code": "SCHEDULER_DATABASE_UNAVAILABLE"},
                )
            if decision is SubmissionDecision.CONFLICT:
                return JSONResponse(
                    status_code=409,
                    content={"code": "JOB_ID_CONFLICT", "jobId": value.job_id},
                )
            created = decision is SubmissionDecision.CREATED
            return JSONResponse(
                status_code=201 if created else 200,
                content={
                    "jobId": value.job_id,
                    "created": created,
                    "statusUrl": (
                        f"{visibility_base_url.rstrip('/')}/scheduled-jobs/{value.job_id}"
                    ),
                },
            )

    @app.post("/edrs", status_code=202)
    def ingest_edr(value: EventInput) -> dict[str, str | None]:
        event = value.to_event()
        if edr_ingress is not None:
            edr_ingress.publish(event)
            decision_values = (event.event_id, "ACCEPTED", "awaiting durable projection")
        else:
            decision = visibility.apply(event)
            decision_values = (decision.event_id, decision.decision, decision.reason)
        lifecycle = classify_event(value.event_type)
        return {
            "eventId": decision_values[0],
            "decision": decision_values[1],
            "reason": decision_values[2],
            "edrType": lifecycle.edr_type.value,
            "edrGroup": lifecycle.group.value,
            "edrRequirement": lifecycle.requirement.value,
        }

    @app.get("/edr-lifecycle")
    def retrieve_edr_lifecycle() -> dict[str, object]:
        items = []
        for event_type in EventType:
            lifecycle = classify_event(event_type)
            items.append(
                {
                    "eventType": event_type.value,
                    "edrType": lifecycle.edr_type.value,
                    "edrGroup": lifecycle.group.value,
                    "requirement": lifecycle.requirement.value,
                }
            )
        return {"items": items, "count": len(items)}

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
            if durable_reader is not None:
                return durable_reader.get(job_id)
            return visibility.get(job_id, datetime.now(UTC))
        except JobNotFoundError:
            return not_found()

    @app.get("/scheduled-jobs/{job_id}/attempts", response_model=None)
    def retrieve_attempts(job_id: str) -> dict[str, object] | JSONResponse:
        try:
            if durable_reader is not None:
                return {"jobId": job_id, "attempts": durable_reader.attempts(job_id)}
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
        if durable_reader is not None:
            jobs = durable_reader.search(
                status=status.value if status else None, correlation_id=correlation_id
            )
            if scheduled_from:
                jobs = [
                    item
                    for item in jobs
                    if item.get("scheduledAt")
                    and datetime.fromisoformat(item["scheduledAt"].replace("Z", "+00:00"))
                    >= scheduled_from
                ]
            if scheduled_to:
                jobs = [
                    item
                    for item in jobs
                    if item.get("scheduledAt")
                    and datetime.fromisoformat(item["scheduledAt"].replace("Z", "+00:00"))
                    <= scheduled_to
                ]
        else:
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
        if durable_reader is not None:
            findings = durable_reader.findings()
            return {"findings": findings, "count": len(findings)}
        findings = visibility.reconcile(datetime.now(UTC))
        return {"findings": [finding.to_dict() for finding in findings], "count": len(findings)}

    return app


app = create_app()
