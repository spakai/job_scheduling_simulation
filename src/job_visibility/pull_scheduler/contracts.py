from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OwnerType(StrEnum):
    SUBSCRIBER = "SUBSCRIBER"
    GROUP = "GROUP"


class PullJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    schema_version: int = Field(alias="schemaVersion", default=1, ge=1)
    job_id: str = Field(alias="jobId", min_length=1)
    owner_id: str = Field(alias="ownerId", min_length=1)
    owner_type: OwnerType = Field(alias="ownerType")
    correlation_id: str = Field(alias="correlationId", min_length=1)
    job_type: str = Field(alias="jobType", min_length=1)
    requested_at: datetime = Field(alias="requestedAt")
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(alias="maxAttempts", default=3, ge=1)
    payload: dict[str, Any] | None = None
    payload_reference: str | None = Field(alias="payloadReference", default=None)

    @model_validator(mode="after")
    def validate_contract(self) -> Self:
        if self.requested_at.tzinfo is None:
            raise ValueError("requestedAt must be timezone-aware")
        if (self.payload is None) == (self.payload_reference is None):
            raise ValueError("exactly one of payload and payloadReference is required")
        prefix = "subscriber:" if self.owner_type is OwnerType.SUBSCRIBER else "group:"
        if not self.owner_id.startswith(prefix) or self.owner_id == prefix:
            raise ValueError(f"ownerId must use the {prefix} namespace")
        if self.attempt > self.max_attempts:
            raise ValueError("attempt cannot exceed maxAttempts")
        return self

    def wire(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, mode="json", exclude_none=True)


class JobResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    schema_version: int = Field(alias="schemaVersion", default=1)
    job_id: str = Field(alias="jobId")
    owner_id: str = Field(alias="ownerId")
    correlation_id: str = Field(alias="correlationId")
    attempt: int
    status: str
    completed_at: datetime = Field(alias="completedAt")
    result: dict[str, Any] | None = None
    error_code: str | None = Field(alias="errorCode", default=None)
    retryable: bool | None = None

    def wire(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, mode="json", exclude_none=True)
