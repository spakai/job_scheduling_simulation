from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class JobSubmission:
    job_id: str
    correlation_id: str
    job_type: str
    scheduled_at: datetime
    payload: dict[str, Any] | None = field(default_factory=dict)
    payload_reference: str | None = None
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if not self.job_id or not self.correlation_id or not self.job_type:
            raise ValueError("job_id, correlation_id, and job_type are required")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if (self.payload is None) == (self.payload_reference is None):
            raise ValueError("exactly one of payload and payload_reference is required")
        if self.scheduled_at.tzinfo is None:
            raise ValueError("scheduled_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    job_id: str
    correlation_id: str
    job_type: str
    payload: dict[str, Any] | None
    payload_reference: str | None
    scheduled_at: datetime
    attempt_number: int
    max_attempts: int
    claimed_by: str
    claim_token: UUID
    claim_expires_at: datetime


@dataclass(frozen=True, slots=True)
class HandlerResult:
    summary: dict[str, Any]
