"""Validated declarations for bounded application-level chaos experiments."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Checkpoint(StrEnum):
    SCHEDULER_BEFORE_COMMIT = "scheduler.before_commit"
    SCHEDULER_AFTER_COMMIT = "scheduler.after_commit"
    WORKER_AFTER_CLAIM = "worker.after_claim"
    WORKER_BEFORE_COMPLETE = "worker.before_complete"
    PUBLISHER_BEFORE_SEND = "publisher.before_send"
    PUBLISHER_AFTER_BROKER_ACK = "publisher.after_broker_ack"
    PROJECTOR_BEFORE_APPLY = "projector.before_apply"
    PROJECTOR_AFTER_APPLY = "projector.after_apply"
    VISIBILITY_BEFORE_QUERY = "visibility.before_query"


class FaultAction(StrEnum):
    DELAY = "delay"
    RAISE = "raise"
    EXIT = "exit"
    PAUSE = "pause"


class FaultRule(BaseModel):
    """One finite allowlisted fault rule.

    Exact invocation counts make correctness experiments reproducible. Identity selectors
    narrow the blast radius further but are optional for process-wide publisher/projector
    experiments.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint: Checkpoint
    action: FaultAction
    invocations: int = Field(default=1, ge=1, le=100)
    activate_on: int = Field(default=1, ge=1, le=100)
    delay_seconds: float = Field(default=0, ge=0, le=30)
    job_id: str | None = Field(default=None, min_length=1, max_length=200)
    correlation_id: str | None = Field(default=None, min_length=1, max_length=200)
    error_code: str = Field(default="CHAOS_INJECTED", pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    exit_code: int = Field(default=86, ge=1, le=125)
    pause_timeout_seconds: float = Field(default=10, gt=0, le=30)

    @model_validator(mode="after")
    def validate_action(self) -> FaultRule:
        if self.activate_on > self.invocations:
            raise ValueError("activate_on cannot exceed invocations")
        if self.action is FaultAction.DELAY and self.delay_seconds <= 0:
            raise ValueError("delay action requires a positive delay_seconds")
        if self.action is not FaultAction.DELAY and self.delay_seconds != 0:
            raise ValueError("delay_seconds is valid only for delay action")
        return self
