from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from job_visibility.config import ConfigurationError


class PullSchedulerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    work_topic: str = "job-requests.v1"
    result_topic: str = "job-results.v1"
    lifecycle_topic: str = "job-lifecycle-edr.v1"
    dlq_topic: str = "job-requests-dlq.v1"
    consumer_group: str = "job-workers-v1"
    instance_id: str
    transactional_id: str
    rate_limit_tps: float = Field(default=10, ge=0)
    rate_limit_burst: int = Field(default=10, ge=1)
    max_concurrency: int = Field(default=20, ge=1)
    poll_timeout_seconds: float = Field(default=1, gt=0, le=30)
    transaction_timeout_ms: int = Field(default=30_000, ge=1)

    @model_validator(mode="after")
    def validate_rate(self) -> Self:
        if self.rate_limit_tps == 0 and self.rate_limit_burst != 1:
            raise ValueError("rateLimitBurst must be 1 when TPS is zero")
        if len({self.work_topic, self.result_topic, self.lifecycle_topic, self.dlq_topic}) != 4:
            raise ValueError("pull scheduler topics must be distinct")
        return self


def pull_scheduler_config_from_env(
    environ: Mapping[str, str] | None = None,
) -> PullSchedulerConfig:
    values = os.environ if environ is None else environ
    instance = values.get("PULL_WORKER_INSTANCE_ID", socket.gethostname())
    try:
        return PullSchedulerConfig(
            bootstrap_servers=values.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            schema_registry_url=values.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            work_topic=values.get("KAFKA_WORK_TOPIC", "job-requests.v1"),
            result_topic=values.get("KAFKA_RESULT_TOPIC", "job-results.v1"),
            lifecycle_topic=values.get("KAFKA_EDR_TOPIC", "job-lifecycle-edr.v1"),
            dlq_topic=values.get("KAFKA_WORK_DLQ_TOPIC", "job-requests-dlq.v1"),
            consumer_group=values.get("KAFKA_WORKER_GROUP", "job-workers-v1"),
            instance_id=instance,
            transactional_id=values.get("KAFKA_TRANSACTIONAL_ID", f"job-worker-{instance}"),
            rate_limit_tps=float(values.get("PULL_WORKER_TPS", "10")),
            rate_limit_burst=int(values.get("PULL_WORKER_BURST", "10")),
            max_concurrency=int(values.get("PULL_WORKER_MAX_CONCURRENCY", "20")),
            poll_timeout_seconds=float(values.get("PULL_WORKER_POLL_SECONDS", "1")),
            transaction_timeout_ms=int(values.get("KAFKA_TRANSACTION_TIMEOUT_MS", "30000")),
        )
    except ValueError as exc:
        raise ConfigurationError("invalid pull scheduler configuration") from exc
