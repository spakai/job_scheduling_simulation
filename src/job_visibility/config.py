"""Typed runtime configuration for the durable Spec 002 components.

The simulator does not require these settings. Durable process entry points can build an
``AppConfig`` from environment variables and receive validated, role-specific groups.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Self
from urllib.parse import unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class ConfigurationError(ValueError):
    """Raised when runtime settings violate an architecture boundary."""


class _ConfigGroup(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DatabaseConfig(_ConfigGroup):
    url: SecretStr
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    pool_timeout_seconds: float = Field(default=30, gt=0)


class KafkaConfig(_ConfigGroup):
    bootstrap_servers: str = "localhost:9092"
    edr_topic: str = "job-lifecycle-edr.v1"
    edr_dlq_topic: str = "job-lifecycle-edr-dlq.v1"
    consumer_group: str = "job-visibility-projector-v1"
    schema_registry_url: str = "http://localhost:8081"


class CassandraConfig(_ConfigGroup):
    contact_points: tuple[str, ...] = ("localhost",)
    port: int = Field(default=9042, ge=1, le=65535)
    keyspace: str = "worker_demo"
    consistency: str = "LOCAL_QUORUM"
    request_timeout_ms: int = Field(default=2_000, ge=1)
    execution_timeout_ms: int = Field(default=30_000, ge=1)
    max_record_count: int = Field(default=10_000, ge=1)
    max_input_number: int = Field(default=10_000, ge=0)
    page_size: int = Field(default=500, ge=1)
    read_concurrency: int = Field(default=8, ge=1)
    max_processing_delay_ms: int = Field(default=5_000, ge=0)
    conditional_retry_limit: int = Field(default=3, ge=0)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.request_timeout_ms > self.execution_timeout_ms:
            raise ValueError("request timeout cannot exceed the whole-execution timeout")
        if self.page_size > self.max_record_count:
            raise ValueError("page size cannot exceed the maximum record count")
        return self


class SchedulerTuning(_ConfigGroup):
    batch_size: int = Field(default=100, ge=1)
    claim_lease_seconds: int = Field(default=60, ge=1)


class OutboxTuning(_ConfigGroup):
    batch_size: int = Field(default=100, ge=1)
    retry_initial_seconds: float = Field(default=1, gt=0)
    retry_max_seconds: float = Field(default=60, gt=0)

    @model_validator(mode="after")
    def validate_retry_range(self) -> Self:
        if self.retry_initial_seconds > self.retry_max_seconds:
            raise ValueError("initial outbox retry cannot exceed maximum retry")
        return self


class SinkTuning(_ConfigGroup):
    batch_size: int = Field(default=500, ge=1)
    max_retries: int = Field(default=10, ge=0)
    retry_backoff_ms: int = Field(default=3_000, ge=1)


class ProjectionTuning(_ConfigGroup):
    batch_size: int = Field(default=500, ge=1)


class ReconciliationTuning(_ConfigGroup):
    batch_size: int = Field(default=500, ge=1)
    interval_seconds: int = Field(default=60, ge=1)


def _logical_database(url: SecretStr) -> tuple[str, str, int | None, str]:
    """Return identity without credentials or query-string connection options."""
    parsed = urlsplit(url.get_secret_value())
    database = unquote(parsed.path).strip("/")
    if not parsed.scheme or not parsed.hostname or not database:
        raise ConfigurationError("database URLs must include scheme, host, and database name")
    return parsed.scheme.lower(), parsed.hostname.lower(), parsed.port, database


class AppConfig(_ConfigGroup):
    scheduler_database: DatabaseConfig
    edr_database: DatabaseConfig
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    cassandra: CassandraConfig = Field(default_factory=CassandraConfig)
    scheduler: SchedulerTuning = Field(default_factory=SchedulerTuning)
    outbox: OutboxTuning = Field(default_factory=OutboxTuning)
    sink: SinkTuning = Field(default_factory=SinkTuning)
    projection: ProjectionTuning = Field(default_factory=ProjectionTuning)
    reconciliation: ReconciliationTuning = Field(default_factory=ReconciliationTuning)

    @model_validator(mode="after")
    def enforce_database_ownership(self) -> Self:
        if _logical_database(self.scheduler_database.url) == _logical_database(
            self.edr_database.url
        ):
            raise ConfigurationError(
                "scheduler and EDR URLs must identify different logical databases"
            )
        return self

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Self:
        """Load the required deployment names plus bounded tuning values from an environment."""
        values = os.environ if environ is None else environ

        def required(name: str) -> str:
            try:
                return values[name]
            except KeyError as exc:
                raise ConfigurationError(f"missing required setting: {name}") from exc

        def integer(name: str, default: int) -> int:
            raw = values.get(name)
            if raw is None:
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigurationError(f"{name} must be an integer") from exc

        def number(name: str, default: float) -> float:
            raw = values.get(name)
            if raw is None:
                return default
            try:
                return float(raw)
            except ValueError as exc:
                raise ConfigurationError(f"{name} must be a number") from exc

        def database(prefix: str) -> DatabaseConfig:
            return DatabaseConfig(
                url=required(f"{prefix}_DATABASE_URL"),
                pool_size=integer(f"{prefix}_DATABASE_POOL_SIZE", 5),
                max_overflow=integer(f"{prefix}_DATABASE_MAX_OVERFLOW", 5),
                pool_timeout_seconds=number(f"{prefix}_DATABASE_POOL_TIMEOUT_SECONDS", 30),
            )

        return cls(
            scheduler_database=database("SCHEDULER"),
            edr_database=database("EDR"),
            kafka=KafkaConfig(
                bootstrap_servers=values.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                edr_topic=values.get("KAFKA_EDR_TOPIC", "job-lifecycle-edr.v1"),
                edr_dlq_topic=values.get("KAFKA_EDR_DLQ_TOPIC", "job-lifecycle-edr-dlq.v1"),
                consumer_group=values.get("KAFKA_CONSUMER_GROUP", "job-visibility-projector-v1"),
                schema_registry_url=values.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            ),
            cassandra=CassandraConfig(
                contact_points=tuple(
                    point.strip()
                    for point in values.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
                    if point.strip()
                ),
                port=integer("CASSANDRA_PORT", 9042),
                keyspace=values.get("CASSANDRA_KEYSPACE", "worker_demo"),
                consistency=values.get("CASSANDRA_CONSISTENCY", "LOCAL_QUORUM"),
                request_timeout_ms=integer("CASSANDRA_REQUEST_TIMEOUT_MS", 2_000),
                execution_timeout_ms=integer("CASSANDRA_EXECUTION_TIMEOUT_MS", 30_000),
                max_record_count=integer("CASSANDRA_MAX_RECORD_COUNT", 10_000),
                max_input_number=integer("CASSANDRA_MAX_INPUT_NUMBER", 10_000),
                page_size=integer("CASSANDRA_PAGE_SIZE", 500),
                read_concurrency=integer("CASSANDRA_READ_CONCURRENCY", 8),
                max_processing_delay_ms=integer("CASSANDRA_MAX_PROCESSING_DELAY_MS", 5_000),
                conditional_retry_limit=integer("CASSANDRA_CONDITIONAL_RETRY_LIMIT", 3),
            ),
            scheduler=SchedulerTuning(
                batch_size=integer("SCHEDULER_BATCH_SIZE", 100),
                claim_lease_seconds=integer("SCHEDULER_CLAIM_LEASE_SECONDS", 60),
            ),
            outbox=OutboxTuning(
                batch_size=integer("OUTBOX_BATCH_SIZE", 100),
                retry_initial_seconds=number("OUTBOX_RETRY_INITIAL_SECONDS", 1),
                retry_max_seconds=number("OUTBOX_RETRY_MAX_SECONDS", 60),
            ),
            sink=SinkTuning(
                batch_size=integer("SINK_BATCH_SIZE", 500),
                max_retries=integer("SINK_MAX_RETRIES", 10),
                retry_backoff_ms=integer("SINK_RETRY_BACKOFF_MS", 3_000),
            ),
            projection=ProjectionTuning(batch_size=integer("PROJECTION_BATCH_SIZE", 500)),
            reconciliation=ReconciliationTuning(
                batch_size=integer("RECONCILIATION_BATCH_SIZE", 500),
                interval_seconds=integer("RECONCILIATION_INTERVAL_SECONDS", 60),
            ),
        )
