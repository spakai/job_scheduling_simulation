import pytest
from pydantic import ValidationError

from job_visibility.config import AppConfig, ConfigurationError

BASE_ENV = {
    "SCHEDULER_DATABASE_URL": "postgresql+psycopg://scheduler:secret@db:5432/scheduler",
    "EDR_DATABASE_URL": "postgresql+psycopg://edr:other-secret@db:5432/edr",
}


def test_loads_role_specific_configuration_from_environment() -> None:
    config = AppConfig.from_env(
        BASE_ENV
        | {
            "KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
            "SCHEDULER_CLAIM_LEASE_SECONDS": "90",
            "CASSANDRA_CONTACT_POINTS": "cassandra-1, cassandra-2",
            "CASSANDRA_PAGE_SIZE": "250",
            "CASSANDRA_PORT": "9142",
            "SCHEDULER_DATABASE_POOL_SIZE": "7",
            "EDR_DATABASE_MAX_OVERFLOW": "2",
            "OUTBOX_RETRY_INITIAL_SECONDS": "0.25",
            "SINK_BATCH_SIZE": "1000",
        }
    )

    assert config.kafka.bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert config.scheduler.claim_lease_seconds == 90
    assert config.cassandra.contact_points == ("cassandra-1", "cassandra-2")
    assert config.cassandra.page_size == 250
    assert config.cassandra.port == 9142
    assert config.scheduler_database.pool_size == 7
    assert config.edr_database.max_overflow == 2
    assert config.outbox.retry_initial_seconds == 0.25
    assert config.sink.batch_size == 1000


def test_rejects_same_logical_database_even_with_different_credentials() -> None:
    env = BASE_ENV | {
        "EDR_DATABASE_URL": "postgresql+psycopg://another:password@db:5432/scheduler?sslmode=require"
    }

    with pytest.raises(ValidationError, match="different logical databases"):
        AppConfig.from_env(env)


def test_rejects_missing_required_database_setting_without_exposing_secrets() -> None:
    with pytest.raises(ConfigurationError, match="EDR_DATABASE_URL"):
        AppConfig.from_env({"SCHEDULER_DATABASE_URL": "postgresql://user:secret@db/scheduler"})


def test_rejects_unbounded_or_inconsistent_tuning() -> None:
    with pytest.raises(ValidationError, match="request timeout"):
        AppConfig.from_env(
            BASE_ENV
            | {
                "CASSANDRA_REQUEST_TIMEOUT_MS": "5000",
                "CASSANDRA_EXECUTION_TIMEOUT_MS": "1000",
            }
        )


def test_database_passwords_are_redacted_from_repr() -> None:
    config = AppConfig.from_env(BASE_ENV)

    assert "secret" not in repr(config)
    assert "other-secret" not in repr(config)
