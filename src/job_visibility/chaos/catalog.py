"""Versioned Spec 005 experiment catalog used by the CLI and documentation."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class Scenario:
    id: str
    tier: str
    fault: str
    hypothesis: str


SCENARIOS = (
    Scenario("NET-01", "pr", "dependency latency and jitter", "deadlines remain bounded"),
    Scenario("NET-02", "nightly", "Connect bandwidth restriction", "lag drains without loss"),
    Scenario("NET-03", "pr", "scheduler DB pool saturation", "bounded backpressure recovers"),
    Scenario("MEM-01", "pr", "worker OOM after claim", "lease recovery yields one outcome"),
    Scenario("MEM-02", "nightly", "publisher OOM after acknowledgement", "replay is idempotent"),
    Scenario("MEM-03", "nightly", "visibility API OOM", "scheduler remains isolated"),
    Scenario("CPU-01", "pr", "worker CPU saturation", "fencing holds and backlog drains"),
    Scenario("CPU-02", "nightly", "PostgreSQL CPU pressure", "deadlines and pools recover"),
    Scenario("CPU-03", "nightly", "Connect CPU pressure", "lag drains without loss"),
    Scenario("DISK-01", "nightly", "scheduler DB write latency", "submission remains atomic"),
    Scenario("DISK-02", "nightly", "EDR DB write latency", "scheduler remains available"),
    Scenario(
        "DISK-03", "nightly", "disposable DB volume full", "failure and recovery are explicit"
    ),
    Scenario("APP-01", "pr", "exception before scheduler commit", "no partial state persists"),
    Scenario("APP-02", "pr", "failure after scheduler commit", "retry is idempotent"),
    Scenario("APP-03", "pr", "publisher exit after broker ack", "redelivery deduplicates"),
    Scenario("APP-04", "pr", "projector exit before commit", "checkpoint replay is safe"),
    Scenario("APP-05", "nightly", "application checkpoint delay", "backpressure is bounded"),
    Scenario("APP-06", "nightly", "poison event", "DLQ does not block progress"),
)


def catalog_json() -> list[dict[str, str]]:
    return [asdict(scenario) for scenario in SCENARIOS]
