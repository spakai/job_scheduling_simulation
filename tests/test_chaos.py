from __future__ import annotations

import json
from threading import Thread

import pytest
from pydantic import ValidationError

from job_visibility.chaos import (
    Checkpoint,
    ConfiguredFaultInjector,
    FaultAction,
    FaultContext,
    FaultRule,
    NoOpFaultInjector,
    SyntheticFault,
    fault_injector_from_env,
)
from job_visibility.chaos.catalog import SCENARIOS, catalog_json


def test_fault_rule_rejects_unbounded_or_contradictory_actions() -> None:
    with pytest.raises(ValidationError, match="less than or equal to 30"):
        FaultRule(
            checkpoint=Checkpoint.VISIBILITY_BEFORE_QUERY,
            action=FaultAction.DELAY,
            delay_seconds=31,
        )
    with pytest.raises(ValidationError, match="delay action requires"):
        FaultRule(
            checkpoint=Checkpoint.SCHEDULER_BEFORE_COMMIT,
            action=FaultAction.DELAY,
        )
    with pytest.raises(ValidationError, match="valid only for delay"):
        FaultRule(
            checkpoint=Checkpoint.SCHEDULER_BEFORE_COMMIT,
            action=FaultAction.RAISE,
            delay_seconds=1,
        )


def test_injector_matches_identity_and_exact_invocation() -> None:
    waits: list[float] = []
    injector = ConfiguredFaultInjector(
        "experiment-1",
        [
            FaultRule(
                checkpoint=Checkpoint.WORKER_AFTER_CLAIM,
                action=FaultAction.DELAY,
                delay_seconds=0.25,
                job_id="selected",
                invocations=2,
                activate_on=2,
            )
        ],
        wait=waits.append,
    )

    injector.inject(Checkpoint.WORKER_AFTER_CLAIM, FaultContext(job_id="other"))
    injector.inject(Checkpoint.WORKER_AFTER_CLAIM, FaultContext(job_id="selected"))
    injector.inject(Checkpoint.WORKER_AFTER_CLAIM, FaultContext(job_id="selected"))
    injector.inject(Checkpoint.WORKER_AFTER_CLAIM, FaultContext(job_id="selected"))

    assert waits == [0.25]


def test_raise_and_exit_are_allowlisted_and_testable() -> None:
    exits: list[int] = []
    raising = ConfiguredFaultInjector(
        "raise-test",
        [
            FaultRule(
                checkpoint=Checkpoint.PUBLISHER_AFTER_BROKER_ACK,
                action=FaultAction.RAISE,
                error_code="ACK_LOST",
            )
        ],
    )
    exiting = ConfiguredFaultInjector(
        "exit-test",
        [
            FaultRule(
                checkpoint=Checkpoint.PROJECTOR_AFTER_APPLY,
                action=FaultAction.EXIT,
                exit_code=87,
            )
        ],
        terminate=exits.append,
    )

    with pytest.raises(SyntheticFault, match="ACK_LOST"):
        raising.inject(Checkpoint.PUBLISHER_AFTER_BROKER_ACK)
    exiting.inject(Checkpoint.PROJECTOR_AFTER_APPLY)

    assert exits == [87]


def test_pause_is_bounded_and_releasable() -> None:
    injector = ConfiguredFaultInjector(
        "pause-test",
        [
            FaultRule(
                checkpoint=Checkpoint.WORKER_BEFORE_COMPLETE,
                action=FaultAction.PAUSE,
                pause_timeout_seconds=1,
            )
        ],
    )
    thread = Thread(target=injector.inject, args=(Checkpoint.WORKER_BEFORE_COMPLETE,))
    thread.start()
    injector.release_pauses()
    thread.join(timeout=1)

    assert not thread.is_alive()


def test_environment_loader_is_disabled_by_default_and_forbidden_in_production() -> None:
    assert isinstance(fault_injector_from_env({}), NoOpFaultInjector)
    with pytest.raises(ValueError, match="forbidden in production"):
        fault_injector_from_env(
            {
                "CHAOS_MODE": "enabled",
                "APP_ENVIRONMENT": "production",
                "CHAOS_EXPERIMENT_ID": "unsafe",
                "CHAOS_FAULTS_JSON": "[]",
            }
        )


def test_environment_loader_requires_bounded_valid_rules() -> None:
    with pytest.raises(ValueError, match="requires CHAOS_EXPERIMENT_ID"):
        fault_injector_from_env({"CHAOS_MODE": "enabled"})

    rules = [
        {
            "checkpoint": "scheduler.before_commit",
            "action": "raise",
            "job_id": "job-1",
        }
    ]
    injector = fault_injector_from_env(
        {
            "CHAOS_MODE": "enabled",
            "APP_ENVIRONMENT": "test",
            "CHAOS_EXPERIMENT_ID": "app-01",
            "CHAOS_FAULTS_JSON": json.dumps(rules),
        }
    )

    with pytest.raises(SyntheticFault):
        injector.inject(Checkpoint.SCHEDULER_BEFORE_COMMIT, FaultContext(job_id="job-1"))


def test_catalog_has_unique_spec_scenario_ids() -> None:
    identifiers = [scenario.id for scenario in SCENARIOS]

    assert len(identifiers) == 18
    assert len(set(identifiers)) == len(identifiers)
    assert catalog_json()[0]["id"] == "NET-01"
