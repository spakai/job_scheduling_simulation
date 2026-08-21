"""Deterministic application fault injector with production-safe defaults."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from pydantic import TypeAdapter, ValidationError

from .model import Checkpoint, FaultAction, FaultRule

LOGGER = logging.getLogger(__name__)
_RULES = TypeAdapter(list[FaultRule])


class SyntheticFault(RuntimeError):
    """Allowlisted exception raised by a configured chaos experiment."""

    def __init__(self, error_code: str, checkpoint: Checkpoint) -> None:
        super().__init__(f"{error_code} at {checkpoint.value}")
        self.error_code = error_code
        self.checkpoint = checkpoint


@dataclass(frozen=True, slots=True)
class FaultContext:
    job_id: str | None = None
    correlation_id: str | None = None
    event_id: str | None = None


EMPTY_CONTEXT = FaultContext()


class FaultInjector(Protocol):
    def inject(self, checkpoint: Checkpoint, context: FaultContext = EMPTY_CONTEXT) -> None: ...


class NoOpFaultInjector:
    def inject(self, checkpoint: Checkpoint, context: FaultContext = EMPTY_CONTEXT) -> None:
        return None


class ConfiguredFaultInjector:
    def __init__(
        self,
        experiment_id: str,
        rules: Sequence[FaultRule],
        *,
        wait: Callable[[float], None] = time.sleep,
        terminate: Callable[[int], None] = os._exit,
    ) -> None:
        if not experiment_id or len(experiment_id) > 100:
            raise ValueError("a bounded experiment_id is required")
        self.experiment_id = experiment_id
        self.rules = tuple(rules)
        self.wait = wait
        self.terminate = terminate
        self._counts = [0 for _ in rules]
        self._lock = threading.Lock()
        self._barriers = {
            index: threading.Event()
            for index, rule in enumerate(rules)
            if rule.action is FaultAction.PAUSE
        }

    def inject(self, checkpoint: Checkpoint, context: FaultContext = EMPTY_CONTEXT) -> None:
        for index, rule in enumerate(self.rules):
            if not self._matches(rule, checkpoint, context):
                continue
            with self._lock:
                if self._counts[index] >= rule.invocations:
                    continue
                self._counts[index] += 1
                count = self._counts[index]
            if count != rule.activate_on:
                continue
            LOGGER.warning(
                "chaos fault activated experiment=%s checkpoint=%s action=%s activation=%d",
                self.experiment_id,
                checkpoint.value,
                rule.action.value,
                count,
            )
            if rule.action is FaultAction.DELAY:
                self.wait(rule.delay_seconds)
            elif rule.action is FaultAction.RAISE:
                raise SyntheticFault(rule.error_code, checkpoint)
            elif rule.action is FaultAction.EXIT:
                self.terminate(rule.exit_code)
            else:
                self._barriers[index].wait(rule.pause_timeout_seconds)

    def release_pauses(self) -> None:
        for barrier in self._barriers.values():
            barrier.set()

    @staticmethod
    def _matches(rule: FaultRule, checkpoint: Checkpoint, context: FaultContext) -> bool:
        return (
            rule.checkpoint is checkpoint
            and (rule.job_id is None or rule.job_id == context.job_id)
            and (rule.correlation_id is None or rule.correlation_id == context.correlation_id)
        )


def fault_injector_from_env(
    environ: Mapping[str, str] | None = None,
    *,
    wait: Callable[[float], None] = time.sleep,
    terminate: Callable[[int], None] = os._exit,
) -> FaultInjector:
    """Return a configured injector only behind explicit, non-production safety gates."""

    values = os.environ if environ is None else environ
    if values.get("CHAOS_MODE", "disabled").lower() != "enabled":
        return NoOpFaultInjector()
    environment = values.get("APP_ENVIRONMENT", "development").lower()
    if environment in {"prod", "production"}:
        raise ValueError("chaos mode is forbidden in production")
    experiment_id = values.get("CHAOS_EXPERIMENT_ID", "")
    raw_rules = values.get("CHAOS_FAULTS_JSON")
    if not experiment_id or not raw_rules:
        raise ValueError("chaos mode requires CHAOS_EXPERIMENT_ID and CHAOS_FAULTS_JSON")
    try:
        data = json.loads(raw_rules)
        rules = _RULES.validate_python(data)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ValueError("CHAOS_FAULTS_JSON must contain valid bounded fault rules") from exc
    if not rules:
        raise ValueError("chaos mode requires at least one fault rule")
    return ConfiguredFaultInjector(experiment_id, rules, wait=wait, terminate=terminate)
