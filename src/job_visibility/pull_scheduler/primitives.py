from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import StrEnum


class TokenBucket:
    def __init__(
        self,
        rate: float,
        burst: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate < 0 or burst < 1:
            raise ValueError("rate must be non-negative and burst must be positive")
        self.rate, self.burst, self.clock = rate, float(burst), clock
        self.tokens, self.updated_at = float(burst), clock()
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        with self._lock:
            now = self.clock()
            self.tokens = min(self.burst, self.tokens + (now - self.updated_at) * self.rate)
            self.updated_at = now
            if self.rate == 0 or self.tokens < 1:
                return False
            self.tokens -= 1
            return True


class OwnerGate:
    def __init__(self) -> None:
        self._active: set[str] = set()
        self._lock = threading.Lock()

    def acquire(self, owner_id: str) -> bool:
        with self._lock:
            if owner_id in self._active:
                return False
            self._active.add(owner_id)
            return True

    def release(self, owner_id: str) -> None:
        with self._lock:
            self._active.discard(owner_id)


class ContiguousOffsetTracker:
    """Track completed offsets without committing across a gap."""

    def __init__(self, next_offset: int) -> None:
        self.next_offset = next_offset
        self._completed: set[int] = set()

    def complete(self, offset: int) -> int:
        if offset < self.next_offset:
            return self.next_offset
        self._completed.add(offset)
        while self.next_offset in self._completed:
            self._completed.remove(self.next_offset)
            self.next_offset += 1
        return self.next_offset


class BackpressureState(StrEnum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    PROBING = "PROBING"


class BackpressureController:
    def __init__(
        self,
        *,
        failure_threshold: int = 1,
        recovery_threshold: int = 2,
        initial_backoff_seconds: float = 1,
        max_backoff_seconds: float = 30,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if min(failure_threshold, recovery_threshold) < 1:
            raise ValueError("backpressure thresholds must be positive")
        if initial_backoff_seconds <= 0 or initial_backoff_seconds > max_backoff_seconds:
            raise ValueError("backpressure backoff range is invalid")
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self.initial_backoff = initial_backoff_seconds
        self.max_backoff = max_backoff_seconds
        self.clock = clock
        self.state = BackpressureState.RUNNING
        self.failures = 0
        self.successes = 0
        self.pause_count = 0
        self.paused_at: float | None = None
        self.next_probe_at = 0.0
        self._backoff = initial_backoff_seconds

    def failure(self) -> BackpressureState:
        self.failures += 1
        self.successes = 0
        if self.failures >= self.failure_threshold:
            now = self.clock()
            self.state = BackpressureState.PAUSED
            self.paused_at = self.paused_at or now
            self.next_probe_at = now + self._backoff
            self._backoff = min(self.max_backoff, self._backoff * 2)
            self.pause_count += 1
        return self.state

    def should_probe(self) -> bool:
        if self.state is BackpressureState.PAUSED and self.clock() >= self.next_probe_at:
            self.state = BackpressureState.PROBING
            return True
        return self.state is BackpressureState.PROBING

    def success(self) -> BackpressureState:
        if self.state is BackpressureState.RUNNING:
            self.failures = 0
            return self.state
        self.successes += 1
        if self.successes >= self.recovery_threshold:
            self.state = BackpressureState.RUNNING
            self.failures = 0
            self.successes = 0
            self.paused_at = None
            self._backoff = self.initial_backoff
        else:
            self.state = BackpressureState.PROBING
        return self.state
