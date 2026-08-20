from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PollTimeout(TimeoutError):
    description: str
    elapsed_seconds: float
    last_value: Any
    attempts: int

    def __str__(self) -> str:
        return (
            f"timed out waiting for {self.description} after {self.elapsed_seconds:.3f}s "
            f"({self.attempts} attempts); last observed value: {self.last_value!r}"
        )


def poll_until[T](
    observe: Callable[[], T],
    accept: Callable[[T], bool],
    *,
    description: str,
    timeout_seconds: float,
    interval_seconds: float = 0.1,
    clock: Callable[[], float] = time.monotonic,
    wait: Callable[[float], None] = time.sleep,
) -> T:
    if timeout_seconds <= 0:
        raise ValueError("poll timeout must be positive")
    if interval_seconds <= 0:
        raise ValueError("poll interval must be positive")

    started = clock()
    attempts = 0
    last_value: T | None = None
    while True:
        attempts += 1
        last_value = observe()
        if accept(last_value):
            return last_value
        elapsed = clock() - started
        if elapsed >= timeout_seconds:
            raise PollTimeout(description, elapsed, last_value, attempts)
        wait(min(interval_seconds, timeout_seconds - elapsed))
