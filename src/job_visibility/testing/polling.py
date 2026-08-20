from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


class PollTimeout(TimeoutError):
    def __init__(
        self, description: str, elapsed_seconds: float, last_value: Any, attempts: int
    ) -> None:
        self.description = description
        self.elapsed_seconds = elapsed_seconds
        self.last_value = last_value
        self.attempts = attempts
        super().__init__(str(self))

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
    retry_exceptions: tuple[type[Exception], ...] = (),
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
        try:
            last_value = observe()
        except retry_exceptions as exc:
            last_value = exc
        else:
            if accept(last_value):
                return last_value
        elapsed = clock() - started
        if elapsed >= timeout_seconds:
            raise PollTimeout(description, elapsed, last_value, attempts)
        wait(min(interval_seconds, timeout_seconds - elapsed))
