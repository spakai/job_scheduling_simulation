from __future__ import annotations

import threading
import time
from collections.abc import Callable


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
