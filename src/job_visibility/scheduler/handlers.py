from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import Protocol

from .models import ClaimedJob, HandlerResult

LOGGER = logging.getLogger(__name__)


class HandlerError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class JobHandler(Protocol):
    def __call__(self, job: ClaimedJob) -> HandlerResult: ...


def fibonacci(number: int) -> int:
    """Return F(number) using bounded fast doubling."""
    if number < 0:
        raise ValueError("number cannot be negative")

    def pair(value: int) -> tuple[int, int]:
        if value == 0:
            return 0, 1
        first, second = pair(value // 2)
        low = first * (2 * second - first)
        high = first * first + second * second
        return (high, low + high) if value % 2 else (low, high)

    return pair(number)[0]


def print_handler(job: ClaimedJob) -> HandlerResult:
    message = str((job.payload or {}).get("message", ""))
    LOGGER.info("scheduled print job", extra={"job_id": job.job_id, "message": message})
    return HandlerResult({"messageLength": len(message)})


def fibonacci_handler(job: ClaimedJob) -> HandlerResult:
    payload = job.payload or {}
    limit = payload.get("limit", 10_000)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 0 <= limit <= 10_000:
        raise HandlerError(
            "INVALID_FIBONACCI_LIMIT", "limit must be an integer from 0 to 10000", retryable=False
        )
    values: list[int] = []
    index = 0
    while True:
        value = fibonacci(index)
        if value > limit:
            break
        values.append(value)
        index += 1
    encoded = json.dumps(values, separators=(",", ":")).encode()
    return HandlerResult(
        {
            "count": len(values),
            "lastValue": values[-1] if values else None,
            "resultHash": hashlib.sha256(encoded).hexdigest(),
        }
    )


class HandlerRegistry:
    def __init__(self, handlers: Mapping[str, JobHandler] | None = None) -> None:
        self._handlers: dict[str, JobHandler] = {
            "PRINT": print_handler,
            "FIBONACCI": fibonacci_handler,
        }
        if handlers:
            self._handlers.update(handlers)

    def register(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler

    def execute(self, job: ClaimedJob) -> HandlerResult:
        try:
            handler = self._handlers[job.job_type]
        except KeyError as exc:
            raise HandlerError(
                "UNKNOWN_JOB_TYPE", "no handler is registered", retryable=False
            ) from exc
        return handler(job)
