from __future__ import annotations

import hashlib
import json
from typing import Any

from job_visibility.model import Event, iso


def canonical_edr(event: Event) -> tuple[str, str]:
    value: dict[str, Any] = {
        "schemaVersion": 1,
        "eventId": event.event_id,
        "eventType": event.event_type.value,
        "eventTime": iso(event.event_time),
        "ingestionTime": iso(event.ingestion_time),
        "jobId": event.job_id,
        "correlationId": event.correlation_id or None,
        "jobType": event.job_type or None,
        "attemptNumber": event.attempt_number,
        "maxAttempts": event.max_attempts,
        "scheduledAt": iso(event.scheduled_at),
        "nextRetryAt": iso(event.next_retry_at),
        "retryable": event.retryable,
        "resultCode": event.result_code,
        "errorCode": event.error_code,
    }
    domain_payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(domain_payload.encode("utf-8")).hexdigest()
    value["canonicalPayload"] = domain_payload
    value["payloadHash"] = digest
    wire_payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return wire_payload, digest
