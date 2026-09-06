from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from .config import pull_scheduler_config_from_env
from .contracts import PullJobRequest
from .runtime import run_pull_worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka pull scheduler")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("worker")
    produce = subparsers.add_parser("produce")
    produce.add_argument("--job-id", required=True)
    produce.add_argument("--owner-id", required=True)
    produce.add_argument("--owner-type", required=True, choices=("SUBSCRIBER", "GROUP"))
    produce.add_argument("--correlation-id", required=True)
    produce.add_argument("--job-type", default="PRINT")
    produce.add_argument("--payload", default='{"message":"hello"}')
    args = parser.parse_args()
    config = pull_scheduler_config_from_env()
    if args.command == "worker":
        run_pull_worker(config)
        return

    from confluent_kafka import Producer

    request = PullJobRequest(
        jobId=args.job_id,
        ownerId=args.owner_id,
        ownerType=args.owner_type,
        correlationId=args.correlation_id,
        jobType=args.job_type,
        requestedAt=datetime.now(UTC),
        payload=json.loads(args.payload),
    )
    producer = Producer(
        {"bootstrap.servers": config.bootstrap_servers, "enable.idempotence": True, "acks": "all"}
    )
    errors: list[str] = []
    producer.produce(
        config.work_topic,
        key=request.owner_id.encode(),
        value=json.dumps(request.wire()).encode(),
        callback=lambda error, _message: errors.append(str(error)) if error else None,
    )
    if producer.flush(10) or errors:
        raise RuntimeError(errors[0] if errors else "request delivery timed out")


if __name__ == "__main__":
    main()
