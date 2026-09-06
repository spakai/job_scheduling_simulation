from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

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
    load = subparsers.add_parser("load")
    load.add_argument("--count", type=int, default=20_000)
    load.add_argument("--owners", type=int, default=1_000)
    load.add_argument("--hot-owner-share", type=float, default=0)
    load.add_argument("--seed", type=int, default=6)
    migrate = subparsers.add_parser("migrate")
    migrate.add_argument("jsonl", type=Path)
    migrate.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    config = pull_scheduler_config_from_env()
    if args.command == "worker":
        run_pull_worker(config)
        return

    from confluent_kafka import Producer

    producer = Producer(
        {"bootstrap.servers": config.bootstrap_servers, "enable.idempotence": True, "acks": "all"}
    )
    errors: list[str] = []
    manifest: list[dict[str, object]] = []

    def send(request: PullJobRequest) -> None:
        def delivered(error: object, message: object) -> None:
            if error:
                errors.append(str(error))
                return
            manifest.append(
                {
                    "jobId": request.job_id,
                    "ownerId": request.owner_id,
                    "partition": message.partition(),  # type: ignore[attr-defined]
                    "offset": message.offset(),  # type: ignore[attr-defined]
                }
            )

        producer.produce(
            config.work_topic,
            key=request.owner_id.encode(),
            value=json.dumps(request.wire()).encode(),
            callback=delivered,
        )
        producer.poll(0)

    if args.command == "produce":
        send(
            PullJobRequest(
                jobId=args.job_id,
                ownerId=args.owner_id,
                ownerType=args.owner_type,
                correlationId=args.correlation_id,
                jobType=args.job_type,
                requestedAt=datetime.now(UTC),
                payload=json.loads(args.payload),
            )
        )
    elif args.command == "load":
        if args.count < 1 or args.owners < 1 or not 0 <= args.hot_owner_share <= 1:
            parser.error("load bounds must be positive and hot-owner-share must be from 0 to 1")
        random_source = random.Random(args.seed)
        hot_count = round(args.count * args.hot_owner_share)
        for index in range(args.count):
            owner = 0 if index < hot_count else random_source.randrange(args.owners)
            send(
                PullJobRequest(
                    jobId=f"load-{args.seed}-{index}-{uuid4()}",
                    ownerId=f"subscriber:{owner}",
                    ownerType="SUBSCRIBER",
                    correlationId=f"load-{args.seed}",
                    jobType="PRINT",
                    requestedAt=datetime.now(UTC),
                    payload={"message": "load"},
                )
            )
    else:
        with args.jsonl.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    send(PullJobRequest.model_validate_json(line))
    if producer.flush(10) or errors:
        raise RuntimeError(errors[0] if errors else "request delivery timed out")
    if args.command == "migrate":
        args.manifest.write_text(
            "\n".join(json.dumps(item, sort_keys=True) for item in manifest) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
