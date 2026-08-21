from __future__ import annotations

import argparse
import os
import socket
import time

from job_visibility.config import (
    database_config_from_env,
    kafka_config_from_env,
    outbox_tuning_from_env,
    projection_tuning_from_env,
    scheduler_tuning_from_env,
)
from job_visibility.edr_store import ProjectionWorker
from job_visibility.outbox import ConfluentBrokerProducer, OutboxPublisher
from job_visibility.persistence import build_database_sessions
from job_visibility.scheduler import SchedulerService, SchedulerWorker


def main() -> None:
    parser = argparse.ArgumentParser(description="Spec 002 durable process roles")
    parser.add_argument("role", choices=("scheduler", "publisher", "projector", "rebuild"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.role == "scheduler":
        tuning = scheduler_tuning_from_env()
        database = build_database_sessions(
            database_config_from_env("SCHEDULER"), role="scheduler-worker"
        )
        scheduler = SchedulerService(
            database.session_factory,
            claim_lease_seconds=tuning.claim_lease_seconds,
        )
        worker = SchedulerWorker(
            scheduler,
            owner=os.getenv("SCHEDULER_WORKER_OWNER", socket.gethostname()),
            batch_size=tuning.batch_size,
            recovery_batch_size=tuning.recovery_batch_size,
            poll_interval_seconds=tuning.poll_interval_seconds,
        )
        try:
            if args.once:
                worker.run_once()
            else:
                worker.run_forever()
        except KeyboardInterrupt:
            worker.stop()
        finally:
            database.dispose()
        return

    database_prefix = "SCHEDULER" if args.role == "publisher" else "EDR"
    database = build_database_sessions(database_config_from_env(database_prefix), role=args.role)
    try:
        if args.role == "publisher":
            kafka = kafka_config_from_env()
            outbox = outbox_tuning_from_env()
            producer = ConfluentBrokerProducer(
                kafka.bootstrap_servers,
                schema_registry_url=kafka.schema_registry_url,
                socket_timeout_ms=kafka.socket_timeout_ms,
                request_timeout_ms=kafka.request_timeout_ms,
                delivery_timeout_ms=kafka.delivery_timeout_ms,
                metadata_timeout_ms=kafka.metadata_timeout_ms,
                schema_registry_connect_timeout_seconds=(
                    kafka.schema_registry_connect_timeout_seconds
                ),
                schema_registry_read_timeout_seconds=(kafka.schema_registry_read_timeout_seconds),
            )
            worker = OutboxPublisher(
                database.session_factory,
                producer,
                owner="publisher",
                batch_size=outbox.batch_size,
                retry_initial=outbox.retry_initial_seconds,
                retry_max=outbox.retry_max_seconds,
            )
        else:
            projection = projection_tuning_from_env()
            worker = ProjectionWorker(database.session_factory, batch_size=projection.batch_size)
            if args.role == "rebuild":
                print(worker.rebuild())
                return
        while True:
            processed = worker.run_once()
            if args.once:
                return
            if processed == 0:
                time.sleep(0.5)
    except KeyboardInterrupt:
        if args.role == "publisher":
            worker.stop(kafka.flush_timeout_seconds)  # type: ignore[union-attr]
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
