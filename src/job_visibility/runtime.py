from __future__ import annotations

import argparse
import time

from job_visibility.config import AppConfig
from job_visibility.edr_store import ProjectionWorker
from job_visibility.outbox import ConfluentBrokerProducer, OutboxPublisher
from job_visibility.persistence import build_durable_sessions


def main() -> None:
    parser = argparse.ArgumentParser(description="Spec 002 durable process roles")
    parser.add_argument("role", choices=("publisher", "projector", "rebuild"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    config = AppConfig.from_env()
    sessions = build_durable_sessions(config)
    try:
        if args.role == "publisher":
            producer = ConfluentBrokerProducer(
                config.kafka.bootstrap_servers,
                schema_registry_url=config.kafka.schema_registry_url,
            )
            worker = OutboxPublisher(
                sessions.scheduler.session_factory,
                producer,
                owner="publisher",
                batch_size=config.outbox.batch_size,
                retry_initial=config.outbox.retry_initial_seconds,
                retry_max=config.outbox.retry_max_seconds,
            )
        else:
            worker = ProjectionWorker(
                sessions.edr.session_factory, batch_size=config.projection.batch_size
            )
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
            worker.stop()  # type: ignore[union-attr]
    finally:
        sessions.dispose()


if __name__ == "__main__":
    main()
