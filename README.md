# Vert.x Kafka Pull Worker (Spec 007)

This repository contains the Spec 007 Java 21 / Vert.x 5 Kafka pull worker. It consumes
immediately eligible subscriber and group requests, executes different owners concurrently,
preserves FIFO ordering for the same owner, and commits completed Kafka prefixes atomically
with their source offsets.

Spec 007 is implemented and validated locally. Production migration, environment-specific
capacity, security, and acceptance gates remain open.

## Start here

- [Worker source and direct-run notes](vertx-pull-worker/README.md)
- [Spec 007 specification](specs/007-vertx-kafka-pull-worker/spec.md)
- [Spec 007 implementation plan](specs/007-vertx-kafka-pull-worker/plan.md)
- [Spec 007 arc42 architecture](specs/007-vertx-kafka-pull-worker/arc42.md)
- [Spec 007 runbook](docs/spec-007-runbook.md)
- [Spec 007 evidence](docs/spec-007-evidence.md)
- [Root architecture overview](architecture.md)

## Architecture at a glance

Each workload fleet uses one subscribed Kafka consumer per pod. Kafka assigns partitions to
the pods, and each assigned partition has a context-confined lane with bounded concurrency.
Different owners may run at the same time; records for one owner remain serialized until the
Kafka transaction containing their safe source-offset prefix commits.

The worker provides:

- subscriber and group workload isolation through separate topics, consumer groups, fleets,
  capacity budgets, and TPS limits;
- exact manual source-offset tracking with contiguous-completion prefixes;
- durable attempt and completion EDRs, result records, retry/DLQ records, and completed-request
  deduplication;
- partition pause/resume, bounded queues, pod-wide adaptive admission, and isolated retry
  requeue deployments; and
- readiness, health, metrics, tracing, rebalance fencing, bounded drain, and recovery behavior.

The worker does not own a scheduler database, due-job query, scheduler outbox, or visibility
API. Kafka is the work and execution-state boundary. External business effects remain at least
once across an ambiguous external-call failure, so handlers must use the stable `jobId` as an
idempotency key when supported.

## Quick start

Run from the repository root. Host tests require Java 21 and Maven 3.6.3 or newer. Container
workflows require Docker Engine and Docker Compose.

```bash
scripts/spec007 test     # unit and real-Kafka integration tests
scripts/spec007 build    # build spec007-worker:local
scripts/spec007 smoke    # two workers and synthetic HTTP dependency
scripts/spec007 produce 100
```

Run the larger local checks with a running baseline:

```bash
scripts/spec007 baseline  # five workers and ten partitions
scripts/spec007 capacity 1000
scripts/spec007 capacity  # 20,000 and 100,000 compressed requests
scripts/spec007 chaos     # kill one baseline worker and verify recovery
```

Check readiness inside a worker:

```bash
docker compose -f compose.yaml -f compose.spec007.yaml exec -T spec007-subscriber-0 \
  wget -q -O - http://localhost:8080/health/ready
```

Readiness waits for Kafka assignment and durable ledger restoration before dispatch.

## Workloads

| Workload | Default work topic | Kafka key | Envelope owner |
| --- | --- | --- | --- |
| Subscriber | `v007-subscriber-rerate` | `42` | `subscriber:42` |
| Group | `v007-group-rerate` | `17` | `group:17` |

The exact production topic names, schemas, deployment settings, and migration rules are
defined by the [Spec 007 specification](specs/007-vertx-kafka-pull-worker/spec.md). The local
demo uses synthetic data and a synthetic dependency only.

## Validation status

The current local evidence includes unit and Kafka integration tests, compressed 20,000 and
100,000 request capacity runs, and recovery of requests after a worker container kill. These
results demonstrate the local implementation and do not establish production business
throughput or availability.

See the [evidence report](docs/spec-007-evidence.md) for measured results and remaining gates,
and the [runbook](docs/spec-007-runbook.md) for operations, migration, and rollback.