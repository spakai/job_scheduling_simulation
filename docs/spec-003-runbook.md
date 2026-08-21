# Spec 003 resilience runbook

## Local workflow

Use the named, bounded interface:

```bash
scripts/infra bootstrap
scripts/infra test-postgres
scripts/infra test-resilience
scripts/infra diagnostics
scripts/infra down
```

The default Compose project is `job-visibility-resilience`. Override it with
`JOB_VISIBILITY_COMPOSE_PROJECT`. Volume deletion is intentionally separate and requires
`CONFIRM_DELETE_TEST_VOLUMES` to exactly equal the project name.

## Diagnose the last durable fact

Inspect the queues in order:

1. due scheduler jobs;
2. unpublished scheduler outbox rows;
3. Kafka/Connect lag and the DLQ; and
4. unprojected EDR journal rows.

`/health/ready` reports database reachability and configured oldest-item thresholds. A
running connector process is not healthy when its connector or any task is not `RUNNING`.

On CI failure, run `scripts/collect-test-evidence <directory>` before teardown. It records
the commit, container state/logs, connector status, and active toxics with common credential
forms redacted.

## Fault cleanup

Every Toxiproxy test resets toxics before and after execution. If a test is interrupted:

```bash
curl -fsS -X POST http://localhost:8474/reset
scripts/infra diagnostics
```

Do not delete durable volumes until the evidence is collected. Never rebuild scheduler state
from EDR data or visibility state from scheduler rows.

## Test tiers

- Pull requests run the two-PostgreSQL integration suite.
- Scheduled and manually dispatched resilience workflows run the complete local stack,
  Cassandra proxy tests, and the Kafka-to-EDR path.
- Release load evidence uses
  `resilience-results/workloads/representative-v1.json`; its status remains `proposed` until
  the resource envelope is agreed and a load runner is implemented.
