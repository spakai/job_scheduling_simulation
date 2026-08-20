# Spec 002 Phase 9 evidence

Executed locally on 2026-08-20 with Docker Engine 29.3.1 and the pinned Compose stack.

## Automated baseline

- Ruff formatting and lint: passed.
- Pytest: 64 passed.
- Deterministic simulation: 18/18 scenarios passed.
- Scheduler and EDR Alembic histories reached their independent heads.

## Durable path

A real `FIBONACCI` job with limit 10000 completed with 21 values and last value 6765.
Its five lifecycle facts followed this path:

```text
scheduler transaction -> scheduler_outbox -> Schema Registry-framed Kafka
-> JDBC Sink -> edr_events -> projector -> job_visibility/job_attempts
```

The durable API returned `SUCCEEDED`, attempt 1 returned `SUCCEEDED`, and a journal-only
rebuild reproduced the projection. The measured healthy-path pipeline completed within one
second in the local single-node stack.

## Concurrency and recovery

- Two concurrent pollers claimed 20 due jobs in two non-overlapping ordered batches.
- A completion carrying a different fencing token was rejected.
- A retryable handler ran exactly three attempts and became `RETRIES_EXHAUSTED`.
- PostgreSQL and Kafka Connect restarts retained raw EDRs and the durable projection; the
  connector and task returned to `RUNNING`.
- Republished identical EDR identity/payload was a no-op and retained the first Kafka
  coordinate. A different payload with the same event identity left the first row unchanged
  and was routed to the DLQ.

## Cassandra chaos and identity

- The worker role selected a deterministic four-record sample, chose the maximum, computed
  Fibonacci, incremented checksum once, and reused the same stable operation ID on replay.
- Repeating the operation did not increment checksum again.
- A Toxiproxy downstream timeout produced a real driver `NoHostAvailable` failure.
- Removing the toxic restored bounded reads without restarting Cassandra.
- The worker role could not drop the seeded dataset table.

## Ownership and final state

- `scheduler_owner` could not connect to the EDR database.
- `edr_owner` could not connect to the scheduler database.
- A differing update through `edr_sink` was rejected by the immutable journal trigger.
- Final background-service health: PostgreSQL, Kafka, Schema Registry, Kafka Connect,
  Cassandra, and Toxiproxy healthy; JDBC connector and task `RUNNING`.
- Final observed counts: 23 scheduler jobs, zero unpublished outbox rows, 81 raw EDRs,
  81 projected-event checkpoints, and 22 durable job projections. One early diagnostic job
  intentionally used the superseded schema and remained only as DLQ evidence.
