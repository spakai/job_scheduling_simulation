# Spec 006 Kafka Pull Scheduler Runbook

## Start

```bash
docker compose --profile pull up --build kafka schema-registry kafka-setup \
  pull-worker-1 pull-worker-2
```

Workers use group `job-workers-v1`, unique transactional IDs, six work partitions, and no
scheduler database credentials. Each container exposes liveness, readiness, and metrics on
port 8080 inside the Compose network.

## Submit and load test

Use `job-visibility-pull produce` as shown in the README. Generate the 20,000-request
baseline with:

```bash
docker compose --profile pull run --rm pull-worker-1 job-visibility-pull load \
  --count 20000 --owners 1000 --hot-owner-share 0.5 --seed 6
```

The load command preserves canonical `subscriber:<id>` owner keys. It produces as quickly
as Kafka accepts requests; arrival-window pacing is controlled by the chaos harness.

## Migrate a bounded legacy export

Export one validated request envelope per JSONL line, then run:

```bash
docker compose --profile pull run --rm pull-worker-1 job-visibility-pull migrate \
  /data/jobs.jsonl --manifest /data/kafka-manifest.jsonl
```

Mount `/data` explicitly. The manifest records acknowledged job, owner, partition, and
offset coordinates. Spec 006 has no durable job deduplication, so never rerun a partially
successful migration without reconciling the manifest first.

## Diagnose

Check, in order:

1. worker `/health/live` and `/health/ready`;
2. `pull_scheduler_backpressure`, assigned partitions, and transaction failures;
3. consumer-group lag and oldest record age for `job-workers-v1`;
4. result, lifecycle, and DLQ topic production;
5. repeated `(jobId, attempt)` observations, which are permitted by Spec 006.

During output failure the worker aborts the transaction, seeks to the source offset, pauses
assignments, continues heartbeat polls, and probes with bounded backoff. Do not manually
advance offsets to clear lag.

## Shutdown

```bash
docker compose --profile pull stop pull-worker-1 pull-worker-2
```

SIGTERM stops new loop iterations, closes health serving, closes the consumer, and flushes
the producer. An uncommitted record is redelivered. Duplicate handler execution is possible
and is deferred to the next specification.
