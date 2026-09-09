# Spec 007 operations

## Build and local validation

Run `scripts/spec007 test` with Docker available. It starts only the repository Kafka broker,
then runs Java unit and real-broker integration tests in uniquely named topics/groups.
Run `scripts/spec007 build` and `scripts/spec007 smoke` for two non-root workers and the
explicit demo HTTP dependency. `scripts/spec007 produce 100` publishes requests using raw
entity keys. `scripts/spec007 baseline` starts the five-slot subscriber fleet on the same
10 partitions. Do not run smoke and baseline as separate deployments: they share slot IDs.
The demo dependency returns synthetic results; it is not a production business implementation.

Inspect `/health/live`, `/health/ready`, and `/metrics` inside a worker container. Prometheus
includes JVM/Vert.x Micrometer metrics and `vtx_*` lane/admission counters. Optional tracing
uses `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (HTTP endpoint including `/v1/traces`). Trace fields
include logical/physical attempt IDs, correlation ID, hashed owner, source coordinates and
epoch; payloads are excluded from logs and spans. Governed payloads do exist in Kafka outputs
and the local ledger, so apply Kafka ACLs and volume controls accordingly.

## Configuration and capacity

`RuntimeConfig` is the executable configuration reference. Required production inputs are
`HANDLER_URL`, `KAFKA_BOOTSTRAP_SERVERS`, and an explicitly assigned `WORKER_SLOT`. Use stable
StatefulSet names for slots. `TOPIC_NAMESPACE` defaults to `v007-`; use a separate namespace
per environment. Work topic/key examples are `v007-subscriber-rerate` / `42` with envelope
`ownerId=subscriber:42`, and `v007-group-rerate` / `17` with `ownerId=group:17`.
`KAFKA_GROUP_ID` must equal the namespace + workload + `-workers` (`-retry-workers` for requeue).

The subscriber defaults are 10 partitions, 10 pod slots, 10 partition slots, 2 TPS and burst 2.
Partitions share the pod slots. Pod tracking has a hard record limit of twice `WINDOW_RECORDS`;
per-partition tracking is bounded by `WINDOW_RECORDS`/`WINDOW_BYTES`. Fetch pauses before the
hard limits, retaining poll headroom. Completed outcomes still occupy tracking capacity.
HTTP responses are bounded by `MAX_RECORD_BYTES`. State restoration uses disk, not an
unbounded in-memory map. Each assigned state store is limited to 256 MiB; size the bounded
pod volume for the maximum assignments. Disk exhaustion triggers fenced recovery and an
operator alarm; do not delete the Kafka authority to recover disk.

TPS is per pod, not fleet-wide. Rebudget subscriber, group and retry allocations on scaling.
At the default rate the adaptive targets are 2, 1, 0.5 and 0 TPS. Five samples with at least
50% dependency failures degrade one step. Thirty seconds and five consecutive healthy
samples are required to step up. Open state permits one business probe after 30 seconds;
administrative zero allows none. Set `ADAPTIVE_MIN_SAMPLES` and `ADAPTIVE_COOLDOWN_MS` to configure the sample
window and cooldown. A permit carries its circuit generation: an older in-flight result
cannot resolve the current half-open probe.

Internal retries default to two, with fresh durable attempt IDs, fresh TPS/capacity admission,
jittered timer backoff and a total elapsed budget. A successful completion is retained by job
ID across logical generations. A new intentional rerate must have a new job ID.

## Pause, gaps and recovery

For administrative pause, set `RATE_LIMIT_TPS=0` and roll the relevant fleet. Inspect
`vtx_effective_tps`, tracking gauges, committed-next offsets and dependency metrics separately.
A completed follower does not move a partition past its unfinished head. Fetch position is
not the committed position, and Kafka commit does not delete records.

Required Kafka-output failure invalidates assignment tokens and replaces the runtime after
bounded backoff. A replacement initializes the stable source producer, fences partition EDR
writers, captures read-committed ledger ends, restores state, and only then resumes partitions.
There is no same-epoch rewind. A failed drain requires process replacement; do not start a
second runtime alongside possibly live native calls. Orchestrator termination grace is 90s.

TPS admission precedes lease creation, so administrative zero TPS creates no attempt EDRs.
Before business invocation, `ATTEMPT_STARTED` is durable with a lease and physical token.
Before source completion, `JOB_COMPLETED` and its recoverable result are durable.
Exhausted/nonretryable invocations persist `DISPOSITION_COMPLETED` with the exact result,
lifecycle and retry/DLQ records under a logical-attempt key. Replay reconstructs those outputs
without another business call, including failures completed behind an unresolved prefix gap. A live
started-only lease waits; an expired lease allows a new invocation with the same logical
idempotency key. A handler that does not settle by its deadline requires replacement and
retains its permit until its callback exits. Lease expiry does not prove an external effect
stopped. The HTTP dependency **must** honor `Idempotency-Key: jobId` across attempts and pods.

An ambiguous source commit may already be visible. Inspect broker offsets and read-committed
outputs after producer fencing; never infer an abort from a client timeout. Source finalization
keys are separate from job EDR keys. A former source producer cannot mutate job EDR state.

## Retention and requeue

Set `DEDUP_RETENTION_MS` greater than `REPLAY_WINDOW_MS` and all outage/support windows.
The execution ledger is compact-only. Restoration always rebuilds local state from Kafka;
local file/cache loss cannot erase a durable completion. Expired identities produce an EXPIRED
audit plus a ledger tombstone transaction, then local deletion. Replays after expiry can execute
again. A bounded background sweep processes 100 files per tick; expiry is also checked on read.

Retry profiles are not started by default. Start `spec007-retry-0` for subscriber quarantine
or `spec007-group-retry-0` for group quarantine. Their topics, consumer groups and TPS
budgets remain isolated. It validates the original topic, partition, key, generation, age and stable handoff
hash. It atomically republishes to the original work topic and commits its retry offset; it
never invokes business logic. Handoff identities are materialized from the compacted
finalization topic to suppress repeated requeue. The finalization topic uses compact+delete
with the same retention window. Terminal DLQs never replay automatically.

## Migration and rollback

1. Quiesce old producers; let Python drain `job-requests.v1` / `job-workers-v1`.
2. Scale Python to zero. Run `Operations migration-check job-workers-v1 job-requests.v1`
   (via `java -cp ... com.example.jobs.pull.Operations ...`). It rejects active members or lag.
3. Translate producer routing to workload topics with raw canonical entity keys. Preserve
   job IDs and immutable envelopes. Never copy numerical offsets between topics.
4. Keep the old fleet disabled through the deployment controller. Set `LEGACY_GROUP_ID`
   and `LEGACY_WORK_TOPIC` on production workers for a startup precondition check.
5. Start new workers and wait for EDR restoration/readiness, then resume producers.
6. For rollback, quiesce producers and drain/stop the new fleets first. Run the same inactive/
   drained check for each new group/topic. Resolve outstanding quarantine using the EDR-aware
   path before restoring legacy routing. Python does not understand the new EDR authority;
   never blindly replay the new backlog into Python.

The read-only check is a cutover precondition, not a distributed deployment lock. Enforce
mutually exclusive fleet deployment in the release controller. No production cutover is
performed by the local scripts.

## Production release checks

Use `infra/spec007/kubernetes/workers.yaml` as a template: replace image with the tested digest,
configure the real dependency and brokers, create the `rerate-kafka-client` secret containing
only Kafka security properties, and provision production topics with replication >=3 and
minimum ISR >=2. The local topic tool defaults to a single broker only.

Grant each workload principal READ/DESCRIBE on its source, WRITE/DESCRIBE on its output topics,
READ on its ledger, group access only to its allowlisted group, source transactional IDs for
its stable slots and EDR IDs only for its workload partitions. Retry principals need READ on
retry/finalization and WRITE on original work/finalization/DLQ; no business credentials.

Run multi-broker loss, repeated reassignment/pod kill, disk pressure, dependency outage, and
real 60–180-second load profiles before production approval. The deterministic 20,000/100,000
tracker test is bookkeeping stress, not business throughput or an SLO result. Fifty slots at
180 seconds imply a theoretical 24,000/day before overhead; actual capacity must be measured.

## Repeatable capacity and producer migration checks

`scripts/spec007 capacity` runs 20,000 and 100,000 requests through five in-process worker
runtimes and a real Kafka broker with ten fixed work partitions. Reports are written to
`vertx-pull-worker/target/capacity-evidence/`. The default handler is a synthetic 1 ms timer,
with 1,000 TPS admission per worker to exercise Kafka/EDR throughput. This measures the
compressed pipeline, not business dependency capacity. Override `spec007.capacity.handlerMs`,
`spec007.capacity.tps`, and `spec007.capacity.timeoutSeconds` as Maven system properties to
exercise measured dependency durations and quotas. `scripts/spec007 capacity 1000` is the
short harness check. CI runs full volumes on scheduled and manually dispatched builds.

`Operations translate LEGACY_JSONL_FILE` reads lines containing `{ "key": "subscriber:42",
"value": { ...request... } }`, validates the legacy key against owner metadata, and prints
`topic`, canonical entity `key`, and the unchanged request `value`. It does not publish or
translate offsets. Both subscriber and group routing have identity-preservation tests.
Use this output as reviewable producer migration evidence before enabling new routing.

`scripts/spec007 chaos` requires the local baseline and the repository Python virtualenv
(with `confluent-kafka`). It publishes 100 synthetic requests, SIGKILLs only
`spec007-subscriber-0`, restarts that slot, and checks that every result is visible through
`read_committed`. It waits up to six minutes to accommodate a live attempt lease. Evidence
is written to `target/capacity-evidence/container-kill.json`. This local container test
has no arbitrary external side effect and does not establish exactly-once business execution.
