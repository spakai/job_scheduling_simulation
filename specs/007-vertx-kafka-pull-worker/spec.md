# Vert.x Kafka Pull Worker Specification

Status: implemented locally; production acceptance gates remain open

Supersedes: the Python pull-worker runtime and concurrency model in Spec 006

Preserves: the direct-producer, database-free scheduler path and at-least-once foundation
of [`../006-kafka-pull-job-scheduler/spec.md`](../006-kafka-pull-job-scheduler/spec.md).
Revises: topic/key routing for subscriber/group isolation, concurrency, durable EDRs, retry
deployment, and adaptive pod-level admission. These require an explicit producer migration.

Architecture: [`arc42.md`](arc42.md)

Implementation plan: [`plan.md`](plan.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Required Runtime Architecture](#3-required-runtime-architecture)
- [4. Vert.x Execution Model](#4-vertx-execution-model)
- [5. Kafka Consumption and Partition Lanes](#5-kafka-consumption-and-partition-lanes)
- [6. Manual Offset and Transaction Semantics](#6-manual-offset-and-transaction-semantics)
- [7. Backpressure and Rate Limiting](#7-backpressure-and-rate-limiting)
- [8. Duplicate Handling](#8-duplicate-handling)
- [9. Rebalance and Shutdown](#9-rebalance-and-shutdown)
- [10. Configuration](#10-configuration)
- [11. Deployment and Docker](#11-deployment-and-docker)
- [12. Observability](#12-observability)
- [13. Testing and Chaos](#13-testing-and-chaos)
- [14. Migration](#14-migration)
- [15. Acceptance Criteria](#15-acceptance-criteria)
- [16. Out of Scope](#16-out-of-scope)

## 1. Purpose

Replace the Spec 006 Python pull worker with a Java 21, Vert.x 5 microservice while
preserving its Kafka contracts and delivery guarantees. The new worker processes bounded
concurrent records within each assigned Kafka partition as well as across partitions. Keep partition counts low: handler concurrency is configured independently of
partition count. Preserve same-owner serialization, but allow different owners in the same
partition to complete out of order. A per-partition contiguous-completion tracker controls
manual offset advancement.

The service remains a pull worker, not a scheduler database. Clients continue to publish
immediately eligible requests directly to `subscriber-rerate` keyed by `subscriberId`, or
`group-rerate` keyed by `groupId`. Canonical `ownerId` remains envelope/audit metadata;
the new wire key is the canonical identifier for the selected topic.

Spec 007 also closes the completed-request duplicate gap deferred by Spec 006. It suppresses
re-execution of a previously completed logical `(jobId, attempt)` inside the configured
deduplication retention window. It does not claim exactly-once execution across a crash
that occurs during an external side effect.

## 2. Scope

This specification covers:

- a Dockerized Vert.x pull-worker microservice;
- Java 21 and a supported, pinned Vert.x 5 release managed through the Vert.x BOM;
- one Vert.x Kafka consumer control plane per pod;
- one concurrent asynchronous coordinator (partition lane) per assigned partition;
- concurrency within and across partitions, bounded by partition and pod capacity;
- per-owner FIFO gates and a contiguous-completion tracker per partition;
- non-blocking handlers and isolated execution for unavoidable blocking handlers;
- explicit offset maps and transactional Kafka output/offset completion;
- partition pause/resume backpressure and bounded local queues;
- a pod-wide TPS token bucket shared by all partition lanes;
- Kafka-backed completed-request deduplication with bounded retention;
- rebalance fencing, bounded drain, health, metrics, tracing, and chaos tests; and
- an explicit producer/topic migration from the Python reference worker;
- isolated subscriber/group fleets and a separately operated retry deployment; and
- durable attempt EDRs, bounded internal retries, and adaptive dependency admission.

The Python simulator, visibility APIs, and Spec 006 worker remain test/reference assets
during migration. They are not the target production pull runtime after Spec 007 cutover.

## 3. Required Runtime Architecture

```text
 Client producers -- entity ID key --> subscriber-rerate / group-rerate
                                           |
                                  consumer group assignment
                                           |
                                  Vert.x worker pod
                         +-----------------+-----------------+
                         |                 |                 |
                  partition 0 lane  partition 2 lane  partition 5 lane
                    bounded overlap   bounded overlap   bounded overlap
                         +-----------------+-----------------+
                                           |
                                  pod TPS + capacity gate
                                           |
                                 asynchronous handlers
                                           |
                              serialized transaction adapter
                                           |
                    result + lifecycle/retry/DLQ + source offset
```

Each pod owns exactly one Kafka consumer instance for the worker group. It may own several
partitions. Each assigned partition has one lane coordinating multiple active jobs. With `P` assigned
partitions, concurrency is at most `min(maxInFlightPerPod, P * maxInFlightPerPartition)`,
also limited by runnable distinct owners, executor capacity, and TPS. For example, one
partition with eight distinct owners can run eight handlers when both limits permit it.

All replicas within a workload fleet use the same `group.id`. Subscriber and group fleets
use different topics and groups. Pods beyond that fleet's work-topic partition count are idle.
No scheduler database, due-job query, database claim, or scheduler outbox is introduced.

### 3.1 Workload isolation and initial sizing

| Workload | Work topic / key | Deployment |
| --- | --- | --- |
| Subscriber rerating | `subscriber-rerate` / canonical `subscriberId` | Initially 10 partitions, 5 pods, 10 active handler slots per pod. |
| Slow group rerating | `group-rerate` / canonical `groupId` | Separate consumer group, pods, worker pool, TPS/circuit settings, and capacity budget. |
| Retry processing | Workload-specific retry topic / original entity ID | Separate consumer group/deployment and limiter; normally may be scaled to zero. |

Deploy exactly one dedicated work-consumer verticle per pod; use `subscribe`, letting the
consumer group assign partitions (approximately two per subscriber pod). Do not manually
pin pods or create consumers/consumer verticles per partition. Partition lanes are local
coordinators, not extra consumers. Dedicated ledger restoration readers are separate from
work consumption. A second consumer group on the same work topic would receive all its
records and does not isolate subscriber work from group work.

Start subscriber `maxInFlightPerPod=10` and `maxInFlightPerPartition=10`, sharing the ten
pod slots fairly across assignments so skew can use spare slots. Jobs take approximately
60–180 seconds; no such call may occupy the consumer or event-loop thread.

At 20,000 jobs/day the mean arrival rate is 20,000 / 86,400 ≈ 0.2315 jobs/s. With a
180-second mean service time, ideal required concurrency is `ceil(jobs * duration / window)`:

| Completion window | Ideal minimum slots |
| --- | ---: |
| 24 hours | 42 |
| 12 hours | 84 |
| 8 hours | 125 |

Fifty slots at 180 seconds yield approximately 24,000 jobs/day, 20% capacity above demand
before downtime, retry, EDR/commit overhead, skew, and owner-gate waiting. This is a starting
assumption, not an SLO proof. Validate the fixed 10-partition/5-pod baseline under measured
job durations and bursts; separately budget group and retry capacity.

## 4. Vert.x Execution Model

### 4.1 Event-loop ownership

The Kafka consumer controller, lane state machines, token bucket, backpressure controller,
and health state run on Vert.x contexts. Mutable lane state must be context-confined; code
must not add locks as a substitute for preserving context ownership.

No handler may block an event-loop thread. Preferred business integrations use Vert.x
non-blocking clients and return `Future<Outcome>`. Unavoidable blocking work runs through a
named, bounded `WorkerExecutor` with `ordered=false`; explicit owner gates supply
same-owner ordering. Using `ordered=true` on one shared Vert.x context is prohibited because it can
accidentally serialize unrelated partitions across the whole pod.

Blocking calls longer than the approved worker-executor budget require a dedicated bounded
executor or an explicitly reviewed virtual-thread adapter. They must still obey the lane's
deadline, cancellation, and capacity contract.

### 4.2 Partition lane state

Each lane is `RESTORING`, `ACTIVE`, `PAUSED`, or `REVOKED`. These are assignment/dispatch
states, separate from the states of its many tracked records:

```text
QUEUED -> START_PERSISTING -> RUNNING -> EDR_PERSISTING -> COMPLETED -> COMMITTING -> COMMITTED
```

`COMPLETED` means required durable outcome EDRs are confirmed and immutable output commands
are ready (or a retry/DLQ disposition is staged for the prefix transaction). It does not
mean the source offset is committed. `JOB_COMPLETED` can be durable behind a source gap.
A deduplicated record can move directly from `QUEUED` to `COMPLETED`.

Each lane owns an ordered deque of every delivered, uncommitted record, completion status,
output commands, owner gates, committed next offset, and assignment epoch. Register records
in delivery order before dispatch. Completion callbacks run on the lane context and must
match both epoch and execution token. A transaction snapshots a bounded completed prefix;
later completions cannot mutate that snapshot. Only confirmed commit success retires its
entries and releases their owner gates. Confirmed EDR writes update the restored EDR
view independently of the source watermark.

A record may start while earlier records for other owners are running or awaiting commit.
For each owner, start only the earliest queued record, and hold its gate until its safe
Kafka boundary commits. This retains existing same-owner ordering and suppresses concurrent
in-flight duplicates. Scan runnable owners fairly so a queued hot owner does not prevent
other owners from using available slots. Global partition execution order is not promised.

### 4.3 Bounded intake

Fetched records are routed to bounded per-partition queues. Queue capacity must account for
Vert.x Kafka client buffering and `max.poll.records`; pausing a partition is not assumed to
erase records already delivered to the application. A record that arrives after a pause
request is accepted only if its bounded queue has capacity.

High and low watermarks control per-partition pause and resume. The service must continue
the consumer activity required for heartbeats while Kafka partitions are paused.

## 5. Kafka Consumption and Partition Lanes

The consumer must use:

- `enable.auto.commit=false`;
- `isolation.level=read_committed`;
- one stable production `group.id` for the logical fleet;
- cooperative rebalancing where supported and verified;
- bounded `max.poll.records`, fetch sizes, and local queues;
- assignment and revocation handlers; and
- explicit per-partition pause/resume.

The consumer must not call the no-argument `commit()` operation for processed work. That
operation can commit positions beyond records that have only been fetched or queued. Any
non-transactional diagnostic implementation must call the explicit offset-map overload
with the next safe offset for each named partition.

The record handler routes a record by `(topic, partition)` to its lane. Lane execution is:

1. register the delivered record in the ordered tracker under the current assignment epoch;
2. validate key/envelope and check the restored completed-request ledger;
3. select an eligible owner in FIFO order, acquire bounded capacity, and persist
   `ATTEMPT_STARTED` before acquiring a TPS start permit and invoking the handler;
4. classify the outcome; durably persist `JOB_COMPLETED` on success before marking it
   completed, and retain immutable result/lifecycle/retry/DLQ commands;
5. mark that record completed, releasing handler capacity but retaining tracker capacity;
6. collect only the completed prefix from the oldest uncommitted delivered record;
7. submit its outputs and exact safe next offset to the serialized transaction adapter; and
8. retire that prefix and release owner gates only after confirmed transaction success.

Validation failures and deduplicated records also pass through the tracker; neither may
commit past unfinished work. Recheck deduplication when an owner gate becomes available.
Same-owner records remain serial; different owners sharing their partition can overlap.

## 6. Manual Offset and Transaction Semantics

### 6.1 Offset meaning

Kafka stores one committed **next offset** for each `(group.id, topic, partition)`. It does
not store an individual acknowledgement for every record. Completing source offset `41`
permits committing `42`, which means all offsets below `42` in that partition are safe.

Different partition offsets are independent. One transaction may contain:

```text
partition 0 -> next offset 14
partition 2 -> next offset 87
partition 5 -> next offset 32
```

No value may move past an unfinished delivered record in its partition. Walk the tracker
from its oldest uncommitted entry and stop at the first entry not completed. Propose one
past the final offset in that completed prefix; never use the maximum completed offset.
Kafka offsets can contain gaps (for example aborted transactions or control records), so
contiguity means delivery order, not requiring every integer offset to exist. Register the
entire delivered batch before accepting its completion callbacks; never infer a missing
record is safe merely because a later handler finished.

Example for partition 3, with committed next offset initially 100:

| Source offset | State | Effect on commit |
| --- | --- | --- |
| 100, 101 | Completed | Commit their outputs and next offset 102. |
| 102 | Running | Blocks further advancement. |
| 103, 104 | Completed | Retain outcomes; cannot advance past 102. |
| 105 | Queued | Remains uncommitted and must still execute. |

When 102 completes, commit outputs for 102–104 and next offset **105** atomically.
The committed offset is the recovery position; committing does not seek or reset the
current consumer fetch position, which may already be beyond 105. Uncommitted records are
not normally polled repeatedly by that live consumer: replay follows restart, rebalance,
explicit seek, or loss of its uncommitted state. Committing records progress, not deletion
of individual jobs; Kafka removes records according to topic retention. Early commit before
processing is prohibited because this design has no separate durable claim for lost work.

### 6.2 Transaction adapter

The high-level Vert.x Kafka producer supports transaction lifecycle operations but does not
currently expose the complete consumed-offset transaction operation required by this
design. The worker therefore uses an isolated adapter over the underlying Apache Kafka
producer for the complete sequence:

1. `beginTransaction`;
2. send all required result/lifecycle/retry/DLQ and deduplication records;
3. `sendOffsetsToTransaction` with exact next offsets and current consumer-group metadata;
4. `commitTransaction`; or
5. `abortTransaction` on any failure.

The adapter runs on one named, dedicated single-thread executor per pod. Transaction calls
must never run on an event loop. Commands may arrive concurrently from partition lanes,
but transactions never overlap. The adapter validates the assignment epoch immediately
before beginning and immediately before committing.

The same serialized adapter also accepts short EDR-only transactions, without source
offsets, before handler invocation and after business success. It never holds a transaction
open during a 1–3 minute operation. An EDR-only transaction must not advance any source
offset. Resolve ambiguous EDR writes and restore authoritative state before retrying work.

The initial implementation batches a bounded contiguous completed prefix from one partition
per transaction, including every required output and ledger update for that prefix. A
prefix may be split to meet record/byte/time limits; commit only through the included
entries. Do not open a transaction while waiting for a handler or a gap to finish. Only one
prefix per partition may be outstanding in the adapter; no duplicate or regressing offset
proposals are allowed. Cross-partition batching is deferred and requires safe prefixes and
current group metadata for every included partition.

### 6.3 Safe boundaries

Success first durably commits `JOB_COMPLETED` and its recoverable outcome to the EDR
authority. Later, its contiguous-prefix transaction commits result, required lifecycle,
source-finalization state (separate source-coordinate keys), and source next offset atomically in Kafka. Replayed
completed work reconstructs pending outputs from the EDR without rerunning the handler. Retry commits the retry request, required lifecycle, and source next
offset atomically. Terminal failure commits result, lifecycle, DLQ, completed-dedup record,
and source next offset atomically.

Fetch, validation start, queue insertion, rate-limit waiting, handler start, and external
side-effect completion are never offset commit points. A failed or ambiguous transaction
does not advance the local confirmed watermark and pauses dispatch. A commit timeout may
have committed at the broker: resolve/fence the producer and recover the broker-committed
offset and ledger before replay. Invalidate the old execution epoch/tokens, cancel or drain
old work, clear its buffered state, and replay from that recovered position. Never seek
back while accepting completions from the previous execution. Completed outcomes behind a
gap with confirmed `JOB_COMPLETED` are recoverable and skip business execution on replay.
Without that durable completion, the external side effect remains ambiguous and may repeat.

## 7. Backpressure and Rate Limiting

All partition lanes in a pod share one monotonic token bucket. `rateLimitTps` limits
handler starts per second and `rateLimitBurst` limits accumulated burst. The approximate
fleet ceiling remains `active pods * rateLimitTps`; it is not a global exact rate.

Backpressure has two scopes:

- **partition scope** for a full tracking window, a stalled earliest record, or exhausted
  partition capacity;
- **pod scope** for shared Kafka output, transaction coordinator, or business dependency
  failure.

The controller uses `RUNNING`, `PAUSED`, and `PROBING`. Required-output failure immediately
stops new handler starts, retains affected offsets, and uses bounded jittered recovery
probes. Readiness becomes false after the sustained-pause threshold; liveness remains true
while the Vert.x event loop, consumer membership, and recovery controller are responsive.

Bound both queued records and the entire uncommitted tracking window (queued, running,
completed behind gaps, and committing), in records and bytes, per partition and pod.
Completed outcomes still consume this window after releasing handler capacity. At high
watermark pause fetching and stop admitting new work; already admitted work must retain
capacity to finish and close the earliest gap. Resume below the low watermark. Reserve
headroom for client buffering and an in-progress poll; never silently drop overflow records
and subsequently commit past them. Overflow triggers fenced recovery from committed state.
Bound transaction batch bytes and output size too. A stuck earliest record reaches its
handler deadline and retry/DLQ policy; timeout alone is never successful completion.
A timed-out call that cannot be cancelled retains its execution permit until it exits, or
requires worker replacement; do not free capacity and accumulate unbounded zombie calls.
External fencing/idempotency is still required across ambiguous calls and ownership changes.

TPS waiting must not occupy a worker thread. It uses Vert.x timers and resumes the lane
when a permit and capacity are available. Zero TPS stops ordinary starts for either an
administrative pause or an open dependency circuit.

### 7.1 Adaptive dependency admission

Rate limits govern starts; concurrency limits independently govern active 1–3 minute calls.
Use healthy/degraded/severe/open target rates of **2 / 1 / 0.5 / 0 TPS per pod** initially.
Configurable timeout, connection-failure, HTTP 429/503, error-rate windows, and minimum sample
counts drive degradation. Define cooldowns, hysteresis, and sustained-success windows;
recover in steps rather than jumping to 2 TPS. An open circuit allows only explicitly
budgeted, bounded half-open probes. Administrative zero TPS allows no business probes.
The 0.5 TPS value is a degraded floor, never an outage floor. Recovery probes that invoke
business work also acquire concurrency and a dedicated probe-rate permit.

Retain the asynchronous monotonic token bucket; Guava is optional, not a required dependency.
If used, `tryAcquire`/timer scheduling must replace blocking permit waits on Vert.x contexts.
A rate limiter replaces neither concurrency admission nor the circuit breaker. Five pods
at 2 TPS permit up to 10 starts/s, although long-running capacity may bind first. If the
dependency's strict fleet quota is 2 TPS, configure 0.4 TPS per pod for five active pods,
including retry/group allocations and burst limits, or supply a shared quota service.
Recalculate allocations on scaling; this local limiter does not enforce a strict global cap.

### 7.2 Internal retry and quarantine

Use configurable bounded internal retries (initially two retries after the first call),
with jittered timer backoff and a total elapsed-time budget. Every invocation, including a
retry, reacquires both concurrency and TPS admission. Release execution capacity only when
the previous call has actually stopped; retain the owner gate and source tracker entry
through backoff. Persist a distinct attempt EDR for each invocation. Validate its lease/token again at
actual dispatch after TPS waiting; renew or recover an expired lease before invoking work. Neither retry scheduling
nor an exception makes the source record safe to commit.

After exhaustion, stage the failed request and failure metadata for a workload-specific
retry/DLQ topic. The required prefix transaction publishes that handoff and lifecycle/ledger
state with the safe source offset; commit confirmation is the durable handoff acknowledgement.
Publish failure leaves the original unresolved. Kafka transaction atomicity is required in
this production design; the notes' non-transactional fallback is not selected. Give handoffs
a stable identity from workload, source coordinates, and retry generation for deduplication.

A retry consumer is separately deployed and may be started by alarm/runbook automation.
It validates quarantine metadata and republishes to the original workload topic with the
original entity key, atomically with its retry-topic offset, under separate admission limits.
Business execution remains in the workload fleet so parallel retry workers do not bypass
owner gates or compete as another execution authority. Requeue ordering is arrival order:
a retried job may follow newer jobs for the same owner. Strict original order across a
quarantine boundary requires leaving that owner blocked and is not guaranteed here.
Retry generation and total retry age/count are bounded; terminal DLQ never auto-replays.

## 8. Duplicate Handling

### 8.1 Guarantee

Spec 007 suppresses a conforming duplicate whose logical `(jobId, attempt)` already reached
a completed Kafka boundary and remains within `dedupRetention`. It returns or republishes
the recorded logical outcome without invoking the business handler again.

The guarantee does not cover a crash after an external side effect begins but before the
`JOB_COMPLETED` EDR transaction commits. The redelivery cannot know whether that external call occurred.
Every business dependency must therefore accept stable `jobId` as an idempotency key across
all internal retries, redeliveries and quarantine generations, or
explicitly document possible duplicate effects.

### 8.2 Durable Kafka EDR authority

Use workload-scoped append-only EDR topics plus compacted execution-state topics (for example
`subscriber-rerate-edr.v1` / `subscriber-rerate-execution-ledger.v1`, with separate group
variants). The compacted state is a transactional materialization of the durable EDR, not
an independent cache authority. Each pair mirrors its work topic's partition count. Write
both the EDR event and corresponding state in the same short Kafka transaction, explicitly
to the source partition number, keyed by the logical job identity. No scheduler DB is added.

Before every business invocation, confirm durable
`ATTEMPT_STARTED(jobId, attemptId, timestamp, leaseUntil, executionToken)`. After business
success, confirm durable `JOB_COMPLETED(jobId, attemptId, timestamp, outcome)` **before**
source-offset advancement. `attemptId` identifies a physical invocation; the existing
`attempt` is a logical retry generation. Redelivery/internal retries retain the logical
idempotency key `jobId` and use a new `attemptId`. Preserve successful `jobId`
completion across retry generations so an accidental later retry cannot rerate it again;
an intentional fresh rerate requires a new `jobId`. Store immutable request hash, owner,
workload, source coordinates, recoverable outcome, and expiry. A later started/failure event
must never overwrite a known successful completion.

On assignment, fence prior writers and restore EDR state to a captured committed end before
dispatch. The implementation must prove fencing of EDR-only writes across assignment changes
(for example a workload/partition-scoped transactional writer identity initialized by its
new owner); local epoch checks alone do not fence a former pod's already-sent transaction.
Keep native producers behind the serialized adapter and initialize/fence partition writers
before capturing restore targets. Use separate IDs from source-prefix producers. Source-prefix finalization records use
separate source-coordinate keys and never overwrite job EDR state; only the fenced
partition EDR writer may mutate that state. Former owners must close fenced producers,
never reinitialize them without a fresh verified assignment.

On redelivery, a matching durable `JOB_COMPLETED` suppresses business execution and supplies
pending output commands to the contiguous tracker. `ATTEMPT_STARTED` alone is not success:
wait/reconcile an unexpired lease; a stale lease triggers bounded reconciliation or retry
with the original stable job idempotency key. Lease expiry is not proof an external call stopped.
Downstream idempotency/fencing is necessary for ambiguous effects and ownership transfer.
Owner gates prevent simultaneous in-assignment invocations. Immutable identity conflicts
are quarantined; cross-owner/global identity policing remains outside this local guarantee.

A one-hour memory/distributed cache is optional. Cache loss or expiry falls back to the
restored durable EDR state and must not reopen execution. `dedupRetention` governs durable
state, must exceed the declared outage, replay, retry and operator window, and is independent
of cache TTL. Audit and compacted-state retention/tombstones must preserve that guarantee;
expiry means a later replay may execute again. Revocation clears old tracker/owner state only
after fencing callbacks and resolving transactions; new owners rebuild from Kafka.

## 9. Rebalance and Shutdown

Assignment creates a monotonically increasing local epoch and starts ledger restoration.
Revocation marks the lane `REVOKED` before any drain begins, pauses dispatch, and rejects
new completion commands for the old epoch.

After revocation, no new transaction for that partition may begin. Resolve or abort any
already-submitted transaction with broker group fencing; then recover authoritative offsets
and ledger state on reassignment. Drain/cancel handlers within a configured deadline;
uncommitted records, including completed records behind gaps, are redelivered. A late
external result from a revoked lane is observable but cannot publish Kafka outputs or
offsets through the stale epoch.

Graceful shutdown performs:

1. readiness false;
2. pause all assigned partitions;
3. stop new lane dispatch;
4. drain within the bounded deadline;
5. abort unfinished transactions and leave their offsets uncommitted;
6. close consumer, transaction adapter, worker executors, metrics server, and Vert.x; and
7. exit before Kubernetes termination grace expires.

## 10. Configuration

The worker validates at startup:

| Setting | Requirement |
| --- | --- |
| Java/Vert.x | Java 21; supported pinned Vert.x 5.x BOM. |
| Work/output/EDR/ledger topics | Workload-isolated, environment-scoped; EDR/state partitions mirror work. |
| `group.id` | Stable and allowlisted for the production worker fleet. |
| Source-prefix transactional ID | Unique per live worker slot; successor reuses it to fence a zombie. |
| EDR transactional ID | Stable per workload/source partition; new assigned owner fences old writer before restoration. |
| Auto commit | Exactly `false`. |
| Isolation | Exactly `read_committed`. |
| Partition assignment strategy | Supported cooperative strategy, integration-tested. |
| Queue high/low watermarks | Positive, bounded, and compatible with `max.poll.records`. |
| `maxInFlightPerPartition` | Positive; greater than one in the target concurrent deployment. |
| `maxInFlightPerPod` | Positive, independently limits total active handlers. |
| Tracking window records/bytes | Bounded per partition and pod, includes completed outcomes and client-buffer headroom. |
| Transaction batch records/bytes | Positive, bounded by window, producer limits, and transaction timeout. |
| Handler/worker capacity | Positive and bounded by pod resources. |
| TPS and burst | Fractional non-negative TPS; bounded burst; initial targets 2/1/0.5/0 per pod, adjusted to dependency quota. |
| Subscriber baseline | 10 partitions, 5 pods, 10 pod slots, 10 partition slots sharing pod capacity. |
| Internal retries | Initially two; bounded backoff and total duration; every invocation reacquires admission. |
| Adaptive thresholds | Explicit windows, sample counts, cooldown, hysteresis, step-up criteria, probe budget. |
| Attempt lease/cache | Bounded lease and recovery policy; optional 1-hour cache, independent durable retention. |
| Retry deployment | Isolated topic/group, bounded retry age/generation, normally may be at zero replicas. |
| Handler/transaction/drain timeouts | Positive and consistent with Kafka and Kubernetes timing. |
| Dedup retention | Greater than the declared replay and recovery window. |
| Ledger partitions | Equal to work-topic partitions. |

Invalid configuration fails before joining the consumer group. Secrets are mounted through
the platform secret mechanism and never emitted in configuration logs.

## 11. Deployment and Docker

The service is built with Maven using the Vert.x BOM and a reproducible dependency lock or
enforcer policy. The Docker image uses a multi-stage build and a minimal non-root Java 21
runtime. It contains no Python runtime and receives no scheduler-database credentials.

The image exposes only health/metrics HTTP and optional management endpoints. Read-only
root filesystem, dropped Linux capabilities, resource requests/limits, JVM container
awareness, and a writable bounded temporary/state volume are required.

Compose must support Kafka, topic/schema setup, a five-pod subscriber baseline profile,
an isolated group-worker profile, and a normally-zero retry deployment; smaller two-pod
profiles are permitted for smoke tests.
Kubernetes uses stable worker-slot identities for transactional IDs (for example, StatefulSet
ordinals), a disruption budget, readiness, liveness, startup, and sufficient termination
grace. A successor reuses its slot's transactional ID so broker initialization fences a
zombie predecessor; two live slots never share an ID. Autoscaling uses lag/oldest-age with
a maximum that respects the fleet TPS ceiling and partition count.

## 12. Observability

Metrics and structured logs include:

- event-loop delay and blocked-thread warnings;
- worker-executor active, queued, rejected, and duration measurements;
- assigned/restoring/ready/paused partitions and rebalance epochs;
- lane state, queued/running/completed-uncommitted counts and bytes, owner-gate wait,
  committed and candidate next offsets, earliest-gap age, and transaction wait;
- fetched, started, succeeded, retried, DLQ, deduplicated, and conflicted totals;
- per-pod TPS permits, effective rate and adaptive/circuit state;
- dependency timeout, connection failure, HTTP 429/503 rates and recovery probes;
- stale attempt leases and time from business completion to EDR durability to source commit;
- transaction begin/commit/abort/fence latency and failures;
- committed next offset, consumer lag, and oldest record age per partition;
- ledger restore position, target, duration, and readiness; and
- shutdown drain duration and abandoned in-flight work.

Traces propagate `jobId`, `correlationId`, `ownerId` hash, attempt, source topic/partition/
offset, assignment epoch, and transaction identity. Payload and credentials are redacted.

## 13. Testing and Chaos

Unit tests use JUnit 5 and deterministic clocks. Vert.x asynchronous tests use the Vert.x
JUnit 5 integration. Integration tests use real Kafka containers and `read_committed`
verification; mocks alone cannot prove transactions, group assignment, or offsets.

Required scenarios are:

| ID | Scenario | Required proof |
| --- | --- | --- |
| `VTX-LANE-01` | One pod owns three partitions | Handlers overlap within and across partitions under both capacity limits. |
| `VTX-LANE-02` | Different owners share one partition | Handlers overlap and finish out of order; same-owner records wait for the prior commit. |
| `VTX-OFF-01` | Four records fetched from one partition | Completion order never skips unfinished delivered records; fetch position is never committed. |
| `VTX-OFF-02` | 100/101 done, 102 running, 103/104 done, 105 queued | Commit 102, then 105 only after 102 completes; verify prefix outputs atomically. |
| `VTX-OFF-03` | Delivered offsets contain Kafka gaps | Tracker advances across absent broker offsets, never across unfinished delivered entries. |
| `VTX-OFF-04` | Crash with completed work behind a gap | Recovery starts at broker commit; durable EDR completions skip handlers; pre-EDR ambiguous effects use idempotent replay. |
| `VTX-POS-01` | Uncommitted record behind a gap in a live consumer | Normal polling advances without repeatedly delivering it; restart/seek replays it; commit does not delete the retained Kafka record. |
| `VTX-OFF-05` | Execution verticle fails on 102 after 100/101/103/104 finish | Only 100/101 commit (next offset 102); failed/unresolved 102 blocks 103/104 until safe resolution; recovery never skips 102. |
| `VTX-WINDOW-01` | Earliest record stalls while later handlers finish | Tracking bytes/count stay bounded; head can finish; heartbeat and other partitions remain responsive. |
| `VTX-TX-03` | Commit times out or prefix exceeds batch limits | Resolve broker state before replay; split batches commit only included prefixes. |
| `VTX-DEDUP-04` | Same identity arrives while original is running | Owner gate prevents overlap; post-commit ledger recheck suppresses duplicate. |
| `VTX-TX-01` | Result send succeeds, lifecycle send fails | Prefix transaction aborts; none of its outputs is visible, source offset is unchanged; previously durable EDR remains visible. |
| `VTX-TX-02` | Two partition lanes complete together | Dedicated adapter serializes transactions and commits independent exact offsets. |
| `VTX-BP-01` | Output Kafka is unavailable | Lanes pause, queues remain bounded, event loop/heartbeats remain responsive, then recover. |
| `VTX-BLOCK-01` | Blocking handler exceeds event-loop threshold | Work runs outside event loop; health and other partitions remain responsive. |
| `VTX-REB-01` | Revoke during external call | Old epoch cannot commit; record redelivers without offset loss. |
| `VTX-DEDUP-01` | Completed `(jobId, attempt)` redelivers | Handler is not called; recorded outcome is returned/published. |
| `VTX-DEDUP-02` | Pod dies and another restores durable EDR state | New lane becomes ready only after catch-up and suppresses the completed duplicate. |
| `VTX-DEDUP-03` | Same identity has different immutable data | Conflict is observable and reaches DLQ. |
| `VTX-TTL-01` | Ledger entry expires | Tombstone removes it; documentation states replay may execute again. |
| `VTX-EDR-01` | Start EDR cannot commit | No business invocation or source commit; recovery remains responsive. |
| `VTX-EDR-02` | Completion EDR commits behind gap, then pod dies | Replay skips completed business work and reconstructs outputs; offset never bypasses gap. |
| `VTX-EDR-03` | Started-only live/stale lease and late former writer | Reconcile live lease, safely recover stale attempt, and fence former EDR producer; completion cannot regress. |
| `VTX-RETRY-01` | Internal retries then quarantine | Each call has a new attempt EDR and reacquires TPS/capacity; failed handoff leaves offset unresolved. |
| `VTX-RETRY-02` | Retry deployment starts from zero | Bounded idempotent requeue to original topic/key; no direct parallel business execution. |
| `VTX-RATE-01` | Inject timeout/429/503, then recovery | 2→1→0.5→0 TPS; bounded half-open probes and gradual recovery; no event-loop permit waits. |
| `VTX-ISO-01` | Slow group workload saturates | Separate topic/group/pool/limiter protects subscriber capacity. |
| `VTX-CACHE-01` | One-hour cache expires or disappears | Durable EDR still suppresses completed work. |
| `VTX-POD-01` | One pod is killed during load | Assignments recover, queues stay bounded, and duplicates/lag are measured. |
| `VTX-CAP-01` | 20,000/day and compressed bursts | Per-pod TPS, owner serialization, lag, and recovery objectives hold. |
| `VTX-CAP-02` | 100,000/day and compressed bursts | Meet agreed throughput with low fixed partition counts by tuning within-partition concurrency and pod resources; measure limits before proposing expansion. |

Every test asserts zero event-loop blocking, zero skipped source offsets, zero same-owner
overlap within the active assignment, bounded queues, and no prefix-output/source-offset
partial commit. EDR-before-offset durability is intentional; cross-assignment external
ambiguity is tested with downstream idempotency/fencing.

### Failed execution verticle at offset 102 (`VTX-OFF-05`)

Implement a deterministic tracker unit test and a real-Kafka integration/chaos test with
execution barriers, rather than timing-dependent sleeps:

1. Assign one partition to one consumer pod. Deliver offsets 100–105 for distinct owners,
   enable at least five concurrent handler slots, and keep 105 queued using a test barrier.
   Start with broker-committed next offset 100. Execution verticles/adapters return outcomes
   to the partition coordinator; they never commit consumer offsets themselves.
2. Complete 100 and 101 and await their confirmed prefix transaction. Independently query
   the consumer group's committed offset and assert **102**, meaning records through **101**
   are committed.
3. Let the handlers for 103 and 104 finish while holding 102 at a barrier. Assert their
   outcomes are retained as `COMPLETED`, with durable `JOB_COMPLETED` EDR/state visible but no prefix result/lifecycle output visible to
   `read_committed` consumers yet, and committed next offset still 102.
4. Inject failure into the execution handling 102. Cover both a failed handler Future and
   execution verticle undeployment/lost completion. Hold retry/DLQ resolution at a test
   barrier so the source record remains unresolved. Assert no success acknowledgement,
   owner-gate release, or offset advancement results merely from failure, undeployment,
   timeout, or completion of 103/104. A failed Future alone does not imply the verticle was
   undeployed; record the actual lifecycle event separately from the failed job outcome.
5. Verify the partition coordinator and consumer stay responsive, the missing completion
   is detected by supervision/deadline, and retained outcomes remain within window limits.
   The failure must not silently strand 102 forever: classify it for the configured recovery
   policy, but leave it uncommitted while that disposition is unresolved. Confirm the failed
   execution's token is invalidated and late success callbacks cannot mark it completed.
6. Exercise two independent recovery branches from the unresolved state:
   - Release the recovery barrier and safely resolve 102 through success or its configured
     retry/DLQ disposition. Commit all required outputs for the included prefix atomically;
     the watermark may reach **105** only when 102–104 are covered by confirmed transactions.
     A retry/DLQ outcome counts as resolved only at that durable boundary, not at the throw.
   - Kill/restart the pod before resolution. Verify reassignment starts from committed next
     offset **102**, replays 102/103/104, and restores the ledger before dispatch. The prior
     durable EDR outcomes for 103/104 suppress their handlers and reconstruct pending outputs.
     Also test a crash before EDR durability, where downstream idempotency handles replay. Records 100/101 must not replay in normal recovery.

Capture handler start/completion and verticle lifecycle events, execution tokens, tracker
states, broker-committed offsets, transaction outcomes, and `read_committed` output records.
The test fails if any commit exceeds 102 while source record 102 remains unresolved.

## 14. Migration

Migration changes the runtime and explicitly versions the producer topic/key contract:

1. freeze compatible envelope fields; define subscriber/group topic routing and canonical raw
   identifier key serialization, with fixtures mapping legacy `ownerId` keys;
2. provision isolated work/retry/DLQ/EDR/state topics and inactive Vert.x fleets;
3. replay representative production-like traffic into an isolated group and compare
   results with the Python reference worker;
4. prove assignment, offset, transaction, backpressure, dedup, and load scenarios;
5. stop Python worker dispatch and let its transactions finish or abort;
6. verify no Python group member remains and capture committed offsets;
7. after draining the legacy topic, switch producers to isolated new topics and start their
   workload-specific consumer groups at controlled TPS; old topic offsets do not map to new topics;
8. restore ledger partitions before readiness;
9. observe lag, outcomes, duplicates, and partition ownership through the rollback window;
10. retain the Python image only as a rollback artifact, not a concurrent consumer; and
11. remove the Python pull-worker deployment after acceptance.

Rollback first stops producer routing and Vert.x dispatch, drains or explicitly migrates new-topic
backlogs with identity-preserving evidence, and then restores legacy producer routing/Python.
Never translate offsets numerically between topics or leave the new backlog orphaned. Mixed
old/new routing for one owner is forbidden during cutover; new runtime and legacy runtime must
not perform overlapping business work. Retain EDR completion checks in any rollback bridge
to avoid reopening completed jobs. Quiesce and prove this migration in an isolated rehearsal.

## 15. Acceptance Criteria

Spec 007 is complete when:

1. the production pull worker runs as a Dockerized Java 21/Vert.x 5 service;
2. producer migration to isolated subscriber/group topics and canonical entity keys is proven;
3. no scheduler database or Python runtime is present in the worker image;
4. different owners execute concurrently within an assigned partition and across partitions,
   under explicit pod/partition limits, while same-owner records remain serial;
5. no blocking handler or Kafka transaction operation runs on an event loop;
6. the no-argument consumer commit is not used for processed work;
7. bounded contiguous completed prefixes and all their required outputs commit atomically,
   never advancing past an unfinished delivered record;
8. required-publication failure pauses intake without losing group responsiveness;
9. one pod-wide TPS limiter governs all partition lanes;
10. current assignment epochs fence late completions after rebalance;
11. durable attempt/completion EDRs govern recovery; cache expiry does not defeat deduplication;
12. the remaining external-side-effect duplicate window is explicit and tested;
13. health, metrics, logs, and traces expose lane, Kafka, executor, and dedup state;
14. the full real-Kafka, EDR, isolation, retry, adaptive-rate, chaos, 20,000/day baseline
    and 100,000/day scale suites pass; and
15. migration and rollback never run Python and Vert.x production consumers concurrently.

## 16. Out of Scope

- Future/delayed job scheduling and delayed retry.
- Exactly-once execution of an arbitrary external business side effect.
- A globally exact TPS limit across pods.
- Concurrent execution of records for the same owner within an assignment.
- Automatic Kafka partition-count changes without an ordering migration.
- Rewriting the Python simulator or visibility projection in Java.
- Cross-owner global enforcement of a reused `jobId` without an ingestion identity service.
- Automatic DLQ replay.

## References

- [Vert.x Kafka Client documentation](https://vertx.io/docs/vertx-kafka-client/java/)
- [Vert.x KafkaConsumer API](https://vertx.io/docs/apidocs/io/vertx/kafka/client/consumer/KafkaConsumer.html)
- [Vert.x KafkaProducer API](https://vertx.io/docs/apidocs/io/vertx/kafka/client/producer/KafkaProducer.html)
- [Vert.x Kafka client issue for consumed offsets in transactions](https://github.com/vert-x3/vertx-kafka-client/issues/269)
- [Vert.x Core threading model](https://vertx.io/docs/vertx-core/java/)
- [Vert.x 5 migration guide](https://vertx.io/docs/guides/vertx-5-migration-guide/)

- [Apache Kafka consumer offset and position semantics](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
