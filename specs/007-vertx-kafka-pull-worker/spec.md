# Vert.x Kafka Pull Worker Specification

Status: proposed

Supersedes: the Python pull-worker runtime and concurrency model in Spec 006

Preserves: the Kafka topics, direct-producer contract, `ownerId` partitioning, database-free
scheduler path, per-pod TPS limit, backpressure, retry, DLQ, and at-least-once semantics
defined by [`../006-kafka-pull-job-scheduler/spec.md`](../006-kafka-pull-job-scheduler/spec.md)

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
preserving its Kafka contracts and delivery guarantees. The new worker processes one
record at a time in each Kafka partition and processes different assigned partitions
concurrently. This is the default design because it preserves Kafka ordering and makes
manual offset advancement unambiguous without restricting a pod to one job globally.

The service remains a pull worker, not a scheduler database. Clients continue to publish
immediately eligible requests directly to `job-requests.v1`, keyed by canonical `ownerId`.

Spec 007 also closes the completed-request duplicate gap deferred by Spec 006. It suppresses
re-execution of a previously completed logical `(jobId, attempt)` inside the configured
deduplication retention window. It does not claim exactly-once execution across a crash
that occurs during an external side effect.

## 2. Scope

This specification covers:

- a Dockerized Vert.x pull-worker microservice;
- Java 21 and a supported, pinned Vert.x 5 release managed through the Vert.x BOM;
- one Vert.x Kafka consumer control plane per pod;
- one serial asynchronous execution lane per assigned Kafka partition;
- concurrency across partitions, bounded by assigned partitions and pod capacity;
- non-blocking handlers and isolated execution for unavoidable blocking handlers;
- explicit offset maps and transactional Kafka output/offset completion;
- partition pause/resume backpressure and bounded local queues;
- a pod-wide TPS token bucket shared by all partition lanes;
- Kafka-backed completed-request deduplication with bounded retention;
- rebalance fencing, bounded drain, health, metrics, tracing, and chaos tests; and
- migration from the Python reference worker without changing client topic contracts.

The Python simulator, visibility APIs, and Spec 006 worker remain test/reference assets
during migration. They are not the target production pull runtime after Spec 007 cutover.

## 3. Required Runtime Architecture

```text
 Client producers -- ownerId key --> Kafka job-requests.v1
                                           |
                                  consumer group assignment
                                           |
                                  Vert.x worker pod
                         +-----------------+-----------------+
                         |                 |                 |
                  partition 0 lane  partition 2 lane  partition 5 lane
                    one at a time     one at a time     one at a time
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
partitions. Each assigned partition has one lane, and each lane has at most one active job.
A pod owning three partitions may therefore execute up to three jobs concurrently, subject
to its worker-pool, queue, and TPS limits.

All replicas use the same `group.id`. Pods beyond the work-topic partition count are idle.
No scheduler database, due-job query, database claim, or scheduler outbox is introduced.

## 4. Vert.x Execution Model

### 4.1 Event-loop ownership

The Kafka consumer controller, lane state machines, token bucket, backpressure controller,
and health state run on Vert.x contexts. Mutable lane state must be context-confined; code
must not add locks as a substitute for preserving context ownership.

No handler may block an event-loop thread. Preferred business integrations use Vert.x
non-blocking clients and return `Future<Outcome>`. Unavoidable blocking work runs through a
named, bounded `WorkerExecutor` with `ordered=false`; the partition lane already supplies
ordering. Using `ordered=true` on one shared Vert.x context is prohibited because it can
accidentally serialize unrelated partitions across the whole pod.

Blocking calls longer than the approved worker-executor budget require a dedicated bounded
executor or an explicitly reviewed virtual-thread adapter. They must still obey the lane's
deadline, cancellation, and capacity contract.

### 4.2 Partition lane state

Each lane has exactly one of these states:

```text
RESTORING -> READY -> RUNNING -> COMMITTING -> READY
               |         |          |
               +-------> PAUSED <----+
                          |
                       REVOKED
```

- `RESTORING`: assignment exists but deduplication state is not caught up; no job starts.
- `READY`: the next record may acquire capacity and a TPS token.
- `RUNNING`: one handler is active for the partition.
- `COMMITTING`: required Kafka outputs and the source next offset are being committed.
- `PAUSED`: dispatch is disabled because of capacity, TPS, dependency, or Kafka failure.
- `REVOKED`: no new operation or Kafka completion is accepted for the old assignment epoch.

The lane does not start offset `N+1` until offset `N` reaches a committed safe boundary or
is deliberately left uncommitted and the lane is rewound/paused. A contiguous-offset
tracker is therefore unnecessary in the default design.

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

1. verify the lane still owns the current assignment epoch;
2. validate the record key and envelope;
3. check the completed-request deduplication store;
4. acquire pod capacity and a TPS permit;
5. invoke the non-blocking handler or bounded blocking adapter;
6. classify success, retryable failure, or terminal failure;
7. create immutable result/lifecycle/retry/DLQ output commands;
8. submit one completion command to the serialized transaction adapter; and
9. release the lane only after transaction success, or pause/rewind after failure.

Because every conforming job for an owner uses the same canonical `ownerId` Kafka key, all
jobs for that owner use one partition. Serial processing of that partition is therefore a
stronger invariant than a separate in-memory owner lock. An owner guard remains as a
defensive metric/assertion during migration and rebalance testing.

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

No value may move past an unfinished record in its partition. The serial lane makes the
current record the only candidate for advancement.

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

One record per transaction is the initial implementation. Batching completed records from
different partitions is a later optimization and is allowed only when every entry belongs
to the same current group generation and each proposed offset is safe.

### 6.3 Safe boundaries

Success commits result, required lifecycle, completed-dedup record, and source next offset
atomically in Kafka. Retry commits the retry request, required lifecycle, and source next
offset atomically. Terminal failure commits result, lifecycle, DLQ, completed-dedup record,
and source next offset atomically.

Fetch, validation start, queue insertion, rate-limit waiting, handler start, and external
side-effect completion are never offset commit points. A failed or ambiguous transaction
leaves the source record uncommitted, pauses its lane, and causes replay from the last
committed offset after recovery.

## 7. Backpressure and Rate Limiting

All partition lanes in a pod share one monotonic token bucket. `rateLimitTps` limits
handler starts per second and `rateLimitBurst` limits accumulated burst. The approximate
fleet ceiling remains `active pods * rateLimitTps`; it is not a global exact rate.

Backpressure has two scopes:

- **partition scope** for a full lane queue, one poison record, or one lane transaction;
- **pod scope** for shared Kafka output, transaction coordinator, or business dependency
  failure.

The controller uses `RUNNING`, `PAUSED`, and `PROBING`. Required-output failure immediately
stops new handler starts, retains affected offsets, and uses bounded jittered recovery
probes. Readiness becomes false after the sustained-pause threshold; liveness remains true
while the Vert.x event loop, consumer membership, and recovery controller are responsive.

TPS waiting must not occupy a worker thread. It uses Vert.x timers and resumes the lane
when a permit and capacity are available. Zero TPS is an administrative pause.

## 8. Duplicate Handling

### 8.1 Guarantee

Spec 007 suppresses a conforming duplicate whose logical `(jobId, attempt)` already reached
a completed Kafka boundary and remains within `dedupRetention`. It returns or republishes
the recorded logical outcome without invoking the business handler again.

The guarantee does not cover a crash after an external side effect begins but before the
Kafka transaction commits. The redelivery cannot know whether that external call occurred.
Every business dependency must therefore accept `(jobId, attempt)` as an idempotency key or
explicitly document possible duplicate effects.

### 8.2 Kafka-backed ledger

The deduplication authority is a compacted Kafka topic, `job-execution-ledger.v1`, not a
scheduler database and not a pod-local cache alone. It has the same partition count as the
work topic. A completion record is keyed by the logical identity and written to the same
partition number as its source work record inside the completion transaction.

Each lane restores the ledger partition for its work partition into a local embedded state
store before becoming ready. Assignment is not ready until restoration reaches the
captured ledger end offset. Revocation closes or transfers that partition store. A local
memory cache may accelerate lookups but is never the durable authority.

Conforming duplicates must retain the original immutable `ownerId`, so they return to the
same work partition. Reusing `(jobId, attempt)` with a different owner or immutable payload
is a contract conflict: when detected it is sent to DLQ and never treated as a successful
duplicate. Cross-owner identity policing at ingestion requires a global registry and is
outside the worker's partition-local guarantee.

`dedupRetention` is configurable and must exceed the maximum broker redelivery, outage,
retry, replay, and operator-response window. Two hours is permitted only when those limits
are all demonstrably shorter; it is not a safe universal default. Expiry is performed by
an auditable tombstone process, and expiry means a later replay may execute again.

## 9. Rebalance and Shutdown

Assignment creates a monotonically increasing local epoch and starts ledger restoration.
Revocation marks the lane `REVOKED` before any drain begins, pauses dispatch, and rejects
new completion commands for the old epoch.

The worker drains within a configured deadline. Work that reaches a safe Kafka transaction
may complete; other work is abandoned without committing and is redelivered. A late
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
| Work/output/ledger topics | Non-empty, environment-scoped, expected partition counts. |
| `group.id` | Stable and allowlisted for the production worker fleet. |
| Instance/transactional ID | Unique per live worker slot and reused by its successor to fence a zombie. |
| Auto commit | Exactly `false`. |
| Isolation | Exactly `read_committed`. |
| Partition assignment strategy | Supported cooperative strategy, integration-tested. |
| Queue high/low watermarks | Positive, bounded, and compatible with `max.poll.records`. |
| Handler/worker capacity | Positive and bounded by pod resources. |
| TPS and burst | Non-negative TPS; positive burst for positive TPS. |
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

Compose must run Kafka, Schema Registry/topic setup, and at least two Vert.x worker pods.
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
- lane state, queue depth, current source offset, and transaction wait;
- fetched, started, succeeded, retried, DLQ, deduplicated, and conflicted totals;
- per-pod TPS permits and start rate;
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
| `VTX-LANE-01` | One pod owns three partitions | Three handlers may overlap, but never two from one partition. |
| `VTX-LANE-02` | Two records share one partition | Second starts only after first transaction commits. |
| `VTX-OFF-01` | Four records fetched from one partition | Only the current lane record can advance that partition to `offset + 1`; fetch position is never committed. |
| `VTX-TX-01` | Result send succeeds, lifecycle send fails | Transaction aborts; no output is visible to `read_committed`, source offset is unchanged. |
| `VTX-TX-02` | Two partition lanes complete together | Dedicated adapter serializes transactions and commits independent exact offsets. |
| `VTX-BP-01` | Output Kafka is unavailable | Lanes pause, queues remain bounded, event loop/heartbeats remain responsive, then recover. |
| `VTX-BLOCK-01` | Blocking handler exceeds event-loop threshold | Work runs outside event loop; health and other partitions remain responsive. |
| `VTX-REB-01` | Revoke during external call | Old epoch cannot commit; record redelivers without offset loss. |
| `VTX-DEDUP-01` | Completed `(jobId, attempt)` redelivers | Handler is not called; recorded outcome is returned/published. |
| `VTX-DEDUP-02` | Pod dies and another restores ledger | New lane becomes ready only after catch-up and suppresses the completed duplicate. |
| `VTX-DEDUP-03` | Same identity has different immutable data | Conflict is observable and reaches DLQ. |
| `VTX-TTL-01` | Ledger entry expires | Tombstone removes it; documentation states replay may execute again. |
| `VTX-POD-01` | One pod is killed during load | Assignments recover, queues stay bounded, and duplicates/lag are measured. |
| `VTX-CAP-01` | 20,000/day and compressed bursts | Per-pod TPS, owner serialization, lag, and recovery objectives hold. |
| `VTX-CAP-02` | 100,000/day and compressed bursts | Scale partitions/pods under the controlled partition-expansion procedure. |

Every test asserts zero event-loop blocking, zero skipped source offsets, zero same-owner
overlap, bounded queues, and no Kafka output/source-offset partial commit.

## 14. Migration

Migration changes the worker implementation, not the work-topic producer contract:

1. freeze Spec 006 schemas and canonical `ownerId` serialization;
2. deploy the ledger topic and inactive Vert.x workers;
3. replay representative production-like traffic into an isolated group and compare
   results with the Python reference worker;
4. prove assignment, offset, transaction, backpressure, dedup, and load scenarios;
5. stop Python worker dispatch and let its transactions finish or abort;
6. verify no Python group member remains and capture committed offsets;
7. start Vert.x workers with the same production `group.id` at controlled TPS;
8. restore ledger partitions before readiness;
9. observe lag, outcomes, duplicates, and partition ownership through the rollback window;
10. retain the Python image only as a rollback artifact, not a concurrent consumer; and
11. remove the Python pull-worker deployment after acceptance.

Rollback stops Vert.x dispatch before restarting Python workers. Both runtimes must never
consume the production group concurrently during cutover or rollback.

## 15. Acceptance Criteria

Spec 007 is complete when:

1. the production pull worker runs as a Dockerized Java 21/Vert.x 5 service;
2. client topic, schema, and `ownerId` partitioning contracts remain compatible;
3. no scheduler database or Python runtime is present in the worker image;
4. each partition is processed sequentially while different assigned partitions run
   concurrently;
5. no blocking handler or Kafka transaction operation runs on an event loop;
6. the no-argument consumer commit is not used for processed work;
7. exact source next offsets and required outputs commit in one Kafka transaction;
8. required-publication failure pauses intake without losing group responsiveness;
9. one pod-wide TPS limiter governs all partition lanes;
10. current assignment epochs fence late completions after rebalance;
11. completed conforming duplicates are suppressed within the configured retention window;
12. the remaining external-side-effect duplicate window is explicit and tested;
13. health, metrics, logs, and traces expose lane, Kafka, executor, and dedup state;
14. the full real-Kafka, chaos, 20,000/day, and 100,000/day suites pass; and
15. migration and rollback never run Python and Vert.x production consumers concurrently.

## 16. Out of Scope

- Future/delayed job scheduling and delayed retry.
- Exactly-once execution of an arbitrary external business side effect.
- A globally exact TPS limit across pods.
- Concurrent execution of multiple offsets from the same partition.
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
