# Kafka Pull-Based Job Scheduler Specification

Status: proposed

Supersedes: the database-polling execution model in Specs 002 and 004

Depends on: [`../002-real-persistence-kafka/spec.md`](../002-real-persistence-kafka/spec.md),
[`../003-production-hardening-resilience/spec.md`](../003-production-hardening-resilience/spec.md), and
[`../004-production-api-runtime/spec.md`](../004-production-api-runtime/spec.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Required Architecture](#3-required-architecture)
- [4. Topics and Partitioning](#4-topics-and-partitioning)
- [5. Message Contract](#5-message-contract)
- [6. Producer and Scheduling Behaviour](#6-producer-and-scheduling-behaviour)
- [7. Consumer Pod Behaviour](#7-consumer-pod-behaviour)
- [8. Backpressure](#8-backpressure)
- [9. Per-Pod TPS Rate Limiting](#9-per-pod-tps-rate-limiting)
- [10. Retry, DLQ, and Manual Commit Semantics](#10-retry-dlq-and-manual-commit-semantics)
- [11. Rebalancing, Shutdown, and Idempotency](#11-rebalancing-shutdown-and-idempotency)
- [12. Configuration](#12-configuration)
- [13. Observability and Operations](#13-observability-and-operations)
- [14. Testing Strategy](#14-testing-strategy)
- [15. Migration](#15-migration)
- [16. Acceptance Criteria](#16-acceptance-criteria)
- [17. Out of Scope](#17-out-of-scope)

## 1. Purpose

Replace the existing database-backed scheduler with a Kafka pull-consumer model. The
requesting client is the Kafka producer and publishes immediately eligible work directly
to the work topic. Worker pods pull executable jobs from Kafka, enforce
a local transactions-per-second limit, stop taking new work when required downstream
updates are unhealthy, and manually commit Kafka offsets only after a message reaches a
durable processing boundary.

The design provides at-least-once delivery. It does not claim exactly-once execution of an
external side effect. Duplicate delivery is expected after crashes, timeouts, and consumer
group rebalances and must be safe through stable job identity and idempotent handlers.

## 2. Scope

This specification covers:

- a versioned Kafka topic carrying executable job requests;
- deterministic partitioning and consumer-group scaling across worker pods;
- pull consumers with bounded concurrency and manual offset management;
- per-instance/per-pod TPS rate limiting;
- backpressure when result or lifecycle topic updates fail;
- retry classification and a separate dead-letter topic;
- rebalance, shutdown, replay, and duplicate-delivery behaviour;
- metrics, alerts, operational controls, and acceptance tests; and
- migration from the existing database-polling scheduler worker.

Spec 006 removes the scheduler database, due-job dispatcher, scheduler status tables, and
transactional work outbox from the new execution path. Kafka offsets and immutable Kafka
records are its durable control state. The existing `job-lifecycle-edr.v1` topic remains the
lifecycle evidence stream and must not be reused as the work queue.

## 3. Required Architecture

```text
 Client / Kafka producer
            |
            | keyed, immediately eligible request
            v
    Kafka: job-requests.v1
            |
    group: job-workers-v1
       /    |    \
 worker  worker  worker pods
       \    |    /
            | Kafka transaction where supported
            +------------+------------------+
            |            |                  |
            v            v                  v
 job-results.v1  job-lifecycle-edr.v1  job-requests-dlq.v1
```

All worker replicas for one logical workload use the same consumer group. Kafka therefore
assigns each partition to at most one group member at a time. Replicas beyond the work
topic's partition count remain idle for that consumer group.

Kafka does not provide native delayed delivery. Therefore every Spec 006 request is
eligible when published. A client must not publish a future job and expect a worker to hold
it until `scheduledAt`, because doing so would block later records in the partition. Future
scheduling requires a separate durable timer service and is outside this specification.

`job-results.v1` is an immutable result stream keyed by `jobId`. It replaces scheduler
status/attempt rows as the operational result record. A compacted Kafka state/changelog
topic may additionally back the worker's idempotency state; it is Kafka-owned state, not a
scheduler database.

## 4. Topics and Partitioning

### 4.1 Work topic

The work topic is `job-requests.v1` by default. It must have:

- an explicit partition count based on peak parallelism, throughput, and key skew;
- production-appropriate replication and minimum in-sync replicas;
- retention longer than the maximum supported outage and recovery window;
- `cleanup.policy=delete`, unless compaction is separately reviewed; and
- a registered schema with a rolling-upgrade-compatible policy.

The producer must set `ownerId` as the Kafka record key. `ownerId` is the stable
serialization owner for the job and may identify either one subscriber or one business
group. All jobs for the same owner therefore hash to the same partition. `ownerType`
(`SUBSCRIBER` or `GROUP`) records which ownership scope was selected; `ownerId` values must
be canonical and globally unambiguous across those namespaces.

The ownership rule must be deterministic for a workload. If subscriber jobs must never
overlap, every such job uses that subscriber's canonical `ownerId`. If the business group
is the serialization boundary, every member job uses the group's canonical `ownerId`.
Producers must not alternate between subscriber and group ownership for the same ordering
requirement, because those keys may map to different partitions. Recommended canonical
values include their namespace, for example `subscriber:123` or `group:123`.

Null, randomly changing, or `jobId` keys are prohibited. Every producer must use the same
key normalization and serializer. Clients must not select a partition explicitly. Retries
and DLQ replays must preserve the original serialized `ownerId` key.

Partition count is a capacity contract. Increasing it changes the partition selected for
some newly published keys and can weaken ordering across the change. Any increase requires
a rollout plan and an ordering-impact assessment.

### 4.2 Dead-letter topic

The dead-letter topic is `job-requests-dlq.v1` by default. It uses the original work-record
key and has enough partitions and retention for the expected failure and support workload.
DLQ payload access is limited to authorized operators.

A DLQ record is a new immutable envelope; the source Kafka record is never modified.
Duplicate DLQ publication is possible at an ambiguous acknowledgement boundary and must be
deduplicated by the original `(topic, partition, offset)`.

## 5. Message Contract

Each work record contains at least:

| Field | Requirement |
| --- | --- |
| `schemaVersion` | Required work-envelope version. |
| `jobId` | Stable globally unique job and idempotency key. |
| `ownerId` | Required stable subscriber or business-group identifier; also the Kafka record key. |
| `ownerType` | `SUBSCRIBER` or `GROUP`; declares the serialization scope represented by `ownerId`. |
| `correlationId` | Trace/business correlation identifier. |
| `jobType` | Allowlisted handler identifier. |
| `payload` or `payloadReference` | Versioned, size-bounded input or durable reference. |
| `requestedAt` | UTC time at which the client created the request. |
| `attempt` | Positive logical execution attempt number. |
| `maxAttempts` | Positive terminal attempt limit. |
| `createdAt` | UTC creation time. |
| `traceContext` | Optional allowlisted distributed tracing fields. |

The consumer validates the envelope and handler payload before execution. Secrets,
credentials, and unrestricted executable content must not be embedded. Malformed,
oversized, schema-incompatible, unsupported, or policy-invalid records are permanent
failures and follow the DLQ path.

## 6. Producer and Scheduling Behaviour

The requesting client is a Kafka producer. It must enable idempotent production, require
broker acknowledgement from the configured in-sync replicas, use the registered schema,
and always provide the stable `ownerId` key. A successful broker acknowledgement is the
submission acknowledgement. A timeout is ambiguous: the client retries with the same
`jobId` and identical immutable request fields.

Kafka producer idempotence does not deduplicate submissions made in a new producer session.
The client-supplied `jobId` is therefore mandatory. Workers maintain a Kafka-backed inbox
or result state keyed by `(jobId, attempt)` and reject a duplicate whose immutable fields
differ. A matching duplicate returns/re-emits the recorded outcome without repeating the
business side effect.

The producer must not wait for worker execution before acknowledging submission. It may
optionally wait for a result by consuming `job-results.v1` using `jobId` correlation, but
that is a separate end-to-end operation with its own deadline.

Direct production requires Kafka credentials and network reachability in every client.
Clients must receive least-privilege write-only access to the work topic, use centrally
maintained producer/schema libraries, and obey broker quotas and payload limits. If clients
cannot meet those controls, a thin stateless submission API may produce on their behalf;
that API is not a scheduler and adds no database.

Without scheduler query tables, status lookup, cancellation, search, and audit are
event-derived capabilities. They must read the result/lifecycle projections rather than
query operational scheduler rows. Cancellation, if later required, needs a separately
specified keyed command and race-resolution contract.

## 7. Consumer Pod Behaviour

Each worker pod must:

1. join the configured consumer group with a unique instance identity;
2. set `enable.auto.commit=false`;
3. poll records in bounded batches;
4. maintain bounded handler concurrency and a bounded local queue;
5. acquire a per-pod rate-limit permit immediately before starting a handler;
6. validate and idempotently process the job;
7. publish the outcome and required lifecycle records;
8. create a retry or DLQ handoff when required; and
9. commit only offsets whose preceding records in that partition are also safe to commit.

An offset must not be acknowledged when work has merely been fetched, queued, throttled,
or started. Concurrent processing within one partition requires an offset tracker that
advances only across the highest contiguous completed sequence. A simpler implementation
may process one record at a time per partition.

Partition affinity alone does not prevent concurrent execution when a pod dispatches
multiple records from one partition. The worker must allow at most one active job for each
`ownerId`. It may enforce this by processing one record at a time per partition or by using
a bounded keyed execution queue/lock. Jobs for different owners in the same partition may
run concurrently only when contiguous offset tracking remains correct.

Fetch size, local queue capacity, handler concurrency, and handler deadlines are bounded.
The local queue must not exceed the work a pod can safely finish within its shutdown and
consumer-session timing budget.

## 8. Backpressure

The pod stops accepting new work whenever a Kafka publication required to make processing
durable is failing. Required publications include result, lifecycle, retry, and DLQ records.

Backpressure pauses assigned Kafka partitions; it never commits or discards affected
records. While paused, the consumer continues calling `poll` frequently enough to maintain
group membership and heartbeats. It may finish bounded in-flight work but starts no more
handlers from paused partitions.

The controller uses this state machine:

```text
RUNNING -> PAUSED -> PROBING -> RUNNING
                  \-> PAUSED
```

- `RUNNING`: dispatch is permitted within capacity and TPS limits.
- `PAUSED`: new handler execution stops; health checks use bounded exponential backoff with
  jitter.
- `PROBING`: one bounded dependency probe or controlled record is attempted.
- Return to `RUNNING` only after the configured recovery success threshold.

Failure threshold, cooldown, maximum backoff, recovery threshold, and maximum sustained
pause before readiness fails are configurable. A permanently bad individual record must
not pause the whole consumer indefinitely; after bounded attempts it follows the DLQ path.
A shared dependency failure may pause all assignments in the pod.

Readiness reports not ready after the sustained-pause threshold. Liveness remains healthy
while the process can heartbeat and recover, preventing an orchestrator restart loop caused
only by a temporary downstream outage.

## 9. Per-Pod TPS Rate Limiting

Every worker pod has an independent token-bucket limiter. `rateLimitTps` is the sustained
number of handler starts per second and `rateLimitBurst` is the maximum accumulated burst.
A token is consumed immediately before a handler begins. Polling, validation, idempotency
lookup, offset commits, and internal dependency calls do not consume separate tokens.

The approximate configured fleet ceiling is:

```text
active worker pods * rateLimitTps
```

Actual throughput may be lower because of partition count, key skew, handler latency,
concurrency, and backpressure. This is intentionally not a globally exact rate limit;
autoscaling changes aggregate capacity and maximum replicas must be selected accordingly.

Rate-limited records remain uncommitted. The pod pauses partitions or defers dispatch
without busy-waiting while continuing heartbeat polls. Runtime rate changes apply atomically
inside a pod and expose the effective value. Zero TPS means administratively paused, not
unlimited.

## 10. Retry, DLQ, and Manual Commit Semantics

### 10.1 Success

After successful execution, the consumer publishes the result and lifecycle records and
may manually commit `processed offset + 1` only after those records are acknowledged and
the business side effect is complete or idempotently recorded. Where supported, the result,
lifecycle records, Kafka-backed inbox update, and consumed offset must be committed in one
Kafka transaction. A transaction or commit failure leaves the delivery uncommitted.

### 10.2 Transient failure

Retryable failures include explicitly classified timeouts, throttling, and temporary
dependency unavailability. Spec 006 supports bounded immediate retry: the consumer
publishes a new `job-requests.v1` record with `attempt + 1`, the same key and immutable
request fields, plus the failure lifecycle record. It then commits the source offset. These
Kafka writes and the consumed offset must use one Kafka transaction where supported.

The source offset is committed only after Kafka acknowledges the retry handoff. Handoff
failure pauses consumption and leaves it uncommitted. Delayed retry/backoff needs a timer
service and is outside this database-free design; rate limiting and backpressure still
protect dependencies from tight retry loops.

### 10.3 Permanent or exhausted failure

A permanently invalid/unsupported request, or one that exhausts `maxAttempts`, is published
to the DLQ. Its envelope includes:

- original topic, partition, offset, timestamp, key, and payload or governed reference;
- job, correlation, schema, and attempt identifiers when extractable;
- stable failure category and sanitized error summary;
- consumer group, worker identity, and failure timestamp; and
- replay count and original DLQ record ID for previously replayed work.

The consumer must receive Kafka acknowledgement for the DLQ, result, and terminal lifecycle
records before committing the source offset. If any publication fails, the source offset
remains uncommitted and backpressure applies.

Kafka transactions should atomically publish result/lifecycle/retry/DLQ records and advance
the consumed offset. They cannot make an external business side effect atomic, so handler
idempotency remains mandatory.

### 10.4 Forbidden commit points

The source offset must not be committed:

- on receipt, deserialization attempt, queue insertion, or handler start;
- after a failed or timed-out required result/lifecycle publication;
- before Kafka acknowledges a required DLQ publication;
- before a durable retry handoff;
- over an unfinished earlier record in the same partition; or
- during revocation unless the record already reached a safe durable boundary.

## 11. Rebalancing, Shutdown, and Idempotency

On partition revocation, the pod stops dispatching from that partition, waits only for the
bounded drain deadline, commits the highest contiguous safe offset, and abandons remaining
local work for safe redelivery. Late completion from a revoked owner must be rejected by a
durable fencing or idempotency check.

On shutdown, readiness fails first, new dispatch stops, in-flight work drains for a bounded
deadline, safe offsets are committed, and the consumer leaves the group. The deadline must
fit inside the platform termination grace period. `max.poll.interval.ms`, session timeout,
batch size, maximum handler time, and shutdown deadline form one reviewed timing budget.

End-to-end delivery is at least once. Before a non-repeatable side effect, a handler uses
`jobId` plus `attempt`, or a stable operation ID, as an idempotency key. `ownerId` controls
serialization and is not a replacement for the job idempotency key. Where the target
does not support idempotency keys, the handler uses a durable inbox, conditional update, or
compare-and-set boundary. An in-memory completed set is insufficient.

One `(jobId, attempt)` may produce at most one logical terminal outcome. Replayed success,
retry, and DLQ records converge without overwriting immutable fields or regressing terminal
state.

## 12. Configuration

The runtime validates at least:

| Setting | Requirement |
| --- | --- |
| Work and DLQ topic names | Non-empty and environment scoped. |
| Consumer group ID | Stable per logical worker fleet. |
| Consumer instance ID | Unique per live pod. |
| Transactional producer ID | Unique per live pod/process generation. |
| Auto commit | Disabled. |
| Consumer isolation | `read_committed` when Kafka transactions are enabled. |
| Batch, fetch, and local queue sizes | Positive and bounded. |
| Handler concurrency | Positive and compatible with partition strategy. |
| `rateLimitTps` / `rateLimitBurst` | Non-negative TPS; positive burst for positive TPS. |
| Pause/recovery thresholds | Positive and finite. |
| Retry count/backoff | Bounded with a finite maximum. |
| Poll, session, request, and shutdown deadlines | Mutually consistent timing budget. |

Invalid configuration fails startup before the consumer joins the group. Topic
auto-creation is disabled outside disposable local environments.

Kafka consumer `group.id` identifies the worker fleet and is unrelated to the business
group represented by an `ownerId` whose `ownerType` is `GROUP`.

## 13. Observability and Operations

Each pod emits structured logs and metrics for:

- assigned/paused partitions, rebalances, poll age, and commit failures;
- records fetched, started, succeeded, retried, exhausted, and sent to DLQ;
- lag and oldest unprocessed age by topic and partition;
- in-flight count, queue depth, handler latency, and update failures;
- configured/effective TPS, available tokens, and rate-limit wait time;
- backpressure state, reason, transitions, and pause duration; and
- duplicate replays and stale-owner rejections.

Traces carry `jobId`, `correlationId`, topic, partition, offset, attempt, and consumer
identity without prohibited payload data. Alerts cover sustained lag/age, all pods paused,
repeated rebalances, update or commit failure, DLQ growth, no active consumers, and
hot-partition skew.

Runbooks define how to pause/resume the group, inspect a DLQ record, fix its cause, replay
with authorization, and prove replay caused no duplicate logical outcome. Replay is audited
and creates a new work record; original Kafka records are not edited.

## 14. Testing Strategy

Unit tests cover `ownerId` normalization/key selection, ownership validation, failure
classification, token-bucket behaviour with a virtual clock, pause/resume transitions, and
contiguous offset tracking.

Integration tests against real Kafka prove:

1. equal keys remain ordered in one partition;
2. multiple jobs for one canonical `ownerId` always reach one partition and never execute
   concurrently;
3. one consumer group shares partitions across pods without simultaneous ownership;
4. each pod respects sustained TPS and burst capacity;
5. required-update failure pauses new work while heartbeats retain membership;
6. recovery resumes consumption and drains lag;
7. a crash before the durable result boundary redelivers;
8. a crash after durable completion but before commit produces a safe duplicate;
9. transient failure transactionally creates the next immediate attempt before source commit;
10. permanent/exhausted failure reaches the DLQ before source commit;
11. DLQ failure leaves the source offset uncommitted and applies backpressure;
12. out-of-order completion never commits over an unfinished earlier offset; and
13. rebalance and forced shutdown lose no acknowledged work.

A load test demonstrates throughput and lag for the selected partitions, maximum replicas,
per-pod TPS, realistic latency, and skewed keys. Evidence records configured and measured
TPS for each pod, not only aggregate throughput.

### 14.1 Capacity baseline and chaos profiles

The initial planning volume is 20,000 requests per day. This is approximately 0.23 requests
per second when evenly distributed, but partition sizing must use peak arrival rate,
handler duration, active-owner count, and owner skew rather than the daily average alone.
The initial test topology is six work partitions, three worker pods, 10 sustained starts
per second per pod, burst 10, bounded concurrency 20 per pod, and one active job per
`ownerId`. These are test assumptions to validate, not production guarantees.

The chaos harness from Spec 005 must add these bounded scenarios:

| ID | Workload/fault | Required proof |
| --- | --- | --- |
| `PULL-CAP-01` | 20,000 requests distributed across 24 hours, accelerated deterministically | No owner overlaps; measured TPS, lag, and partition balance match the baseline. |
| `PULL-CAP-02` | 20,000 requests compressed into one hour (about 5.6 requests/second) | Three pods remain within their individual TPS limits and backlog stays within the declared objective. |
| `PULL-CAP-03` | 20,000 requests compressed into ten minutes (about 33.4 requests/second) | Rate limiting caps starts near 30/second, excess becomes observable lag, and the backlog drains after the burst. |
| `PULL-CAP-04` | 20,000 requests compressed into one minute (about 333.4 requests/second) | Intake remains durable, memory/queues stay bounded, no offsets are skipped, and recovery time is measured. |
| `PULL-SKEW-01` | One hot `ownerId` receives 50% of requests | That owner never overlaps; its partition skew and head-of-line effects are observable without corrupting other partitions. |
| `PULL-POD-01` | Terminate one of three pods during the ten-minute burst profile | Rebalance is bounded, ownership remains exclusive, duplicates converge, and remaining pods obey their own TPS limits. |
| `PULL-BP-01` | Fail result/lifecycle publication during the burst | Affected partitions pause without source commits, heartbeat polls continue, and backlog drains after recovery. |
| `PULL-REB-01` | Repeated controlled rebalances with concurrent owners | No owner executes concurrently across generations and no offset advances over incomplete work. |

Each scenario records the generated arrival curve; unique and hot-owner distributions;
handler p50/p95/p99 duration; per-pod starts per second; partition lag and oldest age;
queue/concurrency high-water marks; owner-overlap violations; rebalance/pause history; and
time to drain below the recovery threshold. Fault duration, request count, affected pods,
abort conditions, and cleanup follow Spec 005 safety requirements.

Six partitions are accepted only if these tests meet the declared latency and recovery
objectives. Twelve partitions should be evaluated before production when peak bursts,
handler duration, or active-owner distribution cannot be served by six. Changing partition
count after cutover remains a controlled ordering migration because existing `ownerId`
values may map differently.

## 15. Migration

Cutover must:

1. deploy the work topic, schema, DLQ, metrics, and inactive consumers;
2. stop new claims by database-polling execution workers;
3. finish or recover their existing claims to a known durable state;
4. run a one-time migration producer that publishes each remaining eligible database job
   with a stable `jobId`, recording an auditable migration manifest;
5. start the Kafka consumer group and verify processing, lag, and completion; and
6. retain a rollback point that cannot activate both execution paths for the same jobs.

A migration rehearsal must prove the old pollers and new consumers never execute the same
eligible population concurrently. After cutover, new clients publish directly to Kafka and
the scheduler database is not part of the Spec 006 runtime. Removing legacy database data
or infrastructure requires a separately approved retention and rollback decision.

## 16. Acceptance Criteria

Spec 006 is complete when:

1. executable jobs use a versioned work topic with canonical `ownerId` as the deterministic
   partition key, preserved through retry and DLQ replay;
2. worker pods pull through one consumer group with auto-commit disabled;
3. a pod starts no new work while required downstream updates are failing;
4. paused consumers maintain the polling/heartbeats needed to avoid unintended churn;
5. every pod enforces its configured sustained TPS and burst;
6. success offsets are committed only after required result and lifecycle records are
   acknowledged;
7. retry offsets are committed only after the next immediate attempt is acknowledged;
8. permanent/exhausted failures are acknowledged in the DLQ and durably recorded before
   source commit;
9. duplicates, crashes, and rebalances create at most one logical terminal outcome per
   attempt;
10. no two jobs for the same `ownerId` execute concurrently, including when one partition
    is processed with concurrency greater than one;
11. lag, throttling, pauses, commits, retries, and DLQ growth are observable and alerted;
12. automated real-infrastructure tests cover the failure boundaries in Section 14; and
13. cutover and rollback prevent concurrent ownership by old and new execution paths.

## 17. Out of Scope

- Exactly-once execution of arbitrary external side effects.
- A globally exact TPS limit across replicas; this specification limits each pod.
- Kafka topic auto-scaling or automatic partition-count changes.
- Future/delayed job scheduling and delayed retry; both require a separate timer service.
- Automatic replay or deletion of DLQ records.
- Priority scheduling, total ordering across partitions, and tenant-fair queuing.
- Replacing the lifecycle EDR journal or visibility projection.
