# Vert.x Kafka Pull Worker Architecture

Status: proposed for review

Last updated: 2026-09-06

Governing specification: [`spec.md`](spec.md)

This arc42 document is the target production architecture for the Kafka pull worker. It
supersedes the Python worker architecture in Spec 006 while retaining Spec 006 topic and
producer contracts.

## Contents

- [1. Introduction and Goals](#1-introduction-and-goals)
- [2. Architecture Constraints](#2-architecture-constraints)
- [3. Context and Scope](#3-context-and-scope)
- [4. Solution Strategy](#4-solution-strategy)
- [5. Building-Block View](#5-building-block-view)
- [6. Runtime View](#6-runtime-view)
- [7. Deployment View](#7-deployment-view)
- [8. Cross-Cutting Concepts](#8-cross-cutting-concepts)
- [9. Architecture Decisions](#9-architecture-decisions)
- [10. Quality Requirements](#10-quality-requirements)
- [11. Risks and Technical Debt](#11-risks-and-technical-debt)
- [12. Glossary](#12-glossary)

## 1. Introduction and Goals

### 1.1 Purpose

Clients publish immediately executable jobs directly to Kafka using canonical `ownerId` as
the record key. Vert.x worker pods consume the topic, serialize work within each partition,
run different assigned partitions concurrently, limit starts per pod, and commit required
Kafka outcomes with exact source offsets.

The scheduler database remains removed. Kafka contains the work, durable outcomes,
lifecycle evidence, failure quarantine, completed-request ledger, and consumer offsets.

### 1.2 Architecture goals

| Priority | Goal | Architectural response |
| ---: | --- | --- |
| 1 | Offset correctness | One serial lane per partition and explicit next-offset transactions. |
| 2 | Owner serialization | Canonical `ownerId` partition key and no same-partition overlap. |
| 3 | Reactive responsiveness | Event-loop-confined control state and no blocking event-loop calls. |
| 4 | Useful concurrency | Independent assigned partition lanes execute concurrently. |
| 5 | Bounded load | Pod-wide token bucket, lane queues, worker executor, and pause/resume. |
| 6 | Atomic Kafka completion | Required outputs, ledger record, and source offset use one transaction. |
| 7 | Completed duplicate suppression | Kafka-backed partition-local ledger restored before readiness. |
| 8 | Safe ownership transfer | Assignment epochs reject late completions after revocation. |
| 9 | Operability | Lane, event-loop, executor, transaction, lag, and ledger state are observable. |

### 1.3 Stakeholders

| Stakeholder | Concern |
| --- | --- |
| Client teams | Existing topic/key/schema compatibility and acknowledgement semantics. |
| Worker developers | Vert.x threading, lane ordering, handler APIs, and failure classification. |
| Kafka platform | Groups, transactions, partition capacity, ACLs, and ledger compaction. |
| Operations | Lag, pauses, blocked loops, restores, rebalances, and rollback. |
| Business dependency owners | TPS limits, idempotency keys, deadlines, and duplicate effects. |
| Security | Direct Kafka access, least privilege, payload controls, and DLQ access. |

## 2. Architecture Constraints

### 2.1 Functional constraints

- Requests are eligible immediately and are keyed by canonical `ownerId`.
- `ownerId` represents either `subscriber:<id>` or `group:<id>` as declared by `ownerType`.
- All production replicas use one consumer group.
- Each assigned partition processes exactly one active record at a time.
- Different assigned partitions may process concurrently.
- Auto commit is disabled and fetched positions are never treated as processed offsets.
- Required result/lifecycle/retry/DLQ records precede or share the source commit boundary.
- Each pod has one shared TPS limiter.
- Output failure stops new starts and retains uncommitted records.
- A completed logical request is suppressed within the deduplication retention window.

### 2.2 Technology constraints

- Java 21 is the runtime baseline.
- A supported Vert.x 5 release is pinned through the Vert.x BOM.
- Maven builds and tests the service reproducibly.
- Vert.x Core, Kafka Client, Web, Config, Micrometer, and JUnit 5 integrations are used
  where applicable.
- The underlying Apache Kafka producer is isolated behind a transaction adapter for
  `sendOffsetsToTransaction`.
- Kafka transaction operations run on a dedicated single-thread executor.
- Blocking handlers run on a separate bounded executor and never on an event loop.
- Production images run as non-root and contain no Python or scheduler DB client.

### 2.3 Delivery constraints

Kafka transactions provide atomic visibility for Kafka outputs and source offsets. An
arbitrary external side effect cannot participate in that transaction. The end-to-end
guarantee therefore remains at least once across ambiguous external-call failures.

Completed deduplication narrows, but does not eliminate, this window. Business handlers
must propagate `(jobId, attempt)` as an idempotency key whenever the dependency supports it.

## 3. Context and Scope

### 3.1 System context

```mermaid
flowchart LR
    producer["External system: Job producer"]
    execution["Software system: Vert.x pull execution"]
    dependency["External system: Business dependency"]
    visibility["Software system: Visibility and audit"]
    operator["Person: Operator"]

    producer -->|"Keyed immediate request"| execution
    execution -->|"Idempotency-keyed operation"| dependency
    execution -->|"Results and lifecycle"| visibility
    operator -->|"Observe, pause, replay"| execution
    execution -->|"Health, metrics, DLQ"| operator
```

### 3.2 System boundary

Inside the Vert.x worker boundary are configuration validation, Kafka consumer control,
partition lanes, queue/capacity controls, the TPS limiter, handler registry, business
adapters, outcome classification, transaction coordination, ledger restoration, health,
metrics, and tracing.

Kafka, Schema Registry, business dependencies, secret/identity systems, monitoring, and
visibility projections remain external systems. The Python simulator and Spec 006 worker
are migration/reference systems, not production peers after cutover.

### 3.3 External interfaces

| Interface | Direction | Consistency | Purpose |
| --- | --- | --- | --- |
| `job-requests.v1` | Inbound | Broker-acknowledged, at least once | Immediately eligible work. |
| Business handler | Outbound | At least once | Perform the requested effect. |
| `job-results.v1` | Outbound | Transactional with source offset | Immutable attempt outcome. |
| `job-lifecycle-edr.v1` | Outbound | Transactional with source offset | Lifecycle evidence. |
| `job-requests-dlq.v1` | Outbound | Transactional with source offset | Permanent/exhausted failures. |
| `job-execution-ledger.v1` | Internal | Transactional, compacted | Completed duplicate authority. |
| `/health/live`, `/health/ready`, `/metrics` | Outbound | Near real time | Platform and operator status. |

## 4. Solution Strategy

1. Keep Spec 006's work, result, lifecycle, retry, DLQ, and `ownerId` contracts.
2. Replace the production Python worker with one Vert.x service per pod.
3. Confine Kafka consumer control and mutable lane state to Vert.x contexts.
4. Route each record to a serial lane identified by its topic and partition.
5. Allow lanes for different partitions to execute concurrently.
6. Prefer non-blocking handlers; isolate unavoidable blocking work in a bounded executor.
7. Serialize Kafka transactions on a dedicated adapter outside the event loop.
8. Atomically publish required outcomes, ledger updates, and exact source offsets.
9. Pause partitions at bounded queue watermarks or failure boundaries.
10. Restore the ledger for an assignment before advertising readiness.

This structure obtains concurrency from Kafka partitions. It does not need concurrent
offsets within one partition, so manual commit semantics remain simple and auditable.

## 5. Building-Block View

### 5.1 Containers

```mermaid
flowchart LR
    clients["External: producer clients"]
    work[("Kafka work partitions")]
    workers["Container set: Vert.x worker pods"]
    target["External: business dependency"]
    outputs[("Kafka result lifecycle and DLQ")]
    ledger[("Kafka compacted execution ledger")]
    visibility["Container: visibility projection"]

    clients -->|"ownerId-keyed records"| work
    work -->|"one consumer group"| workers
    workers -->|"non-blocking or isolated call"| target
    workers -->|"transactional outputs"| outputs
    workers -->|"transactional completion"| ledger
    outputs --> visibility
```

### 5.2 Vert.x worker components

```mermaid
flowchart LR
    boot["Bootstrap and config"]
    consumer["Consumer controller"]
    router["Partition router"]
    lanes["Serial partition lanes"]
    capacity["Capacity gate"]
    limiter["Pod TPS limiter"]
    handlers["Handler registry and adapters"]
    classifier["Outcome classifier"]
    tx["Single-thread transaction adapter"]
    state["Partition ledger stores"]
    pressure["Backpressure controller"]
    telemetry["Health metrics tracing"]

    boot --> consumer
    boot --> tx
    consumer --> router --> lanes
    lanes --> state
    lanes --> capacity --> limiter --> handlers --> classifier --> tx
    tx -->|"success or failure"| lanes
    tx --> pressure --> consumer
    consumer --> telemetry
    lanes --> telemetry
    capacity --> telemetry
    tx --> telemetry
    state --> telemetry
```

### 5.3 Component responsibilities

| Component | Responsibility | Threading rule |
| --- | --- | --- |
| Bootstrap | Validate config/topics and construct dependencies. | Startup context; bounded async calls. |
| Consumer controller | Subscription, assignment, pause/resume, close. | One context-owned actor. |
| Partition router | Route records and enforce bounded queues. | Consumer context. |
| Partition lane | Serialize one partition's state and Future chain. | Context-confined; one active job. |
| TPS limiter | Grant pod-wide start permits using monotonic time. | Context-confined timers. |
| Async handler | Invoke non-blocking Vert.x clients. | Event loop, never blocking. |
| Blocking adapter | Bridge legacy blocking handler. | Dedicated bounded worker executor. |
| Transaction adapter | Produce outputs and send exact group offsets. | Dedicated single thread; no overlap. |
| Ledger store | Restore and query completed identities per partition. | Lane/context-owned access. |
| Backpressure | Decide pause, probe, resume, readiness. | Context-confined state machine. |

## 6. Runtime View

### 6.1 Successful request

```mermaid
sequenceDiagram
    participant C as Client producer
    participant K as Kafka
    participant V as Vert.x consumer controller
    participant L as Partition lane
    participant B as Business dependency
    participant T as Transaction adapter

    C->>K: Produce request keyed by ownerId
    K-->>C: Broker acknowledgement
    V->>L: Enqueue source record
    L->>L: Check ledger and acquire TPS
    L->>B: Execute with jobId and attempt idempotency key
    B-->>L: Success
    L->>T: Submit outputs ledger record and exact next offset
    T->>K: Begin transaction and send records
    T->>K: Send source next offset with group metadata
    T->>K: Commit transaction
    K-->>T: Transaction committed
    T-->>L: Safe completion
    L->>L: Release lane and start next offset
```

### 6.2 Concurrent partitions

```mermaid
flowchart LR
    consumer["One consumer controller"]
    p0["Partition 0 lane: 10 then 11"]
    p2["Partition 2 lane: 40 then 41"]
    p5["Partition 5 lane: 70 then 71"]
    tx["Serialized transaction adapter"]

    consumer --> p0
    consumer --> p2
    consumer --> p5
    p0 -->|"completion command"| tx
    p2 -->|"completion command"| tx
    p5 -->|"completion command"| tx
```

Offsets 10, 40, and 70 may execute simultaneously. Offsets 11, 41, and 71 cannot start
until the preceding transaction in their own lane succeeds. Transaction serialization may
briefly queue completed lanes but does not force their business handlers to run globally
one at a time.

### 6.3 Completed duplicate

```mermaid
sequenceDiagram
    participant K as Kafka
    participant L as Partition lane
    participant S as Local restored ledger
    participant T as Transaction adapter

    K->>L: Redeliver completed jobId and attempt
    L->>S: Lookup logical identity and immutable hash
    S-->>L: Completed matching outcome
    L->>T: Submit duplicate outcome and exact source next offset
    T->>K: Commit Kafka transaction without business call
    K-->>T: Transaction committed
```

### 6.4 Required-output failure

```mermaid
sequenceDiagram
    participant L as Partition lane
    participant T as Transaction adapter
    participant K as Kafka
    participant P as Backpressure controller

    L->>T: Submit completion command
    T->>K: Begin transaction and write output
    K--xT: Timeout or failure
    T->>K: Abort transaction
    T-->>L: Completion failed
    L->>P: Pause with source offset retained
    P->>K: Pause affected assignments
    P->>P: Run bounded recovery probes
    P->>K: Resume after recovery threshold
```

### 6.5 Rebalance during external execution

```mermaid
sequenceDiagram
    participant A as Old partition lane
    participant G as Kafka group coordinator
    participant B as New partition lane
    participant D as Business dependency
    participant T as Transaction adapter

    A->>D: Begin operation
    G->>A: Revoke partition
    A->>A: Mark assignment epoch revoked
    G->>B: Assign partition
    B->>B: Restore ledger and become ready
    D-->>A: Late success
    A->>T: Submit completion with old epoch
    T--xA: Reject stale epoch
    G->>B: Redeliver from committed offset
```

The stale lane cannot commit Kafka state. The business dependency may already have applied
the first call; its idempotency key is the only way to prevent a repeated physical effect.

## 7. Deployment View

```mermaid
flowchart TB
    subgraph kafka["Kafka cluster"]
        requests["job-requests.v1 with N partitions"]
        results["result lifecycle and DLQ topics"]
        ledger["execution ledger with N partitions"]
    end

    subgraph k8s["Kubernetes deployment"]
        pod1["Pod 1: Java 21 Vert.x"]
        pod2["Pod 2: Java 21 Vert.x"]
        podN["Pod N: Java 21 Vert.x"]
    end

    requests --> pod1
    requests --> pod2
    requests --> podN
    pod1 --> results
    pod2 --> results
    podN --> results
    pod1 <--> ledger
    pod2 <--> ledger
    podN <--> ledger
```

One pod can actively use at most its assigned partition count. With six partitions and two
pods, the stable distribution is normally three lanes per pod and six concurrent jobs in
the fleet. With six partitions and more than six pods, extra pods have no work assignment.
Stable worker-slot identities, such as StatefulSet ordinals, form transactional IDs. A
replacement reuses the slot ID to fence a zombie predecessor; separate live slots never
share one.

Scaling to 100,000 requests/day is driven by peak rate, handler duration, owner skew, and
recovery objective rather than the daily average. Add pods up to the existing partition
count first. Increase partitions only through an ordering-aware migration because the
`ownerId` hash mapping changes for newly produced records. The ledger topic must expand in
lockstep, and cutover must prevent one owner from overlapping old and new mappings.

## 8. Cross-Cutting Concepts

### 8.1 Threading and reactive execution

Event loops own control-plane state and compose asynchronous Futures. Blocking work is
explicitly isolated. `executeBlocking(..., ordered=false)` is used behind a named bounded
executor when required because partition lanes already establish order. Event-loop delay
and executor saturation are release metrics, not merely debug logs.

### 8.2 Offset safety

The service never calls no-argument `consumer.commit()` for work completion. It constructs
an exact map of next offsets. A partition lane can propose only its current source offset
plus one. A transaction adapter may combine proposals only after verifying current group
generation and distinct partitions.

If four offsets were fetched from one partition, processing offset 10 permits committing
11, not the consumer's fetched position 14. Offset 11 starts only after 11 is durably the
committed next position.

### 8.3 Transaction ownership

The transaction adapter is a serial actor backed by one dedicated thread. It is the sole
owner of transaction lifecycle operations. It receives immutable completion commands,
checks assignment epochs, writes all required Kafka records, sends offsets with consumer
group metadata, and reports a Future back to the lane context.

The adapter's queue is bounded. Saturation backpressures lane dispatch. Transactional IDs
are unique by environment, workload, and stable worker slot. The replacement for a slot
reuses its ID during transaction initialization, fencing a stale producer from that slot.

### 8.4 Deduplication

Completed ledger records contain logical identity, immutable request hash, outcome
reference, completion time, source coordinates, and expiry time. They are written in the
same Kafka transaction as the logical result and source offset.

Ledger partitions mirror work partitions. Assignment restoration reads to a captured end
offset before the lane becomes ready. Compaction bounds historical keys; an explicit
tombstone process applies business retention. A two-hour TTL is valid only when the entire
redelivery and support window is shorter than two hours.

### 8.5 Backpressure and capacity

Per-partition high/low watermarks protect memory. The pod TPS token bucket protects the
business dependency. The transaction-adapter queue protects Kafka/JVM resources. Any gate
can pause affected partitions without committing records merely because they were fetched.

### 8.6 Security

Clients can write only the request topic. Workers can read only the configured group and
write named result/lifecycle/retry/DLQ/ledger topics. Only the worker transactional-ID
prefix is authorized. DLQ and replay are separately privileged. Payload logging is denied
by default.

### 8.7 Observability

OpenTelemetry context is propagated through Future chains and worker-executor boundaries.
Micrometer exposes Vert.x/JVM, custom lane, TPS, Kafka, transaction, ledger, and business
adapter metrics. Cardinality is bounded: raw `jobId` and `ownerId` are trace/log fields, not
metric labels.

## 9. Architecture Decisions

| ID | Decision | Rationale | Consequence |
| --- | --- | --- | --- |
| ADR-007-01 | Production pull workers use Java 21 and Vert.x 5. | Matches the target reactive microservice platform. | A separate Maven module and image replace the Python worker. |
| ADR-007-02 | One consumer controller exists per pod. | Keeps group membership and assignment state coherent. | Scale comes from partitions and pods, not extra consumers in one pod. |
| ADR-007-03 | One active record is allowed per partition. | Makes ordering and manual commits unambiguous. | Unrelated owners sharing a partition are serialized. |
| ADR-007-04 | Different partition lanes execute concurrently. | Uses Kafka parallelism without offset gaps. | Pod concurrency is bounded by assignments and capacity. |
| ADR-007-05 | Event loops never run blocking handlers or Kafka transaction calls. | Preserves consumer and health responsiveness. | Bounded dedicated executors are required. |
| ADR-007-06 | No-argument consumer commit is forbidden. | It may acknowledge fetched but unfinished records. | Exact offset maps are always constructed. |
| ADR-007-07 | Kafka transaction lifecycle is serialized in one adapter. | Producer transactions cannot overlap and Vert.x lacks the required high-level offset API. | Transaction throughput is measured as a potential bottleneck. |
| ADR-007-08 | One source record uses one transaction initially. | Simplifies failure, lane release, and evidence. | Cross-partition transaction batching is deferred. |
| ADR-007-09 | Completed deduplication uses a compacted Kafka ledger plus local restored stores. | Avoids reintroducing a scheduler database or relying on ephemeral caches. | Assignment readiness waits for restore. |
| ADR-007-10 | Dedup retention follows the recovery/replay window, not an arbitrary TTL. | A short TTL silently reopens duplicate execution. | Cleanup requires governed tombstones. |
| ADR-007-11 | External operations receive `(jobId, attempt)` idempotency keys. | Kafka cannot atomically commit an external side effect. | Dependencies unable to honor the key retain at-least-once effects. |
| ADR-007-12 | Python remains a test oracle during migration only. | Enables differential validation without dual production processing. | Cutover must enforce mutually exclusive consumers. |

## 10. Quality Requirements

| Scenario | Required response |
| --- | --- |
| Pod owns three partitions | Up to three handlers run concurrently, one per partition. |
| Four records are fetched from one partition | Only the lowest record executes; commit advances exactly one safe next offset. |
| TPS capacity is exhausted | Lanes wait on timers without occupying worker/event-loop threads. |
| Output Kafka fails | Transaction aborts, offset remains unchanged, queues stay bounded, and intake pauses. |
| One blocking handler stalls | Event loop, health, Kafka control, and other lanes remain responsive. |
| Partition is revoked mid-call | Old epoch cannot commit; unfinished offset redelivers. |
| Completed request redelivers | Ledger suppresses the handler inside retention. |
| New pod receives a partition | It restores ledger state before readiness and dispatch. |
| Immutable duplicate conflicts | It is quarantined and does not reuse a prior success. |
| One owner carries half of traffic | Owner remains serial; skew and head-of-line delay are visible. |
| Volume rises to 100,000/day | Existing partitions scale through more pods first; expansion follows controlled remapping. |
| Transaction adapter saturates | Its bounded queue pauses lanes and emits saturation metrics. |

## 11. Risks and Technical Debt

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Transaction adapter becomes bottleneck | Handler concurrency does not translate into completion throughput. | Benchmark one-record transactions; later batch safe cross-partition completions. |
| Incorrect native-client threading | Event-loop stalls or unsafe consumer metadata access. | Encapsulated adapter, thread-affinity tests, no general `unwrap()` usage. |
| Vert.x pause has buffered records | Queue temporarily receives records after pause. | Size for `max.poll.records`, enforce bounded router, test high-water behavior. |
| Ledger restore delays readiness | Rebalance recovery takes longer. | Partitioned restore, restore metrics, retention/compaction tuning. |
| Ledger TTL is too short | Old duplicate executes again. | Tie retention to declared recovery/replay window and alert before expiry. |
| External effect is ambiguous at crash | Physical duplicate remains possible. | Dependency idempotency key, explicit metrics, chaos proof. |
| Hot owner limits one partition | Owner throughput cannot scale horizontally. | Measure skew; split owner only through a business-approved ordering change. |
| Partition expansion remaps owners | Old and new owner jobs may overlap. | Quiesce/drain or versioned-topic migration with owner fencing. |

## 12. Glossary

| Term | Meaning |
| --- | --- |
| Partition lane | Serial asynchronous state machine for one assigned topic partition. |
| Consumer controller | Context-owned Vert.x component controlling subscription and assignments. |
| Exact next offset | The processed record offset plus one, committed for a named partition. |
| Transaction adapter | Single-thread component coupling Kafka outputs and consumed offsets. |
| Assignment epoch | Local generation token invalidated when a partition is revoked. |
| Completed ledger | Kafka-backed record of a completed logical `(jobId, attempt)`. |
| Restore | Catching a partition-local ledger store up before processing assigned work. |
| Safe boundary | Point where required Kafka outputs and source next offset are atomically durable. |
