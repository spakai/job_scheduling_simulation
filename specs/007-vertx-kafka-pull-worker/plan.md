# Vert.x Kafka Pull Worker Implementation Plan

Status: proposed

Implements: [`spec.md`](spec.md)

Architecture: [`arc42.md`](arc42.md)

Depends on: completed Spec 006 Kafka contracts and Dockerized reference worker

## Contents

- [1. Objective](#1-objective)
- [2. Baseline and Gaps](#2-baseline-and-gaps)
- [3. Delivery Principles](#3-delivery-principles)
- [4. Target Repository Shape](#4-target-repository-shape)
- [5. Delivery Sequence](#5-delivery-sequence)
- [6. Detailed Phase Plan](#6-detailed-phase-plan)
- [7. Test Matrix](#7-test-matrix)
- [8. CI and Evidence](#8-ci-and-evidence)
- [9. Migration and Rollback](#9-migration-and-rollback)
- [10. Traceability](#10-traceability)
- [11. Review Gates](#11-review-gates)
- [12. Definition of Done](#12-definition-of-done)

## 1. Objective

Deliver a Java 21/Vert.x 5 production pull-worker that preserves Spec 006's database-free
Kafka interface while changing execution from one record per pod to bounded concurrent records within each assigned
partition, preserving same-owner FIFO and keeping partition counts low. The implementation must keep event loops non-blocking, serialize Kafka
transactions, provide exact manual-offset semantics, add completed-request deduplication,
and run as a hardened Docker container.

## 2. Baseline and Gaps

The repository already has:

- versioned request/result/DLQ contracts and `ownerId` key validation;
- six-partition local topics and two Dockerized Python workers;
- transactional result/lifecycle/DLQ plus source-offset completion;
- per-pod TPS, backpressure, health, retry, and manual replay behavior;
- owner-affinity, rebalance, Docker, and performance evidence; and
- deterministic Python primitives that can act as a behavior oracle.

The migration gaps are:

| Gap | Consequence |
| --- | --- |
| No Java/Vert.x module | Target microservice cannot be built or deployed. |
| Python worker is globally sequential | A pod underuses multiple assigned partitions. |
| No explicit partition-lane runtime | Concurrent dispatch, owner gates, and contiguous-completion tracking are missing. |
| Vert.x producer lacks high-level consumed-offset transaction API | Native Kafka adapter is required and must not block event loops. |
| No completed-request ledger | Redelivery after completion may invoke the handler again. |
| No Vert.x event-loop/executor evidence | Responsiveness under blocking work is unproven. |
| No same-group cross-runtime cutover guard | Python and Vert.x could overlap during migration. |

## 3. Delivery Principles

- Freeze Kafka contracts before replacing the runtime.
- Keep one mutable-state owner for consumer control and each partition lane.
- Compose `Future` chains; do not block event loops with `await`, sleeps, Kafka transaction
  calls, or synchronous business clients.
- Obtain concurrency within and across partitions, with explicit pod/partition limits and same-owner FIFO gates.
- Use exact next-offset maps; never commit consumer fetch position implicitly.
- Serialize all transaction lifecycle calls in one bounded adapter.
- Restore durable dedup state before lane readiness.
- Make queues, executors, transactions, handlers, restores, and shutdown bounded.
- Compare Vert.x outcomes against the Python reference before production cutover.
- Never run Python and Vert.x production consumers concurrently.

## 4. Target Repository Shape

```text
vertx-pull-worker/
  pom.xml
  Dockerfile
  src/main/java/com/example/jobs/pull/
    Main.java
    WorkerVerticle.java
    config/
      WorkerConfig.java
      ConfigValidator.java
    contracts/
      JobRequest.java
      JobResult.java
      DlqEnvelope.java
      LedgerRecord.java
    kafka/
      ConsumerController.java
      PartitionRouter.java
      TransactionAdapter.java
      TransactionCommand.java
      AssignmentEpoch.java
    lane/
      PartitionLane.java
      LaneRegistry.java
      LaneState.java
      ContiguousCompletionTracker.java
      OwnerGate.java
      TrackedRecord.java
    execution/
      Handler.java
      HandlerRegistry.java
      BlockingHandlerAdapter.java
      OutcomeClassifier.java
      CapacityGate.java
      TokenBucket.java
    dedup/
      LedgerStore.java
      LedgerRestorer.java
      LedgerCleanup.java
    resilience/
      BackpressureController.java
      RecoveryProbe.java
    telemetry/
      HealthRoutes.java
      Metrics.java
      Tracing.java
  src/main/resources/
    logback.xml
    schemas/
  src/test/java/com/example/jobs/pull/
    unit/
    integration/
    chaos/
infra/kafka/schemas/
  job-execution-ledger-v1.json
compose.yaml
docs/
  spec-007-runbook.md
  spec-007-evidence.md
specs/007-vertx-kafka-pull-worker/
  spec.md
  arc42.md
  plan.md
```

Package names may change to the organization's namespace, but component boundaries and
threading ownership are architectural requirements.

## 5. Delivery Sequence

```text
Phase 0  Contract freeze, technical spikes, and build foundation
Phase 1  Vert.x bootstrap, configuration, health, and Docker image
Phase 2  Consumer controller, bounded routing, and partition lanes
Phase 3  Handlers, worker isolation, TPS, and backpressure
Phase 4  Transaction adapter and exact source offsets
Phase 5  Kafka-backed completed ledger and restoration
Phase 6  Rebalance, shutdown, observability, and security hardening
Phase 7  Performance, chaos, migration, rollback, and retirement
```

The transaction and consumer-threading spikes in Phase 0 are gates. Production code must
not spread native-client `unwrap()` calls while those ownership rules are unresolved.

## 6. Detailed Phase Plan

### Phase 0 — Contracts and technical spikes

Tasks:

1. Freeze Spec 006 topic names, schema versions, owner normalization, retry, and DLQ fields.
2. Select and pin Java 21, the approved Vert.x 5 BOM, Kafka client, testcontainers, JUnit 5,
   logging, OpenTelemetry, and Micrometer versions.
3. Create a Maven module with compiler, test, coverage, dependency convergence, formatting,
   static analysis, and reproducible-build rules.
4. Spike the Vert.x Kafka consumer assignment, pause/resume, and explicit offset-map APIs.
5. Spike the native producer transaction sequence including current consumer-group metadata.
6. Prove native transaction calls run only on one dedicated thread and return Vert.x Futures
   to the originating contexts.
7. Prove a transaction containing result plus exact source offset is atomically visible to
   `read_committed` consumers.
8. Decide embedded ledger storage implementation and state-directory lifecycle.
9. Add architecture fitness tests forbidding no-argument work commits and general native
   consumer/producer access outside approved adapters.

Exit criteria:

- One executable spike commits one exact source offset with one output transaction.
- A blocked transaction call does not delay the Vert.x event loop or health endpoint.
- Dependency and license/security scans pass.
- Schema fixtures round-trip identically between Python and Java.

### Phase 1 — Service bootstrap and Docker

Tasks:

1. Implement `Main` and `WorkerVerticle` lifecycle with asynchronous start/stop.
2. Load environment/file configuration and fail startup on all unsafe Kafka/timing settings.
3. Add liveness, readiness, startup, and Prometheus metrics routes using Vert.x Web.
4. Add structured JSON logging and trace-context propagation foundations.
5. Build a multi-stage Dockerfile with Maven cache layers and a minimal non-root Java 21
   runtime.
6. Add read-only-root-filesystem support and bounded writable state/temp directories.
7. Add two inactive Vert.x worker services to Compose without replacing Python yet.
8. Add SBOM/image scanning and container startup tests.

Exit criteria:

- The image starts without Python or scheduler-database configuration.
- Health/metrics remain responsive under a deliberately occupied worker executor.
- Invalid `enable.auto.commit`, group, topic, partition, or timing configuration fails fast.

### Phase 2 — Concurrent partition lanes and completion tracking

Tasks:

1. Implement one consumer controller per pod with assignment and revocation callbacks.
2. Implement assignment epochs and a lane registry keyed by topic/partition.
3. Route fetched records into bounded per-partition queues.
4. Pause at high watermark and resume at low watermark, allowing for client-buffered records.
5. Implement separate lane and record states, an ordered uncommitted tracker, and epoch/token-checked callbacks.
6. Enforce same-owner FIFO gates held through confirmed commit; fairly dispatch other owners
   within the same partition while earlier records remain unfinished.
7. Enforce partition/pod handler limits and record/byte bounds over queued, running, completed,
   and committing records; reserve fetch headroom and capacity for the earliest gap to finish.
8. Validate record key equals normalized `ownerId`; quarantine mismatch after Phase 4 lands.
9. Add owner-overlap assertion metrics and group-assignment telemetry.

Exit criteria:

- `VTX-LANE-01/02`, `VTX-OFF-01/02/03`, and `VTX-WINDOW-01` pass against real Kafka.
- Queue size never exceeds its declared buffer allowance during pause races.
- No event loop performs blocking polling or handler work.

### Phase 3 — Handler execution, TPS, and backpressure

Tasks:

1. Define `Handler` as a non-blocking `Future<Outcome>` contract.
2. Port Spec 006 reference handlers and outcome taxonomy.
3. Add a named bounded `WorkerExecutor` adapter for blocking handlers with `ordered=false`.
4. Add separate handler permits and uncommitted-window accounting; completion releases only handler capacity.
5. Port the virtual-clock-testable per-pod token bucket using Vert.x timers in production.
6. Implement zero-TPS administrative pause.
7. Implement `RUNNING`, `PAUSED`, and `PROBING` with partition and pod scopes.
8. Prevent new starts during required-output/dependency failure while keeping consumer and
   health contexts responsive.
9. Add timeouts, cancellation signals, and late-completion classification.

Exit criteria:

- Per-pod starts obey sustained TPS and burst under multi-partition load.
- `VTX-BLOCK-01` and the backpressure state tests pass with no blocked event-loop warning.
- Worker executor and transaction queue saturation produce backpressure, not unbounded work.

### Phase 4 — Transaction adapter and manual offsets

Tasks:

1. Implement immutable `TransactionCommand` with assignment epoch, required records, and
   a bounded contiguous-prefix snapshot and exact safe next offset.
2. Create one bounded single-thread transaction adapter per pod.
3. Implement initialize, begin, send, `sendOffsetsToTransaction`, commit, abort, and close.
4. Obtain consumer-group metadata through one reviewed adapter path without unsafe native
   consumer access.
5. Revalidate assignment epoch before transaction begin and immediately before commit.
6. Wire success result/lifecycle plus source offset.
7. Wire retry request/lifecycle plus source offset while preserving `ownerId` key.
8. Wire terminal result/lifecycle/DLQ plus source offset.
9. Retain the confirmed watermark on failure; resolve ambiguous broker commits and restore ledger/offset
   state before fenced replay. Reject callbacks from old epochs/execution tokens.
10. Batch all outputs for a contiguous completed prefix, split by record/byte/time limits, and
   permit only one outstanding prefix per partition; never wait for handlers inside a transaction.
11. Add failpoints around every send, group-offset operation, commit, abort, and callback.
    Include `VTX-OFF-05`: barrier-controlled failed Future and execution-verticle undeployment
    at 102, retained completions at 103/104, committed next offset 102, and both resolution
    and pod-restart replay branches. Implement tracker unit and real-Kafka integration tests.
12. Add static/fitness checks that prohibit no-argument work commit.

Exit criteria:

- `VTX-TX-01/02/03` and `VTX-OFF-01/02/03/04/05` pass with `read_committed` evidence.
- Out-of-order completion never advances past the earliest unfinished delivered record; Kafka offset gaps do not stall the tracker.
- Concurrent lane completions never create overlapping producer transactions.
- Stale assignment epochs cannot commit.

### Phase 5 — Completed-request deduplication

Tasks:

1. Create `job-execution-ledger.v1` with the work topic's partition count, compaction,
   retention, replication, minimum-ISR, schema, and ACLs.
2. Define logical identity, immutable request hash, outcome reference, expiry, and source
   coordinates.
3. Implement a partitioned embedded store and crash-safe local state directory handling.
4. Restore each assigned ledger partition to a captured end offset before lane readiness.
5. Add the completed ledger record to success/terminal transactions.
6. Suppress matching completed duplicates without invoking the handler; recheck the ledger after
   owner-gate acquisition and update local state only on confirmed prefix commit.
7. DLQ conflicting duplicates detected within the partition-local authority.
8. Implement audited expiry/tombstone production and local deletion.
9. Validate that retention exceeds maximum outage, redelivery, retry, replay, and support
   windows; document why two hours is accepted or rejected for each environment.
10. Add ledger restore, compaction, tombstone, corrupted-local-state, and disk-pressure tests.

Exit criteria:

- `VTX-DEDUP-01/02/03/04` and `VTX-TTL-01` pass.
- Pod replacement restores completed identities before processing work.
- A pod-local cache loss alone cannot cause a completed duplicate execution.
- External-effect ambiguity remains documented and measured.

### Phase 6 — Rebalance, shutdown, observability, and security

Tasks:

1. Mark epochs revoked before draining and reject all stale completion commands.
2. Resolve/abort submitted transactions on revocation; reject new stale transactions and drain/cancel
   handlers. For graceful shutdown while still assigned, commit only completed prefixes before deadline.
3. Implement readiness-first graceful shutdown within Kubernetes termination grace.
4. Add Micrometer metrics for Vert.x/JVM, lanes, queues, TPS, transactions, lag, restores,
   duplicates, and drain.
5. Add OpenTelemetry spans across consumer, Future, worker-executor, external, and
   transaction boundaries.
6. Add alerts for event-loop delay, executor/transaction queue saturation, no consumers,
   sustained lag, restore delay, all lanes paused, rebalance storm, aborts, and DLQ growth.
7. Apply least-privilege topic/group/transactional-ID ACLs and secret redaction.
8. Harden Docker/Kubernetes security contexts and state-volume limits.
9. Write `docs/spec-007-runbook.md` for pause, restore, transaction, dedup, replay, and
   rollback diagnosis.

Exit criteria:

- `VTX-REB-01` and shutdown kill tests lose no acknowledged request.
- Operators can distinguish event-loop blocking, worker saturation, Kafka failure, TPS
  throttling, hot partitions, and ledger restore.
- Security and observability review gates pass.

### Phase 7 — Performance, chaos, and migration

Tasks:

1. Run identical seeded workloads through Python and Vert.x isolated consumer groups and
   compare logical outcomes, offsets, retries, DLQs, and owner ordering.
2. Run 20,000/day plus one-hour, ten-minute, and one-minute compressed profiles.
3. Run 100,000/day plus agreed peak/recovery profiles.
4. Benchmark bounded contiguous-prefix transaction throughput, gap age, and transaction saturation.
5. Test one and two partitions per pod with concurrency 1, 2, 4, 8, and 16, using distinct-owner
   and hot-owner traffic; hold partition count fixed while measuring throughput and CPU/memory.
6. Run hot-owner, output outage, broker loss, long blocking handler, pod kill, rebalance
   storm, ledger restore, corrupt state, and state-disk pressure chaos.
7. Establish SLO evidence and choose initial pod/resource/partition/TPS configuration.
8. Rehearse production cutover and rollback with mutually exclusive runtime checks.
9. Stop Python consumers, capture offsets, start Vert.x consumers with the same group, and
   observe the rollback window.
10. Retire Python pull-worker deployment definitions after approval while retaining the
   simulator and test oracle.

Exit criteria:

- All Spec 007 scenarios and SLOs pass repeatedly.
- The selected configuration has bounded queues, no offset gaps, no owner overlap, and no
  event-loop blocking.
- Cutover and rollback evidence proves Python and Vert.x never process concurrently.

## 7. Test Matrix

| Concern | Unit | Real-Kafka integration | Chaos/load |
| --- | --- | --- | --- |
| Contracts | Java codecs and Python parity | Schema Registry compatibility | Rolling-version fixtures |
| Lanes | Record/lane states and owner gates | Within/across-partition concurrency | Hot owner and slow partition |
| Offsets | Contiguous-prefix tracker and sparse offsets | 100–105 gap and atomic-prefix proof | Crash at every boundary |
| Transactions | Adapter queue and state machine | `read_committed` atomicity | Broker loss, timeout, fencing |
| Vert.x threading | Context assertions | Responsive health during work | Blocked handler and saturated pool |
| TPS | Deterministic token bucket | Starts per pod | Burst and replica scale |
| Backpressure | Scope/state transitions | Pause/resume and bounded buffer | Output/dependency outage |
| Rebalance | Epoch invalidation | Assignment/revocation transfer | Rebalance storm and pod kill |
| Dedup | Identity/hash/expiry | Ledger restore and suppression | State loss, compaction, disk pressure |
| Docker | Config and lifecycle | Two worker containers | Resource limits and termination |
| Migration | Parity comparator | Shadow isolated group | Cutover/rollback rehearsal |
| Capacity | Queue and limiter math | 20,000 and 100,000 profiles | Hot owner, gap/window saturation, fixed-partition concurrency |

## 8. CI and Evidence

CI tiers are:

1. **Pull request:** compile, format/static analysis, unit tests, schema parity, dependency
   convergence, image build, and bounded single-broker transaction tests.
2. **Nightly:** multi-broker Kafka, all `VTX-*` integration/chaos scenarios, ledger restore,
   Docker resource tests, and 20,000-request profiles.
3. **Release:** nightly suite plus 100,000-request profiles, repeated pod/rebalance faults,
   security scans, SLO report, and migration/rollback rehearsal.

Evidence captures source revision, dependency versions, image digest/SBOM, topic settings,
assignment epochs, lane histories, fetched and committed offsets, transaction IDs/states,
read-committed outputs, event-loop delay, executor/queue high-water marks, per-pod TPS, lag,
owner overlap, duplicate decisions, ledger restore positions, JVM/container resources, and
recovery duration. Credentials and governed payload data are redacted.

## 9. Migration and Rollback

The migration unit is the worker consumer group. Shadow comparison uses a separate isolated
topic or non-side-effecting handler; it must not call production business dependencies.

Production cutover:

1. fail Python readiness and stop its dispatch;
2. wait for transactions to commit/abort and instances to leave the group;
3. record group assignments and committed offsets;
4. start Vert.x pods with the production group at controlled TPS;
5. wait for ledger restoration and readiness;
6. verify offsets continue from the captured positions; and
7. observe outcomes, duplicates, errors, lag, and event-loop health.

Rollback performs the reverse order. Vert.x must fully stop dispatch and leave the group
before Python starts. Offset rewind is not an automatic rollback because it may repeat an
external effect.

## 10. Traceability

| Specification concern | Phase | Primary proof |
| --- | ---: | --- |
| Java 21/Vert.x/Docker runtime | 0–1 | Build, image, health, and security tests |
| Concurrent per-partition execution with owner FIFO | 2 | `VTX-LANE-01/02` |
| Cross-partition concurrency | 2 | Multi-lane overlap evidence |
| Non-blocking event loops | 0, 3, 6 | `VTX-BLOCK-01` and delay metrics |
| TPS/backpressure | 3 | Rate and outage evidence |
| Exact manual offsets | 4 | `VTX-OFF-01/02/03/04/05`, `VTX-WINDOW-01` |
| Atomic Kafka completion | 4 | `VTX-TX-01/02/03` |
| Rebalance fencing | 4, 6 | `VTX-REB-01` |
| Completed deduplication | 5 | `VTX-DEDUP-01/02/03/04` |
| Retention/expiry | 5 | `VTX-TTL-01` |
| 20,000/100,000 capacity | 7 | `VTX-CAP-01/02` |
| Safe runtime replacement | 7 | Cutover and rollback evidence |

## 11. Review Gates

### Gate A — Platform and API viability

- Vert.x/Kafka versions, threading adapter, schemas, and exact-offset transaction spike are
  approved.

### Gate B — Reactive correctness

- Partition lanes, bounded queues, blocking isolation, TPS, and backpressure pass with no
  blocked event-loop evidence.

### Gate C — Kafka correctness

- Transactions, exact offsets, retries, DLQ, assignment epochs, and rebalance pass real
  Kafka failure tests.

### Gate D — Deduplication correctness

- Ledger atomicity, restore, conflict, compaction, expiry, and remaining external duplicate
  window are approved.

### Gate E — Production readiness

- Docker/Kubernetes hardening, observability, capacity, chaos, runbook, cutover, and rollback
  evidence are approved.

## 12. Definition of Done

Implementation is complete when:

1. a pinned Java 21/Vert.x 5 Maven service and hardened image exist;
2. all Spec 006 Kafka client contracts remain compatible;
3. one consumer controller per pod and concurrent coordinator with completion tracker per partition exist;
4. different owners overlap within and across partitions while same-owner records remain FIFO;
5. event loops remain non-blocking under normal, failure, and shutdown paths;
6. only bounded contiguous completed prefixes and their required outputs commit atomically;
7. per-pod TPS, bounded capacity, and pause/resume backpressure are enforced;
8. rebalance epochs fence late completions and shutdown is bounded;
9. completed duplicates are suppressed by restored Kafka-backed state within retention;
10. the external-effect duplicate limitation is tested and documented;
11. health, metrics, logs, traces, alerts, and runbooks are production-ready;
12. all unit, integration, chaos, Docker, 20,000, and 100,000 tests pass; and
13. migration replaces the Python production worker without dual processing.
