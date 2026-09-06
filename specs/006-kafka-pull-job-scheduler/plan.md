# Kafka Pull-Based Job Scheduler Implementation Plan

Status: proposed

Implements: [`spec.md`](spec.md)

Architecture: [`arc42.md`](arc42.md)

Depends on: merged Specs 002–004

## Contents

- [1. Objective](#1-objective)
- [2. Current Baseline and Gaps](#2-current-baseline-and-gaps)
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

Replace database-polled execution with a Kafka-only pull path. Clients publish immediately
eligible requests, worker pods consume with bounded concurrency and per-pod TPS, required
publication failures pause new work, and the consumer controls offsets only after durable
success, retry, or DLQ boundaries.

Delivery is split into independently reviewable phases. Topic and transaction correctness
land before handlers; deterministic rate/backpressure logic lands before Kubernetes
scaling; migration rehearsals land before legacy pollers are disabled.

## 2. Current Baseline and Gaps

The repository already provides:

- Kafka, Schema Registry, lifecycle EDRs, DLQ patterns, and producer abstractions;
- durable database-polled scheduler jobs, attempts, claims, fencing, and outbox publication;
- standalone scheduler worker, handlers, configuration, health, and metrics;
- lifecycle projection and visibility APIs;
- deterministic simulation plus real-infrastructure integration/chaos tests; and
- Kubernetes/Compose-oriented role isolation and runbooks.

The target gaps are:

| Gap | Consequence |
| --- | --- |
| No work-command topic/schema | Clients cannot submit executable work through Kafka. |
| Worker claims database rows | Execution cannot scale through Kafka partition ownership. |
| No transactional consume-transform-produce boundary | Output and source offset can diverge. |
| No Kafka-backed inbox | New-session duplicate submissions can repeat side effects. |
| No per-pod TPS gate | Replica concurrency can overload dependencies. |
| No partition pause controller | Output failures allow unsafe intake or consumer churn. |
| Retry depends on database timing | Kafka-only immediate retry semantics are absent. |
| Legacy and new paths can overlap | Cutover could execute the same job twice. |

## 3. Delivery Principles

- Preserve the existing simulator and visibility semantics.
- Treat broker acknowledgement as submission acknowledgement, never completion.
- Require stable client-generated `jobId`, `ownerId`, and `ownerType`; use canonical
  `ownerId` as the Kafka key and reject conflicting duplicates.
- Keep all work immediately eligible in Spec 006.
- Use Kafka transactions for output records plus consumed offsets.
- Treat external handler execution as at least once and require domain idempotency.
- Keep every queue, deadline, retry count, pause, and drain bounded.
- Test offset boundaries with real Kafka and deterministic failpoints.
- Make the old and new execution paths mutually exclusive during migration.

## 4. Target Repository Shape

Names may evolve, but the intended shape is:

```text
src/job_visibility/
  pull_scheduler/
    contracts.py                 work, result, retry, and DLQ envelopes
    producer.py                  shared direct-producer adapter
    consumer.py                  poll/assignment/pause loop
    worker.py                    dispatch and outcome orchestration
    offsets.py                   per-partition contiguous offset tracker
    owner_gate.py                one active handler per ownerId
    rate_limit.py                virtual-clock-compatible token bucket
    backpressure.py              RUNNING/PAUSED/PROBING controller
    inbox.py                     Kafka-backed idempotency state
    transactions.py              output plus source-offset transaction
    config.py                    validated timing/topic/TPS configuration
    health.py                    readiness/liveness and metrics
  applications/
    submission_api.py            optional stateless Kafka-producing API
  cli/
    pull_worker.py               worker process entry point
infra/
  kafka/
    schemas/                     request/result/DLQ schemas
    topics/                      topic policies and ACL declarations
tests/
  pull_scheduler/
  integration/
    test_pull_scheduler_kafka.py
    test_pull_scheduler_failures.py
    test_pull_scheduler_migration.py
docs/
  spec-006-runbook.md
  spec-006-evidence.md
specs/006-kafka-pull-job-scheduler/
  spec.md
  arc42.md
  plan.md
```

## 5. Delivery Sequence

```text
Phase 0  Contracts, decisions, and executable test fixtures
Phase 1  Topic, schema, security, and producer path
Phase 2  Consumer foundation and manual offset tracking
Phase 3  Kafka transactions and Kafka-backed idempotency
Phase 4  TPS limiting, bounded concurrency, and backpressure
Phase 5  Retry, DLQ, rebalance, and shutdown safety
Phase 6  Observability, deployment, load, and resilience evidence
Phase 7  Migration, rollback rehearsal, and legacy retirement
```

Phases 2 and producer SDK work in Phase 1 may proceed independently after contracts freeze.
No production handler traffic moves until Phases 0–6 pass their gates.

## 6. Detailed Phase Plan

### Phase 0 — Contracts and executable decisions

Tasks:

1. Freeze topic names, consumer group, canonical `ownerId` key encoding, `ownerType`
   semantics, and partition-count assumptions.
2. Define a deterministic owner-selection rule per workload and forbid mixing subscriber
   and business-group scopes for the same ordering requirement.
3. Define versioned request, result, lifecycle-reference, retry, and DLQ models.
4. Define immutable-field comparison and `(jobId, attempt)` identity rules.
5. Define the failure taxonomy: success, retryable, permanent, exhausted, and conflict.
6. Define exact safe commit boundaries and failpoint names around each boundary.
7. Decide the Kafka state-store implementation and restore/readiness contract.
8. Define timing-budget relationships for poll, session, handler, transaction, and shutdown.
9. Add serialization, compatibility, `ownerId` normalization/key-selection, ownership, and
   error-classification unit tests.

Exit criteria:

- Every acceptance scenario has a named durable boundary and expected offset.
- Schema compatibility checks run locally and in CI.
- No contract contains future-delivery semantics.
- Tests prove namespace-safe keys and reject mixed owner selection for one workload.

### Phase 1 — Topics, security, and producer path

Tasks:

1. Add declarative configuration for work, result, inbox/changelog, lifecycle, and DLQ
   topics with partitions, replication, retention, and cleanup policy.
2. Add least-privilege producer, worker, visibility, and DLQ-operator ACL definitions.
3. Implement a shared producer configured for idempotence, required acknowledgements,
   retries, delivery timeout, schema validation, canonical `ownerId` keying, and payload
   bounds.
4. Return success only after broker acknowledgement and expose ambiguous timeout semantics.
5. Add duplicate same-session/new-session tests plus rejection of null, `jobId`,
   inconsistently normalized, or client-selected partition keys.
6. Implement an optional stateless HTTP submission adapter using the same producer library.
7. Add startup validation that rejects missing topics, incompatible schemas, or unsafe
   producer settings in non-local environments.

Exit criteria:

- A client can publish and receive a broker-backed submission acknowledgement.
- Invalid schema, missing key, oversized payload, and unauthorized topic writes fail safely.
- The submission path has no scheduler-database dependency.

### Phase 2 — Consumer foundation and manual offsets

Tasks:

1. Implement a consumer with auto-commit disabled and `read_committed` isolation.
2. Add bounded poll batches, per-partition queues, and global/per-partition concurrency.
3. Implement assignment and revocation callbacks with ownership generation/fencing state.
4. Implement the contiguous offset tracker; never advance over unfinished earlier records.
5. Implement a bounded keyed execution gate allowing at most one active handler per
   `ownerId`, even when multiple records from its partition have been fetched.
6. Separate polling/heartbeats from handler dispatch so throttling does not exceed the poll
   interval.
7. Add deterministic tests for fetch, queue, owner blocking, completion, out-of-order completion,
   revoke, and commit transitions.
8. Add a no-side-effect handler to prove multi-pod partition sharing and owner affinity
   against real Kafka.

Exit criteria:

- No offset is committed at fetch, queue, or start.
- Concurrent completion commits only the highest contiguous safe offset.
- Each partition has at most one active group owner.
- Two jobs with the same `ownerId` never execute concurrently; different owners sharing a
  partition may do so safely.

### Phase 3 — Transactions and idempotency

Tasks:

1. Give each pod/process generation a unique transactional producer ID.
2. Implement begin, produce outputs, send group offsets, commit, and abort operations.
3. Fence stale producer generations and classify transaction timeout/fencing failures.
4. Implement Kafka-backed inbox/result state restoration keyed by `(jobId, attempt)`.
5. Block readiness for assigned partitions until required state is restored.
6. Return the recorded outcome for matching duplicates and quarantine conflicting immutable
   duplicates.
7. Thread a stable operation idempotency key into every handler contract.
8. Adapt built-in handlers to demonstrate idempotent or conditional side effects.

Exit criteria:

- A crash before transaction commit exposes neither outputs nor advanced offsets to
  `read_committed` consumers.
- A committed transaction exposes outputs and offset together.
- Replayed work and restored pods produce one logical outcome.

### Phase 4 — TPS, concurrency, and backpressure

Tasks:

1. Implement a monotonic/virtual-clock token bucket with sustained TPS and burst settings.
2. Acquire a token immediately before handler start and expose effective runtime settings.
3. Make zero TPS an administrative pause and reject inconsistent TPS/burst configuration.
4. Implement `RUNNING`, `PAUSED`, and `PROBING` with failure and recovery thresholds.
5. Pause affected partitions on required result/lifecycle/retry/DLQ publication failure.
6. Continue heartbeat polls while paused and prevent new handler starts.
7. Add bounded jittered recovery probes and readiness behavior for sustained pause.
8. Emit queue, token, wait, pause reason/duration, and state-transition metrics.

Exit criteria:

- Per-pod measured starts respect sustained TPS and burst under a virtual clock and load.
- Publication failure starts no new work, commits no affected offset, and does not cause
  avoidable group churn.
- Recovery resumes and drains backlog without manual offset changes.

### Phase 5 — Retry, DLQ, rebalance, and shutdown

Tasks:

1. Implement bounded immediate retry by publishing `attempt + 1` with immutable fields and
   the original serialized `ownerId` key.
2. Publish retry/lifecycle plus source offset in one Kafka transaction.
3. Implement the governed DLQ envelope with original coordinates and sanitized failure.
4. Publish terminal result/lifecycle/DLQ plus source offset in one transaction.
5. Deduplicate DLQ records by original topic/partition/offset.
6. Implement cooperative bounded draining on revocation and signal shutdown.
7. Stop readiness before drain and fit the drain budget within Kubernetes termination grace.
8. Add failpoints before/after external effect, each output send, transaction commit, and
   revocation.
9. Add audited manual replay tooling that emits a new work record with the original
   `ownerId` key without editing history.

Exit criteria:

- Retry and DLQ offsets never advance without their complete required output set.
- Forced termination and rebalance lose no acknowledged work.
- Exhausted poison records cannot block a partition indefinitely.

### Phase 6 — Operations, deployment, and evidence

Tasks:

1. Add pull-worker CLI/configuration and Compose/Kubernetes deployment definitions.
2. Add liveness, readiness, startup, and graceful-termination probes.
3. Add lag/age, assignments, transaction, offset, TPS, pause, retry, DLQ, restore, and
   duplicate metrics.
4. Add trace propagation using job, attempt, topic, partition, offset, and worker identity.
5. Add alerts for no consumers, sustained lag, all pods paused, rebalances, transaction
   aborts, DLQ growth, state restore delay, and partition skew.
6. Load-test partitions, maximum replicas, skewed keys, realistic handler latency, TPS, and
   autoscaling changes.
7. Run broker outage, network ambiguity, pod kill, rebalance storm, and output-topic outage
   scenarios.
8. Write `docs/spec-006-runbook.md` and evidence index, including pause/resume, DLQ inspect,
   authorized replay, and transaction diagnosis.

Exit criteria:

- Operators can distinguish throttling, capacity saturation, key skew, dependency failure,
  state restore, and broker failure.
- Load evidence demonstrates the selected partition/replica/TPS envelope.
- Every recovery scenario is bounded and retains sanitized evidence.

### Phase 7 — Migration and retirement

Tasks:

1. Inventory all database jobs as unclaimed, claimed, completed, retry-wait, or terminal.
2. Deploy topics and inactive consumers; validate ACLs, schemas, dashboards, and alerts.
3. Stop legacy workers from taking new claims and drain/recover current claims.
4. Build a one-time migration producer with a durable manifest of database identity to
   Kafka topic/partition/offset acknowledgement.
5. Reconcile manifest entries against source eligibility before enabling new consumers.
6. Start consumers at controlled TPS and compare results/lifecycle visibility with the
   manifest.
7. Switch clients to direct Kafka or the stateless adapter and reject legacy submissions.
8. Exercise rollback without allowing old and new paths to own the same population.
9. Observe for the agreed period, then separately approve database retention/removal.
10. Update root architecture, README, runbooks, and evidence to mark the new current state.

Exit criteria:

- Migration reconciliation accounts for every eligible legacy job exactly once logically.
- No interval permits concurrent old-poller and new-consumer ownership.
- The steady-state Spec 006 runtime has no scheduler-database connection or credentials.

## 7. Test Matrix

| Area | Unit | Integration | Failure/load proof |
| --- | --- | --- | --- |
| Contracts/keying | Schema, immutable fields, canonical owner keys | Registry compatibility and same-owner affinity | Partition expansion/owner-skew review |
| Producer | Ack/timeout mapping | Real broker acknowledgement | Ambiguous timeout and retry |
| Offsets | Contiguous tracker | Multi-partition manual commits | Crash/revoke at every boundary |
| Transactions | State machine | `read_committed` atomic visibility | Abort, timeout, fencing |
| Idempotency | Duplicate/conflict reducer | State restore and reassignment | Crash after external effect |
| Owner serialization | Keyed gate | Same owner never overlaps | Hot-owner load and rebalance |
| TPS | Virtual-clock token bucket | Per-pod measured rate | Replica scale and bursts |
| Backpressure | Transition model | Pause with heartbeat polls | Output-topic outage/recovery |
| Retry/DLQ | Classification/envelope | Transactional handoff | Poison, exhaustion, DLQ outage |
| Migration | Manifest/reconciliation | Legacy snapshot to Kafka | Cutover and rollback rehearsal |

## 8. CI and Evidence

CI tiers are:

1. **Pull request:** unit tests, schema compatibility, static config/ACL validation, and a
   bounded single-broker transaction suite.
2. **Nightly:** multi-broker Kafka, multi-pod ownership, rebalance, state restore, output
   outage, pod-kill, and TPS/load scenarios.
3. **Release:** nightly suite plus repeated migration/rollback rehearsal and representative
   capacity evidence.

Evidence includes effective topic configuration, schema versions, consumer assignments,
committed offsets, transaction state, result/DLQ coordinates, per-pod TPS measurements,
pause transitions, lag/age, state restore time, process restarts, and recovery duration.
Credentials and governed payload fields are redacted.

## 9. Migration and Rollback

The cutover unit is an explicitly reconciled population, not a time window inferred from
logs. Legacy claim acquisition is disabled before migration publication. The manifest
records each source job and its acknowledged Kafka coordinates; rerunning the migration
uses the same `jobId` and is duplicate-safe.

Rollback stops new consumer dispatch first and drains or abandons in-flight Kafka work.
Legacy workers may restart only for a population proven not to have a Kafka logical
outcome. Offset rewinds alone are not rollback because they can repeat external effects.
Database deletion is never part of automated cutover.

## 10. Traceability

| Specification concern | Delivery phase | Primary proof |
| --- | --- | --- |
| Kafka topic and owner partitioning | 0–2 | Canonical key, affinity, and non-overlap tests |
| Direct client producer | 1 | Broker ack, auth, duplicate tests |
| Pull pods/manual commit | 2 | Offset and ownership tests |
| Kafka-backed idempotency | 3 | Restore, duplicate, conflict tests |
| Transactional outputs/offsets | 3, 5 | Crash and `read_committed` tests |
| Per-pod TPS | 4 | Virtual-clock and load measurements |
| Update-failure backpressure | 4 | Pause/heartbeat/recovery test |
| Retry and DLQ | 5 | Transactional handoff/failure tests |
| Rebalance/shutdown | 5 | Forced revoke/termination tests |
| Observability/operations | 6 | Alert and runbook evidence |
| Database-free migration | 7 | Manifest reconciliation and credential audit |

## 11. Review Gates

### Gate A — Contract approval

- Immediate-only semantics, keys, schemas, retry classification, and commit boundaries are
  approved by client, worker, platform, visibility, and security owners.

### Gate B — Kafka correctness

- Transactions, manual offsets, inbox restoration, duplicate behavior, and fencing pass
  real-Kafka fault tests.

### Gate C — Flow control

- TPS, concurrency, backpressure, readiness, and heartbeat behavior pass deterministic and
  outage tests.

### Gate D — Operational readiness

- Capacity evidence, alerts, dashboards, ACLs, runbooks, and replay controls are reviewed.

### Gate E — Cutover authorization

- Migration and rollback rehearsals reconcile every job and prove mutually exclusive
  execution ownership.

## 12. Definition of Done

Implementation is complete when:

1. clients publish schema-valid, keyed work and receive broker-backed acknowledgement;
2. the production pull runtime has no scheduler database, poller, due dispatcher, or work
   outbox dependency;
3. workers manually control source offsets with auto-commit disabled;
4. required Kafka outputs and offsets commit atomically;
5. matching duplicates converge and conflicting duplicates are quarantined;
6. all retries and DLQ replays preserve canonical `ownerId`, and no two jobs for the same
   owner execute concurrently;
7. handlers demonstrate durable external-side-effect idempotency;
8. every pod enforces configured TPS, burst, concurrency, and queue limits;
9. output failure pauses new work while consumer heartbeats continue;
10. retry, DLQ, rebalance, shutdown, and crash scenarios lose no acknowledged work;
11. status and lifecycle remain observable through Kafka-derived projections;
12. CI and release evidence cover the complete matrix; and
13. migration, rollback, documentation, and legacy retirement are approved and reproducible.
