# Production Hardening and Resilience Implementation Plan

Status: proposed  
Implements: [`spec.md`](spec.md)  
Depends on: completion of Spec 002 on the branch from which this work is based

## Contents

- [1. Objective](#1-objective)
- [2. Current Baseline](#2-current-baseline)
- [3. Delivery Principles](#3-delivery-principles)
- [4. Target Repository Shape](#4-target-repository-shape)
- [5. Delivery Sequence](#5-delivery-sequence)
- [6. Detailed Phase Plan](#6-detailed-phase-plan)
- [7. Test Matrix](#7-test-matrix)
- [8. CI and Evidence Plan](#8-ci-and-evidence-plan)
- [9. Rollout and Compatibility](#9-rollout-and-compatibility)
- [10. Traceability](#10-traceability)
- [11. Review Gates](#11-review-gates)
- [12. Definition of Done](#12-definition-of-done)

## 1. Objective

Close the production-readiness gaps in `docs/chaos.md` without changing the durable
architecture established by Spec 002. Deliver bounded dependency behavior, deterministic
infrastructure failure tests, independent PostgreSQL outage testing, and release evidence
with declared workloads and objectives.

The implementation order deliberately proves correctness before performance. Load results
are not meaningful release evidence while timeout, duplicate, fencing, or recovery behavior
remains unverified.

## 2. Current Baseline

The Spec 002 branch already provides:

- durable scheduler and EDR schemas with separate roles and migration histories;
- transactional outbox publication through Kafka and Kafka Connect;
- an immutable EDR journal and replayable projection;
- Cassandra conditional reservation/finalization with stable operation identity;
- Toxiproxy in the local composition;
- unit and deterministic scenario tests; and
- recorded manual Phase 9 evidence and an operations runbook.

The implementation plan must close these observed gaps:

| Baseline gap | Consequence |
| --- | --- |
| PostgreSQL exposes only pool acquisition tuning | Connect, lock, statement, and transaction waits are not uniformly bounded. |
| Kafka timeout behavior is mostly library-defaulted | Broker outage duration and shutdown behavior are not an explicit contract. |
| Declared integration pytest markers have no infrastructure suites | CI does not prove the live recovery claims. |
| Scheduler and EDR databases share one container | One database authority cannot be stopped independently. |
| Several Cassandra cases are manually demonstrated or conceptual | Unknown outcomes, lease loss, and races lack repeatable evidence. |
| CI runs one unit/static job | Outage, restart, and DLQ regressions can merge unnoticed. |
| No versioned representative workload | Freshness and capacity targets cannot be evaluated consistently. |

The unrelated deterministic simulator and its existing result files must remain intact.

## 3. Delivery Principles

- Add configuration with safe finite defaults and backwards-compatible environment names.
- Introduce the smallest failpoints needed to establish exact pre/post-commit boundaries;
  failpoints are test-only and unavailable in production mode.
- Use pytest fixtures and service APIs rather than ad hoc shell sleeps.
- Keep each infrastructure test independently rerunnable and diagnosable.
- Make infrastructure cleanup label-scoped and preserve diagnostics before teardown.
- Land vertical scenario slices: trigger, intermediate assertions, recovery, final assertion,
  and CI evidence together.
- Do not weaken uniqueness, fencing, immutability, or ownership rules to make tests pass.

## 4. Target Repository Shape

The precise names may change during implementation, but the repository should converge on:

```text
compose.yaml                         local development profile
compose.test.yaml                    isolated scheduler/EDR databases and test overrides
src/job_visibility/
  config.py                          validated dependency deadlines
  observability/                     queue diagnostics and artifact-safe snapshots
  testing/                           optional test-only failpoint protocol
tests/
  integration/
    conftest.py                      bounded service fixtures and run isolation
    test_postgres_timeouts.py
    test_scheduler_concurrency.py
    test_kafka_connect.py
    test_cassandra_chaos.py
  e2e/
    test_failure_matrix.py
    test_restart_matrix.py
    test_projection_rebuild.py
  performance/
    workloads/representative-v1.json
    test_representative_load.py
scripts/
  infra                              single documented developer/CI interface
  collect-test-evidence              redacted diagnostic collection
simulation-results/                  existing deterministic results, unchanged
resilience-results/                  timestamped resilience/performance evidence
docs/
  chaos.md
  spec-003-runbook.md
  spec-003-evidence.md
```

Generated evidence should normally be a CI artifact. Only reviewed summaries and small
machine-readable baselines should be committed.

## 5. Delivery Sequence

```text
Phase 0  Testability spikes and exact fault boundaries
Phase 1  Finite PostgreSQL and Kafka deadlines
Phase 2  Independent database composition and harness foundations
Phase 3  PostgreSQL, projection, and restart correctness
Phase 4  Cassandra timeout, unknown-outcome, lease, and conflict correctness
Phase 5  Kafka, Connect, DLQ, and outage recovery
Phase 6  Full failure/restart matrix and diagnostics
Phase 7  Representative load, recovery objectives, and release evidence
```

Phases 1 and 2 may proceed in parallel after Phase 0 decisions are recorded. Phase 7 starts
only after correctness gates A through D pass.

## 6. Detailed Phase Plan

### Phase 0 — Testability spikes and exact fault boundaries

Tasks:

1. Determine how to pause/crash the publisher after broker acknowledgement and before the
   outbox commit without relying on a race-prone sleep.
2. Prototype a proxy or Cassandra test hook that drops the final write response only after
   the server accepts the mutation.
3. Add a barrier that lets two workers read the same checksum before either reserves it.
4. Define projection pre-commit and post-commit crash barriers.
5. Verify the chosen mechanisms expose no production endpoint and are rejected outside test
   mode.
6. Record whether each mechanism is a transport control, process signal, or test failpoint,
   and why it reaches the required boundary.

Exit criteria:

- `WORKER-05`, `WORKER-06`, `KAFKA-01`, and `PROJ-01` each have a deterministic trigger
  design and a minimal executable spike.
- No acceptance scenario depends on an arbitrary timing window.

### Phase 1 — Finite PostgreSQL and Kafka deadlines

Tasks:

1. Extend each database role's typed configuration with connect, statement, lock,
   idle-in-transaction, and transaction deadlines.
2. Apply connection/session settings consistently in engine/session creation.
3. Classify timeout and lock errors without leaking statements or payloads.
4. Extend Kafka configuration with socket, request, delivery, metadata, and flush deadlines.
5. Add Schema Registry connect/read deadlines.
6. Validate cross-setting relationships and document defaults.
7. Add unit tests for parsing, bounds, invalid relationships, and secret-safe errors.
8. Add live tests that hold a lock, exceed a statement deadline, refuse a connection, and
   withhold broker acknowledgement.

Exit criteria:

- No relevant dependency operation relies on an unreviewed infinite/default wait.
- Live timeout tests complete within their outer test deadline and leave reusable pools and
  unpublished outbox rows in the expected state.

### Phase 2 — Independent database composition and harness foundations

Tasks:

1. Split the acceptance profile into `scheduler-postgres` and `edr-postgres`, each with its
   own volume, port, health check, owner, and runtime roles.
2. Make migrations and application processes consume distinct service URLs.
3. Retain an optional single-server lightweight development profile if it remains useful.
4. Implement a unique Compose project/run identifier and resource labels.
5. Add bounded readiness polling for PostgreSQL, Kafka, Registry, Connect, Cassandra,
   Toxiproxy, and application health endpoints.
6. Add fixtures for toxics, process lifecycle, connector status, database inspection, Kafka
   offsets, and unique Cassandra datasets.
7. Ensure fixture cleanup removes toxics even after assertion failure and deletes only the
   current run's resources.
8. Provide one command interface for up, ready, migrate, connector apply, test tiers,
   diagnostics, and down.

Exit criteria:

- Either PostgreSQL container can be stopped while the other remains healthy.
- A deliberately timed-out fixture prints last-observed state and component diagnostics.
- Two sequential test runs do not collide or inherit mutable data/toxics.

### Phase 3 — PostgreSQL, projection, and restart correctness

Tasks:

1. Automate `PERSIST-01` across API, worker, publisher, and projector restarts.
2. Automate `PERSIST-02` with more eligible jobs than one combined batch.
3. Prove scheduler claim, attempt, and outbox atomicity under timeout/rollback.
4. Automate `PROJ-01` at the pre-commit crash boundary.
5. Port deterministic out-of-order cases to durable adapters for `PROJ-02`.
6. Automate isolated journal-only rebuild equivalence for `PROJ-03`.
7. Automate both directions of `ISOLATION-01` and assert that no fallback query crosses the
   ownership boundary.
8. Add recovery cases for expired claims, outbox leases, and projection work.

Exit criteria:

- PostgreSQL and projection merge-gating scenarios pass repeatedly from clean and reused
  infrastructure.
- Every scenario asserts intermediate durable state as well as final API state.

### Phase 4 — Cassandra resilience correctness

Tasks:

1. Automate deterministic seed/setup and normal `CASSANDRA_FIB_UPDATE` execution.
2. Automate `WORKER-02`, including future `available_at` and no-early-reclaim assertions.
3. Automate `WORKER-03` with exactly `max_attempts` and no terminal eligibility.
4. Automate `WORKER-04` with actual response delay, heartbeat/lease loss, replacement claim,
   and stale-token rejection.
5. Implement the Phase 0 post-finalization response-loss trigger and automate `WORKER-05`.
6. Implement the same-checksum barrier and automate `WORKER-06` with two distinct stable
   operation IDs.
7. Assert selected row, operation markers, checksums, scheduler attempts, retry EDRs, and
   final visibility for each scenario.
8. Cover client timeout, unavailable, connection loss, conditional conflict, and whole
   execution deadline as distinct classifications.

Exit criteria:

- Each logical operation increments the checksum at most once under response loss and
  retries.
- The race test proves it exercised a conditional conflict.
- Stale ownership cannot commit either success or failure.

### Phase 5 — Kafka, Connect, DLQ, and outage recovery

Tasks:

1. Automate the publisher acknowledgement crash window for `KAFKA-01`.
2. Stop the broker for `KAFKA-02`; assert scheduler commits and outbox growth, restart it,
   then assert lossless drain and record recovery time.
3. Stop only EDR PostgreSQL for `SINK-01`; assert Kafka retention, stale `dataAsOf`, and full
   catch-up after restart.
4. Automate converter poison and identity collision cases for `SINK-02`/`SINK-03`.
5. Assert that later valid records progress after bounded connector error handling.
6. Test Connect restart with committed and uncommitted batches.
7. Verify topic, partition, offset, event identity, and projection checkpoint uniqueness.
8. Add configurable backlog/recovery thresholds and readiness diagnostics.

Exit criteria:

- Broker and EDR outages lose no committed facts.
- Duplicate delivery produces one immutable journal row and one applied checkpoint.
- DLQ cases are observable without exposing prohibited message bodies.

### Phase 6 — Full matrix, CI tiers, and diagnostics

Tasks:

1. Map every Spec 002 section 15 scenario to a test node ID and tier.
2. Complete the process/dependency pre/post-commit restart matrix and document consolidated
   equivalent boundaries.
3. Split CI into static/unit, migration/PostgreSQL, Kafka/Connect, Cassandra, end-to-end,
   and nightly resilience jobs.
4. Give every job an outer deadline and every poll an inner deadline.
5. Collect sanitized configuration, logs, health, active toxics, connector tasks, queue
   snapshots, offsets, and relevant identifiers before teardown on failure.
6. Upload original failed-attempt evidence when CI retries a job.
7. Update the runbook with exact local commands and recovery expectations.
8. Replace future-tense entries in `docs/chaos.md` with links to automated evidence only
   after the associated tests pass.

Exit criteria:

- The traceability table contains no unassigned Spec 002 failure scenario.
- A deliberately broken scenario produces sufficient artifacts to identify the last durable
  fact without interactive access to the runner.

### Phase 7 — Representative load and release evidence

Tasks:

1. Create and review `representative-v1` with fixed job mix, payload distribution, arrival
   pattern, duration, concurrency, Cassandra parameters, and runner resources.
2. Instrument scheduler, outbox, sink, projection, API, and Cassandra measurements required
   by the specification.
3. Run baseline throughput/freshness tests and record p50/p95/p99.
4. Run broker-outage backlog accumulation and recovery; measure time and drain rate.
5. Run a bounded soak that includes controlled dependency interruption and recovery.
6. Select and document the percentile for the 10-second healthy freshness objective and the
   broker recovery objective before the release run.
7. Tune initial batches, pools, concurrency, topic partitions, and alert thresholds from
   evidence; record each changed assumption.
8. Write timestamped JSON plus a concise Markdown summary under `resilience-results/` without
   modifying `simulation-results/`.

Exit criteria:

- The reviewed workload is reproducible from one documented command.
- Both release objectives pass; otherwise the release gate remains blocked with an owner and
  measured gap.

## 7. Test Matrix

| Test ID | Spec 002 scenario | Primary phase | PR | Nightly | Release |
| --- | --- | ---: | :---: | :---: | :---: |
| `R-PERSIST-01` | `PERSIST-01` | 3 | Yes | Yes | Yes |
| `R-PERSIST-02` | `PERSIST-02` | 3 | Yes | Yes | Yes |
| `R-WORKER-01` | `WORKER-01` | 4 | Yes | Yes | Yes |
| `R-WORKER-02` | `WORKER-02` | 4 | Yes | Yes | Yes |
| `R-WORKER-03` | `WORKER-03` | 4 | Yes | Yes | Yes |
| `R-WORKER-04` | `WORKER-04` | 4 | No | Yes | Yes |
| `R-WORKER-05` | `WORKER-05` | 4 | No | Yes | Yes |
| `R-WORKER-06` | `WORKER-06` | 4 | No | Yes | Yes |
| `R-KAFKA-01` | `KAFKA-01` | 5 | Yes | Yes | Yes |
| `R-KAFKA-02` | `KAFKA-02` | 5 | No | Yes | Yes |
| `R-SINK-01` | `SINK-01` | 5 | No | Yes | Yes |
| `R-SINK-02` | `SINK-02` | 5 | No | Yes | Yes |
| `R-SINK-03` | `SINK-03` | 5 | No | Yes | Yes |
| `R-PROJ-01` | `PROJ-01` | 3 | Yes | Yes | Yes |
| `R-PROJ-02` | `PROJ-02` | 3 | Yes | Yes | Yes |
| `R-PROJ-03` | `PROJ-03` | 3 | Yes | Yes | Yes |
| `R-ISOLATION-01` | `ISOLATION-01` | 3 | No | Yes | Yes |

PR cases may be separated into required jobs so infrastructure starts in parallel. A test
may move from nightly to PR when its median duration and reliability fit the PR budget; it
must not move in the other direction merely because it exposes a regression.

## 8. CI and Evidence Plan

### 8.1 Jobs

1. `quality`: formatting, lint, unit, deterministic scenarios, configuration tests.
2. `postgres-integration`: two databases, migrations, locks, atomicity, isolation roles.
3. `kafka-integration`: producer, schema, Connect, sink, duplicate, crash window.
4. `cassandra-integration`: deterministic workload and PR worker scenarios.
5. `e2e`: durable path, projection/rebuild, selected restart cases.
6. `nightly-resilience`: complete failure and restart matrices.
7. `release-performance`: representative load, recovery, and soak.

Infrastructure versions must remain pinned. Jobs should cache dependencies but never reuse
mutable database volumes across CI runs.

### 8.2 Evidence manifest

Every infrastructure run emits a manifest containing:

- commit, workflow/run identifier, UTC start/end, and test seed;
- dependency image versions and sanitized configuration hash;
- selected test nodes and result;
- artifact names and checksums; and
- workload profile and resource limits for performance runs.

The manifest and summary may be committed for a release candidate. Raw logs remain CI
artifacts according to the retention policy.

## 9. Rollout and Compatibility

New timeout environment variables receive finite local defaults. Deployment manifests must
set reviewed production values before enabling strict production validation. Roll out in
this order:

1. ship parsing and metrics without changing existing explicit deployment values;
2. enable PostgreSQL session deadlines one role at a time;
3. enable Kafka delivery/flush deadlines and watch unpublished age;
4. deploy queue-age readiness thresholds initially in report-only mode;
5. enable readiness gating after thresholds are validated under representative load.

Rollback may restore the previous application version, but must not remove durable outbox,
journal, or projection data. Timeout rollback changes configuration only. The split local
database volumes are test/development resources; migration histories remain unchanged.

## 10. Traceability

| Spec 003 requirement | Delivery phase | Evidence |
| --- | ---: | --- |
| Finite PostgreSQL/Kafka deadlines | 1 | Unit plus live timeout tests |
| Independent scheduler/EDR failures | 2, 3 | `R-ISOLATION-01` |
| Automated Spec 002 matrix | 3–6 | Test matrix and CI manifest |
| Cassandra post-write response loss | 0, 4 | `R-WORKER-05` |
| Cassandra synchronized conflict | 0, 4 | `R-WORKER-06` |
| Kafka outage and recovery objective | 5, 7 | `R-KAFKA-02` and release results |
| Redacted failure diagnostics | 2, 6 | Deliberate-failure artifact review |
| p95/p99 freshness and throughput | 7 | Versioned workload result |
| Updated runbook and chaos status | 6, 7 | Documentation review |

## 11. Review Gates

### Gate A — Specification and fault-boundary approval

- Spec 003 scope and objective definitions are approved.
- Exact triggers for the four difficult commit-boundary/race scenarios are demonstrated.
- Test-only controls have an accepted production-exclusion design.

### Gate B — Bounded dependencies and harness

- Timeout defaults and validation are reviewed.
- Two-database composition and bounded fixtures pass.
- Cleanup and artifact redaction receive a destructive-action/security review.

### Gate C — Correctness resilience

- PR correctness subset passes without retry.
- Cassandra exactly-once logical effect and stale fencing are proven.
- Kafka duplicate, projection replay, and ownership isolation are proven.

### Gate D — Full automated matrix

- Every Spec 002 section 15 scenario has passing automated evidence.
- Nightly restart matrix passes.
- Failure artifacts identify the last durable fact.

### Gate E — Release evidence

- Representative workload and objectives were fixed before execution.
- Freshness, throughput, backlog recovery, and soak targets pass.
- Runbook, chaos documentation, evidence summary, and initial tuning are reviewed.

## 12. Definition of Done

- All Spec 003 acceptance criteria are satisfied.
- Unit, integration, end-to-end, nightly resilience, and release workflows are documented and
  reproducible.
- Tests use real PostgreSQL, Kafka/Connect, and Cassandra where the specification requires
  those failure surfaces.
- No required scenario relies solely on the prior manual Phase 9 evidence.
- All dependency and polling waits are finite and diagnostics are actionable.
- Scheduler and EDR outage isolation is proven using separate containers.
- Unknown Cassandra outcomes and concurrent updates preserve one logical effect per operation.
- Results are timestamped, tied to a workload and commit, and do not overwrite simulator
  evidence.
- Documentation accurately distinguishes implemented guarantees, automated evidence,
  production-topology validation, and remaining out-of-scope risks.
