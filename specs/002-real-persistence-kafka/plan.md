# Real Persistence and Kafka Implementation Plan

Status: proposed for review  
Architecture basis: [`arc42.md`](arc42.md)  
Governing specification: [`spec.md`](spec.md)  
Depends on: [`../001-scheduled-job-visibility/plan.md`](../001-scheduled-job-visibility/plan.md)

## Contents

- [1. Objective](#1-objective)
- [2. Planning Principles](#2-planning-principles)
- [3. Current Baseline](#3-current-baseline)
- [4. Target Technology and Repository Shape](#4-target-technology-and-repository-shape)
- [5. Delivery Sequence](#5-delivery-sequence)
- [6. Detailed Phase Plan](#6-detailed-phase-plan)
- [7. Database Migration Plan](#7-database-migration-plan)
- [8. Kafka and Connector Delivery Plan](#8-kafka-and-connector-delivery-plan)
- [9. Test Plan](#9-test-plan)
- [10. CI and Developer Workflow](#10-ci-and-developer-workflow)
- [11. Rollout and Recovery Plan](#11-rollout-and-recovery-plan)
- [12. Traceability](#12-traceability)
- [13. Review Gates](#13-review-gates)
- [14. Definition of Done](#14-definition-of-done)

## 1. Objective

Implement the Spec 002 architecture without regressing the deterministic behavior from
Spec 001.

The delivered system will:

- Store scheduler jobs, attempts, claims, and outbox records in a scheduler PostgreSQL
  database.
- Publish every scheduler-originated lifecycle EDR through a transactional outbox to Kafka.
- Persist raw Kafka EDRs into a different PostgreSQL database through Kafka Connect JDBC
  Sink.
- Project persisted EDRs into durable job visibility, attempt, decision, and finding tables.
- Serve the existing visibility API from those durable projections.
- Run a Cassandra workload job that reads reproducible `X` records, finds the greatest
  number, computes Fibonacci, delays, conditionally updates that record/checksum, and turns
  real read/write failures into scheduler retry behavior.
- Survive duplicate delivery, reordering, component restart, and temporary dependency
  outages.
- Preserve infrastructure-free unit and simulation tests while adding real PostgreSQL,
  Kafka, Schema Registry, Kafka Connect, Cassandra, and network-chaos integration tests.

Implementation does not start until the Spec 002 documents have been reviewed. Proposed
architecture decisions ADR-002-04, ADR-002-06, ADR-002-07, ADR-002-10, ADR-002-12,
ADR-002-13, and ADR-002-14 are validated at the review gates before broad dependent changes
are made.

## 2. Planning Principles

1. **Prove the riskiest connector path first.** Validate JSON Schema, JSONB, metadata SMTs,
   upsert immutability, and DLQ behavior before building repositories around assumptions.
2. **Keep one durable boundary per transaction.** Scheduler changes plus outbox commit in
   the scheduler DB; projections plus checkpoints commit in the EDR DB.
3. **Extract before replacing.** Characterize and isolate the existing lifecycle reducer,
   then add PostgreSQL adapters without reimplementing its rules.
4. **Deliver vertical recovery slices.** Each infrastructure phase includes restart,
   duplicate, and failure tests instead of deferring resilience to the end.
5. **Keep database ownership visible.** Separate migration trees, roles, URLs, repository
   packages, and tests enforce the boundary.
6. **Prefer bounded work.** Scheduler claims, outbox leases, sink batches, projection scans,
   retries, and shutdown waits all have explicit limits.
7. **Do not hide eventual consistency.** API and diagnostics expose freshness and pipeline
   lag; tests poll for outcomes with deadlines rather than assume immediate projection.
8. **Do not claim production readiness without measurements.** Load targets, retention,
   partition counts, and pool sizes remain configurable until representative tests exist.
9. **Make chaos realistic and reproducible.** Cassandra faults come from actual driver,
   server, or network behavior; seeded record selection keeps retries comparable.

## 3. Current Baseline

### 3.1 Implemented behavior to preserve

- Canonical lifecycle event model and taxonomy.
- Exact and semantic duplicate handling.
- Terminal-state precedence and late timestamp backfill.
- Attempts, retry summaries, delay classification, and reconciliation findings.
- Visibility retrieval, attempt retrieval, search, and reconciliation routes.
- Deterministic virtual clock and scenario runner.
- Unit tests and CI simulation reports.

### 3.2 Gaps this plan closes

| Current state | Target state |
| --- | --- |
| `VisibilityEngine` dictionaries | Pure reducer plus in-memory and PostgreSQL repositories |
| Process-local `RLock` | PostgreSQL row locks, unique keys, versions, and durable checkpoints |
| API directly applies EDR | Broker-acknowledged Kafka publish, then sink and projection |
| Scheduler queue is simulated in memory | Durable indexed scheduler queue with claim leases |
| No event transport | Kafka topic keyed by `jobId` and schema governed by Schema Registry |
| No EDR journal | Immutable `edr_events` written by JDBC Sink |
| One implicit state owner | Separate scheduler and EDR databases with least-privilege roles |
| No real worker dependency | Cassandra read–compute–delay–conditional-update workload with chaos faults |
| No dependency health/lag | Per-stage readiness, backlog, lag, retry, and DLQ diagnostics |

Before refactoring, record a clean baseline by running:

```bash
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/job-visibility-sim ci --output simulation-results/ci.json
```

Generated reports should only be updated when behavior intentionally changes.

## 4. Target Technology and Repository Shape

### 4.1 Proposed dependencies

Exact compatible versions are pinned after the Phase 0 spike.

| Purpose | Proposed technology |
| --- | --- |
| ORM and transactions | SQLAlchemy 2 |
| PostgreSQL driver | Psycopg 3 |
| Migrations | Alembic, with separate scheduler and EDR environments |
| Kafka producer | `confluent-kafka` Python client |
| Wire contract | JSON Schema with Schema Registry |
| Settings | Pydantic Settings or an equivalent typed settings layer |
| Metrics | Prometheus-compatible Python client |
| Integration infrastructure | Container composition and/or Testcontainers |
| Kafka sink | Confluent JDBC Sink connector with PostgreSQL driver |
| Workload datastore | Apache Cassandra with a compatible Python driver |
| Network chaos | A pinned programmable TCP fault proxy, such as Toxiproxy |

The implementation must pin container images, the connector plugin, Python dependencies,
and schema compatibility policy. Avoid floating `latest` tags.

### 4.2 Proposed repository shape

The change should be incremental, but the final responsibilities should be recognizable as:

```text
job_scheduling_simulation/
  compose.yaml
  alembic.ini
  migrations/
    scheduler/
    edr/
  infra/
    kafka/
      connect/edr-jdbc-sink.json
      schemas/job-lifecycle-edr-v1.json
      topics/
    postgres/
      init-roles/
    cassandra/
      schema.cql
      seed/
    chaos/
      proxy-config/
  src/job_visibility/
    api.py
    config.py
    domain/
      edr.py
      job.py
      projection.py
      policies.py
    scheduler/
      models.py
      repositories.py
      service.py
      worker.py
      handlers/
        print.py
        fibonacci.py
        cassandra_read.py
      cassandra_client.py
    outbox/
      publisher.py
      repository.py
      serialization.py
    edr_store/
      models.py
      repositories.py
      projection_worker.py
    persistence/
      scheduler_session.py
      edr_session.py
    observability/
      health.py
      metrics.py
  tests/
    unit/
    contracts/
    integration/postgres/
    integration/kafka/
    integration/cassandra/
    e2e/
    scenarios/
```

This tree is directional rather than a requirement to move all current modules in one
commit. Small moves with compatibility imports are preferred where they keep reviews clear.

### 4.3 Process roles

One Python package may provide multiple commands, but production processes are separated by
role:

- Job/visibility HTTP API, optionally split later if permissions require it.
- Polling scheduler and attempt executor.
- Outbox publisher.
- Projection worker.
- Reconciliation worker.
- Simulation CLI.

Each command initializes only the database pools and infrastructure clients it needs.

## 5. Delivery Sequence

```text
Phase 0  Connector, Cassandra driver, and chaos feasibility spikes
   |
Phase 1  Configuration, dependencies, local infrastructure, migrations framework
   |
Phase 2  Pure lifecycle reducer and repository contracts
   |
Phase 3  Scheduler database, durable queue, claims, and attempts
   |
Phase 4  Transactional outbox, canonical serializer, and Kafka publisher
   |
Phase 5  EDR database, immutable sink table, and JDBC Sink
   |
Phase 6  Durable projection worker and rebuild
   |
Phase 7  Durable API, EDR ingress, and reconciliation
   |
Phase 8  Operations, security boundaries, and resilience
   |
Phase 9  End-to-end scenarios, performance evidence, documentation, and rollout
```

Phases are ordered by dependency. Work within a phase may be parallelized only when it does
not require concurrent edits to the same migrations, domain models, or public contracts.

## 6. Detailed Phase Plan

### Phase 0 — Connector, Cassandra driver, and chaos feasibility spikes

**Purpose:** retire the highest-risk assumptions in ADR-002-04, ADR-002-07, ADR-002-12,
ADR-002-13, and ADR-002-14 before application code depends on them.

Tasks:

1. Start isolated Kafka, Schema Registry, Kafka Connect, and EDR PostgreSQL containers.
2. Define a minimal JSON Schema record with:
   - primitive string key containing `jobId`;
   - `schemaVersion`, `eventId`, `eventType`, timestamps, and `jobId`;
   - optional canonical EDR fields;
   - `canonicalPayload` as a JSON string.
3. Pre-create a spike `edr_events` table with JSONB, timestamp, Kafka-coordinate, primary,
   and uniqueness columns.
4. Configure JSON Schema conversion plus `InsertField` transforms for topic, partition, and
   offset.
5. Configure JDBC Sink with `auto.create=false`, `auto.evolve=false`, `insert.mode=upsert`,
   `pk.mode=record_value`, and the `eventId` field.
6. Prove correct mapping for UTC timestamps, null optional fields, JSONB canonical payload,
   camel-to-snake field names, and database defaults.
7. Add an immutability trigger or equivalent guard and prove:
   - identical `eventId` plus payload is a no-op;
   - different payload with the same `eventId` leaves the first row unchanged;
   - the collision becomes diagnosable in the DLQ.
8. Stop the EDR database, recover it, and prove the connector replays without losing valid
   records.
9. Restart Kafka Connect and prove sink recovery is duplicate-safe.
10. Start a pinned Cassandra container and network fault proxy; create the bucketed demo
    table and seed a versioned dataset larger than the maximum test read.
11. Select a Python Cassandra driver compatible with Python 3.12 and the pinned Cassandra
    version; prove bounded paging, asynchronous/bounded concurrency, request timeout, and
    connection cleanup.
12. Prove that `(datasetId, recordCount, seed)` selects the same rows and deterministic
    maximum/tie-break repeatedly without `ALLOW FILTERING` or unbounded scans.
13. Prototype bounded Fibonacci calculation plus processing delay, then a normal-checksum
    conditional update and same-partition operation marker using a stable logical operation
    ID. Explicitly prove no Cassandra counter column is needed.
14. Inject read timeout, write timeout/lost response, connection cutoff, Cassandra outage,
    and concurrent checksum conflicts; prove reconciliation distinguishes applied from
    unapplied operations and never increments one logical operation twice.
15. Prototype a scheduler claim token and heartbeat; prove a delayed Cassandra result cannot
    commit a scheduler outcome after the token is replaced, while a recovered worker can
    reconcile an already-applied Cassandra operation.
16. Record compatible component/plugin/driver versions and update the proposed ADR statuses in
    `arc42.md`.

Deliverables:

- Executable spike composition and connector configuration.
- Versioned JSON Schema draft.
- Cassandra schema/seed fixture, bounded transform/idempotency prototype, and network-chaos
  fixture.
- Automated smoke test or script with explicit assertions.
- Short decision note for any deviation from the arc42 proposal.

Exit criteria:

- Typed fields, JSONB, Kafka metadata, upsert, no-op duplicate, collision rejection, DLQ,
  database outage recovery, and connector restart have all been observed using real
  components.
- No application persistence implementation begins if the immutability guarantee is still
  unproven.
- Cassandra selection is reproducible; timeouts are bounded; and stale-token outcome
  rejection has been observed against real components.
- Lost write responses and concurrent selection conflicts never cause one logical operation
  to increment checksum more than once.

### Phase 1 — Foundations and local infrastructure

Tasks:

1. Add and pin SQLAlchemy, Psycopg, Alembic, Kafka, Cassandra driver, settings, metrics, and
   test dependencies proven in Phase 0.
2. Implement typed configuration groups for:
   - scheduler database;
   - EDR database;
   - Kafka and Schema Registry;
   - Cassandra contact points/keyspace/consistency, request/execution deadlines, and
     record/page/concurrency limits;
   - scheduler, outbox, sink, projection, and reconciliation tuning.
3. Reject one logical database being configured for both roles outside a dedicated negative
   test fixture.
4. Add independent SQLAlchemy engines/session factories with role-specific pool settings.
5. Create independent scheduler and EDR Alembic environments and version tables.
6. Add `compose.yaml` with two logical PostgreSQL databases, Kafka in KRaft mode, Schema
   Registry, Kafka Connect with the pinned JDBC plugin, Cassandra, and the fault proxy.
7. Add health checks and idempotent setup for topics, schemas, connector registration,
   migrations, Cassandra schema, and deterministic seed data.
8. Document start, wait-for-ready, migrate, inspect, test, and safe teardown commands.
9. Add pytest markers so the normal unit suite does not start infrastructure.

Exit criteria:

- A fresh checkout can start the local stack reproducibly.
- Both migration histories reach head independently.
- Each role can connect only to its intended database with its intended credentials.
- Worker identity has bounded select and approved result/checksum/operation-marker mutation
  permissions; seed identity has separate schema/data-management permissions.
- Existing unit/simulation tests remain green without containers.

### Phase 2 — Pure reducer and persistence contracts

Tasks:

1. Add characterization tests around every current `VisibilityEngine.apply` decision,
   including version increments, exact duplicates, semantic duplicates, terminal conflicts,
   and timestamp backfill.
2. Extract lifecycle state reduction from dictionary mutation and `RLock` use into a pure
   function/service:

   ```text
   current job + current attempts + incoming EDR
       -> updated job + updated attempts + decision + findings
   ```

3. Keep time-derived read and reconciliation policies injectable and free of direct
   `datetime.now()` calls.
4. Define narrow protocols for:
   - scheduler job/attempt repositories and scheduler unit of work;
   - outbox leases and publication completion;
   - raw EDR scans;
   - visibility job/attempt repositories;
   - projected-event checkpoints and findings.
5. Adapt the existing in-memory engine to those contracts so CLI behavior remains intact.
6. Add shared repository contract tests that both in-memory and PostgreSQL adapters must
   satisfy where semantics overlap.

Exit criteria:

- Existing Spec 001 tests and scenario output remain behaviorally equivalent.
- The pure reducer opens no connection, acquires no process lock, and reads no wall clock.
- PostgreSQL work can proceed without duplicating lifecycle rules.

### Phase 3 — Scheduler persistence and durable claims

#### 3.1 Scheduler migrations

Create:

- `scheduler_jobs` with stable `job_id`, business metadata, payload/reference, original UTC
  schedule, current `available_at`, internal status, attempts, claim lease, timestamps, and
  version.
- `scheduler_attempts`, unique on `(job_id, attempt_number)`.
- `scheduler_outbox` with immutable event identity/payload plus publication lease and retry
  fields.

Prefer string/check-constrained statuses over PostgreSQL enum types initially so rolling
status additions are migration-safe.

Add:

- an eligible partial index ordered by `available_at, job_id` for `PENDING` and
  `RETRY_WAIT` jobs;
- correlation and claim-expiry indexes;
- an unpublished/next-attempt outbox partial index;
- check constraints for attempts, versions, schedules, and lease consistency.

Claims include an opaque fencing token. Heartbeat and outcome updates must match job ID,
attempt number, worker ID, and the current token.

#### 3.2 Scheduler repositories and service

1. Implement job creation with caller `jobId` or idempotency key.
2. In one transaction, persist the job and initial `JOB_CREATED` and
   `JOB_SCHEDULER_SUBMISSION_REQUESTED` outbox records.
3. Implement bounded eligible selection using `FOR UPDATE SKIP LOCKED`.
4. Set a database-clock claim lease and write retrieval EDR outbox rows in the claim
   transaction.
5. Execute handlers outside the claim transaction.
6. Add a small handler registry with:
   - `PRINT`, which writes a configured message to structured output;
   - `FIBONACCI`, which calculates values up to a configured maximum of `10000` and returns
     a summary ending at `6765`;
   - `CASSANDRA_FIB_UPDATE`, which streams reproducibly selected `X` records, chooses the
     greatest number with deterministic tie-break, computes Fibonacci, delays, and
     conditionally updates that record/checksum.
7. Commit `RUNNING`, the attempt start, and `JOB_EXECUTION_STARTED` atomically before calling
   a handler, without holding the transaction during handler execution.
8. Heartbeat long-running claims in short scheduler transactions and cancel/discard work if
   the fencing token is no longer current.
9. Configure Cassandra request deadline, whole-execution deadline, page size, record-count,
   maximum Fibonacci input, processing delay, concurrency, and conditional-conflict ceilings;
   payloads may lower but never exceed deployment limits.
10. Use fast doubling or another bounded Fibonacci algorithm and store the result as a
    decimal string.
11. Derive one stable operation ID per logical job/handler contract, reused by all attempts.
12. Reserve/update by observed normal checksum and operation state; write
    `fibonacci_result`, checksum `N+1`, last operation ID, and the operation marker using the
    Phase 0 conditional/same-partition protocol. Do not use a Cassandra counter.
13. On write timeout or lost connection, reread the row and operation marker before
    deciding whether the effect needs retry; an existing matching marker is success.
14. Classify unresolved read/write timeout, unavailable, connection loss, conditional
    conflict, and execution deadline as retryable; classify invalid payload/dataset/schema,
    input bound, and count integrity mismatch as non-retryable by default.
15. Keep driver retries explicitly bounded so scheduler attempts remain the visible retry
    unit.
16. On success, verify selected record, Fibonacci result, and one checksum increment, then
    atomically commit the attempt/job as `SUCCEEDED`, persist a compact rows/max/record/
    operation/checksum/delay/result-hash summary, clear claim/retry fields, and add
    `JOB_EXECUTION_SUCCEEDED`; never add retry EDRs.
17. On retryable failure, atomically commit the failed attempt, set the job to `RETRY_WAIT`,
   move `available_at` to the configured backoff instant, clear the claim, and add failure,
   retry-requested, and retry-acknowledged EDRs.
18. When that retry becomes due, atomically advance the attempt number and create the unique
    scheduler-attempt row as part of its claim; never advance it merely because time passed.
19. On non-retryable or final-attempt failure, commit `FAILED` or `RETRIES_EXHAUSTED` and no
    future eligibility.
20. Convert unexpected handler exceptions into a redacted classified failure transition.
21. Implement expired-claim recovery without silently incrementing attempts.
22. Implement optimistic version checks for non-claim mutations.
23. Add a durable submission endpoint; the proposed route is `POST /scheduler/jobs`, kept
   distinct from visibility reads under `/scheduled-jobs`.

Exit criteria:

- Restart does not lose jobs or attempts.
- Two pollers never hold a live claim for the same `(job_id, attempt_number)`.
- More than `X` due jobs drain in stable order across polls.
- A Fibonacci job with limit `10000` succeeds with last value `6765` and no retry.
- A Cassandra job reads the requested rows, selects the expected maximum, calculates
  Fibonacci, delays, updates the selected row, and increments checksum once with bounded
  resources, then succeeds with no retry.
- A real Cassandra timeout remains unclaimable until future `available_at`, then succeeds
  after the fault is removed or continues toward the configured attempt limit.
- Sustained Cassandra unavailability runs exactly `max_attempts` and becomes terminal.
- A stale worker cannot commit after another worker obtains a new fencing token.
- A lost Cassandra update response is reconciled by operation ID and never double-increments.
- Concurrent jobs selecting one row either each increment once or fail retryably after the
  configured conditional-conflict bound.
- Every committed lifecycle mutation has its outbox record in the same transaction.
- An injected rollback leaves neither the mutation nor its EDR outbox record committed.

### Phase 4 — Transactional outbox and Kafka publication

#### 4.1 Canonical EDR serialization

1. Add `schemaVersion=1` to the transport model without changing Spec 001 domain semantics.
2. Define deterministic canonical JSON:
   - UTF-8;
   - sorted object keys;
   - compact separators;
   - normalized UTC timestamp strings;
   - explicit null policy fixed by the schema.
3. Store canonical bytes/string in the outbox at event creation; retries must never
   reserialize from mutable job state.
4. Compute and retain SHA-256 for diagnostics and sink comparison.
5. Register the JSON Schema under a stable subject using backward-transitive compatibility.
6. Disable schema auto-registration outside local development.

#### 4.2 Publisher

1. Lease due unpublished outbox rows in bounded batches with `SKIP LOCKED` and database
   time.
2. Publish outside the lease transaction with:
   - key `jobId`;
   - lifecycle topic from configuration;
   - producer idempotence enabled;
   - `acks=all`;
   - bounded delivery timeout and retry behavior.
3. Mark `published_at`, Kafka partition/offset when returned, and clear the lease after
   broker acknowledgement.
4. On failure, store a redacted error, increment attempts, and schedule bounded exponential
   backoff with jitter.
5. Allow lease recovery after a publisher crash.
6. Expose unpublished count/age and publish metrics.
7. Implement graceful shutdown that stops leasing and resolves in-flight delivery callbacks
   within a deadline.

Exit criteria:

- Kafka outage grows a durable, observable outbox backlog while scheduler writes continue.
- Recovery publishes all rows.
- Crash after broker acknowledgement causes, at worst, an idempotently handled duplicate.
- Multiple publisher processes share work without losing rows.

### Phase 5 — EDR journal and Kafka JDBC sink

#### 5.1 EDR migrations

Create `edr_events` with:

- typed canonical search/projection columns;
- canonical payload as JSONB;
- derived or validated SHA-256 payload hash;
- schema version;
- Kafka topic, partition, and offset;
- database-assigned `persisted_at`;
- primary key `event_id`;
- unique Kafka coordinate tuple;
- checks and indexes required by Spec 002.

Add the immutable upsert guard proven in Phase 0. Grant the Connect role only the minimum
table/sequence/function privileges needed for its insert/upsert path. Deny raw update/delete
outside the controlled guard.

#### 5.2 Connector configuration

1. Check in a secret-free connector template.
2. Subscribe to the configured lifecycle topic only.
3. Apply JSON Schema conversion and Kafka topic/partition/offset enrichment.
4. Map the record into the pre-migrated `edr_events` table.
5. Configure record-value primary key `eventId`, upsert, batching, UTC, retries, and DLQ.
6. Disable delete, auto-create, and auto-evolve.
7. Log error context without logging full message bodies or secrets.
8. Add connector worker/task health and DLQ metrics.
9. Make connector registration/update idempotent and verify the active config after apply.

Exit criteria:

- A published EDR appears once in `edr_events` with correct typed values, JSONB, hash, and
  Kafka coordinates.
- Identical delivery is a no-op.
- Identity collision cannot mutate the first row and is observable.
- Sink restart and EDR database outage recover without valid-record loss.
- The application has no code path that inserts raw EDR rows directly.

### Phase 6 — Durable projection and rebuild

#### 6.1 Projection migrations

Create:

- `projected_events` keyed by `event_id`;
- `job_visibility` with recorded-state inputs, time fields, retry summary, quality flags,
  `data_as_of`, and integer version;
- `job_attempts` unique on `(job_id, attempt_number)`;
- `projection_decisions` linked to job/event identity;
- `reconciliation_findings` with active/resolved history.

Projection foreign keys may reference raw EDR and visibility tables within the EDR
database. There are no references to scheduler tables.

#### 6.2 Projection worker

1. Select the oldest unprojected rows by `persisted_at, event_id` in a bounded batch.
2. Claim with row locking or the lease method validated by concurrency tests.
3. For each event transaction:
   - establish one-writer coordination for `job_id`, including the first-event insert race;
   - recheck the unique projection checkpoint;
   - load job and attempt state;
   - call the pure reducer;
   - write job, attempts, decisions, findings, and checkpoint;
   - commit once.
4. On a version conflict, reload and reapply within a bounded retry count.
5. Leave a failed event unprojected after rollback; do not skip it silently.
6. Expose throughput, conflict retries, failures, and oldest-unprojected age.
7. Stop claiming on shutdown and finish or roll back the current transaction.

#### 6.3 Rebuild command

1. Add a command that targets explicitly named shadow projection tables or an isolated
   database.
2. Never delete raw `edr_events`.
3. Replay through the production reducer.
4. Produce comparison counts/hashes for jobs, attempts, terminal states, and findings.
5. Require explicit operator confirmation outside test mode before switching tables.

Exit criteria:

- Restart before and after a projection commit yields exactly one durable application.
- Multiple workers process independent jobs concurrently and same-job events safely.
- Spec 001 duplicate, out-of-order, terminal, and retry tests pass through PostgreSQL.
- Rebuild produces an equivalent projection using only the EDR database.

### Phase 7 — Durable API, Kafka EDR ingress, and reconciliation

Tasks:

1. Replace the default API `VisibilityEngine` singleton with application services backed by
   EDR repositories.
2. Keep dependency injection so unit tests can supply in-memory adapters.
3. Serve existing job, attempt, and search endpoints from durable projections.
4. Preserve the 404 warning that missing visibility does not prove scheduler absence.
5. Change `POST /edrs` to:
   - validate the versioned EDR;
   - publish to Kafka with `jobId` key;
   - return `202` only after broker acknowledgement;
   - never mutate in-memory or EDR database state directly.
6. Define a bounded request timeout and a service-unavailable response for Kafka/registry
   failure.
7. Make read-after-write eventual consistency explicit in API documentation and tests.
8. Persist reconciliation finding creation and resolution in EDR transactions.
9. Use database/effective time consistently for periodic reconciliation while preserving
   virtual time in simulations.
10. Expose `dataAsOf` on every visibility response and pipeline lag only on operational
    diagnostics.

Exit criteria:

- API state survives restart and is shared across replicas.
- The API has no scheduler-database connection or imports from scheduler persistence.
- `POST /edrs` follows the Kafka/Sink/projector path end to end.
- Search and reconciliation behavior matches Spec 001 against durable data.

### Phase 8 — Operations, security, and resilience

Tasks:

1. Implement per-role liveness, readiness, and dependency diagnostics.
2. Treat failed Connect tasks, excessive outbox age, excessive projection lag, and DLQ
   growth as explicit unhealthy/degraded conditions according to configurable thresholds.
3. Add structured redacted logs with correlation identifiers.
4. Add metrics listed in arc42 section 8.8 and dashboards/alert rules or documented queries.
5. Create least-privilege PostgreSQL roles, Cassandra worker/seed roles, and Kafka ACL
   examples/tests.
6. Require TLS/auth settings outside local mode and reject unsafe production configuration.
7. Add connection-pool bounds and statement/lock timeouts per role.
8. Add scheduler claim, outbox lease, and projector recovery jobs.
9. Add operator procedures for:
   - Kafka outage;
   - scheduler/EDR database outage;
   - connector task failure;
   - poison record and DLQ inspection;
   - event identity collision;
   - projection backlog/rebuild;
   - Cassandra read/write timeout, unknown update outcome, conditional conflict, operation
     reconciliation, unavailability, driver saturation, and seed integrity;
   - lost worker heartbeat/fencing-token rejection;
   - secret rotation.
10. Verify that logs, metrics, exception responses, and DLQ policy do not expose prohibited
    payload data.

Exit criteria:

- Failure scenarios in Spec 002 section 15 are automated or have an executable runbook.
- Each process receives only its role-specific secrets and permissions.
- Operators can identify which of the four queues is delayed without database shell access.
- Operators can distinguish Cassandra client timeout, server read/write timeout, unknown
  update outcome, conditional conflict, unavailability, connection loss, execution deadline,
  and stale claim ownership.

### Phase 9 — End-to-end evidence, performance, and release preparation

Tasks:

1. Execute the full durable path:
   scheduler transaction -> outbox -> Kafka -> JDBC Sink -> raw EDR -> projector -> API.
2. Port the representative Spec 001 scenario set to durable adapters:
   - happy-path completion and retry;
   - exact/semantic duplicates;
   - out-of-order start/outcome;
   - missing/delayed evidence;
   - batch overflow and concurrent pollers.
3. Execute every Spec 002 failure scenario.
4. Add a restart matrix covering API, scheduler, publisher, Connect, projector, both
   PostgreSQL databases, and Cassandra at pre-commit/post-commit boundaries.
5. Measure:
   - scheduler claim throughput and oldest-due delay;
   - outbox publish rate and age;
   - sink throughput and Kafka lag;
   - projection throughput/latency;
   - API query latency;
   - storage bytes per job and per EDR;
   - Cassandra rows/pages per second, max-selection/Fibonacci/delay/update time, conditional
     conflicts, operation reconciliation, p95/p99 request time, timeout rate, worker memory,
     and the effect of page/concurrency limits.
6. Demonstrate the configurable 10-second healthy-pipeline freshness target at an agreed
   percentile using a documented representative load.
7. Use results to select initial topic partitions, worker concurrency, database pool sizes,
   batch sizes, and alert thresholds.
8. Update root architecture documentation to mark the durable target implemented and retain
   a clear description of simulation mode.
9. Document release, rollback, backup/restore, retention, and projection rebuild.
10. Generate final test evidence without silently overwriting unrelated simulation results.

Exit criteria:

- All Spec 002 acceptance criteria have linked automated evidence or a reviewed operational
  verification.
- Known capacity limits and unmeasured production assumptions are documented.
- Release and rollback have been rehearsed in a disposable environment.

## 7. Database Migration Plan

### 7.1 Separate migration histories

Use one Alembic configuration with named environments or two explicit configurations, but
maintain:

- separate migration directories;
- separate version tables;
- separate database URLs and credentials;
- separate deployment commands;
- no migration importing ORM metadata from the other database.

Recommended commands should make the target explicit, for example:

```text
db migrate scheduler
db migrate edr
db current scheduler
db current edr
```

### 7.2 Migration order

Initial deployment order is:

1. Create PostgreSQL roles/logical databases and Cassandra worker/seed identities.
2. Apply scheduler base migration.
3. Apply EDR raw-journal migration and immutable guard.
4. Apply EDR projection migration.
5. Apply the Cassandra keyspace/table CQL and seed the named demonstration dataset.
6. Register topic and schema.
7. Deploy application roles with workers disabled.
8. Register/enable the JDBC connector.
9. Enable outbox publishers.
10. Enable projector and reconciler.
11. Enable durable API traffic and scheduler polling.

The connector must not start before `edr_events` and its guard exist.

### 7.3 Schema-change policy

- Expand: add nullable/defaulted columns, indexes concurrently where supported, and code
  that understands both forms.
- Migrate: backfill or dual-read inside one database only.
- Contract: remove old fields only after all processes and schemas stop using them.
- Register compatible Kafka schemas before enabling producers that use them.
- Never use connector auto-evolution as a migration mechanism.
- Test downgrade/rollback compatibility for the immediately prior application version.

### 7.4 Data lifecycle

- Scheduler jobs and published outbox records get explicit archival windows.
- Unpublished outbox records are never retention-deleted.
- Raw EDRs are immutable for their configured retention window and are partition-ready.
- Projection rows can be rebuilt and have no independent archival authority.
- Retention jobs emit metrics and use bounded partition/table operations.

## 8. Kafka and Connector Delivery Plan

### 8.1 Topic creation

Create lifecycle and DLQ topics explicitly. Configuration includes partitions, replication,
minimum in-sync replicas, retention, and cleanup policy. Local values may be one broker and
replication factor one; non-local values require environment-specific review.

The lifecycle topic is not compacted solely by `jobId`, because multiple facts for one job
must be retained. The DLQ has separate retention and tighter access controls.

### 8.2 Schema delivery

- Check JSON Schema into source control.
- Validate examples in tests.
- Run registry compatibility checks in CI.
- Register schemas during deployment, not from production request paths.
- Keep `schemaVersion` in the record even though Schema Registry has its own version.
- Document optional/additive evolution and breaking-version topic strategy.

### 8.3 Connector delivery

- Store a template without passwords.
- Inject database and registry secrets at deployment.
- Validate the installed connector plugin/version before registration.
- Compare desired and active connector config after update.
- Wait for all tasks to become RUNNING before enabling upstream publication.
- Treat task FAILED as a deployment/health failure.
- Roll connector configuration back independently from application code.

### 8.4 DLQ process

1. Alert on the first new record and on growing age/count.
2. Inspect metadata and redacted error context with restricted access.
3. Classify schema error, invalid domain value, database mapping error, or identity collision.
4. Fix the producer/schema/configuration before replay.
5. Republish a corrected fact with the original `eventId` only when canonical content is
   identical; use a new event and explicit correction semantics otherwise.
6. Record operator action and verify downstream projection.

## 9. Test Plan

### 9.1 Test layers

| Layer | Infrastructure | Main evidence |
| --- | --- | --- |
| Unit | None | Reducer, canonicalization, settings, policies, retry math |
| Repository contract | In-memory or one PostgreSQL DB | Shared adapter semantics |
| PostgreSQL integration | Two PostgreSQL databases | Constraints, transactions, locks, leases, roles, migrations |
| Kafka/Connect integration | Kafka, Registry, Connect, EDR PostgreSQL | Schema, keying, sink mapping, upsert guard, retry, DLQ |
| Cassandra/worker integration | Cassandra, fault proxy, scheduler PostgreSQL | Seeded reads, Fibonacci/update, idempotency, timeouts, retries, fencing |
| End to end | Full stack | Scheduler-to-API lifecycle and outage recovery |
| Scenario regression | In-memory and selected durable runs | Spec 001 behavior parity |
| Performance | Full isolated stack | Throughput, lag, latency, storage, scaling limits |

### 9.2 Mandatory PostgreSQL cases

- Job plus outbox commit and rollback atomicity.
- Concurrent `SKIP LOCKED` scheduler batches.
- Attempt uniqueness and stale version rejection.
- Expired claim and outbox lease recovery.
- `PRINT` and Fibonacci-through-`6765` success transitions with no artificial failure mode.
- Claim heartbeat, fencing-token replacement, and stale outcome rejection.
- Two URLs/roles cannot access the other database.
- EDR primary key and Kafka-coordinate uniqueness.
- Exact upsert no-op and identity-collision rejection.
- Projection checkpoint, job, attempt, decision, and finding atomicity.
- Concurrent first event for the same job.
- Independent migration upgrade and downgrade checks where reversible.

### 9.3 Mandatory Kafka/Connect cases

- `jobId` produces stable same-job partitioning.
- JSON Schema optional/null/timestamp behavior.
- Canonical payload reaches JSONB unchanged in meaning.
- Topic, partition, and offset reach the expected columns.
- Connector restart after committed and uncommitted batches.
- EDR database outage and catch-up.
- Duplicate delivery and event collision.
- Poison record reaches DLQ; later valid record behavior matches configured policy.
- Schema incompatibility fails CI/deployment before publication.

### 9.4 Mandatory Cassandra/worker cases

- Idempotent keyspace/table creation and versioned deterministic seeding.
- Exactly `X` seeded records, stable maximum/tie-break, bounded pages/concurrency/memory,
  bounded Fibonacci input, and bounded processing delay.
- Correct Fibonacci result and exactly one normal-checksum increment per stable operation ID.
- Proxy latency causing a real client read or write operation timeout.
- Lost update response reconciled through the row and operation marker without a second
  increment.
- Concurrent jobs selecting one maximum record and bounded conditional-conflict handling.
- Cassandra stop/disconnect causing unavailable or connection failure.
- Fault removal followed by retry of the same logical selection.
- Sustained outage through `max_attempts`.
- Delayed reply after heartbeat/token loss being unable to commit.

### 9.5 Mandatory end-to-end cases

Use bounded condition polling with diagnostic timeout output; never use an unexplained fixed
sleep.

- Create -> schedule -> retrieve -> start -> succeed -> durable `SUCCEEDED` API state.
- Execute real `PRINT` and `FIBONACCI(limit=10000)` jobs through that path.
- Execute `CASSANDRA_FIB_UPDATE(recordCount=10000, seed=42)`, verify selected maximum,
  Fibonacci result, configured delay, and checksum `N+1`.
- Inject Cassandra latency on attempt one, verify no early reclaim, remove it, then retry the
  same selection at `available_at`.
- Keep Cassandra unavailable through exactly `max_attempts` and verify terminal exhaustion.
- Fail -> request retry -> acknowledge -> second attempt succeeds.
- Cancel and retry exhaustion.
- `X+1` and multi-batch polling.
- Concurrent pollers and publishers.
- Publisher crash window duplicate.
- Out-of-order and delayed EDR delivery.
- API, worker, Connect, and database restarts.
- Kafka, EDR DB, and scheduler DB outage isolation.
- Projection rebuild equivalence.
- `POST /edrs` eventual-consistency behavior.

### 9.6 Test data isolation

- Generate a unique test-run ID and prefix topic/schema/connector names where tests run in
  shared infrastructure.
- Use dedicated logical databases or schemas only when the test explicitly preserves the
  two logical ownership boundary.
- Use a unique Cassandra `dataset_id` per mutable test fixture; shared seed data must not be
  mutated by update tests.
- Reset network proxy faults in test teardown even when assertions fail.
- Teardown only labeled resources belonging to the current test run.
- Retain logs and connector status on failure before cleanup.

## 10. CI and Developer Workflow

### 10.1 CI jobs

1. **Static and unit:** formatting, Ruff, typing when introduced, unit tests, fast scenario
   set.
2. **Migration:** upgrade fresh scheduler/EDR databases, inspect heads, and run constraint
   tests.
3. **Kafka integration:** start pinned infrastructure, register schema/connector, run sink
   and DLQ cases.
4. **Cassandra integration:** seed data and run bounded read/compute/delay/update,
   unknown-write-outcome, conflict, timeout, disconnect, heartbeat, and fencing tests through
   the fault proxy.
5. **End to end:** run the durable happy path, duplicate/restart cases, and selected Spec 001
   scenarios.
6. **Nightly/resilience:** outage matrix, full scenarios, rebuild, Cassandra chaos, and
   representative load.

Each infrastructure job must have an overall deadline, print component health on failure,
and upload relevant logs/configuration with secrets removed.

### 10.2 Local commands

Provide one documented interface, such as `make`, `just`, or Python CLI commands, for:

```text
infra-up
infra-ready
db-migrate
connector-apply
test-unit
test-postgres
test-kafka
test-cassandra
test-e2e
infra-status
infra-down
```

The exact command runner can be selected during Phase 1. Commands must be non-destructive by
default. Teardown requiring volume deletion must name the project and state that durable
local data will be removed.

## 11. Rollout and Recovery Plan

### 11.1 Initial rollout

Because current state is process memory, there is no durable production data migration.
Rollout is a controlled enablement:

1. Back up both fresh PostgreSQL configurations and verify restore procedures.
2. Apply PostgreSQL migrations and validate least-privilege roles.
3. Apply Cassandra keyspace/table CQL, seed the versioned dataset, and verify the constrained
   worker mutation identity and initial checksums.
4. Create topics, register schema, and start a paused/no-upstream connector.
5. Run a synthetic EDR through the sink and projection in the target environment.
6. Start APIs and workers with scheduler polling/outbox publication disabled.
7. Enable connector, then projector, then outbox publisher.
8. Submit `PRINT`, `FIBONACCI`, and bounded `CASSANDRA_FIB_UPDATE` canaries and verify the
   Cassandra result/checksum plus scheduler row, outbox, Kafka, EDR, projection, and API.
9. Enable scheduler polling for a bounded cohort or environment.
10. Monitor all four queue ages, Cassandra read metrics, and error rates through the
    observation window.
11. Expand traffic only after acceptance thresholds hold.

### 11.2 Rollback

- Stop new job admission or route it to the previously approved behavior if available.
- Stop scheduler claims before stopping executors.
- Stop outbox leasing; do not delete unpublished rows.
- Pause the connector rather than delete topics or EDR rows.
- Stop projector claims after current transactions finish.
- Roll application processes back only to versions compatible with current database and
  Kafka schemas.
- Prefer forward-fix migrations; never use destructive rollback against accumulated EDRs.

Backlogs are durable and should normally be drained after correction. Do not manually mark
outbox rows published or raw EDRs projected without a reviewed recovery procedure.

### 11.3 Restore and rebuild

- Scheduler restore uses scheduler backups and then resumes outbox publication from durable
  unpublished state.
- EDR restore uses EDR backups plus retained Kafka records where necessary; recovery must
  reconcile Kafka coordinates and event IDs before resuming the connector.
- Projection corruption is repaired by a shadow rebuild from `edr_events`, not by reading
  scheduler tables.
- The demonstration Cassandra dataset is recreated from its versioned schema/seed manifest;
  it is never used to reconstruct scheduler or visibility state.
- Recovery exercises are part of release readiness, not documentation-only claims.

## 12. Traceability

### 12.1 Architecture decision to implementation phase

| Architecture decision | Implemented/validated in |
| --- | --- |
| ADR-002-01: two PostgreSQL databases | Phases 1, 3, 5, 8 |
| ADR-002-02: transactional outbox | Phases 3 and 4 |
| ADR-002-03: Kafka key `jobId` | Phases 0 and 4 |
| ADR-002-04: JSON Schema/Registry | Phases 0, 1, and 4 |
| ADR-002-05: raw-only JDBC sink | Phase 5 |
| ADR-002-06: project from `edr_events` | Phase 6 |
| ADR-002-07: upsert plus hash guard | Phases 0 and 5 |
| ADR-002-08: at least once plus idempotency | Phases 3–9 |
| ADR-002-09: in-memory and PostgreSQL adapters | Phase 2 and test plan |
| ADR-002-10: initial EDR-primary reads | Phase 7 |
| ADR-002-11: Cassandra workload store | Phases 0, 1, 3, 8, and 9 |
| ADR-002-12: seeded partition/range selection | Phases 0, 3, and Cassandra tests |
| ADR-002-13: heartbeat and claim fencing | Phases 0, 3, and Cassandra chaos tests |
| ADR-002-14: idempotent Cassandra update operation | Phases 0, 3, and Cassandra chaos tests |

### 12.2 Quality scenario to test evidence

| Quality scenarios | Evidence phase/suite |
| --- | --- |
| Q-REL-01 through Q-REL-04 | Phases 4–6; restart/outage integration tests |
| Q-INT-01 and Q-INT-02 | Phase 5 collision test; Phase 6 Spec 001 projection suite |
| Q-ISO-01 | Phase 1/8 permission and outage tests |
| Q-CON-01 and Q-CON-02 | Phase 3/6 PostgreSQL concurrency tests |
| Q-WORK-01 through Q-WORK-07 | Phase 3 worker integration and Phase 9 end-to-end tests |
| Q-PERF-01 and Q-PERF-02 | Phase 9 performance and batch scenarios |
| Q-OPS-01 and Q-OPS-02 | Phase 5/8 DLQ and health tests |
| Q-MNT-01 and Q-MNT-02 | Phase 1/2 unit workflow and infrastructure command |

### 12.3 Spec 002 acceptance coverage

| Requirement group | Primary phases |
| --- | --- |
| Durable scheduler state | 3 |
| Durable raw EDR and visibility state | 5–7 |
| Separate databases and credentials | 1, 3, 5, 8 |
| Transactional outbox and Kafka path | 3–5 |
| JDBC Sink ownership and immutable idempotency | 0, 5 |
| Kafka coordinates and schema | 0, 4, 5 |
| Concurrent poller/projector safety | 3, 6 |
| Cassandra workload, timeout retry, and worker fencing | 0, 1, 3, 8, 9 |
| Outage recovery and DLQ | 4, 5, 8, 9 |
| Projection rebuild | 6, 9 |
| Reproducible local/CI workflow | 1, 9, 10 |

## 13. Review Gates

### Gate A — Document approval

Before Phase 0:

- Approve or revise `spec.md`, `arc42.md`, and this plan.
- Confirm the hard requirement that JDBC Sink is the raw EDR writer.
- Confirm the two logical database boundary.
- Confirm Cassandra as a third workload database with constrained demo-result writes rather
  than scheduler or EDR persistence.
- Confirm whether `POST /scheduler/jobs` is the desired durable submission route.

### Gate B — Feasibility approval

After Phase 0:

- Review the pinned stack and licensing/operating implications.
- Accept or replace JSON Schema plus Schema Registry.
- Accept the immutable upsert implementation and its measured DLQ behavior.
- Accept the Cassandra driver, seeded selection, timeout classification, fault proxy, and
  claim-fencing/idempotent-update prototype.
- Update ADR-002-04 and ADR-002-07 from proposed to accepted/rejected.
- Update ADR-002-12 and ADR-002-13 from proposed to accepted/rejected.
- Update ADR-002-14 from proposed to accepted/rejected.

### Gate C — Persistence contracts

After Phases 2 and 3:

- Review pure reducer parity.
- Review scheduler and outbox schemas, claim semantics, retry semantics, and API contract.
- Confirm no cross-store transaction/join or authority leakage, and verify each role has only
  its intended PostgreSQL/Cassandra permissions.

### Gate D — Pipeline acceptance

After Phases 5–7:

- Review a full trace from scheduler transaction to visibility response.
- Review duplicate, collision, outage, and rebuild evidence.
- Accept ADR-002-06 and ADR-002-10 or request measured alternatives.

### Gate E — Production readiness

After Phase 9:

- Review performance results and initial sizing.
- Review alert thresholds, security roles, retention, backup/restore, and rollback rehearsal.
- Approve traffic enablement separately from code merge.

## 14. Definition of Done

Implementation is complete only when:

- All acceptance criteria in [`spec.md`](spec.md#17-acceptance-criteria) have evidence.
- All accepted decisions in [`arc42.md`](arc42.md#9-architecture-decisions) are reflected in
  code and deployment configuration.
- Proposed decisions have been resolved or explicitly retained with a named validation
  owner and deadline.
- Scheduler and EDR migration heads are independently reproducible from empty databases.
- JDBC Sink is the only normal raw EDR writer and its active configuration is verified.
- Duplicate, collision, concurrency, restart, outage, DLQ, and rebuild tests pass.
- Cassandra seeded read/maximum/Fibonacci/delay/update, exactly-once logical checksum,
  unknown-write-outcome, conflict, timeout/recovery, sustained-outage, heartbeat, and
  stale-token chaos tests pass against real infrastructure.
- Existing Spec 001 unit and deterministic scenario behavior remains green.
- Healthy-pipeline freshness meets the agreed target under documented representative load.
- Least-privilege credentials and unsafe-configuration rejection are tested.
- Local setup, CI, deployment, rollback, restore, retention, DLQ, and rebuild procedures are
  current and executable.
- Architecture and root README documentation describe the implemented state accurately.
- No commit, merge, or production enablement occurs without the corresponding review gate.
