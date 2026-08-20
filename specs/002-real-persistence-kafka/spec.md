# Real Persistence and Kafka Specification

Status: proposed  
Supersedes: the in-memory persistence and technology-neutral EDR transport described in
Spec 001  
Depends on: [`../001-scheduled-job-visibility/spec.md`](../001-scheduled-job-visibility/spec.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Required Architecture](#3-required-architecture)
- [4. Data Ownership and Isolation](#4-data-ownership-and-isolation)
- [5. Scheduler Database](#5-scheduler-database)
- [6. Kafka Contract](#6-kafka-contract)
- [7. EDR Database and Kafka Sink](#7-edr-database-and-kafka-sink)
- [8. Projection and Query Flow](#8-projection-and-query-flow)
- [9. Transaction and Delivery Semantics](#9-transaction-and-delivery-semantics)
- [10. Service Behaviour](#10-service-behaviour)
- [11. Configuration and Secrets](#11-configuration-and-secrets)
- [12. Migrations and Compatibility](#12-migrations-and-compatibility)
- [13. Observability and Operations](#13-observability-and-operations)
- [14. Security](#14-security)
- [15. Failure Scenarios](#15-failure-scenarios)
- [16. Testing Strategy](#16-testing-strategy)
- [17. Acceptance Criteria](#17-acceptance-criteria)
- [18. Out of Scope](#18-out-of-scope)

## 1. Purpose

Replace process-local job and EDR state with durable persistence and a real Kafka delivery
path.

The implementation must use two independently owned authoritative databases:

1. A **scheduler database** containing the operational jobs polled and executed by the job
   scheduler.
2. An **EDR database** containing the immutable lifecycle EDR journal and the visibility
   projections derived from those EDRs.

It must also provide a separate **Cassandra workload database** for the demonstration worker.
Cassandra contains seeded records read and updated by a job handler; it is not an authority
for scheduler state, EDR history, or visibility.

Lifecycle EDRs must reach the EDR database through a Kafka topic and a Kafka Connect JDBC
sink. Application code must not copy scheduler rows directly into the EDR database.

All evidence and state rules from Spec 001 remain applicable. In particular, operational
scheduler state and externally visible evidence are different concepts. The visibility API
must not infer scheduler acknowledgement, retrieval, execution, or completion from a row in
the scheduler database; it may expose those facts only after their EDRs have been persisted
and projected.

## 2. Scope

This specification covers:

- PostgreSQL persistence for scheduler jobs.
- A transactional outbox in the scheduler database.
- Kafka publication of canonical lifecycle EDRs.
- Kafka partitioning, schema, and delivery requirements.
- Kafka Connect JDBC sink persistence into a separate PostgreSQL EDR database.
- Cassandra persistence for a realistic read–compute–delay–conditional-update job.
- Network and Cassandra read-timeout chaos testing.
- Durable visibility projections, attempts, decisions, and reconciliation findings.
- Restart recovery, duplicate delivery, poison records, and broker/database outages.
- Local container-based development and integration tests using real infrastructure.
- Migration of the API and scheduler from in-memory repositories to durable repositories.

The deterministic, virtual-clock simulation remains supported. Unit tests may use in-memory
adapters, but integration and end-to-end tests for this specification must use PostgreSQL,
Kafka, and Kafka Connect.

## 3. Required Architecture

```text
                         scheduler database
                      +------------------------+
Job API / Producer -->| scheduler_jobs         |
Polling Scheduler --->| scheduler_attempts     |
                      | scheduler_outbox       |
                      +-----------+------------+
                                  |
                                  | committed outbox records
                                  v
                         Outbox Publisher
                                  |
                                  | keyed by jobId
                                  v
                      Kafka: job-lifecycle-edr.v1
                         |                    |
                         |                    +--> dead-letter topic
                         v
                  Kafka Connect JDBC Sink
                         |
                         v
                           EDR database
                      +------------------------+
                      | edr_events             |
                      | projected_events       |
Projection Worker --->| job_visibility         |<--- Visibility API
Reconciler ---------->| job_attempts           |
                      | projection_decisions   |
                      | reconciliation_findings|
                      +------------------------+

Polling Scheduler / Worker --> Chaos proxy (test only) --> Cassandra workload database
```

Required runtime components are:

- PostgreSQL instance or logical database for scheduler data.
- PostgreSQL instance or logical database for EDR and visibility data.
- Kafka broker cluster.
- Outbox publisher.
- Kafka Connect worker with a JDBC sink connector.
- Projection worker.
- Visibility API and reconciliation worker.
- Cassandra workload database and a worker-only Cassandra client.
- A controllable network fault proxy in local chaos tests.

The two PostgreSQL databases may share a physical server in local development. They must
still use different databases, credentials, migrations, and connection strings so that the
ownership boundary is exercised. Production may place them on separate servers or clusters
without application changes.

## 4. Data Ownership and Isolation

| Data | Owner | Allowed writers | Allowed readers |
| --- | --- | --- | --- |
| Scheduler jobs and claims | Scheduler database | Job API and scheduler | Scheduler operations only |
| Scheduler outbox | Scheduler database | Transaction creating the associated fact | Outbox publisher |
| Raw EDR journal | EDR database | Kafka JDBC sink only | Projector, audit, support |
| Visibility projections and attempts | EDR database | Projection worker | Visibility API, reconciler |
| Reconciliation findings | EDR database | Reconciler/projector | Visibility API, support |
| Demonstration workload records | Cassandra | Seed tooling and `CASSANDRA_TRANSFORM` worker | Worker and test inspection |

The following rules are mandatory:

- There are no foreign keys across databases.
- Runtime code must not perform cross-database joins or distributed transactions.
- The visibility API must not read the scheduler database.
- The scheduler must not read or update visibility projection tables.
- Cassandra must not contain scheduler status, outbox, raw EDR, or visibility tables.
- The worker must not use Cassandra data to infer or repair scheduler/visibility state.
- `jobId`, `eventId`, `correlationId`, and Kafka metadata are the only cross-system
  correlation mechanisms.
- A loss of either database must not silently cause the other database to be treated as its
  authoritative replacement.

## 5. Scheduler Database

### 5.1 `scheduler_jobs`

The operational queue must include at least:

| Column | Requirement |
| --- | --- |
| `job_id` | UUID or stable string primary key supplied by the caller |
| `correlation_id` | Business correlation identifier; indexed |
| `job_type` | Required job handler/type |
| `payload` or `payload_reference` | Job input or durable reference to it |
| `scheduled_at` | Original intended execution time; UTC |
| `available_at` | Current eligibility time; initially `scheduled_at`, moved forward for retry; indexed |
| `status` | Internal scheduler status, not the visibility API status |
| `attempt_number` | Non-negative current attempt |
| `max_attempts` | Positive maximum attempt count |
| `claimed_by` | Nullable poller/worker identifier |
| `claim_token` | Nullable opaque fencing token replaced on every new claim owner |
| `claimed_at` | Nullable claim time |
| `claim_expires_at` | Nullable lease expiry used for crash recovery |
| `created_at` / `updated_at` | Database-recorded UTC timestamps |
| `version` | Integer optimistic-lock version |

The eligibility index must support the scheduler's principal query: jobs in an eligible
internal state ordered by `available_at` and `job_id`.

Pollers must atomically claim no more than the configured batch size with PostgreSQL row
locking equivalent to:

```sql
SELECT ...
FROM scheduler_jobs
WHERE status IN ('PENDING', 'RETRY_WAIT') AND available_at <= :poll_time
ORDER BY available_at, job_id
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

The claim update and its `JOB_SCHEDULER_ITEM_RETRIEVED` outbox record must commit in the same
database transaction. Equivalent atomic behavior is acceptable.

### 5.2 `scheduler_attempts`

There must be at most one scheduler attempt row per `(job_id, attempt_number)`. It records
claims, execution timing, outcome, and retry information needed by the scheduler itself.
It is not the API's evidence store and must not be exposed as visibility history.

### 5.3 `scheduler_outbox`

Every scheduler mutation that produces a lifecycle fact must write an outbox row in the
same transaction as that mutation. The table must include:

- `event_id` as its primary key.
- Kafka topic and message key.
- Canonical EDR payload and schema version.
- Creation time.
- Publication state, attempt count, last error, and next-attempt time.
- Published time when Kafka acknowledges the record.

The publisher may publish a record more than once if it crashes after Kafka accepts a
message but before `published_at` commits. It must never mark a record published before the
broker acknowledges it. Retry must use bounded exponential backoff with jitter.

Creation, acknowledgement, retrieval, start, outcome, retry, exhaustion, and cancellation
events are all subject to the transactional-outbox rule when they originate with the job
service or scheduler.

### 5.4 Cassandra demonstration dataset

Cassandra is a third, worker-owned workload store. The initial keyspace and table are:

```sql
CREATE KEYSPACE IF NOT EXISTS worker_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS worker_demo.records_by_bucket (
    dataset_id text,
    bucket int,
    record_id bigint,
    input_number int,
    fibonacci_result text,
    checksum bigint,
    pending_operation_id uuid,
    last_operation_id uuid,
    updated_at timestamp,
    PRIMARY KEY ((dataset_id, bucket), record_id)
);

CREATE TABLE IF NOT EXISTS worker_demo.update_operations_by_bucket (
    dataset_id text,
    bucket int,
    record_id bigint,
    operation_id uuid,
    previous_checksum bigint,
    new_checksum bigint,
    fibonacci_result text,
    applied_at timestamp,
    PRIMARY KEY ((dataset_id, bucket), record_id, operation_id)
);
```

`SimpleStrategy` and replication factor one are local-development defaults only. Production
topology and replication require environment-specific configuration.

Seed tooling must create a named, versioned dataset with enough rows for the configured
maximum test read. The handler selects a reproducible pseudo-random sequence of buckets and
record ranges from a stored seed. It must not attempt server-side random sampling, unbounded
partition scans, `ALLOW FILTERING`, or cross-database joins.

The same `(datasetId, recordCount, seed)` must select the same logical records on every
retry. This makes retries and acceptance tests reproducible while still distributing reads
across Cassandra partitions. Seeded `input_number` values are non-negative and bounded by a
deployment limit, initially `10000`.

`checksum` is a normal `bigint` version, not a Cassandra counter. Every distinct logical job
may increment the selected record from `N` to `N+1` at most once. The stable operation ID is
derived from the job identity and handler contract, not the attempt number, so all retries
reuse it. The operation-marker and record mutation must use a same-partition conditional
batch or an equivalent lightweight-transaction workflow proven by integration tests.

## 6. Kafka Contract

### 6.1 Topics

| Topic | Purpose |
| --- | --- |
| `job-lifecycle-edr.v1` | Canonical scheduler and execution lifecycle EDRs |
| `job-lifecycle-edr.v1.dlq` | Records the sink cannot deserialize, validate, or persist |

Topic names must be configurable. Production topics must have replication and minimum
in-sync replica settings appropriate to the environment. Local development may use one
broker and replication factor one.

### 6.2 Key and partitioning

- The Kafka record key must be `jobId`.
- All events for one job therefore enter the same partition.
- Consumers must not assume ordering across different jobs or partitions.
- Producers must enable idempotence and acknowledgements from all configured in-sync
  replicas.
- Domain logic must still tolerate out-of-order events because separate producers, retries,
  and historical replay can violate lifecycle order.

### 6.3 Value schema

The value is the canonical EDR from Spec 001 plus `schemaVersion`:

```json
{
  "schemaVersion": 1,
  "eventId": "evt-001",
  "eventType": "JOB_CREATED",
  "eventTime": "2026-07-22T10:00:00Z",
  "ingestionTime": "2026-07-22T10:00:01Z",
  "jobId": "job-123",
  "correlationId": "subscription-456:RENEW:2026-07-23",
  "jobType": "RENEW_SUBSCRIPTION",
  "sourceSystem": "subscription-service",
  "schedulerReference": null,
  "scheduledAt": "2026-07-23T00:00:00Z",
  "attemptNumber": 0,
  "retryable": null,
  "maxAttempts": 3,
  "nextRetryAt": null,
  "resultCode": null,
  "errorCode": null,
  "errorMessage": null,
  "traceId": "trace-789",
  "payloadReference": "subscription-456"
}
```

The existing polling metadata remains optional. Unknown additive fields must be retained in
the raw payload and ignored safely by older projectors. Removing a field, changing its type,
or changing its meaning requires a new schema version and a documented migration path.

The wire format must expose a Kafka Connect schema, either through JSON Schema/Avro with a
schema registry or schema-enabled Kafka Connect JSON. Schemaless JSON is not sufficient for
the JDBC sink contract.

### 6.4 Record identity

- `eventId` identifies one immutable fact and is globally unique.
- Re-delivery of the same `eventId` with the same canonical payload is an exact duplicate.
- Reuse of an `eventId` with a different canonical payload is an identity collision. It
  must not overwrite the original EDR and must raise an operational alert.
- Kafka topic, partition, and offset must be retained with each persisted event for audit
  and replay diagnosis.

## 7. EDR Database and Kafka Sink

### 7.1 `edr_events`

The JDBC sink must persist every accepted record to an immutable journal containing at
least:

- Canonical EDR columns used for search and projection.
- The complete source payload as `jsonb`.
- `schema_version`.
- `payload_hash` calculated from canonical content.
- `kafka_topic`, `kafka_partition`, and `kafka_offset`.
- `persisted_at`, assigned by the EDR database.

Required constraints are:

- Primary key on `event_id`.
- Unique constraint on `(kafka_topic, kafka_partition, kafka_offset)`.
- UTC-aware timestamps.
- Non-negative attempt numbers and positive maximum attempts.
- An index on `(job_id, event_time, event_id)`.
- An index on `persisted_at` for projection scans.

The connector must use record-value fields for the event primary key and an idempotent
insert/upsert strategy. Database protection must ensure an exact duplicate is a no-op and a
different payload for an existing `event_id` cannot mutate the original row. Invalid or
colliding records must be sent to the dead-letter topic with enough error context to
diagnose the source record.

### 7.2 Kafka Connect JDBC sink

The connector configuration must:

- Subscribe only to the configured lifecycle topic.
- Target only the EDR database.
- Use `eventId` as the database record identity.
- Persist Kafka coordinates, using supported metadata fields or transforms.
- Avoid automatic destructive schema evolution.
- Use bounded retries for transient PostgreSQL failures.
- Send non-retryable records to the configured dead-letter topic.
- Emit connector and task health metrics.

Application startup must not silently create or alter sink tables. EDR database migrations
own the schema; the connector validates compatibility and fails visibly when the schema is
not compatible.

### 7.3 Projection tables

The EDR database also owns:

- `projected_events`, with one row per applied `event_id`.
- `job_visibility`, the current durable materialized view.
- `job_attempts`, unique on `(job_id, attempt_number)`.
- `projection_decisions`, including exact duplicate, semantic duplicate, applied, ignored,
  backfilled, and conflicting decisions.
- `reconciliation_findings`, including resolved finding history.

Raw EDR rows and projection rows must use separate tables. The sink writes raw EDR rows
only; it does not update materialized job state.

## 8. Projection and Query Flow

The projection worker reads unprojected `edr_events` from the EDR database in stable
`persisted_at, event_id` order. Multiple workers may claim batches using `FOR UPDATE SKIP
LOCKED` or equivalent leases.

For each event, one EDR database transaction must:

1. Claim or insert its `projected_events` identity.
2. Load the relevant job visibility and attempt rows.
3. Apply the deterministic lifecycle rules from Spec 001.
4. Persist the projection decision and findings.
5. Update the projection using optimistic concurrency.
6. Mark the event projected.

A crash before commit leaves the event eligible for replay. A crash after commit is a
no-op on replay because `projected_events.event_id` is unique.

The existing visibility endpoints must read from the EDR database:

- `GET /scheduled-jobs/{jobId}`
- `GET /scheduled-jobs/{jobId}/attempts`
- `GET /scheduled-jobs` with the Spec 001 filters
- `POST /reconciliation-runs`

`POST /edrs`, when retained as a development or integration endpoint, must validate and
publish to Kafka. It must return `202 Accepted` after broker acknowledgement; it must no
longer apply an event directly to process memory. Read-after-write is therefore eventually
consistent.

Every visibility response must expose `dataAsOf`. Operational deployments should also
expose the most recently persisted and projected Kafka coordinates or equivalent lag data
through diagnostics, without treating them as business fields.

## 9. Transaction and Delivery Semantics

The end-to-end guarantee is **at-least-once delivery with idempotent persistence and
projection**, not global exactly-once processing.

Required invariants:

- A committed scheduler change and its outbox event cannot be separated.
- A broker or publisher retry may create duplicate Kafka records.
- Duplicate `eventId` values do not create duplicate raw facts, attempts, or projection
  versions.
- No Kafka offset is acknowledged by the sink before its database transaction commits.
- No raw event is marked projected before all projection writes commit.
- Per-job Kafka ordering is helpful but not required for domain correctness.
- Terminal-state and late-evidence behavior remains as specified in Spec 001.
- Database timestamps do not replace `eventTime` or `ingestionTime`; all three have distinct
  meanings.

There is no dual write from scheduler code to Kafka and PostgreSQL. The scheduler writes
PostgreSQL plus its local outbox, and the outbox publisher writes Kafka.

## 10. Service Behaviour

### 10.1 Startup and shutdown

- Services must validate configuration and database migration versions before accepting
  traffic.
- Readiness must remain false when a required database, Kafka, or connector dependency is
  unavailable for the service's role.
- Workers must stop claiming new work during shutdown and complete or safely release the
  current transaction.

### 10.2 Scheduler recovery

- Expired claims are recoverable after the configured lease duration.
- Claim recovery must not create a second logical attempt unless policy explicitly starts a
  retry.
- Any recovered lifecycle fact is written through the outbox.

### 10.3 Demonstration execution worker

The first durable implementation must include a simple worker so that persistence and Kafka
are exercised with real job execution rather than database-only state changes.

The poller selects jobs whose `available_at` has passed, claims each selected attempt, and
hands it to the worker. The worker must provide these handlers:

- `PRINT`: write a configured message to structured application output and return success.
- `FIBONACCI`: calculate the Fibonacci sequence up to and including the greatest value not
  exceeding a configured limit. The default and maximum demonstration limit is `10000`;
  the final value for that limit is `6765`.
- `CASSANDRA_FIB_UPDATE`: read a configured large `X` records from the seeded Cassandra
  dataset, find the greatest `input_number` (breaking ties by smallest `record_id`), compute
  the Fibonacci number at that index, wait for a configured processing delay, then update
  that selected record and increment its checksum exactly once.

`PRINT` and `FIBONACCI` are success-only smoke handlers. They do not contain artificial
business failure switches. Failure and retry acceptance tests use real Cassandra client or
network failures.

Example:

```json
{
  "handler": "CASSANDRA_FIB_UPDATE",
  "datasetId": "demo-v1",
  "recordCount": 10000,
  "seed": 42,
  "pageSize": 500,
  "processingDelayMs": 2000,
  "requestTimeoutMs": 1000,
  "executionTimeoutMs": 30000
}
```

Initial execution is attempt 1. A retry claim atomically advances the job to the next attempt
number and inserts the corresponding unique scheduler-attempt row.

`recordCount`, input number, page size, processing delay, per-request timeout,
whole-execution timeout, and concurrency must have deployment-configured upper bounds. A
seeded selection prevents test randomness from making results irreproducible. The worker
must stream the maximum selection rather than retain all record bodies. Fibonacci is
defined as `F(0)=0`, `F(1)=1`, calculated with a bounded algorithm such as fast doubling,
and stored as a decimal string.

Execution uses three boundaries:

1. In a scheduler-database transaction, change the claimed attempt to `RUNNING`, record its
   start time, and write `JOB_EXECUTION_STARTED` to the outbox.
2. Run the handler outside the database transaction: read `X`, choose the maximum, calculate
   Fibonacci, wait the configured delay, and conditionally update Cassandra.
3. In a new scheduler-database transaction, persist exactly one outcome for the attempt and
   write the corresponding outcome/retry EDRs to the outbox.

Long reads must renew their claim lease with short scheduler-database heartbeat transactions.
Each claim has an opaque fencing token. Heartbeat and outcome updates must match the current
`job_id`, attempt number, worker ID, and fencing token. If the lease or token is lost, the old
worker must stop and cannot commit a late outcome.

Immediately before the Cassandra mutation, the worker must confirm that its scheduler claim
token is still current. Cassandra and PostgreSQL cannot share a transaction, so this check
does not prevent every race. Instead, the stable operation ID makes the Cassandra effect
idempotent: a recovered worker observes an already-applied operation as success and does not
increment the checksum again. A stale worker still cannot commit the scheduler outcome.

The Cassandra mutation protocol must:

1. Use the job's stable operation ID across all attempts.
2. Reserve the selected row conditionally against its observed checksum and pending
   operation state.
3. Update `fibonacci_result`, set `checksum = previous_checksum + 1`, record
   `last_operation_id`, clear the reservation, and retain an operation marker.
4. On a write timeout or connection loss, reread the row and operation marker before deciding
   whether to retry the mutation.
5. Treat an existing matching operation marker as successful completion.
6. Resolve a competing checksum/reservation change with bounded reread/recompute attempts or
   a retryable scheduler failure.

Blind increments and Cassandra counter columns are forbidden because a timeout leaves the
client unable to know whether a counter mutation was applied.

On success, the worker must:

- Verify that `CASSANDRA_FIB_UPDATE` has stored the expected Fibonacci result and changed the
  selected record checksum by exactly one for the logical operation.
- Store a small scheduler-attempt result summary with `rowsRead`, selected record identity,
  maximum input, previous/new checksum, operation ID, processing delay, and a hash or digit
  count of the Fibonacci result. The full result remains in Cassandra.
- Set the scheduler job and attempt to `SUCCEEDED`.
- Clear claim/lease and retry fields.
- Write `JOB_EXECUTION_SUCCEEDED` with result code `CASSANDRA_FIB_UPDATED` and a payload
  reference identifying the updated Cassandra row when this handler is used.
- Never schedule a retry for that successful attempt.

On failure before `max_attempts`, the worker must:

- Set the attempt to `FAILED` and the scheduler job to `RETRY_WAIT`.
- Clear the current claim and set `available_at` to the calculated retry time.
- Write `JOB_EXECUTION_FAILED` with `retryable=true`, followed by retry-requested and
  retry-acknowledged evidence representing the locally accepted retry.
- Leave the job ineligible until `available_at` is reached; the same polling path then claims
  the next attempt.

Retryable Cassandra failures include client operation timeout, server read/write timeout,
unavailable replicas, connection loss, unresolved conditional conflict, and execution
deadline expiry. Invalid job payload, unknown dataset, incompatible schema, invalid input
number, or a row-count integrity mismatch is non-retryable unless an explicit policy says
otherwise. Driver-level retries must be bounded so they do not multiply scheduler retry
attempts invisibly.

On a non-retryable failure or failure at `max_attempts`, the worker must set a terminal
`FAILED` or `RETRIES_EXHAUSTED` scheduler state, clear its claim, and emit the matching
terminal evidence without scheduling another retry.

Handler exceptions must be classified and converted to a redacted error code/message before
the scheduler transition. The worker must never hold a scheduler row lock while printing,
calculating Fibonacci numbers, delaying, or reading/updating Cassandra.

### 10.4 Backpressure

- Scheduler commits must not depend synchronously on EDR database availability.
- An unavailable Kafka cluster causes outbox backlog, not scheduler transaction loss.
- An unavailable EDR database causes Kafka consumer lag, not deletion of Kafka records.
- Operators can configure thresholds that fail readiness or raise alerts for outbox age,
  Kafka lag, projection lag, and DLQ growth.

## 11. Configuration and Secrets

At minimum, configuration must distinguish:

```text
SCHEDULER_DATABASE_URL
EDR_DATABASE_URL
KAFKA_BOOTSTRAP_SERVERS
KAFKA_EDR_TOPIC
KAFKA_EDR_DLQ_TOPIC
KAFKA_CONSUMER_GROUP or connector name
OUTBOX_BATCH_SIZE
OUTBOX_RETRY settings
PROJECTION_BATCH_SIZE
SCHEDULER_CLAIM_LEASE_SECONDS
CASSANDRA_CONTACT_POINTS
CASSANDRA_KEYSPACE
CASSANDRA_CONSISTENCY
CASSANDRA_REQUEST_TIMEOUT_MS
CASSANDRA_EXECUTION_TIMEOUT_MS
CASSANDRA_MAX_RECORD_COUNT
CASSANDRA_MAX_INPUT_NUMBER
CASSANDRA_PAGE_SIZE
CASSANDRA_READ_CONCURRENCY
CASSANDRA_MAX_PROCESSING_DELAY_MS
CASSANDRA_CONDITIONAL_RETRY_LIMIT
```

The two database URLs must not resolve to the same logical database outside test fixtures
that explicitly verify rejection. Passwords, Kafka credentials, and TLS private material
must come from the runtime secret mechanism and must never be committed or logged.

Local development must provide a reproducible container environment with separate
scheduler and EDR databases, Kafka in KRaft mode or an equivalent supported setup, and
Kafka Connect with the JDBC driver installed. It must also include a pinned Cassandra node,
repeatable dataset seeding, and a controllable network fault proxy between the worker and
Cassandra for chaos tests.

## 12. Migrations and Compatibility

- Scheduler and EDR databases have independent migration histories.
- Migrations run as explicit deployment steps, not concurrently from every application
  replica.
- Schema changes follow expand/migrate/contract ordering.
- A projector deployment must be able to read the current schema version and at least the
  immediately preceding supported version during a rolling deployment.
- Topic and database retention must be configured independently and documented.
- Rebuilding projections from retained `edr_events` must not require access to the
  scheduler database.

No migration from existing process memory is required. A development process restarted
after this feature is enabled begins from its configured durable databases.

## 13. Observability and Operations

The implementation must expose structured logs and metrics for:

- Unpublished outbox row count and oldest-row age.
- Publish success, retry, and permanent-failure counts.
- Kafka producer errors and latency.
- Connector/task status, batch failures, retries, and DLQ writes.
- Kafka consumer lag per partition.
- Raw EDR persist rate and age of the newest persisted event.
- Projection throughput, failure count, conflict retries, and oldest unprojected-event age.
- Scheduler eligible-job count, claim count, claim age, and expired-claim recovery.
- Database connection-pool saturation and query latency.
- Cassandra rows/pages read, selected maximum, Fibonacci calculation/delay/update latency,
  checksum changes, conditional conflicts, operation-marker recovery, timeouts,
  unavailable/connection failures, active requests, and retry classification.
- Claim heartbeat success/failure and stale-worker outcome rejections.

Logs must include `eventId`, `jobId`, and `traceId` when available, and Kafka coordinates
after publication. Payloads and error messages must be redacted according to the configured
data policy.

Required health surfaces are liveness, readiness, and dependency diagnostics. A running
process with a failed connector task is not considered a healthy EDR ingestion pipeline.

## 14. Security

- Scheduler credentials have no access to the EDR database.
- Kafka Connect credentials can insert raw EDRs but cannot update scheduler data or
  projection tables.
- Cassandra worker credentials can select the dataset and conditionally update only the
  result/checksum/operation fields and operation-marker table; seed tooling uses a separate
  schema/data-management identity.
- Projection credentials can read raw EDRs and update projection tables but cannot mutate
  raw EDR content.
- Visibility API credentials are read-only except for the reconciliation operation when it
  is hosted in the same service.
- Connections use TLS in non-local environments.
- Kafka and database authorization use least-privilege service identities.
- Raw payload retention and deletion policy must account for sensitive business data.

## 15. Failure Scenarios

### PERSIST-01 — Process restart

Create and project a job, restart the API and workers, and verify that its visibility and
attempt history are unchanged.

### PERSIST-02 — Concurrent pollers

Run two pollers against more than one batch of eligible rows. Each job is claimed once per
attempt, and every committed retrieval has one logical EDR.

### WORKER-01 — Demonstration handler succeeds

Submit a `FIBONACCI` job with limit `10000`. The worker calculates through `6765`, commits a
successful attempt and job, emits start and success EDRs, and does not create a retry.

Submit a `CASSANDRA_FIB_UPDATE` job for 10,000 seeded records. It reads exactly that count in
bounded pages, selects the expected maximum/tie-break record, computes its Fibonacci value,
waits the configured delay, updates the result, increments checksum exactly once, commits
success, and does not create a retry.

### WORKER-02 — Cassandra read times out, then succeeds

Add proxy latency greater than the Cassandra request/execution timeout for attempt 1. The
worker records a retryable timeout, sets `RETRY_WAIT` and a future `available_at`, and does
not reclaim it early. Remove the fault before the retry time. Attempt 2 reads the same
seeded records, performs one checksum increment, and the durable visibility projection
becomes `SUCCEEDED` with two attempts.

### WORKER-03 — Cassandra remains unavailable through retry exhaustion

Cut the worker-to-Cassandra connection for every attempt. The job runs no more than
`max_attempts`, finishes in `RETRIES_EXHAUSTED`, and has no future eligibility time or active
claim.

### WORKER-04 — Cassandra reply arrives after claim loss

Delay a Cassandra response beyond the claim lease and prevent heartbeat renewal. Another
worker recovers the claim. The old worker's fencing token is rejected and it cannot commit a
late scheduler success or failure over the recovered attempt. If the Cassandra operation was
already applied, the recovered worker detects its stable operation marker and does not
increment checksum twice.

### WORKER-05 — Cassandra update times out after applying

Cause the conditional update response to be lost after Cassandra applies it. The worker
treats the outcome as unknown, rereads the selected row and operation marker, recognizes the
completed logical operation, increments checksum only once, and safely commits success.

### WORKER-06 — Concurrent checksum conflict

Two jobs select the same maximum record. Only one observed checksum transition wins. The
other job performs a bounded reread/recompute and applies its distinct operation exactly
once, or records a retryable conflict without a blind increment.

### KAFKA-01 — Publisher crash after broker acknowledgement

Crash after Kafka accepts an EDR but before the outbox is marked published. The event is
republished, stored once by `eventId`, and projected once.

### KAFKA-02 — Kafka unavailable

Scheduler transactions continue to commit with outbox rows. After Kafka recovers, the
backlog publishes without loss and within the configured recovery objective.

### SINK-01 — EDR database unavailable

The sink retries and Kafka retains the records. After recovery, every record is persisted;
the visibility API reports stale `dataAsOf` while lag exists.

### SINK-02 — Poison record

An incompatible or invalid record is written to the DLQ with error context. Later valid
records continue to flow according to connector policy, and an alert is raised.

### SINK-03 — Event identity collision

Two records use one `eventId` with different payloads. The first immutable event remains
unchanged; the conflicting record reaches the DLQ or quarantine path and raises an alert.

### PROJ-01 — Projector crash before commit

The event remains eligible, is replayed after restart, and produces one committed decision.

### PROJ-02 — Out-of-order persisted events

Deliver lifecycle events out of order across separate producer sessions. The final state,
backfilled timestamps, findings, and terminal precedence match Spec 001.

### PROJ-03 — Projection rebuild

Clear only rebuildable projection tables in an isolated test database and replay the raw
EDR journal. The resulting business state and attempts are equivalent to the original
projection.

### ISOLATION-01 — Database boundary

Make the scheduler database unavailable while leaving the EDR database available, then
reverse the failure. Each component fails only for its documented dependency, and neither
service attempts a cross-database fallback.

## 16. Testing Strategy

### Unit tests

- Scheduler repository eligibility, ordering, leasing, and optimistic locking.
- Outbox serialization and retry classification.
- EDR schema compatibility and payload hashing.
- Projector idempotency using durable repository interfaces.
- Configuration rejects one logical database for both roles.

### PostgreSQL integration tests

- Independent migrations for both databases.
- `FOR UPDATE SKIP LOCKED` with concurrent pollers and projectors.
- Outbox mutation atomicity.
- EDR immutability, uniqueness constraints, and identity collisions.
- Optimistic update conflicts and transaction rollback.

### Kafka integration tests

- Real producer publication and per-job partitioning.
- Real Kafka Connect JDBC sink into the EDR database.
- Duplicate delivery, connector restart, database outage, and DLQ handling.
- Persistence of Kafka topic, partition, and offset.

### Cassandra and worker integration tests

- Idempotent keyspace/table setup and deterministic dataset seeding.
- Reproducible `(datasetId, recordCount, seed)` selection, maximum/tie-break choice, and
  Fibonacci result.
- Bounded paging, concurrency, memory, Fibonacci input, processing delay, request timeout,
  and whole-execution deadline.
- Exactly-once logical checksum increment using operation identity, conditional reservation,
  and operation-marker recovery; no Cassandra counter column.
- Real read/write timeout, lost update response, unavailable, connection, and conditional
  conflict failures through a network fault proxy and concurrent workers.
- Claim heartbeat, fencing-token rejection, retry timing, and retry exhaustion.

Mocks are insufficient for Kafka or Cassandra chaos acceptance tests. Tests may use
Testcontainers or the repository's container composition and must use bounded polling with
explicit timeouts, not arbitrary sleeps.

### End-to-end tests

- Submit a durable job, poll and execute it, observe its outbox EDRs in Kafka, verify them
  in the EDR database, and query the durable visibility API.
- Restart every application component without losing scheduler or visibility state.
- Run the failure scenarios in section 15.
- Run representative Spec 001 happy-path, chaos, and polling scenarios through the durable
  adapters.
- Run `CASSANDRA_FIB_UPDATE` success, timeout-then-recovery, unknown write outcome,
  conditional conflict, sustained outage, and stale-worker fencing through the complete
  EDR/visibility path.

## 17. Acceptance Criteria

The feature is complete when:

- Scheduler jobs survive process and container restarts.
- Raw EDRs and visibility projections survive process and container restarts.
- Scheduler data and EDR data use different logical PostgreSQL databases, credentials, and
  migration histories.
- No application transaction spans both databases.
- No transaction spans PostgreSQL and Cassandra; the Cassandra read/compute/delay/update
  happens between scheduler transactions and is reconciled by stable operation identity.
- All scheduler-originated EDRs use the transactional outbox and Kafka path.
- Kafka Connect JDBC sink, not application code, persists raw Kafka EDRs into `edr_events`.
- The sink is idempotent by `eventId` and cannot overwrite an event with different content.
- Duplicate delivery does not duplicate attempts, decisions, or projection versions.
- Kafka coordinates are retained for every persisted EDR.
- The visibility API reads durable projections only and exposes eventual-consistency
  freshness.
- Concurrent pollers do not double-claim a job attempt.
- A due `PRINT` or `FIBONACCI` job is executed by a real worker and reaches a durable
  scheduler outcome.
- A due `CASSANDRA_FIB_UPDATE` job reads reproducible `X` records, finds the deterministic
  maximum, computes Fibonacci, delays, updates that record, and increments checksum exactly
  once using bounded resources.
- Successful execution emits success evidence and never schedules a retry.
- Retryable failure becomes ineligible until its future `available_at`, then runs the next
  attempt; exhausted failure never runs again.
- Cassandra read/write timeout, unknown write outcome, conditional conflict, unavailability,
  connection loss, recovery, and stale-worker fencing are covered by automated chaos tests.
- Concurrent projection workers do not lose or double-apply events.
- Kafka, sink, database, and projector outages recover without losing committed facts.
- Poison and identity-collision records are observable through a DLQ or quarantine path.
- Projection state can be rebuilt solely from the EDR database.
- Unit, PostgreSQL integration, Kafka integration, and end-to-end tests pass in CI or the
  documented infrastructure test workflow.
- Local setup, migrations, connector deployment, health checks, and teardown are documented
  and reproducible.

## 18. Out of Scope

- Replacing the external scheduler with Kafka consumers.
- Using Cassandra as scheduler, outbox, EDR, or visibility persistence.
- Cassandra mutations outside the demonstration result/checksum and operation-marker
  workflow.
- Global exactly-once delivery across PostgreSQL and Kafka.
- Cross-database foreign keys, joins, or distributed transactions.
- A general-purpose event schema registry for events unrelated to scheduled jobs.
- Multi-region active-active PostgreSQL or Kafka deployment.
- Production sizing, retention periods, recovery objectives, and cloud-provider selection;
  these must be supplied as deployment configuration and operational policy.
