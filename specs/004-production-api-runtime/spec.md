# Production API and Scheduler Runtime Specification

Status: proposed
Depends on: [`../002-real-persistence-kafka/spec.md`](../002-real-persistence-kafka/spec.md),
[`../003-production-hardening-resilience/spec.md`](../003-production-hardening-resilience/spec.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Architecture and Authority Boundaries](#3-architecture-and-authority-boundaries)
- [4. Runtime Roles](#4-runtime-roles)
- [5. Scheduler HTTP Contract](#5-scheduler-http-contract)
- [6. Scheduler Worker Behaviour](#6-scheduler-worker-behaviour)
- [7. Visibility HTTP Contract](#7-visibility-http-contract)
- [8. Lifecycle and Shutdown](#8-lifecycle-and-shutdown)
- [9. Configuration](#9-configuration)
- [10. Health, Metrics, and Logging](#10-health-metrics-and-logging)
- [11. Security](#11-security)
- [12. Failure and Recovery Behaviour](#12-failure-and-recovery-behaviour)
- [13. Testing and Evidence](#13-testing-and-evidence)
- [14. Compatibility and Rollout](#14-compatibility-and-rollout)
- [15. Acceptance Criteria](#15-acceptance-criteria)
- [16. Out of Scope](#16-out-of-scope)

## 1. Purpose

Make the durable architecture delivered by Specs 002 and 003 runnable as application
services rather than only through tests or direct Python composition.

A client must be able to submit a scheduled job over HTTP, allow a bounded polling worker to
claim and execute it, and retrieve the resulting visibility projection over HTTP. Runtime
composition must preserve the existing database authority boundary and transactional-outbox
delivery path.

## 2. Scope

This specification covers:

- production application factories for scheduler and visibility APIs;
- a durable scheduler/worker process role;
- dependency creation and cleanup through ASGI lifespan;
- the existing scheduler submission and visibility query HTTP contracts;
- role-specific configuration, credentials, health, metrics, and shutdown;
- local Compose services and documented developer commands;
- end-to-end HTTP evidence from submission through projected visibility; and
- deployment and rollout requirements for independently scalable roles.

The specification wires existing durable services. It does not replace the scheduler,
outbox, Kafka Connect, journal, reducer, or Cassandra correctness models.

## 3. Architecture and Authority Boundaries

```text
Client
  |
  | POST /scheduler/jobs
  v
Scheduler API -----> scheduler PostgreSQL <----- Scheduler Worker
                          |
                          | scheduler_outbox
                          v
                    Outbox Publisher
                          |
                          v
                        Kafka
                          |
                          v
                     Kafka Connect
                          |
                          v
                    EDR PostgreSQL <----- Projector
                          |
                          v
Visibility API <---- GET /scheduled-jobs...
```

Production deployment must use separate API roles:

| Role | Required authority | Prohibited authority |
| --- | --- | --- |
| Scheduler API | Scheduler PostgreSQL write role | EDR PostgreSQL and Cassandra |
| Scheduler worker | Scheduler PostgreSQL worker role; handler-specific Cassandra access | EDR PostgreSQL |
| Publisher | Scheduler outbox read/update role; Kafka producer | EDR PostgreSQL |
| Visibility API | EDR PostgreSQL read role | Scheduler PostgreSQL and Cassandra |
| EDR ingress API, when enabled | Kafka producer | Both PostgreSQL databases |
| Projector | EDR journal/projection role | Scheduler PostgreSQL and Cassandra |

A combined developer application may expose all routes only when an explicit non-production
mode is selected. Production configuration must reject a combined role. The visibility API
must never answer from scheduler tables, including while the EDR pipeline is delayed.

## 4. Runtime Roles

The installed command interface must expose these long-running roles:

```text
job-visibility-api scheduler
job-visibility-api visibility
job-visibility-api edr-ingress        optional
job-visibility-runtime scheduler
job-visibility-runtime publisher
job-visibility-runtime projector
job-visibility-runtime rebuild --once
```

Equivalent subcommand names are acceptable if they remain explicit and documented.

### 4.1 Scheduler API

The scheduler API constructs one scheduler engine/session factory and one
`SchedulerService`. It exposes scheduler submission plus health, readiness, metrics, and
OpenAPI endpoints. It does not construct an EDR engine or durable visibility reader.

### 4.2 Visibility API

The visibility API constructs only the EDR engine/session factory and a
`DurableVisibilityReader`. It exposes job visibility queries, attempt queries,
reconciliation findings, lifecycle taxonomy, health, readiness, metrics, and OpenAPI.

### 4.3 EDR ingress API

Direct `POST /edrs` is an optional adapter for external EDR producers. In production it must
publish the canonical event to Kafka and return asynchronous acceptance. It must not write
the EDR journal directly. Deployments that accept EDRs only from internal publishers may
disable this role.

### 4.4 Scheduler worker

The scheduler runtime repeatedly:

1. recovers a bounded batch of expired claims;
2. claims a bounded ordered batch of eligible jobs;
3. executes each claim through the registered handler; and
4. waits for the configured poll interval when no work is available.

Worker replicas use distinct owner identities. PostgreSQL locking and fencing remain the
coordination mechanism; there is no process-local leader election.

## 5. Scheduler HTTP Contract

### 5.1 Submit a job

```http
POST /scheduler/jobs
Content-Type: application/json
```

Request:

```json
{
  "jobId": "job-1001",
  "correlationId": "order-789",
  "jobType": "FIBONACCI",
  "scheduledAt": "2026-08-21T12:00:00Z",
  "payload": {"limit": 10000},
  "payloadReference": null,
  "maxAttempts": 3
}
```

Requirements:

- `jobId`, `correlationId`, `jobType`, and timezone-aware `scheduledAt` are required.
- `maxAttempts` is a positive bounded integer.
- Payload size is bounded by configuration and rejected before a database transaction.
- Unknown job types may be rejected at submission or accepted and terminally rejected by the
  worker; the selected policy must be explicit and consistent. The initial implementation
  rejects unknown types at submission.
- The submitted job and initial `JOB_CREATED` and
  `JOB_SCHEDULER_SUBMISSION_REQUESTED` outbox rows commit atomically.

Responses:

- `201 Created` when the job ID is inserted.
- `200 OK` with `created: false` when an identical job ID already exists.
- `409 Conflict` when the job ID exists with a materially different immutable submission.
- `422 Unprocessable Entity` for invalid fields or unsupported job type.
- `413 Content Too Large` when the configured payload limit is exceeded.
- `503 Service Unavailable` when scheduler storage is unavailable or its operation deadline
  expires.

The success response includes `jobId`, `created`, and a stable status URL. It must not claim
that the job has executed or that its EDR is already visible.

### 5.2 Submission idempotency

`jobId` is the idempotency key. A replay is considered identical only when the immutable
submission fields match: correlation ID, type, scheduled time, payload or reference, and
maximum attempts. The API must not silently treat a conflicting payload as success.

## 6. Scheduler Worker Behaviour

Worker configuration includes batch size, idle poll interval, claim lease duration,
heartbeat interval, expired-claim recovery batch, concurrency, and shutdown deadline. All
values are finite and validated together.

The initial worker may execute a claimed batch sequentially. If concurrency is added, it is
bounded and shutdown still waits only until the configured deadline.

For every claimed job the worker must:

- start and finish using the current owner, attempt number, fencing token, and unexpired
  lease;
- heartbeat before lease expiry for handlers that can exceed one heartbeat interval;
- map classified handler errors to retry or terminal state;
- preserve the exact `maxAttempts` rule; and
- avoid acknowledging success after ownership is stale.

Supported initial handlers are `PRINT` and `FIBONACCI`. Cassandra-backed handlers may be
enabled only when their required configuration and credentials are present.

## 7. Visibility HTTP Contract

The production visibility role exposes:

| Endpoint | Meaning |
| --- | --- |
| `GET /scheduled-jobs/{jobId}` | Current EDR-derived job projection |
| `GET /scheduled-jobs/{jobId}/attempts` | EDR-derived attempt projections |
| `GET /scheduled-jobs` | Search projections by status, correlation, and schedule range |
| `GET /edr-lifecycle` | Static event taxonomy |
| `POST /reconciliation-runs` | Return or refresh durable consistency findings, as documented |

A missing projection returns the existing qualified `404`; it does not imply that the
scheduler lacks the job. Responses retain `dataAsOf`, projection version, findings, and
freshness meaning.

This specification does not add raw journal query endpoints. Direct EDR submission remains
on the optional ingress role, not the visibility reader role.

## 8. Lifecycle and Shutdown

ASGI factories must use application lifespan to create dependencies once per process and
close them once during shutdown. Importing an application module must not connect to a
database, Kafka, Schema Registry, or Cassandra.

Shutdown order is role-specific:

- APIs stop accepting new requests, allow bounded in-flight completion, then dispose pools.
- Scheduler workers stop claiming, allow current fenced work until the deadline, then leave
  unfinished claims for lease recovery.
- Publishers stop leasing, flush Kafka within the configured bound, and leave
  unacknowledged rows unpublished.
- Projectors finish or roll back the current transaction before disposing the EDR pool.

SIGTERM and SIGINT must result in the same safe lifecycle behavior and a zero exit status
when shutdown completes normally.

## 9. Configuration

Configuration is environment-driven and validated before the process becomes ready.

Required role-specific settings include:

- application role and environment mode;
- bind host, port, request/payload limits, and graceful-shutdown deadline;
- scheduler or EDR database URL as appropriate;
- scheduler batch, polling, lease, heartbeat, concurrency, and recovery bounds;
- Kafka and Schema Registry settings for producer roles;
- handler enablement and Cassandra credentials for Cassandra workers; and
- existing database, Kafka, queue-age, and projection deadlines from Spec 003.

Production startup fails when a role receives prohibited cross-authority credentials, a
combined role is selected, a secret is embedded in loggable configuration, or deadline
relationships are unsafe. Sanitized effective configuration may be logged once at startup.

## 10. Health, Metrics, and Logging

Every long-running HTTP role exposes:

```text
GET /health/live
GET /health/ready
GET /metrics
```

Liveness reports only process responsiveness. Readiness verifies the dependencies required
for that role and relevant backlog-age gates. A scheduler API can remain live while returning
not-ready because scheduler PostgreSQL is unavailable. A visibility API never checks the
scheduler database.

The worker, publisher, and projector expose metrics on processed, succeeded, failed,
retried, stale-fenced, recovered, and in-flight work. Logs include role, instance/owner,
job ID, attempt, correlation ID where allowed, and a classified error code. Logs exclude
payload bodies, database URLs with credentials, and unredacted exception data.

## 11. Security

- Production API roles use distinct least-privilege database credentials.
- Authentication and TLS may terminate at a trusted ingress, but production deployment must
  document that boundary and must not expose an unauthenticated scheduler write endpoint to
  an untrusted network.
- CORS is deny-by-default unless an explicit allow-list is configured.
- Request bodies, identifiers, and headers are bounded before logging or persistence.
- OpenAPI documentation may be disabled in production without disabling API routes.
- Error responses do not expose SQL, credentials, internal hostnames, payloads, or traces.

## 12. Failure and Recovery Behaviour

| Failure | Required result |
| --- | --- |
| Scheduler API exits before commit | Client receives no success; no partial job/outbox fact exists. |
| Scheduler API exits after commit | Retry by `jobId` reports the existing identical submission. |
| Worker exits with a claim | Lease expires; another worker recovers it with a new fencing token. |
| Publisher or Kafka unavailable | Scheduler continues committing; outbox remains durable. |
| Connect or EDR database unavailable | Kafka buffers acknowledged EDRs; visibility may be stale but never fabricated. |
| Projector unavailable | Raw journal grows; visibility reports its last `dataAsOf`; replay catches up. |
| Visibility database unavailable | Visibility readiness fails and reads return bounded `503`. |

All HTTP dependency failures terminate within the configured request deadline. Recovery
must not require manual row edits or cross-database copying.

## 13. Testing and Evidence

Required automated tests include:

1. application-factory tests proving each role constructs only permitted dependencies;
2. route-presence tests proving scheduler and visibility endpoints are not cross-exposed;
3. lifespan tests for startup failure, cleanup, and graceful shutdown;
4. HTTP contract tests for validation, idempotent replay, conflict, and dependency timeout;
5. worker-loop tests for due-time filtering, bounded batches, idle polling, recovery,
   execution, retry, fencing, and signal shutdown;
6. real PostgreSQL integration tests for submission atomicity and concurrent API requests;
7. one real end-to-end test:
   `POST job -> worker -> outbox -> Kafka -> Connect -> projector -> GET visibility`;
8. restart tests for scheduler API and worker at pre/post-commit and active-claim boundaries;
9. role credential tests proving prohibited database access fails; and
10. OpenAPI snapshot or schema assertions for the public contracts.

Tests use unique job IDs, bounded polling, and the existing redacted failure evidence
harness. CI runs unit/contract tests on pull requests and the real HTTP path in the bounded
resilience tier.

## 14. Compatibility and Rollout

Existing simulation factories and tests remain supported. Durable factories are additive;
the module-level simulation `app` must be clearly named or deprecated so operators cannot
mistake it for production wiring.

Roll out in this order:

1. ship role-specific factories and contract tests;
2. deploy visibility API against the existing EDR database;
3. deploy scheduler API with write traffic disabled and validate readiness;
4. deploy one scheduler worker with demonstration handlers;
5. enable submission traffic for a bounded canary set;
6. scale worker and API replicas after queue and freshness metrics remain within objectives;
7. remove any undocumented direct-Python submission procedure from the normal runbook.

Rollback stops the new API/worker roles. Durable jobs, attempts, outbox rows, Kafka records,
journal rows, and projections remain compatible and must not be deleted.

## 15. Acceptance Criteria

Spec 004 is complete when:

- documented commands start role-specific production APIs without custom Python assembly;
- `POST /scheduler/jobs` durably creates one job and its initial outbox EDRs;
- identical submission replay is idempotent and conflicting replay is rejected;
- a standalone scheduler worker claims and executes due jobs with fencing and bounded waits;
- publisher, Connect, and projector deliver the resulting EDR-derived projection;
- the job and attempt GET APIs return that projection through the visibility role;
- production roles receive only their permitted credentials and combined mode is rejected;
- health, readiness, metrics, startup failure, and graceful shutdown are verified;
- the real HTTP end-to-end scenario passes from a clean Compose project; and
- README and operations documentation distinguish simulation and production commands.

## 16. Out of Scope

- Raw EDR journal browsing endpoints such as `GET /edrs`.
- Job cancellation, update, pause, resume, bulk submission, or deletion APIs.
- A general workflow/DAG engine or cron-expression parser.
- Replacing the polling scheduler with timers or a managed scheduling service.
- Distributed transactions across scheduler PostgreSQL, Kafka, or EDR PostgreSQL.
- A new identity provider or API gateway implementation.
- Multi-region active-active APIs or scheduler workers.
- Changing the EDR lifecycle taxonomy or projection precedence rules.
