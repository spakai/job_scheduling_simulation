# Production API and Scheduler Runtime Implementation Plan

Status: proposed
Implements: [`spec.md`](spec.md)
Depends on: merged Specs 002 and 003

## Contents

- [1. Objective](#1-objective)
- [2. Current Baseline](#2-current-baseline)
- [3. Delivery Principles](#3-delivery-principles)
- [4. Target Repository Shape](#4-target-repository-shape)
- [5. Delivery Sequence](#5-delivery-sequence)
- [6. Detailed Phase Plan](#6-detailed-phase-plan)
- [7. Test Matrix](#7-test-matrix)
- [8. CI and Evidence](#8-ci-and-evidence)
- [9. Rollout and Compatibility](#9-rollout-and-compatibility)
- [10. Traceability](#10-traceability)
- [11. Review Gates](#11-review-gates)
- [12. Definition of Done](#12-definition-of-done)

## 1. Objective

Deliver a runnable HTTP-to-worker-to-visibility path while preserving the scheduler/EDR
authority boundary. A developer or deployment system must be able to start each role with a
documented command; clients must not need direct Python service composition.

## 2. Current Baseline

The merged repository already provides:

- `SchedulerService.submit`, claim, fencing, execution, retry, and recovery operations;
- a conditional `POST /scheduler/jobs` route when a scheduler dependency is injected;
- durable visibility GET routes when a reader dependency is injected;
- transactional outbox publication, Kafka Connect journal persistence, and projection;
- publisher, projector, and rebuild runtime roles;
- separate scheduler and EDR PostgreSQL services and credentials;
- health/metrics primitives and bounded dependency configuration; and
- passing resilience tests for database, Kafka, Cassandra, DLQ, and Connect restart faults.

The current gaps are:

| Gap | Consequence |
| --- | --- |
| Module-level `api:app` constructs only the in-memory engine | The documented Uvicorn command is simulation-only. |
| No production application factory or API CLI | Durable dependencies require custom Python assembly. |
| No scheduler worker runtime loop | Submitted durable jobs do not execute automatically. |
| One generic app factory can mix scheduler and EDR dependencies | Production credential boundaries are easy to violate accidentally. |
| Lifespan does not own durable clients | Startup failure and shutdown cleanup are not process contracts. |
| No HTTP-driven durable E2E test | The complete user journey is not release evidence. |

## 3. Delivery Principles

- Preserve role-specific credentials and imports from the first implementation slice.
- Keep the in-memory simulation available but make production commands unmistakable.
- Reuse `SchedulerService`, `DurableVisibilityReader`, `KafkaEdrIngress`, and health helpers;
  do not duplicate business logic in route handlers.
- Build vertical slices that include composition, command, tests, and documentation.
- Use application lifespan for resource ownership and bounded shutdown.
- Add no raw-EDR or job-management features that are unnecessary for runnable production
  wiring.

## 4. Target Repository Shape

Names may vary during implementation, but the intended shape is:

```text
src/job_visibility/
  api.py                         shared request/response models and route builders
  applications/
    scheduler_api.py             scheduler-only ASGI factory
    visibility_api.py            EDR-reader-only ASGI factory
    edr_ingress_api.py           optional Kafka ingress ASGI factory
  runtime.py                     publisher/projector/rebuild roles
  scheduler_runtime.py           bounded claim/execute/recovery loop
  cli/
    api.py                       role-specific Uvicorn launcher
  config.py                      API and worker configuration
  observability/                 role readiness and runtime metrics
tests/
  test_application_factories.py
  test_scheduler_api.py
  test_visibility_api.py
  test_scheduler_runtime.py
  integration/
    test_scheduler_http.py
    test_production_http_path.py
compose.yaml                     optional API/worker/publisher/projector services
docs/
  spec-004-runbook.md
specs/004-production-api-runtime/
  spec.md
  plan.md
```

## 5. Delivery Sequence

```text
Phase 0  Contract and composition decisions
Phase 1  Role-specific configuration and factories
Phase 2  Scheduler submission HTTP hardening
Phase 3  Scheduler worker runtime
Phase 4  Visibility and ingress production roles
Phase 5  Compose and end-to-end HTTP path
Phase 6  Operations, CI evidence, and rollout documentation
```

Phases 2 and 3 may proceed in parallel after the Phase 1 factory interfaces stabilize.

## 6. Detailed Phase Plan

### Phase 0 — Contract and composition decisions

Tasks:

1. Record the production roles and exact credentials each role receives.
2. Decide the CLI names and whether Uvicorn is launched programmatically or with `--factory`.
3. Define identical-versus-conflicting job replay comparison rules.
4. Select maximum request body and job payload sizes.
5. Decide whether direct EDR ingress is deployed by default or remains opt-in.
6. Define worker ownership identity, heartbeat strategy, and signal-shutdown deadline.
7. Confirm the reconciliation endpoint's durable semantics; rename or document it if it only
   reads existing findings.

Exit criteria:

- API role matrix and public endpoint matrix are reviewed.
- No production process requires both scheduler and EDR database credentials.
- HTTP error and idempotency semantics are fixed before implementation.

### Phase 1 — Role-specific configuration and factories

Tasks:

1. Add typed API configuration for environment, role, bind address, port, payload limits,
   OpenAPI, CORS, and graceful shutdown.
2. Add typed scheduler-loop configuration for batch, polling, recovery, lease, heartbeat,
   concurrency, and owner identity.
3. Validate cross-setting relationships and reject combined production mode.
4. Split shared route construction from dependency composition.
5. Implement scheduler, visibility, and optional ingress ASGI factories.
6. Use lifespan context managers to create and dispose engines, producers, and clients.
7. Ensure module import and OpenAPI generation perform no network connection.
8. Add a CLI or documented `uvicorn --factory` commands for every API role.

Exit criteria:

- Factory tests prove each role constructs only permitted adapters.
- Startup errors are classified and secret-safe.
- Repeated lifespan startup/shutdown leaves no open engine or producer.

### Phase 2 — Scheduler submission HTTP hardening

Tasks:

1. Mount `POST /scheduler/jobs` only on the scheduler role.
2. Validate timezone awareness, known handler type, maximum attempts, and payload size.
3. Change repository/service submission to distinguish created, identical replay, and
   conflicting replay without a read/write race.
4. Return `201`, idempotent `200`, `409`, `413`, `422`, and bounded `503` responses.
5. Add a stable `statusUrl` pointing to the visibility API contract without claiming current
   visibility.
6. Add structured request/error logging without payload bodies.
7. Generate and assert the scheduler OpenAPI schema.
8. Add concurrent replay tests against real scheduler PostgreSQL.

Exit criteria:

- Submission and initial outbox rows are atomic.
- Two concurrent identical requests create one job and return compatible success responses.
- A conflicting replay cannot overwrite or masquerade as the original job.

### Phase 3 — Scheduler worker runtime

Tasks:

1. Extract a testable `SchedulerWorker` loop around recover, claim, heartbeat, and execute.
2. Register `PRINT` and `FIBONACCI` handlers explicitly at startup.
3. Provide handler-specific configuration hooks without granting unrelated credentials.
4. Implement bounded idle polling without a busy loop.
5. Add safe SIGTERM/SIGINT handling: stop claiming, finish or abandon current claims within
   the deadline, and dispose resources.
6. Emit counters/histograms for claims, execution outcomes, retries, stale fences, recovery,
   duration, and idle cycles.
7. Test multiple workers, more jobs than one batch, future scheduling, retries, expired
   claims, and stale completion.
8. Add process restart integration tests around active claims.

Exit criteria:

- A submitted due job executes without direct test calls to `claim_due` or `execute`.
- Future jobs are not claimed early.
- Process loss leaves only lease-recoverable work and never permits stale commit.

### Phase 4 — Visibility and ingress production roles

Tasks:

1. Mount durable job, attempt, search, lifecycle, and reconciliation routes only on the
   visibility role.
2. Ensure every dynamic response reads only EDR projection tables.
3. Map EDR database deadline/unavailability to bounded `503` responses.
4. Preserve the qualified missing-record `404` and `dataAsOf` semantics.
5. Implement the optional Kafka-only EDR ingress factory and bounded producer shutdown.
6. Return `202` from ingress only after Kafka acknowledgement; describe projection lag in
   the response.
7. Add role-specific readiness and OpenAPI assertions.
8. Test that scheduler credentials cannot be used by visibility and vice versa.

Exit criteria:

- Visibility continues to respect evidence authority during scheduler or pipeline delay.
- Neither API constructs or queries the other role's database.

### Phase 5 — Compose and end-to-end HTTP path

Tasks:

1. Add scheduler API, scheduler worker, publisher, projector, and visibility API services to
   the development/acceptance topology.
2. Assign role-specific URLs, credentials, health checks, stop grace periods, and dependency
   conditions.
3. Extend `scripts/infra` with bounded `up-apps`, `ready-apps`, and HTTP smoke commands, or
   equivalent documented operations.
4. Submit a unique `FIBONACCI` and `PRINT` job over HTTP.
5. Poll the visibility API until each terminal projection is observed.
6. Assert scheduler attempts, outbox drain, Kafka/Connect persistence, projection
   checkpoints, API attempt history, and correlation search.
7. Restart scheduler API before and after commit and restart a worker with an active claim.
8. Capture redacted diagnostics on failure with the existing evidence collector.

Exit criteria:

- One command starts the entire runnable topology.
- A client can use only HTTP to submit and observe the demonstration jobs.
- Restart tests preserve one logical execution and EDR-derived visibility.

### Phase 6 — Operations, CI evidence, and rollout documentation

Tasks:

1. Add pull-request unit/contract gates and bounded production-wiring integration coverage.
2. Add the full HTTP path to nightly resilience and retain redacted artifacts on failure.
3. Document start, stop, readiness, submission, retrieval, retry diagnosis, and queue
   inspection in `docs/spec-004-runbook.md`.
4. Update README to separate simulation, infrastructure-only, and production-like commands.
5. Record exact test nodes, commit, topology, and results in a Spec 004 evidence document.
6. Document trusted-ingress authentication/TLS assumptions and least-privilege credentials.
7. Run a canary submission and rollback rehearsal.

Exit criteria:

- CI and runbook commands reproduce the accepted path.
- Operators can locate the last durable fact without querying across authorities.
- Production deployment requirements contain no undocumented manual composition step.

## 7. Test Matrix

| ID | Tier | Scenario | Required assertion |
| --- | --- | --- | --- |
| `API-01` | Unit | Role route isolation | Scheduler and visibility routes are not cross-mounted. |
| `API-02` | Unit | Lifespan cleanup | Engines/producers close once on normal and failed startup. |
| `API-03` | Contract | Valid submission | `201`, job row, and two initial outbox rows commit. |
| `API-04` | Contract | Identical replay | `200 created=false`; no additional job or initial EDR. |
| `API-05` | Contract | Conflicting replay | `409`; original submission remains unchanged. |
| `API-06` | Contract | Validation and payload limits | `413`/`422`; no durable mutation. |
| `API-07` | Integration | Database deadline | Bounded `503`; pool remains reusable after recovery. |
| `WORKER-01` | Unit | Due/future polling | Only due jobs are claimed in bounded order. |
| `WORKER-02` | Integration | Standalone execution | Both built-in handlers reach correct terminal outcomes. |
| `WORKER-03` | Integration | Worker restart | Expired claim is recovered; stale worker cannot commit. |
| `VIS-01` | Contract | Durable GET APIs | Job, attempts, search, taxonomy, and qualified `404` match schema. |
| `VIS-02` | Integration | EDR outage | Reads fail boundedly; scheduler state is never substituted. |
| `E2E-01` | E2E | HTTP production path | POST through terminal GET uses outbox/Kafka/Connect/projector. |
| `E2E-02` | E2E | API restart after commit | Retry is idempotent and final visibility appears once. |
| `SEC-01` | Integration | Credential isolation | Each API is denied access to the other authority. |

## 8. CI and Evidence

Pull requests run formatting, lint, unit, factory, route, OpenAPI, worker-loop, and PostgreSQL
submission contract tests. The bounded resilience workflow runs the full Compose HTTP path
and restart cases. Nightly adds repeated multi-worker and dependency-recovery cases.

Failure artifacts include sanitized role configuration, API health/readiness, process logs,
scheduler job/attempt/outbox identifiers, connector status, EDR journal/projection counts,
and relevant Kafka offsets. Payload bodies and credentials remain excluded.

## 9. Rollout and Compatibility

- Preserve the simulation application during migration, but label its command explicitly.
- Add production role commands before changing any existing default.
- Deploy visibility first because it is read-only with respect to existing projections.
- Deploy scheduler API dark, then one worker, then canary submissions.
- Scale horizontally only after claim, queue-age, and projection-freshness metrics are stable.
- Rollback stops application roles without schema downgrade or durable-data deletion.

## 10. Traceability

| Spec requirement | Phase | Evidence |
| --- | ---: | --- |
| Role-specific production factories | 1 | `API-01`, `API-02` |
| Durable HTTP submission and idempotency | 2 | `API-03`–`API-07` |
| Standalone scheduler worker | 3 | `WORKER-01`–`WORKER-03` |
| EDR-derived visibility role | 4 | `VIS-01`, `VIS-02` |
| Least-privilege credentials | 1, 4, 5 | `SEC-01` |
| Complete HTTP production path | 5 | `E2E-01`, `E2E-02` |
| Health, metrics, and graceful shutdown | 1, 3, 4 | lifecycle and process tests |
| Runnable documentation and CI evidence | 6 | runbook and evidence manifest |

## 11. Review Gates

### Gate A — Contract and boundary approval

- Public endpoint, role, credential, idempotency, and error contracts are approved.
- Combined production mode is prohibited by validated configuration.

### Gate B — Production factories

- Role-specific factories, lifespan, readiness, and OpenAPI tests pass.
- Importing application modules performs no external I/O.

### Gate C — Scheduler API and worker

- Submission atomicity, replay conflict, polling, execution, fencing, and shutdown pass.
- Both demonstration handlers complete through standalone runtime wiring.

### Gate D — Full durable HTTP path

- Clean-stack `E2E-01` and restart `E2E-02` pass without direct service invocation.
- Credential isolation and failure diagnostics pass.

### Gate E — Operational readiness

- CI, runbook, canary, rollback, and evidence review are complete.
- Simulation and production commands cannot reasonably be confused.

## 12. Definition of Done

- Specs 004 acceptance criteria are satisfied.
- `POST /scheduler/jobs` works through a documented production scheduler API command.
- A standalone worker executes due jobs with bounded polling, fencing, and graceful shutdown.
- Visibility GET APIs use only durable EDR projections.
- Every long-running role has role-correct health, readiness, metrics, and cleanup.
- The full HTTP path passes against real PostgreSQL, Kafka, Connect, and the projector.
- Role credentials are least-privilege and cross-authority access is denied.
- CI retains actionable redacted evidence for production-path failures.
- README and the Spec 004 runbook contain copy-pasteable start, submit, retrieve, and stop
  procedures.
