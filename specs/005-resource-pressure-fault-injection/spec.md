# Resource Pressure and Application Fault Injection Specification

Status: proposed
Depends on: [`../003-production-hardening-resilience/spec.md`](../003-production-hardening-resilience/spec.md),
[`../004-production-api-runtime/spec.md`](../004-production-api-runtime/spec.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Experiment Model](#3-experiment-model)
- [4. Safety and Blast-Radius Controls](#4-safety-and-blast-radius-controls)
- [5. Steady State and Invariants](#5-steady-state-and-invariants)
- [6. Infrastructure Faults](#6-infrastructure-faults)
- [7. Application-Level Fault Injection](#7-application-level-fault-injection)
- [8. Required Scenarios](#8-required-scenarios)
- [9. Observability and Evidence](#9-observability-and-evidence)
- [10. Automation and CI](#10-automation-and-ci)
- [11. Recovery Objectives](#11-recovery-objectives)
- [12. Security](#12-security)
- [13. Acceptance Criteria](#13-acceptance-criteria)
- [14. Out of Scope](#14-out-of-scope)

## 1. Purpose

Extend the outage and restart testing from Specs 002 and 003 into controlled experiments
that exercise degraded resources and failures inside application correctness boundaries.
The system must remain explainable when disks, networks, memory, or CPU are constrained and
when a process fails at a named pre-commit or post-commit checkpoint.

Every experiment follows this sequence:

1. establish observability;
2. define and measure steady state;
3. state a falsifiable hypothesis;
4. inject one bounded fault;
5. measure behavior during the fault;
6. remove the fault and prove recovery; and
7. retain evidence and restore the environment.

The goal is not merely to crash containers. The goal is to prove or disprove the durable
invariants at the boundaries where acknowledged work can otherwise be lost, duplicated, or
left permanently blocked.

## 2. Scope

This specification covers:

- disk latency and capacity exhaustion affecting scheduler and EDR PostgreSQL;
- latency, jitter, bandwidth restriction, disconnects, and connection-pool saturation;
- memory pressure and OOM termination of application roles;
- CPU pressure affecting application and infrastructure roles;
- deterministic delay, exception, process-exit, and pause failpoints inside owned code;
- steady-state, hypothesis, blast-radius, abort, recovery, and evidence contracts;
- bounded local and CI automation using isolated Compose projects; and
- a scenario matrix for scheduler API, worker, publisher, projector, and visibility API.

Existing Cassandra timeout, PostgreSQL timeout, Kafka outage, Kafka Connect restart, DLQ,
process restart, and database-isolation scenarios remain authoritative. Spec 005 may reuse
their harnesses but must not count an existing binary outage as evidence for resource
degradation.

## 3. Experiment Model

An experiment is a versioned declaration with these required fields:

```yaml
id: APP-02
title: scheduler response lost after commit
target: scheduler-api
steadyState: acknowledged jobs become visible within the test objective
hypothesis: retrying the same jobId returns the committed job without duplication
fault:
  type: application-exception
  checkpoint: scheduler.after_commit
  invocations: 1
blastRadius:
  jobId: chaos-app-02
  durationSeconds: 30
abort:
  errorRatePercent: 20
recovery:
  timeoutSeconds: 60
```

Declarations may be represented as Python fixtures or structured files, but the effective
experiment must be emitted as sanitized evidence. Each run has a unique experiment/run ID.

An experiment must identify:

- one primary fault and target;
- the steady-state indicators measured before injection;
- a falsifiable hypothesis;
- durable invariants that must never be violated;
- maximum duration, affected jobs/requests, and resource intensity;
- abort conditions and cleanup actions; and
- a bounded recovery condition and deadline.

## 4. Safety and Blast-Radius Controls

Chaos capabilities are disabled by default. Application failpoints may activate only when
all of the following are true:

- an explicit non-production chaos mode is enabled at process startup;
- a named allowlisted checkpoint is selected;
- a finite duration or invocation count is configured;
- a target experiment ID is present; and
- the request, job, correlation, or process target is bounded.

The harness must:

- use a unique Compose project and experiment labels;
- reject destructive host-wide targets;
- apply CPU, memory, disk, and network pressure only to named test containers;
- record the original resource state before mutation;
- remove faults in fixture finalization or a shell `trap`;
- provide an idempotent cleanup command;
- stop injection immediately when an abort condition is reached; and
- collect diagnostics before removing failed resources.

No public production HTTP route may enable chaos. Fault configuration must not accept
arbitrary Python, shell, SQL, file paths, or commands. Probabilistic injection requires a
recorded seed; correctness-gating experiments should prefer exact invocation counts.

## 5. Steady State and Invariants

Before injection, the harness must prove:

- scheduler and visibility readiness succeed;
- both PostgreSQL authorities are healthy and migrations are current;
- Kafka, Schema Registry, Connect, and required connector tasks are healthy;
- due-job, outbox, Connect, and projection lag are within declared thresholds; and
- a control job completes and becomes visible within the selected objective.

The following invariants apply to every experiment:

1. An acknowledged job is not lost.
2. One `jobId` produces at most one logical terminal outcome.
3. Submission retries cannot overwrite immutable job fields.
4. A stale lease/fencing token cannot commit completion.
5. Scheduler roles never write EDR storage, and visibility roles never read scheduler
   storage.
6. Outbox replay and projection replay remain logically idempotent.
7. A fault must not leave the test topology permanently poisoned after cleanup.
8. Recovery is established by durable state and user-visible behavior, not process health
   alone.

## 6. Infrastructure Faults

### 6.1 Disk degradation

Disk experiments must distinguish latency, throttled throughput, and capacity exhaustion.
Acceptable mechanisms include a dedicated throttled block device, container runtime I/O
limits, a filesystem fault proxy, or a disposable constrained volume. Host filesystems and
developer data must not be filled intentionally.

PostgreSQL disk-latency experiments must observe transaction latency, timeout behavior,
connection-pool occupancy, WAL/storage errors, and recovery. A client must not receive a
false acknowledgement for an uncommitted transaction.

EDR disk degradation must not make scheduler submission depend on EDR availability. Kafka
or Connect lag may grow, but it must be observable and must drain after recovery without
loss or mutation of immutable journal facts.

Disk-full tests use disposable capped storage. Recovery must define whether space is freed,
the database is restarted, or the test volume is recreated from known test data.

### 6.2 Network degradation

Spec 003 covers bounded connection and operation timeouts and selected Cassandra latency.
Spec 005 adds:

- fixed latency plus deterministic jitter;
- bandwidth restriction;
- intermittent connection reset or packet loss;
- slow connection establishment; and
- database connection-pool saturation.

Faults must target one dependency edge at a time. Results must distinguish connection,
pool-acquisition, statement/request, and end-to-end deadlines. Retry loops require bounded
backoff and must not amplify load into a retry storm.

### 6.3 Memory pressure

Memory experiments apply explicit container memory limits and observe memory use before
termination. An OOM experiment is successful only when the expected process is terminated
or fails allocation, the event is visible in evidence, and durable recovery is proved.

Processes must not rely on an in-memory acknowledgement for correctness. Worker OOM must
leave work recoverable through lease expiry; publisher OOM at an ambiguous acknowledgement
boundary must be safe to replay; visibility API OOM must remain isolated from scheduling
and ingestion.

### 6.4 CPU pressure

CPU experiments use named container quotas or a bounded stress process. They must measure
request latency, worker heartbeat/lease timing, queue age, Kafka/Connect lag, and recovery.
CPU saturation must not permit a stale worker to commit after lease loss. Health and
readiness must distinguish a running but unusably delayed process where practical.

## 7. Application-Level Fault Injection

Owned application code must expose an internal `FaultInjector` abstraction. Production
composition uses a no-op implementation. Chaos composition uses an allowlisted,
configuration-driven implementation.

Initial actions are:

| Action | Behavior |
| --- | --- |
| `delay` | Wait for a finite configured duration using an interruptible mechanism. |
| `raise` | Raise an allowlisted synthetic exception with a stable error code. |
| `exit` | Terminate the current process with a documented non-zero exit code. |
| `pause` | Block at a barrier until released or until a finite deadline expires. |

Initial checkpoints are:

```text
scheduler.before_commit
scheduler.after_commit
worker.after_claim
worker.before_complete
publisher.before_send
publisher.after_broker_ack
projector.before_apply
projector.after_apply
visibility.before_query
```

Checkpoint names are stable test contracts. A checkpoint must be placed immediately around
the named durable boundary and must not change transaction semantics when the no-op injector
is used. Injection supports exact invocation number and optional `jobId`/`correlationId`
matching. Logs include experiment ID, checkpoint, action, and activation count but exclude
payloads and secrets.

## 8. Required Scenarios

### 8.1 Merge-gating scenarios

| ID | Fault | Required proof |
| --- | --- | --- |
| `NET-01` | Latency and jitter on one application dependency | Deadlines remain bounded, retries do not storm, and the dependency recovers without process restart. |
| `NET-03` | Scheduler DB pool saturation | Excess work receives bounded backpressure/failure; connections are released and later requests succeed. |
| `MEM-01` | Scheduler worker OOM after claim | The process restarts, the lease is reclaimed, stale completion is rejected, and one terminal outcome becomes visible. |
| `CPU-01` | Scheduler worker CPU saturation | Backlog/latency is observable; lease heartbeat or fencing preserves correctness; backlog drains after recovery. |
| `APP-01` | Exception before scheduler commit | No job or initial outbox rows persist and the client receives failure. |
| `APP-02` | Failure after scheduler commit before response | Retrying the same `jobId` returns the original submission without duplicate durable facts. |
| `APP-03` | Publisher exit after broker acknowledgement | Replay may redeliver but produces one logical journal/projection fact. |
| `APP-04` | Projector exit before projection commit | Checkpoint does not advance incorrectly; restart applies the event once logically. |

### 8.2 Nightly scenarios

| ID | Fault | Required proof |
| --- | --- | --- |
| `DISK-01` | Scheduler PostgreSQL write latency | Submission latency/failure is bounded and no false acknowledgement or partial outbox write occurs. |
| `DISK-02` | EDR PostgreSQL write latency | Scheduling remains available, lag rises visibly, and the EDR backlog drains without loss. |
| `DISK-03` | Disposable PostgreSQL volume reaches capacity | Writes fail explicitly, evidence identifies storage exhaustion, and documented recovery restores service. |
| `NET-02` | Kafka Connect bandwidth restriction | Lag increases predictably and drains after restriction removal. |
| `MEM-02` | Publisher OOM at ambiguous publish boundary | Outbox recovery is safe and downstream logical deduplication holds. |
| `MEM-03` | Visibility API OOM | Query availability is affected but scheduling and EDR ingestion remain isolated. |
| `CPU-02` | PostgreSQL CPU pressure | Database operations obey deadlines and connection pools recover without manual data repair. |
| `CPU-03` | Kafka Connect CPU pressure | Connector lag is observable and drains without journal loss. |
| `APP-05` | Bounded latency at owned dependency checkpoints | API deadlines and upstream backpressure match the declared contract. |
| `APP-06` | Malformed/poison event | The record is quarantined or sent to DLQ and a later valid event progresses. |

Experiments may be moved from nightly to merge-gating when their median runtime and
reliability fit the pull-request budget. No required scenario may be marked complete using
only mocks.

## 9. Observability and Evidence

Each experiment produces a timestamped directory containing:

- effective experiment declaration and random seed;
- hypothesis and invariant result summary;
- before, during, and after timestamps;
- container resource limits and sampled CPU, memory, I/O, and restart counts;
- application logs and health/readiness responses;
- active toxics or pressure configuration;
- queue counts and oldest due/unpublished/unprojected ages;
- Kafka offsets, connector state, lag, and DLQ counts where applicable;
- relevant job, attempt, outbox, event, and checkpoint identifiers; and
- cleanup and recovery results.

Evidence must redact credentials and prohibited payloads. A failed hypothesis is a useful
experiment result but a failing release gate until the defect is fixed or a time-bounded,
owned risk decision is recorded.

## 10. Automation and CI

The command interface must support:

```text
scripts/chaos list
scripts/chaos run <scenario-id>
scripts/chaos cleanup <run-id>
scripts/chaos evidence <run-id>
```

Equivalent integration into `scripts/infra` is acceptable. Commands must be non-interactive,
bounded, and safe to rerun. CI tiers are:

1. **Pull request:** unit tests for the injector and merge-gating experiments whose combined
   resource/time budget is explicitly capped.
2. **Nightly:** disk, bandwidth, OOM, CPU, and extended application failpoints.
3. **Release:** nightly suite plus repeated recovery and representative-load experiments.

CI must always run cleanup and upload sanitized evidence on failure. Infrastructure
capability checks may skip an experiment only with an explicit reason; required release
evidence cannot consist of skipped scenarios.

## 11. Recovery Objectives

Every scenario declares its own recovery deadline. The initial defaults are:

- APIs accept a control request within 60 seconds after fault removal;
- expired worker work is reclaimed within lease duration plus 30 seconds;
- outbox and projection lag return below the pre-fault threshold within 120 seconds for the
  bounded CI workload; and
- no test waits indefinitely for a container, lock, lease, message, or projection.

These are test-profile objectives, not production SLOs. Evidence must include the measured
value rather than only pass/fail.

## 12. Security

- Chaos mode must fail closed when configuration is incomplete.
- Production images may contain the no-op interface but must not expose a remote activation
  endpoint.
- Test credentials remain isolated from non-test environments.
- Fault logs and artifacts follow the repository's redaction rules.
- Resource injectors run with the minimum Linux capabilities required by the selected
  mechanism.
- Privileged containers require an explicit documented exception and may not mount broad
  host paths.

## 13. Acceptance Criteria

Spec 005 is complete when:

1. the fault-injection abstraction has zero observable effect when disabled;
2. named application checkpoints deterministically reproduce ambiguous boundary failures;
3. disk, network, memory, and CPU experiments operate only on isolated named resources;
4. all merge-gating scenarios pass against real infrastructure;
5. all nightly scenarios have automated commands and retained evidence;
6. every experiment records steady state, hypothesis, blast radius, abort, cleanup, and
   measured recovery;
7. no acknowledged job is lost and no duplicate logical terminal outcome is created;
8. scheduler/EDR authority isolation holds under pressure;
9. CI cleanup is reliable after both success and forced interruption; and
10. runbook, chaos matrix, architecture risks, and evidence index reflect actual automation.

## 14. Out of Scope

- Running chaos against production without a separate approval and safety design.
- Host-wide disk filling, fork bombs, or unbounded stress tools.
- Multi-region loss, cloud control-plane failure, and Kubernetes node disruption.
- Kernel corruption, bit flips, clock skew, and certificate expiry.
- General performance benchmarking unrelated to a stated resilience hypothesis.
- Automatic remediation or self-healing orchestration beyond proving current recovery paths.
