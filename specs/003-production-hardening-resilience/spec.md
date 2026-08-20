# Production Hardening and Resilience Specification

Status: proposed  
Depends on: [`../002-real-persistence-kafka/spec.md`](../002-real-persistence-kafka/spec.md)  
Closes: the current evidence and production-readiness gaps recorded in
[`../../docs/chaos.md`](../../docs/chaos.md)

## Contents

- [1. Purpose](#1-purpose)
- [2. Scope](#2-scope)
- [3. Principles](#3-principles)
- [4. Bounded External Operations](#4-bounded-external-operations)
- [5. Independent Failure Domains](#5-independent-failure-domains)
- [6. Automated Resilience Harness](#6-automated-resilience-harness)
- [7. Required Resilience Scenarios](#7-required-resilience-scenarios)
- [8. Observability and Failure Evidence](#8-observability-and-failure-evidence)
- [9. Performance and Recovery Objectives](#9-performance-and-recovery-objectives)
- [10. CI and Release Policy](#10-ci-and-release-policy)
- [11. Security and Data Handling](#11-security-and-data-handling)
- [12. Acceptance Criteria](#12-acceptance-criteria)
- [13. Out of Scope](#13-out-of-scope)

## 1. Purpose

Turn the durable architecture delivered by Spec 002 into a repeatably tested, operationally
bounded release candidate. Spec 002 establishes the persistence, delivery, ownership, and
recovery mechanisms. This specification proves those mechanisms against real dependencies
and defines the limits within which operators can rely on them.

The system must fail within configured deadlines, preserve every committed fact, expose the
queue at which progress stopped, and recover within a measured objective after a dependency
returns. A manually executed experiment is useful during development but is not sufficient
release evidence for a scenario required by this specification.

## 2. Scope

This specification covers:

- Configurable PostgreSQL and Kafka deadlines.
- Separate scheduler and EDR PostgreSQL containers for local failure isolation.
- A reusable pytest harness for PostgreSQL, Kafka, Kafka Connect, Cassandra, Toxiproxy, and
  the application processes.
- Automated versions of the Spec 002 section 15 failure scenarios.
- Packet-level Cassandra unknown-outcome and concurrent-write tests.
- Broker, database, process, and projection recovery tests.
- Diagnostics and redacted artifacts produced when resilience tests fail.
- Representative load, freshness, throughput, and recovery measurements.
- Pull-request, nightly, and release test tiers.

This specification does not change the authority boundaries, EDR meanings, or delivery
semantics established by Specs 001 and 002.

## 3. Principles

1. **Preserve authority boundaries.** An outage never permits inference from a different
   subsystem or a direct scheduler-to-EDR shortcut.
2. **Bound waiting.** Every external request and every test wait has an explicit deadline.
3. **Test real failure surfaces.** Kafka and Cassandra acceptance tests use real clients and
   infrastructure, not mocked exceptions.
4. **Assert intermediate safety.** Tests verify backlog, lease, claim, retry, and checkpoint
   state during an outage, not only the final state after recovery.
5. **Prefer deterministic triggers.** Fault placement uses proxy controls, process hooks, or
   test failpoints with an acknowledgement barrier rather than timing guesses.
6. **Retain evidence.** A failed run reports enough redacted state to identify the last
   durable fact and reproduce the failure.
7. **Separate correctness from capacity.** Correctness gates must pass at small bounded
   scale before load and soak results are accepted.

## 4. Bounded External Operations

### 4.1 PostgreSQL

Scheduler and EDR database configuration must independently support:

- connection establishment timeout;
- pool acquisition timeout;
- statement timeout;
- lock timeout;
- idle-in-transaction timeout; and
- an application transaction deadline where a multi-statement unit of work requires one.

Defaults must be finite. Settings must be applied to every connection for the relevant role
and verified by integration tests. A timeout must roll back the transaction, release or
invalidate the connection safely, and produce a classified, redacted error.

The transaction deadline must exceed the expected normal statement sequence while remaining
shorter than the owning process's shutdown deadline. Lock and statement timeout errors must
not be treated as successful commits.

### 4.2 Kafka and Schema Registry

Publisher configuration must independently expose finite:

- socket timeout;
- request timeout;
- message delivery timeout;
- metadata lookup timeout; and
- shutdown flush timeout.

Schema Registry requests must have connect and read deadlines. The publisher must never mark
an outbox row published until delivery acknowledgement is observed. A delivery or flush
deadline must leave the row unpublished and eligible according to the existing bounded retry
policy.

Configuration validation must reject contradictory values, including a request timeout that
exceeds the delivery timeout and a shutdown flush timeout that exceeds the process shutdown
budget.

### 4.3 Test polling

All infrastructure and end-to-end tests must use bounded condition polling. On timeout, the
poller must include the expected condition, elapsed time, last observed value, and relevant
component diagnostics. Unexplained fixed sleeps are prohibited as synchronization.

## 5. Independent Failure Domains

The local composition must provide distinct `scheduler-postgres` and `edr-postgres` services
with separate:

- containers and health checks;
- data volumes;
- database owners and runtime roles;
- ports and connection URLs; and
- migration invocations.

Stopping either container must leave the other database and its dependent read path
available. Application roles must remain unable to connect to the other authority. Test
teardown must identify resources by the test-run project and labels; it must not remove
unrelated developer volumes.

Sharing one physical PostgreSQL service may remain as an explicitly selected lightweight
developer profile, but it cannot provide acceptance evidence for outage isolation.

## 6. Automated Resilience Harness

The repository must provide one documented command interface for bringing infrastructure up,
checking readiness, migrating databases, applying the connector, running each test tier, and
tearing down the named test environment.

The pytest harness must provide:

- a unique run identifier and isolated mutable data;
- bounded readiness checks for every dependency;
- APIs to add, inspect, and remove named Toxiproxy toxics;
- process control and deterministic crash/failpoint barriers;
- scheduler, outbox, Kafka, Connect, EDR, projection, and Cassandra inspection helpers;
- automatic toxic cleanup in `finally`/fixture finalization;
- connector and component diagnostics before failed resources are removed; and
- markers matching the declared `postgres`, `kafka`, `cassandra`, `integration`, and `e2e`
  test classes.

Tests must be safe to rerun after interruption. Shared seed data must not be mutated; each
mutable Cassandra test uses a unique dataset identifier.

## 7. Required Resilience Scenarios

### 7.1 Merge-gating correctness scenarios

The following Spec 002 cases must be automated against real infrastructure:

| Scenario | Required proof |
| --- | --- |
| `PERSIST-01` | Application process restart preserves journal, projection, attempts, and API result. |
| `PERSIST-02` | Concurrent pollers claim disjoint jobs and emit one logical retrieval per attempt. |
| `WORKER-01` | Both demonstration handlers complete through the durable path with one successful effect. |
| `WORKER-02` | Cassandra timeout creates a future retry; no early claim occurs; recovery succeeds once. |
| `WORKER-03` | Sustained outage executes exactly `max_attempts` and leaves no eligible claim. |
| `KAFKA-01` | Crash after acknowledgement republishes safely and produces one journal/projection fact. |
| `PROJ-01` | Crash before projection commit replays and commits exactly one decision. |
| `PROJ-03` | Journal-only rebuild is equivalent to the original tested projection. |

### 7.2 Nightly correctness scenarios

| Scenario | Required proof |
| --- | --- |
| `WORKER-04` | A delayed reply after lease loss cannot commit with the stale fencing token. |
| `WORKER-05` | Loss after Cassandra finalization is reconciled without a second checksum increment. |
| `WORKER-06` | Two workers racing on one checksum each apply their logical operation at most once. |
| `KAFKA-02` | Scheduler commits during broker outage and the outbox drains without loss after recovery. |
| `SINK-01` | EDR outage leaves Kafka authoritative for pending delivery and catches up after recovery. |
| `SINK-02` | Poison input reaches the DLQ and a subsequent valid record progresses. |
| `SINK-03` | Event identity collision cannot mutate the journal and is observable. |
| `PROJ-02` | Out-of-order facts retain Spec 001 terminal precedence and findings. |
| `ISOLATION-01` | Scheduler and EDR databases can be stopped independently without cross-authority fallback. |

`WORKER-05` must place the fault after Cassandra has accepted the final conditional write but
before the client observes the response. A generic pre-request disconnect does not satisfy
the scenario. `WORKER-06` must synchronize workers after the same observed checksum so that
the conditional conflict is exercised rather than merely hoped for.

### 7.3 Restart matrix

Nightly tests must cover API, scheduler/worker, publisher, Kafka Connect, projector,
scheduler PostgreSQL, EDR PostgreSQL, Kafka, and Cassandra at the meaningful pre-commit and
post-commit boundaries. Equivalent boundaries may be consolidated when they exercise the
same durable fact and recovery mechanism; the mapping must be documented.

## 8. Observability and Failure Evidence

Operators and tests must be able to identify the last durable fact across:

1. due scheduler jobs;
2. unpublished scheduler outbox rows;
3. Kafka/Connect lag and DLQ records; and
4. unprojected EDR journal rows.

Readiness must distinguish dependency reachability from excessive queue age. Configurable
thresholds must cover oldest due job, oldest unpublished outbox row, connector/task failure,
DLQ growth, and oldest unprojected event. Diagnostics must not report a process healthy only
because it is running when its required task has failed.

For a failed infrastructure test, CI must retain, as applicable:

- test result and random/run seed;
- sanitized effective configuration;
- container and application logs;
- health/readiness responses;
- active proxy configuration and toxics;
- connector configuration and task status;
- queue counts and oldest-item timestamps;
- Kafka topic offsets/lag; and
- relevant job, attempt, outbox, event, and checkpoint identifiers.

Artifacts must exclude database passwords, Kafka credentials, full prohibited payloads, and
unredacted exception data.

## 9. Performance and Recovery Objectives

Before performance evidence is accepted, the repository must define a versioned workload
profile containing job mix, event count, payload-size distribution, arrival pattern,
concurrency, Cassandra record count/page size, infrastructure resources, and test duration.

At minimum, measure:

- scheduler claim throughput and oldest-due delay;
- outbox throughput and oldest-unpublished age;
- Kafka/Connect throughput and lag;
- projection throughput and persisted-to-projected latency;
- API p50, p95, and p99 latency;
- end-to-end created-to-visible freshness at p50, p95, and p99;
- outage recovery time and backlog-drain rate;
- Cassandra request p95/p99, timeout/conflict/reconciliation counts, and worker memory; and
- storage bytes per job and per EDR.

The initial release target is healthy-pipeline end-to-end freshness of at most 10 seconds at
an explicitly stated percentile. The project must choose that percentile and a broker-outage
recovery objective before the release run; results without the workload profile and resource
limits are informational only.

Performance runs must produce timestamped, non-overwriting machine-readable results and a
human-readable summary. Failure to meet a target must fail the release gate or be accepted
through a documented decision that states the measured value and remediation owner.

## 10. CI and Release Policy

CI is divided into:

1. **Pull request:** static checks, unit/scenario tests, migration checks, PostgreSQL
   integration, and the bounded merge-gating resilience subset.
2. **Main/nightly:** complete Spec 002 failure matrix, restart matrix, projection rebuild,
   and cross-container outage isolation.
3. **Release:** nightly suite plus representative load, backlog recovery, and soak evidence.

Each infrastructure job must have an overall deadline, must print a concise diagnostic
summary on failure, and must upload redacted artifacts. A test may be retried only when CI
records the original failure; retries must not conceal flaky correctness gates.

Required scenarios may not remain manual runbook steps. Environment-specific production
topology validation may remain outside CI only when the automated local analogue exists and
the release checklist names the external evidence.

## 11. Security and Data Handling

- Each process receives only its role-specific credentials.
- Test artifacts follow the same payload and secret-redaction rules as production logs.
- Unsafe production settings, including plaintext dependency modes where prohibited by the
  deployment policy, must fail configuration validation.
- Chaos controls and failpoints must be disabled or inaccessible in production mode.
- CI teardown may delete only resources carrying the current run's explicit labels and
  project identifier.

## 12. Acceptance Criteria

Spec 003 is complete when:

- PostgreSQL and Kafka operations have validated finite deadlines and integration tests prove
  their failure behavior.
- Scheduler and EDR PostgreSQL run in separate acceptance-test containers, and each outage is
  demonstrated independently.
- Every Spec 002 section 15 scenario has executable automated evidence at its assigned tier.
- Cassandra unknown-outcome testing proves one checksum increment after post-write response
  loss.
- The concurrent Cassandra race deterministically exercises conflict handling and never
  performs a blind or duplicate increment.
- Kafka outage testing proves scheduler continuity, durable backlog, lossless recovery, and a
  measured recovery time.
- Test waits and infrastructure setup have finite deadlines and actionable timeout output.
- Failed CI runs retain the redacted diagnostics required by section 8.
- A documented representative load run records p95/p99 freshness and throughput without
  overwriting existing simulation results.
- The selected healthy freshness and outage recovery objectives pass, or the release is
  explicitly blocked.
- `docs/chaos.md`, the operations runbook, and root documentation link to current automated
  evidence instead of describing required cases as future improvements.

## 13. Out of Scope

- Changing the Spec 001 lifecycle taxonomy or visibility reducer semantics.
- Replacing Kafka Connect with an application-owned EDR consumer.
- Distributed transactions across PostgreSQL, Kafka, or Cassandra.
- Claiming production Cassandra availability behavior from the single-node local topology.
- Selecting production cluster sizes solely from laptop or shared-runner measurements.
- Multi-region disaster recovery, active-active scheduling, and region-failover policy.
- A general-purpose chaos platform beyond the dependencies and failure modes in this system.
