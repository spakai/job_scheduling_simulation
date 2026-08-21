# Resource Pressure and Application Fault Injection Implementation Plan

Status: proposed
Implements: [`spec.md`](spec.md)
Depends on: merged Specs 003 and 004

## Contents

- [1. Objective](#1-objective)
- [2. Current Baseline and Gaps](#2-current-baseline-and-gaps)
- [3. Delivery Principles](#3-delivery-principles)
- [4. Target Repository Shape](#4-target-repository-shape)
- [5. Delivery Sequence](#5-delivery-sequence)
- [6. Detailed Phase Plan](#6-detailed-phase-plan)
- [7. Scenario Implementation Matrix](#7-scenario-implementation-matrix)
- [8. Test Strategy](#8-test-strategy)
- [9. CI and Evidence](#9-ci-and-evidence)
- [10. Rollout and Compatibility](#10-rollout-and-compatibility)
- [11. Traceability](#11-traceability)
- [12. Review Gates](#12-review-gates)
- [13. Definition of Done](#13-definition-of-done)

## 1. Objective

Build a safe, deterministic chaos framework and use it to prove system behavior under slow
or exhausted resources and failures at application durability boundaries. Deliver the work
in independently reviewable slices: safety and observation first, deterministic application
failpoints second, then network, memory/CPU, and disk experiments.

## 2. Current Baseline and Gaps

The repository already has:

- isolated scheduler and EDR PostgreSQL authorities;
- bounded PostgreSQL, Kafka, Schema Registry, and Cassandra deadlines;
- Toxiproxy automation for Cassandra timeout and recovery;
- durable claims, fencing, outbox replay, immutable EDR ingestion, and projector checkpoints;
- restart/outage scenarios for Kafka, Connect, PostgreSQL, Cassandra, publisher, and
  projector paths;
- opt-in integration markers, named Compose projects, diagnostic collection, and CI tiers;
  and
- production-like scheduler and visibility APIs with a standalone worker.

The gaps are:

| Gap | Consequence |
| --- | --- |
| Network tests focus on outage and selected timeout behavior | Jitter, bandwidth loss, pool saturation, and retry amplification are unproved. |
| No controlled disk pressure | Transaction and EDR behavior under slow/full storage is unknown. |
| No memory or CPU limits in chaos scenarios | Lease timing, OOM recovery, and degraded readiness are unproved. |
| Crash timing is encoded ad hoc in test doubles | Ambiguous post-commit/post-ack boundaries cannot be reproduced uniformly in running processes. |
| No experiment manifest | Hypothesis, blast radius, abort criteria, and recovery measurements are inconsistent. |
| Evidence is dependency-centric | Resource samples and exact injection activations are not retained together. |

## 3. Delivery Principles

- Observe before injecting.
- Keep one primary fault per experiment.
- Make faults deterministic before adding probability.
- Put checkpoints around durable boundaries, not arbitrary lines.
- Default to no-op behavior and fail closed when chaos configuration is invalid.
- Bound time, invocation count, affected identity, resource intensity, and cleanup.
- Prefer container-scoped controls and disposable data over privileged host mutation.
- Treat failed hypotheses as defects or explicit risk decisions, never flaky rerun noise.
- Keep simulation behavior and production composition unchanged when chaos is disabled.

## 4. Target Repository Shape

Names may evolve, but the intended shape is:

```text
src/job_visibility/
  chaos/
    __init__.py
    model.py                    experiment/fault declarations
    injector.py                 no-op and configured fault injectors
    checkpoints.py              stable checkpoint names
    safety.py                   environment and bounds validation
  scheduler/service.py          scheduler commit checkpoints
  scheduler/worker.py           claim/completion checkpoints
  outbox/publisher.py           send/ack checkpoints
  projection/projector.py       apply/commit checkpoints
  production_api.py             optional query delay checkpoint composition
scripts/
  chaos                         list/run/cleanup/evidence interface
tests/
  test_fault_injector.py
  test_chaos_safety.py
  integration/
    test_application_faults.py
    test_resource_pressure.py
infra/
  chaos/
    experiments/                versioned declarations
    helpers/                    scoped pressure adapters
docs/
  spec-005-runbook.md
  spec-005-evidence.md
specs/005-resource-pressure-fault-injection/
  spec.md
  plan.md
```

## 5. Delivery Sequence

```text
Phase 0  Experiment contracts and safety gates
Phase 1  Observability and baseline measurement
Phase 2  Application-level injector and checkpoints
Phase 3  Network degradation and pool saturation
Phase 4  Memory and CPU pressure
Phase 5  Disk latency and capacity exhaustion
Phase 6  CI tiers, evidence, runbook, and architecture updates
```

Phases 3 and 4 may proceed in parallel only after Phases 0–2 stabilize the experiment and
evidence interfaces. Disk capacity work remains nightly-only until its isolation and cleanup
are proven repeatedly.

## 6. Detailed Phase Plan

### Phase 0 — Experiment contracts and safety gates

Tasks:

1. Define typed models for experiment identity, hypothesis, target, fault, blast radius,
   abort conditions, recovery condition, and evidence paths.
2. Define the stable scenario IDs and checkpoint registry from the specification.
3. Validate finite duration/invocation bounds and require a run ID.
4. Reject production environment names, unscoped targets, arbitrary commands, broad host
   paths, and unknown checkpoints/actions.
5. Add a unique Compose project/label convention for each run.
6. Implement an idempotent cleanup registry that records every applied mutation.
7. Add unit tests for all rejected unsafe declarations and repeated cleanup.

Exit criteria:

- Invalid or unbounded faults fail before infrastructure mutation.
- Cleanup is safe when no fault exists, when partially applied, and when called twice.
- Every required scenario can be represented without arbitrary executable input.

### Phase 1 — Observability and baseline measurement

Tasks:

1. Add a baseline probe that submits a control job and waits for EDR visibility.
2. Capture readiness, queue ages, outbox counts, projection lag, connector state, and DLQ
   count before injection.
3. Capture per-container CPU, memory, block I/O, restart count, exit code, and OOM state.
4. Add a periodic bounded sampler for during-fault evidence.
5. Define machine-readable result status: `proved`, `disproved`, `aborted`, or `invalid`.
6. Add redaction and artifact-size bounds.
7. Extend evidence collection without breaking Spec 003 consumers.

Exit criteria:

- A no-fault experiment records before/during/after evidence and measured recovery.
- An unhealthy baseline prevents injection and explains which steady-state condition failed.
- Evidence contains no configured database password or full job payload.

### Phase 2 — Application-level injector and checkpoints

Tasks:

1. Add a minimal `FaultInjector` protocol and default `NoOpFaultInjector`.
2. Add configured `delay`, `raise`, `exit`, and bounded `pause` actions.
3. Match exact invocation count and optional job/correlation identity.
4. Add structured activation logging and thread-safe activation counts.
5. Insert scheduler checkpoints immediately before and after transaction commit.
6. Insert worker checkpoints after claim and before completion.
7. Insert publisher checkpoints before send and after broker acknowledgement.
8. Insert projector checkpoints before apply and after apply/commit as transaction structure
   permits; document the exact durable boundary.
9. Add a visibility query delay checkpoint for deadline/backpressure experiments.
10. Compose the configured injector only in explicitly enabled chaos processes.
11. Prove through benchmarks or focused tests that the no-op path adds negligible overhead.

Exit criteria:

- `APP-01` through `APP-04` pass against running application containers.
- One configured activation occurs at the named identity and no unrelated request is hit.
- Disabled injection does not change public APIs, transactions, or test outcomes.

### Phase 3 — Network degradation and pool saturation

Tasks:

1. Add named Toxiproxy edges for scheduler PostgreSQL, EDR PostgreSQL, Kafka, and Connect
   where technically meaningful; retain the existing Cassandra proxy.
2. Add latency/jitter, bandwidth, timeout/reset, and deterministic cleanup helpers.
3. Make application database endpoints optionally route through their scoped proxies in the
   chaos profile.
4. Implement `NET-01` with one dependency edge and an exact seed/profile.
5. Implement `NET-02` around Connect delivery and verify lag/drain measurements.
6. Implement `NET-03` by holding a bounded number of database connections and sending one
   excess request through the API.
7. Assert the distinct pool acquisition, connect, request/statement, and HTTP deadlines.
8. Measure retry count/rate to detect retry amplification.

Exit criteria:

- Network faults affect only their declared edge.
- Pool saturation returns within the configured deadline and later requests recover.
- Toxics are absent after successful, failed, and interrupted tests.

### Phase 4 — Memory and CPU pressure

Tasks:

1. Select a container-scoped pressure mechanism available in local Docker and GitHub-hosted
   runners; feature-detect it before injection.
2. Add explicit baseline resource reservations/limits for chaos-profile application roles.
3. Implement worker OOM after a deterministic claim barrier (`MEM-01`).
4. Implement publisher OOM or forced memory-limit termination at the ambiguous publish
   boundary (`MEM-02`).
5. Implement visibility API OOM isolation (`MEM-03`).
6. Add bounded CPU saturation for worker, PostgreSQL, and Connect (`CPU-01`–`CPU-03`).
7. Capture OOMKilled/exit information, restart count, lease timings, pool use, lag, and
   recovery duration.
8. Ensure stress processes have a maximum duration independent of test-process survival.

Exit criteria:

- OOM evidence proves the intended process, not an unrelated service, was affected.
- Worker lease/fencing and publisher idempotency invariants hold after restart.
- CPU faults stop automatically and the bounded backlog drains.

### Phase 5 — Disk latency and capacity exhaustion

Tasks:

1. Evaluate container runtime block-I/O throttling, a dedicated loopback volume, and a
   filesystem fault layer; document the least-privilege portable choice.
2. Build disposable scheduler and EDR database volumes for disk experiments.
3. Implement scheduler write-latency experiment (`DISK-01`) and verify atomic submission.
4. Implement EDR write-latency experiment (`DISK-02`) and verify authority isolation plus
   lag drain.
5. Implement capacity exhaustion only inside a bounded disposable volume (`DISK-03`).
6. Capture PostgreSQL/WAL errors, transaction outcomes, I/O samples, free capacity, and
   recovery steps.
7. Prove cleanup cannot select normal developer volumes.
8. Keep disk scenarios nightly until repeated teardown evidence is stable.

Exit criteria:

- No experiment writes filler data to the host or an unvalidated volume.
- Clients never receive success for an uncommitted scheduler transaction.
- EDR pressure does not prevent scheduler acceptance, and its backlog later drains.

### Phase 6 — CI, evidence, runbook, and architecture

Tasks:

1. Add `scripts/chaos` commands for list, run, cleanup, and evidence.
2. Add pytest markers for `chaos`, `resource`, `application_fault`, and nightly-only cases.
3. Add bounded pull-request and nightly workflow jobs with explicit resource/time budgets.
4. Always collect evidence before cleanup and upload it on failure.
5. Record capability-based skips and fail release validation when required evidence is
   missing.
6. Write `docs/spec-005-runbook.md` including emergency cleanup and local prerequisites.
7. Write `docs/spec-005-evidence.md` mapping scenarios to commands, tests, and artifacts.
8. Update `docs/chaos.md`, `architecture.md`, and README links/assessment gaps.
9. Run interrupted-test drills to validate cleanup.

Exit criteria:

- Pull-request scenarios are repeatable within the declared CI budget.
- Nightly jobs retain enough redacted evidence to diagnose a disproved hypothesis.
- Documentation describes only implemented mechanisms and clearly labels remaining gaps.

## 7. Scenario Implementation Matrix

| Scenario | Primary mechanism | Initial tier | Principal assertion |
| --- | --- | --- | --- |
| `NET-01` | Toxiproxy latency/jitter | PR | Bounded deadlines and no retry storm |
| `NET-02` | Toxiproxy bandwidth restriction | Nightly | Connect lag drains without loss |
| `NET-03` | Held SQLAlchemy pool connections | PR | Bounded backpressure and pool recovery |
| `MEM-01` | Worker memory limit plus claim barrier | PR | Lease recovery and one terminal outcome |
| `MEM-02` | Publisher memory limit plus ack checkpoint | Nightly | Safe outbox replay |
| `MEM-03` | Visibility API memory limit | Nightly | Authority/failure isolation |
| `CPU-01` | Worker CPU quota/stress | PR | Lease/fencing correctness and drain |
| `CPU-02` | PostgreSQL CPU quota/stress | Nightly | Deadline and pool recovery |
| `CPU-03` | Connect CPU quota/stress | Nightly | Observable lag and drain |
| `DISK-01` | Scheduler DB I/O throttle | Nightly | Atomic submission under latency |
| `DISK-02` | EDR DB I/O throttle | Nightly | Scheduler isolation and lag drain |
| `DISK-03` | Disposable capped DB volume | Nightly | Explicit failure and safe recovery |
| `APP-01` | `scheduler.before_commit:raise` | PR | No partial durable state |
| `APP-02` | `scheduler.after_commit:raise/exit` | PR | Idempotent retry after lost response |
| `APP-03` | `publisher.after_broker_ack:exit` | PR | Logical downstream deduplication |
| `APP-04` | `projector.before_apply:exit` | PR | Correct checkpoint replay |
| `APP-05` | Named checkpoint delay | Nightly | Deadline/backpressure contract |
| `APP-06` | Invalid Kafka event | Nightly | DLQ/quarantine and continued progress |

## 8. Test Strategy

### 8.1 Unit tests

- experiment schema validation and serialization;
- environment/scope safety rejection;
- deterministic activation matching and invocation counts;
- no-op injector behavior and concurrency safety;
- delay cancellation/deadline, synthetic exception, and barrier release;
- cleanup registration and idempotency;
- evidence redaction and result classification; and
- production composition cannot activate configured faults.

### 8.2 Integration tests

- each application checkpoint against real PostgreSQL/Kafka boundaries;
- Toxiproxy edges and cleanup;
- pool saturation with real SQLAlchemy pools;
- container OOM/restart and lease recovery;
- CPU quota/stress with real worker timing;
- disk experiments against disposable PostgreSQL data; and
- full HTTP submission-to-visibility recovery.

Mocks may validate injector mechanics but cannot satisfy a scenario's durable proof.

### 8.3 Negative and interruption tests

- refuse chaos mode in a production environment;
- refuse unknown targets, checkpoints, actions, and unlimited durations;
- kill the pytest process while a toxic/stressor is active, then run cleanup;
- stop a target before injection and ensure the harness aborts safely;
- force evidence collection failure without skipping resource cleanup; and
- verify unrelated Compose projects and volumes are unchanged.

## 9. CI and Evidence

The pull-request job should initially include application failpoints plus the fastest
network/pool scenario. `MEM-01` and `CPU-01` enter merge gating only after repeated stable
runs demonstrate that GitHub runner variance does not make them flaky.

Nightly jobs should group scenarios by topology reset cost:

1. network and application faults;
2. memory and CPU pressure; and
3. disposable disk experiments.

Each group has an overall timeout and uploads one evidence bundle. Machine-readable summaries
include scenario ID, commit, run ID, seed, hypothesis result, invariant results, measured
recovery, and artifact checksums.

## 10. Rollout and Compatibility

- Introduce injector parameters with no-op defaults so existing callers remain compatible.
- Add chaos-only Compose overrides/profile rather than changing normal service limits.
- Land checkpoints with unit/integration tests before enabling destructive actions.
- Run new nightly scenarios as informational for an agreed burn-in period.
- Promote a scenario to required only after its capability, runtime, and cleanup are stable.
- Never silently weaken existing Spec 003 release gates when a new chaos job is unavailable.

## 11. Traceability

| Specification requirement | Delivery phase |
| --- | --- |
| Experiment model and bounded declarations | Phase 0 |
| Blast-radius and cleanup controls | Phases 0 and 6 |
| Steady state and evidence | Phase 1 |
| Application failpoints | Phase 2 |
| Network degradation and pool saturation | Phase 3 |
| OOM and CPU pressure | Phase 4 |
| Disk latency and exhaustion | Phase 5 |
| CI tiers, runbook, and retained evidence | Phase 6 |

## 12. Review Gates

### Gate A — Safety design

- Can any input select a host-wide or arbitrary target?
- Are duration, intensity, identity, and cleanup bounded independently?
- Does production composition fail closed?

### Gate B — Durable boundary accuracy

- Is each checkpoint immediately before or after the claimed durable fact?
- Does no-op injection leave transaction and acknowledgement semantics unchanged?
- Does the test prove the intended ambiguous outcome rather than a convenient earlier crash?

### Gate C — Resource realism

- Is the observed failure caused by the declared disk/network/memory/CPU mechanism?
- Is resource pressure sampled and retained?
- Does the experiment avoid relying solely on mocks or fixed sleeps?

### Gate D — Recovery and evidence

- Is recovery proven from durable and client-visible state?
- Are all invariants evaluated after recovery?
- Can an operator reproduce, diagnose, and clean up the experiment from retained evidence?

## 13. Definition of Done

The implementation is done when:

- all Spec 005 acceptance criteria are mapped to automated tests;
- the default runtime has no active chaos behavior or public activation surface;
- application checkpoints deterministically reproduce the four critical durability-boundary
  failures;
- network, pool, memory, CPU, and disk scenarios have isolated real-infrastructure evidence;
- required PR and nightly jobs pass within their declared budgets;
- forced-interruption cleanup leaves no toxics, stress processes, or test volumes behind;
- measured recovery and invariant outcomes appear in retained evidence; and
- README, chaos documentation, architecture risks, runbook, and evidence index agree with
  the implemented system.
