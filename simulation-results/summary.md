# Simulation Results Summary

Run date: 2026-07-22

| Suite | Passed | Failed | Total |
| --- | ---: | ---: | ---: |
| CI | 18 | 0 | 18 |
| Full | 35 | 0 | 35 |

All scenario assertions passed. The full machine-readable evidence is available in
[`full.json`](full.json), with the smaller build-gating subset in [`ci.json`](ci.json).

## Happy-path scenarios

| Scenario | Result | Verified outcome |
| --- | --- | --- |
| HP-01 — Job created and scheduled successfully | **PASS** | Scheduler acknowledgement produces `SCHEDULED`, preserves `scheduledAt`, records the scheduler reference, and exposes freshness. |
| HP-02 — Job executes successfully | **PASS** | Start and completion evidence produce one successful attempt and a stable `SUCCEEDED` terminal state. |
| HP-03 — Job fails and retry is scheduled | **PASS** | A retryable failure followed by retry acknowledgement produces `RETRY_SCHEDULED` for attempt 2. |
| HP-04 — Retry succeeds | **PASS** | Attempt 1 remains failed, attempt 2 succeeds, and the final job state is `SUCCEEDED`. |
| HP-05 — Retry exhausted | **PASS** | Three completed attempts produce `RETRIES_EXHAUSTED`, with no next retry and no remaining eligibility. |

## Chaos and lifecycle scenarios

| Scenario | Result | Verified outcome |
| --- | --- | --- |
| CH-01 — Duplicate `JOB_CREATED` | **PASS** | An identical `eventId` is applied once and does not increment the record version twice. |
| CH-02 — Semantic duplicate execution outcome | **PASS** | Different event IDs for the same successful attempt do not create an additional attempt. |
| CH-03 — Out-of-order `STARTED` after `SUCCEEDED` | **PASS** | A late start backfills `startedAt` without regressing the successful terminal state. |
| CH-04 — Scheduler acknowledgement missing | **PASS** | The job remains `PENDING_SUBMISSION`; reconciliation emits `SCHEDULER_ACK_TIMEOUT`. |
| CH-05 — Acknowledged but execution EDR missing | **PASS** | The derived state becomes `OVERDUE` while the evidence-backed recorded state remains `SCHEDULED`. |
| CH-06 — Execution delayed within grace period | **PASS** | A job with no start evidence remains `AWAITING_EXECUTION` before the grace period expires. |
| CH-07 — Retry acknowledgement missing | **PASS** | Retry intent remains `RETRY_PENDING`; scheduler acknowledgement and `nextRetryAt` remain absent. |
| CH-08 — Retry acknowledgement arrives late | **PASS** | Late acknowledgement moves the job to `RETRY_SCHEDULED` and retains the resolved timeout finding in history. |
| CH-09 — Retry executes before acknowledgement EDR | **PASS** | A late retry acknowledgement cannot move an already running attempt back to `RETRY_SCHEDULED`. |
| CH-10 — EDR delivery delay | **PASS** | Lifecycle state follows event semantics, `dataAsOf` follows ingestion, and the 600-second processing delay is visible. |
| CH-11 — Execution start EDR dropped | **PASS** | A success outcome remains authoritative while `startedAt` stays unknown and the lifecycle is marked incomplete. |
| CH-12 — Invalid attempt number | **PASS** | Attempt 2 does not cause attempt 1 to be invented; reconciliation emits `ATTEMPT_SEQUENCE_GAP`. |
| CH-13 — Concurrent job updates | **PASS** | A stale optimistic writer is rejected, reloads, reapplies safely, and loses no event. |
| CH-14 — Stale API replica | **PASS** | `dataAsOf` is returned and the delayed snapshot is marked as possibly stale. |
| CH-15 — Job succeeds after overdue | **PASS** | Late execution can finish successfully while the earlier overdue finding remains auditable. |
| CH-16 — Late failure after success | **PASS** | Success remains terminal and reconciliation records `CONFLICTING_TERMINAL_OUTCOME`. |
| CH-17 — Cancelled job executes later | **PASS** | The observed operational outcome becomes `SUCCEEDED`, with a cancellation violation recorded. |
| CH-18 — Attempt starts after retry exhaustion | **PASS** | Observed execution becomes `RUNNING`, with an execution-after-exhaustion violation recorded. |

## Polling and capacity scenarios

The simulation uses a one-minute poll interval. Polling scenarios use `X=3` to make
batch boundaries and backlog behavior easy to inspect.

| Scenario | Result | Verified outcome |
| --- | --- | --- |
| POLL-01 — Eligible just before a poll | **PASS** | The job is selected at the next polling opportunity. |
| POLL-02 — Eligible just after a poll | **PASS** | The job is not marked overdue during its normal wait and is selected by the following poll. |
| POLL-03 — Exactly `X` eligible jobs | **PASS** | One poll retrieves all three jobs exactly once. |
| POLL-04 — `X + 1` eligible jobs | **PASS** | Three jobs are selected by the first poll and the remaining job by the next poll. |
| POLL-05 — Multi-batch backlog | **PASS** | Ten eligible jobs drain across four polls without duplicate retrieval or starvation. |
| POLL-06 — Poll skipped | **PASS** | The job retains its evidence-backed scheduler state when no polling evidence is observed. |
| POLL-07 — Scheduler retrieves fewer than `X` | **PASS** | Poll telemetry exposes three eligible jobs but only two retrieved jobs. |
| POLL-08 — Retrieved but worker unavailable | **PASS** | The job becomes `AWAITING_EXECUTION` with `RETRIEVED_AWAITING_WORKER` delay classification. |
| POLL-09 — Multiple concurrent pollers | **PASS** | Two pollers retrieve six jobs without double-claiming any job. |
| POLL-10 — Retrieval EDR missing | **PASS** | Execution succeeds without inventing retrieval evidence, and the lifecycle is marked incomplete. |
| POLL-11 — Retrieval EDR delayed | **PASS** | A late retrieval EDR backfills evidence without moving an already running job backwards. |
| POLL-12 — Stale visibility data | **PASS** | The API exposes that the returned visibility snapshot is stale. |

## Reproduce

```bash
.venv/bin/pytest
.venv/bin/job-visibility-sim ci --output simulation-results/ci.json
.venv/bin/job-visibility-sim --output simulation-results/full.json
```
