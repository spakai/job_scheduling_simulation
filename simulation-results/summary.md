# Simulation Results Summary

Run date: 2026-07-22

| Suite | Passed | Failed | Total |
| --- | ---: | ---: | ---: |
| CI | 18 | 0 | 18 |
| Full | 35 | 0 | 35 |

All scenario assertions passed. The full machine-readable evidence is available in
[`full.json`](full.json), with the smaller build-gating subset in [`ci.json`](ci.json).

## Reading the sample EDRs

Each scenario below includes a compact EDR sequence. The fragments show only fields that
matter to that scenario; omitted fields use this canonical envelope:

```json
{
  "eventId": "evt-00001",
  "eventType": "JOB_CREATED",
  "eventTime": "2026-07-22T10:00:00Z",
  "ingestionTime": "2026-07-22T10:00:00Z",
  "jobId": "job-123",
  "correlationId": "subscription-456:RENEW:2026-07-23",
  "jobType": "RENEW_SUBSCRIPTION",
  "scheduledAt": "2026-07-23T00:00:00Z",
  "attemptNumber": 0,
  "maxAttempts": 3
}
```

An arrow means delivery order. `× N` means the fragment is emitted for `N` jobs or
attempts. A fault such as a dropped poll or stale read is shown after the EDR sequence
because it is not itself an EDR.

## Happy-path scenarios

| Scenario | Sample EDR sequence | Result | Verified outcome |
| --- | --- | --- | --- |
| HP-01 — Job created and scheduled successfully | `{"eventType":"JOB_CREATED"}` → `{"eventType":"JOB_SCHEDULER_SUBMISSION_REQUESTED"}` → `{"eventType":"JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED","schedulerReference":"scheduler-789"}` | **PASS** | Scheduler acknowledgement produces `SCHEDULED`, preserves `scheduledAt`, records the scheduler reference, and exposes freshness. |
| HP-02 — Job executes successfully | `JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → `JOB_EXECUTION_STARTED {attemptNumber:1}` → `JOB_EXECUTION_SUCCEEDED {attemptNumber:1}` | **PASS** | Start and completion evidence produce one successful attempt and a stable `SUCCEEDED` terminal state. |
| HP-03 — Job fails and retry is scheduled | `JOB_EXECUTION_STARTED {attemptNumber:1}` → `JOB_EXECUTION_FAILED {attemptNumber:1,retryable:true,errorCode:UPSTREAM_TIMEOUT}` → `JOB_RETRY_REQUESTED {attemptNumber:2}` → `JOB_RETRY_ACKNOWLEDGED {attemptNumber:2,nextRetryAt:10:05Z}` | **PASS** | A retryable failure followed by retry acknowledgement produces `RETRY_SCHEDULED` for attempt 2. |
| HP-04 — Retry succeeds | `JOB_EXECUTION_FAILED {attemptNumber:1,retryable:true}` → `JOB_RETRY_ACKNOWLEDGED {attemptNumber:2}` → `JOB_EXECUTION_STARTED {attemptNumber:2}` → `JOB_EXECUTION_SUCCEEDED {attemptNumber:2}` | **PASS** | Attempt 1 remains failed, attempt 2 succeeds, and the final job state is `SUCCEEDED`. |
| HP-05 — Retry exhausted | `JOB_EXECUTION_STARTED` → `JOB_EXECUTION_FAILED` × 3 attempts → `JOB_RETRIES_EXHAUSTED {attemptNumber:3}` | **PASS** | Three completed attempts produce `RETRIES_EXHAUSTED`, with no next retry and no remaining eligibility. |

## Chaos and lifecycle scenarios

| Scenario | Sample EDR sequence | Result | Verified outcome |
| --- | --- | --- | --- |
| CH-01 — Duplicate `JOB_CREATED` | `JOB_CREATED {eventId:evt-1}` → `JOB_CREATED {eventId:evt-1}` | **PASS** | An identical `eventId` is applied once and does not increment the record version twice. |
| CH-02 — Semantic duplicate execution outcome | `JOB_EXECUTION_SUCCEEDED {eventId:evt-1,attemptNumber:1}` → `JOB_EXECUTION_SUCCEEDED {eventId:evt-2,attemptNumber:1}` | **PASS** | Different event IDs for the same successful attempt do not create an additional attempt. |
| CH-03 — Out-of-order `STARTED` after `SUCCEEDED` | `JOB_EXECUTION_SUCCEEDED {attemptNumber:1}` → `JOB_EXECUTION_STARTED {attemptNumber:1,eventTime:09:59:55Z}` | **PASS** | A late start backfills `startedAt` without regressing the successful terminal state. |
| CH-04 — Scheduler acknowledgement missing | `JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_REQUESTED` → no acknowledgement EDR | **PASS** | The job remains `PENDING_SUBMISSION`; reconciliation emits `SCHEDULER_ACK_TIMEOUT`. |
| CH-05 — Acknowledged but execution EDR missing | `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED {scheduledAt:10:00Z}` → no start EDR | **PASS** | The derived state becomes `OVERDUE` while the evidence-backed recorded state remains `SCHEDULED`. |
| CH-06 — Execution delayed within grace period | `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED {scheduledAt:10:00Z}` → no start EDR after five minutes | **PASS** | A job with no start evidence remains `AWAITING_EXECUTION` before the grace period expires. |
| CH-07 — Retry acknowledgement missing | `JOB_EXECUTION_FAILED {attemptNumber:1,retryable:true}` → `JOB_RETRY_REQUESTED {attemptNumber:2}` → no retry acknowledgement EDR | **PASS** | Retry intent remains `RETRY_PENDING`; scheduler acknowledgement and `nextRetryAt` remain absent. |
| CH-08 — Retry acknowledgement arrives late | `JOB_RETRY_REQUESTED {attemptNumber:2}` → three-minute delay → `JOB_RETRY_ACKNOWLEDGED {attemptNumber:2,nextRetryAt:10:03Z}` | **PASS** | Late acknowledgement moves the job to `RETRY_SCHEDULED` and retains the resolved timeout finding in history. |
| CH-09 — Retry executes before acknowledgement EDR | `JOB_EXECUTION_STARTED {attemptNumber:2}` → `JOB_RETRY_ACKNOWLEDGED {attemptNumber:2}` | **PASS** | A late retry acknowledgement cannot move an already running attempt back to `RETRY_SCHEDULED`. |
| CH-10 — EDR delivery delay | `JOB_EXECUTION_STARTED {attemptNumber:1,eventTime:10:00Z,ingestionTime:10:10Z}` | **PASS** | Lifecycle state follows event semantics, `dataAsOf` follows ingestion, and the 600-second processing delay is visible. |
| CH-11 — Execution start EDR dropped | dropped `JOB_EXECUTION_STARTED` → `JOB_EXECUTION_SUCCEEDED {attemptNumber:1}` | **PASS** | A success outcome remains authoritative while `startedAt` stays unknown and the lifecycle is marked incomplete. |
| CH-12 — Invalid attempt number | `JOB_EXECUTION_FAILED {attemptNumber:2,retryable:true}` with no attempt 1 EDR | **PASS** | Attempt 2 does not cause attempt 1 to be invented; reconciliation emits `ATTEMPT_SEQUENCE_GAP`. |
| CH-13 — Concurrent job updates | `JOB_CREATED` → concurrent `JOB_SCHEDULER_SUBMISSION_REQUESTED` and `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED`; both projector writes initially use expected version 1 | **PASS** | A stale optimistic writer is rejected, reloads, reapplies safely, and loses no event. |
| CH-14 — Stale API replica | `JOB_CREATED {ingestionTime:10:00Z}` → API read at `10:00:30Z` | **PASS** | `dataAsOf` is returned and the delayed snapshot is marked as possibly stale. |
| CH-15 — Job succeeds after overdue | `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED {scheduledAt:10:00Z}` → 11-minute delay → `JOB_EXECUTION_STARTED` → `JOB_EXECUTION_SUCCEEDED` | **PASS** | Late execution can finish successfully while the earlier overdue finding remains auditable. |
| CH-16 — Late failure after success | `JOB_EXECUTION_SUCCEEDED {attemptNumber:1}` → `JOB_EXECUTION_FAILED {attemptNumber:1,retryable:false}` | **PASS** | Success remains terminal and reconciliation records `CONFLICTING_TERMINAL_OUTCOME`. |
| CH-17 — Cancelled job executes later | `JOB_CANCELLED` → `JOB_EXECUTION_STARTED {attemptNumber:1}` → `JOB_EXECUTION_SUCCEEDED {attemptNumber:1}` | **PASS** | The observed operational outcome becomes `SUCCEEDED`, with a cancellation violation recorded. |
| CH-18 — Attempt starts after retry exhaustion | `JOB_RETRIES_EXHAUSTED {attemptNumber:3}` → `JOB_EXECUTION_STARTED {attemptNumber:4}` | **PASS** | Observed execution becomes `RUNNING`, with an execution-after-exhaustion violation recorded. |

## Polling and capacity scenarios

The simulation uses a one-minute poll interval. Polling scenarios use `X=3` to make
batch boundaries and backlog behavior easy to inspect.

| Scenario | Sample EDR sequence | Result | Verified outcome |
| --- | --- | --- | --- |
| POLL-01 — Eligible just before a poll | `JOB_CREATED {scheduledAt:10:00:59Z}` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → `JOB_SCHEDULER_ITEM_RETRIEVED {pollTime:10:01Z,attemptNumber:1}` → `JOB_EXECUTION_STARTED` | **PASS** | The job is selected at the next polling opportunity. |
| POLL-02 — Eligible just after a poll | `JOB_CREATED {scheduledAt:10:00:01Z}` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → empty 10:00 poll → `JOB_SCHEDULER_ITEM_RETRIEVED {pollTime:10:01Z}` | **PASS** | The job is not marked overdue during its normal wait and is selected by the following poll. |
| POLL-03 — Exactly `X` eligible jobs | (`JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED`) × 3 jobs → `JOB_SCHEDULER_ITEM_RETRIEVED {batchLimit:3}` × 3 | **PASS** | One poll retrieves all three jobs exactly once. |
| POLL-04 — `X + 1` eligible jobs | (`JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED`) × 4 jobs → three retrieval EDRs at 10:00 → one retrieval EDR at 10:01 | **PASS** | Three jobs are selected by the first poll and the remaining job by the next poll. |
| POLL-05 — Multi-batch backlog | (`JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED`) × 10 jobs → `JOB_SCHEDULER_ITEM_RETRIEVED {batchLimit:3}` × 10 over four polls | **PASS** | Ten eligible jobs drain across four polls without duplicate retrieval or starvation. |
| POLL-06 — Poll skipped | `JOB_CREATED {scheduledAt:10:00Z}` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → no poll or retrieval EDR for two minutes | **PASS** | The job retains its evidence-backed scheduler state when no polling evidence is observed. |
| POLL-07 — Scheduler retrieves fewer than `X` | acknowledged jobs × 3 → `JOB_SCHEDULER_ITEM_RETRIEVED {batchLimit:2}` × 2 | **PASS** | Poll telemetry exposes three eligible jobs but only two retrieved jobs. |
| POLL-08 — Retrieved but worker unavailable | `JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → `JOB_SCHEDULER_ITEM_RETRIEVED {attemptNumber:1}` → no start EDR | **PASS** | The job becomes `AWAITING_EXECUTION` with `RETRIEVED_AWAITING_WORKER` delay classification. |
| POLL-09 — Multiple concurrent pollers | acknowledged jobs × 6 → retrieval EDRs with `pollId:a-poll-0001` × 3 → retrieval EDRs with `pollId:b-poll-0002` × 3 | **PASS** | Two pollers retrieve six jobs without double-claiming any job. |
| POLL-10 — Retrieval EDR missing | `JOB_CREATED` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → dropped `JOB_SCHEDULER_ITEM_RETRIEVED` → `JOB_EXECUTION_STARTED` → `JOB_EXECUTION_SUCCEEDED` | **PASS** | Execution succeeds without inventing retrieval evidence, and the lifecycle is marked incomplete. |
| POLL-11 — Retrieval EDR delayed | `JOB_EXECUTION_STARTED {attemptNumber:1}` → late `JOB_SCHEDULER_ITEM_RETRIEVED {attemptNumber:1,eventTime:09:59:59Z}` | **PASS** | A late retrieval EDR backfills evidence without moving an already running job backwards. |
| POLL-12 — Stale visibility data | `JOB_CREATED {ingestionTime:10:00Z}` → `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED` → API read after 30 seconds | **PASS** | The API exposes that the returned visibility snapshot is stale. |

## Reproduce

```bash
.venv/bin/pytest
.venv/bin/job-visibility-sim ci --output simulation-results/ci.json
.venv/bin/job-visibility-sim --output simulation-results/full.json
```
