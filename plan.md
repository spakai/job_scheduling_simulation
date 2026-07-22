# Scheduled Job Visibility Simulation — Implementation Plan

## 1. Objective

Build a Python simulation that validates the accuracy and explainability of scheduled-job visibility when an external scheduler:

- Is outside the application's control.
- Polls for eligible jobs once every minute.
- Retrieves at most `X` jobs per poll.
- Communicates scheduler and execution evidence through EDRs.

The simulation must explain whether a job was acknowledged, became eligible, was retrieved, started, completed, retried, or became late. It must never claim that the external scheduler holds or retrieved a job unless the corresponding evidence was observed.

The primary operational problem to model is that a job may not run at its intended `scheduledAt` time because of polling cadence, batch limits, backlog, worker capacity, skipped polls, or delayed/missing EDRs.

## 2. Recommended Technology

- Python 3.12+
- FastAPI for the visibility API
- Pydantic for EDR, configuration, and API models
- SQLAlchemy 2 for persistence
- Alembic for database migrations
- PostgreSQL for transactions, row locking, and optimistic concurrency
- pytest for unit and scenario tests
- Testcontainers for PostgreSQL integration tests
- Ruff and mypy for linting and type checking

The simulator will use an injectable virtual clock. Tests must advance simulated time rather than sleep for real minutes.

## 3. Proposed Repository Structure

```text
job_scheduling_simulation/
  pyproject.toml
  alembic.ini
  src/job_visibility/
    api/
      app.py
      routes.py
      schemas.py
    domain/
      edr.py
      enums.py
      job.py
      attempts.py
      projection.py
      policies.py
    persistence/
      models.py
      repositories.py
      unit_of_work.py
      migrations/
    projector/
      consumer.py
      service.py
    reconciler/
      service.py
      findings.py
    simulation/
      clock.py
      producer.py
      scheduler.py
      transport.py
      runner.py
      assertions.py
      scenarios/
    config.py
  tests/
    unit/
    integration/
    scenarios/
```

## 4. Scheduler Model

The external scheduler simulator will run a poll cycle every configured interval:

1. Find jobs where `scheduledAt <= pollTime` that have not been retrieved or cancelled.
2. Order them using the configured selection policy.
3. Retrieve no more than `maxItemsPerPoll` (`X`).
4. Emit retrieval evidence for selected jobs.
5. Start retrieved jobs subject to worker capacity.
6. Leave overflow jobs eligible for a later poll.

Default configuration:

```yaml
scheduler:
  pollIntervalSeconds: 60
  maxItemsPerPoll: 100
  workerCapacity: 20
  selectionOrder: scheduledAt_asc
  defaultExecutionDurationSeconds: 5

visibility:
  executionGracePeriodSeconds: 600
  runningTimeoutSeconds: 1800
  retryAcknowledgementTimeoutSeconds: 120
  eventualConsistencyTargetSeconds: 10

retry:
  maxAttemptsDefault: 3
  backoffSeconds: [60, 300, 900]

edr:
  deduplicationRetentionSeconds: 86400
```

The simulator should support one or more pollers. Concurrent polling must use behavior equivalent to `FOR UPDATE SKIP LOCKED` so that two pollers cannot retrieve the same job.

## 5. Lifecycle Evidence

Use the lifecycle events from the specification and add an explicit retrieval event:

- `JOB_CREATED`
- `JOB_SCHEDULER_SUBMISSION_REQUESTED`
- `JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED`
- `JOB_SCHEDULER_SUBMISSION_FAILED`
- `JOB_SCHEDULER_ITEM_RETRIEVED`
- `JOB_EXECUTION_STARTED`
- `JOB_EXECUTION_SUCCEEDED`
- `JOB_EXECUTION_FAILED`
- `JOB_RETRY_REQUESTED`
- `JOB_RETRY_ACKNOWLEDGED`
- `JOB_RETRY_REJECTED`
- `JOB_RETRIES_EXHAUSTED`
- `JOB_CANCELLED`

`JOB_SCHEDULER_ITEM_RETRIEVED` is important because acknowledgement only proves acceptance or registration. It does not prove that a polling scheduler selected the job for execution.

If the real company integration cannot emit retrieval evidence, the simulator must support that mode. In that case, visibility may infer that the job was eligible for a poll, but it must not claim that the scheduler retrieved it.

## 6. Visibility State Model

Retain the specified business states:

- `CREATED`
- `PENDING_SUBMISSION`
- `SCHEDULED`
- `AWAITING_EXECUTION`
- `RUNNING`
- `RETRY_PENDING`
- `RETRY_SCHEDULED`
- `SUCCEEDED`
- `FAILED`
- `RETRIES_EXHAUSTED`
- `OVERDUE`
- `CANCELLED`
- `UNKNOWN`

Expose a separate delay classification so that state and explanation are not conflated:

- `NOT_YET_ELIGIBLE`
- `AWAITING_POLL_WINDOW`
- `POSSIBLE_BATCH_BACKLOG`
- `RETRIEVED_AWAITING_WORKER`
- `EXECUTION_OVERDUE`
- `EDR_DELIVERY_DELAY`
- `CAUSE_UNKNOWN`

The API must return both:

- `recordedStatus`: the state supported directly by recorded lifecycle EDRs.
- `status`: the current business state after applying time-based rules.

For example, an acknowledged job with no start event may have `recordedStatus=SCHEDULED` and `status=OVERDUE`.

Terminal states are `SUCCEEDED`, `FAILED`, `RETRIES_EXHAUSTED`, and `CANCELLED`. Late non-terminal events must not regress a terminal outcome, except where the explicit violation policies in the specification require observed execution facts to take precedence.

## 7. Timing and Delay Measurements

Store and expose the following timestamps when evidence exists:

- `createdAt`
- `submissionRequestedAt`
- `schedulerAcknowledgedAt`
- `scheduledAt`
- `schedulerRetrievedAt`
- `startedAt`
- `completedAt`
- `eventTime`
- `ingestionTime`
- `dataAsOf`

Derive:

- `eligibilityDelaySeconds = max(0, schedulerRetrievedAt - scheduledAt)`
- `startDelaySeconds = max(0, startedAt - scheduledAt)`
- `postRetrievalDelaySeconds = max(0, startedAt - schedulerRetrievedAt)`
- `processingDelaySeconds = ingestionTime - eventTime`
- `overdueDurationSeconds = now - overdueThreshold`

Missing timestamps must remain `null`; they must not be invented.

For an unlimited-worker polling model, an estimate may be exposed separately:

```text
estimatedPollsRequired = ceil(queuePosition / maxItemsPerPoll)
estimatedRetrievalDelay = estimatedPollsRequired * pollIntervalSeconds
```

This is an estimate, not a recorded fact. The API must label the estimate and include `estimateAsOf`.

## 8. Time-Based Rules

The expected first polling opportunity is the first poll at or after `scheduledAt`. A job scheduled immediately after a poll may normally wait almost one complete poll interval.

Rules:

1. Before `scheduledAt`, an acknowledged job remains `SCHEDULED`.
2. After `scheduledAt` but before its first expected poll, classify it as `AWAITING_POLL_WINDOW` without calling it overdue.
3. After an expected poll with no retrieval evidence, retain the recorded scheduler state and classify the delay as possible backlog, skipped polling, missing retrieval evidence, or unknown according to available facts.
4. After retrieval but before execution starts, return `AWAITING_EXECUTION` with `RETRIEVED_AWAITING_WORKER`.
5. After the configured business grace period with no start evidence, return `OVERDUE`.
6. A late `JOB_EXECUTION_STARTED` moves an overdue job to `RUNNING` while retaining overdue history and measured delay.
7. A later terminal execution outcome becomes the final state while preserving all delay metrics and findings.

The implementation must not report batch backlog as fact unless queue or poll evidence supports it.

## 9. Canonical EDR Model

Extend the canonical schema with optional polling metadata:

```json
{
  "eventId": "evt-001",
  "eventType": "JOB_SCHEDULER_ITEM_RETRIEVED",
  "eventTime": "2026-07-23T00:03:00Z",
  "ingestionTime": "2026-07-23T00:03:01Z",
  "jobId": "job-123",
  "correlationId": "subscription-456:RENEW:2026-07-23",
  "jobType": "RENEW_SUBSCRIPTION",
  "sourceSystem": "scheduler-simulator",
  "schedulerReference": "scheduler-789",
  "scheduledAt": "2026-07-23T00:00:00Z",
  "pollId": "poll-20260723-0003",
  "pollTime": "2026-07-23T00:03:00Z",
  "batchPosition": 42,
  "batchLimit": 100,
  "attemptNumber": 1,
  "version": 4
}
```

Polling metadata is optional because the real scheduler may not provide it.

## 10. Persistence Design

Create these tables:

### `edr_events`

- Immutable event journal.
- Unique constraint on `event_id`.
- Retains raw payload, event time, ingestion time, and processing decision.

### `job_visibility`

- Current materialized job summary.
- Stores recorded state, derived-state inputs, timestamps, retry summary, quality flags, and `data_as_of`.
- Uses an integer `version` for optimistic concurrency.

### `job_attempts`

- One logical record per `(job_id, attempt_number)`.
- Prevents semantic duplicate outcomes from inflating attempt counts.

### `scheduler_polls`

- Records poll time, requested batch limit, retrieved count, eligible count when known, poller ID, and outcome.
- Supports skipped-poll and backlog analysis in the simulator.

### `projection_decisions`

- Records whether an event was applied, ignored as an exact duplicate, ignored as a semantic duplicate, used to backfill data, or marked conflicting.

### `reconciliation_findings`

- Stores finding code, supporting evidence, first/last observed time, current status, and resolution time.
- Findings remain in history after late evidence repairs the current state.

### `simulation_runs`

- Stores scenario definition, delivery order, faults, API result, assertions, and final pass/fail result.

## 11. Projection Engine

Implement projection as a deterministic domain function:

```text
current projection + attempts + incoming EDR + effective time
    -> projection decision + updated projection + findings
```

For each EDR, the projector will:

1. Insert the immutable event.
2. Reject duplicate `eventId` delivery without incrementing the job version.
3. Load the job and attempts.
4. Apply lifecycle precedence and semantic deduplication.
5. Merge newly observed timestamps without deleting existing evidence.
6. Update the job using `WHERE version = expectedVersion`.
7. Reload and reapply safely after an optimistic-lock conflict.
8. Persist the projection decision and findings transactionally.

Required invariants:

- Submission request does not imply scheduler acknowledgement.
- Scheduler acknowledgement does not imply polling retrieval.
- Retrieval does not imply execution start.
- Retry request does not imply retry acknowledgement.
- Exact duplicates do not increment the version.
- Semantic duplicates do not create additional attempts.
- Late events may backfill missing timestamps without regressing state.
- Missing attempt numbers are not inferred.
- Conflicting terminal outcomes are retained and flagged.
- Every API response exposes data freshness.

## 12. Reconciliation

Implement reconciliation as a callable service and periodic worker. It should detect:

- `SCHEDULER_ACK_TIMEOUT`
- `EXPECTED_POLL_NOT_OBSERVED`
- `POSSIBLE_BATCH_BACKLOG`
- `EXECUTION_START_OVERDUE`
- `RETRIEVED_BUT_NOT_STARTED`
- `RUNNING_TIMEOUT`
- `RETRY_ACK_TIMEOUT`
- `RETRY_WINDOW_EXPIRED`
- `ATTEMPT_SEQUENCE_GAP`
- `INCONSISTENT_ATTEMPT_COUNTER`
- `CONFLICTING_TERMINAL_OUTCOME`
- `EXECUTED_AFTER_CANCEL`
- `EXECUTION_AFTER_RETRY_EXHAUSTION`
- `EDR_PROCESSING_DELAY_EXCEEDED`

The wording of each finding must distinguish observed evidence from inference. For example, absence of retrieval evidence may mean a backlog, skipped poll, lost EDR, or stale visibility data.

## 13. Visibility API

Implement:

- `GET /scheduled-jobs/{jobId}`
- `GET /scheduled-jobs/{jobId}/attempts`
- `GET /scheduled-jobs?status=...`
- `GET /scheduled-jobs?correlationId=...`
- `GET /scheduled-jobs?scheduledFrom=...&scheduledTo=...`

Add optional operational filters:

- `delayClassification`
- `overdueBySecondsFrom`
- `schedulerRetrieved=true|false`
- `dataAsOfBefore`

Example response fields:

```json
{
  "jobId": "job-123",
  "status": "OVERDUE",
  "recordedStatus": "SCHEDULED",
  "scheduledAt": "2026-07-23T00:00:00Z",
  "schedulerAcknowledgedAt": "2026-07-22T10:00:02Z",
  "schedulerRetrievedAt": null,
  "startedAt": null,
  "completedAt": null,
  "delay": {
    "classification": "POSSIBLE_BATCH_BACKLOG",
    "startDelaySeconds": 240,
    "overdueDurationSeconds": 120,
    "explanation": "No scheduler retrieval or execution-start evidence was observed after the expected polling window."
  },
  "freshness": {
    "dataAsOf": "2026-07-23T00:04:00Z",
    "processingDelaySeconds": 1,
    "possiblyStale": false
  },
  "version": 7
}
```

The 404 response must retain the specification's warning that no visibility record does not prove that no scheduler job exists.

## 14. Simulation Components

### Job producer

- Creates jobs and writes creation/submission EDRs.
- Supports write failure and duplicate submission.

### Polling scheduler

- Polls every minute using virtual time.
- Selects at most `X` eligible jobs.
- Models ordering, batch overflow, worker capacity, skipped polls, partial batches, and concurrent pollers.
- Emits acknowledgement, retrieval, start, and outcome EDRs as configured.

### EDR transport

- Supports duplicate, drop, delay, reorder, optional-field corruption, and burst delivery.
- Applies faults independently to acknowledgement, retrieval, execution, and retry EDRs.

### Projector

- Consumes EDRs and maintains visibility and attempt projections.

### Reconciler

- Advances time-derived status and emits findings without inventing scheduler facts.

### Scenario runner

- Advances the virtual clock.
- Runs polls and delivers selected EDRs.
- Calls the API and reconciliation service.
- Produces the report required by the specification.

## 15. Scenario Plan

Implement all happy-path and chaos scenarios from the specification, plus these polling-specific scenarios:

### POLL-01: Eligible just before a poll

- Job becomes eligible one second before polling.
- It is retrieved in the next batch.
- Expected start delay remains within the normal polling window.

### POLL-02: Eligible just after a poll

- Job becomes eligible one second after polling.
- It waits almost 60 seconds.
- It must not be marked overdue during the expected polling interval.

### POLL-03: Exactly `X` eligible jobs

- All jobs are retrieved in one poll.
- No duplicate retrieval occurs.

### POLL-04: `X + 1` eligible jobs

- `X` jobs are retrieved on the first poll.
- The remaining job is retrieved on the next poll.
- Its delay is attributed to observed batch backlog when poll evidence is available.

### POLL-05: Multi-batch backlog

- More than `3X` jobs become eligible together.
- Jobs are processed across the expected number of polls.
- Queue ordering is stable and older jobs do not starve.

### POLL-06: Poll skipped

- One scheduled poll does not occur.
- Reconciliation reports the missing poll when poll telemetry is available.
- Affected jobs retain accurate evidence-based states.

### POLL-07: Scheduler retrieves fewer than `X`

- Eligible work exceeds the batch size, but the scheduler retrieves only part of the allowed batch.
- The shortfall and resulting delay are observable.

### POLL-08: Retrieved but worker unavailable

- Retrieval is acknowledged.
- Execution start is delayed by worker capacity.
- Delay classification is `RETRIEVED_AWAITING_WORKER`.

### POLL-09: Multiple concurrent pollers

- Two pollers select work at the same time.
- Each job is retrieved once.
- No event or job is lost.

### POLL-10: Retrieval EDR missing

- The scheduler retrieves and executes a job, but the retrieval EDR is dropped.
- A later execution event produces the correct operational state.
- The lifecycle is marked incomplete.

### POLL-11: Retrieval EDR delayed

- Execution starts before the retrieval EDR reaches the projector.
- The job becomes `RUNNING` immediately.
- The late retrieval event backfills evidence without regression.

### POLL-12: Stale visibility data

- Scheduler and EDR processing are correct, but the API reads a delayed replica.
- `dataAsOf` and `possiblyStale` expose the limitation.

## 16. Declarative Scenario Format

Define scenarios as data so that the same runner can execute all cases:

```python
Scenario(
    id="POLL-04",
    name="Batch limit leaves one job for the next poll",
    scheduler=SchedulerConfig(
        poll_interval_seconds=60,
        max_items_per_poll=100,
    ),
    jobs=[...],
    clock_advances=[...],
    transport_faults=[],
    expected=ScenarioExpectation(...),
)
```

Each run must output:

- Scenario ID and name.
- Input jobs and events.
- Poll times and selected batches.
- EDR delivery order.
- Injected faults.
- Final visibility records.
- API responses.
- Reconciliation findings.
- Individual assertions.
- Overall result.

## 17. Testing Strategy

### Unit tests

- Every EDR-to-state transition.
- Terminal precedence.
- Timestamp backfilling.
- Exact and semantic deduplication.
- Poll-window calculations.
- Batch selection and queue ordering.
- Retry calculations.
- Delay classification.

### Integration tests

- PostgreSQL transactions and constraints.
- Optimistic-lock retry behavior.
- Concurrent pollers with row locking.
- Projector concurrency.
- API filtering and freshness fields.
- Reconciliation finding history.

### End-to-end scenario tests

- All happy-path scenarios.
- All original chaos scenarios.
- All polling-specific scenarios.
- Simulation report schema validation.

CI should run the minimum scenario set from the specification plus `POLL-02`, `POLL-04`, `POLL-08`, `POLL-09`, and `POLL-10`. Run the full chaos and load suite nightly or before release.

## 18. Delivery Milestones

1. Initialize the Python project, quality tools, configuration, and virtual clock.
2. Implement Pydantic domain and EDR models.
3. Add PostgreSQL schema, migrations, and repositories.
4. Implement the pure projection engine and unit tests.
5. Implement transactional EDR consumption and optimistic concurrency.
6. Implement the polling scheduler with configurable `X` and one-minute cadence.
7. Implement worker-capacity and backlog behavior.
8. Implement reconciliation and delay classifications.
9. Implement the FastAPI visibility endpoints.
10. Implement the declarative scenario runner and output report.
11. Add original happy-path and chaos scenarios.
12. Add polling, backlog, skipped-poll, and capacity scenarios.
13. Add CI, nightly tests, documentation, and reproducible commands.

## 19. Acceptance Criteria

The implementation is complete when:

- All happy-path scenarios pass.
- All required CI scenarios pass deterministically with a virtual clock.
- A one-minute polling cadence and configurable batch limit `X` are modeled explicitly.
- Jobs beyond the batch limit remain eligible for later polls without being lost.
- The API explains intended-time delay using recorded evidence when available.
- Acknowledged, retrieved, and started remain distinct facts.
- The API never claims retrieval, execution, or scheduler presence without evidence.
- Normal poll-window delay is distinguishable from overdue execution.
- Backlog delay and post-retrieval worker delay are distinguishable when evidence permits.
- Duplicate events never create duplicate attempts.
- Out-of-order events never incorrectly regress state.
- Late evidence repairs incomplete projections without erasing history.
- Concurrent pollers and projectors do not lose or duplicate work.
- Every API response exposes freshness and version information.
- Reconciliation identifies missing, stuck, conflicting, delayed, and inconsistent lifecycles.

## 20. Early Decisions to Confirm During Implementation

These values should be configuration rather than hard-coded assumptions:

- The production value of `X`.
- Whether selection is strictly oldest `scheduledAt` first or includes priority.
- Whether retrieved jobs count against `X` before or after filtering/validation.
- Whether execution happens inside the scheduler or in separate workers.
- Worker concurrency and typical execution duration.
- Whether the real scheduler provides poll or retrieval evidence.
- The business SLA for "late," separately from the one-minute polling interval.
- Whether retries share the same polling queue and batch limit.
- How multiple scheduler instances coordinate claims.

The implementation can begin with the documented defaults while keeping all of these policies replaceable.
