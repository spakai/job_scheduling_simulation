Scheduled Job Visibility Simulation Specification

1. Purpose

This specification defines a simulation environment for validating scheduled-job visibility when the scheduler is outside our control and the only reliable integration point is the writing and retrieval of EDRs.

The simulation must verify that the visibility API presents an accurate, explainable view of a job across both normal and failure conditions.

The system must never claim that a job exists inside the external scheduler unless scheduler acknowledgement has been observed.

2. Scope

The simulation covers:

Job creation

Scheduler submission

Scheduler acknowledgement

Scheduled execution

Successful completion

Failed execution

Retry eligibility

Retry request

Retry acknowledgement

Retry execution

Retry exhaustion

Duplicate EDRs

Out-of-order EDRs

Missing EDRs

Delayed EDR delivery

Concurrent updates

Stale reads

Visibility API behaviour

Overdue detection

Reconciliation

The simulation does not attempt to reproduce the internal implementation of the external scheduler.

3. High-Level Architecture

Job Producer
    |
    | writes lifecycle EDRs
    v
EDR Store / EDR Stream
    |
    v
EDR Consumer / Projector
    |
    v
Job Visibility Store
    |
    v
Job Visibility API

External Scheduler Simulator
    |
    | produces acknowledgement and execution outcomes
    v
EDR Writer

4. Core Design Principles

EDRs are observable facts.

The visibility store is a materialized view derived from EDRs.

The visibility API exposes business-oriented job state, not raw EDR format.

Duplicate EDRs must not corrupt state.

Out-of-order EDRs must not move terminal state backwards.

Scheduler intent and scheduler acknowledgement must be represented separately.

Retry requested and retry scheduled must be represented separately.

Missing evidence must be reported as unknown, pending, or overdue rather than assumed.

Job state updates must support optimistic concurrency or equivalent version control.

Every API response must expose data freshness.

5. Job Lifecycle

5.1 Recorded lifecycle events

JOB_CREATED
JOB_SCHEDULER_SUBMISSION_REQUESTED
JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED
JOB_SCHEDULER_SUBMISSION_FAILED
JOB_EXECUTION_STARTED
JOB_EXECUTION_SUCCEEDED
JOB_EXECUTION_FAILED
JOB_RETRY_REQUESTED
JOB_RETRY_ACKNOWLEDGED
JOB_RETRY_REJECTED
JOB_RETRIES_EXHAUSTED
JOB_CANCELLED

5.2 Derived visibility states

CREATED
PENDING_SUBMISSION
SCHEDULED
AWAITING_EXECUTION
RUNNING
RETRY_PENDING
RETRY_SCHEDULED
SUCCEEDED
FAILED
RETRIES_EXHAUSTED
OVERDUE
CANCELLED
UNKNOWN

5.3 State rules

JOB_CREATED
  -> CREATED

JOB_SCHEDULER_SUBMISSION_REQUESTED
  -> PENDING_SUBMISSION

JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED
  -> SCHEDULED

scheduledAt passed, within grace period, no start observed
  -> AWAITING_EXECUTION

scheduledAt + grace period passed, no start observed
  -> OVERDUE

JOB_EXECUTION_STARTED
  -> RUNNING

JOB_EXECUTION_SUCCEEDED
  -> SUCCEEDED

JOB_EXECUTION_FAILED and retryable = false
  -> FAILED

JOB_EXECUTION_FAILED and retryable = true and retry not yet requested
  -> RETRY_PENDING

JOB_RETRY_REQUESTED without acknowledgement
  -> RETRY_PENDING

JOB_RETRY_ACKNOWLEDGED
  -> RETRY_SCHEDULED

JOB_RETRIES_EXHAUSTED
  -> RETRIES_EXHAUSTED

JOB_CANCELLED
  -> CANCELLED

Terminal states are:

SUCCEEDED
FAILED
RETRIES_EXHAUSTED
CANCELLED

A late non-terminal event must not move a job out of a terminal state.

6. Canonical EDR Schema

{
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
  "version": 1,
  "payloadReference": "subscription-456"
}

7. Visibility API

7.1 Retrieve job

GET /scheduled-jobs/{jobId}

Example response:

{
  "jobId": "job-123",
  "correlationId": "subscription-456:RENEW:2026-07-23",
  "jobType": "RENEW_SUBSCRIPTION",
  "status": "RETRY_SCHEDULED",
  "recordedStatus": "RETRY_SCHEDULED",
  "createdAt": "2026-07-22T10:00:00Z",
  "scheduledAt": "2026-07-23T00:00:00Z",
  "startedAt": "2026-07-23T00:00:05Z",
  "completedAt": null,
  "schedulerReference": "scheduler-789",
  "retry": {
    "completedAttempts": 1,
    "nextAttemptNumber": 2,
    "maxAttempts": 3,
    "eligible": true,
    "requested": true,
    "schedulerAcknowledged": true,
    "lastFailureAt": "2026-07-23T00:00:35Z",
    "lastErrorCode": "UPSTREAM_TIMEOUT",
    "nextRetryAt": "2026-07-23T00:05:35Z"
  },
  "dataAsOf": "2026-07-23T00:00:40Z",
  "version": 7
}

7.2 Retrieve attempts

GET /scheduled-jobs/{jobId}/attempts

7.3 Search jobs

GET /scheduled-jobs?status=OVERDUE
GET /scheduled-jobs?status=RETRY_PENDING
GET /scheduled-jobs?correlationId=...
GET /scheduled-jobs?scheduledFrom=...&scheduledTo=...

7.4 Missing job response

404 Not Found

{
  "code": "JOB_VISIBILITY_RECORD_NOT_FOUND",
  "message": "No job visibility record was found for the supplied job ID.",
  "meaning": "The absence of a visibility record does not prove that the external scheduler has no such job."
}

8. Simulation Components

8.1 Job Producer Simulator

Responsibilities:

Create jobs

Write JOB_CREATED

Write scheduler submission request EDR

Optionally simulate write failure

Optionally simulate duplicate submission

8.2 Scheduler Simulator

Supported modes:

NORMAL
ACK_DELAYED
ACK_DROPPED
ACK_DUPLICATED
EXECUTION_DELAYED
EXECUTION_DROPPED
EXECUTION_DUPLICATED
EXECUTION_FAILED
RETRY_ACCEPTED
RETRY_REJECTED
RETRY_DROPPED

8.3 EDR Transport Simulator

Supported faults:

NO_FAULT
DUPLICATE
DROP
DELAY
REORDER
CORRUPT_OPTIONAL_FIELD
BURST_DELIVERY

8.4 Visibility Projector

Responsibilities:

Deduplicate by eventId

Order by eventTime and lifecycle precedence

Preserve terminal states

Maintain current job summary

Maintain attempt summaries

Increment record version

Record last processed ingestion time

8.5 Reconciler

Responsibilities:

Detect overdue jobs

Detect stuck RUNNING jobs

Detect retry requested but not acknowledged

Detect expired retry windows

Detect inconsistent attempt counters

Emit reconciliation findings

9. Configuration

gracePeriodSeconds: 600
runningTimeoutSeconds: 1800
retryAcknowledgementTimeoutSeconds: 120
maxAttemptsDefault: 3
retryBackoffSeconds:
  - 60
  - 300
  - 900
deduplicationRetentionSeconds: 86400
eventualConsistencyTargetSeconds: 10

10. Happy-Path Scenarios

HP-01: Job created and scheduled successfully

Given:

A valid job request

Scheduler acknowledges submission

Scheduled time is in the future

Events:

JOB_CREATED
JOB_SCHEDULER_SUBMISSION_REQUESTED
JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED

Expected API state:

SCHEDULED

Acceptance criteria:

schedulerReference is present

scheduledAt is preserved

retry.completedAttempts = 0

dataAsOf is present

HP-02: Job executes successfully

Events:

JOB_CREATED
JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED
JOB_EXECUTION_STARTED
JOB_EXECUTION_SUCCEEDED

Expected API state:

SUCCEEDED

Acceptance criteria:

startedAt is populated

completedAt is populated

attempt count is 1

completedAt >= startedAt

terminal state remains stable

HP-03: Job fails and retry is scheduled

Events:

JOB_EXECUTION_STARTED attempt=1
JOB_EXECUTION_FAILED attempt=1 retryable=true
JOB_RETRY_REQUESTED nextAttempt=2
JOB_RETRY_ACKNOWLEDGED nextAttempt=2

Expected API state:

RETRY_SCHEDULED

Acceptance criteria:

completedAttempts = 1

nextAttemptNumber = 2

nextRetryAt is populated

schedulerAcknowledged = true

HP-04: Retry succeeds

Events:

JOB_EXECUTION_FAILED attempt=1
JOB_RETRY_ACKNOWLEDGED nextAttempt=2
JOB_EXECUTION_STARTED attempt=2
JOB_EXECUTION_SUCCEEDED attempt=2

Expected API state:

SUCCEEDED

Acceptance criteria:

completedAttempts = 2

attempt 1 shows FAILED

attempt 2 shows SUCCEEDED

final result is SUCCESS

HP-05: Retry exhausted

Events:

JOB_EXECUTION_FAILED attempt=1 retryable=true
JOB_EXECUTION_FAILED attempt=2 retryable=true
JOB_EXECUTION_FAILED attempt=3 retryable=false
JOB_RETRIES_EXHAUSTED

Expected API state:

RETRIES_EXHAUSTED

Acceptance criteria:

completedAttempts = 3

nextRetryAt is null

eligible = false

terminal state remains stable

11. Chaos Scenarios

CH-01: Duplicate JOB_CREATED

Fault:

Same eventId delivered twice

Expected:

Only one logical event is applied

Version is not incremented twice for the duplicate

Job remains CREATED

CH-02: Semantically duplicate execution with different event IDs

Fault:

Two JOB_EXECUTION_SUCCEEDED events for the same job and attempt

Different eventId values

Expected:

Final status remains SUCCEEDED

Attempt count does not become 2

Duplicate outcome is recorded as duplicate or ignored

CH-03: Out-of-order STARTED after SUCCEEDED

Delivery order:

JOB_EXECUTION_SUCCEEDED
JOB_EXECUTION_STARTED

Expected:

Final state remains SUCCEEDED

startedAt may be backfilled if missing

terminal state must not regress to RUNNING

CH-04: Scheduler acknowledgement missing

Events:

JOB_CREATED
JOB_SCHEDULER_SUBMISSION_REQUESTED

No acknowledgement arrives before timeout.

Expected:

PENDING_SUBMISSION

Reconciliation finding:

SCHEDULER_ACK_TIMEOUT

The API must not return SCHEDULED.

CH-05: Scheduler acknowledged but execution EDR missing

Events:

JOB_SCHEDULER_SUBMISSION_ACKNOWLEDGED

The scheduled time plus grace period passes.

Expected:

OVERDUE

The API must state that no execution start was observed.

CH-06: Execution delayed but still within grace period

Given:

scheduledAt has passed

grace period has not expired

no execution start EDR

Expected:

AWAITING_EXECUTION

CH-07: Retry requested but scheduler acknowledgement missing

Events:

JOB_EXECUTION_FAILED retryable=true
JOB_RETRY_REQUESTED

Expected:

RETRY_PENDING

Retry response:

{
  "requested": true,
  "schedulerAcknowledged": false,
  "nextRetryAt": null
}

CH-08: Retry acknowledgement arrives late

Events:

JOB_RETRY_REQUESTED

Timeout passes and reconciliation marks retry acknowledgement timeout.

Later event:

JOB_RETRY_ACKNOWLEDGED

Expected:

State transitions from RETRY_PENDING to RETRY_SCHEDULED

Previous timeout finding remains in audit history

No duplicate retry attempt is created

CH-09: Retry executes before retry acknowledgement EDR

Delivery order:

JOB_EXECUTION_STARTED attempt=2
JOB_RETRY_ACKNOWLEDGED nextAttempt=2

Expected:

State becomes RUNNING when attempt 2 starts

Late acknowledgement must not move state back to RETRY_SCHEDULED

CH-10: EDR delivery delay

Fault:

EDR eventTime is correct

ingestionTime is 10 minutes later

Expected:

State is calculated using lifecycle semantics

dataAsOf reflects ingestion freshness

processing delay is observable

CH-11: EDR dropped permanently

Fault:

JOB_EXECUTION_STARTED is dropped

JOB_EXECUTION_SUCCEEDED is delivered

Expected:

Final state is SUCCEEDED

startedAt may remain null

API should expose incompleteLifecycle = true

CH-12: Invalid attempt number

Events:

JOB_EXECUTION_FAILED attempt=2

No attempt 1 exists.

Expected:

Event is retained

Projection marks lifecycle inconsistency

completedAttempts must not be silently inferred as 2 without policy

reconciliation emits ATTEMPT_SEQUENCE_GAP

CH-13: Concurrent job updates

Fault:

Two projector instances update the same visibility record

Expected:

Optimistic lock or conditional update rejects one writer

Failed writer reloads and reapplies safely

No event is lost

CH-14: Stale API replica

Fault:

API reads from a delayed replica

Expected:

dataAsOf exposes stale snapshot time

API does not claim strong consistency

client can detect stale data

CH-15: Job succeeds after being marked overdue

Events:

Job becomes OVERDUE

Later JOB_EXECUTION_STARTED

Later JOB_EXECUTION_SUCCEEDED

Expected:

Final state becomes SUCCEEDED

overdue duration is retained

execution delay is measurable

CH-16: Late failure after success

Delivery order:

JOB_EXECUTION_SUCCEEDED attempt=1
JOB_EXECUTION_FAILED attempt=1

Expected:

Final state remains SUCCEEDED

conflicting terminal event is flagged

reconciliation emits CONFLICTING_TERMINAL_OUTCOME

CH-17: Cancelled job executes later

Events:

JOB_CANCELLED
JOB_EXECUTION_STARTED
JOB_EXECUTION_SUCCEEDED

Expected:

Policy must be explicit

Recommended projection: final operational outcome = SUCCEEDED

cancellationViolation = true

reconciliation emits EXECUTED_AFTER_CANCEL

CH-18: Retry exhausted but another attempt starts

Events:

JOB_RETRIES_EXHAUSTED
JOB_EXECUTION_STARTED attempt=4

Expected:

Lifecycle violation is recorded

State may become RUNNING only if observed facts take precedence

retriesExhaustedViolation = true

reconciliation emits EXECUTION_AFTER_RETRY_EXHAUSTION

12. Mutable Summary EDR Behaviour

If one mutable summary EDR is used instead of fully immutable lifecycle EDRs, it must preserve:

jobId
status
createdAt
scheduledAt
startedAt
completedAt
lastUpdatedAt
attemptCount
nextAttemptNumber
maxAttempts
lastFailureAt
lastErrorCode
nextRetryAt
schedulerReference
version

Updates must use conditional version checks.

Example:

UPDATE job_visibility
SET status = :newStatus,
    completed_at = :completedAt,
    version = version + 1
WHERE job_id = :jobId
  AND version = :expectedVersion;

The simulation must verify:

No lost update

No timestamp deletion

No terminal-state regression

No attempt-count inflation

No retry acknowledgement assumption

13. Assertions

Each scenario must assert:

API status
recorded status
job version
attempt count
retry summary
scheduler acknowledgement state
scheduledAt preservation
startedAt preservation
completedAt preservation
dataAsOf presence
reconciliation findings
terminal-state stability
idempotency

14. Test Data

Default job

{
  "jobId": "job-123",
  "correlationId": "subscription-456:RENEW:2026-07-23",
  "jobType": "RENEW_SUBSCRIPTION",
  "createdAt": "2026-07-22T10:00:00Z",
  "scheduledAt": "2026-07-23T00:00:00Z",
  "maxAttempts": 3
}

Default retry policy

{
  "maxAttempts": 3,
  "backoffSeconds": [60, 300, 900],
  "retryableErrors": [
    "UPSTREAM_TIMEOUT",
    "TEMPORARY_UNAVAILABLE",
    "RATE_LIMITED"
  ],
  "nonRetryableErrors": [
    "INVALID_PAYLOAD",
    "UNAUTHORIZED",
    "BUSINESS_RULE_REJECTED"
  ]
}

15. Simulation Execution Format

Each simulation run should produce:

{
  "scenarioId": "CH-03",
  "scenarioName": "Out-of-order STARTED after SUCCEEDED",
  "inputEvents": [],
  "deliveryOrder": [],
  "faultsInjected": [],
  "finalVisibilityRecord": {},
  "apiResponse": {},
  "reconciliationFindings": [],
  "assertions": [
    {
      "name": "terminal state does not regress",
      "passed": true
    }
  ],
  "result": "PASS"
}

16. Success Criteria

The implementation is acceptable when:

All happy-path scenarios pass.

Duplicate events never create duplicate attempts.

Out-of-order events never regress terminal state.

Missing scheduler acknowledgement is not represented as scheduled.

Missing execution evidence becomes awaiting or overdue based on time.

Retry requested and retry acknowledged remain distinguishable.

Late events can repair incomplete state without corrupting history.

Every API response exposes freshness.

Reconciliation identifies stuck, missing, conflicting, and invalid states.

Concurrent projection updates do not lose events.

17. Recommended Minimum Scenario Set for CI

Run on every build:

HP-01
HP-02
HP-03
HP-04
CH-01
CH-03
CH-04
CH-05
CH-07
CH-09
CH-13
CH-15
CH-16

Run the full chaos suite nightly or before production release.
