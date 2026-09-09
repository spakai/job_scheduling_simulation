# Chaos scenarios and architectural recovery

This document explains why the Spec 002 chaos scenarios exist, what can break, and how the
durable architecture contains each failure. It also records defects discovered while running
the local stack, because those defects are useful evidence: the tests changed implementation
assumptions before they became production incidents.

## Spec 007 worker recovery

The scenarios below describe the PostgreSQL/Connect visibility path. The database-free
Vert.x worker has a separate [runbook](spec-007-runbook.md) and
[evidence report](spec-007-evidence.md). Its recovery authority is the Kafka execution ledger
and broker-committed source offset.

Use `scripts/spec007 test` for broker-backed EDR/prefix failure, lease, fencing, execution
loss and isolation tests. After building and starting the five-worker baseline,
`scripts/spec007 chaos` publishes 100 synthetic requests, kills subscriber slot 0 and
verifies all results through `read_committed`; it requires the repository Python virtualenv.

The recorded container-kill run recovered all 100 results in 244.49 seconds, including a
wait for a live 240-second attempt lease. This is expected lease reconciliation. It does
not prove exactly-once arbitrary external effects. Multi-broker/network and production
dependency chaos remain acceptance gates.

## Spec 007 chaos plan

This plan validates the Kafka-first recovery boundaries of the Vert.x pull worker. The
worker's durable authorities are the broker-committed source offset and the workload-scoped
Kafka execution ledger. A local file, in-memory lane, HTTP response, or health endpoint is
not evidence that a job was safely completed.

### Scope and test topology

Run the scenarios against a disposable environment with:

- one subscriber topic with 10 partitions and at least two consumer pods;
- one isolated group workload and, where relevant, one retry deployment;
- `enable.auto.commit=false` and `isolation.level=read_committed`;
- a controllable business dependency that delays, fails, drops responses, and records
  `jobId` idempotency keys without applying the same key twice;
- broker, consumer-group, transaction, worker, and dependency telemetry retained for the
  complete recovery window; and
- unique topic, group, transactional-ID, and state-volume names for every run.

Use a small two-pod profile for fast checks and the five-pod/ten-partition profile for
capacity and rebalance evidence. Do not treat the synthetic handler used by local smoke
tests as proof of external side-effect safety.

### Scenario matrix

| ID | Fault injection | Expected containment and recovery | Required evidence |
| --- | --- | --- | --- |
| `VTX-CHAOS-01` | Kill one worker while it owns active records. | Kafka reassigns its partitions; the replacement fences the old slot, restores the ledger, and becomes ready. Uncommitted records redeliver. | Assignment/revocation timeline, old/new member IDs, broker offsets, ledger restore end, all expected outcomes. |
| `VTX-CHAOS-02` | Kill a worker after the external call but before `JOB_COMPLETED`. | The call may repeat; the same `jobId` prevents a second physical effect. The source offset advances only after a durable completion or disposition. | Dependency idempotency log, attempt EDRs, source offset, output count, duplicate-effect count. |
| `VTX-CHAOS-03` | Kill a worker after `JOB_COMPLETED` but before the prefix transaction. | The replacement reconstructs the completed outcome from the ledger and commits outputs without invoking the dependency again. | Ledger event/state, no second handler call, output records, committed next offset. |
| `VTX-CHAOS-04` | Revoke a partition while its handler is delayed. | The old lane epoch is invalidated; late completion cannot write outputs or offsets. The new owner restores and redelivers from the authoritative offset. | Revoke/assign timestamps, epoch rejection, producer fencing result, redelivery and final outcome. |
| `VTX-CHAOS-05` | Force a transaction timeout or broker failure during prefix commit. | The transaction aborts or is resolved by broker inspection; dispatch pauses and no offset is assumed committed from the client timeout. | Transaction state, broker offset before/after, read-committed outputs, recovery decision, no regressing commit. |
| `VTX-CHAOS-06` | Stop Kafka while records are fetched and while records are waiting to commit. | Existing durable work remains in Kafka; the worker pauses or fails closed, then resumes after broker recovery without committing past a gap. | Consumer error, pause/readiness transitions, lag, committed offsets, output and ledger counts. |
| `VTX-CHAOS-07` | Drop or delay consumer and producer traffic independently. | Bounded timeouts trigger recovery; no unbounded event-loop blocking or transaction queue growth occurs. | Network fault window, event-loop delay, queue depth, readiness, transaction and consumer metrics. |
| `VTX-CHAOS-08` | Make the dependency return timeouts, 429s, 503s, and slow responses. | Adaptive admission steps down to 1, 0.5, then 0 TPS; retries are bounded and reacquire capacity; exhausted work is handed to retry/DLQ transactionally. | Per-pod starts, adaptive state transitions, retry attempts, lease state, retry/DLQ records, no excess starts. |
| `VTX-CHAOS-09` | Exhaust per-partition and pod tracking windows with a slow earliest record. | Later owners may finish but remain behind the gap; the affected partition/pod pauses at high watermarks and resumes below low watermarks. | Window records/bytes, paused partitions, oldest age, committed offset unchanged across the gap. |
| `VTX-CHAOS-10` | Fill the transaction adapter queue or make Kafka output slow. | The bounded queue backpressures lanes; no unbounded memory growth or offset advancement occurs. | Queue depth, heap/container memory, pause reason, transaction latency and final prefix. |
| `VTX-CHAOS-11` | Exhaust the worker state volume during ledger restore or EDR writes. | Readiness stays false or the runtime is replaced; Kafka remains the recovery authority. Deleting local state does not lose completed work. | Disk usage, failure reason, replacement lifecycle, restore from Kafka, unchanged ledger authority. |
| `VTX-CHAOS-12` | Restart the worker during ledger restoration. | The partial local restore is discarded or rebuilt safely; the partition is not advertised ready until its captured end is restored. | Restore start/end offsets, readiness probe history, assignment epoch, no pre-restore dispatch. |
| `VTX-CHAOS-13` | Start a second process with the same stable worker slot. | Kafka transactional IDs fence the predecessor or reject the duplicate; two live slot owners never commit concurrently. | Transaction fencing error, group membership, producer IDs, exactly one accepted writer. |
| `VTX-CHAOS-14` | Start subscriber and group traffic together, then fail one dependency. | Separate topics, groups, pods, limiters, ledgers, and retry paths isolate the workloads. One fleet's outage does not consume the other's capacity. | Group membership, topic lag, per-fleet starts, output/ledger separation and unaffected workload results. |
| `VTX-CHAOS-15` | Stop the retry deployment while quarantine records accumulate. | Original workers remain unaffected; retry offsets and handoffs remain durable. Restarting retry requeues valid records atomically and never invokes business logic itself. | Retry lag, handoff identity, original-topic key/metadata, source and retry offsets. |
| `VTX-CHAOS-16` | Gracefully terminate a pod during active work. | Readiness falls first, partitions pause, the bounded drain runs, unfinished work remains uncommitted, and the process exits within termination grace. | Probe transitions, drain duration, termination exit, offsets before/after, redelivery results. |

### Execution order

Run the scenarios in increasing blast radius:

1. Execute `VTX-CHAOS-01` through `VTX-CHAOS-04` with one partition and deterministic
  handler delays to establish ownership, epoch, and deduplication behavior.
2. Execute `VTX-CHAOS-05` through `VTX-CHAOS-07` with broker and network faults. Repeat each
  case at least three times because a timeout may resolve as either committed or aborted.
3. Execute `VTX-CHAOS-08` through `VTX-CHAOS-12` with slow, failing, and resource-limited
  dependencies. Record both the first failure and the recovery plateau.
4. Execute `VTX-CHAOS-13` through `VTX-CHAOS-16` against the multi-pod baseline, including
  rolling replacement and workload-isolation checks.
5. Repeat the relevant cases under representative 60-, 120-, and 180-second handler
  durations. Short synthetic calls validate control flow only; they do not validate lease
  expiry or capacity behavior.

Each run must use a unique run ID in topics, keys, logs, and evidence filenames. Before
injecting a fault, capture the assigned partitions, committed offsets, ledger end offsets,
active attempt IDs, and baseline dependency-effect count. After recovery, wait for the
consumer group to stabilize, read outputs with `read_committed`, and compare those values.

### Pass criteria

A scenario passes only when all of the following are true:

- every expected logical request reaches exactly one terminal Kafka disposition;
- no source offset advances beyond an unfinished or unresolved prefix;
- stale assignment epochs and fenced producers cannot publish accepted outputs;
- completed ledger entries suppress duplicate business calls within retention;
- retry and DLQ handoffs preserve workload, owner key, source coordinates, and generation;
- liveness remains available while recovery is active, while readiness accurately reports
  restoration or sustained failure; and
- memory, event-loop delay, transaction queues, and tracking windows return to bounded
  steady state after the fault is removed.

An external side effect is considered safe only when the dependency's idempotency log proves
that repeated delivery of one `jobId` caused one physical effect. Kafka output counts alone
cannot establish that property.

### Evidence record

Store one machine-readable record and a short operator narrative per scenario. The record
must include the run ID, image/build identifier, configuration, topic and group names,
fault start/end, pod and slot identities, assignment history, committed offsets, ledger
restore bounds, transaction IDs/states, readiness/liveness samples, relevant metrics,
expected/actual logical outcomes, dependency-effect counts, and the final verdict.

Link the record from the Spec 007 evidence report. A green local container-kill run covers
`VTX-CHAOS-01` only in the local single-broker scope; it does not close `VTX-CHAOS-02`,
`VTX-CHAOS-05`, multi-broker loss, network isolation, disk pressure, or production dependency
acceptance gates.

## How to read the system during a failure

The pipeline contains four independently observable queues:

```text
due scheduler jobs
    -> unpublished scheduler_outbox rows
    -> Kafka / Kafka Connect lag and DLQ
    -> unprojected edr_events rows
```

Cassandra is a worker dependency beside this pipeline. It may cause an execution attempt to
fail, but it is never used as scheduler or visibility authority.

The most important diagnostic rule is to identify the last durable fact:

- A committed scheduler mutation and outbox row means the scheduler fact is safe even if
  Kafka is unavailable.
- A broker-acknowledged record is safe for Connect to replay even if the EDR database is
  unavailable.
- An `edr_events` row is immutable evidence even if projection is stopped or broken.
- A `projected_events` row means one EDR was transactionally applied to visibility state.
- A Cassandra `last_operation_id` or operation marker means the workload effect happened,
  even if the worker did not receive the response.

## Failures discovered while bringing up the stack

### Toxiproxy setup assumed a shell

What broke: the original setup container tried to execute `/bin/sh` inside the pinned
Toxiproxy image. That image is distroless, so container creation failed before a proxy could
be configured.

Why this mattered: without the proxy, a test could only simulate exceptions in Python. It
could not prove how the real Cassandra driver behaves when packets are delayed or dropped.

Resolution: setup now uses a pinned curl image and the Toxiproxy HTTP API. It first inspects
the named proxy and creates it only when absent, making repeated `docker compose up` calls
safe. A downstream timeout then produced a real driver `NoHostAvailable` result, and reads
recovered after the toxic was removed.

### Cassandra authentication environment variables were ineffective

What broke: setting `CASSANDRA_AUTHENTICATOR` and `CASSANDRA_AUTHORIZER` in Compose did not
change the official image's `cassandra.yaml`. Cassandra started with `AllowAllAuthenticator`
and role creation failed.

Why this mattered: a green container health check would have hidden the fact that worker and
seed identities were not isolated.

Resolution: a small entrypoint edits the pinned container configuration before delegating to
the official entrypoint. Setup creates separate `worker` and `seed_manager` roles. Live tests
confirmed that the worker can read and perform approved mutations but cannot drop the seeded
dataset table.

### Conditional batches cannot span Cassandra tables

What broke: the first client attempted a conditional batch that updated
`records_by_bucket` and inserted into `update_operations_by_bucket`. Cassandra correctly
rejected it: conditional batches cannot span multiple tables.

Why this mattered: retrying that invalid query would never recover, and replacing it with a
blind update could increment the checksum twice after an unknown write outcome.

Resolution: the client now uses a reservation protocol:

1. Read the row and check `last_operation_id`.
2. Conditionally reserve `pending_operation_id` against the observed checksum.
3. Conditionally finalize only for that pending operation and checksum.
4. Insert or repair the idempotent operation marker.
5. On a lost response, reread `last_operation_id` before deciding to retry.

The logical operation ID is derived from the job contract and is stable across scheduler
attempts. Replaying the same operation returned success without another checksum increment.

### Kafka producer and Connect initially disagreed on the wire format

What broke: the publisher initially sent plain JSON while Kafka Connect used
`JsonSchemaConverter`, which expects Schema Registry framing.

Why this mattered: Kafka acknowledged the records, but every record failed conversion and
went to the DLQ instead of the EDR database.

Resolution: the producer fetches the registered subject, serializes with the Confluent JSON
Schema serializer, enables Kafka idempotence, and waits for broker acknowledgement before
marking an outbox record published. Schema Registry dependencies are installed through the
`confluent-kafka[json]` extra.

### `schemaVersion` was underspecified for Kafka Connect

What broke: JSON Schema declared `schemaVersion` with `const: 1` but no explicit integer
type. Validation accepted the value, but Connect inferred an incompatible field while its
metadata transform rebuilt the record.

Resolution: the schema now declares both `type: integer` and `const: 1`. Fresh records passed
through metadata enrichment and JDBC persistence. The rejected diagnostic records remained
in the DLQ as evidence rather than being silently discarded.

### JDBC retries could temporarily pin an identity collision

What broke: a different payload using an existing `eventId` correctly activated the
immutability trigger, but JDBC treated the database exception as retryable before applying
the connector's error policy.

Resolution: connector retries and backoff are explicitly bounded. After those attempts, the
record is sent to the DLQ and later valid records can continue. The first journal row remains
unchanged.

The immutable trigger separately recognizes an identical `eventId`, payload hash, and
canonical payload as a no-op. This is required because a publisher can crash after Kafka
acknowledgement but before committing `published_at`, causing the same fact to arrive with a
different Kafka offset.

### Connector registration had a startup race and host dependency

What broke: connector creation succeeded, but an immediate status request sometimes returned
404 while Connect propagated the new configuration. The installer also assumed a `python`
binary that was not present on every host.

Resolution: the installer uses configurable `python3` by default and retries status lookup.
Connector application is now repeatable and verifies the resulting task state.

## Runtime chaos scenarios

### Process and container restart (`PERSIST-01`)

Failure: API, publisher, Connect, projector, or a database process restarts.

Containment:

- Scheduler jobs and attempts are PostgreSQL rows, not process memory.
- Publication intent remains in `scheduler_outbox`.
- Kafka retains acknowledged records and Connect offsets.
- Raw EDRs and projections are separate durable PostgreSQL tables.
- Projection checkpoints make replay idempotent.

Verified live: PostgreSQL and Kafka Connect were restarted. The connector task returned to
`RUNNING`, raw EDRs remained present, and the durable API still returned `SUCCEEDED`.

### Concurrent pollers (`PERSIST-02`)

Failure: two pollers see the same due queue at the same time.

Containment: `FOR UPDATE SKIP LOCKED` selects bounded ordered batches inside transactions.
The claim update, unique attempt row, opaque fencing token, and retrieval EDR outbox row
commit together.

Verified live: two pollers claimed 20 jobs in non-overlapping batches. A deliberately altered
fencing token was rejected when it attempted to start work.

### Cassandra request timeout followed by recovery (`WORKER-02`)

Failure: the network exceeds the driver's request deadline.

Containment: the handler classifies dependency timeouts as retryable. Scheduler failure,
future `available_at`, cleared claim, and failure/retry EDRs commit atomically. The row cannot
be reclaimed before its retry time. Stable selection seed and operation ID make the later
attempt comparable to the first.

Verified live: a Toxiproxy downstream timeout caused a real driver connection failure;
removing it restored bounded reads. The full attempt-1-fails/attempt-2-succeeds scenario is
defined but should still be promoted into a repeatable integration test.

### Sustained Cassandra outage (`WORKER-03`)

Failure: every execution attempt encounters an unavailable dependency.

Containment: scheduler attempts, not unbounded driver retries, are the visible retry unit.
Once `attempt_number == max_attempts`, the job becomes `RETRIES_EXHAUSTED`, loses its claim,
and has no future eligible attempt.

Verified live: an injected retryable handler ran exactly three attempts, created three
attempt rows and one exhaustion EDR, and ended in `RETRIES_EXHAUSTED`. Repeating this with the
real Toxiproxy cut for every attempt remains an automation improvement.

### Late Cassandra reply after claim loss (`WORKER-04`)

Failure: a worker completes after its lease expires and another worker owns the job.

Containment: start, heartbeat, success, and failure updates must match job ID, attempt,
worker ID, fencing token, and an unexpired lease. A stale worker cannot overwrite the new
owner. Cassandra effects are reconciled independently by stable operation identity.

Verified live: a stale token was rejected. Lease-expiry during an actual delayed Cassandra
response remains a targeted integration-test improvement.

### Unknown Cassandra update outcome (`WORKER-05`)

Failure: Cassandra applies the update but the response is lost.

Containment: reservation and finalization are fenced by checksum and operation ID. Before
retrying, the client rereads `last_operation_id`; if it matches, the effect is successful and
the marker can be repaired. No counter column or blind increment is used.

Verified live: deterministic response loss immediately after Cassandra finalization was
reconciled as success. Replaying the logical operation retained the same operation ID and
checksum, proving one increment.

### Concurrent checksum conflict (`WORKER-06`)

Failure: two jobs select one maximum record using the same observed checksum.

Containment: only one conditional reservation wins. The loser returns a retryable conflict
instead of incrementing blindly. A later attempt rereads and recomputes against the new
checksum.

Verified live: two Cassandra drivers were synchronized after observing the same checksum.
Exactly one reservation won; the loser retried against the new checksum, and each logical
operation incremented once.

### Publisher crash or Kafka outage (`KAFKA-01`, `KAFKA-02`)

Failure: Kafka is unavailable, or the publisher dies after broker acknowledgement but before
updating the outbox row.

Containment:

- Scheduler transactions do not depend on Kafka availability.
- Unpublished rows remain durable and observable.
- Publication is never marked complete before acknowledgement.
- Leases expire after publisher crashes.
- Retries use bounded exponential backoff with jitter.
- Duplicate delivery is a no-op at the immutable EDR journal and projection checkpoint.

Verified live: identical republishing produced one journal row. During a real broker stop,
scheduler facts accumulated in the durable outbox. After Kafka restarted, the outbox drained
and Connect persisted the buffered EDRs within the bounded recovery window.

### EDR database outage (`SINK-01`)

Failure: Connect cannot reach the EDR database.

Containment: Kafka remains the durable buffer. Connect retries and resumes from committed
offsets after PostgreSQL recovers. The visibility API never substitutes scheduler state for
missing EDR evidence; `dataAsOf` exposes stale projection data.

Verified live: the independently deployed EDR PostgreSQL container was stopped while the
scheduler database remained reachable. Kafka buffered a valid EDR and Connect persisted it
after EDR PostgreSQL recovered.

### Poison record and identity collision (`SINK-02`, `SINK-03`)

Failure: a record cannot be converted or violates journal immutability.

Containment: converter/database retries are bounded, errors omit message bodies, and the DLQ
retains diagnostic context. An event ID collision cannot mutate the first row.

Verified live: an unframed poison record reached the DLQ and a subsequent valid record
persisted. A deliberate different-payload collision also reached the DLQ, the original event
remained `JOB_CREATED`, and the following valid record progressed. These are automated by
`test_poison_record_reaches_dlq_and_subsequent_record_progresses` and
`test_event_identity_collision_is_immutable_and_partition_continues`.

### Kafka Connect restart with buffered input (`PERSIST-01`)

Failure: Kafka Connect stops before consuming a broker-acknowledged EDR.

Containment: Kafka retains the record independently of the Connect worker. Connect stores
consumer offsets in Kafka and resumes from its last committed offset after restart; journal
upsert and immutability rules make replay safe.

Verified live: a unique EDR was published while the Connect container was stopped. It was
absent from `edr_events` during the outage and persisted after Connect returned healthy. This
is automated by `test_connect_restart_replays_buffered_record`.

### Projector crash, ordering, and rebuild (`PROJ-01`–`PROJ-03`)

Failure: projection stops before commit, receives late facts, or must be reconstructed.

Containment: the journal is immutable authority. Projection state and its event checkpoint
commit in one EDR-database transaction. The reducer preserves terminal precedence and
backfills timestamps. Rebuild reads only `edr_events`.

Verified live: all 81 raw EDRs had projection checkpoints, and journal-only rebuild reproduced
the tested `SUCCEEDED` state. Deterministic tests cover duplicate, late, and conflicting
lifecycle facts.

### Database ownership boundary (`ISOLATION-01`)

Failure: one logical database is unavailable or credentials are accidentally reused.

Containment: databases, roles, URLs, pools, migrations, and version tables are separate.
There are no cross-database foreign keys or transactions. Visibility never treats scheduler
rows as evidence.

Verified live: `scheduler_owner` was denied connection to EDR, `edr_owner` was denied
connection to scheduler, the Cassandra worker could not manage schema, and a differing
`edr_sink` update was rejected by the immutable trigger.

## Current gaps

### Spec 005 resource pressure and application failpoints

Spec 005 adds a no-op-by-default fault injector at scheduler commit, worker claim/completion,
publisher acknowledgement, projector apply, and visibility query boundaries. `APP-01` through
`APP-04` and the `NET-01`/`NET-03` PostgreSQL degradation scenarios are automated. Scoped
CPU, memory, and disposable disk controls are available only for `job-visibility-chaos-*`
projects.

The remaining evidence gap is not the ability to create pressure: it is proving the complete
OOM, CPU, bandwidth, and disk hypothesis/recovery matrices under bounded nightly workloads.
See [`spec-005-evidence.md`](spec-005-evidence.md).

Spec 003 tracks production hardening and evidence. The implementation status is:

| Item | Status |
| --- | --- |
| Configurable PostgreSQL connect, statement, transaction, and lock timeouts | Implemented; live timeout tests added. |
| Configurable Kafka request, socket, metadata, delivery, and flush timeouts | Implemented; broker outage/backlog/drain passes live. |
| Automated live procedures | PostgreSQL, Cassandra, publisher, projector, Kafka path, broker outage, sink outage, poison/DLQ continuation, Connect restart, and isolation suites pass; the rest of the restart matrix remains. |
| Loss after Cassandra finalization | Deterministic post-finalize injection passes against Cassandra. |
| Concurrent two-worker checksum race | Same-checksum two-driver race passes against Cassandra. |
| Representative load with p95/p99 evidence | Workload profile proposed; durable runner and results remain. |
| Independent scheduler and EDR database outages | Separate containers and automated stop/recovery test pass in both directions. |

See [`spec-003-evidence.md`](spec-003-evidence.md) for exact test names and the evidence that
must still be produced. These are explicit evidence gaps, not reasons to infer state from
another subsystem or relax the durability boundaries.

## Running the scenarios locally

Run `./try.sh` to bootstrap the isolated `job-visibility-resilience` Compose project and
execute the poison-record, identity-collision, and Connect-restart scenarios. Run
`./try.sh full` for the complete infrastructure resilience suite. Both modes use bounded
test waits and leave the stack running for diagnostics; use `scripts/infra down` when it is
no longer needed.
