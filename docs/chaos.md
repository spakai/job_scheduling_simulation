# Chaos scenarios and architectural recovery

This document explains why the Spec 002 chaos scenarios exist, what can break, and how the
durable architecture contains each failure. It also records defects discovered while running
the local stack, because those defects are useful evidence: the tests changed implementation
assumptions before they became production incidents.

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

Verified live: invoking the same logical operation twice retained the same operation ID and
checksum. Packet-level loss specifically after the final write remains to be automated.

### Concurrent checksum conflict (`WORKER-06`)

Failure: two jobs select one maximum record using the same observed checksum.

Containment: only one conditional reservation wins. The loser returns a retryable conflict
instead of incrementing blindly. A later attempt rereads and recomputes against the new
checksum.

Status: implemented by the conditional reservation protocol; a two-worker live race should
be added to the integration suite.

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

Verified live: identical republishing produced one journal row. A real broker-stop/backlog/
recovery timing test is still needed, along with configurable producer delivery and socket
timeouts.

### EDR database outage (`SINK-01`)

Failure: Connect cannot reach the EDR database.

Containment: Kafka remains the durable buffer. Connect retries and resumes from committed
offsets after PostgreSQL recovers. The visibility API never substitutes scheduler state for
missing EDR evidence; `dataAsOf` exposes stale projection data.

Verified live: PostgreSQL and Connect restart recovery preserved the journal and projection.
A timed EDR-only outage is constrained locally because both logical databases share one
PostgreSQL container.

### Poison record and identity collision (`SINK-02`, `SINK-03`)

Failure: a record cannot be converted or violates journal immutability.

Containment: converter/database retries are bounded, errors omit message bodies, and the DLQ
retains diagnostic context. An event ID collision cannot mutate the first row.

Verified live: an early incompatible schema and a deliberate different-payload collision
reached the DLQ. The original event remained `JOB_CREATED`.

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

Spec 003 tracks production hardening and evidence. The implementation status is:

| Item | Status |
| --- | --- |
| Configurable PostgreSQL connect, statement, transaction, and lock timeouts | Implemented; live timeout tests added. |
| Configurable Kafka request, socket, metadata, delivery, and flush timeouts | Implemented; real broker-outage automation remains. |
| Automated live procedures | Initial PostgreSQL, Cassandra, publisher, projector, and Kafka-path suites added; the complete matrix remains. |
| Loss after Cassandra finalization | Deterministic post-finalize injection and real-infrastructure test added; CI execution remains. |
| Concurrent two-worker checksum race | Same-checksum barrier and real-infrastructure test added; CI execution remains. |
| Representative load with p95/p99 evidence | Workload profile proposed; durable runner and results remain. |
| Independent scheduler and EDR database outages | Separate containers implemented; automated container-stop matrix remains. |

See [`spec-003-evidence.md`](spec-003-evidence.md) for exact test names and the evidence that
must still be produced. These are explicit evidence gaps, not reasons to infer state from
another subsystem or relax the durability boundaries.
