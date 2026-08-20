# Job Scheduling Simulation

A deterministic Python simulation for scheduled-job visibility when an external scheduler polls once per minute and retrieves at most `X` jobs per poll.

The model keeps submission acknowledgement, scheduler retrieval, execution start, and terminal outcome as separate observed facts. It also measures polling delay, batch backlog, worker delay, EDR freshness, retries, and lifecycle inconsistencies.

## Run

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/job-visibility-sim --pretty --output simulation-results/full.json
```

Run a single scenario or the CI subset:

```bash
.venv/bin/job-visibility-sim POLL-04 --pretty
.venv/bin/job-visibility-sim ci --output simulation-results/ci.json
```

Start the visibility API:

```bash
.venv/bin/uvicorn job_visibility.api:app --reload
```

The visibility specification and implementation plan are in
[`specs/001-scheduled-job-visibility`](specs/001-scheduled-job-visibility/). The durable
PostgreSQL and Kafka architecture is specified in
[`specs/002-real-persistence-kafka`](specs/002-real-persistence-kafka/).
The arc42-aligned C4 architecture documentation is in [`architecture.md`](architecture.md).
The latest human-readable run report is in
[`simulation-results/summary.md`](simulation-results/summary.md).

## Spec 002 local infrastructure

The durable stack uses separate `scheduler` and `edr` PostgreSQL databases, Kafka in KRaft
mode, Schema Registry, Kafka Connect, Cassandra, and a Toxiproxy endpoint on port 9042.
Container tags and the JDBC connector version are pinned in `compose.yaml`.

```bash
docker compose up -d --build
docker compose ps --all

export SCHEDULER_DATABASE_URL='postgresql+psycopg://scheduler_owner:scheduler-local@localhost:5432/scheduler'
export EDR_DATABASE_URL='postgresql+psycopg://edr_owner:edr-local@localhost:5432/edr'
.venv/bin/alembic -n scheduler upgrade head
.venv/bin/alembic -n edr upgrade head
bash infra/kafka/connect/apply.sh
```

Run the durable background roles independently:

```bash
.venv/bin/job-visibility-runtime publisher
.venv/bin/job-visibility-runtime projector
.venv/bin/job-visibility-runtime rebuild --once
```

The publisher leases scheduler outbox rows and records Kafka acknowledgements. Kafka Connect
is the only writer to the immutable raw EDR journal. The projector reads that journal and
updates the durable visibility tables; rebuild derives projections from the EDR database
alone. Operational recovery guidance is in
[`docs/spec-002-runbook.md`](docs/spec-002-runbook.md).
The failure model, discovered defects, recovery mechanisms, verified scenarios, and remaining
chaos-test gaps are documented in [`docs/chaos.md`](docs/chaos.md).

Inspect migration and pipeline state with:

```bash
.venv/bin/alembic -n scheduler current
.venv/bin/alembic -n edr current
curl -fsS http://localhost:8083/connectors/edr-jdbc-sink-v1/status
docker compose exec cassandra cqlsh -u worker -p worker-local -e \
  'SELECT * FROM worker_demo.datasets'
```

The normal unit suite never starts containers. Infrastructure tests are opt-in through the
`integration`, `postgres`, `kafka`, `cassandra`, and `e2e` pytest markers. Stop the stack
with `docker compose down`; use `docker compose down --volumes` only when intentionally
discarding all local PostgreSQL, Kafka, and Cassandra data.

## EDR lifecycle taxonomy

Every canonical `eventType` is classified without requiring callers to duplicate metadata:

- `edrType`: `SCHEDULING` for scheduler-control rows or `ATTEMPT` for execution rows.
- `edrGroup`: `SCHEDULING`, `EXECUTION`, `RETRY`, or `TERMINAL`.
- `requirement`: intermediate/configurable rows are `OPTIONAL`; terminal outcome rows are
  `MANDATORY`.

`GET /edr-lifecycle` returns the complete mapping. `POST /edrs` returns the classification
applied to the accepted event, and serialized simulation input events include
`edr_type`, `edr_group`, and `edr_requirement`.

## Architecture assessment

The following assessment uses the four 1–10 criteria from section 7.7 of
[*Serverless Architectures on AWS*](https://learning.oreilly.com/library/view/serverless-architectures-on/9781617295423/OEBPS/Text/ch07.htm#sigil_toc_id_109):
precision, scalability by number of open tasks, hotspot scalability, and cost. Higher
scores are better.

These scores apply to the current in-memory simulation, not the stronger PostgreSQL
architecture proposed in the implementation plan. The scalability scores are provisional
until they are supported by load tests.

| Criterion | Score | Assessment |
| --- | ---: | --- |
| Precision | **5/10** | One-minute polling introduces 0–60 seconds of normal delay. The `X` batch limit and worker capacity can add further polling intervals during a backlog. |
| Scalability—open tasks | **3/10** | Jobs are held in memory, and eligible items are scanned and sorted by a single process. The current implementation is a simulator rather than durable production infrastructure. |
| Scalability—hotspots | **4/10** | Bounded batches protect the scheduler from immediate overload, but excess jobs become backlog. The global lock and single scheduler queue remain bottlenecks. |
| Cost | **9/10** | The simulator is inexpensive to run because it requires only a Python process. Some of this advantage comes from not yet providing production durability and availability. |
| **Total** | **21/40** | **5.25/10 average** |

For context, the chapter's unweighted scores are:

| Solution | Total |
| --- | ---: |
| Cron job | 25/40 |
| DynamoDB TTL | 27/40 |
| Step Functions | 23/40 |
| SQS | 24/40 |
| SQS + DynamoDB TTL | 32/40 |
| **Current simulator scheduler** | **21/40** |

### Precision and polling backlog

If `X=100` and 401 jobs are already eligible, the last batch cannot be retrieved until
approximately the fifth poll:

```text
polls required = ceil(401 / 100) = 5
```

With one-minute polling, some jobs therefore wait about four additional minutes even
when the scheduler operates exactly as designed. The visibility projection explains this
delay, but visibility alone cannot improve execution precision.

### Production target

Completing the persistence and horizontal-scaling work in the implementation plan should
make the following scores realistic:

| Criterion | Target |
| --- | ---: |
| Precision | **6–7** |
| Scalability—open tasks | **8** |
| Scalability—hotspots | **7–8** |
| Cost | **7** |
| **Total** | **28–30/40** |

Reaching that target requires:

- PostgreSQL persistence with an index on eligibility and schedule time.
- Atomic job claims using `FOR UPDATE SKIP LOCKED`.
- Multiple concurrent pollers.
- Queue partitioning or sharding for large hotspots.
- Separate worker queues and autoscaling.
- Backpressure and backlog-age metrics.
- Load tests with hundreds of thousands or millions of open jobs.
- A shorter polling interval or event/timer-based wake-up when sub-minute precision matters.

Visibility and explainability are the current implementation's strongest capabilities. It
distinguishes acknowledgement, retrieval, polling backlog, worker delay, missing EDRs,
retries, conflicts, and stale data. It observes and explains scheduler behavior; it does
not yet provide production-grade scheduling scalability.
