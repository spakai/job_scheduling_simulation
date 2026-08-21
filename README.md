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
Production hardening, automated chaos evidence, and release gates are defined in
[`specs/003-production-hardening-resilience`](specs/003-production-hardening-resilience/).
Production API composition and the standalone scheduler runtime are specified in
[`specs/004-production-api-runtime`](specs/004-production-api-runtime/).
Resource-pressure experiments and deterministic application fault injection are specified in
[`specs/005-resource-pressure-fault-injection`](specs/005-resource-pressure-fault-injection/).
The arc42-aligned C4 architecture documentation is in [`architecture.md`](architecture.md).
The latest human-readable run report is in
[`simulation-results/summary.md`](simulation-results/summary.md).

## Spec 002 local infrastructure

The durable stack uses physically separate scheduler and EDR PostgreSQL containers, Kafka in
KRaft mode, Schema Registry, Kafka Connect, Cassandra, and a Toxiproxy endpoint on port 9042.
Container tags and the JDBC connector version are pinned in `compose.yaml`.

```bash
scripts/infra bootstrap

export SCHEDULER_DATABASE_URL='postgresql+psycopg://scheduler_owner:scheduler-local@localhost:5432/scheduler'
export EDR_DATABASE_URL='postgresql+psycopg://edr_owner:edr-local@localhost:5433/edr'
.venv/bin/alembic -n scheduler upgrade head
.venv/bin/alembic -n edr upgrade head
bash infra/kafka/connect/apply.sh
```

The bounded infrastructure interface also provides `ready`, `migrate`, `connector-apply`,
`test-postgres`, `test-resilience`, `diagnostics`, and `down` commands. Destructive volume
cleanup requires `CONFIRM_DELETE_TEST_VOLUMES` to exactly match the named Compose project.

Run the durable background roles independently:

```bash
.venv/bin/job-visibility-runtime publisher
.venv/bin/job-visibility-runtime projector
.venv/bin/job-visibility-runtime rebuild --once
```

## Production-like HTTP path

Start the infrastructure, migrations/connector, and role-isolated application services:

```bash
scripts/infra bootstrap
scripts/infra up-apps
scripts/infra smoke-http
```

The scheduler API listens on port 8000 and owns only scheduler PostgreSQL access. The
visibility API listens on port 8001 and reads only EDR PostgreSQL projections. The apps
profile also starts the scheduler worker, outbox publisher, and projector.

Submit a job directly:

```bash
curl -X POST http://localhost:8000/scheduler/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "jobId":"example-fibonacci-1",
    "correlationId":"example-order-1",
    "jobType":"FIBONACCI",
    "scheduledAt":"2026-08-21T12:00:00Z",
    "payload":{"limit":10000},
    "maxAttempts":3
  }'
```

Read its EDR-derived projection and attempts independently:

```bash
curl http://localhost:8001/scheduled-jobs/example-fibonacci-1
curl http://localhost:8001/scheduled-jobs/example-fibonacci-1/attempts
curl 'http://localhost:8001/scheduled-jobs?correlationId=example-order-1'
```

For host-managed processes, use `job-visibility-api scheduler`,
`job-visibility-api visibility`, and `job-visibility-runtime scheduler`. The original
`uvicorn job_visibility.api:app` command remains the in-memory simulation API.

The publisher leases scheduler outbox rows and records Kafka acknowledgements. Kafka Connect
is the only writer to the immutable raw EDR journal. The projector reads that journal and
updates the durable visibility tables; rebuild derives projections from the EDR database
alone. Operational recovery guidance is in
[`docs/spec-002-runbook.md`](docs/spec-002-runbook.md).
The failure model, discovered defects, recovery mechanisms, verified scenarios, and remaining
chaos-test gaps are documented in [`docs/chaos.md`](docs/chaos.md).
The bounded resilience workflow and current automation evidence are documented in
[`docs/spec-003-runbook.md`](docs/spec-003-runbook.md) and
[`docs/spec-003-evidence.md`](docs/spec-003-evidence.md).
The production HTTP operating procedure is in
[`docs/spec-004-runbook.md`](docs/spec-004-runbook.md).
The bounded resource-pressure and application-fault procedure is in
[`docs/spec-005-runbook.md`](docs/spec-005-runbook.md).

List the Spec 005 experiment catalog and run a deterministic application-boundary scenario:

```bash
scripts/chaos list
scripts/chaos run APP-01
scripts/chaos run APP-02
```

Chaos mode is disabled by default and forbidden when `APP_ENVIRONMENT` is `production`.
Resource controls additionally require an isolated Compose project named
`job-visibility-chaos-*`.

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

The current Specs 001–004 implementation scores **86/100 (A−)** as a strong,
production-oriented prototype. The assessment covers the durable runtime and production-like
HTTP path, not only the original in-memory simulator.

| Area | Score | Assessment |
| --- | ---: | --- |
| Service boundaries | **9/10** | Scheduler command handling and EDR visibility queries are clearly separated. |
| Durability and recovery | **9/10** | PostgreSQL queues, a transactional outbox, Kafka, immutable EDR records, retries, and restart tests provide a strong reliability model. |
| API design | **8.5/10** | Separate scheduling and visibility APIs expose realistic external contracts with idempotency and conflict behavior. |
| Data ownership | **9/10** | Separate databases and a read-only EDR API account enforce ownership boundaries. |
| Event-driven design | **9/10** | Transactional outbox publishing and durable projection avoid unsafe database/event dual writes. |
| Observability | **7/10** | Health endpoints and evidence collection exist; production metrics, tracing, alerting, and correlation tooling remain limited. |
| Security | **7/10** | Database least privilege is present, but API authentication, authorization, TLS, secret management, and audit controls remain. |
| Scalability | **8/10** | Roles can scale independently; partitioning, backpressure limits, leader coordination, and capacity evidence need strengthening. |
| Operability | **8.5/10** | Compose profiles, migrations, smoke tests, runbooks, CI, and chaos coverage support repeatable operation. |
| Documentation | **9/10** | Specifications, plans, runbooks, evidence, and architecture flows closely match the implementation. |

The deployed responsibility flow is:

```text
client -> scheduler API -> durable queue -> scheduler worker -> transactional outbox
       -> Kafka -> immutable EDR/projection -> visibility API
```

The architecture is sound for a portfolio system and production-oriented prototype. Before a
real production release, it still needs:

- API authentication, tenant authorization, TLS, managed secrets, and audit controls.
- Production metrics, distributed tracing, SLOs, alerting, and correlation dashboards.
- A Kubernetes or cloud deployment model with autoscaling and graceful-rollout evidence.
- Explicit Kafka partitioning and ordering guarantees keyed by `jobId`.
- Rate limits, pagination limits, quotas, API versioning, and backpressure policies.
- Retention, archival, disaster-recovery, and database backup/restore testing.
- Load and capacity tests that prove behavior at the intended scale.
- A final decision on Cassandra's long-term role in the visibility architecture.

Addressing those gaps through a dedicated security, observability, and deployment-hardening
specification would provide a credible path beyond **90/100**.
