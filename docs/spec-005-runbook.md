# Spec 005 resource-pressure and fault-injection runbook

## Safety boundary

Application faults are disabled unless `CHAOS_MODE=enabled`, a finite experiment ID and
validated JSON rule list are supplied, and `APP_ENVIRONMENT` is not `production`. There is
no HTTP activation endpoint.

Container pressure commands additionally reject every Compose project that does not match
`job-visibility-chaos-*`, reject unknown services, cap pressure at 60 seconds, cap memory at
1024 MiB, and write disk pressure only to a fixed file inside a chaos PostgreSQL container.

Never point these commands at a developer or production Compose project.

## Application experiments

Start and migrate PostgreSQL, then run the implemented durability-boundary scenarios:

```bash
scripts/infra up
scripts/infra ready-postgres
scripts/infra migrate
scripts/chaos run APP-01
scripts/chaos run APP-02
scripts/chaos run APP-03
scripts/chaos run APP-04
```

`APP-01` proves rollback before scheduler commit. `APP-02` proves idempotent client retry
after commit but before response. `APP-03` proves outbox replay after broker acknowledgement.
`APP-04` proves projection rollback and replay before checkpoint commit.

To configure a running application process directly:

```bash
export APP_ENVIRONMENT=test
export CHAOS_MODE=enabled
export CHAOS_EXPERIMENT_ID=APP-02
export CHAOS_FAULTS_JSON='[{"checkpoint":"scheduler.after_commit","action":"raise","job_id":"chaos-app-02"}]'
```

Rules support `delay`, `raise`, `exit`, and bounded `pause`, exact activation count, and
optional job/correlation matching. Validate a rule file without activating it:

```bash
scripts/chaos validate infra/chaos/example-rules.json
```

## Network experiments

The base topology exposes these scoped Toxiproxy edges:

| Proxy | Host port | Upstream |
| --- | ---: | --- |
| `cassandra` | 9042 | Cassandra 9042 |
| `scheduler-postgres` | 15432 | Scheduler PostgreSQL 5432 |
| `edr-postgres` | 15433 | EDR PostgreSQL 5432 |

Run the implemented latency/jitter and pool-saturation checks:

```bash
RUN_CHAOS_TESTS=1 scripts/infra test-postgres tests/integration/test_resource_faults.py
```

Use `compose.chaos.yaml` when application containers themselves must traverse the PostgreSQL
proxies:

```bash
export JOB_VISIBILITY_COMPOSE_PROJECT=job-visibility-chaos-local
docker compose -f compose.yaml -f compose.chaos.yaml --profile apps \
  --project-name "$JOB_VISIBILITY_COMPOSE_PROJECT" up -d --build
```

## CPU, memory, and disk controls

Examples for an isolated chaos project:

```bash
scripts/chaos pressure cpu scheduler-worker \
  --project job-visibility-chaos-local --amount 0.25 --duration 15
scripts/chaos pressure memory visibility-api \
  --project job-visibility-chaos-local --amount 128 --duration 15
scripts/chaos pressure disk edr-postgres \
  --project job-visibility-chaos-local --amount 64
scripts/chaos stats scheduler-worker --project job-visibility-chaos-local
scripts/chaos cleanup scheduler-worker --project job-visibility-chaos-local
scripts/chaos cleanup edr-postgres --project job-visibility-chaos-local
```

CPU and memory pressure use a finite helper process inside the target container. CPU quota
is reset by cleanup. Disk pressure uses only
`/var/lib/postgresql/data/.chaos-pressure` and is capped at 512 MiB; cleanup removes it.
These controls are implemented, but the Spec 005 OOM, CPU, and disk correctness scenarios
remain nightly evidence work until their end-to-end assertions are automated.

## Evidence and emergency cleanup

Collect evidence before cleanup when possible:

```bash
JOB_VISIBILITY_COMPOSE_PROJECT=job-visibility-chaos-local \
  scripts/chaos evidence my-run-id
```

Artifacts include Compose state/logs, Docker resource samples, container exit/OOM state,
connector status, and active toxics. Then reset proxy toxics and pressure artifacts:

```bash
curl -fsS -X POST http://localhost:8474/reset
scripts/chaos cleanup scheduler-worker --project job-visibility-chaos-local
scripts/chaos cleanup scheduler-postgres --project job-visibility-chaos-local
scripts/chaos cleanup edr-postgres --project job-visibility-chaos-local
```

If the isolated environment is disposable, use the existing guarded volume cleanup:

```bash
export JOB_VISIBILITY_COMPOSE_PROJECT=job-visibility-chaos-local
export CONFIRM_DELETE_TEST_VOLUMES=job-visibility-chaos-local
scripts/infra down-volumes
```
