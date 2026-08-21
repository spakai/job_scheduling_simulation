# Spec 004 production API runbook

## Start and verify

```bash
scripts/infra bootstrap
scripts/infra up-apps
scripts/infra smoke-http
```

The smoke command verifies creation (`201`), identical replay (`200`), conflicting replay
(`409`), terminal visibility, attempt history, and correlation search through HTTP.

## Role endpoints

| Role | Address | Authority |
| --- | --- | --- |
| Scheduler API | `http://localhost:8000` | Scheduler PostgreSQL |
| Visibility API | `http://localhost:8001` | EDR PostgreSQL read-only role |

Both APIs expose `/health/live`, `/health/ready`, `/metrics`, `/docs`, and `/openapi.json`.
Scheduler submission is available only at `POST /scheduler/jobs` on port 8000. Visibility
queries are available only on port 8001.

## Diagnose delayed visibility

Follow the last durable fact in order:

1. `scheduler_jobs` contains and eventually completes the job.
2. `scheduler_outbox` contains unpublished or acknowledged lifecycle records.
3. Kafka and the Connect task contain delivery/lag state.
4. `edr_events` contains immutable journal records.
5. `projected_events` and `job_visibility` contain the projected result.

Use `scripts/infra diagnostics` for container, connector, and proxy state. A visibility `404`
means no projection has been observed; it does not prove the scheduler lacks the job.

## Stop

```bash
scripts/infra down
```

Stopping processes does not delete durable volumes. Destructive test-volume removal remains
guarded by the explicit confirmation mechanism in `scripts/infra down-volumes`.
