# Spec 004 production API evidence

Status: the production HTTP slice and local live evidence are implemented. Hosted CI
evidence remains pending until the branch is published.

Delivered contracts:

- scheduler-only and visibility-only production ASGI factories;
- role-specific database configuration and pools;
- durable submission create, identical replay, and conflict decisions;
- bounded scheduler recover/claim/execute runtime;
- Compose application profile with scheduler API, visibility API, worker, publisher, and
  projector;
- least-privilege local EDR reader provisioning; and
- bounded HTTP smoke coverage from submission through projected visibility.

Local live execution on 2026-08-21 proved:

- the HTTP smoke passed creation, identical replay, conflicting replay, terminal visibility,
  attempt history, and correlation search;
- a unique `FIBONACCI` submission reached `SUCCEEDED` through the standalone worker,
  publisher, Kafka, Connect, and projector;
- the PostgreSQL integration subset passed 9 tests, including durable replay/conflict
  assertions;
- the full resilience subset passed 19 tests in 76.05 seconds; and
- formatting, Ruff, shell syntax, Compose validation, and the non-infrastructure suite
  passed.

The full-suite run also exposed and fixed an inherited ordering defect: a broker outage can
terminate the single local Connect worker, so the Kafka recovery scenario now explicitly
restores Connect before later DLQ scenarios execute.
