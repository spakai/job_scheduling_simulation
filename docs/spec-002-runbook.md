# Spec 002 operations runbook

Use `/health/ready` and Prometheus metrics to identify the delayed stage: scheduler due
jobs, unpublished outbox, Kafka/Connect task and DLQ, unprojected EDRs, or Cassandra.

- Kafka outage: leave scheduler writes enabled, restore brokers, then watch outbox age fall.
- Scheduler database outage: stop pollers/publishers; never reconstruct jobs from EDR data.
- EDR database outage: pause projection; Connect replays committed offsets after recovery.
- Failed connector or poison record: inspect task status and the DLQ headers, correct the
  connector/schema issue, and replay only after preserving the original record.
- Identity collision: retain the first immutable `event_id`; quarantine the conflicting
  payload and investigate its producer.
- Projection backlog: restart workers or run rebuild against an isolated target; never
  delete `edr_events`.
- Cassandra timeout or unknown write outcome: look up the stable operation marker before
  retrying. Conditional conflicts and unavailable/connection failures remain scheduler
  retries; validation and input-bound failures do not.
- Lost worker lease: discard its result. Fenced completion will reject the stale token.

Local credentials in `compose.yaml` are development-only. Production deployments must use
TLS, authenticated Kafka, secret-managed database credentials, Cassandra authentication,
and per-role network policies. Never log payload bodies, credentials, or raw exception text.
