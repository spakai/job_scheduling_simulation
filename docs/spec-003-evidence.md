# Spec 003 implementation evidence

Status: bounded configuration, failure-domain isolation, and the current automated
correctness/outage suite are implemented and verified against the local container stack.
Complete restart-matrix and load acceptance evidence remain outstanding.

## Automated baseline

The local non-infrastructure suite passes with its opt-in infrastructure tests skipped. It
covers:

- deadline configuration validation;
- bounded polling diagnostics;
- Toxiproxy API lifecycle and cleanup behavior;
- queue-age, connector-task, and DLQ health classification;
- Cassandra lost-finalization reconciliation logic; and
- conditional-conflict behavior without blind finalization.

## Infrastructure suites added

The opt-in suites provide executable coverage for:

| Evidence | Test |
| --- | --- |
| Separate scheduler/EDR endpoints | `test_authorities_use_independent_endpoints` |
| PostgreSQL statement/lock deadlines | `test_postgres_resilience.py` |
| Concurrent scheduler pollers | `test_concurrent_pollers_claim_disjoint_jobs` |
| Exact retry exhaustion | `test_sustained_retryable_failure_stops_at_exact_attempt_limit` |
| Stale fencing after claim recovery | `test_expired_worker_cannot_commit_over_recovered_claim` |
| Publisher acknowledgement crash window | `test_publisher_crash_after_ack_leaves_republishable_outbox` |
| Projector pre-commit rollback/replay | `test_projection_crash_rolls_back_and_replay_commits_once` |
| Real Cassandra timeout/recovery through Toxiproxy | `test_real_cassandra_read_times_out_through_proxy_and_recovers` |
| Cassandra post-finalize lost response | `test_post_finalize_response_loss_reconciles_exactly_once` |
| Forced same-checksum two-worker conflict | `test_two_workers_force_one_checksum_conflict_then_apply_distinct_operations_once` |
| Kafka/Schema Registry/Connect/EDR path | `test_real_kafka_connect_path_persists_canonical_edr` |

The local Docker execution on 2026-08-21 produced:

- PostgreSQL subset: 8 passed in 0.75 seconds.
- Full resilience and outage subset: 15 passed in 74.82 seconds.
- Broker recovery required a normal Connect consumer-group rebalance; the bounded test
  observed outbox drain followed by persistence of both buffered EDRs.
- Scheduler and EDR database stop/start isolation passed in both directions.
- EDR database outage buffered a valid event in Kafka and persisted it after database
  recovery.

The `Resilience` workflow runs the PostgreSQL subset on relevant pull requests and the full
current infrastructure subset nightly or by manual dispatch. Failures upload redacted
Compose, connector, and proxy evidence.

## Configuration delivered

- Per-role PostgreSQL connect, pool, statement, lock, idle-transaction, and transaction
  deadlines.
- Kafka socket, request, delivery, metadata, and shutdown flush deadlines.
- Separate Schema Registry connect/read deadlines using the HTTP client's structured timeout.
- Validated relationships between lock/statement/transaction and request/delivery/flush
  values.
- Queue-age and connector/DLQ readiness evaluation.

## Failure-domain isolation delivered

The Compose stack now has independent `scheduler-postgres` and `edr-postgres` services,
ports, volumes, bootstrap scripts, health checks, owners, and migration URLs. Kafka Connect
depends only on and writes only to `edr-postgres`.

## Evidence still required

The following work cannot be claimed complete until run on real infrastructure:

- repeat the live matrix in hosted CI and address any platform-specific instability;
- automate poison-record continuation and identity-collision DLQ assertions;
- complete the component pre/post-commit restart matrix;
- implement the durable representative-load driver;
- agree the resource envelope for `representative-v1`; and
- record p95/p99 freshness, throughput, recovery, and soak results.

The workload profile is deliberately marked `proposed`; it is not performance evidence.
