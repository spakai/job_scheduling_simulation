# Spec 005 implementation and evidence

Status: application fault injection and bounded resource-control foundations are
implemented. Four durability-boundary scenarios and two PostgreSQL degradation scenarios
have live automated evidence. OOM, CPU, bandwidth, and disk correctness matrices remain to
be promoted from controls into nightly end-to-end assertions.

## Delivered controls

- `FaultRule` validates allowlisted checkpoints/actions and finite bounds.
- `fault_injector_from_env` is no-op by default and rejects production activation.
- Scheduler, worker, publisher, projector, and visibility query checkpoints are composed in
  production process factories.
- `scripts/chaos` lists/validates experiments, runs `APP-01`–`APP-04`, collects evidence,
  and controls bounded CPU, memory, and disk pressure.
- `ResourcePressureController` requires a `job-visibility-chaos-*` project and allowlisted
  service.
- Scheduler and EDR PostgreSQL Toxiproxy edges support latency/jitter experiments.
- Failure artifacts now include Docker stats plus container exit/OOM state.

## Automated evidence map

| Scenario | Automated proof | Status |
| --- | --- | --- |
| `APP-01` | `test_app_01_exception_before_commit_leaves_no_partial_state` | Passed against PostgreSQL |
| `APP-02` | `test_app_02_exception_after_commit_replays_idempotently` | Passed against PostgreSQL |
| `APP-03` | `test_publisher_crash_after_ack_leaves_republishable_outbox` | Passed against PostgreSQL with shared injector |
| `APP-04` | `test_projection_crash_rolls_back_and_replay_commits_once` | Passed against PostgreSQL with shared injector |
| `NET-01` | `test_net_01_postgres_latency_is_observable_and_recovers` | Passed through Toxiproxy with latency and jitter |
| `NET-03` | `test_net_03_pool_saturation_is_bounded_and_recovers` | Passed against a real bounded SQLAlchemy/PostgreSQL pool |
| `NET-02` | Bandwidth control and Connect lag/drain assertion | Pending |
| `MEM-01`–`MEM-03` | Bounded memory control exists; OOM/restart invariants | Pending |
| `CPU-01`–`CPU-03` | Bounded CPU control exists; lease/lag recovery assertions | Pending |
| `DISK-01`–`DISK-03` | Disposable fixed-file control exists; transaction/recovery assertions | Pending |
| `APP-05` | Checkpoint delay action exists; HTTP deadline experiment | Pending |
| `APP-06` | Existing poison/DLQ continuation scenario | Implemented under Spec 003; Spec 005 manifest integration pending |

## Local validation

The initial implementation passed:

- Ruff formatting and linting;
- the complete non-infrastructure pytest suite;
- four application-fault tests against live PostgreSQL; and
- `NET-01` and `NET-03` against live PostgreSQL/Toxiproxy.

This document deliberately distinguishes available injection controls from completed chaos
evidence. A pressure command alone does not prove a resilience hypothesis.
