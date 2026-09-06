# Spec 006 implementation evidence

Last updated: 2026-09-06

## Local Docker verification

The `pull` Compose profile was built and exercised with Docker Engine 29.3.1.

| Check | Result |
| --- | --- |
| Compose rendering | Passed (`docker compose --profile pull config --quiet`). |
| Application image build | Passed from `python:3.12-slim`. |
| Kafka and Schema Registry | Healthy. |
| Topic/schema setup | Completed successfully; work/result/DLQ topics created with six partitions. |
| Two-worker readiness | Both healthy; each reported three assigned partitions in `job-workers-v1`. |
| Successful request | `PRINT` request produced a `SUCCEEDED` result visible with `read_committed`. |
| Manual source offset | Consumed partition advanced to the next offset with lag zero. |
| Lifecycle output | One schema-framed lifecycle record was visible with `read_committed`. |
| Permanent failure | Unknown handler produced result/DLQ handoff and source lag returned to zero. |
| DLQ source coordinates | 21 committed test records had 21 unique `(partition, offset)` coordinates. |
| Owner affinity integration | Twenty equal owner keys selected exactly one of six isolated test partitions. |
| Pod removal | After stopping one worker, the survivor owned all six partitions within four seconds. |
| Pod restoration | Restart restored the balanced three-partition-per-worker assignment. |
| Transaction failures | Worker metric remained zero during smoke verification. |

The raw DLQ topic log end includes Kafka transaction control batches and is therefore
larger than the number of application records. `read_committed` consumption is the evidence
source for application record counts.

## Automated verification

```text
ruff: passed
pytest: 105 passed, 24 skipped
real-Kafka owner-affinity test: passed
```

Skipped tests require other optional infrastructure profiles. Durable job deduplication is
not a Spec 006 acceptance requirement and was not tested.

## Evidence still required for release

- Full 20,000-request capacity profiles and six-versus-twelve partition comparison.
- Output-topic outage with pause/probe/resume measurements.
- Transaction abort and stale-generation fencing at deterministic failpoints.
- Rebalance with an in-flight external handler.
- Extended soak and repeated broker/pod recovery runs.

## Bounded performance results

The following local Docker measurements used six partitions, two worker pods, 10 sustained
starts/second per pod, burst 10, and the built-in `PRINT` handler. They are engineering
signals, not production benchmarks.

| Scenario | Result |
| --- | --- |
| 400 jobs, 200 owners | Produced in 1.113s; drained in 20.205s total (19.80 jobs/s); observed peak lag 294; final lag 0. |
| Per-pod distribution | Worker 1 processed 206 and worker 2 processed 194; transaction failures 0. |
| Resulting work-topic offsets | Partition log ends were 63, 64, 81, 52, 77, and 85; these include 22 earlier smoke/test records. |
| 400 jobs, 50% hot owner | Produced in 1.251s; drained in 28.970s total (13.81 jobs/s); final lag 0. |
| Hot-owner partition distribution | Partition deltas were 25, 33, 43, 25, 234, and 40, demonstrating the expected single-partition bottleneck. |
| 400-job burst plus one pod stopped | Stop began at 2.005s and completed in 1.225s; survivor acquired all six partitions and drained to lag 0 in 35.310s total. |
| Pod-loss lag | Observed peak lag 339; final lag 0; surviving pod owned all six partitions. |

The balanced test closely matched the configured aggregate ceiling of 20 starts/second.
The hot-owner result confirms that adding owners/partitions improves parallelism only when
traffic is distributed; one owner remains serialized on its partition. Pod loss reduced
the fleet ceiling to about 10 starts/second until the second worker restarted.
