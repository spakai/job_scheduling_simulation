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
