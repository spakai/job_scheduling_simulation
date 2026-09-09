# Spec 007 implementation and local acceptance evidence

Validated locally on 2026-09-08. Runtime implementation, deployment assets and executable
acceptance tooling are present. Production release approval is separate from these results.

## Build and correctness

- Java 21, pinned Vert.x BOM 5.0.12, Maven dependency convergence and shaded packaging pass.
- **29 unit tests pass**, with zero failures/errors/skips.
- **16 real-Kafka integration tests pass**, with zero failures/errors/skips. The final
  correctness suite ran in an isolated build directory while capacity ran in the workspace,
  avoiding concurrent replacement of the shaded JAR used by Failsafe.
- Docker build passes. The image runs as UID/GID 10001 with read-only root, dropped
  capabilities and bounded writable `/tmp`; Java 21 is present and Python is absent.
- Compose baseline starts five healthy workers on ten subscriber partitions. An additional
  100-request smoke run drained all partitions to zero lag.
- Shell syntax, Python chaos-tool lint and Compose profile validation pass.

Final local image ID:
`sha256:ebecef4975d8b0b0ded083ede0b3ff71895389c185c2df471cf6555dfc516eb8`.

Repeatable commands:

```bash
mvn -B -f vertx-pull-worker/pom.xml verify -Pkafka
scripts/spec007 build
scripts/spec007 baseline
scripts/spec007 capacity
scripts/spec007 chaos
```

The broker must be running for direct Maven integration commands. `scripts/spec007 test`
starts the repository broker before invoking Maven. Kafka test topics/groups use unique
names. Do not run two Maven packaging commands concurrently against the same build directory.

Unit and Kafka suite summaries: [unit tests](evidence/spec-007/unit-tests.json) and
[Kafka tests](evidence/spec-007/kafka-tests.json).

## Recovery and correctness coverage

| Concern | Evidence |
| --- | --- |
| Concurrent lanes and owner FIFO | Real Kafka: one pod, three partitions, five active slots; different owners overlap and a same-owner follower waits for confirmed prefix commit. |
| Contiguous offsets | Deterministic shuffled/sparse tracker tests, bounded split batches, and real-Kafka failed-102 barriers proving next offset stays 102 until disposition resolves. |
| Execution loss | An actual execution verticle is undeployed with a pending event-bus request. The deadline triggers recovery, late success is fenced, and completed followers restore without execution. |
| Durable work behind gaps | Real-Kafka restart restores completed outcomes before dispatch and reconciles a started-only live lease. |
| Start EDR failure | Injected EDR send failure prevents business invocation and source commit; replacement recovers. |
| Prefix publication failure | Injected lifecycle send failure leaves result/lifecycle invisible and source unchanged while completion EDR remains durable; replacement suppresses business replay. |
| Transaction ambiguity/fencing | Real broker commit followed by lost acknowledgement is resolved from broker state; successor EDR producer fences the former owner. |
| Failed-attempt dedup | Durable `DISPOSITION_COMPLETED` stores exact retry/DLQ/result/lifecycle outputs. Restart behind a gap and duplicate delivery reconstruct them without another invocation. |
| Internal retry/requeue | Three physical attempts have distinct durable attempt IDs; separately subscribed retry worker atomically requeues, then the work fleet processes the next logical attempt. |
| Identity and routing | Conflicting requests and legacy keys reach DLQ. Nested JSON hashing is canonical; subscriber/group translation preserves the envelope and job identity. |
| Admission | Deterministic fractional/adaptive tests include old-generation results failing to resolve a half-open probe. Real Kafka confirms zero TPS creates no attempts or commits. |
| Workload isolation | A stalled group handler leaves subscriber capacity/readiness available and subscriber work commits independently. |
| Migration preconditions | Real Kafka: active group rejected, inactive group with backlog rejected, inactive drained group accepted. This is not a distributed deployment lock. |
| Bounds | Tracker count/byte limits, retained completion bounds, shared handler capacity, transaction limits and deterministic 20,000/100,000 tracker stress. |
| Context ownership | Foreign-thread completion is returned to the owning context. Native transactions are serialized on a dedicated thread; architecture tests guard consumer/producer access. |

## Measured compressed Kafka capacity

Five worker runtimes in one JVM, ten fixed partitions, ten slots per worker, 500 repeating
owners, synthetic 1 ms asynchronous handlers, and 1,000 TPS admission per worker. Each
invocation uses durable start/completion EDR transactions and exact source-prefix commits.
The result verifier reads with `isolation.level=read_committed`.

| Requests | Elapsed | Throughput | Unique successful results | Owner overlap |
| ---: | ---: | ---: | ---: | ---: |
| 20,000 | 104.65 s | 191.12/s | 20,000 | 0 |
| 100,000 | 517.69 s | 193.16/s | 100,000 | 0 |

Both profiles passed. Peak observed business concurrency was 4 and 7 respectively, below
the 50-slot bound; a 1 ms handler does not occupy slots long enough to demonstrate sustained
50-way business execution. No runtime fault or duplicate prefix result was observed.

Raw reports: [20,000](evidence/spec-007/capacity-20000.json) and
[100,000](evidence/spec-007/capacity-100000.json). These are pipeline volume/throughput results,
**not** production SLO proof, five-container load measurements, or evidence for real
60–180-second dependency calls. The separate Compose smoke check exercises five containers.

## Realistic business workload profile

The production workload is materially different from the compressed capacity profile above.
After a request is polled, processing may take approximately 1–5 minutes because the handler
calls multiple external services and processes roughly one month of records. A single request
may represent approximately 1,000–100,000 records. This profile is a workload assumption from
the operating scenario, not a completed measurement in this repository.

The next performance test must therefore use representative external-service stubs or a
controlled test environment with realistic latency, fan-out, response sizes, failures, and
rate limits. It must measure end-to-end completion time and percentiles, not only Kafka drain
rate, across at least these cases:

- 1,000, 10,000, and 100,000 records per request;
- one request, concurrent requests, and burst arrivals;
- external-service latency and failure distributions that produce 1–5 minute completion;
- owner skew and overlapping owners;
- configured pod/partition concurrency and TPS limits; and
- restart, retry, timeout, and idempotency behavior while work is in flight.

For orientation, processing 100,000 records in 1–5 minutes requires an average record rate of
approximately 333–1,667 records/second if the records are processed continuously. That rate
does not determine worker capacity by itself: external-service latency, fan-out, concurrency,
and service quotas are the controlling inputs.

The focused integration test `realHttpWorkloadTakesOneMinuteThen503BackpressureReducesTps`
now covers the first realistic slice. It ran in **62.66 seconds** against the production
`HttpBusinessHandler` and a real local HTTP server: two concurrent requests completed after a
60-second dependency delay, then four requests received `503` responses. All six source records
advanced safely, four `ATTEMPT_FAILED`/`DISPOSITION_COMPLETED` outcomes were durable, and the
pod admission rate reduced from `2 TPS` to `0.5 TPS`. The stub accounts for three external
service stages per request so the test records 18 stage calls overall; those stages are still
modeled within one local endpoint and must be replaced by separate service stubs for a full
fan-out benchmark.

The focused `AdaptiveAdmissionTest` also passes all three tests. With a healthy rate of `2 TPS`
and two-sample adaptive windows, it verifies the progression `2.0 -> 1.0 -> 0.5 -> 0.0 TPS`,
the half-open probe after cooldown, successful recovery to `0.5 TPS`, and administrative
`RATE_LIMIT_TPS=0`, which remains stopped and does not probe.

## Actual container kill

The final five-container baseline published 100 synthetic requests and SIGKILLed subscriber
slot 0 during processing. The same slot restarted and became healthy. All **100 successful
results** were recovered through `read_committed`, with no duplicate prefix result observed.
Recovery took **244.49 seconds**: a started-only record correctly retained its live 240-second
lease while other partitions drained. This demonstrates bounded lease reconciliation rather
than premature external replay. The demo dependency has no arbitrary business side effect.

Raw report: [container kill](evidence/spec-007/container-kill.json).

## Remaining production acceptance

The implementation does not perform production cutover or retire the Python deployment.
Before release, complete the environment-specific gates in the specification and runbook:

- Full multi-broker/network-outage, repeated rebalance, disk-pressure and compaction chaos.
- Measured 60–180-second dependency workloads, burst/recovery SLOs, owner skew, and fleet TPS budgets.
- Downstream idempotency/fencing evidence for a crash during an actual external side effect.
- Security/dependency/image review, production ACLs and observability review.
- Python/Java business parity, quiesced producer migration and rollback with mutually
  exclusive fleets, followed by approved retirement of the Python production worker.

A completed-request ledger cannot prove exactly-once arbitrary external effects. Started-only
lease recovery deliberately waits for the live lease; expiry permits idempotent replay.
