# Vert.x Kafka pull worker (Spec 007)

Java 21 / Vert.x 5 worker with one subscribed Kafka consumer, concurrent partition lanes,
owner FIFO gates, exact-prefix transactions, partition-fenced durable EDR state, disk-backed
restoration, bounded HTTP execution, adaptive TPS, internal retry and separate requeue mode.

```bash
scripts/spec007 test     # unit + real Kafka tests
scripts/spec007 build
scripts/spec007 smoke    # two workers + explicit demo dependency
scripts/spec007 produce 100
scripts/spec007 capacity # 20,000 + 100,000 compressed real-Kafka requests
scripts/spec007 chaos    # kill one local worker and verify recovery
```

For direct execution set `HANDLER_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `WORKER_SLOT` and workload
settings, initialize topics with `com.example.jobs.pull.Operations topics`, then run
`java -jar vertx-pull-worker/target/vertx-pull-worker-0.1.0-SNAPSHOT.jar`.
The production endpoint must implement stable job-ID idempotency. The demo handler only
returns synthetic data and is selected explicitly through the local Compose profile.

See [operations](../docs/spec-007-runbook.md), [evidence](../docs/spec-007-evidence.md), and
[specification](../specs/007-vertx-kafka-pull-worker/spec.md). Production release still requires
the environment-specific capacity, security and migration acceptance gates.
