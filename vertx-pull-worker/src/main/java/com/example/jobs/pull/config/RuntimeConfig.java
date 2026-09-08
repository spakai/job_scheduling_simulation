package com.example.jobs.pull.config;

import java.util.HashMap;
import java.util.Map;

/** Validated production settings; all topic names are workload scoped. */
public record RuntimeConfig(String brokers, String namespace, String workload, String group,
    String slot, String handlerUrl, boolean retryWorker, int partitions, int windowRecords,
    long windowBytes, int pollRecords, int maxRecordBytes, double tps, int burst,
    long handlerTimeoutMs, long leaseMs, long retentionMs, long replayWindowMs,
    int internalRetries, long retryBudgetMs, long drainMs, String stateDir,
    int adaptiveSamples, long adaptiveCooldownMs) {
  public RuntimeConfig {
    if (brokers.isBlank() || !namespace.matches("[a-zA-Z0-9._-]+")
        || !slot.matches("[a-zA-Z0-9._-]+") || !java.util.Set.of("subscriber", "group").contains(workload)
        || !group.equals(namespace + workload + (retryWorker ? "-retry-workers" : "-workers"))) {
      throw new IllegalArgumentException("Invalid fleet, stable slot or allowlisted group");
    }
    if (partitions < 1 || partitions > 100 || windowRecords < 4 || pollRecords < 1
        || pollRecords > windowRecords / 2 || maxRecordBytes < 1024
        || windowBytes < (long) windowRecords * maxRecordBytes * 4
        || !Double.isFinite(tps) || tps < 0 || burst < 1 || handlerTimeoutMs < 1
        || leaseMs < handlerTimeoutMs + 30_000 || replayWindowMs < 1 || retentionMs <= replayWindowMs
        || internalRetries < 0 || internalRetries > 10 || retryBudgetMs < handlerTimeoutMs
        || drainMs < 1000 || adaptiveSamples < 1 || adaptiveCooldownMs < 1 || stateDir.isBlank()) throw new IllegalArgumentException("Unsafe runtime bounds");
    if (!retryWorker && !(handlerUrl.startsWith("http://") || handlerUrl.startsWith("https://"))) {
      throw new IllegalArgumentException("HANDLER_URL must be an HTTP idempotent business endpoint");
    }
  }
  public String workTopic() { return namespace + workload + "-rerate"; }
  public String sourceTopic() { return retryWorker ? topic("retry") : workTopic(); }
  public String topic(String kind) { return workTopic() + "-" + kind + ".v1"; }
  public String writerId(int partition) { return workTopic() + "-edr-p" + partition; }
  public String transactionId() { return group + "-" + slot; }
  public Map<String, Object> consumerProperties() {
    Map<String, Object> props = securityProperties();
    props.put("bootstrap.servers", brokers); props.put("group.id", group);
    props.put("allow.auto.create.topics", false); props.put("enable.auto.commit", false); props.put("isolation.level", "read_committed");
    props.put("auto.offset.reset", "earliest"); props.put("max.poll.records", pollRecords);
    props.put("max.partition.fetch.bytes", maxRecordBytes); props.put("fetch.max.bytes", maxRecordBytes * pollRecords);
    props.put("max.poll.interval.ms", 300000); props.put("default.api.timeout.ms", 10000);
    props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }
  public Map<String, Object> producerProperties(String id) {
    Map<String, Object> props = securityProperties();
    props.put("bootstrap.servers", brokers); props.put("transactional.id", id);
    props.put("enable.idempotence", true); props.put("acks", "all");
    props.put("max.block.ms", 10000); props.put("delivery.timeout.ms", 15000);
    props.put("request.timeout.ms", 10000); props.put("transaction.timeout.ms", 30000);
    props.put("max.request.size", 4 * 1024 * 1024);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
  private static Map<String,Object> securityProperties() {
    Map<String,Object> result=new HashMap<>();
    String file=System.getenv("KAFKA_PROPERTIES_FILE");
    if(file!=null) {
      java.util.Properties properties=new java.util.Properties();
      try(var input=java.nio.file.Files.newInputStream(java.nio.file.Path.of(file))) { properties.load(input); }
      catch(java.io.IOException error) { throw new IllegalArgumentException("Cannot read mounted Kafka properties"); }
      for(String key:properties.stringPropertyNames()) {
        if(!(key.equals("security.protocol") || key.startsWith("ssl.") || key.startsWith("sasl."))) throw new IllegalArgumentException("Only Kafka security properties may be mounted");
        result.put(key,properties.getProperty(key));
      }
    }
    return result;
  }
  public Map<String,Object> adminProperties() {
    var properties=securityProperties(); properties.put("bootstrap.servers",brokers); return properties;
  }
  public static RuntimeConfig fromEnvironment(Map<String, String> env) {
    String ns = env.getOrDefault("TOPIC_NAMESPACE", "");
    // An explicit namespace separates deployments from legacy topics. A dash is conventional.
    if (ns.isEmpty()) ns = "v007-";
    String workload = env.getOrDefault("WORKLOAD", "subscriber");
    boolean retry = Boolean.parseBoolean(env.getOrDefault("RETRY_WORKER", "false"));
    return new RuntimeConfig(env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ns,
        workload, env.getOrDefault("KAFKA_GROUP_ID", ns + workload + (retry ? "-retry-workers" : "-workers")),
        env.getOrDefault("WORKER_SLOT", "local-0"), env.getOrDefault("HANDLER_URL", ""), retry,
        integer(env,"WORK_PARTITIONS",10), integer(env,"WINDOW_RECORDS",100),
        number(env,"WINDOW_BYTES",64L * 1024 * 1024), integer(env,"MAX_POLL_RECORDS",20),
        integer(env,"MAX_RECORD_BYTES",65536), Double.parseDouble(env.getOrDefault("RATE_LIMIT_TPS","2")),
        integer(env,"RATE_LIMIT_BURST",2), number(env,"HANDLER_TIMEOUT_MS",200000),
        number(env,"ATTEMPT_LEASE_MS",240000), number(env,"DEDUP_RETENTION_MS",2592000000L),
        number(env,"REPLAY_WINDOW_MS",604800000), integer(env,"INTERNAL_RETRIES",2),
        number(env,"RETRY_BUDGET_MS",660000), number(env,"DRAIN_MS",25000),
        env.getOrDefault("STATE_DIR","/tmp/vertx-pull-state"),
        integer(env,"ADAPTIVE_MIN_SAMPLES",5), number(env,"ADAPTIVE_COOLDOWN_MS",30000));
  }
  private static int integer(Map<String,String> e,String k,int d) { return Integer.parseInt(e.getOrDefault(k,""+d)); }
  private static long number(Map<String,String> e,String k,long d) { return Long.parseLong(e.getOrDefault(k,""+d)); }
}
