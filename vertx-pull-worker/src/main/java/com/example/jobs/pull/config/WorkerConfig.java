package com.example.jobs.pull.config;

import java.util.Map;

/** Validated bootstrap and handler-capacity settings; RuntimeConfig owns Kafka fleet bounds. */
public record WorkerConfig(int healthPort, int podSlots, int partitionSlots, String workload) {
  public WorkerConfig {
    if (healthPort < 0 || healthPort > 65535 || podSlots < 1 || partitionSlots < 1) {
      throw new IllegalArgumentException("Invalid health port or concurrency limits");
    }
    if (!"subscriber".equals(workload) && !"group".equals(workload)) {
      throw new IllegalArgumentException("WORKLOAD must be subscriber or group");
    }
  }

  public static WorkerConfig fromEnvironment(Map<String, String> env) {
    if (!"false".equals(env.getOrDefault("KAFKA_ENABLE_AUTO_COMMIT", "false"))) {
      throw new IllegalArgumentException("Kafka auto commit is prohibited");
    }
    if (!"read_committed".equals(env.getOrDefault("KAFKA_ISOLATION_LEVEL", "read_committed"))) {
      throw new IllegalArgumentException("Kafka isolation must be read_committed");
    }
    return new WorkerConfig(Integer.parseInt(env.getOrDefault("HEALTH_PORT", "8080")),
        Integer.parseInt(env.getOrDefault("MAX_IN_FLIGHT_PER_POD", "10")),
        Integer.parseInt(env.getOrDefault("MAX_IN_FLIGHT_PER_PARTITION", "10")),
        env.getOrDefault("WORKLOAD", "subscriber"));
  }
}
