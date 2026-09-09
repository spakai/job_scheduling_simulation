package com.example.jobs.pull;

import static org.junit.jupiter.api.Assertions.*;
import com.example.jobs.pull.config.WorkerConfig;
import io.vertx.core.Vertx;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WorkerBootstrapTest {
  @Test void bootstrapIsLiveButNotReadyWithoutKafkaAndDurableEdr() throws Exception {
    Vertx vertx = Vertx.vertx();
    try (HttpClient client = HttpClient.newHttpClient()) {
      var worker = new WorkerVerticle(new WorkerConfig(0, 10, 10, "subscriber"));
      vertx.deployVerticle(worker).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      var live = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + worker.port() + "/health/live"))
          .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
      var ready = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + worker.port() + "/health/ready"))
          .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals(200, live.statusCode());
      assertEquals(503, ready.statusCode());
      assertTrue(ready.body().contains("restoration"));
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
  }

  @Test void unsafeKafkaConfigurationFailsBeforeBootstrap() {
    assertThrows(IllegalArgumentException.class,
        () -> WorkerConfig.fromEnvironment(Map.of("KAFKA_ENABLE_AUTO_COMMIT", "true")));
    assertThrows(IllegalArgumentException.class,
        () -> WorkerConfig.fromEnvironment(Map.of("KAFKA_ISOLATION_LEVEL", "read_uncommitted")));
    assertThrows(IllegalArgumentException.class,
        () -> WorkerConfig.fromEnvironment(Map.of("MAX_IN_FLIGHT_PER_POD", "0")));
    assertThrows(IllegalArgumentException.class,
        () -> WorkerConfig.fromEnvironment(Map.of("WORKLOAD", "both")));
    assertEquals(new WorkerConfig(8080, 10, 10, "subscriber"), WorkerConfig.fromEnvironment(Map.of()));
  }
}
