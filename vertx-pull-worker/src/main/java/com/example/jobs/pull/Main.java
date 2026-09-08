package com.example.jobs.pull;

import com.example.jobs.pull.config.WorkerConfig;
import com.example.jobs.pull.config.RuntimeConfig;
import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;

public final class Main {
  private Main() {}

  public static void main(String[] args) {
    WorkerConfig config = WorkerConfig.fromEnvironment(System.getenv());
    RuntimeConfig runtimeConfig = RuntimeConfig.fromEnvironment(System.getenv());
    Vertx vertx = Vertx.vertx(com.example.jobs.pull.runtime.Telemetry.options());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try { vertx.close().toCompletionStage().toCompletableFuture().get(90, TimeUnit.SECONDS); }
      catch (Exception failure) { System.err.println("Shutdown did not finish: " + failure.getClass().getSimpleName()); }
    }, "worker-shutdown"));
    vertx.deployVerticle(new WorkerVerticle(config, runtimeConfig)).onFailure(failure -> {
      System.err.println("Worker bootstrap failed: " + failure.getClass().getSimpleName());
      vertx.close().onComplete(ignored -> System.exit(1));
    });
  }
}
