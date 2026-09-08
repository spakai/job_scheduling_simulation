package com.example.jobs.pull;

import com.example.jobs.pull.config.WorkerConfig;
import com.example.jobs.pull.config.RuntimeConfig;
import com.example.jobs.pull.execution.HttpBusinessHandler;
import com.example.jobs.pull.runtime.PullRuntime;
import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.core.http.HttpServer;

/** HTTP health survives recovery; each replacement must fence and restore before becoming ready. */
public final class WorkerVerticle extends VerticleBase {
  private final WorkerConfig config;
  private final RuntimeConfig runtimeConfig;
  private HttpServer server;
  private PullRuntime runtime;
  private HttpBusinessHandler handler;
  private boolean stopping;
  private boolean recovering;
  private int failures;

  public WorkerVerticle(WorkerConfig config) { this(config,null); }
  public WorkerVerticle(WorkerConfig config, RuntimeConfig runtimeConfig) {
    this.config = config; this.runtimeConfig = runtimeConfig;
  }
  @Override public Future<?> start() {
    return vertx.createHttpServer().requestHandler(request -> {
      switch (request.path()) {
        case "/health/live", "/health/startup" -> request.response().end("live\n");
        case "/health/ready" -> {
          boolean ready = !stopping && !recovering && runtime != null && runtime.ready();
          request.response().setStatusCode(ready ? 200:503).end(ready ? "ready\n":"not ready: awaiting Kafka assignment and EDR restoration\n");
        }
        case "/metrics" -> request.response().putHeader("Content-Type","text/plain; version=0.0.4")
            .end(com.example.jobs.pull.runtime.Telemetry.scrape() + (runtime == null ? "":runtime.metrics()));
        default -> request.response().setStatusCode(404).end();
      }
    }).listen(config.healthPort()).onSuccess(started -> {
      server = started;
      if (runtimeConfig != null) launch();
    });
  }
  private void launch() {
    if (stopping) return;
    recovering = false;
    handler = runtimeConfig.retryWorker() ? null : new HttpBusinessHandler(vertx,runtimeConfig.handlerUrl(),runtimeConfig.handlerTimeoutMs(),runtimeConfig.maxRecordBytes());
    runtime = new PullRuntime(vertx,runtimeConfig,config,handler,this::recover);
    runtime.start().onFailure(this::recover);
  }
  private void recover(Throwable error) {
    if (stopping || recovering) return;
    recovering = true;
    System.err.println(new io.vertx.core.json.JsonObject().put("event","runtime_recovery").put("errorClass",error.getClass().getSimpleName()).encode());
    Future<Void> cancel = handler == null ? Future.succeededFuture():handler.close();
    cancel.eventually(runtime::close).onComplete(closed -> {
      if (closed.failed()) {
        // A failed drain can retain native calls. Do not create a replacement in the same process.
        System.err.println("Runtime close failed; process replacement required");
        vertx.close(); return;
      }
      long backoff = Math.min(30000,1000L << Math.min(5,failures++));
      vertx.setTimer(backoff+java.util.concurrent.ThreadLocalRandom.current().nextLong(250),id -> launch());
    });
  }
  public int port() { return server.actualPort(); }
  @Override public Future<?> stop() {
    stopping = true;
    Future<Void> drain = runtime == null ? Future.succeededFuture():runtime.close();
    return drain.eventually(() -> handler == null ? Future.succeededFuture():handler.close())
        .eventually(() -> server == null ? Future.succeededFuture():server.close());
  }
}
