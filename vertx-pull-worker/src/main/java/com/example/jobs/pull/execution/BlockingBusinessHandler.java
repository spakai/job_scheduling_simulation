package com.example.jobs.pull.execution;

import com.example.jobs.pull.contract.Job;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Optional bounded blocking port. Lane capacity bounds submissions; calls retain permits until exit. */
public final class BlockingBusinessHandler implements BusinessHandler {
  private final WorkerExecutor executor;
  private final Function<Job,JsonObject> operation;
  public BlockingBusinessHandler(Vertx vertx,int slots,long approvedBudgetMs,Function<Job,JsonObject> operation) {
    if(slots<1 || approvedBudgetMs<1 || approvedBudgetMs>60000) throw new IllegalArgumentException("Longer blocking work requires a dedicated reviewed adapter");
    this.operation=java.util.Objects.requireNonNull(operation);
    executor=vertx.createSharedWorkerExecutor("business-blocking",slots,approvedBudgetMs,TimeUnit.MILLISECONDS);
  }
  public Future<JsonObject> execute(Job job,String attemptId) {
    return executor.executeBlocking(() -> operation.apply(job),false);
  }
  public Future<Void> close() { return executor.close(); }
}
