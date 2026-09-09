package com.example.jobs.pull.execution;

import com.example.jobs.pull.contract.Job;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

@FunctionalInterface
public interface BusinessHandler {
  Future<JsonObject> execute(Job job, String attemptId);
  final class Failure extends RuntimeException {
    public final boolean retryable;
    public Failure(String code, boolean retryable) { super(code); this.retryable = retryable; }
  }
}
