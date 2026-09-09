package com.example.jobs.pull.execution;

import com.example.jobs.pull.contract.Job;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonObject;

/** Nonblocking integration. The dependency must implement the stable Idempotency-Key contract. */
public final class HttpBusinessHandler implements BusinessHandler {
  private final HttpClient client;
  private final String url;
  private final long timeout;
  private final int maxBytes;
  public HttpBusinessHandler(Vertx vertx, String url, long timeout, int maxBytes) {
    client = vertx.createHttpClient(); this.url = url; this.timeout = timeout; this.maxBytes = maxBytes;
  }
  public Future<JsonObject> execute(Job job, String attemptId) {
    return client.request(new RequestOptions().setAbsoluteURI(url).setMethod(HttpMethod.POST).setTimeout(timeout))
        .compose(request -> request.putHeader("Content-Type","application/json")
            .putHeader("Idempotency-Key",job.id()).putHeader("X-Attempt-Id",attemptId)
            .putHeader("X-Correlation-Id",job.wire().getString("correlationId"))
            .send(job.wire().toBuffer()).compose(response -> {
              Promise<JsonObject> result = Promise.promise();
              Buffer body = Buffer.buffer();
              response.exceptionHandler(result::tryFail);
              response.handler(chunk -> {
                if (chunk.length() > maxBytes - body.length()) {
                  request.reset(); result.tryFail(new Failure("RESPONSE_TOO_LARGE",false));
                } else body.appendBuffer(chunk);
              });
              response.endHandler(ignored -> {
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                  try { result.tryComplete(body.length() == 0 ? new JsonObject() : body.toJsonObject()); }
                  catch (Exception invalid) { result.tryFail(new Failure("INVALID_RESPONSE",false)); }
                } else result.tryFail(new Failure("HTTP_"+status, status == 429 || status >= 500));
              });
              return result.future();
            })).recover(error -> Future.failedFuture(error instanceof Failure ? error : new Failure("DEPENDENCY_IO",true)));
  }
  public Future<Void> close() { return client.close(); }
}
