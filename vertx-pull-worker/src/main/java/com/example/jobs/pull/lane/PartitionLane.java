package com.example.jobs.pull.lane;

import com.example.jobs.pull.execution.CapacityGate;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Local concurrent coordinator, not a Kafka consumer. All methods belong to one pod context.
 * Execution and commit ports are nonblocking and supplied by the runtime EDR/Kafka adapters.
 */
public final class PartitionLane<T> {
  public record Outcome<T>(T value, long bytes) {
    public Outcome { Objects.requireNonNull(value); if (bytes < 0) throw new IllegalArgumentException("bytes"); }
  }

  private final Context context;
  private final ContiguousCompletionTracker<T> tracker;
  private final CapacityGate podCapacity;
  private final int partitionCapacity;
  private final int batchRecords;
  private final long batchBytes;
  private final Function<ContiguousCompletionTracker.Delivery, Future<Outcome<T>>> execution;
  private final Function<ContiguousCompletionTracker.Prefix<T>, Future<Void>> commit;
  private final Runnable capacityAvailable;
  private final Map<Long, ContiguousCompletionTracker.Delivery> queued = new LinkedHashMap<>();
  private final Map<Long, ContiguousCompletionTracker.Delivery> unresolved = new LinkedHashMap<>();
  private final Map<String, Long> ownerGates = new HashMap<>();
  private final Map<Long, Promise<Void>> settled = new HashMap<>();
  private int active;
  private boolean paused;
  private boolean revoked;
  private boolean dispatchStopped;
  private Throwable recoveryCause;

  public PartitionLane(Context context, ContiguousCompletionTracker<T> tracker,
      CapacityGate podCapacity, int partitionCapacity, int batchRecords, long batchBytes,
      Function<ContiguousCompletionTracker.Delivery, Future<Outcome<T>>> execution,
      Function<ContiguousCompletionTracker.Prefix<T>, Future<Void>> commit,
      Runnable capacityAvailable) {
    if (partitionCapacity < 1 || batchRecords < 1 || batchBytes < 1) throw new IllegalArgumentException("Invalid limits");
    this.context = Objects.requireNonNull(context);
    this.tracker = Objects.requireNonNull(tracker);
    this.podCapacity = Objects.requireNonNull(podCapacity);
    this.partitionCapacity = partitionCapacity;
    this.batchRecords = batchRecords;
    this.batchBytes = batchBytes;
    this.execution = Objects.requireNonNull(execution);
    this.commit = Objects.requireNonNull(commit);
    this.capacityAvailable = Objects.requireNonNull(capacityAvailable);
  }

  public void accept(List<ContiguousCompletionTracker.Delivery> records) {
    checkContext();
    if (paused || revoked) throw new IllegalStateException("Lane is paused/revoked");
    tracker.registerBatch(records);
    for (var record : records) {
      queued.put(record.offset(), record);
      settled.put(record.offset(), Promise.promise());
    }
    pump();
  }

  /** Controller calls this for all lanes when shared pod capacity becomes available. */
  public void pump() {
    checkContext();
    if (paused || revoked || dispatchStopped) return;
    for (var record : List.copyOf(queued.values())) {
      if (active >= partitionCapacity) break;
      Long ownerOffset = ownerGates.get(record.owner());
      if (ownerOffset != null && ownerOffset != record.offset()) continue;
      if (!podCapacity.tryAcquire()) break;
      active++;
      ownerGates.put(record.owner(), record.offset());
      queued.remove(record.offset());
      var token = tracker.start(record.offset());
      Future<Outcome<T>> result;
      try { result = Objects.requireNonNull(execution.apply(record)); }
      catch (Exception failure) { result = Future.failedFuture(failure); }
      result.onComplete(completion -> context.runOnContext(ignored -> {
        active--;
        podCapacity.release();
        Promise<Void> notification = settled.get(record.offset());
        if (!revoked) {
          try {
            if (completion.succeeded()) {
              tracker.complete(token, completion.result().value(), completion.result().bytes());
            } else {
              if (tracker.fail(token)) unresolved.put(record.offset(), record);
            }
          } catch (RuntimeException failure) {
            pauseForRecovery(failure);
          }
        }
        if (notification != null) notification.tryComplete();
        capacityAvailable.run();
        pump();
      }));
    }
  }

  /** Explicit flush keeps transaction scheduling separate from execution admission. */
  public Future<Void> flush() {
    checkContext();
    if (paused || revoked) return Future.failedFuture("Lane requires recovery");
    final ContiguousCompletionTracker.Prefix<T> prefix;
    try {
      var candidate = tracker.preparePrefix(batchRecords, batchBytes);
      if (candidate.isEmpty()) return Future.succeededFuture();
      prefix = candidate.get();
    } catch (RuntimeException failure) {
      pauseForRecovery(failure);
      return Future.failedFuture(failure);
    }
    Promise<Void> result = Promise.promise();
    Future<Void> transaction;
    try { transaction = Objects.requireNonNull(commit.apply(prefix)); }
    catch (Exception failure) { transaction = Future.failedFuture(failure); }
    transaction.onComplete(completion -> context.runOnContext(ignored -> {
      if (revoked) {
        result.tryFail("Assignment revoked; recover broker state");
      } else if (completion.failed()) {
        pauseForRecovery(completion.cause());
        result.tryFail(completion.cause());
      } else if (tracker.committed(prefix)) {
        for (var record : prefix.records()) {
          ownerGates.remove(record.owner(), record.offset());
          settled.remove(record.offset());
        }
        result.tryComplete();
        pump();
      } else {
        pauseForRecovery(new IllegalStateException("Unexpected commit snapshot"));
        result.tryFail(recoveryCause);
      }
    }));
    return result.future();
  }

  /** Called by recovery policy after backoff; execution must reacquire durable EDR/TPS admission. */
  public void retryUnresolved(long offset) {
    checkContext();
    if (paused || revoked) throw new IllegalStateException("Lane requires recovery");
    var delivery = unresolved.get(offset);
    if (delivery == null) throw new IllegalArgumentException("Offset is not unresolved");
    tracker.requeue(offset);
    unresolved.remove(offset);
    queued.put(offset, delivery);
    settled.put(offset, Promise.promise());
    pump();
  }

  public Future<Void> executionSettled(long offset) {
    checkContext();
    Promise<Void> notification = settled.get(offset);
    if (notification == null) return Future.failedFuture("Unknown or committed record");
    return notification.future();
  }

  public void stopDispatch() { checkContext(); dispatchStopped = true; }

  public void revoke() {
    checkContext();
    revoked = true;
    tracker.revoke();
    queued.clear();
    unresolved.clear();
    ownerGates.clear();
    settled.values().forEach(promise -> promise.tryFail("Assignment revoked"));
    settled.clear();
    // Actual running calls retain pod permits until their callbacks arrive.
  }

  public boolean requiresRecovery() { checkContext(); return paused; }
  public int active() { checkContext(); return active; }

  private void pauseForRecovery(Throwable cause) { paused = true; recoveryCause = cause; }
  private void checkContext() {
    if (Vertx.currentContext() != context) throw new IllegalStateException("Wrong lane context");
  }
}
