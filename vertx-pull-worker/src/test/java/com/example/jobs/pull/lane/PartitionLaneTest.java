package com.example.jobs.pull.lane;

import static org.junit.jupiter.api.Assertions.*;
import com.example.jobs.pull.execution.CapacityGate;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionLaneTest {
  private Vertx vertx;
  private Context context;
  private final Map<Long, Promise<PartitionLane.Outcome<String>>> handlers = new HashMap<>();
  private final List<Long> starts = new ArrayList<>();
  private final List<Long> commits = new ArrayList<>();

  @BeforeEach void setup() { vertx = Vertx.vertx(); context = vertx.getOrCreateContext(); }
  @AfterEach void close() throws Exception { await(vertx.close()); }

  private <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private <T> T onContext(Supplier<T> action) throws Exception {
    Promise<T> result = Promise.promise();
    context.runOnContext(ignored -> {
      try { result.complete(action.get()); } catch (Throwable failure) { result.fail(failure); }
    });
    return await(result.future());
  }

  private PartitionLane<String> lane(ContiguousCompletionTracker<String> tracker, CapacityGate capacity,
      int partitionSlots, java.util.function.Function<ContiguousCompletionTracker.Prefix<String>, Future<Void>> sink) {
    return new PartitionLane<>(context, tracker, capacity, partitionSlots, 10, 1000, record -> {
      starts.add(record.offset());
      Promise<PartitionLane.Outcome<String>> handler = Promise.promise();
      handlers.put(record.offset(), handler);
      return handler.future();
    }, sink, () -> {});
  }

  private ContiguousCompletionTracker.Delivery record(long offset, String owner) {
    return new ContiguousCompletionTracker.Delivery(offset, owner, 10);
  }

  private void finish(PartitionLane<String> lane, long offset, boolean success) throws Exception {
    Future<Void> settled = onContext(() -> {
      Future<Void> result = lane.executionSettled(offset);
      if (success) handlers.get(offset).complete(new PartitionLane.Outcome<>("durable-edr-" + offset, 10));
      else handlers.get(offset).fail("injected execution failure at " + offset);
      return result;
    });
    await(settled);
  }

  @Test void failed102BlocksCommitDespiteConcurrentSuccessesOnRealVertxContext() throws Exception {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 10, 1000);
    var capacity = new CapacityGate(5);
    var lane = lane(tracker, capacity, 5, prefix -> {
      commits.add(prefix.nextOffset()); return Future.succeededFuture();
    });
    onContext(() -> {
      lane.accept(List.of(record(100, "a"), record(101, "b"), record(102, "c"), record(103, "d"), record(104, "e"), record(105, "c")));
      assertEquals(List.of(100L, 101L, 102L, 103L, 104L), starts);
      assertEquals(5, capacity.active());
      return null;
    });
    for (long offset : List.of(100L, 101L, 103L, 104L)) finish(lane, offset, true);
    finish(lane, 102, false);
    await(onContext(lane::flush));
    await(onContext(lane::flush));
    onContext(() -> {
      assertEquals(List.of(102L), commits);
      assertEquals(102, tracker.committedNext());
      assertEquals(ContiguousCompletionTracker.State.UNRESOLVED, tracker.state(102));
      assertEquals(ContiguousCompletionTracker.State.COMPLETED, tracker.state(104));
      assertEquals(ContiguousCompletionTracker.State.QUEUED, tracker.state(105));
      assertEquals(0, capacity.active());
      assertFalse(lane.requiresRecovery(), "ordinary job failure is unresolved, not a Kafka ambiguity");
      return null;
    });
  }

  @Test void failedHeadCanRecoverWithoutReexecutingCompletedFollowers() throws Exception {
    var tracker = new ContiguousCompletionTracker<String>(1, 102, 10, 1000);
    var capacity = new CapacityGate(3);
    var lane = lane(tracker, capacity, 3, prefix -> {
      commits.add(prefix.nextOffset()); return Future.succeededFuture();
    });
    onContext(() -> {
      lane.accept(List.of(record(102, "a"), record(103, "b"), record(104, "c"), record(105, "a")));
      return null;
    });
    finish(lane, 103, true);
    finish(lane, 104, true);
    finish(lane, 102, false);
    await(onContext(lane::flush));
    onContext(() -> {
      assertEquals(102, tracker.committedNext());
      lane.retryUnresolved(102);
      assertEquals(List.of(102L, 103L, 104L, 102L), starts);
      assertEquals(1, capacity.active());
      return null;
    });
    finish(lane, 102, true);
    await(onContext(lane::flush));
    onContext(() -> {
      assertEquals(List.of(105L), commits);
      assertEquals(105, tracker.committedNext());
      assertEquals(List.of(102L, 103L, 104L, 102L, 105L), starts);
      return null;
    });
  }

  @Test void sameOwnerWaitsForConfirmedCommitWhileOtherOwnerRuns() throws Exception {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 10, 1000);
    Promise<Void> broker = Promise.promise();
    var lane = lane(tracker, new CapacityGate(3), 3, prefix -> broker.future());
    onContext(() -> {
      lane.accept(List.of(record(100, "a"), record(101, "a"), record(102, "b")));
      assertEquals(List.of(100L, 102L), starts);
      return null;
    });
    finish(lane, 100, true);
    Future<Void> transaction = onContext(lane::flush);
    onContext(() -> {
      assertEquals(List.of(100L, 102L), starts);
      assertEquals(100, tracker.committedNext());
      broker.complete(); return null;
    });
    await(transaction);
    onContext(() -> { assertEquals(List.of(100L, 102L, 101L), starts); return null; });
  }

  @Test void ambiguousCommitPausesDispatchAndDoesNotAdvanceLocalWatermark() throws Exception {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 10, 1000);
    var lane = lane(tracker, new CapacityGate(2), 2, prefix -> Future.failedFuture("commit timeout"));
    onContext(() -> { lane.accept(List.of(record(100, "a"), record(101, "a"))); return null; });
    finish(lane, 100, true);
    Future<Void> transaction = onContext(lane::flush);
    assertThrows(java.util.concurrent.ExecutionException.class, () -> await(transaction));
    onContext(() -> {
      assertTrue(lane.requiresRecovery());
      assertEquals(100, tracker.committedNext());
      assertEquals(List.of(100L), starts);
      return null;
    });
  }

  @Test void revocationFencesLateCallbacksWithoutPrematurelyReleasingCapacity() throws Exception {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 10, 1000);
    var capacity = new CapacityGate(1);
    var lane = lane(tracker, capacity, 1, prefix -> Future.succeededFuture());
    onContext(() -> {
      lane.accept(List.of(record(100, "a")));
      lane.revoke();
      assertEquals(1, capacity.active());
      handlers.get(100L).complete(new PartitionLane.Outcome<>("late", 10));
      return null;
    });
    // The handler callback is queued before this context barrier, with no timing sleeps.
    onContext(() -> {
      assertEquals(0, capacity.active());
      assertEquals(ContiguousCompletionTracker.State.RUNNING, tracker.state(100));
      assertEquals(100, tracker.committedNext());
      return null;
    });
  }

  @Test void lanesSharePodCapacityAndRejectCallsFromWrongContext() throws Exception {
    var capacity = new CapacityGate(2);
    var first = lane(new ContiguousCompletionTracker<String>(1, 100, 10, 1000), capacity, 2, prefix -> Future.succeededFuture());
    var second = lane(new ContiguousCompletionTracker<String>(1, 200, 10, 1000), capacity, 2, prefix -> Future.succeededFuture());
    assertThrows(IllegalStateException.class, first::pump);
    onContext(() -> {
      first.accept(List.of(record(100, "a"), record(101, "b")));
      second.accept(List.of(record(200, "c")));
      assertEquals(List.of(100L, 101L), starts);
      assertEquals(2, capacity.active());
      return null;
    });
    finish(first, 100, true);
    onContext(() -> {
      second.pump();
      assertEquals(List.of(100L, 101L, 200L), starts);
      assertEquals(2, capacity.active());
      return null;
    });
  }
}
