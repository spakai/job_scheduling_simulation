package com.example.jobs.pull.lane;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ContiguousCompletionTrackerTest {
  private static ContiguousCompletionTracker.Delivery delivery(long offset) {
    return new ContiguousCompletionTracker.Delivery(offset, "owner-" + offset, 10);
  }

  @Test void failed102BlocksCompleted103And104UntilSafeResolution() {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 10, 1000);
    tracker.registerBatch(List.of(delivery(100), delivery(101), delivery(102), delivery(103), delivery(104), delivery(105)));
    var tokens = new ArrayList<ContiguousCompletionTracker.Execution>();
    for (long offset = 100; offset <= 104; offset++) tokens.add(tracker.start(offset));
    for (int index : List.of(0, 1, 3, 4)) assertTrue(tracker.complete(tokens.get(index), "durable outcome", 10));
    assertTrue(tracker.fail(tokens.get(2)));
    assertFalse(tracker.complete(tokens.get(2), "late success", 10));
    var first = tracker.preparePrefix(10, 1000).orElseThrow();
    assertEquals(102, first.nextOffset());
    assertEquals(100, tracker.committedNext(), "submission is not confirmation");
    assertTrue(tracker.committed(first));
    assertEquals(102, tracker.committedNext());
    assertTrue(tracker.preparePrefix(10, 1000).isEmpty());
    assertEquals(ContiguousCompletionTracker.State.COMPLETED, tracker.state(104));
    tracker.requeue(102);
    assertTrue(tracker.complete(tracker.start(102), "durable recovery outcome", 10));
    var second = tracker.preparePrefix(10, 1000).orElseThrow();
    assertEquals(List.of(102L, 103L, 104L), second.records().stream().map(ContiguousCompletionTracker.Completed::offset).toList());
    assertEquals(105, second.nextOffset());
    assertTrue(tracker.committed(second));
    assertEquals(ContiguousCompletionTracker.State.QUEUED, tracker.state(105));
  }

  @Test void sparseOffsetsAreContiguousInDeliveryOrderAndSnapshotsDoNotGrow() {
    var tracker = new ContiguousCompletionTracker<String>(2, 100, 10, 1000);
    tracker.registerBatch(List.of(delivery(100), delivery(103), delivery(108)));
    tracker.complete(tracker.start(100), "a", 1);
    var first = tracker.preparePrefix(5, 1000).orElseThrow();
    tracker.complete(tracker.start(103), "b", 1);
    tracker.complete(tracker.start(108), "c", 1);
    assertEquals(101, first.nextOffset());
    assertTrue(tracker.preparePrefix(5, 1000).isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> first.records().clear());
    tracker.committed(first);
    var second = tracker.preparePrefix(5, 1000).orElseThrow();
    assertEquals(109, second.nextOffset());
  }

  @Test void revokeRejectsLateCompletionAndCommitConfirmation() {
    var tracker = new ContiguousCompletionTracker<String>(3, 100, 4, 100);
    tracker.registerBatch(List.of(delivery(100), delivery(101)));
    tracker.complete(tracker.start(100), "done", 1);
    var running = tracker.start(101);
    var prefix = tracker.preparePrefix(4, 100).orElseThrow();
    tracker.revoke();
    assertFalse(tracker.complete(running, "late", 1));
    assertFalse(tracker.committed(prefix));
    assertEquals(100, tracker.committedNext());
  }

  @Test void failedOrOversizedBatchNeverPartiallyRegistersAndOutcomesRemainBounded() {
    var tracker = new ContiguousCompletionTracker<String>(1, 100, 3, 25);
    assertThrows(IllegalArgumentException.class, () -> tracker.registerBatch(List.of(delivery(100), delivery(99))));
    assertEquals(0, tracker.size());
    assertThrows(IllegalStateException.class, () -> tracker.registerBatch(List.of(delivery(100), delivery(101), delivery(102))));
    assertEquals(0, tracker.size());
    tracker.registerBatch(List.of(delivery(100), delivery(101)));
    var token = tracker.start(101);
    assertThrows(IllegalStateException.class, () -> tracker.complete(token, "too large", 6));
    assertEquals(20, tracker.usedBytes());
    tracker.complete(token, "fits", 5);
    assertEquals(25, tracker.usedBytes());
    assertTrue(tracker.preparePrefix(3, 25).isEmpty());
  }

  @Test void abortedPrefixCanRetryButCannotRegressOrExceedBatchLimits() {
    var tracker = new ContiguousCompletionTracker<String>(1, 0, 4, 100);
    tracker.registerBatch(List.of(delivery(0), delivery(1), delivery(2)));
    for (long offset = 0; offset < 3; offset++) tracker.complete(tracker.start(offset), "done", 10);
    var first = tracker.preparePrefix(3, 20).orElseThrow();
    assertEquals(2, first.nextOffset());
    assertTrue(tracker.aborted(first));
    assertEquals(0, tracker.committedNext());
    var retry = tracker.preparePrefix(1, 20).orElseThrow();
    assertFalse(tracker.committed(first));
    assertTrue(tracker.committed(retry));
    assertEquals(1, tracker.committedNext());
    assertEquals(40, tracker.usedBytes());
  }

  @Test void randomizedCompletionOrdersNeverSkipUnfinishedWork() {
    for (int seed = 0; seed < 100; seed++) {
      var tracker = new ContiguousCompletionTracker<Integer>(seed, 0, 32, 10000);
      var deliveries = new ArrayList<ContiguousCompletionTracker.Delivery>();
      var order = new ArrayList<Integer>();
      for (int i = 0; i < 32; i++) { deliveries.add(delivery(i)); order.add(i); }
      tracker.registerBatch(deliveries);
      var tokens = deliveries.stream().map(d -> tracker.start(d.offset())).toList();
      Collections.shuffle(order, new Random(seed));
      boolean[] completed = new boolean[32];
      int expected = 0;
      for (int offset : order) {
        tracker.complete(tokens.get(offset), offset, 1);
        completed[offset] = true;
        while (expected < 32 && completed[expected]) expected++;
        var prefix = tracker.preparePrefix(32, 10000);
        if (prefix.isPresent()) tracker.committed(prefix.get());
        assertEquals(expected, tracker.committedNext(), "seed=" + seed);
      }
    }
  }
}
