package com.example.jobs.pull.execution;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketTest {
  @Test void fractionalRateAndBurstAreSharedWithoutBlocking() {
    AtomicLong clock = new AtomicLong();
    var bucket = new TokenBucket(0.5, 2, clock::get);
    assertTrue(bucket.tryAcquire());
    assertTrue(bucket.tryAcquire());
    assertFalse(bucket.tryAcquire());
    assertEquals(2_000_000_000L, bucket.delayNanos());
    clock.set(1_000_000_000L);
    assertFalse(bucket.tryAcquire());
    clock.set(2_000_000_000L);
    assertTrue(bucket.tryAcquire());
    clock.set(100_000_000_000L);
    assertTrue(bucket.tryAcquire());
    assertTrue(bucket.tryAcquire());
    assertFalse(bucket.tryAcquire());
  }

  @Test void administrativePauseDiscardsPermitsAndNeverAllowsProbes() {
    AtomicLong clock = new AtomicLong();
    var bucket = new TokenBucket(2, 2, clock::get);
    bucket.setRate(0);
    clock.set(10_000_000_000L);
    assertFalse(bucket.tryAcquire());
    assertEquals(Long.MAX_VALUE, bucket.delayNanos());
    bucket.setRate(0.5);
    assertFalse(bucket.tryAcquire());
    clock.addAndGet(2_000_000_000L);
    assertTrue(bucket.tryAcquire());
  }
}
