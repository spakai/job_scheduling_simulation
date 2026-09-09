package com.example.jobs.pull.execution;

import java.util.function.LongSupplier;

/** Context-confined monotonic pod-wide start limiter; callers schedule timers, never sleep. */
public final class TokenBucket {
  private final LongSupplier clock;
  private final double burst;
  private double rate;
  private double tokens;
  private long last;

  public TokenBucket(double rate, double burst, LongSupplier clock) {
    if (!Double.isFinite(rate) || rate < 0 || !Double.isFinite(burst) || burst < 1) {
      throw new IllegalArgumentException("Invalid TPS/burst");
    }
    this.clock = java.util.Objects.requireNonNull(clock);
    this.burst = burst;
    this.rate = rate;
    tokens = rate == 0 ? 0 : burst;
    last = clock.getAsLong();
  }

  public boolean tryAcquire() {
    refill();
    if (rate == 0 || tokens < 1) return false;
    tokens -= 1;
    return true;
  }

  public void setRate(double rate) {
    if (!Double.isFinite(rate) || rate < 0) throw new IllegalArgumentException("Invalid TPS");
    refill();
    this.rate = rate;
    if (rate == 0) tokens = 0;
  }

  public long delayNanos() {
    refill();
    if (rate == 0) return Long.MAX_VALUE;
    return tokens >= 1 ? 0 : (long) Math.ceil((1 - tokens) / rate * 1_000_000_000d);
  }

  private void refill() {
    long now = clock.getAsLong();
    long elapsed = now - last;
    if (elapsed < 0) throw new IllegalStateException("Clock must be monotonic");
    tokens = Math.min(burst, tokens + elapsed / 1_000_000_000d * rate);
    last = now;
  }
}
