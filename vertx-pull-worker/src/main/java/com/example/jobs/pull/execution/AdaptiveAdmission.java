package com.example.jobs.pull.execution;

import java.util.function.LongSupplier;

/** Bounded sample window, cooldown, stepped recovery and one half-open business probe. */
public final class AdaptiveAdmission {
  private final LongSupplier clock;
  private final TokenBucket bucket;
  private final double healthyRate;
  private final int minimumSamples;
  private final long cooldown;
  private int level;
  private int samples;
  private int failures;
  private int successes;
  private long changed;
  private boolean probe;
  private long generation;
  public record Permit(long generation, boolean probe) {}
  public AdaptiveAdmission(double rate, int burst, LongSupplier clock) {
    this(rate, burst, clock, 5, 30_000_000_000L);
  }
  public AdaptiveAdmission(double rate, int burst, LongSupplier clock, int minimumSamples, long cooldown) {
    if (minimumSamples < 1 || cooldown < 1) throw new IllegalArgumentException("Invalid adaptive policy");
    this.clock = clock; this.healthyRate = rate; this.minimumSamples = minimumSamples;
    this.cooldown = cooldown; this.bucket = new TokenBucket(rate, burst, clock); changed = clock.getAsLong();
  }
  public boolean acquire() { return acquirePermit() != null; }
  public Permit acquirePermit() {
    if (healthyRate == 0) return null;
    if (level == 3) {
      if (probe || clock.getAsLong() - changed < cooldown) return null;
      probe = true; return new Permit(generation,true);
    }
    return bucket.tryAcquire() ? new Permit(generation,false) : null;
  }
  public void outcome(Permit permit, boolean dependencyHealthy) {
    if (permit.generation() != generation || (level == 3 && !permit.probe())) return;
    outcome(dependencyHealthy);
  }
  public void outcome(boolean dependencyHealthy) {
    if (level == 3) {
      if (!probe) return;
      probe = false; changed = clock.getAsLong();
      if (dependencyHealthy) change(2);
      return;
    }
    samples++;
    if (!dependencyHealthy) { failures++; successes = 0; } else successes++;
    if (samples >= minimumSamples) {
      if (failures * 2 >= samples) change(Math.min(3, level + 1));
      else if (level > 0 && successes >= minimumSamples && clock.getAsLong() - changed >= cooldown) change(level - 1);
      samples = 0; failures = 0;
    }
  }
  private void change(int level) {
    generation++;
    this.level = level; changed = clock.getAsLong(); successes = 0;
    bucket.setRate(effectiveRate());
  }
  public double effectiveRate() { return healthyRate * switch(level) { case 0 -> 1; case 1 -> 0.5; case 2 -> 0.25; default -> 0; }; }
  public String state() { return switch(level) { case 0 -> "HEALTHY"; case 1 -> "DEGRADED"; case 2 -> "SEVERE"; default -> "OPEN"; }; }
}
