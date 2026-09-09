package com.example.jobs.pull.execution;

/** Shared by partition coordinators on one pod control context. */
public final class CapacityGate {
  private final int limit;
  private int active;

  public CapacityGate(int limit) {
    if (limit < 1) throw new IllegalArgumentException("Capacity must be positive");
    this.limit = limit;
  }

  public boolean tryAcquire() {
    if (active == limit) return false;
    active++;
    return true;
  }

  public void release() {
    if (active == 0) throw new IllegalStateException("Capacity released twice");
    active--;
  }

  public int active() { return active; }
}
