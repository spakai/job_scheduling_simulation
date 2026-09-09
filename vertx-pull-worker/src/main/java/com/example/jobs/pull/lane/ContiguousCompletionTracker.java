package com.example.jobs.pull.lane;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Context-confined delivery-order window. Completion is not a source acknowledgement. */
public final class ContiguousCompletionTracker<T> {
  public enum State { QUEUED, RUNNING, UNRESOLVED, COMPLETED, COMMITTING }

  public record Delivery(long offset, String owner, long bytes) {
    public Delivery {
      if (offset < 0 || offset == Long.MAX_VALUE || owner == null || owner.isBlank() || bytes < 0) {
        throw new IllegalArgumentException("Invalid delivery coordinates, owner, or byte size");
      }
    }
  }

  public record Execution(long epoch, long offset, long token) {}
  public record Completed<T>(long offset, String owner, T outcome) {}
  public record Prefix<T>(long epoch, long id, long nextOffset, List<Completed<T>> records) {
    public Prefix { records = List.copyOf(records); }
  }

  private static final class Entry<T> {
    final Delivery delivery;
    State state = State.QUEUED;
    long token;
    long outcomeBytes;
    T outcome;
    Entry(Delivery delivery) { this.delivery = delivery; }
  }

  private final long epoch;
  private final int maxRecords;
  private final long maxBytes;
  private final LinkedHashMap<Long, Entry<T>> entries = new LinkedHashMap<>();
  private long committedNext;
  private long lastDelivered;
  private long usedBytes;
  private long sequence;
  private boolean revoked;
  private Prefix<T> pending;

  public ContiguousCompletionTracker(long epoch, long committedNext, int maxRecords, long maxBytes) {
    if (epoch < 0 || committedNext < 0 || maxRecords < 1 || maxBytes < 1) {
      throw new IllegalArgumentException("Invalid tracker bounds or assignment");
    }
    this.epoch = epoch;
    this.committedNext = committedNext;
    this.lastDelivered = committedNext - 1;
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
  }

  /** Validate the entire delivered batch before mutation or any dispatch callback. */
  public void registerBatch(List<Delivery> batch) {
    requireActive();
    if (batch.size() > maxRecords - entries.size()) throw new IllegalStateException("Record window full");
    long previous = lastDelivered;
    long bytes = usedBytes;
    for (Delivery delivery : batch) {
      if (delivery.offset() <= previous || delivery.offset() < committedNext) {
        throw new IllegalArgumentException("Delivery offsets must increase within the assignment");
      }
      if (delivery.bytes() > maxBytes - bytes) throw new IllegalStateException("Byte window full");
      bytes += delivery.bytes();
      previous = delivery.offset();
    }
    for (Delivery delivery : batch) entries.put(delivery.offset(), new Entry<>(delivery));
    lastDelivered = previous;
    usedBytes = bytes;
  }

  public Execution start(long offset) {
    requireActive();
    Entry<T> entry = entry(offset);
    if (entry.state != State.QUEUED) throw new IllegalStateException("Record is not queued");
    entry.state = State.RUNNING;
    entry.token = ++sequence;
    return new Execution(epoch, offset, entry.token);
  }

  /** Only call after required completion EDR durability (or staged quarantine disposition). */
  public boolean complete(Execution execution, T outcome, long outcomeBytes) {
    if (!matches(execution)) return false;
    if (outcome == null || outcomeBytes < 0) throw new IllegalArgumentException("Invalid outcome");
    if (outcomeBytes > maxBytes - usedBytes) throw new IllegalStateException("Outcome window full; recovery required");
    Entry<T> entry = entry(execution.offset());
    entry.outcome = outcome;
    entry.outcomeBytes = outcomeBytes;
    usedBytes += outcomeBytes;
    entry.state = State.COMPLETED;
    return true;
  }

  /** Failure blocks the prefix and invalidates the failed invocation's completion token. */
  public boolean fail(Execution execution) {
    if (!matches(execution)) return false;
    entry(execution.offset()).state = State.UNRESOLVED;
    return true;
  }

  /** Recovery policy must reacquire EDR, rate and capacity admission before retrying. */
  public void requeue(long offset) {
    requireActive();
    Entry<T> entry = entry(offset);
    if (entry.state != State.UNRESOLVED) throw new IllegalStateException("Record is not unresolved");
    entry.state = State.QUEUED;
  }

  public Optional<Prefix<T>> preparePrefix(int batchRecords, long batchBytes) {
    requireActive();
    if (batchRecords < 1 || batchBytes < 1) throw new IllegalArgumentException("Invalid batch limits");
    if (pending != null) return Optional.empty();
    List<Completed<T>> completed = new ArrayList<>();
    long bytes = 0;
    for (Entry<T> entry : entries.values()) {
      if (entry.state != State.COMPLETED || completed.size() == batchRecords) break;
      if (entry.outcomeBytes > batchBytes - bytes) {
        if (completed.isEmpty()) throw new IllegalStateException("Outcome exceeds transaction batch limit");
        break;
      }
      completed.add(new Completed<>(entry.delivery.offset(), entry.delivery.owner(), entry.outcome));
      bytes += entry.outcomeBytes;
    }
    if (completed.isEmpty()) return Optional.empty();
    pending = new Prefix<>(epoch, ++sequence, completed.getLast().offset() + 1, completed);
    for (Completed<T> record : completed) entry(record.offset()).state = State.COMMITTING;
    return Optional.of(pending);
  }

  /** Retire only the exact snapshot after confirmed broker commit, never on submission. */
  public boolean committed(Prefix<T> prefix) {
    if (revoked || pending != prefix) return false;
    for (Completed<T> record : prefix.records()) {
      Entry<T> entry = entries.remove(record.offset());
      usedBytes -= entry.delivery.bytes() + entry.outcomeBytes;
    }
    committedNext = prefix.nextOffset();
    pending = null;
    return true;
  }

  /** Use only for a confirmed abort; ambiguity requires revocation and broker recovery. */
  public boolean aborted(Prefix<T> prefix) {
    if (revoked || pending != prefix) return false;
    for (Completed<T> record : prefix.records()) entry(record.offset()).state = State.COMPLETED;
    pending = null;
    return true;
  }

  public void revoke() { revoked = true; }
  public long committedNext() { return committedNext; }
  public int size() { return entries.size(); }
  public long usedBytes() { return usedBytes; }
  public State state(long offset) { return entry(offset).state; }
  public Map<Long, State> states() {
    Map<Long, State> result = new LinkedHashMap<>();
    entries.forEach((offset, entry) -> result.put(offset, entry.state));
    return Map.copyOf(result);
  }

  private boolean matches(Execution execution) {
    Entry<T> entry = entries.get(execution.offset());
    return !revoked && execution.epoch() == epoch && entry != null
        && entry.state == State.RUNNING && entry.token == execution.token();
  }

  private Entry<T> entry(long offset) {
    Entry<T> entry = entries.get(offset);
    if (entry == null) throw new IllegalArgumentException("Offset is not registered: " + offset);
    return entry;
  }

  private void requireActive() {
    if (revoked) throw new IllegalStateException("Assignment is revoked");
  }
}
