package com.example.jobs.pull.kafka;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/** One bounded, dedicated producer thread. Any transaction failure requires replacement/restoration. */
public final class TransactionAdapter {
  public record Output(String topic, int partition, String key, String value) {
    public Output {
      if (topic == null || topic.isBlank() || partition < 0 || key == null) {
        throw new IllegalArgumentException("Invalid output coordinates");
      }
    }
  }

  public record Command(List<Output> outputs, Map<TopicPartition, OffsetAndMetadata> offsets,
      ConsumerGroupMetadata group, BooleanSupplier assignmentCurrent) {
    public Command {
      outputs = List.copyOf(outputs);
      offsets = Map.copyOf(offsets);
      Objects.requireNonNull(assignmentCurrent);
      if (!offsets.isEmpty() && group == null) throw new IllegalArgumentException("Group metadata required");
      if (offsets.size() > 1) throw new IllegalArgumentException("Only one partition prefix per transaction");
    }
  }

  private final Context context;
  private final ThreadPoolExecutor executor;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final Duration timeout;
  private final int maxRecords;
  private final long maxBytes;
  private Producer<String, String> producer;
  private final Map<String, Producer<String, String>> writers = new java.util.HashMap<>();
  private boolean failed;
  private final java.util.concurrent.atomic.AtomicLong committed = new java.util.concurrent.atomic.AtomicLong();
  private final java.util.concurrent.atomic.AtomicLong aborted = new java.util.concurrent.atomic.AtomicLong();
  private final java.util.concurrent.atomic.AtomicLong failures = new java.util.concurrent.atomic.AtomicLong();
  private final java.util.concurrent.atomic.AtomicLong durationNanos = new java.util.concurrent.atomic.AtomicLong();
  public String metrics() {
    return "vtx_transaction_queue " + executor.getQueue().size() + "\n"
        + "vtx_transaction_active " + executor.getActiveCount() + "\n"
        + "vtx_transaction_committed_total " + committed.get() + "\n"
        + "vtx_transaction_aborted_total " + aborted.get() + "\n"
        + "vtx_transaction_failures_total " + failures.get() + "\n"
        + "vtx_transaction_duration_seconds_total " + durationNanos.get()/1e9 + "\n";
  }
  private volatile java.util.function.Consumer<String> failpoint = ignored -> {};
  public void failpoint(java.util.function.Consumer<String> hook) { failpoint = Objects.requireNonNull(hook); }

  public TransactionAdapter(Context context, int queueCapacity, Duration timeout) {
    this(context, queueCapacity, timeout, 500, 4 * 1024 * 1024);
  }

  public TransactionAdapter(Context context, int queueCapacity, Duration timeout,
      int maxRecords, long maxBytes) {
    this.context = Objects.requireNonNull(context);
    if (maxRecords < 1 || maxBytes < 1) throw new IllegalArgumentException("Invalid batch bounds");
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
    if (queueCapacity < 1 || timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("Invalid transaction bounds");
    }
    this.timeout = timeout;
    executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity), task -> new Thread(task, "kafka-transactions"),
        new ThreadPoolExecutor.AbortPolicy());
  }

  public Future<Void> initialize(Supplier<Producer<String, String>> factory) {
    return enqueue(() -> {
      if (producer != null) throw new IllegalStateException("Already initialized");
      try {
        producer = Objects.requireNonNull(factory.get());
        producer.initTransactions();
      } catch (Exception error) {
        failed = true;
        throw error;
      }
    });
  }

  public Future<Void> initializeWriter(String id, Supplier<Producer<String, String>> factory,
      BooleanSupplier current) {
    return enqueue(() -> {
      if (!current.getAsBoolean()) throw new IllegalStateException("Assignment revoked");
      var old = writers.remove(id);
      if (old != null) old.close(timeout);
      var writer = factory.get();
      writers.put(id, writer);
      writer.initTransactions();
      if (!current.getAsBoolean()) throw new IllegalStateException("Assignment revoked");
    });
  }

  public Future<Void> closeWriter(String id) {
    return enqueue(() -> {
      var writer = writers.remove(id);
      if (writer != null) writer.close(timeout);
    });
  }

  /** EDR-only commands use an empty offset map and never advance source progress. */
  public Future<Void> submit(Command command) { return submit(null, command); }

  public Future<Void> submit(String writerId, Command command) {
    if (command.outputs().size() > maxRecords) return Future.failedFuture("Transaction record limit exceeded");
    long bytes = 0;
    for (Output output : command.outputs()) {
      long size = output.topic().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
          + (long) output.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
          + (output.value() == null ? 0 : output.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length)
          + 128L; // Conservative record framing reservation; producer limits remain mandatory.
      if (size > maxBytes - bytes) return Future.failedFuture("Transaction byte limit exceeded");
      bytes += size;
    }
    return enqueue(() -> {
      Producer<String, String> producer = writerId == null ? this.producer : writers.get(writerId);
      if (producer == null || failed) throw new IllegalStateException("Producer requires recovery");
      requireAssignment(command);
      boolean begun = false;
      long started = System.nanoTime();
      try {
        failpoint.accept("before_begin");
        producer.beginTransaction();
        begun = true;
        List<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>> sends = new java.util.ArrayList<>();
        for (Output output : command.outputs()) {
          failpoint.accept("before_send:"+output.topic());
          sends.add(producer.send(new ProducerRecord<>(output.topic(), output.partition(), output.key(), output.value())));
        }
        long sendDeadline = System.nanoTime()+timeout.toNanos();
        for (var send : sends) send.get(Math.max(1,sendDeadline-System.nanoTime()),TimeUnit.NANOSECONDS);
        requireAssignment(command);
        if (!command.offsets().isEmpty()) producer.sendOffsetsToTransaction(command.offsets(), command.group());
        requireAssignment(command);
        failpoint.accept("before_commit");
        producer.commitTransaction();
        committed.incrementAndGet();
        failpoint.accept("after_commit");
      } catch (Exception error) {
        // Even successful abort cannot prove a timed-out commit did not commit. Never retry here.
        failed = true;
        failures.incrementAndGet();
        if (begun) {
          try { producer.abortTransaction(); aborted.incrementAndGet(); }
          catch (Exception abortError) { error.addSuppressed(abortError); }
        }
        throw error;
      } finally { durationNanos.addAndGet(System.nanoTime()-started); }
    });
  }

  private void requireAssignment(Command command) {
    if (!command.assignmentCurrent().getAsBoolean()) throw new IllegalStateException("Assignment revoked");
  }

  private interface Operation { void run() throws Exception; }

  private Future<Void> enqueue(Operation operation) {
    Promise<Void> result = Promise.promise();
    if (closing.get()) return Future.failedFuture("Adapter is closing");
    try {
      executor.execute(() -> {
        try {
          operation.run();
          context.runOnContext(ignored -> result.complete());
        } catch (Exception error) {
          context.runOnContext(ignored -> result.fail(error));
        }
      });
    } catch (java.util.concurrent.RejectedExecutionException error) {
      result.fail(error);
    }
    return result.future();
  }

  /** Drains accepted commands off the event loop, then closes the producer with a bounded timeout. */
  public Future<Void> close() {
    if (!closing.compareAndSet(false, true)) return Future.failedFuture("Already closing");
    Promise<Void> result = Promise.promise();
    executor.shutdown();
    Thread.ofPlatform().name("kafka-transaction-close").start(() -> {
      try {
        // Each queued command is bounded by the configured Kafka max.block.ms and send timeout.
        if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
          throw new java.util.concurrent.TimeoutException("Transaction drain deadline exceeded; replace worker");
        }
        for (var writer : writers.values()) writer.close(timeout);
        if (producer != null) producer.close(timeout);
        context.runOnContext(ignored -> result.complete());
      } catch (Exception error) {
        context.runOnContext(ignored -> result.fail(error));
      }
    });
    return result.future();
  }
}
