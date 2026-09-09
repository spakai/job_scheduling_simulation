package com.example.jobs.pull.kafka;

import static org.junit.jupiter.api.Assertions.*;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class TransactionAdapterTest {
  private static void await(Future<Void> future) throws Exception {
    future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @Test void serializesPrefixesAndEdrWithoutInventingOffsets() throws Exception {
    Vertx vertx = Vertx.vertx();
    var adapter = new TransactionAdapter(vertx.getOrCreateContext(), 8, Duration.ofSeconds(1));
    var producer = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
    try {
      await(adapter.initialize(() -> {
        assertFalse(io.vertx.core.Context.isOnEventLoopThread());
        return producer;
      }));
      var partition = new TopicPartition("subscriber-rerate", 0);
      var output = new TransactionAdapter.Output("results", 0, "job", "done");
      var first = adapter.submit(new TransactionAdapter.Command(List.of(output),
          Map.of(partition, new OffsetAndMetadata(102)), new ConsumerGroupMetadata("workers"), () -> true));
      var edr = adapter.submit(new TransactionAdapter.Command(List.of(output), Map.of(), null, () -> true));
      var second = adapter.submit(new TransactionAdapter.Command(List.of(output),
          Map.of(partition, new OffsetAndMetadata(105)), new ConsumerGroupMetadata("workers"), () -> true));
      await(first); await(edr); await(second);
      assertEquals(3, producer.commitCount());
      assertEquals(2, producer.consumerGroupOffsetsHistory().size());
      assertEquals(102, producer.consumerGroupOffsetsHistory().getFirst().get("workers").get(partition).offset());
      assertEquals(105, producer.consumerGroupOffsetsHistory().getLast().get("workers").get(partition).offset());
    } finally { await(adapter.close()); await(vertx.close()); }
  }

  @Test void failedCommitPoisonsAdapterAndNeverAcknowledgesNextCommand() throws Exception {
    Vertx vertx = Vertx.vertx();
    var adapter = new TransactionAdapter(vertx.getOrCreateContext(), 8, Duration.ofSeconds(1));
    var producer = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
    try {
      await(adapter.initialize(() -> producer));
      producer.commitTransactionException = new org.apache.kafka.common.errors.TimeoutException("ambiguous");
      var command = new TransactionAdapter.Command(List.of(new TransactionAdapter.Output("edr", 0, "job", "done")),
          Map.of(), null, () -> true);
      assertThrows(Exception.class, () -> await(adapter.submit(command)));
      assertThrows(Exception.class, () -> await(adapter.submit(command)));
      assertEquals(0, producer.commitCount());
      assertTrue(producer.transactionAborted());
      assertTrue(producer.history().isEmpty());
    } finally { await(adapter.close()); await(vertx.close()); }
  }

  @Test void revocationBetweenSendAndCommitAbortsOutputs() throws Exception {
    Vertx vertx = Vertx.vertx();
    var adapter = new TransactionAdapter(vertx.getOrCreateContext(), 8, Duration.ofSeconds(1));
    var producer = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
    AtomicBoolean firstCheck = new AtomicBoolean(true);
    try {
      await(adapter.initialize(() -> producer));
      var command = new TransactionAdapter.Command(List.of(new TransactionAdapter.Output("edr", 0, "job", "done")),
          Map.of(), null, () -> firstCheck.getAndSet(false));
      assertThrows(Exception.class, () -> await(adapter.submit(command)));
      assertTrue(producer.transactionAborted());
      assertTrue(producer.history().isEmpty());
    } finally { await(adapter.close()); await(vertx.close()); }
  }
}
