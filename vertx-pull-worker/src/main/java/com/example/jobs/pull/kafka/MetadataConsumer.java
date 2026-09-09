package com.example.jobs.pull.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/** Native state is read only on the Vert.x Kafka consumer thread, then safely published. */
public final class MetadataConsumer extends KafkaConsumer<String,String> {
  private volatile ConsumerGroupMetadata metadata;
  private final Map<TopicPartition,AtomicBoolean> assignments = new ConcurrentHashMap<>();
  public MetadataConsumer(Map<String,Object> properties) { super(properties); }
  public ConsumerGroupMetadata metadataSnapshot() { return metadata; }
  public AtomicBoolean assignmentToken(TopicPartition partition) { return assignments.get(partition); }
  @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    super.subscribe(topics, new ConsumerRebalanceListener() {
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        invalidate(partitions); listener.onPartitionsRevoked(partitions);
      }
      public void onPartitionsLost(Collection<TopicPartition> partitions) {
        invalidate(partitions); listener.onPartitionsLost(partitions);
      }
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (var p : partitions) assignments.computeIfAbsent(p, ignored -> new AtomicBoolean(true));
        // Pause synchronously before the first poll can deliver newly assigned records.
        pause(partitions);
        metadata = groupMetadata();
        listener.onPartitionsAssigned(partitions);
      }
    });
  }
  private void invalidate(Collection<TopicPartition> partitions) {
    for (var p : partitions) {
      var token = assignments.remove(p);
      if (token != null) token.set(false);
    }
  }
  @Override public ConsumerRecords<String,String> poll(Duration timeout) {
    var records = super.poll(timeout);
    metadata = groupMetadata();
    return records;
  }
}
