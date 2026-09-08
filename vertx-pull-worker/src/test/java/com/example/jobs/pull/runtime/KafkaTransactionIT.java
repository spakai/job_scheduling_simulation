package com.example.jobs.pull.runtime;

import static org.junit.jupiter.api.Assertions.*;
import com.example.jobs.pull.config.RuntimeConfig;
import com.example.jobs.pull.kafka.TransactionAdapter;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaTransactionIT {
  @Test void abortedPrefixIsInvisibleAndAmbiguousCommittedPrefixRequiresBrokerRecovery() throws Exception {
    String ns="tx-"+UUID.randomUUID()+"-";
    var config=RuntimeConfig.fromEnvironment(Map.of("TOPIC_NAMESPACE",ns,"HANDLER_URL","http://localhost:1",
        "KAFKA_BOOTSTRAP_SERVERS",System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")));
    String results=ns+"results", lifecycle=ns+"lifecycle", source=ns+"source";
    try(var admin=Admin.create(Map.of("bootstrap.servers",config.brokers()))) {
      admin.createTopics(List.of(new NewTopic(results,1,(short)1),new NewTopic(lifecycle,1,(short)1),new NewTopic(source,1,(short)1))).all().get();
      Vertx vertx=Vertx.vertx();
      var adapter=new TransactionAdapter(vertx.getOrCreateContext(),10,Duration.ofSeconds(10));
      try {
        KafkaRuntimeIT.await(adapter.initialize(() -> new KafkaProducer<>(config.producerProperties(ns+"slot"))));
        adapter.failpoint(point -> { if(point.equals("before_send:"+lifecycle)) throw new IllegalStateException("injected lifecycle send failure"); });
        var command=new TransactionAdapter.Command(List.of(new TransactionAdapter.Output(results,0,"job","result"),new TransactionAdapter.Output(lifecycle,0,"job","lifecycle")),
            Map.of(new TopicPartition(source,0),new OffsetAndMetadata(102)),new ConsumerGroupMetadata(config.group()),() -> true);
        assertThrows(Exception.class,() -> KafkaRuntimeIT.await(adapter.submit(command)));
        assertTrue(read(config,results).isEmpty()); assertTrue(read(config,lifecycle).isEmpty());
        assertTrue(admin.listConsumerGroupOffsets(config.group()).partitionsToOffsetAndMetadata().get().isEmpty());
        KafkaRuntimeIT.await(adapter.close());
        var replacement=new TransactionAdapter(vertx.getOrCreateContext(),10,Duration.ofSeconds(10));
        try {
          KafkaRuntimeIT.await(replacement.initialize(() -> new KafkaProducer<>(config.producerProperties(ns+"slot"))));
          replacement.failpoint(point -> { if(point.equals("after_commit")) throw new org.apache.kafka.common.errors.TimeoutException("lost acknowledgement"); });
          assertThrows(Exception.class,() -> KafkaRuntimeIT.await(replacement.submit(command)));
          assertEquals(1,read(config,results).size()); assertEquals(1,read(config,lifecycle).size());
          assertEquals(102,admin.listConsumerGroupOffsets(config.group()).partitionsToOffsetAndMetadata().get().get(new TopicPartition(source,0)).offset());
          assertThrows(Exception.class,() -> KafkaRuntimeIT.await(replacement.submit(command)));
        } finally { KafkaRuntimeIT.await(replacement.close()); }
      } finally { KafkaRuntimeIT.await(vertx.close()); admin.deleteTopics(List.of(results,lifecycle,source)).all().get(); }
    }
  }
  @Test void newPartitionWriterFencesFormerOwnersEdrOnlyTransaction() throws Exception {
    String ns="fence-"+UUID.randomUUID()+"-";
    var config=RuntimeConfig.fromEnvironment(Map.of("TOPIC_NAMESPACE",ns,"HANDLER_URL","http://localhost:1",
        "KAFKA_BOOTSTRAP_SERVERS",System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")));
    String topic=ns+"edr";
    try(var admin=Admin.create(Map.of("bootstrap.servers",config.brokers()))) {
      admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get();
      try(var old=new KafkaProducer<String,String>(config.producerProperties(ns+"partition-0"))) {
        old.initTransactions(); old.beginTransaction(); old.send(new ProducerRecord<>(topic,0,"job","stale")).get();
        try(var successor=new KafkaProducer<String,String>(config.producerProperties(ns+"partition-0"))) {
          successor.initTransactions();
          assertThrows(org.apache.kafka.common.KafkaException.class,old::commitTransaction);
          successor.beginTransaction(); successor.send(new ProducerRecord<>(topic,0,"job","completed")).get(); successor.commitTransaction();
          var records=read(config,topic); assertEquals(1,records.size()); assertEquals("completed",records.getFirst().value());
        }
      } finally { admin.deleteTopics(List.of(topic)).all().get(); }
    }
  }

  @Test void migrationGuardRejectsActiveAndUndrainedFleetBeforeAllowingCutover() throws Exception {
    String ns="migration-"+UUID.randomUUID()+"-";
    var config=RuntimeConfig.fromEnvironment(Map.of("TOPIC_NAMESPACE",ns,"HANDLER_URL","http://localhost:1",
        "KAFKA_BOOTSTRAP_SERVERS",System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")));
    String topic=ns+"legacy";
    try(var admin=Admin.create(config.adminProperties())) {
      admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get();
      KafkaRuntimeIT.awaitTopics(admin,List.of(topic),1);
      var props=config.producerProperties("unused"); props.remove("transactional.id");
      try(var producer=new KafkaProducer<String,String>(props)) { producer.send(new ProducerRecord<>(topic,"subscriber:42","request")).get(); }
      try(var consumer=new KafkaConsumer<String,String>(config.consumerProperties())) {
        consumer.subscribe(List.of(topic));
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(consumer.assignment().isEmpty()) {
          assertTrue(System.nanoTime()<deadline); consumer.poll(Duration.ofMillis(100));
        }
        assertThrows(IllegalStateException.class,() -> com.example.jobs.pull.kafka.MigrationGuard.requireDrained(admin,config.group(),topic));
      }
      assertThrows(IllegalStateException.class,() -> com.example.jobs.pull.kafka.MigrationGuard.requireDrained(admin,config.group(),topic));
      admin.alterConsumerGroupOffsets(config.group(),Map.of(new TopicPartition(topic,0),new OffsetAndMetadata(1))).all().get();
      com.example.jobs.pull.kafka.MigrationGuard.requireDrained(admin,config.group(),topic);
      admin.deleteTopics(List.of(topic)).all().get();
    }
  }
  static List<ConsumerRecord<String,String>> read(RuntimeConfig config,String topic) {
    var props=config.consumerProperties(); props.remove("group.id");
    try(var reader=new KafkaConsumer<String,String>(props)) {
      var tp=new TopicPartition(topic,0); reader.assign(List.of(tp)); reader.seekToBeginning(List.of(tp));
      long end=reader.endOffsets(List.of(tp)).get(tp); List<ConsumerRecord<String,String>> records=new ArrayList<>();
      long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
      while(reader.position(tp)<end) {
        if(System.nanoTime()>deadline) fail("read_committed deadline");
        reader.poll(Duration.ofMillis(100)).forEach(records::add);
      }
      return records;
    }
  }
}
