package com.example.jobs.pull.runtime;

import static org.junit.jupiter.api.Assertions.*;
import com.example.jobs.pull.config.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in real Kafka volume acceptance. Compressed handler time does not prove production SLOs. */
@Tag("capacity")
class KafkaCapacityIT {
  @TempDir java.nio.file.Path state;

  @Test void fixedTenPartitionsFiveWorkersProcessConfiguredVolumes() throws Exception {
    for(String size:System.getProperty("spec007.capacity.counts","20000,100000").split(",")) run(Integer.parseInt(size));
  }
  private void run(int count) throws Exception {
    assertTrue(count>0);
    String namespace="capacity-"+UUID.randomUUID()+"-";
    int delay=Integer.getInteger("spec007.capacity.handlerMs",1);
    int timeout=Integer.getInteger("spec007.capacity.timeoutSeconds",1800);
    Map<String,String> env=new HashMap<>(Map.of("TOPIC_NAMESPACE",namespace,"HANDLER_URL","http://localhost:1",
        "KAFKA_BOOTSTRAP_SERVERS",System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"),
        "STATE_DIR",state.toString(),"RATE_LIMIT_TPS",System.getProperty("spec007.capacity.tps","1000"),
        "RATE_LIMIT_BURST","10","DRAIN_MS","1000"));
    RuntimeConfig base=RuntimeConfig.fromEnvironment(env);
    List<String> topics=new ArrayList<>(List.of(base.workTopic()));
    for(String kind:List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) topics.add(base.topic(kind));
    Vertx vertx=Vertx.vertx();
    List<PullRuntime> runtimes=new CopyOnWriteArrayList<>();
    List<Context> contexts=new ArrayList<>();
    Queue<Throwable> faults=new ConcurrentLinkedQueue<>();
    Set<String> owners=ConcurrentHashMap.newKeySet();
    AtomicInteger overlap=new AtomicInteger(), calls=new AtomicInteger(), active=new AtomicInteger(), peak=new AtomicInteger();
    long started=System.nanoTime();
    try(var admin=Admin.create(base.adminProperties())) {
      List<NewTopic> definitions=new ArrayList<>();
      for(String topic:topics) {
        var definition=new NewTopic(topic,10,(short)1);
        if(topic.equals(base.topic("execution-ledger")) || topic.equals(base.topic("finalization"))) definition.configs(Map.of("cleanup.policy","compact"));
        definitions.add(definition);
      }
      admin.createTopics(definitions).all().get(20,TimeUnit.SECONDS);
      KafkaRuntimeIT.awaitTopics(admin,topics,10);
      for(int slot=0;slot<5;slot++) {
        env.put("WORKER_SLOT","capacity-"+slot); RuntimeConfig config=RuntimeConfig.fromEnvironment(env);
        Context context=vertx.getOrCreateContext(); contexts.add(context);
        Promise<Void> ready=Promise.promise();
        context.runOnContext(v -> {
          var runtime=new PullRuntime(vertx,config,new WorkerConfig(0,10,10,"subscriber"),(job,attempt) -> {
            if(!owners.add(job.owner())) overlap.incrementAndGet();
            calls.incrementAndGet(); peak.accumulateAndGet(active.incrementAndGet(),Math::max);
            Promise<JsonObject> result=Promise.promise();
            vertx.setTimer(Math.max(1,delay),id -> {
              owners.remove(job.owner()); active.decrementAndGet(); result.complete(new JsonObject().put("compressed",true));
            });
            return result.future();
          },faults::add);
          runtimes.add(runtime); runtime.start().onComplete(ready);
        });
        KafkaRuntimeIT.await(ready.future());
      }
      var properties=base.producerProperties("unused"); properties.remove("transactional.id");
      try(var producer=new KafkaProducer<String,String>(properties)) {
        for(int i=0;i<count;i++) {
          String entity="owner-"+(i%500);
          JsonObject job=new JsonObject().put("schemaVersion",1).put("jobId",namespace+i).put("ownerId","subscriber:"+entity)
              .put("ownerType","SUBSCRIBER").put("correlationId",namespace).put("jobType","RERATE")
              .put("requestedAt",Instant.now().toString()).put("attempt",1).put("maxAttempts",3)
              .put("payload",new JsonObject().put("subscriberId",entity));
          producer.send(new ProducerRecord<>(base.workTopic(),entity,job.encode()));
        }
        producer.flush();
      }
      long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(timeout);
      while(true) {
        assertTrue(faults.isEmpty(),() -> "Runtime fault: "+faults.peek());
        long committed=admin.listConsumerGroupOffsets(base.group()).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS)
            .values().stream().mapToLong(o -> o.offset()).sum();
        if(committed==count) break;
        assertTrue(System.nanoTime()<deadline,"Capacity deadline, committed="+committed+"/"+count);
        Thread.sleep(250);
      }
      assertEquals(count,calls.get()); assertEquals(0,overlap.get()); assertTrue(peak.get()<=50);
      var readerProps=base.consumerProperties(); readerProps.remove("group.id");
      Set<String> results=new HashSet<>();
      try(var reader=new KafkaConsumer<String,String>(readerProps)) {
        List<TopicPartition> partitions=new ArrayList<>();
        for(int p=0;p<10;p++) partitions.add(new TopicPartition(base.topic("results"),p));
        reader.assign(partitions); reader.seekToBeginning(partitions);
        long readDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);
        while(results.size()<count) {
          assertTrue(System.nanoTime()<readDeadline,"Missing read_committed results");
          for(var record:reader.poll(Duration.ofMillis(100))) {
            assertTrue(results.add(record.key()),"Duplicate prefix result");
            assertEquals("SUCCEEDED",new JsonObject(record.value()).getString("status"));
          }
        }
      }
      double seconds=(System.nanoTime()-started)/1e9;
      JsonObject report=new JsonObject().put("requests",count).put("partitions",10).put("workers",5)
          .put("slotsPerWorker",10).put("handlerMs",delay).put("seconds",seconds).put("jobsPerSecond",count/seconds)
          .put("peakBusinessConcurrency",peak.get()).put("ownerOverlap",overlap.get()).put("readCommittedResults",results.size())
          .put("productionSloProof",false);
      Files.createDirectories(java.nio.file.Path.of("target","capacity-evidence"));
      Files.writeString(java.nio.file.Path.of("target","capacity-evidence",count+".json"),report.encodePrettily());
      System.out.println(report.encode());
    } finally {
      for(int i=0;i<runtimes.size();i++) {
        Promise<Void> closed=Promise.promise(); PullRuntime runtime=runtimes.get(i);
        contexts.get(i).runOnContext(v -> runtime.close().onComplete(closed)); KafkaRuntimeIT.await(closed.future());
      }
      KafkaRuntimeIT.await(vertx.close());
      try(var admin=Admin.create(base.adminProperties())) { admin.deleteTopics(topics).all().get(20,TimeUnit.SECONDS); }
    }
  }
}
