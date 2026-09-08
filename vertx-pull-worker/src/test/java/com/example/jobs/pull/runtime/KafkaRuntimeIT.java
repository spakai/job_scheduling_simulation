package com.example.jobs.pull.runtime;

import static org.junit.jupiter.api.Assertions.*;
import com.example.jobs.pull.config.*;
import com.example.jobs.pull.execution.BusinessHandler;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Real broker suite, enabled with mvn -Pkafka verify. Uses isolated names and no production dependencies. */
class KafkaRuntimeIT {
  @TempDir java.nio.file.Path state;
  Vertx vertx;
  Context context;
  RuntimeConfig config;
  Map<String,String> settings;
  PullRuntime runtime;
  final BlockingQueue<Throwable> faults = new LinkedBlockingQueue<>();
  final Map<String,Promise<JsonObject>> barriers = new ConcurrentHashMap<>();
  final List<String> starts = new CopyOnWriteArrayList<>();
  final AtomicInteger calls = new AtomicInteger();
  final String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092");

  static <T> T await(Future<T> future) throws Exception { return future.toCompletionStage().toCompletableFuture().get(45,TimeUnit.SECONDS); }
  <T> T context(Supplier<T> action) throws Exception {
    Promise<T> result = Promise.promise(); context.runOnContext(v -> { try { result.complete(action.get()); } catch(Throwable e) { result.fail(e); } }); return await(result.future());
  }
  @BeforeEach void setup() throws Exception {
    vertx = Vertx.vertx(); context = vertx.getOrCreateContext();
    settings = new HashMap<>(Map.ofEntries(Map.entry("TOPIC_NAMESPACE","it-"+UUID.randomUUID()+"-"),
        Map.entry("WORK_PARTITIONS","1"),Map.entry("KAFKA_BOOTSTRAP_SERVERS",brokers),Map.entry("HANDLER_URL","http://localhost:1"),
        Map.entry("RATE_LIMIT_TPS","1000"),Map.entry("RATE_LIMIT_BURST","10"),Map.entry("STATE_DIR",state.toString()),
        Map.entry("HANDLER_TIMEOUT_MS","1000"),Map.entry("ATTEMPT_LEASE_MS","31000"),Map.entry("DRAIN_MS","1000"),Map.entry("INTERNAL_RETRIES","0")));
    config = RuntimeConfig.fromEnvironment(settings);
    try(var admin = Admin.create(Map.of("bootstrap.servers",brokers))) {
      List<NewTopic> topics = new ArrayList<>(); topics.add(new NewTopic(config.workTopic(),1,(short)1));
      for(String kind:List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) {
        var topic = new NewTopic(config.topic(kind),1,(short)1);
        if(kind.equals("execution-ledger") || kind.equals("finalization")) topic.configs(Map.of("cleanup.policy","compact"));
        topics.add(topic);
      }
      admin.createTopics(topics).all().get(20,TimeUnit.SECONDS);
      awaitTopics(admin,topics.stream().map(NewTopic::name).toList(),1);
    }
  }
  static void awaitTopics(Admin admin,List<String> names,int partitions) throws Exception {
    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
    while(true) {
      try {
        var descriptions=admin.describeTopics(names).allTopicNames().get(5,TimeUnit.SECONDS);
        if(descriptions.values().stream().allMatch(t -> t.partitions().size()==partitions
            && t.partitions().stream().allMatch(p -> p.leader()!=null && p.leader().id()>=0))) {
          var resources=names.stream().map(n -> new org.apache.kafka.common.config.ConfigResource(
              org.apache.kafka.common.config.ConfigResource.Type.TOPIC,n)).toList();
          admin.describeConfigs(resources).all().get(5,TimeUnit.SECONDS);
          return;
        }
      } catch(java.util.concurrent.ExecutionException transientMetadata) {
        if(System.nanoTime()>=deadline) throw transientMetadata;
      }
      if(System.nanoTime()>=deadline) fail("Topic metadata propagation deadline");
      Thread.sleep(50);
    }
  }
  void start(BusinessHandler handler) throws Exception {
    runtime = context(() -> new PullRuntime(vertx,config,new WorkerConfig(0,5,5,"subscriber"),handler,faults::add));
    await(context(runtime::start));
    until(() -> context(runtime::ready));
  }
  interface Check { boolean get() throws Exception; }
  void until(Check check) throws Exception {
    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);
    while(!check.get()) {
      if(!faults.isEmpty()) throw new AssertionError("Runtime fault",faults.peek());
      if(System.nanoTime()>deadline) fail("Condition deadline; starts="+starts+" metrics="+context(runtime::metrics));
      Thread.sleep(25);
    }
  }
  JsonObject job(String id,String entity) {
    return new JsonObject().put("schemaVersion",1).put("jobId",id).put("ownerId","subscriber:"+entity).put("ownerType","SUBSCRIBER")
        .put("correlationId","c-"+id).put("jobType","RERATE").put("requestedAt",Instant.parse("2026-01-01T00:00:00Z").toString())
        .put("attempt",1).put("maxAttempts",3).put("payload",new JsonObject().put("subscriberId",entity));
  }
  void publish(String id,String entity) throws Exception {
    var props=config.producerProperties("unused"); props.remove("transactional.id");
    try(var producer = new KafkaProducer<String,String>(props)) { producer.send(new ProducerRecord<>(config.workTopic(),0,entity,job(id,entity).encode())).get(10,TimeUnit.SECONDS); }
  }
  long committed() throws Exception {
    try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))) {
      var offsets=admin.listConsumerGroupOffsets(config.group()).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);
      var offset=offsets.get(new TopicPartition(config.workTopic(),0)); return offset==null ? 0:offset.offset();
    }
  }
  List<ConsumerRecord<String,String>> read(String topic) {
    var props=config.consumerProperties(); props.put("group.id","observer-"+UUID.randomUUID());
    try(var reader = new KafkaConsumer<String,String>(props)) {
      var tp=new TopicPartition(topic,0); reader.assign(List.of(tp)); reader.seekToBeginning(List.of(tp));
      long end=reader.endOffsets(List.of(tp)).get(tp);
      List<ConsumerRecord<String,String>> result=new ArrayList<>();
      while(reader.position(tp)<end) reader.poll(Duration.ofMillis(100)).forEach(result::add);
      return result;
    }
  }
  @AfterEach void close() throws Exception {
    if(runtime!=null) await(context(runtime::close)); await(vertx.close());
  }
  @Test void completedBehindGapRestoresWithoutBusinessReplayAndOwnerWaitsForCommit() throws Exception {
    start((job,attempt) -> { starts.add(job.id()); calls.incrementAndGet(); var p=Promise.<JsonObject>promise(); barriers.put(job.id(),p); return p.future(); });
    publish("head","a"); publish("later","b"); publish("same-owner","a");
    until(() -> starts.size()==2);
    context(() -> { barriers.get("later").complete(new JsonObject().put("done",true)); return null; });
    until(() -> read(config.topic("execution-ledger")).stream().anyMatch(r -> r.value().contains("JOB_COMPLETED")));
    assertEquals(0,committed()); assertTrue(read(config.topic("results")).isEmpty());
    assertEquals(List.of("head","later"),starts);
    context(() -> { barriers.get("head").complete(new JsonObject()); return null; });
    until(() -> committed()>=2);
    until(() -> starts.contains("same-owner"));
    context(() -> { barriers.get("same-owner").complete(new JsonObject()); return null; });
    until(() -> committed()==3);
    await(context(runtime::close)); runtime=null;
    publish("later","b");
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==4);
    assertEquals(3,calls.get()); assertEquals(4,read(config.topic("results")).size());
    assertTrue(faults.isEmpty());
  }
  @Test void crashBehindGapRestoresCompletionAndReconcilesStartedLease() throws Exception {
    start((job,attempt) -> { calls.incrementAndGet(); starts.add(job.id()); var p=Promise.<JsonObject>promise(); barriers.put(job.id(),p); return p.future(); });
    publish("head","a"); publish("later","b");
    until(() -> barriers.size()==2);
    context(() -> { barriers.get("later").complete(new JsonObject().put("recovered",true)); return null; });
    until(() -> read(config.topic("execution-ledger")).stream().anyMatch(r -> r.value().contains("JOB_COMPLETED")));
    assertEquals(0,committed());
    await(context(runtime::close)); runtime=null;
    start((job,attempt) -> { calls.incrementAndGet(); starts.add("replay-"+job.id()); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==2);
    assertEquals(3,calls.get());
    assertFalse(starts.contains("replay-later"));
    assertEquals(2,read(config.topic("results")).size());
  }

  @Test void internalRetriesUseDistinctEdrThenSeparateWorkerRequeues() throws Exception {
    settings.put("INTERNAL_RETRIES","2"); settings.put("RETRY_BUDGET_MS","10000");
    config=RuntimeConfig.fromEnvironment(settings);
    start((job,attempt) -> {
      calls.incrementAndGet();
      return job.attempt()==1 ? Future.failedFuture(new BusinessHandler.Failure("HTTP_503",true)) : Future.succeededFuture(new JsonObject());
    });
    publish("retry-job","a"); until(() -> committed()==1);
    assertEquals(3,calls.get()); assertEquals(1,read(config.topic("retry")).size());
    var attempts=read(config.topic("edr")).stream().map(r -> new JsonObject(r.value()))
        .filter(e -> "ATTEMPT_STARTED".equals(e.getString("eventType"))).map(e -> e.getString("attemptId")).toList();
    assertEquals(3,new HashSet<>(attempts).size());
    Map<String,String> retrySettings=new HashMap<>(settings); retrySettings.put("RETRY_WORKER","true"); retrySettings.put("WORKER_SLOT","retry-0");
    var retryConfig=RuntimeConfig.fromEnvironment(retrySettings);
    var retryRuntime=context(() -> new PullRuntime(vertx,retryConfig,new WorkerConfig(0,2,2,"subscriber"),null,faults::add));
    try {
      await(context(retryRuntime::start)); until(() -> context(retryRuntime::ready)); until(() -> committed()==2);
      assertEquals(4,calls.get());
      assertEquals(1,read(config.topic("results")).stream().filter(r -> r.value().contains("SUCCEEDED")).count());
    } finally { await(context(retryRuntime::close)); }
  }

  @Test void invalidAndConflictingDuplicatesReachDlqWithoutAnotherBusinessCall() throws Exception {
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    publish("job","a"); until(() -> committed()==1);
    var props=config.producerProperties("unused"); props.remove("transactional.id");
    try(var producer=new KafkaProducer<String,String>(props)) {
      producer.send(new ProducerRecord<>(config.workTopic(),0,"a",job("job","a").put("payload",new JsonObject().put("changed",true)).encode())).get();
      producer.send(new ProducerRecord<>(config.workTopic(),0,"subscriber:a",job("legacy","a").encode())).get();
    }
    until(() -> committed()==3);
    assertEquals(1,calls.get()); assertEquals(2,read(config.topic("dlq")).size());
  }

  @Test void vtxOff05Failed102CannotAdvanceUntilQuarantineDispositionIsReleased() throws Exception {
    settings.put("HANDLER_TIMEOUT_MS","10000"); settings.put("ATTEMPT_LEASE_MS","40000"); config=RuntimeConfig.fromEnvironment(settings);
    var props=config.producerProperties("unused"); props.remove("transactional.id");
    try(var producer=new KafkaProducer<String,String>(props)) {
      for(int i=0;i<100;i++) producer.send(new ProducerRecord<>(config.workTopic(),0,"seed",job("seed-"+i,"seed").encode())).get();
    }
    try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))) {
      admin.alterConsumerGroupOffsets(config.group(),Map.of(new TopicPartition(config.workTopic(),0),new OffsetAndMetadata(100))).all().get();
    }
    start((job,attempt) -> { starts.add(job.id()); var p=Promise.<JsonObject>promise(); barriers.put(job.id(),p); return p.future(); });
    Promise<Void> disposition=Promise.promise();
    context(() -> { runtime.dispositionGate(job -> disposition.future()); return null; });
    for(int offset=100;offset<=105;offset++) publish(""+offset,offset==105 ? "102":""+offset);
    until(() -> barriers.size()==5);
    for(String id:List.of("100","101","103","104")) context(() -> { barriers.get(id).complete(new JsonObject()); return null; });
    until(() -> committed()==102);
    context(() -> { barriers.get("102").fail(new BusinessHandler.Failure("INJECTED",true)); return null; });
    until(() -> read(config.topic("edr")).stream().filter(r -> r.value().contains("JOB_COMPLETED")).count()==4);
    assertEquals(102,committed()); assertEquals(2,read(config.topic("results")).size());
    assertFalse(starts.contains("105"));
    context(() -> { disposition.complete(); return null; });
    until(() -> committed()==105);
    until(() -> starts.contains("105"));
    context(() -> { barriers.get("105").complete(new JsonObject()); return null; });
    until(() -> committed()==106);
    assertEquals(1,read(config.topic("retry")).size());
  }

  @Test void durableQuarantineBehindGapReplaysOutputsWithoutBusinessInvocation() throws Exception {
    settings.put("HANDLER_TIMEOUT_MS","10000"); settings.put("ATTEMPT_LEASE_MS","40000");
    config=RuntimeConfig.fromEnvironment(settings);
    start((job,attempt) -> {
      calls.incrementAndGet();
      if(job.id().equals("failed")) return Future.failedFuture(new BusinessHandler.Failure("HTTP_503",true));
      var p=Promise.<JsonObject>promise(); barriers.put(job.id(),p); return p.future();
    });
    publish("head","a"); publish("failed","b");
    until(() -> read(config.topic("edr")).stream().anyMatch(r -> r.value().contains("DISPOSITION_COMPLETED")));
    assertEquals(0,committed()); assertTrue(read(config.topic("retry")).isEmpty());
    await(context(runtime::close)); runtime=null;
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==2);
    assertEquals(3,calls.get()); assertEquals(1,read(config.topic("retry")).size());
    publish("failed","b"); until(() -> committed()==3);
    assertEquals(3,calls.get());
    var handoffs=read(config.topic("retry"));
    assertEquals(handoffs.get(0).value(),handoffs.get(1).value());
  }

  @Test void zeroTpsDoesNotCreateAttemptsOrAdvanceSource() throws Exception {
    settings.put("RATE_LIMIT_TPS","0"); config=RuntimeConfig.fromEnvironment(settings);
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    publish("paused","a");
    until(() -> context(runtime::metrics).contains("vtx_active_handlers 1"));
    // Round-trip through the broker after lane admission; no durable attempt should exist.
    assertTrue(read(config.topic("edr")).isEmpty());
    assertEquals(0,calls.get()); assertEquals(0,committed()); assertTrue(context(runtime::ready));
  }

  @Test void callbackFromForeignThreadReturnsToOwningContext() throws Exception {
    start((job,attempt) -> {
      var result=Promise.<JsonObject>promise();
      Thread.ofPlatform().start(() -> result.complete(new JsonObject().put("foreign",true)));
      return result.future();
    });
    publish("foreign","a"); until(() -> committed()==1);
    assertEquals(1,read(config.topic("results")).size()); assertTrue(faults.isEmpty());
  }

  @Test void onePodOverlapsWithinAndAcrossThreePartitionsUnderPodLimit() throws Exception {
    settings.put("WORK_PARTITIONS","3"); settings.put("HANDLER_TIMEOUT_MS","10000");
    settings.put("ATTEMPT_LEASE_MS","40000"); config=RuntimeConfig.fromEnvironment(settings);
    try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))) {
      Map<String,NewPartitions> changes=new HashMap<>(); changes.put(config.workTopic(),NewPartitions.increaseTo(3));
      for(String kind:List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) changes.put(config.topic(kind),NewPartitions.increaseTo(3));
      admin.createPartitions(changes).all().get();
      awaitTopics(admin,new ArrayList<>(changes.keySet()),3);
    }
    start((job,attempt) -> {
      starts.add(job.id()); var promise=Promise.<JsonObject>promise(); barriers.put(job.id(),promise); return promise.future();
    });
    var props=config.producerProperties("unused"); props.remove("transactional.id");
    try(var producer=new KafkaProducer<String,String>(props)) {
      for(int partition=0;partition<3;partition++) for(int i=0;i<2;i++) {
        String id=partition+"-"+i;
        producer.send(new ProducerRecord<>(config.workTopic(),partition,id,job(id,id).encode())).get();
      }
    }
    until(() -> barriers.size()==5);
    assertEquals(5,starts.size());
    assertEquals(3,starts.stream().map(id -> id.substring(0,1)).distinct().count());
    context(() -> { List.copyOf(barriers.values()).forEach(p -> p.tryComplete(new JsonObject())); return null; });
    until(() -> barriers.size()==6);
    context(() -> { barriers.values().forEach(p -> p.tryComplete(new JsonObject())); return null; });
    until(() -> {
      try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))) {
        var offsets=admin.listConsumerGroupOffsets(config.group()).partitionsToOffsetAndMetadata().get();
        return offsets.size()==3 && offsets.values().stream().allMatch(o -> o.offset()==2);
      }
    });
    assertTrue(faults.isEmpty());
  }

  @Test void executionVerticleUndeploymentTriggersRecoveryAndFencesLateSuccess() throws Exception {
    // The execution verticle deliberately loses its outstanding completion on undeployment.
    String address="execution-"+UUID.randomUUID();
    String deployment=await(vertx.deployVerticle(new VerticleBase() {
      @Override public Future<?> start() {
        return vertx.eventBus().<String>consumer(address,message -> {
          starts.add(message.body());
          var p=Promise.<JsonObject>promise(); barriers.put(message.body(),p);
          p.future().onSuccess(message::reply);
        }).completion();
      }
    }));
    start((job,attempt) -> vertx.eventBus().<JsonObject>request(address,job.id(),
        new io.vertx.core.eventbus.DeliveryOptions().setSendTimeout(60000)).map(message -> message.body()));
    publish("lost","a"); publish("durable","b");
    until(() -> barriers.size()==2);
    context(() -> { barriers.get("durable").complete(new JsonObject()); return null; });
    until(() -> read(config.topic("edr")).stream().anyMatch(r -> r.value().contains("JOB_COMPLETED")));
    await(vertx.undeploy(deployment));
    assertFalse(vertx.deploymentIDs().contains(deployment));
    assertNotNull(faults.poll(5,TimeUnit.SECONDS),"Lost completion must be detected by the deadline");
    assertEquals(0,committed()); assertTrue(read(config.topic("results")).isEmpty());
    context(() -> { barriers.get("lost").complete(new JsonObject().put("late",true)); return null; });
    assertEquals(0,committed());
    await(context(runtime::close)); runtime=null; faults.clear();
    start((job,attempt) -> { starts.add("replay-"+job.id()); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==2);
    assertTrue(starts.contains("replay-lost")); assertFalse(starts.contains("replay-durable"));
    assertEquals(2,read(config.topic("results")).size());
  }

  @Test void startEdrFailurePreventsBusinessInvocationAndReplacementRecovers() throws Exception {
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    context(() -> { runtime.transactionFailpoint(point -> {
      if(point.equals("before_send:"+config.topic("edr"))) throw new IllegalStateException("EDR unavailable");
    }); return null; });
    publish("start-failure","a");
    assertNotNull(faults.poll(10,TimeUnit.SECONDS));
    assertEquals(0,calls.get()); assertEquals(0,committed()); assertTrue(read(config.topic("edr")).isEmpty());
    await(context(runtime::close)); runtime=null; faults.clear();
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==1); assertEquals(1,calls.get());
  }

  @Test void prefixPublicationFailureLeavesEdrDurableAndRestoresWithoutReexecution() throws Exception {
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    context(() -> { runtime.transactionFailpoint(point -> {
      if(point.equals("before_send:"+config.topic("lifecycle"))) throw new IllegalStateException("Lifecycle unavailable");
    }); return null; });
    publish("prefix-failure","a");
    assertNotNull(faults.poll(10,TimeUnit.SECONDS));
    assertEquals(1,calls.get()); assertEquals(0,committed());
    assertTrue(read(config.topic("results")).isEmpty()); assertTrue(read(config.topic("lifecycle")).isEmpty());
    assertTrue(read(config.topic("edr")).stream().anyMatch(r -> r.value().contains("JOB_COMPLETED")));
    await(context(runtime::close)); runtime=null; faults.clear();
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    until(() -> committed()==1); assertEquals(1,calls.get()); assertEquals(1,read(config.topic("results")).size());
  }

  @Test void slowGroupFleetDoesNotConsumeSubscriberCapacity() throws Exception {
    settings.put("HANDLER_TIMEOUT_MS","10000"); settings.put("ATTEMPT_LEASE_MS","40000"); config=RuntimeConfig.fromEnvironment(settings);
    start((job,attempt) -> { calls.incrementAndGet(); return Future.succeededFuture(new JsonObject()); });
    Map<String,String> groupSettings=new HashMap<>(settings); groupSettings.put("WORKLOAD","group"); groupSettings.put("WORKER_SLOT","group-0");
    RuntimeConfig group=RuntimeConfig.fromEnvironment(groupSettings);
    List<NewTopic> topics=new ArrayList<>(List.of(new NewTopic(group.workTopic(),1,(short)1)));
    for(String kind:List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) {
      var topic=new NewTopic(group.topic(kind),1,(short)1);
      if(kind.equals("execution-ledger")) topic.configs(Map.of("cleanup.policy","compact"));
      topics.add(topic);
    }
    try(var admin=Admin.create(group.adminProperties())) {
      admin.createTopics(topics).all().get(); awaitTopics(admin,topics.stream().map(NewTopic::name).toList(),1);
    }
    Promise<JsonObject> slow=Promise.promise(); Promise<Void> invoked=Promise.promise();
    var groupRuntime=context(() -> new PullRuntime(vertx,group,new WorkerConfig(0,1,1,"group"),
        (job,attempt) -> { invoked.tryComplete(); return slow.future(); },faults::add));
    try {
      await(context(groupRuntime::start)); until(() -> context(groupRuntime::ready));
      var props=group.producerProperties("unused"); props.remove("transactional.id");
      try(var producer=new KafkaProducer<String,String>(props)) {
        var request=job("slow-group","g").put("ownerId","group:g").put("ownerType","GROUP")
            .put("payload",new JsonObject().put("groupId","g"));
        producer.send(new ProducerRecord<>(group.workTopic(),"g",request.encode())).get();
      }
      await(invoked.future()); publish("subscriber","s"); until(() -> committed()==1);
      assertEquals(1,calls.get()); assertFalse(slow.future().isComplete()); assertTrue(context(runtime::ready));
      context(() -> { slow.complete(new JsonObject()); return null; });
    } finally { await(context(groupRuntime::close)); }
  }

}
