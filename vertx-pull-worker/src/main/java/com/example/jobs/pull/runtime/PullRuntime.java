package com.example.jobs.pull.runtime;

import com.example.jobs.pull.config.RuntimeConfig;
import com.example.jobs.pull.config.WorkerConfig;
import com.example.jobs.pull.contract.Job;
import com.example.jobs.pull.execution.*;
import com.example.jobs.pull.kafka.*;
import com.example.jobs.pull.lane.*;
import com.example.jobs.pull.ledger.LedgerStore;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.common.TopicPartition;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;

/** One consumer control plane. All lane and admission state is confined to its Vert.x context. */
public final class PullRuntime {
  private final Vertx vertx;
  private final Context context;
  private final RuntimeConfig config;
  private final WorkerConfig slots;
  private final BusinessHandler handler;
  private java.util.function.Function<Job,Future<Void>> dispositionGate = job -> Future.succeededFuture();
  public void dispositionGate(java.util.function.Function<Job,Future<Void>> gate) { dispositionGate=Objects.requireNonNull(gate); }
  private final java.util.function.Consumer<Throwable> fatal;
  private final CapacityGate capacity;
  private final AdaptiveAdmission admission;
  private final WorkerExecutor storage;
  private final Map<TopicPartition,Lane> lanes = new LinkedHashMap<>();
  private final Map<String,Long> counters = new TreeMap<>();
  private TransactionAdapter transactions;
  private MetadataConsumer nativeConsumer;
  private KafkaConsumer<String,String> consumer;
  private long epoch;
  private long lastSweep;
  private boolean sweeping;
  private long timer;
  private Future<Void> closing;
  private boolean polling;
  private boolean stopping;
  private volatile boolean failed;
  private int rotation;
  private long lastTick = System.nanoTime();
  private long eventLoopDelay;

  private final class Lane {
    final TopicPartition tp;
    final AtomicBoolean token;
    final long epochId = ++epoch;
    final Map<Long,KafkaConsumerRecord<String,String>> records = new LinkedHashMap<>();
    final Map<Long,String> jobIds = new HashMap<>();
    final Map<Long,Integer> attempts = new HashMap<>();
    final Map<Long,Long> firstStarted = new HashMap<>();
    LedgerStore ledger;
    ContiguousCompletionTracker<List<TransactionAdapter.Output>> tracker;
    PartitionLane<List<TransactionAdapter.Output>> coordinator;
    boolean restoring = true;
    boolean paused = true;
    boolean flushing;
    Lane(TopicPartition tp, AtomicBoolean token) { this.tp = tp; this.token = token; }
    boolean current() { return token.get() && !failed; }
  }

  public PullRuntime(Vertx vertx, RuntimeConfig config, WorkerConfig slots, BusinessHandler handler,
      java.util.function.Consumer<Throwable> fatal) {
    this.vertx = vertx; this.context = Vertx.currentContext(); this.config = config; this.slots = slots;
    this.handler = handler; this.fatal = fatal; this.capacity = new CapacityGate(slots.podSlots());
    this.admission = new AdaptiveAdmission(config.tps(),config.burst(),System::nanoTime,
        config.adaptiveSamples(),java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.adaptiveCooldownMs()));
    this.storage = vertx.createSharedWorkerExecutor("ledger-storage",2,6,java.util.concurrent.TimeUnit.MINUTES);
  }
  private <T> Future<T> confined(Future<T> future) {
    Promise<T> promise = Promise.promise();
    future.onComplete(done -> context.runOnContext(v -> promise.handle(done)));
    return promise.future();
  }
  private void count(String name) { counters.merge(name,1L,Long::sum); }
  public boolean ready() { return !stopping && !failed && !lanes.isEmpty() && lanes.values().stream().noneMatch(l -> l.restoring); }
  public String metrics() {
    StringBuilder out = new StringBuilder();
    counters.forEach((name,value) -> out.append("vtx_").append(name).append("_total ").append(value).append('\n'));
    out.append("vtx_assigned_partitions ").append(lanes.size()).append('\n');
    out.append("vtx_active_handlers ").append(capacity.active()).append('\n');
    out.append("vtx_effective_tps ").append(admission.effectiveRate()).append('\n');
    out.append("vtx_event_loop_delay_seconds ").append(eventLoopDelay / 1e9).append('\n');
    out.append("vtx_restoring_partitions ").append(lanes.values().stream().filter(l -> l.restoring).count()).append('\n');
    out.append("vtx_paused_partitions ").append(lanes.values().stream().filter(l -> l.paused).count()).append('\n');
    out.append("vtx_adaptive_state{state=\"").append(admission.state()).append("\"} 1\n");
    if(transactions != null) out.append(transactions.metrics());
    for (var lane : lanes.values()) if (lane.tracker != null) {
      String label = "{partition=\""+lane.tp.getPartition()+"\"}";
      out.append("vtx_tracking_records").append(label).append(' ').append(lane.tracker.size()).append('\n');
      out.append("vtx_tracking_bytes").append(label).append(' ').append(lane.tracker.usedBytes()).append('\n');
      out.append("vtx_committed_next").append(label).append(' ').append(lane.tracker.committedNext()).append('\n');
      out.append("vtx_assignment_epoch").append(label).append(' ').append(lane.epochId).append('\n');
      for(var state : ContiguousCompletionTracker.State.values()) {
        out.append("vtx_lane_records{partition=\"").append(lane.tp.getPartition()).append("\",state=\"")
            .append(state).append("\"} ").append(lane.tracker.states().values().stream().filter(value -> value == state).count()).append('\n');
      }
      long oldest=lane.records.values().stream().mapToLong(r -> r.timestamp()).min().orElse(System.currentTimeMillis());
      out.append("vtx_oldest_record_age_seconds").append(label).append(' ').append(Math.max(0,System.currentTimeMillis()-oldest)/1000.0).append('\n');
    }
    return out.toString();
  }
  public Future<Void> start() {
    transactions = new TransactionAdapter(context, Math.max(32,config.partitions()*4), Duration.ofSeconds(15));
    return storage.<Void>executeBlocking(() -> { validateTopics(); return null; },true)
        .compose(v -> transactions.initialize(() -> new KafkaProducer<>(config.producerProperties(config.transactionId()))))
        .compose(v -> storage.executeBlocking(() -> new MetadataConsumer(config.consumerProperties()),true))
        .compose(nativeClient -> {
          nativeConsumer = nativeClient;
          consumer = KafkaConsumer.create(vertx,nativeClient);
          consumer.partitionsAssignedHandler(p -> context.runOnContext(v -> assigned(p)))
              .partitionsRevokedHandler(p -> context.runOnContext(v -> revoked(p)))
              .exceptionHandler(e -> context.runOnContext(v -> fail(e)));
          return confined(consumer.subscribe(config.sourceTopic()));
        }).onSuccess(v -> timer = vertx.setPeriodic(25, ignored -> tick()));
  }
  // Package-scoped acceptance hook; never exposed through management HTTP.
  void transactionFailpoint(java.util.function.Consumer<String> hook) { transactions.failpoint(hook); }
  private void validateTopics() throws Exception {
    try (var admin = org.apache.kafka.clients.admin.Admin.create(config.adminProperties())) {
      String legacyGroup=System.getenv("LEGACY_GROUP_ID");
      if(legacyGroup!=null && !legacyGroup.isBlank()) MigrationGuard.requireDrained(admin,legacyGroup,System.getenv().getOrDefault("LEGACY_WORK_TOPIC","job-requests.v1"));
      List<String> names = new ArrayList<>(List.of(config.workTopic()));
      for (String kind : List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) names.add(config.topic(kind));
      var descriptions = admin.describeTopics(names).allTopicNames().get(10,java.util.concurrent.TimeUnit.SECONDS);
      for (var description : descriptions.values()) if (description.partitions().size() != config.partitions()) {
        throw new IllegalArgumentException("Topic partition mismatch: " + description.name());
      }
      var resource = new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,config.topic("execution-ledger"));
      var ledgerConfig = admin.describeConfigs(List.of(resource)).all().get(10,java.util.concurrent.TimeUnit.SECONDS).get(resource);
      if (!"compact".equals(ledgerConfig.get("cleanup.policy").value())) throw new IllegalArgumentException("Ledger must be compact-only");
    }
  }
  private void assigned(Set<TopicPartition> partitions) {
    for (var tp : partitions) {
      if (lanes.containsKey(tp)) {
        Lane retained=lanes.get(tp);
        if (!retained.restoring && !retained.paused && !stopping) consumer.resume(tp).onFailure(this::fail);
        continue;
      }
      var token = nativeConsumer.assignmentToken(nativeTp(tp));
      if (token == null || !token.get() || stopping) continue;
      Lane lane = new Lane(tp,token); lanes.put(tp,lane); count("assignments");
      Future<Void> restore;
      if (config.retryWorker()) restore = storage.<Void>executeBlocking(() -> {
        lane.ledger = new LedgerStore(Path.of(config.stateDir(),config.slot(),"p"+tp.getPartition()),256L*1024*1024);
        lane.ledger.restore(config,tp.getPartition(),lane::current,config.topic("finalization")); return null;
      },true);
      else restore = transactions.initializeWriter(config.writerId(tp.getPartition()),
          () -> new KafkaProducer<>(config.producerProperties(config.writerId(tp.getPartition()))),lane::current)
          .compose(v -> storage.<Void>executeBlocking(() -> {
            lane.ledger = new LedgerStore(Path.of(config.stateDir(),config.slot(),"p"+tp.getPartition()),256L*1024*1024);
            lane.ledger.restore(config,tp.getPartition(),lane::current); return null;
          },true));
      restore.compose(v -> confined(consumer.committed(tp))).onSuccess(committed -> {
        if (!lane.current()) return;
        long next = committed == null || committed.getOffset() < 0 ? 0 : committed.getOffset();
        lane.tracker = new ContiguousCompletionTracker<>(lane.epochId,next,config.windowRecords(),config.windowBytes());
        lane.coordinator = new PartitionLane<>(context,lane.tracker,capacity,slots.partitionSlots(),20,3*1024*1024,
            delivery -> execute(lane,delivery),prefix -> commit(lane,prefix),this::pump);
        lane.restoring = false; lane.paused = false;
        consumer.resume(tp).onFailure(this::fail); count("restored");
      }).onFailure(error -> { if (lane.current()) fail(error); });
    }
  }
  private void revoked(Set<TopicPartition> partitions) {
    for (var tp : partitions) {
      Lane lane = lanes.remove(tp);
      if (lane == null) continue;
      lane.token.set(false);
      if (lane.coordinator != null) lane.coordinator.revoke();
      if (!config.retryWorker()) transactions.closeWriter(config.writerId(tp.getPartition())).onFailure(this::fail);
      if(lane.ledger!=null) storage.<Void>executeBlocking(() -> { lane.ledger.close(); return null; },true);
      count("revocations");
    }
  }
  private void tick() {
    if (Vertx.currentContext() != context) { context.runOnContext(v -> tick()); return; }
    long now = System.nanoTime(); eventLoopDelay = Math.max(0,now-lastTick-25_000_000); lastTick = now;
    if (failed) return;
    pump();
    if (!stopping && !config.retryWorker() && !sweeping && now-lastSweep > 1_000_000_000L) sweep(now);
    int podRecords=lanes.values().stream().filter(l -> l.tracker!=null).mapToInt(l -> l.tracker.size()).sum();
    long podBytes=lanes.values().stream().filter(l -> l.tracker!=null).mapToLong(l -> l.tracker.usedBytes()).sum();
    boolean podHigh=podRecords>=2*config.windowRecords()-config.pollRecords() || podBytes>=config.windowBytes();
    boolean podLow=podRecords<config.windowRecords() && podBytes<config.windowBytes()/2;
    for (Lane lane : List.copyOf(lanes.values())) {
      if (lane.restoring || !lane.current()) continue;
      if (!lane.flushing) {
        lane.flushing = true;
        lane.coordinator.flush().onComplete(done -> { lane.flushing = false; if (done.failed() && lane.current()) fail(done.cause()); });
      }
      boolean high = stopping || podHigh || lane.tracker.size() >= config.windowRecords()-config.pollRecords()
          || lane.tracker.usedBytes() >= config.windowBytes()/2;
      boolean low = !stopping && podLow && lane.tracker.size() < config.windowRecords()/2 && lane.tracker.usedBytes() < config.windowBytes()/4;
      if (high && !lane.paused) { lane.paused = true; consumer.pause(lane.tp).onFailure(this::fail); }
      else if (low && lane.paused) { lane.paused = false; consumer.resume(lane.tp).onFailure(this::fail); }
    }
    if (!polling && !stopping) {
      polling = true;
      confined(consumer.poll(Duration.ofMillis(50))).onComplete(done -> {
        polling = false;
        if (done.failed()) { fail(done.cause()); return; }
        if (stopping || failed) return;
        Map<Lane,List<ContiguousCompletionTracker.Delivery>> batches = new LinkedHashMap<>();
        try {
          for (int i=0;i<done.result().size();i++) {
            var record = done.result().recordAt(i);
            Lane lane = lanes.get(new TopicPartition(record.topic(),record.partition()));
            if (lane == null || !lane.current() || lane.restoring) throw new IllegalStateException("Delivery before restoration");
            long bytes = utf8(record.value()) + utf8(record.key());
            if (bytes > config.maxRecordBytes()) throw new IllegalStateException("Oversize delivery; operator quarantine required");
            lane.records.put(record.offset(),record);
            String owner = record.key() == null || record.key().isBlank() ? "invalid-"+record.offset() : record.key();
            batches.computeIfAbsent(lane,ignored -> new ArrayList<>()).add(new ContiguousCompletionTracker.Delivery(record.offset(),owner,bytes));
            count("fetched");
          }
          int retained=lanes.values().stream().filter(l -> l.tracker!=null).mapToInt(l -> l.tracker.size()).sum();
          if(retained+done.result().size()>2*config.windowRecords()) throw new IllegalStateException("Pod tracking window full; fenced replay required");
          // Each partition batch is registered atomically before its asynchronous callbacks can run.
          for (var entry : batches.entrySet()) entry.getKey().coordinator.accept(entry.getValue());
        } catch (Exception error) { fail(error); }
      });
    }
  }
  private void sweep(long now) {
    lastSweep=now;
    var candidates=lanes.values().stream().filter(l -> !l.restoring && l.current()).toList();
    if(candidates.isEmpty()) return;
    Lane lane=candidates.get((int)((now/1_000_000_000L)%candidates.size()));
    sweeping=true;
    storage.executeBlocking(() -> lane.ledger.expired(System.currentTimeMillis(),100),true).compose(keys -> {
      Future<Void> chain=Future.succeededFuture();
      for(String key:keys) {
        chain=chain.compose(v -> storage.executeBlocking(() -> lane.ledger.get(key),true).compose(state -> {
          if(!lane.current() || state == null || lane.jobIds.containsValue(state.getString("jobId"))) return Future.succeededFuture();
          return persist(lane,key,null,new JsonObject().put("eventType","EXPIRED").put("jobId",state.getString("jobId")));
        }));
      }
      return chain;
    }).onComplete(done -> { sweeping=false; if(done.failed() && lane.current()) fail(done.cause()); });
  }
  private void pump() {
    if (Vertx.currentContext() != context) { context.runOnContext(v -> pump()); return; }
    if (stopping || failed || lanes.isEmpty()) return;
    List<Lane> fair = new ArrayList<>(lanes.values());
    Collections.rotate(fair,rotation++ % fair.size());
    for (Lane lane : fair) if (!lane.restoring && lane.current()) lane.coordinator.pump();
  }
  private Future<PartitionLane.Outcome<List<TransactionAdapter.Output>>> execute(Lane lane, ContiguousCompletionTracker.Delivery delivery) {
    var record = lane.records.get(delivery.offset());
    if (config.retryWorker()) return requeue(lane,record);
    final Job job;
    try { job = Job.parse(record.value(),record.key(),config.workload()); }
    catch (Exception invalid) { count("invalid"); return Future.succeededFuture(outcome(List.of(output(lane,"dlq",record.key(),new JsonObject().put("code","INVALID_REQUEST").put("sourceOffset",record.offset()).put("raw",record.value()))))); }
    lane.jobIds.put(record.offset(),job.id());
    return storage.executeBlocking(() -> {
      JsonObject latest = lane.ledger.get("job:"+job.id());
      if (latest != null && ("JOB_COMPLETED".equals(latest.getString("eventType"))
          || !job.hash().equals(latest.getString("requestHash")))) return latest;
      JsonObject attempt = lane.ledger.get(attemptKey(job));
      return attempt == null ? latest : attempt;
    },true).compose(state -> {
      if (!lane.current()) return Future.failedFuture("Revoked");
      if (state != null && state.getLong("expiresAt") <= System.currentTimeMillis()) {
        return persist(lane,"job:"+job.id(),null,new JsonObject().put("eventType","EXPIRED").put("jobId",job.id()))
            .compose(v -> invoke(lane,record,job));
      }
      if (state != null && !job.hash().equals(state.getString("requestHash"))) {
        count("conflicted");
        return Future.succeededFuture(outcome(List.of(output(lane,"dlq",job.entity(),new JsonObject().put("code","IDENTITY_CONFLICT").put("jobId",job.id()).put("sourceOffset",record.offset())))));
      }
      if (state != null && "JOB_COMPLETED".equals(state.getString("eventType"))) {
        count("deduplicated"); return Future.succeededFuture(outcome(successOutputs(lane,job,state.getJsonObject("outcome"))));
      }
      if (state != null && "DISPOSITION_COMPLETED".equals(state.getString("eventType")) && state.getInteger("attempt") == job.attempt()) {
        count("deduplicated");
        List<TransactionAdapter.Output> outputs = new ArrayList<>();
        for (Object value : state.getJsonArray("outputs")) {
          JsonObject stored = (JsonObject) value;
          outputs.add(new TransactionAdapter.Output(stored.getString("topic"), lane.tp.getPartition(),
              stored.getString("key"), stored.getString("value")));
        }
        return Future.succeededFuture(outcome(outputs));
      }
      if (state != null && "TERMINAL".equals(state.getString("eventType")) && state.getInteger("attempt") == job.attempt()) {
        count("deduplicated"); return Future.succeededFuture(outcome(failureOutputs(lane,record,job,false,"RETRIES_EXHAUSTED")));
      }
      long wait = state == null || lane.attempts.containsKey(record.offset()) ? 0 : Math.max(0,state.getLong("leaseUntil",0L)-System.currentTimeMillis());
      if (wait > 0) count("live_leases");
      return delay(lane,wait).compose(v -> invoke(lane,record,job));
    }).onFailure(error -> { if (!(error instanceof RetryScheduled) && lane.current()) fail(error); });
  }
  private Future<PartitionLane.Outcome<List<TransactionAdapter.Output>>> invoke(Lane lane, KafkaConsumerRecord<String,String> record, Job job) {
    // An administrative pause must not create attempt leases while waiting for admission.
    return permit(lane,Long.MAX_VALUE).compose(permit -> invokeAdmitted(lane,record,job,permit));
  }
  private Future<PartitionLane.Outcome<List<TransactionAdapter.Output>>> invokeAdmitted(Lane lane, KafkaConsumerRecord<String,String> record, Job job, AdaptiveAdmission.Permit permit) {
    int invocation = lane.attempts.merge(record.offset(),1,Integer::sum);
    lane.firstStarted.putIfAbsent(record.offset(),System.currentTimeMillis());
    String attemptId = UUID.randomUUID().toString();
    JsonObject started = state(job,"ATTEMPT_STARTED").put("attemptId",attemptId).put("executionToken",attemptId)
        .put("leaseUntil",System.currentTimeMillis()+config.leaseMs()).put("sourceOffset",record.offset());
    return persist(lane,"job:"+job.id(),started,started)
        .compose(v -> {
          if (!lane.current() || System.currentTimeMillis() >= started.getLong("leaseUntil"))
            return Future.failedFuture("Assignment or attempt lease expired before business invocation");
          count("started");
          var span = io.opentelemetry.api.GlobalOpenTelemetry.getTracer("spec007").spanBuilder("business.invoke")
              .setAttribute("jobId",job.id()).setAttribute("correlationId",job.wire().getString("correlationId"))
              .setAttribute("ownerHash",Job.digest(job.owner())).setAttribute("attemptId",attemptId)
              .setAttribute("source.topic",record.topic()).setAttribute("source.partition",record.partition())
              .setAttribute("source.offset",record.offset()).setAttribute("assignment.epoch",lane.epochId).startSpan();
          Future<JsonObject> call;
          try (var scope = span.makeCurrent()) { call = Objects.requireNonNull(handler.execute(job,attemptId)); }
          catch (Exception error) { call = Future.failedFuture(error); }
          long watchdog = vertx.setTimer(config.handlerTimeoutMs()+1000,id -> {
            if (lane.current()) fail(new IllegalStateException("Handler did not settle by deadline; replacement required"));
          });
          return confined(call).onComplete(done -> { vertx.cancelTimer(watchdog); if(done.failed()) span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR); span.end(); }).transform(done -> {
            if (!lane.current()) return Future.failedFuture("Revoked");
            if (done.succeeded()) {
              admission.outcome(permit,true);
              JsonObject result = result(job,"SUCCEEDED").put("result",done.result());
              JsonObject completed = state(job,"JOB_COMPLETED").put("attemptId",attemptId).put("outcome",result).put("sourceOffset",record.offset());
              return persist(lane,"job:"+job.id(),completed,completed).map(ignored -> {
                count("succeeded"); return outcome(successOutputs(lane,job,result));
              });
            }
            boolean retryable = !(done.cause() instanceof BusinessHandler.Failure f) || f.retryable;
            admission.outcome(permit,!retryable); count("handler_failures");
            JsonObject failure = state(job,"ATTEMPT_FAILED").put("attemptId",attemptId).put("leaseUntil",0L).put("sourceOffset",record.offset());
            boolean retry = retryable && invocation <= config.internalRetries()
                && System.currentTimeMillis()-lane.firstStarted.get(record.offset())+config.handlerTimeoutMs()+1000 < config.retryBudgetMs();
            if (retry) return persist(lane,"job:"+job.id(),failure,failure).compose(ignored -> {
              count("internal_retries");
              vertx.setTimer(100L * invocation + java.util.concurrent.ThreadLocalRandom.current().nextLong(100),timerId -> {
                if (lane.current() && !stopping) lane.coordinator.retryUnresolved(record.offset());
              });
              return Future.failedFuture(new RetryScheduled());
            });
            return persist(lane,"job:"+job.id(),failure,failure).compose(vv -> dispositionGate.apply(job)).compose(gate -> {
            List<TransactionAdapter.Output> outputs = failureOutputs(lane,record,job,retryable,"HANDLER_FAILED");
            JsonObject disposition = state(job,"DISPOSITION_COMPLETED").put("attemptId",attemptId).put("leaseUntil",0L).put("sourceOffset",record.offset());
            io.vertx.core.json.JsonArray stored = new io.vertx.core.json.JsonArray();
            for (var output : outputs) stored.add(new JsonObject().put("topic",output.topic()).put("key",output.key()).put("value",output.value()));
            disposition.put("outputs",stored);
            return persist(lane,attemptKey(job),disposition,disposition).map(ignored -> outcome(outputs));
            });
          });
        });
  }
  private static final class RetryScheduled extends RuntimeException {}
  private Future<AdaptiveAdmission.Permit> permit(Lane lane,long leaseUntil) {
    Promise<AdaptiveAdmission.Permit> result=Promise.promise();
    Runnable[] check=new Runnable[1];
    check[0]=() -> {
      if(!lane.current() || stopping) result.tryFail("Dispatch stopped");
      else if(System.currentTimeMillis()>=leaseUntil) result.tryFail("Attempt lease expired before dispatch");
      else {
        var permit=admission.acquirePermit();
        if(permit!=null) result.tryComplete(permit);
        else vertx.setTimer(50,id -> check[0].run());
      }
    };
    check[0].run(); return result.future();
  }
  private Future<Void> delay(Lane lane,long millis) {
    Promise<Void> result=Promise.promise();
    long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(millis);
    Runnable[] check=new Runnable[1];
    check[0]=() -> {
      if(!lane.current() || stopping) result.tryFail("Assignment stopped");
      else if(System.nanoTime()>=deadline) result.tryComplete();
      else vertx.setTimer(Math.max(1,Math.min(100,java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime()))),id -> check[0].run());
    };
    check[0].run(); return result.future();
  }
  private static String attemptKey(Job job) { return "attempt:"+job.id()+":"+job.attempt(); }
  private JsonObject state(Job job,String event) {
    return new JsonObject().put("schemaVersion",1).put("eventType",event).put("jobId",job.id()).put("ownerId",job.owner())
        .put("workload",config.workload()).put("attempt",job.attempt()).put("requestHash",job.hash())
        .put("timestamp",Instant.now().toString()).put("expiresAt",System.currentTimeMillis()+config.retentionMs());
  }
  private Future<Void> persist(Lane lane,String key,JsonObject state,JsonObject event) {
    var outputs = List.of(output(lane,"edr",key,event),new TransactionAdapter.Output(config.topic("execution-ledger"),lane.tp.getPartition(),key,state == null ? null : state.encode()));
    return transactions.submit(config.writerId(lane.tp.getPartition()),new TransactionAdapter.Command(outputs,Map.of(),null,lane::current))
        .compose(v -> storage.<Void>executeBlocking(() -> { if (!lane.current()) throw new IllegalStateException("Revoked before materialization"); lane.ledger.put(key,state == null ? null : state.encode()); return null; },true))
        .onSuccess(v -> {
          count("edr_durable");
          System.out.println(new JsonObject().put("event","edr_durable").put("eventType",event.getString("eventType"))
              .put("jobId",event.getString("jobId")).put("attemptId",event.getString("attemptId"))
              .put("sourceTopic",lane.tp.getTopic()).put("sourcePartition",lane.tp.getPartition())
              .put("sourceOffset",event.getLong("sourceOffset")).put("epoch",lane.epochId).encode());
        });
  }
  private JsonObject lifecycle(Job job,String type) {
    String now = Instant.now().toString();
    JsonObject event = new JsonObject().put("schemaVersion",1).put("eventId",UUID.randomUUID().toString())
        .put("eventType",type).put("eventTime",now).put("ingestionTime",now).put("jobId",job.id())
        .put("correlationId",job.wire().getString("correlationId")).put("jobType",job.wire().getString("jobType"))
        .put("attemptNumber",job.attempt()).put("maxAttempts",job.maxAttempts());
    String canonical = new JsonObject(new TreeMap<>(event.getMap())).encode();
    return event.put("canonicalPayload",canonical).put("payloadHash",Job.digest(canonical));
  }
  private JsonObject result(Job job,String status) {
    return new JsonObject().put("schemaVersion",1).put("jobId",job.id()).put("ownerId",job.owner()).put("correlationId",job.wire().getString("correlationId"))
        .put("attempt",job.attempt()).put("status",status).put("completedAt",Instant.now().toString());
  }
  private List<TransactionAdapter.Output> successOutputs(Lane lane,Job job,JsonObject result) {
    return List.of(output(lane,"results",job.id(),result),output(lane,"lifecycle",job.id(),lifecycle(job,"JOB_EXECUTION_SUCCEEDED")));
  }
  private List<TransactionAdapter.Output> failureOutputs(Lane lane,KafkaConsumerRecord<String,String> source,Job job,boolean retryable,String code) {
    List<TransactionAdapter.Output> outputs = new ArrayList<>();
    boolean retry = retryable && job.attempt() < job.maxAttempts();
    outputs.add(output(lane,"results",job.id(),result(job,retry ? "FAILED":"RETRIES_EXHAUSTED").put("errorCode",code).put("retryable",retryable)));
    outputs.add(output(lane,"lifecycle",job.id(),lifecycle(job,retry ? "JOB_EXECUTION_FAILED":"JOB_RETRIES_EXHAUSTED")));
    JsonObject handoff = new JsonObject().put("schemaVersion",1).put("handoffId",Job.digest(config.workload()+":"+source.topic()+":"+source.partition()+":"+source.offset()+":"+job.attempt()))
        .put("sourceTopic",source.topic()).put("sourcePartition",source.partition()).put("sourceOffset",source.offset())
        .put("createdAt",Instant.now().toString()).put("request",job.wire().copy().put("attempt",job.attempt()+1)).put("code",code);
    outputs.add(output(lane,retry ? "retry":"dlq",job.entity(),handoff));
    count(retry ? "quarantined":"dlq");
    return List.copyOf(outputs);
  }
  private Future<PartitionLane.Outcome<List<TransactionAdapter.Output>>> requeue(Lane lane,KafkaConsumerRecord<String,String> record) {
    try {
      JsonObject handoff = new JsonObject(record.value());
      Job job = Job.parse(handoff.getJsonObject("request").encode(),record.key(),config.workload());
      long age=Duration.between(Instant.parse(handoff.getString("createdAt")),Instant.now()).toMillis();
      String expected=Job.digest(config.workload()+":"+config.workTopic()+":"+handoff.getInteger("sourcePartition")+":"+handoff.getLong("sourceOffset")+":"+(job.attempt()-1));
      if (!config.workTopic().equals(handoff.getString("sourceTopic")) || !expected.equals(handoff.getString("handoffId"))
          || handoff.getInteger("schemaVersion",0)!=1 || job.attempt()<=1
          || handoff.getInteger("sourcePartition",-1)!=lane.tp.getPartition() || handoff.getLong("sourceOffset",-1L)<0
          || age<0 || age>config.replayWindowMs()) throw new IllegalArgumentException("Expired/invalid quarantine");
      String handoffKey="handoff:"+handoff.getString("handoffId");
      return storage.executeBlocking(() -> lane.ledger.get(handoffKey),true).compose(previous -> {
        if(previous!=null && previous.getLong("expiresAt",0L)>System.currentTimeMillis()) {
          count("deduplicated"); return Future.succeededFuture(outcome(List.of()));
        }
        return permit(lane,System.currentTimeMillis()+config.leaseMs()).map(v -> outcome(List.of(
            new TransactionAdapter.Output(config.workTopic(),lane.tp.getPartition(),job.entity(),job.wire().encode()),
            output(lane,"finalization",handoffKey,new JsonObject().put("expiresAt",System.currentTimeMillis()+config.retentionMs())))));
      });
    } catch(Exception invalid) { return Future.succeededFuture(outcome(List.of(output(lane,"dlq",record.key(),new JsonObject().put("code","INVALID_QUARANTINE").put("raw",record.value()))))); }
  }
  private TransactionAdapter.Output output(Lane lane,String kind,String key,JsonObject json) {
    return new TransactionAdapter.Output(config.topic(kind),lane.tp.getPartition(),key == null ? "invalid":key,json.encode());
  }
  private PartitionLane.Outcome<List<TransactionAdapter.Output>> outcome(List<TransactionAdapter.Output> outputs) {
    return new PartitionLane.Outcome<>(List.copyOf(outputs),outputs.stream().mapToLong(o -> utf8(o.value())+utf8(o.key())+128).sum());
  }
  private Future<Void> commit(Lane lane,ContiguousCompletionTracker.Prefix<List<TransactionAdapter.Output>> prefix) {
    List<TransactionAdapter.Output> outputs = new ArrayList<>();
    for (var completed : prefix.records()) {
      outputs.addAll(completed.outcome());
      outputs.add(output(lane,"finalization",config.sourceTopic()+":"+lane.tp.getPartition()+":"+completed.offset(),new JsonObject().put("nextOffset",completed.offset()+1)));
    }
    var metadata = nativeConsumer.metadataSnapshot();
    if (metadata == null) return Future.failedFuture("No group metadata");
    return transactions.submit(new TransactionAdapter.Command(outputs,Map.of(nativeTp(lane.tp),new OffsetAndMetadata(prefix.nextOffset())),metadata,lane::current))
        .compose(v -> config.retryWorker() ? storage.<Void>executeBlocking(() -> {
          if (!lane.current()) throw new IllegalStateException("Revoked during requeue materialization");
          for(var output:outputs) if(output.key().startsWith("handoff:")) lane.ledger.put(output.key(),output.value());
          return null;
        },true) : Future.succeededFuture())
        .onSuccess(v -> {
          count("transactions");
          for (var completed : prefix.records()) { lane.records.remove(completed.offset()); lane.jobIds.remove(completed.offset()); lane.attempts.remove(completed.offset()); lane.firstStarted.remove(completed.offset()); }
        });
  }
  private void fail(Throwable error) {
    if (failed || stopping) return;
    failed = true; count("recovery_required");
    for (Lane lane : lanes.values()) { lane.token.set(false); if (lane.coordinator != null) lane.coordinator.revoke(); }
    // No seek within an old epoch. Supervisor replaces this runtime, fences producers, restores Kafka state.
    fatal.accept(error);
  }
  public Future<Void> close() {
    if (closing != null) return closing;
    closing = closeRuntime();
    return closing;
  }
  private Future<Void> closeRuntime() {
    stopping = true;
    for (Lane lane : lanes.values()) if (lane.coordinator != null) lane.coordinator.stopDispatch();
    if (consumer == null) return transactions == null ? storage.close() : transactions.close().eventually(storage::close);
    return confined(consumer.assignment().compose(consumer::pause)).recover(e -> Future.succeededFuture())
        .compose(v -> drain(System.currentTimeMillis()+config.drainMs()))
        .eventually(() -> {
          vertx.cancelTimer(timer);
          for (Lane lane : lanes.values()) { lane.token.set(false); if (lane.coordinator != null) lane.coordinator.revoke(); }
          return consumer.close();
        }).eventually(transactions::close).eventually(() -> storage.<Void>executeBlocking(() -> {
          for(var lane:lanes.values()) if(lane.ledger!=null) lane.ledger.close(); return null;
        },true)).eventually(storage::close);
  }
  private Future<Void> drain(long deadline) {
    if (failed || System.currentTimeMillis() >= deadline || lanes.values().stream().allMatch(l -> l.tracker == null || l.tracker.size()==0)) return Future.succeededFuture();
    Promise<Void> result = Promise.promise();
    vertx.setTimer(50,id -> drain(deadline).onComplete(result)); return result.future();
  }
  private static long utf8(String value) { return value == null ? 0 : value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length; }
  private static org.apache.kafka.common.TopicPartition nativeTp(TopicPartition tp) { return new org.apache.kafka.common.TopicPartition(tp.getTopic(),tp.getPartition()); }
}
