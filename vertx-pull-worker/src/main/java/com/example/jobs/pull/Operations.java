package com.example.jobs.pull;

import com.example.jobs.pull.config.RuntimeConfig;
import com.example.jobs.pull.contract.Job;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;

/** Explicit local tooling; never selected by the production worker entrypoint. */
public final class Operations {
  private Operations() {}
  private static void registerSchema(String registry,String topic,String file) throws Exception {
    String schema;
    try(var stream=Operations.class.getResourceAsStream("/schemas/"+file)) {
      schema=new String(Objects.requireNonNull(stream).readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
    }
    try(var client=java.net.http.HttpClient.newHttpClient()) {
      var request=java.net.http.HttpRequest.newBuilder(java.net.URI.create(registry+"/subjects/"+topic+"-value/versions"))
          .timeout(java.time.Duration.ofSeconds(10)).header("Content-Type","application/vnd.schemaregistry.v1+json")
          .POST(java.net.http.HttpRequest.BodyPublishers.ofString(new JsonObject().put("schemaType","JSON").put("schema",schema).encode())).build();
      var response=client.send(request,java.net.http.HttpResponse.BodyHandlers.ofString());
      if(response.statusCode()/100!=2) throw new IllegalStateException("Schema registration rejected for "+topic);
    }
  }
  public static void main(String[] args) throws Exception {
    if (args.length == 0) throw new IllegalArgumentException("topics | produce COUNT | demo-handler");
    if (args[0].equals("demo-handler")) {
      Vertx vertx = Vertx.vertx();
      int delay = Integer.parseInt(System.getenv().getOrDefault("DEMO_DELAY_MS","100"));
      vertx.createHttpServer().requestHandler(request -> {
        if (request.path().equals("/health/live")) { request.response().end("live"); return; }
        request.body().onSuccess(body -> vertx.setTimer(Math.max(1,delay),id -> request.response()
            .putHeader("Content-Type","application/json").end(new JsonObject().put("demo",true)
                .put("idempotencyKey",request.getHeader("Idempotency-Key")).encode())));
      }).listen(8080).onFailure(e -> { e.printStackTrace(); System.exit(1); });
      return;
    }
    Map<String,String> env = new HashMap<>(System.getenv()); env.putIfAbsent("HANDLER_URL","http://demo-handler:8080");
    RuntimeConfig config = RuntimeConfig.fromEnvironment(env);
    if (args[0].equals("translate")) {
      if(args.length!=2) throw new IllegalArgumentException("translate LEGACY_JSONL_FILE");
      try(var lines=java.nio.file.Files.lines(java.nio.file.Path.of(args[1]))) {
        lines.filter(line -> !line.isBlank()).forEach(line -> {
          var record=new JsonObject(line);
          System.out.println(com.example.jobs.pull.contract.RouteMigration.translate(config.namespace(),
              record.getString("key"),record.getJsonObject("value").encode()).encode());
        });
      }
    } else if (args[0].equals("migration-check")) {
      if(args.length!=3) throw new IllegalArgumentException("migration-check OTHER_GROUP OTHER_TOPIC");
      try(var admin=Admin.create(config.adminProperties())) {
        com.example.jobs.pull.kafka.MigrationGuard.requireDrained(admin,args[1],args[2]);
      }
      System.out.println("Other runtime is inactive and drained; preserve this state during cutover");
    } else if (args[0].equals("topics")) {
      short replicas = Short.parseShort(env.getOrDefault("TOPIC_REPLICATION_FACTOR","1"));
      String minIsr = env.getOrDefault("TOPIC_MIN_ISR","1");
      try(var admin = Admin.create(config.adminProperties())) {
        List<NewTopic> topics = new ArrayList<>();
        for(String workload:List.of("subscriber","group")) {
          env.put("WORKLOAD",workload); var fleet=RuntimeConfig.fromEnvironment(env);
          topics.add(new NewTopic(fleet.workTopic(),config.partitions(),replicas).configs(Map.of("min.insync.replicas",minIsr,"retention.ms",""+config.retentionMs(),"max.message.bytes",""+config.maxRecordBytes())));
          for(String kind:List.of("edr","execution-ledger","finalization","results","lifecycle","retry","dlq")) {
            Map<String,String> settings=new HashMap<>(Map.of("min.insync.replicas",minIsr,"retention.ms",""+config.retentionMs()));
            if(kind.equals("execution-ledger") || kind.equals("finalization")) {
              settings.put("cleanup.policy",kind.equals("finalization") ? "compact,delete":"compact"); settings.put("delete.retention.ms",""+config.retentionMs());
            }
            topics.add(new NewTopic(fleet.topic(kind),config.partitions(),replicas).configs(settings));
          }
        }
        var existing=admin.listTopics().names().get(); topics.removeIf(t -> existing.contains(t.name()));
        if(!topics.isEmpty()) admin.createTopics(topics).all().get();
        String registry=env.get("SCHEMA_REGISTRY_URL");
        if(registry!=null) {
          for(String workload:List.of("subscriber","group")) {
            env.put("WORKLOAD",workload); var fleet=RuntimeConfig.fromEnvironment(env);
            for(var mapping:Map.of(fleet.workTopic(),"job-requests-v1.json",fleet.topic("results"),"job-results-v1.json",
                fleet.topic("lifecycle"),"job-lifecycle-edr-v1.json",fleet.topic("edr"),"execution-edr-v1.json",
                fleet.topic("execution-ledger"),"execution-edr-v1.json",fleet.topic("retry"),"quarantine-v1.json").entrySet()) {
              registerSchema(registry,mapping.getKey(),mapping.getValue());
            }
          }
        }
        System.out.println("Spec 007 workload topics initialized");
      }
    } else if(args[0].equals("produce")) {
      int count=Integer.parseInt(args[1]); String run=UUID.randomUUID().toString();
      var props=config.producerProperties("unused"); props.remove("transactional.id");
      long start=System.nanoTime();
      try(var producer=new KafkaProducer<String,String>(props)) {
        for(int i=0;i<count;i++) {
          String entity="entity-"+i; String jobId=run+"-"+i;
          JsonObject job=new JsonObject().put("schemaVersion",1).put("jobId",jobId).put("ownerId",config.workload()+":"+entity)
              .put("ownerType",config.workload().toUpperCase(Locale.ROOT)).put("correlationId",run).put("jobType","RERATE")
              .put("requestedAt",Instant.now().toString()).put("attempt",1).put("maxAttempts",3).put("payload",new JsonObject().put(config.workload()+"Id",entity));
          Job.parse(job.encode(),entity,config.workload());
          producer.send(new ProducerRecord<>(config.workTopic(),entity,job.encode()));
        }
        producer.flush();
      }
      System.out.println(new JsonObject().put("runId",run).put("published",count).put("seconds",(System.nanoTime()-start)/1e9).encode());
    } else throw new IllegalArgumentException("Unknown operation");
  }
}
