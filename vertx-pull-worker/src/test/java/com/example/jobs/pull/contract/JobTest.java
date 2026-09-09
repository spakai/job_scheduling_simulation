package com.example.jobs.pull.contract;

import static org.junit.jupiter.api.Assertions.*;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class JobTest {
  JsonObject request() {
    return new JsonObject().put("schemaVersion",1).put("jobId","job").put("ownerId","subscriber:42").put("ownerType","SUBSCRIBER")
        .put("correlationId","c").put("jobType","RERATE").put("requestedAt","2026-01-01T00:00:00Z").put("attempt",1)
        .put("maxAttempts",3).put("payload",new JsonObject().put("x",1).put("y",2));
  }
  @Test void hashPreservesIdentityAcrossRetriesAndObjectOrder() {
    var original=Job.parse(request().encode(),"42","subscriber");
    var retry=Job.parse(request().put("attempt",2).put("payload",new JsonObject().put("y",2).put("x",1)).encode(),"42","subscriber");
    assertEquals(original.hash(),retry.hash());
    assertNotEquals(original.hash(),Job.parse(request().put("payload",new JsonObject().put("x",2)).encode(),"42","subscriber").hash());
  }
  @Test void rejectsLegacyKeyAndCrossFleetAndInvalidPayload() {
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().encode(),"subscriber:42","subscriber"));
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().encode(),"42","group"));
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().put("payloadReference","s3://x").encode(),"42","subscriber"));
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().put("attempt",4).encode(),"42","subscriber"));
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().put("attempt",Long.MIN_VALUE+1).encode(),"42","subscriber"));
  }
  @Test void canonicalizesObjectsNestedInsideArraysAndRejectsConflictingEntity() {
    var a=new io.vertx.core.json.JsonArray().add(new JsonObject().put("x",1).put("y",2));
    var b=new io.vertx.core.json.JsonArray().add(new JsonObject().put("y",2).put("x",1));
    assertEquals(Job.parse(request().put("payload",new JsonObject().put("items",a)).encode(),"42","subscriber").hash(),
        Job.parse(request().put("payload",new JsonObject().put("items",b)).encode(),"42","subscriber").hash());
    assertThrows(IllegalArgumentException.class,() -> Job.parse(request().put("payload",new JsonObject().put("subscriberId","43")).encode(),"42","subscriber"));
  }

}
