package com.example.jobs.pull.contract;

import static org.junit.jupiter.api.Assertions.*;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class RouteMigrationTest {
  @Test void translatesBothFleetsWithoutChangingEnvelopeOrIdentity() {
    for(String workload:java.util.List.of("subscriber","group")) {
      JsonObject original=new JsonObject().put("schemaVersion",1).put("jobId","stable-job")
          .put("ownerId",workload+":42").put("ownerType",workload.toUpperCase(java.util.Locale.ROOT))
          .put("correlationId","correlation").put("jobType","RERATE").put("requestedAt","2026-01-01T00:00:00Z")
          .put("attempt",2).put("maxAttempts",3).put("payload",new JsonObject().put(workload+"Id","42"));
      var translated=RouteMigration.translate("test-",workload+":42",original.encode());
      assertEquals("test-"+workload+"-rerate",translated.getString("topic"));
      assertEquals("42",translated.getString("key")); assertEquals(original,translated.getJsonObject("value"));
      assertFalse(translated.containsKey("offset"));
      assertThrows(IllegalArgumentException.class,() -> RouteMigration.translate("test-","42",original.encode()));
    }
  }
}
