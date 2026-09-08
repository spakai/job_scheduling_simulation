package com.example.jobs.pull.contract;

import io.vertx.core.json.JsonObject;

/** Pure producer routing translation. No offsets or business calls are copied between topics. */
public final class RouteMigration {
  private RouteMigration() {}
  public static JsonObject translate(String namespace, String legacyKey, String value) {
    if(namespace == null || !namespace.matches("[a-zA-Z0-9._-]+")) throw new IllegalArgumentException("Invalid namespace");
    JsonObject request = new JsonObject(value);
    String owner = request.getString("ownerId");
    if(owner == null || !owner.equals(legacyKey) || !owner.contains(":")) throw new IllegalArgumentException("Legacy key must match ownerId");
    String workload = owner.substring(0,owner.indexOf(':'));
    if(!java.util.Set.of("subscriber","group").contains(workload)) throw new IllegalArgumentException("Invalid workload");
    String entity = owner.substring(owner.indexOf(':')+1);
    Job.parse(value,entity,workload);
    return new JsonObject().put("topic",namespace+workload+"-rerate").put("key",entity).put("value",request);
  }
}
