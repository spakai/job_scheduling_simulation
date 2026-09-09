package com.example.jobs.pull.contract;

import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public record Job(JsonObject wire, String id, String owner, String entity, String hash) {
  private static final Set<String> FIELDS = Set.of("schemaVersion","jobId","ownerId","ownerType",
      "correlationId","jobType","requestedAt","attempt","maxAttempts","payload","payloadReference");
  public static Job parse(String value, String key, String workload) {
    JsonObject json = new JsonObject(value);
    if (!FIELDS.containsAll(json.fieldNames())) throw new IllegalArgumentException("Unknown envelope field");
    for (String field : Set.of("jobId","ownerId","ownerType","correlationId","jobType","requestedAt")) {
      if (json.getString(field) == null || json.getString(field).isBlank()) throw new IllegalArgumentException("Missing " + field);
    }
    for(String field:Set.of("schemaVersion","attempt","maxAttempts")) {
      Object number=json.getValue(field);
      if(number!=null && (!(number instanceof Integer || number instanceof Long)
          || ((Number)number).longValue()>Integer.MAX_VALUE || ((Number)number).longValue()<1)) throw new IllegalArgumentException("Integer field required: "+field);
    }
    if (json.getInteger("schemaVersion", 1) != 1 || json.getInteger("attempt",1) < 1
        || json.getInteger("maxAttempts",3) < json.getInteger("attempt",1)) throw new IllegalArgumentException("Invalid version/attempt");
    OffsetDateTime.parse(json.getString("requestedAt"));
    boolean payload = json.getValue("payload") != null;
    if (payload == (json.getValue("payloadReference") != null)) throw new IllegalArgumentException("Exactly one payload source required");
    if (payload) json.getJsonObject("payload");
    else if (json.getString("payloadReference").isBlank()) throw new IllegalArgumentException("Empty reference");
    String owner = json.getString("ownerId");
    if (!json.getString("ownerType").equals(workload.toUpperCase(java.util.Locale.ROOT))
        || !owner.startsWith(workload + ":") || owner.length() == workload.length()+1) throw new IllegalArgumentException("Wrong owner fleet");
    String entity = owner.substring(workload.length()+1);
    if (!entity.equals(key)) throw new IllegalArgumentException("Wire key must be canonical entity ID");
    if (payload) {
      Object payloadEntity=json.getJsonObject("payload").getValue(workload+"Id");
      if(payloadEntity != null && !entity.equals(payloadEntity.toString())) throw new IllegalArgumentException("Payload entity conflicts with owner");
    }
    JsonObject immutable = json.copy().put("schemaVersion",1).put("maxAttempts",json.getInteger("maxAttempts",3));
    immutable.remove("attempt");
    return new Job(json.copy(), json.getString("jobId"), owner, entity, digest(canonical(immutable.getMap())));
  }
  public int attempt() { return wire.getInteger("attempt",1); }
  public int maxAttempts() { return wire.getInteger("maxAttempts",3); }
  public static String digest(String value) {
    try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
    catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }
  private static String canonical(Object value) {
    return io.vertx.core.json.Json.encode(sorted(value));
  }
  private static Object sorted(Object value) {
    if (value instanceof JsonObject j) return sorted(j.getMap());
    if (value instanceof io.vertx.core.json.JsonArray a) return sorted(a.getList());
    if (value instanceof Map<?,?> m) {
      Map<String,Object> result = new TreeMap<>();
      m.forEach((k,v) -> result.put(k.toString(), sorted(v))); return result;
    }
    if (value instanceof java.util.List<?> l) return l.stream().map(Job::sorted).toList();
    return value;
  }
}
