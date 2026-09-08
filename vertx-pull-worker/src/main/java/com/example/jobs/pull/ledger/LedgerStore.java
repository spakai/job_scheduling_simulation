package com.example.jobs.pull.ledger;

import com.example.jobs.pull.config.RuntimeConfig;
import com.example.jobs.pull.contract.Job;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/** Disk-backed materialization, never a cache authority. All calls require a blocking executor. */
public final class LedgerStore {
  private final Path directory;
  private long bytes;
  private final long maxBytes;
  public LedgerStore(Path directory, long maxBytes) throws Exception {
    this.directory = directory; this.maxBytes = maxBytes;
    Files.createDirectories(directory);
    // Each assignment restores from Kafka; never trust a local checkpoint after a crash.
    try (var files = Files.newDirectoryStream(directory)) {
      for (Path file : files) Files.delete(file);
    }
  }
  private Path path(String key) { return directory.resolve(Job.digest(key) + ".json"); }
  public JsonObject get(String key) throws Exception {
    Path file = path(key);
    return Files.exists(file) ? new JsonObject(Files.readString(file)).getJsonObject("value") : null;
  }
  public void put(String key, String value) throws Exception {
    Path file = path(key);
    long old = Files.exists(file) ? Files.size(file) : 0;
    if (value == null) { Files.deleteIfExists(file); bytes -= old; return; }
    byte[] data = new JsonObject().put("key",key).put("value",new JsonObject(value)).encode().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (data.length > maxBytes - bytes + old) throw new IllegalStateException("Ledger disk quota exceeded");
    Path temporary = directory.resolve("write.tmp");
    Files.write(temporary, data);
    Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    bytes += data.length - old;
  }
  private java.nio.file.DirectoryStream<Path> scan;
  private java.util.Iterator<Path> iterator;
  public java.util.List<String> expired(long now,int limit) throws Exception {
    if (scan == null) { scan = Files.newDirectoryStream(directory,"*.json"); iterator = scan.iterator(); }
    var keys = new java.util.ArrayList<String>();
    for(int i=0;i<limit && iterator.hasNext();i++) {
      Path file = iterator.next();
      if (!Files.exists(file)) continue;
      JsonObject wrapper = new JsonObject(Files.readString(file));
      JsonObject state = wrapper.getJsonObject("value");
      if (state.getLong("expiresAt",Long.MAX_VALUE) <= now) keys.add(wrapper.getString("key"));
    }
    if(!iterator.hasNext()) { scan.close(); scan=null; iterator=null; }
    return keys;
  }
  public void close() throws Exception { if(scan!=null) { scan.close(); scan=null; iterator=null; } }
  public long bytes() { return bytes; }
  public void restore(RuntimeConfig config, int partition, java.util.function.BooleanSupplier current) throws Exception {
    restore(config,partition,current,config.topic("execution-ledger"));
  }
  public void restore(RuntimeConfig config,int partition,java.util.function.BooleanSupplier current,String topic) throws Exception {
    var props = config.consumerProperties();
    props.remove("group.id");
    var tp = new TopicPartition(topic, partition);
    try (var reader = new KafkaConsumer<String,String>(props)) {
      reader.assign(List.of(tp));
      reader.seekToBeginning(List.of(tp));
      long end = reader.endOffsets(List.of(tp), Duration.ofSeconds(10)).get(tp);
      long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
      while (reader.position(tp) < end) {
        if (!current.getAsBoolean()) throw new IllegalStateException("Revoked during restore");
        if (System.nanoTime() >= deadline) throw new IllegalStateException("Ledger restore deadline");
        for (var record : reader.poll(Duration.ofMillis(100))) {
          if (record.offset() < end) put(record.key(), record.value());
        }
      }
    }
  }
}
