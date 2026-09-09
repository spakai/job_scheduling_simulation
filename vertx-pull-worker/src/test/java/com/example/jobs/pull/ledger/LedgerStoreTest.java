package com.example.jobs.pull.ledger;

import static org.junit.jupiter.api.Assertions.*;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LedgerStoreTest {
  @TempDir java.nio.file.Path directory;
  @Test void boundsDiskAndDeletesExpiredMaterializationsWithoutTrustingLocalState() throws Exception {
    var store=new LedgerStore(directory,1024);
    store.put("job:a",new JsonObject().put("jobId","a").put("expiresAt",10L).encode());
    store.put("job:b",new JsonObject().put("jobId","b").put("expiresAt",100L).encode());
    assertEquals(java.util.List.of("job:a"),store.expired(20,100));
    assertThrows(IllegalStateException.class,() -> store.put("job:large",new JsonObject().put("body","x".repeat(2000)).encode()));
    assertEquals("b",store.get("job:b").getString("jobId"));
    store.put("job:a",null); assertNull(store.get("job:a")); store.close();
    var replacement=new LedgerStore(directory,1024); assertNull(replacement.get("job:b")); replacement.close();
  }
}
