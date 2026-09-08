package com.example.jobs.pull.lane;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Deterministic bookkeeping stress; these counts do not claim wall-clock business throughput. */
class CapacityProfileTest {
  @Test void twentyAndHundredThousandDeliveriesRetainBoundedSafePrefixes() {
    for(int count:List.of(20000,100000)) for(int concurrency:List.of(1,2,4,8,16)) {
      var tracker=new ContiguousCompletionTracker<String>(1,0,100,1024*1024);
      Random random=new Random(7);
      int high=0;
      for(int base=0;base<count;base+=concurrency) {
        List<ContiguousCompletionTracker.Delivery> records=new ArrayList<>();
        for(int i=base;i<Math.min(count,base+concurrency);i++) records.add(new ContiguousCompletionTracker.Delivery(i,"owner-"+i,128));
        tracker.registerBatch(records); high=Math.max(high,tracker.size());
        var tokens=new ArrayList<>(records.stream().map(r -> tracker.start(r.offset())).toList()); Collections.shuffle(tokens,random);
        for(var token:tokens) {
          tracker.complete(token,"done",128);
          var prefix=tracker.preparePrefix(4,1024);
          if(prefix.isPresent()) {
            long next=prefix.get().nextOffset();
            assertTrue(tracker.states().entrySet().stream().noneMatch(e -> e.getKey()<next && e.getValue()==ContiguousCompletionTracker.State.RUNNING));
            tracker.committed(prefix.get());
          }
        }
        while(tracker.size()>0) tracker.committed(tracker.preparePrefix(4,1024).orElseThrow());
      }
      assertEquals(count,tracker.committedNext()); assertTrue(high<=concurrency); assertEquals(0,tracker.usedBytes());
    }
  }
}
