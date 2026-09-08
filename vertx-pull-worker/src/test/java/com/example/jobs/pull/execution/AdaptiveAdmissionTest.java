package com.example.jobs.pull.execution;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AdaptiveAdmissionTest {
  @Test void degradesToOpenThenUsesBoundedProbeAndSteppedRecovery() {
    AtomicLong now=new AtomicLong();
    var admission=new AdaptiveAdmission(2,2,now::get,2,1000);
    for(double rate:new double[]{1,0.5,0}) {
      admission.outcome(false); admission.outcome(false); assertEquals(rate,admission.effectiveRate());
    }
    assertFalse(admission.acquire()); now.set(1000);
    assertTrue(admission.acquire()); assertFalse(admission.acquire());
    admission.outcome(true); assertEquals(0.5,admission.effectiveRate());
    now.set(2000); admission.outcome(true); admission.outcome(true); assertEquals(1,admission.effectiveRate());
    now.set(3000); admission.outcome(true); admission.outcome(true); assertEquals(2,admission.effectiveRate());
  }
  @Test void administrativeZeroNeverProbes() {
    AtomicLong now=new AtomicLong(); var admission=new AdaptiveAdmission(0,1,now::get,1,1);
    admission.outcome(false); admission.outcome(false); admission.outcome(false); now.set(1000);
    assertFalse(admission.acquire());
  }
  @Test void preOpenInvocationCannotResolveHalfOpenProbe() {
    AtomicLong now=new AtomicLong();
    var admission=new AdaptiveAdmission(2,10,now::get,1,1000);
    var old=admission.acquirePermit();
    admission.outcome(false); admission.outcome(false); admission.outcome(false);
    now.set(1000);
    var probe=admission.acquirePermit(); assertNotNull(probe);
    admission.outcome(old,true);
    assertEquals("OPEN",admission.state()); assertNull(admission.acquirePermit());
    admission.outcome(probe,true); assertEquals("SEVERE",admission.state());
  }

}
