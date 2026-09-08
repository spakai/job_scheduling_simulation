package com.example.jobs.pull;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class ArchitectureFitnessTest {
  @Test void workRuntimeNeverUsesImplicitCommitOrBlockingSleep() throws Exception {
    var implicit=Pattern.compile("\\.commit\\s*\\(\\s*\\)");
    try(var sources=Files.walk(Path.of("src/main/java"))) {
      for(var path:sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        String source=Files.readString(path);
        assertFalse(implicit.matcher(source).find(),"Implicit offset commit in "+path);
        assertFalse(source.contains("Thread.sleep("),"Blocking sleep in "+path);
      }
    }
  }
}
