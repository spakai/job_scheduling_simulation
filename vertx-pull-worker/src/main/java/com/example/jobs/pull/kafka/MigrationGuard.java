package com.example.jobs.pull.kafka;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

/** Read-only cutover precondition. The deployment controller must keep the old fleet scaled to zero. */
public final class MigrationGuard {
  private MigrationGuard() {}
  public static void requireDrained(Admin admin,String group,String topic) throws Exception {
    var description=admin.describeConsumerGroups(List.of(group)).all().get(10,TimeUnit.SECONDS).get(group);
    if(!description.members().isEmpty()) throw new IllegalStateException("Other runtime has active members");
    var partitions=admin.describeTopics(List.of(topic)).allTopicNames().get(10,TimeUnit.SECONDS).get(topic).partitions();
    Map<TopicPartition,OffsetSpec> query=new HashMap<>();
    for(var p:partitions) query.put(new TopicPartition(topic,p.partition()),OffsetSpec.latest());
    var ends=admin.listOffsets(query).all().get(10,TimeUnit.SECONDS);
    var commits=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);
    for(var entry:ends.entrySet()) {
      long next=commits.containsKey(entry.getKey()) ? commits.get(entry.getKey()).offset():0;
      if(next<entry.getValue().offset()) throw new IllegalStateException("Other runtime has undrained backlog");
    }
  }
}
