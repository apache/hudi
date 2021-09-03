package org.apache.hudi.helper;

import org.apache.hudi.connect.core.TransactionParticipant;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Helper class that emulates the Kafka Connect f/w and additionally
 * implements {@link SinkTaskContext} for testing purposes.
 */
public class TestKafkaConnect implements SinkTaskContext {

  private static final int NUM_RECORDS_BATCH = 5;
  private final TopicPartition testPartition;

  private TransactionParticipant participant;
  private long currentKafkaOffset;
  private boolean isPaused;

  public TestKafkaConnect(TopicPartition testPartition) {
    this.testPartition = testPartition;
    isPaused = false;
    currentKafkaOffset = 0L;
  }

  public void setParticipant(TransactionParticipant participant) {
    this.participant = participant;
  }

  public boolean isPaused() {
    return isPaused;
  }

  public boolean isResumed() {
    return !isPaused;
  }

  public int putRecordsToParticipant() {
    for (int i = 1; i <= NUM_RECORDS_BATCH; i++) {
      participant.buffer(getNextKafkaRecord());
    }
    participant.processRecords();
    return NUM_RECORDS_BATCH;
  }

  public SinkRecord getNextKafkaRecord() {
    return new SinkRecord(testPartition.topic(),
        testPartition.partition(),
        Schema.OPTIONAL_BYTES_SCHEMA,
        ("key-" + currentKafkaOffset).getBytes(),
        Schema.OPTIONAL_BYTES_SCHEMA,
        "value".getBytes(), currentKafkaOffset++);
  }

  public long getCurrentKafkaOffset() {
    return currentKafkaOffset;
  }

  @Override
  public void pause(TopicPartition... partitions) {
    if (Arrays.stream(partitions).allMatch(testPartition::equals)) {
      isPaused = true;
    }
  }

  @Override
  public void resume(TopicPartition... partitions) {
    if (Arrays.stream(partitions).allMatch(testPartition::equals)) {
      isPaused = false;
    }
  }

  @Override
  public void offset(Map<TopicPartition, Long> offsets) {
    for (TopicPartition tp : offsets.keySet()) {
      if (tp.equals(testPartition)) {
        currentKafkaOffset = offsets.get(tp);
      }
    }
  }

  @Override
  public void offset(TopicPartition tp, long offset) {
    if (tp.equals(testPartition)) {
      currentKafkaOffset = offset;
    }
  }

  @Override
  public Map<String, String> configs() {
    return null;
  }

  @Override
  public void timeout(long timeoutMs) {

  }

  @Override
  public Set<TopicPartition> assignment() {
    return null;
  }

  @Override
  public void requestCommit() {

  }
}
