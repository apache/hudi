package org.apache.hudi.helper;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Helper class that implements {@link SinkTaskContext}
 * for testing purposes.
 */
public class TestSinkTaskContext implements SinkTaskContext {

  private final TopicPartition testPartition;
  private boolean isPaused;

  public TestSinkTaskContext(TopicPartition testPartition) {
    this.testPartition = testPartition;
    this.isPaused = false;
  }

  public boolean isPaused() {
    return isPaused;
  }

  @Override
  public Map<String, String> configs() {
    return null;
  }

  @Override
  public void offset(Map<TopicPartition, Long> offsets) {

  }

  @Override
  public void offset(TopicPartition tp, long offset) {

  }

  @Override
  public void timeout(long timeoutMs) {

  }

  @Override
  public Set<TopicPartition> assignment() {
    return null;
  }

  @Override
  public void pause(TopicPartition... partitions) {
    if (Arrays.stream(partitions).allMatch(partition -> testPartition.equals(partition))) {
      isPaused = true;
    }
  }

  @Override
  public void resume(TopicPartition... partitions) {
    if (Arrays.stream(partitions).allMatch(partition -> testPartition.equals(partition))) {
      isPaused = false;
    }
  }

  @Override
  public void requestCommit() {

  }
}
