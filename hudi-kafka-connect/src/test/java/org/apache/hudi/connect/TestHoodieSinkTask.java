/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.connect;

import org.apache.hudi.connect.transaction.ConnectTransactionCoordinator;
import org.apache.hudi.connect.transaction.ConnectTransactionParticipant;
import org.apache.hudi.connect.transaction.TransactionParticipant;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the secondary {@code topic -> partition -> participant} index used by
 * {@link HoodieSinkTask#put} stays in sync with the primary {@code transactionParticipants}
 * map across every path that mutates it: open, close and stop.
 */
public class TestHoodieSinkTask {

  private static final String TOPIC_A = "kafka-connect-test-topic-a";
  private static final String TOPIC_B = "kafka-connect-test-topic-b";

  private static final TopicPartition TOPIC_A_COORDINATOR =
      new TopicPartition(TOPIC_A, ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION);
  private static final TopicPartition TOPIC_A_1 = new TopicPartition(TOPIC_A, 1);
  private static final TopicPartition TOPIC_B_1 = new TopicPartition(TOPIC_B, 1);

  private final Map<TopicPartition, ConnectTransactionParticipant> participantMocks = new HashMap<>();
  private final Set<TopicPartition> assignment = new HashSet<>();

  private HoodieSinkTask task;
  private MockedConstruction<ConnectTransactionParticipant> mockedParticipants;
  private MockedConstruction<ConnectTransactionCoordinator> mockedCoordinators;

  @BeforeEach
  public void setUp() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);
    // The real participant and coordinator constructors reach out to a Hudi write client and to Kafka,
    // so intercept them to keep this a unit test of the routing maps alone.
    mockedParticipants = mockConstruction(ConnectTransactionParticipant.class,
        (participant, ctx) -> participantMocks.put((TopicPartition) ctx.arguments().get(1), participant));
    mockedCoordinators = mockConstruction(ConnectTransactionCoordinator.class);
    task = new HoodieSinkTask();
    task.initialize(context);
  }

  @AfterEach
  public void tearDown() {
    mockedCoordinators.close();
    mockedParticipants.close();
  }

  @Test
  public void testOpenIndexesEveryParticipant() {
    open(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);

    assertIndexMirrorsParticipants();
    assertRoutedToOwnParticipant(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);
  }

  @Test
  public void testCloseDeindexesOnlyTheClosedPartition() {
    open(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);

    close(TOPIC_A_1);

    assertIndexMirrorsParticipants();
    assertNotRouted(TOPIC_A_1);
    // The rest of topic A is still indexed, so closing one partition must not evict the whole topic.
    assertRoutedToOwnParticipant(TOPIC_A_COORDINATOR, TOPIC_B_1);
  }

  @Test
  public void testCloseOfLastPartitionDropsTheTopicEntry() {
    open(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);

    // Closing the coordinator partition takes a different branch through close(), so cover it here too.
    close(TOPIC_A_COORDINATOR, TOPIC_A_1);

    assertIndexMirrorsParticipants();
    assertFalse(index().containsKey(TOPIC_A), "index kept an entry for a topic with no open partitions");
    assertNotRouted(TOPIC_A_COORDINATOR, TOPIC_A_1);
    assertRoutedToOwnParticipant(TOPIC_B_1);
  }

  @Test
  public void testStopClearsTheIndex() {
    open(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);

    task.stop();

    assertIndexMirrorsParticipants();
    assertTrue(index().isEmpty(), "index survived stop()");
    assignment.clear();
    assertNotRouted(TOPIC_A_COORDINATOR, TOPIC_A_1, TOPIC_B_1);
  }

  @Test
  public void testReopenAfterCloseReindexesTheParticipant() {
    open(TOPIC_A_1);
    ConnectTransactionParticipant firstParticipant = participantMocks.get(TOPIC_A_1);
    close(TOPIC_A_1);

    open(TOPIC_A_1);

    assertIndexMirrorsParticipants();
    // Records must reach the participant created by the second open, not the stale one.
    SinkRecord record = newRecord(TOPIC_A_1);
    task.put(Collections.singletonList(record));
    verify(firstParticipant, never()).buffer(record);
    verify(participantMocks.get(TOPIC_A_1)).buffer(record);
  }

  @Test
  public void testPutIgnoresRecordsForUnknownTopicPartition() {
    open(TOPIC_A_1);

    // Neither an unknown topic nor an unknown partition of a known topic may reach a participant.
    task.put(Arrays.asList(
        newRecord(new TopicPartition("no-such-topic", 1)),
        newRecord(new TopicPartition(TOPIC_A, 42))));

    verify(participantMocks.get(TOPIC_A_1), never()).buffer(any());
  }

  /**
   * The invariant under test: flattening the secondary index back to {@link TopicPartition} keys must
   * reproduce {@code transactionParticipants} exactly, with no empty per-topic buckets left behind.
   */
  private void assertIndexMirrorsParticipants() {
    Map<TopicPartition, TransactionParticipant> flattened = new HashMap<>();
    for (Map.Entry<String, Map<Integer, TransactionParticipant>> topicEntry : index().entrySet()) {
      assertFalse(topicEntry.getValue().isEmpty(), "index kept an empty bucket for topic " + topicEntry.getKey());
      for (Map.Entry<Integer, TransactionParticipant> partitionEntry : topicEntry.getValue().entrySet()) {
        flattened.put(new TopicPartition(topicEntry.getKey(), partitionEntry.getKey()), partitionEntry.getValue());
      }
    }
    assertEquals(participants(), flattened, "secondary index drifted from transactionParticipants");
  }

  private void assertRoutedToOwnParticipant(TopicPartition... partitions) {
    for (TopicPartition partition : partitions) {
      SinkRecord record = newRecord(partition);
      task.put(Collections.singletonList(record));
      verify(participantMocks.get(partition)).buffer(record);
    }
  }

  private void assertNotRouted(TopicPartition... partitions) {
    for (TopicPartition partition : partitions) {
      SinkRecord record = newRecord(partition);
      task.put(Collections.singletonList(record));
      verify(participantMocks.get(partition), never()).buffer(record);
    }
  }

  private void open(TopicPartition... partitions) {
    List<TopicPartition> opened = Arrays.asList(partitions);
    assignment.addAll(opened);
    task.open(opened);
  }

  private void close(TopicPartition... partitions) {
    List<TopicPartition> closed = Arrays.asList(partitions);
    assignment.removeAll(closed);
    task.close(closed);
  }

  private static SinkRecord newRecord(TopicPartition partition) {
    return new SinkRecord(partition.topic(), partition.partition(), null, null, null, "value", 0L);
  }

  private Map<TopicPartition, TransactionParticipant> participants() {
    return readField("transactionParticipants");
  }

  private Map<String, Map<Integer, TransactionParticipant>> index() {
    return readField("participantsByTopicPartition");
  }

  @SuppressWarnings("unchecked")
  private <T> T readField(String name) {
    try {
      Field field = HoodieSinkTask.class.getDeclaredField(name);
      field.setAccessible(true);
      return (T) field.get(task);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Unable to read HoodieSinkTask." + name, e);
    }
  }
}
