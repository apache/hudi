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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.HudiTransactionParticipant;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.helper.MockKafkaControlAgent;
import org.apache.hudi.helper.TestHudiWriter;
import org.apache.hudi.helper.TestSinkTaskContext;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHudiTransactionParticipant {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int PARTITION_NUMBER = 4;
  private static final int NUM_RECORDS_BATCH = 5;
  private static final int MAX_COMMIT_ROUNDS = 5;

  private HudiTransactionParticipant participant;
  private MockCoordinator coordinator;
  private TopicPartition partition;
  private HudiConnectConfigs configs;
  private KafkaControlAgent kafkaControlAgent;
  private TestHudiWriter testHudiWriter;
  private TestSinkTaskContext taskContext;
  private int numberKafkaRecordsConsumed;
  private long currentKafkaOffset;
  private int currentCommitTime;

  @BeforeEach
  public void setUp() throws Exception {
    kafkaControlAgent = new MockKafkaControlAgent();
    coordinator = new MockCoordinator(kafkaControlAgent);
    coordinator.start();
    partition = new TopicPartition(TOPIC_NAME, PARTITION_NUMBER);
    testHudiWriter = new TestHudiWriter();
    taskContext = new TestSinkTaskContext(partition);
    configs = HudiConnectConfigs.newBuilder()
        .build();
    numberKafkaRecordsConsumed = 0;
    currentKafkaOffset = 0L;
    currentCommitTime = 100;
    initializeParticipant();
  }

  @ParameterizedTest
  @EnumSource(value = CoordinatorFailureTestScenarios.class)
  public void testAllCoordinatorFailureScenarios(CoordinatorFailureTestScenarios testScenario) {
    for (int round = 0; round <= MAX_COMMIT_ROUNDS; round++) {
      switch (testScenario) {
        case REGULAR_SCENARIO:
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        case COORDINATOR_FAILED_AFTER_START_COMMIT:
          testPostStartCommit();
          // Coordinator fails
          coordinator = new MockCoordinator(kafkaControlAgent);
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        case COORDINATOR_FAILED_BEFORE_COMMITTING:
          testPostStartCommit();
          testPostEndCommit(false);
          // Coordinator fails
          coordinator.getCommittedKafkaOffset();
          coordinator = new MockCoordinator(kafkaControlAgent);
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        case COORDINATOR_FAILED_AFTER_COMMITTING:
          testPostStartCommit();
          testPostEndCommit(false);
          // Coordinator fails
          coordinator = new MockCoordinator(kafkaControlAgent);
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }
    }

    participant.stop();
  }

  @ParameterizedTest
  @EnumSource(value = ParticipantFailureTestScenarios.class)
  public void testAllParticipantFailureScenarios(ParticipantFailureTestScenarios testScenario) {
    switch (testScenario) {
      case FAILURE_BEFORE_START_COMMIT:
        // Participant fails
        reinitializeParticipant(0);
        break;
      case FAILURE_AFTER_START_COMMIT:
        testPostStartCommit();
        // Participant fails
        reinitializeParticipant(testHudiWriter.getNumberRecords());
        testPostEndCommit(true);
        testPostAckCommit(true);
        break;
      case FAILURE_AFTER_END_COMMIT:
        testPostStartCommit();
        testPostEndCommit(false);
        // Participant fails
        reinitializeParticipant(testHudiWriter.getNumberRecords());
        testPostAckCommit(true);
        break;
      default:
        throw new HoodieException("Unknown test scenario " + testScenario);
    }
    // Participant should recover on the next START_COMMIT
    testPostStartCommit();
    testPostEndCommit(false);
    testPostAckCommit(false);
  }

  private void initializeParticipant() {
    reinitializeParticipant(0);
  }

  private void reinitializeParticipant(int numberKafkaRecordsConsumed) {
    this.numberKafkaRecordsConsumed = numberKafkaRecordsConsumed;
    participant = new HudiTransactionParticipant(
        configs,
        partition,
        kafkaControlAgent,
        taskContext,
        testHudiWriter);
    participant.start();
    assertTrue(taskContext.isPaused());

    putRecordsToParticipant();
    // The last batch of writes should not be written since they were consumed before START_COMMIT
    assertEquals(testHudiWriter.getNumberRecords(), numberKafkaRecordsConsumed);
  }

  private void testPostStartCommit() {
    // Firstly, ensure the committed Kafka Offsets are in sync with Participant
    putRecordsToParticipant();
    coordinator.sendEventFromCoordinator(
        ControlEvent.MsgType.START_COMMIT,
        String.valueOf(++currentCommitTime));
    putRecordsToParticipant();

    assertEquals(participant.getLastKafkaCommittedOffset(), coordinator.getCommittedKafkaOffset());
    assertFalse(taskContext.isPaused());
    // All the records consumed so far are now flushed to Hudi Writer since a START_COMMIT has been received
    assertEquals(testHudiWriter.getNumberRecords(), numberKafkaRecordsConsumed);
  }

  private void testPostEndCommit(boolean hasParticipantFailed) {
    int currentRecordsConsumed = testHudiWriter.getNumberRecords();
    putRecordsToParticipant();
    // The kafka offset of the last written record before an END_COMMIT should be sent to the leader
    long lastWrittenKafkaOffset = currentKafkaOffset;
    coordinator.sendEventFromCoordinator(
        ControlEvent.MsgType.END_COMMIT,
        String.valueOf(currentCommitTime));
    putRecordsToParticipant();

    assertTrue(taskContext.isPaused());
    if (hasParticipantFailed) {
      validateStateOnParticipantFailure(currentRecordsConsumed);
    } else {
      if (coordinator.getLastReceivedWriteStatusEvent().isPresent()) {
        validateWriteStatus(coordinator.getLastReceivedWriteStatusEvent().get(), lastWrittenKafkaOffset);
      } else {
        throw new HoodieException("Participant did not send a valid WS Event after receiving an END_COMMIT");
      }
      // The last batch of writes should not be written since they were consumed after END_COMMIT
      assertEquals(testHudiWriter.getNumberRecords(), currentRecordsConsumed + NUM_RECORDS_BATCH);
    }
  }

  private void testPostAckCommit(boolean hasParticipantFailed) {
    int currentRecordsConsumed = testHudiWriter.getNumberRecords();
    putRecordsToParticipant();
    coordinator.sendEventFromCoordinator(
        ControlEvent.MsgType.ACK_COMMIT,
        String.valueOf(currentCommitTime));
    putRecordsToParticipant();

    assertTrue(taskContext.isPaused());
    if (hasParticipantFailed) {
      validateStateOnParticipantFailure(currentRecordsConsumed);
    } else {
      // Ensure both batches of records are not written to Hudi since no records are written post END_COMMIT
      assertEquals(testHudiWriter.getNumberRecords(), currentRecordsConsumed);
    }
  }

  // The participant should ignore all events prior to receiving the next START_COMMIT,
  // hence should not have sent the WS and should not be consuming/ writing kafka records.
  private void validateStateOnParticipantFailure(int currentRecordsConsumed) {
    assertEquals(testHudiWriter.getNumberRecords(), currentRecordsConsumed);
  }

  private void putRecordsToParticipant() {
    for (int i = 1; i <= NUM_RECORDS_BATCH; i++) {
      participant.buffer(getNextKafkaRecord());
    }
    participant.processRecords();
  }

  private SinkRecord getNextKafkaRecord() {
    numberKafkaRecordsConsumed++;
    return new SinkRecord(partition.topic(), partition.partition(), Schema.OPTIONAL_BYTES_SCHEMA,
        ("key-" + currentKafkaOffset).getBytes(), Schema.OPTIONAL_BYTES_SCHEMA,
        "value".getBytes(), currentKafkaOffset++);
  }

  private void validateWriteStatus(ControlEvent event, long lastWrittenKafkaOffset) {
    assertEquals(event.getCommitTime(), String.valueOf(currentCommitTime));
    assertEquals(event.senderPartition(), partition);
    assertEquals(event.getParticipantInfo().getKafkaCommitOffset(), lastWrittenKafkaOffset);
  }

  private static class MockCoordinator implements TransactionCoordinator {

    private final KafkaControlAgent kafkaControlAgent;
    private final TopicPartition partition;

    private Option<ControlEvent> lastReceivedWriteStatusEvent;
    private long committedKafkaOffset;

    public MockCoordinator(KafkaControlAgent kafkaControlAgent) {
      this.kafkaControlAgent = kafkaControlAgent;
      partition = new TopicPartition(TOPIC_NAME, 0);
      lastReceivedWriteStatusEvent = Option.empty();
      committedKafkaOffset = 0L;
    }

    public void sendEventFromCoordinator(
        ControlEvent.MsgType type,
        String commitTime) {
      try {
        kafkaControlAgent.publishMessage(new ControlEvent.Builder(
            type,
            ControlEvent.SenderType.COORDINATOR,
            commitTime,
            partition)
            .setCoordinatorInfo(new ControlEvent.CoordinatorInfo(
                Collections.singletonMap(PARTITION_NUMBER, committedKafkaOffset)))
            .build());
      } catch (Exception exception) {
        throw new HoodieException("Fatal error sending control event to Participant");
      }
    }

    public Option<ControlEvent> getLastReceivedWriteStatusEvent() {
      return lastReceivedWriteStatusEvent;
    }

    public long getCommittedKafkaOffset() {
      return committedKafkaOffset;
    }

    @Override
    public void start() {
      kafkaControlAgent.registerTransactionCoordinator(this);
    }

    @Override
    public void stop() {
      kafkaControlAgent.deregisterTransactionCoordinator(this);
    }

    @Override
    public TopicPartition getPartition() {
      return partition;
    }

    @Override
    public void processControlEvent(ControlEvent message) {
      if (message.getMsgType().equals(ControlEvent.MsgType.WRITE_STATUS)) {
        lastReceivedWriteStatusEvent = Option.of(message);
        assertTrue(message.getParticipantInfo().getKafkaCommitOffset() >= committedKafkaOffset);

        // Mimic coordinator failure before committing the write and kafka offset
        committedKafkaOffset = message.getParticipantInfo().getKafkaCommitOffset();
      }
    }
  }

  private enum CoordinatorFailureTestScenarios {
    REGULAR_SCENARIO,
    COORDINATOR_FAILED_AFTER_START_COMMIT,
    COORDINATOR_FAILED_BEFORE_COMMITTING,
    COORDINATOR_FAILED_AFTER_COMMITTING
  }

  private enum ParticipantFailureTestScenarios {
    FAILURE_BEFORE_START_COMMIT,
    FAILURE_AFTER_START_COMMIT,
    FAILURE_AFTER_END_COMMIT,
  }

}
