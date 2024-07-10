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
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.transaction.ConnectTransactionParticipant;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.helper.MockKafkaConnect;
import org.apache.hudi.helper.MockKafkaControlAgent;
import org.apache.hudi.helper.TestHudiWriterProvider;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConnectTransactionParticipant {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int NUM_RECORDS_BATCH = 5;
  private static final int PARTITION_NUMBER = 4;

  private ConnectTransactionParticipant participant;
  private MockCoordinator mockCoordinator;
  private TopicPartition partition;
  private KafkaConnectConfigs configs;
  private KafkaControlAgent kafkaControlAgent;
  private TestHudiWriterProvider testHudiWriterProvider;
  private MockKafkaConnect mockKafkaConnect;

  @BeforeEach
  public void setUp() throws Exception {
    partition = new TopicPartition(TOPIC_NAME, PARTITION_NUMBER);
    kafkaControlAgent = new MockKafkaControlAgent();
    mockKafkaConnect = new MockKafkaConnect(partition);
    mockCoordinator = new MockCoordinator(kafkaControlAgent);
    mockCoordinator.start();
    configs = KafkaConnectConfigs.newBuilder()
        .build();
    initializeParticipant();
  }

  @ParameterizedTest
  @EnumSource(value = CoordinatorFailureTestScenarios.class)
  public void testAllCoordinatorFailureScenarios(CoordinatorFailureTestScenarios testScenario) {
    try {
      assertTrue(mockKafkaConnect.isPaused());
      switch (testScenario) {
        case REGULAR_SCENARIO:
          break;
        case COORDINATOR_FAILED_AFTER_START_COMMIT:
          triggerAndProcessStartCommit();
          // Coordinator Failed
          initializeCoordinator();
          break;
        case COORDINATOR_FAILED_AFTER_END_COMMIT:
          triggerAndProcessStartCommit();
          triggerAndProcessEndCommit();
          // Coordinator Failed
          initializeCoordinator();
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }

      // Despite failures in the previous commit, a fresh 2-phase commit should PASS.
      testTwoPhaseCommit(0);
    } catch (Exception exception) {
      throw new HoodieException("Unexpected test failure ", exception);
    }
    participant.stop();
  }

  @ParameterizedTest
  @EnumSource(value = ParticipantFailureTestScenarios.class)
  public void testAllParticipantFailureScenarios(ParticipantFailureTestScenarios testScenario) {
    try {
      int currentKafkaOffset = 0;
      switch (testScenario) {
        case FAILURE_BEFORE_START_COMMIT:
          // Participant failing after START_COMMIT will not write any data in this commit cycle.
          initializeParticipant();
          break;
        case FAILURE_AFTER_START_COMMIT:
          triggerAndProcessStartCommit();
          // Participant failing after START_COMMIT will not write any data in this commit cycle.
          initializeParticipant();
          triggerAndProcessEndCommit();
          triggerAndProcessAckCommit();
          break;
        case FAILURE_AFTER_END_COMMIT:
          // Regular Case or Coordinator Recovery Case
          triggerAndProcessStartCommit();
          triggerAndProcessEndCommit();
          initializeParticipant();
          triggerAndProcessAckCommit();

          // Participant failing after and END_COMMIT should not cause issues with the present commit,
          // since the data would have been written by previous participant before failing
          // and hence moved the kafka offset.
          currentKafkaOffset = NUM_RECORDS_BATCH;
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }

      // Despite failures in the previous commit, a fresh 2-phase commit should PASS.
      testTwoPhaseCommit(currentKafkaOffset);
    } catch (Exception exception) {
      throw new HoodieException("Unexpected test failure ", exception);
    }
  }

  private void initializeParticipant() {
    testHudiWriterProvider = new TestHudiWriterProvider();
    participant = new ConnectTransactionParticipant(
        partition,
        kafkaControlAgent,
        mockKafkaConnect,
        testHudiWriterProvider);
    mockKafkaConnect.setParticipant(participant);
    participant.start();
  }

  private void initializeCoordinator() {
    mockCoordinator = new MockCoordinator(kafkaControlAgent);
    mockCoordinator.start();
  }

  // Test and validate result of a single 2 Phase commit from START_COMMIT to ACK_COMMIT.
  // Validates that NUM_RECORDS_BATCH number of kafka records are written,
  // and the kafka offset only increments by NUM_RECORDS_BATCH.
  private void testTwoPhaseCommit(long currentKafkaOffset) {
    triggerAndProcessStartCommit();
    triggerAndProcessEndCommit();
    triggerAndProcessAckCommit();

    // Validate records written, current kafka offset and kafka offsets committed across
    // coordinator and participant are in sync despite failure scenarios.
    assertEquals(NUM_RECORDS_BATCH, testHudiWriterProvider.getLatestNumberWrites());
    assertEquals((currentKafkaOffset + NUM_RECORDS_BATCH), mockKafkaConnect.getCurrentKafkaOffset());
    // Ensure Coordinator and participant are in sync in the kafka offsets
    assertEquals(participant.getLastKafkaCommittedOffset(), mockCoordinator.getCommittedKafkaOffset());
  }

  private void triggerAndProcessStartCommit() {
    mockCoordinator.sendEventFromCoordinator(ControlMessage.EventType.START_COMMIT);
    mockKafkaConnect.publishBatchRecordsToParticipant(NUM_RECORDS_BATCH);
    assertTrue(mockKafkaConnect.isResumed());
  }

  private void triggerAndProcessEndCommit() {
    mockCoordinator.sendEventFromCoordinator(ControlMessage.EventType.END_COMMIT);
    mockKafkaConnect.publishBatchRecordsToParticipant(0);
    assertTrue(mockKafkaConnect.isPaused());
  }

  private void triggerAndProcessAckCommit() {
    mockCoordinator.sendEventFromCoordinator(ControlMessage.EventType.ACK_COMMIT);
    mockKafkaConnect.publishBatchRecordsToParticipant(0);
    assertTrue(mockKafkaConnect.isPaused());
  }

  private static class MockCoordinator implements TransactionCoordinator {

    private static int currentCommitTime;

    static {
      currentCommitTime = 101;
    }

    private final KafkaControlAgent kafkaControlAgent;
    private final TopicPartition partition;

    private Option<ControlMessage> lastReceivedWriteStatusEvent;
    private long committedKafkaOffset;

    public MockCoordinator(KafkaControlAgent kafkaControlAgent) {
      this.kafkaControlAgent = kafkaControlAgent;
      partition = new TopicPartition(TOPIC_NAME, 0);
      lastReceivedWriteStatusEvent = Option.empty();
      committedKafkaOffset = 0L;
    }

    public void sendEventFromCoordinator(ControlMessage.EventType type) {
      try {
        if (type.equals(ControlMessage.EventType.START_COMMIT)) {
          ++currentCommitTime;
        }
        kafkaControlAgent.publishMessage(
            ControlMessage.newBuilder()
                .setType(type)
                .setTopicName(partition.topic())
                .setSenderType(ControlMessage.EntityType.COORDINATOR)
                .setSenderPartition(partition.partition())
                .setReceiverType(ControlMessage.EntityType.PARTICIPANT)
                .setCommitTime(String.valueOf(currentCommitTime))
                .setCoordinatorInfo(
                    ControlMessage.CoordinatorInfo.newBuilder()
                        .putAllGlobalKafkaCommitOffsets(Collections.singletonMap(PARTITION_NUMBER, committedKafkaOffset))
                        .build()
                ).build());
      } catch (Exception exception) {
        throw new HoodieException("Fatal error sending control event to Participant");
      }
    }

    public Option<ControlMessage> getLastReceivedWriteStatusEvent() {
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
    public void processControlEvent(ControlMessage message) {
      if (message.getType().equals(ControlMessage.EventType.WRITE_STATUS)) {
        lastReceivedWriteStatusEvent = Option.of(message);
        assertTrue(message.getParticipantInfo().getKafkaOffset() >= committedKafkaOffset);
        committedKafkaOffset = message.getParticipantInfo().getKafkaOffset();
      }
    }
  }

  private enum CoordinatorFailureTestScenarios {
    REGULAR_SCENARIO,
    COORDINATOR_FAILED_AFTER_START_COMMIT,
    COORDINATOR_FAILED_AFTER_END_COMMIT,
  }

  private enum ParticipantFailureTestScenarios {
    FAILURE_BEFORE_START_COMMIT,
    FAILURE_AFTER_START_COMMIT,
    FAILURE_AFTER_END_COMMIT,
  }
}
