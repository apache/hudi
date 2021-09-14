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
import org.apache.hudi.connect.transaction.ControlEvent;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.helper.MockKafkaControlAgent;
import org.apache.hudi.helper.TestHudiWriterProvider;
import org.apache.hudi.helper.TestKafkaConnect;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConnectTransactionParticipant {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int PARTITION_NUMBER = 4;

  private ConnectTransactionParticipant participant;
  private MockCoordinator coordinator;
  private TopicPartition partition;
  private KafkaConnectConfigs configs;
  private KafkaControlAgent kafkaControlAgent;
  private TestHudiWriterProvider testHudiWriterProvider;
  private TestKafkaConnect testKafkaConnect;

  @BeforeEach
  public void setUp() throws Exception {
    partition = new TopicPartition(TOPIC_NAME, PARTITION_NUMBER);
    kafkaControlAgent = new MockKafkaControlAgent();
    testKafkaConnect = new TestKafkaConnect(partition);
    coordinator = new MockCoordinator(kafkaControlAgent);
    coordinator.start();
    configs = KafkaConnectConfigs.newBuilder()
        .build();
    initializeParticipant();
  }

  @ParameterizedTest
  @EnumSource(value = CoordinatorFailureTestScenarios.class)
  public void testAllCoordinatorFailureScenarios(CoordinatorFailureTestScenarios testScenario) {
    int expectedRecordsWritten = 0;
    try {
      switch (testScenario) {
        case REGULAR_SCENARIO:
          expectedRecordsWritten += testKafkaConnect.putRecordsToParticipant();
          assertTrue(testKafkaConnect.isPaused());
          break;
        case COORDINATOR_FAILED_AFTER_START_COMMIT:
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          // Coordinator Failed
          initializeCoordinator();
          break;
        case COORDINATOR_FAILED_AFTER_END_COMMIT:
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
          expectedRecordsWritten += testKafkaConnect.putRecordsToParticipant();
          // Coordinator Failed
          initializeCoordinator();
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }

      // Regular Case or Coordinator Recovery Case
      coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
      expectedRecordsWritten += testKafkaConnect.putRecordsToParticipant();
      assertTrue(testKafkaConnect.isResumed());
      coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
      testKafkaConnect.putRecordsToParticipant();
      assertTrue(testKafkaConnect.isPaused());
      coordinator.sendEventFromCoordinator(ControlEvent.MsgType.ACK_COMMIT);
      testKafkaConnect.putRecordsToParticipant();
      assertEquals(testHudiWriterProvider.getLatestNumberWrites(), expectedRecordsWritten);
      // Ensure Coordinator and participant are in sync in the kafka offsets
      assertEquals(participant.getLastKafkaCommittedOffset(), coordinator.getCommittedKafkaOffset());
    } catch (Exception exception) {
      throw new HoodieException("Unexpected test failure ", exception);
    }
    participant.stop();
  }

  @ParameterizedTest
  @EnumSource(value = ParticipantFailureTestScenarios.class)
  public void testAllParticipantFailureScenarios(ParticipantFailureTestScenarios testScenario) {
    int expectedRecordsWritten = 0;
    try {
      switch (testScenario) {
        case FAILURE_BEFORE_START_COMMIT:
          testKafkaConnect.putRecordsToParticipant();
          // Participant fails
          initializeParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
          expectedRecordsWritten += testKafkaConnect.putRecordsToParticipant();
          assertTrue(testKafkaConnect.isResumed());
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertTrue(testKafkaConnect.isPaused());
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.ACK_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertEquals(testHudiWriterProvider.getLatestNumberWrites(), expectedRecordsWritten);
          // Ensure Coordinator and participant are in sync in the kafka offsets
          assertEquals(participant.getLastKafkaCommittedOffset(), coordinator.getCommittedKafkaOffset());
          break;
        case FAILURE_AFTER_START_COMMIT:
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          // Participant fails
          initializeParticipant();
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertTrue(testKafkaConnect.isPaused());
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.ACK_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertEquals(testHudiWriterProvider.getLatestNumberWrites(), expectedRecordsWritten);
          // Ensure Coordinator and participant are in sync in the kafka offsets
          assertEquals(participant.getLastKafkaCommittedOffset(), coordinator.getCommittedKafkaOffset());
          break;
        case FAILURE_AFTER_END_COMMIT:
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.START_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          // Participant fails
          initializeParticipant();
          testKafkaConnect.putRecordsToParticipant();
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.END_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertTrue(testKafkaConnect.isPaused());
          coordinator.sendEventFromCoordinator(ControlEvent.MsgType.ACK_COMMIT);
          testKafkaConnect.putRecordsToParticipant();
          assertEquals(testHudiWriterProvider.getLatestNumberWrites(), expectedRecordsWritten);
          // Ensure Coordinator and participant are in sync in the kafka offsets
          assertEquals(participant.getLastKafkaCommittedOffset(), coordinator.getCommittedKafkaOffset());
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }
    } catch (Exception exception) {
      throw new HoodieException("Unexpected test failure ", exception);
    }
  }

  private void initializeParticipant() {
    testHudiWriterProvider = new TestHudiWriterProvider();
    participant = new ConnectTransactionParticipant(
        partition,
        kafkaControlAgent,
        testKafkaConnect,
        testHudiWriterProvider);
    testKafkaConnect.setParticipant(participant);
    participant.start();
  }

  private void initializeCoordinator() {
    coordinator = new MockCoordinator(kafkaControlAgent);
    coordinator.start();
  }

  private static class MockCoordinator implements TransactionCoordinator {

    private static int currentCommitTime;

    static {
      currentCommitTime = 101;
    }

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
        ControlEvent.MsgType type) {
      try {
        if (type.equals(ControlEvent.MsgType.START_COMMIT)) {
          ++currentCommitTime;
        }
        kafkaControlAgent.publishMessage(new ControlEvent.Builder(
            type,
            ControlEvent.SenderType.COORDINATOR,
            String.valueOf(currentCommitTime),
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
        committedKafkaOffset = message.getParticipantInfo().getKafkaCommitOffset();
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
