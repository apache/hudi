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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.transaction.ConnectTransactionCoordinator;
import org.apache.hudi.connect.transaction.ControlEvent;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.helper.MockConnectTransactionServices;
import org.apache.hudi.helper.MockKafkaControlAgent;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestConnectTransactionCoordinator {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int NUM_PARTITIONS = 4;
  private static final int MAX_COMMIT_ROUNDS = 5;
  private static final int TEST_TIMEOUT_SECS = 60;

  private KafkaConnectConfigs configs;
  private MockParticipant participant;
  private MockKafkaControlAgent kafkaControlAgent;
  private MockConnectTransactionServices transactionServices;
  private CountDownLatch latch;

  @BeforeEach
  public void setUp() throws Exception {
    transactionServices = new MockConnectTransactionServices();
    configs = KafkaConnectConfigs.newBuilder()
        .withCommitIntervalSecs(1L)
        .withCoordinatorWriteTimeoutSecs(1L)
        .build();
    latch = new CountDownLatch(1);
  }

  @ParameterizedTest
  @EnumSource(value = MockParticipant.TestScenarios.class)
  public void testSingleCommitScenario(MockParticipant.TestScenarios scenario) throws InterruptedException {
    kafkaControlAgent = new MockKafkaControlAgent();
    participant = new MockParticipant(kafkaControlAgent, latch, scenario, MAX_COMMIT_ROUNDS);
    participant.start();

    // Test the coordinator using the mock participant
    TransactionCoordinator coordinator = new ConnectTransactionCoordinator(
        configs,
        new TopicPartition(TOPIC_NAME, 0),
        kafkaControlAgent,
        transactionServices,
        (bootstrapServers, topicName) -> NUM_PARTITIONS);
    coordinator.start();

    latch.await(TEST_TIMEOUT_SECS, TimeUnit.SECONDS);

    if (latch.getCount() > 0) {
      throw new HoodieException("Test timedout resulting in failure");
    }
    coordinator.stop();
    participant.stop();
  }

  /**
   * A mock Transaction Participant, that exercises all the test scenarios
   * for the coordinator as mentioned in {@link TestScenarios}.
   */
  private static class MockParticipant implements TransactionParticipant {

    private final MockKafkaControlAgent kafkaControlAgent;
    private final TopicPartition partition;
    private final CountDownLatch latch;
    private final TestScenarios testScenario;
    private final int maxNumberCommitRounds;
    private final Map<Integer, Long> kafkaOffsetsCommitted;

    private ControlEvent.MsgType expectedMsgType;
    private int numberCommitRounds;

    public MockParticipant(MockKafkaControlAgent kafkaControlAgent,
                           CountDownLatch latch,
                           TestScenarios testScenario,
                           int maxNumberCommitRounds) {
      this.kafkaControlAgent = kafkaControlAgent;
      this.latch = latch;
      this.testScenario = testScenario;
      this.maxNumberCommitRounds = maxNumberCommitRounds;
      this.partition = new TopicPartition(TOPIC_NAME, (NUM_PARTITIONS - 1));
      this.kafkaOffsetsCommitted = new HashMap<>();
      expectedMsgType = ControlEvent.MsgType.START_COMMIT;
      numberCommitRounds = 0;
    }

    @Override
    public void start() {
      kafkaControlAgent.registerTransactionParticipant(this);
    }

    @Override
    public void stop() {
      kafkaControlAgent.deregisterTransactionParticipant(this);
    }

    @Override
    public void buffer(SinkRecord record) {
    }

    @Override
    public void processRecords() {
    }

    @Override
    public TopicPartition getPartition() {
      return partition;
    }

    @Override
    public void processControlEvent(ControlEvent message) {
      assertEquals(message.getSenderType(), ControlEvent.SenderType.COORDINATOR);
      assertEquals(message.senderPartition().topic(), partition.topic());
      testScenarios(message);
    }

    @Override
    public long getLastKafkaCommittedOffset() {
      return 0;
    }

    private void testScenarios(ControlEvent message) {
      assertEquals(expectedMsgType, message.getMsgType());

      switch (message.getMsgType()) {
        case START_COMMIT:
          expectedMsgType = ControlEvent.MsgType.END_COMMIT;
          break;
        case END_COMMIT:
          assertEquals(kafkaOffsetsCommitted, message.getCoordinatorInfo().getGlobalKafkaCommitOffsets());
          int numSuccessPartitions;
          Map<Integer, Long> kafkaOffsets = new HashMap<>();
          List<ControlEvent> controlEvents = new ArrayList<>();
          // Prepare the WriteStatuses for all partitions
          for (int i = 1; i <= NUM_PARTITIONS; i++) {
            try {
              long kafkaOffset = (long) (Math.random() * 10000);
              kafkaOffsets.put(i, kafkaOffset);
              ControlEvent event = successWriteStatus(
                  message.getCommitTime(),
                  new TopicPartition(TOPIC_NAME, i),
                  kafkaOffset);
              controlEvents.add(event);
            } catch (Exception exception) {
              throw new HoodieException("Fatal error sending control event to Coordinator");
            }
          }

          switch (testScenario) {
            case ALL_CONNECT_TASKS_SUCCESS:
              numSuccessPartitions = NUM_PARTITIONS;
              kafkaOffsetsCommitted.putAll(kafkaOffsets);
              expectedMsgType = ControlEvent.MsgType.ACK_COMMIT;
              break;
            case SUBSET_CONNECT_TASKS_FAILED:
              numSuccessPartitions = NUM_PARTITIONS / 2;
              expectedMsgType = ControlEvent.MsgType.START_COMMIT;
              break;
            default:
              throw new HoodieException("Unknown test scenario " + testScenario);
          }

          // Send events based on test scenario
          for (int i = 0; i < numSuccessPartitions; i++) {
            kafkaControlAgent.publishMessage(controlEvents.get(i));
          }
          break;
        case ACK_COMMIT:
          if (numberCommitRounds >= maxNumberCommitRounds) {
            latch.countDown();
          }
          expectedMsgType = ControlEvent.MsgType.START_COMMIT;
          break;
        default:
          throw new HoodieException("Illegal control message type " + message.getMsgType());
      }

      if (message.getMsgType().equals(ControlEvent.MsgType.START_COMMIT)) {
        if (numberCommitRounds >= maxNumberCommitRounds) {
          latch.countDown();
        }
        numberCommitRounds++;
        expectedMsgType = ControlEvent.MsgType.END_COMMIT;
      }
    }

    public enum TestScenarios {
      SUBSET_CONNECT_TASKS_FAILED,
      ALL_CONNECT_TASKS_SUCCESS
    }

    private static ControlEvent successWriteStatus(String commitTime,
                                                   TopicPartition partition,
                                                   long kafkaOffset) throws Exception {
      // send WS
      WriteStatus writeStatus = new WriteStatus();
      WriteStatus status = new WriteStatus(false, 1.0);
      for (int i = 0; i < 1000; i++) {
        status.markSuccess(mock(HoodieRecord.class), Option.empty());
      }
      return new ControlEvent.Builder(ControlEvent.MsgType.WRITE_STATUS,
          ControlEvent.SenderType.PARTICIPANT,
          commitTime,
          partition)
          .setParticipantInfo(new ControlEvent.ParticipantInfo(
              Collections.singletonList(writeStatus),
              kafkaOffset,
              ControlEvent.OutcomeType.WRITE_SUCCESS))
          .build();
    }
  }
}
