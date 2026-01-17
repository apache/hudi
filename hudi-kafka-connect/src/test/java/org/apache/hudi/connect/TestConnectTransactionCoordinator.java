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
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.helper.MockConnectTransactionServices;
import org.apache.hudi.helper.MockKafkaControlAgent;

import lombok.Getter;
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
  private static final int TOTAL_KAFKA_PARTITIONS = 4;
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
    latch = new CountDownLatch(1);
  }

  @ParameterizedTest
  @EnumSource(value = MockParticipant.TestScenarios.class)
  public void testSingleCommitScenario(MockParticipant.TestScenarios scenario) throws InterruptedException {
    kafkaControlAgent = new MockKafkaControlAgent();
    participant = new MockParticipant(kafkaControlAgent, latch, scenario, MAX_COMMIT_ROUNDS);
    participant.start();

    KafkaConnectConfigs.Builder configBuilder = KafkaConnectConfigs.newBuilder()
        .withCommitIntervalSecs(1L)
        .withCoordinatorWriteTimeoutSecs(1L);

    if (scenario.equals(MockParticipant.TestScenarios.SUBSET_WRITE_STATUS_FAILED)) {
      configBuilder.withAllowCommitOnErrors(false);
    }
    configs = configBuilder.build();

    // Test the coordinator using the mock participant
    TransactionCoordinator coordinator = new ConnectTransactionCoordinator(
        configs,
        new TopicPartition(TOPIC_NAME, 0),
        kafkaControlAgent,
        transactionServices,
        (bootstrapServers, topicName) -> TOTAL_KAFKA_PARTITIONS);
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
    @Getter
    private final TopicPartition partition;
    private final CountDownLatch latch;
    private final TestScenarios testScenario;
    private final int maxNumberCommitRounds;
    private final Map<Integer, Long> kafkaOffsetsCommitted;

    private ControlMessage.EventType expectedMsgType;
    private int numberCommitRounds;

    public MockParticipant(MockKafkaControlAgent kafkaControlAgent,
                           CountDownLatch latch,
                           TestScenarios testScenario,
                           int maxNumberCommitRounds) {
      this.kafkaControlAgent = kafkaControlAgent;
      this.latch = latch;
      this.testScenario = testScenario;
      this.maxNumberCommitRounds = maxNumberCommitRounds;
      this.partition = new TopicPartition(TOPIC_NAME, (TOTAL_KAFKA_PARTITIONS - 1));
      this.kafkaOffsetsCommitted = new HashMap<>();
      expectedMsgType = ControlMessage.EventType.START_COMMIT;
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
    public void processControlEvent(ControlMessage message) {
      assertEquals(message.getSenderType(), ControlMessage.EntityType.COORDINATOR);
      assertEquals(message.getTopicName(), partition.topic());
      testScenarios(message);
    }

    @Override
    public long getLastKafkaCommittedOffset() {
      return 0;
    }

    private void testScenarios(ControlMessage message) {
      assertEquals(expectedMsgType, message.getType());
      switch (message.getType()) {
        case START_COMMIT:
          expectedMsgType = ControlMessage.EventType.END_COMMIT;
          break;
        case END_COMMIT:
          assertEquals(kafkaOffsetsCommitted, message.getCoordinatorInfo().getGlobalKafkaCommitOffsets());
          int numPartitionsThatReportWriteStatus;
          Map<Integer, Long> kafkaOffsets = new HashMap<>();
          List<ControlMessage> controlEvents = new ArrayList<>();
          switch (testScenario) {
            case ALL_CONNECT_TASKS_SUCCESS:
              composeControlEvent(
                  message.getCommitTime(), false, false, kafkaOffsets, controlEvents);
              numPartitionsThatReportWriteStatus = TOTAL_KAFKA_PARTITIONS;
              // This commit round should succeed, and the kafka offsets getting committed
              kafkaOffsetsCommitted.putAll(kafkaOffsets);
              expectedMsgType = ControlMessage.EventType.ACK_COMMIT;
              break;
            case ALL_CONNECT_TASKS_WITH_EMPTY_WRITE_STATUS:
              composeControlEvent(
                  message.getCommitTime(), false, true, kafkaOffsets, controlEvents);
              numPartitionsThatReportWriteStatus = TOTAL_KAFKA_PARTITIONS;
              // This commit round should succeed, and the kafka offsets getting committed
              kafkaOffsetsCommitted.putAll(kafkaOffsets);
              expectedMsgType = ControlMessage.EventType.ACK_COMMIT;
              break;
            case SUBSET_WRITE_STATUS_FAILED_BUT_IGNORED:
              composeControlEvent(
                  message.getCommitTime(), true, false, kafkaOffsets, controlEvents);
              numPartitionsThatReportWriteStatus = TOTAL_KAFKA_PARTITIONS;
              // Despite error records, this commit round should succeed, and the kafka offsets getting committed
              kafkaOffsetsCommitted.putAll(kafkaOffsets);
              expectedMsgType = ControlMessage.EventType.ACK_COMMIT;
              break;
            case SUBSET_WRITE_STATUS_FAILED:
              composeControlEvent(
                  message.getCommitTime(), true, false, kafkaOffsets, controlEvents);
              numPartitionsThatReportWriteStatus = TOTAL_KAFKA_PARTITIONS;
              // This commit round should fail, and a new commit round should start without kafka offsets getting committed
              expectedMsgType = ControlMessage.EventType.START_COMMIT;
              break;
            case SUBSET_CONNECT_TASKS_FAILED:
              composeControlEvent(
                  message.getCommitTime(), false, false, kafkaOffsets, controlEvents);
              numPartitionsThatReportWriteStatus = TOTAL_KAFKA_PARTITIONS / 2;
              // This commit round should fail, and a new commit round should start without kafka offsets getting committed
              expectedMsgType = ControlMessage.EventType.START_COMMIT;
              break;
            default:
              throw new HoodieException("Unknown test scenario " + testScenario);
          }

          // Send events based on test scenario
          for (int i = 0; i < numPartitionsThatReportWriteStatus; i++) {
            kafkaControlAgent.publishMessage(controlEvents.get(i));
          }
          break;
        case ACK_COMMIT:
          if (numberCommitRounds >= maxNumberCommitRounds) {
            latch.countDown();
          }
          expectedMsgType = ControlMessage.EventType.START_COMMIT;
          break;
        default:
          throw new HoodieException("Illegal control message type " + message.getType());
      }

      if (message.getType().equals(ControlMessage.EventType.START_COMMIT)) {
        if (numberCommitRounds >= maxNumberCommitRounds) {
          latch.countDown();
        }
        numberCommitRounds++;
        expectedMsgType = ControlMessage.EventType.END_COMMIT;
      }
    }

    public enum TestScenarios {
      SUBSET_CONNECT_TASKS_FAILED,
      SUBSET_WRITE_STATUS_FAILED,
      SUBSET_WRITE_STATUS_FAILED_BUT_IGNORED,
      ALL_CONNECT_TASKS_SUCCESS,
      ALL_CONNECT_TASKS_WITH_EMPTY_WRITE_STATUS
    }

    private static void composeControlEvent(
        String commitTime, boolean shouldIncludeFailedRecords, boolean useEmptyWriteStatus,
        Map<Integer, Long> kafkaOffsets, List<ControlMessage> controlEvents) {
      // Prepare the WriteStatuses for all partitions
      for (int i = 1; i <= TOTAL_KAFKA_PARTITIONS; i++) {
        try {
          long kafkaOffset = (long) (Math.random() * 10000);
          kafkaOffsets.put(i, kafkaOffset);
          ControlMessage event = composeWriteStatusResponse(
              commitTime,
              new TopicPartition(TOPIC_NAME, i),
              kafkaOffset,
              shouldIncludeFailedRecords,
              useEmptyWriteStatus);
          controlEvents.add(event);
        } catch (Exception exception) {
          throw new HoodieException("Fatal error sending control event to Coordinator");
        }
      }
    }

    private static ControlMessage composeWriteStatusResponse(String commitTime,
                                                             TopicPartition partition,
                                                             long kafkaOffset,
                                                             boolean includeFailedRecords,
                                                             boolean useEmptyWriteStatus) throws Exception {
      List<WriteStatus> writeStatusList = useEmptyWriteStatus ? Collections.emptyList()
          : Collections.singletonList(
          includeFailedRecords
              ? getSubsetFailedRecordsWriteStatus()
              : getAllSuccessfulRecordsWriteStatus());

      return ControlMessage.newBuilder()
          .setType(ControlMessage.EventType.WRITE_STATUS)
          .setTopicName(partition.topic())
          .setSenderType(ControlMessage.EntityType.PARTICIPANT)
          .setSenderPartition(partition.partition())
          .setReceiverType(ControlMessage.EntityType.COORDINATOR)
          .setReceiverPartition(ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION)
          .setCommitTime(commitTime)
          .setParticipantInfo(
              ControlMessage.ParticipantInfo.newBuilder()
                  .setWriteStatus(KafkaConnectUtils.buildWriteStatuses(writeStatusList))
                  .setKafkaOffset(kafkaOffset)
                  .build()
          ).build();
    }
  }

  private static WriteStatus getAllSuccessfulRecordsWriteStatus() {
    // send WS
    WriteStatus status = new WriteStatus(false, 0.0);
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(mock(HoodieRecord.class), Option.empty());
    }
    return status;
  }

  private static WriteStatus getSubsetFailedRecordsWriteStatus() {
    // send WS
    WriteStatus status = new WriteStatus(false, 0.0);
    for (int i = 0; i < 1000; i++) {
      if (i % 10 == 0) {
        status.markFailure(mock(HoodieRecord.class), new Throwable("Error writing record on disk"), Option.empty());
      } else {
        status.markSuccess(mock(HoodieRecord.class), Option.empty());
      }
    }
    status.setGlobalError(new Throwable("More than one records failed to be written to storage"));
    return status;
  }
}
