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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.HudiTransactionParticipant;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.core.TransactionParticipant;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.ConnectWriter;
import org.apache.hudi.connect.writers.ConnectWriterProvider;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHudiTransactionParticipant {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int PARTITION_NUMBER = 4;
  private static final int NUM_RECORDS_BATCH = 5;
  private static final int MAX_COMMIT_ROUNDS = 5;

  private HudiTransactionParticipant participant;
  private TopicPartition partition;
  private HudiConnectConfigs configs;
  private TestKafkaControlAgent kafkaControlAgent;
  private TestHudiWriter testHudiWriter;
  private TestSinkTaskContext taskContext;
  private int numberKafkaRecordsConsumed;
  private long currentKafkaOffset;
  private int currentCommitTime;
  private TopicPartition mockCoordinatorPartition;


  @BeforeEach
  public void setUp() throws Exception {
    kafkaControlAgent = new TestKafkaControlAgent();
    partition = new TopicPartition(TOPIC_NAME, PARTITION_NUMBER);
    testHudiWriter = new TestHudiWriter();
    taskContext = new TestSinkTaskContext(partition);
    configs = HudiConnectConfigs.newBuilder()
        .build();
    numberKafkaRecordsConsumed = 0;
    currentKafkaOffset = 0L;
    currentCommitTime = 100;
    mockCoordinatorPartition = new TopicPartition(TOPIC_NAME, 0);
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
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        case COORDINATOR_FAILED_BEFORE_COMMITTING:
          testPostStartCommit();
          // Configure kafka agent to ignore response from client
          kafkaControlAgent.disableCommitOffset();
          testPostEndCommit(false);
          // Coordinator fails
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        case COORDINATOR_FAILED_AFTER_COMMITTING:
          testPostStartCommit();
          testPostEndCommit(false);
          // Coordinator fails
          testPostStartCommit();
          testPostEndCommit(false);
          testPostAckCommit(false);
          break;
        default:
          throw new HoodieException("Unknown test scenario " + testScenario);
      }
    }

    participant.stop();
    assertTrue(kafkaControlAgent.hasDeRegistered);
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
    kafkaControlAgent.setParticipant(participant);
    participant.start();
    assertTrue(kafkaControlAgent.hasRegistered);
    assertTrue(taskContext.isPaused);

    putRecordsToParticipant();
    // The last batch of writes should not be written since they were consumed before START_COMMIT
    assertEquals(testHudiWriter.numberRecords, numberKafkaRecordsConsumed);
  }

  private void testPostStartCommit() {
    // Firstly, ensure the committed Kafka Offsets are in sync with Participant
    putRecordsToParticipant();
    sendEventFromCoordinator(
        mockCoordinatorPartition,
        participant,
        ControlEvent.MsgType.START_COMMIT,
        String.valueOf(++currentCommitTime),
        kafkaControlAgent.committedKafkaOffset);
    putRecordsToParticipant();

    assertEquals(participant.getLastKafkaCommittedOffset(), kafkaControlAgent.committedKafkaOffset);
    assertFalse(taskContext.isPaused);
    // All the records consumed so far are now flushed to Hudi Writer since a START_COMMIT has been received
    assertEquals(testHudiWriter.numberRecords, numberKafkaRecordsConsumed);
  }

  private void testPostEndCommit(boolean hasParticipantFailed) {
    int currentRecordsConsumed = testHudiWriter.getNumberRecords();
    putRecordsToParticipant();
    // The kafka offset of the last written record before an END_COMMIT should be sent to the leader
    long lastWrittenKafkaOffset = currentKafkaOffset;
    sendEventFromCoordinator(
        mockCoordinatorPartition,
        participant,
        ControlEvent.MsgType.END_COMMIT,
        String.valueOf(currentCommitTime),
        kafkaControlAgent.committedKafkaOffset);
    putRecordsToParticipant();

    assertTrue(taskContext.isPaused);
    if (hasParticipantFailed) {
      validateStateOnParticipantFailure(currentRecordsConsumed);
    } else {
      if (kafkaControlAgent.lastReceivedWriteStatusEvent.isPresent()) {
        validateWriteStatus(kafkaControlAgent.lastReceivedWriteStatusEvent.get(), lastWrittenKafkaOffset);
      } else {
        throw new HoodieException("Participant did not send a valid WS Event after receiving an END_COMMIT");
      }
      // The last batch of writes should not be written since they were consumed after END_COMMIT
      assertEquals(testHudiWriter.numberRecords, currentRecordsConsumed + NUM_RECORDS_BATCH);
    }
  }

  private void testPostAckCommit(boolean hasParticipantFailed) {
    int currentRecordsConsumed = testHudiWriter.getNumberRecords();
    putRecordsToParticipant();
    sendEventFromCoordinator(
        mockCoordinatorPartition,
        participant,
        ControlEvent.MsgType.ACK_COMMIT,
        String.valueOf(currentCommitTime),
        kafkaControlAgent.committedKafkaOffset);
    putRecordsToParticipant();

    assertTrue(taskContext.isPaused);
    if (hasParticipantFailed) {
      validateStateOnParticipantFailure(currentRecordsConsumed);
    } else {
      // Ensure both batches of records are not written to Hudi since no records are written post END_COMMIT
      assertEquals(testHudiWriter.numberRecords, currentRecordsConsumed);
    }
  }

  private static void sendEventFromCoordinator(
      TopicPartition mockCoordinatorPartition,
      TransactionParticipant participant,
      ControlEvent.MsgType type,
      String commitTime,
      Long committedKafkaOffset) {
    try {
      participant.publishControlEvent(new ControlEvent.Builder(
          type,
          commitTime,
          mockCoordinatorPartition)
          .setCoodinatorInfo(new ControlEvent.CoordinatorInfo(
              Collections.singletonMap(participant.getPartition().partition(), committedKafkaOffset)))
          .build());
    } catch (Exception exception) {
      throw new HoodieException("Fatal error sending control event to Participant");
    }
  }

  // The participant should ignore all events prior to receiving the next START_COMMIT,
  // hence should not have sent the WS and should not be consuming/ writing kafka records.
  private void validateStateOnParticipantFailure(int currentRecordsConsumed) {
    assertEquals(testHudiWriter.numberRecords, currentRecordsConsumed);
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

  private static class TestKafkaControlAgent implements KafkaControlAgent {

    private HudiTransactionParticipant participant;
    private boolean hasRegistered;
    private boolean hasDeRegistered;
    private Option<ControlEvent> lastReceivedWriteStatusEvent;
    private long committedKafkaOffset;
    private boolean disableCommitOffset;

    public TestKafkaControlAgent() {
      hasRegistered = false;
      hasDeRegistered = false;
      lastReceivedWriteStatusEvent = Option.empty();
      committedKafkaOffset = 0L;
      disableCommitOffset = false;
    }

    public void disableCommitOffset() {
      disableCommitOffset = true;
    }

    public void setParticipant(HudiTransactionParticipant participant) {
      this.participant = participant;
    }

    @Override
    public void registerTransactionCoordinator(TransactionCoordinator leader) {
      // no-op
    }

    @Override
    public void registerTransactionParticipant(TransactionParticipant worker) {
      assertEquals(participant, worker);
      hasRegistered = true;
    }

    @Override
    public void deregisterTransactionCoordinator(TransactionCoordinator leader) {
      // no-op
    }

    @Override
    public void deregisterTransactionParticipant(TransactionParticipant worker) {
      assertEquals(participant, worker);
      hasDeRegistered = true;
    }

    @Override
    public void publishMessage(ControlEvent message) {
      if (message.getMsgType().equals(ControlEvent.MsgType.WRITE_STATUS)) {
        lastReceivedWriteStatusEvent = Option.of(message);
        assertTrue(message.getParticipantInfo().getKafkaCommitOffset() >= committedKafkaOffset);

        // Mimic coordinator failure before committing the write and kafka offset
        if (!disableCommitOffset) {
          committedKafkaOffset = message.getParticipantInfo().getKafkaCommitOffset();
        }
      }
    }
  }

  private static class TestHudiWriter implements ConnectWriterProvider<WriteStatus>, ConnectWriter<WriteStatus> {

    private int numberRecords;
    private boolean isClosed;

    public void TestHudiWriterProvider() {
      this.numberRecords = 0;
      this.isClosed = false;
    }

    public int getNumberRecords() {
      return numberRecords;
    }

    public boolean isClosed() {
      return isClosed;
    }

    @Override
    public void writeRecord(SinkRecord record) {
      numberRecords++;
    }

    @Override
    public List<WriteStatus> close() {
      isClosed = false;
      return null;
    }

    @Override
    public ConnectWriter<WriteStatus> getWriter(String commitTime) {
      return this;
    }
  }

  private static class TestSinkTaskContext implements SinkTaskContext {

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
