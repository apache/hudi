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

package org.apache.hudi.connect.transaction;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.connect.writers.ConnectWriterProvider;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.connect.writers.KafkaConnectWriterProvider;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implementation of the {@link TransactionParticipant} that coordinates the Hudi write transactions
 * based on events from the {@link TransactionCoordinator} and manages the Hudi Writes for a specific Kafka Partition.
 */
public class ConnectTransactionParticipant implements TransactionParticipant {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectTransactionParticipant.class);

  private final LinkedList<SinkRecord> buffer;
  private final BlockingQueue<ControlMessage> controlEvents;
  private final TopicPartition partition;
  private final SinkTaskContext context;
  private final KafkaControlAgent kafkaControlAgent;
  private final ConnectWriterProvider<WriteStatus> writerProvider;

  private TransactionInfo<WriteStatus> ongoingTransactionInfo;
  private long committedKafkaOffset;

  public ConnectTransactionParticipant(KafkaConnectConfigs configs,
                                       TopicPartition partition,
                                       KafkaControlAgent kafkaControlAgent,
                                       SinkTaskContext context) throws HoodieException {
    this(partition, kafkaControlAgent, context, new KafkaConnectWriterProvider(configs, partition));
  }

  public ConnectTransactionParticipant(TopicPartition partition,
                                       KafkaControlAgent kafkaControlAgent,
                                       SinkTaskContext context,
                                       ConnectWriterProvider<WriteStatus> writerProvider) throws HoodieException {
    this.buffer = new LinkedList<>();
    this.controlEvents = new LinkedBlockingQueue<>();
    this.partition = partition;
    this.context = context;
    this.writerProvider = writerProvider;
    this.kafkaControlAgent = kafkaControlAgent;
    this.ongoingTransactionInfo = null;
    this.committedKafkaOffset = 0;
  }

  @Override
  public void start() {
    LOG.info("Start Hudi Transaction Participant for partition " + partition.partition());
    this.kafkaControlAgent.registerTransactionParticipant(this);
    context.pause(partition);
  }

  @Override
  public void stop() {
    this.kafkaControlAgent.deregisterTransactionParticipant(this);
    cleanupOngoingTransaction();
  }

  @Override
  public void buffer(SinkRecord record) {
    buffer.add(record);
  }

  @Override
  public void processControlEvent(ControlMessage message) {
    controlEvents.add(message);
  }

  @Override
  public long getLastKafkaCommittedOffset() {
    return committedKafkaOffset;
  }

  @Override
  public TopicPartition getPartition() {
    return partition;
  }

  @Override
  public void processRecords() {
    while (!controlEvents.isEmpty()) {
      ControlMessage message = controlEvents.poll();
      switch (message.getType()) {
        case START_COMMIT:
          handleStartCommit(message);
          break;
        case END_COMMIT:
          handleEndCommit(message);
          break;
        case ACK_COMMIT:
          handleAckCommit(message);
          break;
        case WRITE_STATUS:
          // ignore write status since its only processed by leader
          break;
        default:
          throw new IllegalStateException("HudiTransactionParticipant received incorrect state " + message.getType().name());
      }
    }

    writeRecords();
  }

  private void handleStartCommit(ControlMessage message) {
    // If there is an existing/ongoing transaction locally
    // but it failed globally since we received another START_COMMIT instead of an END_COMMIT or ACK_COMMIT,
    // so close it and start new transaction
    cleanupOngoingTransaction();
    // Resync the last committed Kafka offset from the leader
    syncKafkaOffsetWithLeader(message);
    context.resume(partition);
    String currentCommitTime = message.getCommitTime();
    LOG.info("Started a new transaction after receiving START_COMMIT for commit " + currentCommitTime);
    try {
      ongoingTransactionInfo = new TransactionInfo<>(currentCommitTime, writerProvider.getWriter(currentCommitTime));
      ongoingTransactionInfo.setExpectedKafkaOffset(committedKafkaOffset);
    } catch (Exception exception) {
      LOG.warn("Failed to start a new transaction", exception);
    }
  }

  private void handleEndCommit(ControlMessage message) {
    if (ongoingTransactionInfo == null) {
      LOG.warn("END_COMMIT {} is received while we were NOT in active transaction", message.getCommitTime());
      return;
    } else if (!ongoingTransactionInfo.getCommitTime().equals(message.getCommitTime())) {
      LOG.error("Fatal error received END_COMMIT with commit time {} while local transaction commit time {}",
          message.getCommitTime(), ongoingTransactionInfo.getCommitTime());
      // Recovery: A new END_COMMIT from leader caused interruption to an existing transaction,
      // explicitly reset Kafka commit offset to ensure no data loss
      cleanupOngoingTransaction();
      syncKafkaOffsetWithLeader(message);
      return;
    }

    context.pause(partition);
    ongoingTransactionInfo.commitInitiated();
    // send Writer Status Message and wait for ACK_COMMIT in async fashion
    try {
      //sendWriterStatus
      List<WriteStatus> writeStatuses = ongoingTransactionInfo.getWriter().close();

      ControlMessage writeStatusEvent = ControlMessage.newBuilder()
          .setProtocolVersion(KafkaConnectConfigs.CURRENT_PROTOCOL_VERSION)
          .setType(ControlMessage.EventType.WRITE_STATUS)
          .setTopicName(partition.topic())
          .setSenderType(ControlMessage.EntityType.PARTICIPANT)
          .setSenderPartition(partition.partition())
          .setReceiverType(ControlMessage.EntityType.COORDINATOR)
          .setReceiverPartition(ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION)
          .setCommitTime(ongoingTransactionInfo.getCommitTime())
          .setParticipantInfo(
              ControlMessage.ParticipantInfo.newBuilder()
                  .setWriteStatus(KafkaConnectUtils.buildWriteStatuses(writeStatuses))
                  .setKafkaOffset(ongoingTransactionInfo.getExpectedKafkaOffset())
                  .build()
          ).build();

      kafkaControlAgent.publishMessage(writeStatusEvent);
    } catch (Exception exception) {
      LOG.error(String.format("Error writing records and ending commit %s for partition %s", message.getCommitTime(), partition.partition()), exception);
      throw new HoodieIOException(String.format("Error writing records and ending commit %s for partition %s", message.getCommitTime(), partition.partition()),
          new IOException(exception));
    }
  }

  private void handleAckCommit(ControlMessage message) {
    // Update committedKafkaOffset that tracks the last committed kafka offset locally.
    if (ongoingTransactionInfo != null && committedKafkaOffset < ongoingTransactionInfo.getExpectedKafkaOffset()) {
      committedKafkaOffset = ongoingTransactionInfo.getExpectedKafkaOffset();
    }
    syncKafkaOffsetWithLeader(message);
    cleanupOngoingTransaction();
  }

  private void writeRecords() {
    if (ongoingTransactionInfo != null && !ongoingTransactionInfo.isCommitInitiated()) {
      while (!buffer.isEmpty()) {
        try {
          SinkRecord record = buffer.peek();
          if (record != null
              && record.kafkaOffset() == ongoingTransactionInfo.getExpectedKafkaOffset()) {
            ongoingTransactionInfo.getWriter().writeRecord(record);
            ongoingTransactionInfo.setExpectedKafkaOffset(record.kafkaOffset() + 1);
          } else if (record != null && record.kafkaOffset() > ongoingTransactionInfo.getExpectedKafkaOffset()) {
            LOG.warn("Received a kafka record with offset {} above the next expected kafka offset {} for partition {}. "
                + "Resetting the kafka offset to {}", record.kafkaOffset(), ongoingTransactionInfo.getExpectedKafkaOffset(), partition, ongoingTransactionInfo.getExpectedKafkaOffset());
            context.offset(partition, ongoingTransactionInfo.getExpectedKafkaOffset());
          } else if (record != null && record.kafkaOffset() < ongoingTransactionInfo.getExpectedKafkaOffset()) {
            LOG.info("Received a kafka record with offset {} below the next expected kafka offset {} for partition {}. "
                + "No action will be taken but this record will be ignored since its already written", record.kafkaOffset(), ongoingTransactionInfo.getExpectedKafkaOffset(), partition);
          }
          buffer.poll();
        } catch (Exception exception) {
          LOG.warn("Failed to write records for transaction [{}] in partition [{}]",
              ongoingTransactionInfo.getCommitTime(), partition.partition(), exception);
        }
      }
    }
  }

  private void cleanupOngoingTransaction() {
    if (ongoingTransactionInfo != null) {
      try {
        ongoingTransactionInfo.getWriter().close();
        ongoingTransactionInfo = null;
      } catch (HoodieIOException exception) {
        LOG.warn("Failed to cleanup existing transaction", exception);
      }
    }
  }

  private void syncKafkaOffsetWithLeader(ControlMessage message) {
    if (message.getCoordinatorInfo().getGlobalKafkaCommitOffsetsMap().containsKey(partition.partition())) {
      Long coordinatorCommittedKafkaOffset = message.getCoordinatorInfo().getGlobalKafkaCommitOffsetsMap().get(partition.partition());
      // Recover kafka committed offsets, treating the commit offset from the coordinator
      // as the source of truth
      if (coordinatorCommittedKafkaOffset != null && coordinatorCommittedKafkaOffset >= 0) {
        // Debug only messages
        if (coordinatorCommittedKafkaOffset != committedKafkaOffset) {
          LOG.warn("The coordinator offset for kafka partition {} is {} while the locally committed offset is {}. "
              + "Resetting the local committed offset to the coordinator provided one to ensure consistency", partition, coordinatorCommittedKafkaOffset, committedKafkaOffset);
        }
        committedKafkaOffset = coordinatorCommittedKafkaOffset;
        return;
      }
    } else {
      LOG.warn("The coordinator offset for kafka partition {} is not present while the locally committed offset is {}. "
          + "Resetting the local committed offset to 0 to avoid data loss", partition, committedKafkaOffset);
    }
    // If the coordinator does not have a committed offset for this partition, reset to zero offset.
    committedKafkaOffset = 0;
  }
}
