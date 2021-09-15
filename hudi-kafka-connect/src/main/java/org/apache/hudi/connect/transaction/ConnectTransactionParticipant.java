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
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.ConnectWriterProvider;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.connect.writers.KafkaConnectWriterProvider;
import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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

  private static final Logger LOG = LogManager.getLogger(ConnectTransactionParticipant.class);

  private final LinkedList<SinkRecord> buffer;
  private final BlockingQueue<ControlEvent> controlEvents;
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
  public void processControlEvent(ControlEvent message) {
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
  public void processRecords() throws IOException {
    while (!controlEvents.isEmpty()) {
      ControlEvent message = controlEvents.poll();
      switch (message.getMsgType()) {
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
          throw new IllegalStateException("HudiTransactionParticipant received incorrect state " + message.getMsgType());
      }
    }

    writeRecords();
  }

  private void handleStartCommit(ControlEvent message) {
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
      ongoingTransactionInfo.setLastWrittenKafkaOffset(committedKafkaOffset);
    } catch (Exception exception) {
      LOG.warn("Error received while starting a new transaction", exception);
    }
  }

  private void handleEndCommit(ControlEvent message) throws IOException {
    if (ongoingTransactionInfo == null) {
      LOG.warn(String.format("END_COMMIT %s is received while we were NOT in active transaction", message.getCommitTime()));
      return;
    } else if (!ongoingTransactionInfo.getCommitTime().equals(message.getCommitTime())) {
      LOG.error(String.format("Fatal error received END_COMMIT with commit time %s while local transaction commit time %s",
          message.getCommitTime(), ongoingTransactionInfo.getCommitTime()));
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
      ControlEvent writeStatusEvent = new ControlEvent.Builder(ControlEvent.MsgType.WRITE_STATUS,
          ControlEvent.SenderType.PARTICIPANT, ongoingTransactionInfo.getCommitTime(), partition)
          .setParticipantInfo(new ControlEvent.ParticipantInfo(
              writeStatuses,
              ongoingTransactionInfo.getLastWrittenKafkaOffset(),
              ControlEvent.OutcomeType.WRITE_SUCCESS))
          .build();
      kafkaControlAgent.publishMessage(writeStatusEvent);
    } catch (Exception exception) {
      LOG.error(String.format("Error writing records and ending commit %s for partition %s", message.getCommitTime(), partition.partition()), exception);
      throw new IOException(String.format("Error writing records and ending commit %s for partition %s", message.getCommitTime(), partition.partition()), exception);
    }
  }

  private void handleAckCommit(ControlEvent message) {
    // Update lastKafkCommitedOffset locally.
    if (ongoingTransactionInfo != null && committedKafkaOffset < ongoingTransactionInfo.getLastWrittenKafkaOffset()) {
      committedKafkaOffset = ongoingTransactionInfo.getLastWrittenKafkaOffset();
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
              && record.kafkaOffset() >= ongoingTransactionInfo.getLastWrittenKafkaOffset()) {
            ongoingTransactionInfo.getWriter().writeRecord(record);
            ongoingTransactionInfo.setLastWrittenKafkaOffset(record.kafkaOffset() + 1);
          } else if (record != null && record.kafkaOffset() < committedKafkaOffset) {
            LOG.warn(String.format("Received a kafka record with offset %s prior to last committed offset %s for partition %s",
                record.kafkaOffset(), ongoingTransactionInfo.getLastWrittenKafkaOffset(),
                partition));
          }
          buffer.poll();
        } catch (Exception exception) {
          LOG.warn(String.format("Error received while writing records for transaction %s in partition %s",
              ongoingTransactionInfo.getCommitTime(), partition.partition()),
              exception);
        }
      }
    }
  }

  private void cleanupOngoingTransaction() {
    if (ongoingTransactionInfo != null) {
      try {
        ongoingTransactionInfo.getWriter().close();
        ongoingTransactionInfo = null;
      } catch (IOException exception) {
        LOG.warn("Error received while trying to cleanup existing transaction", exception);
      }
    }
  }

  private void syncKafkaOffsetWithLeader(ControlEvent message) {
    if (message.getCoordinatorInfo() != null) {
      Long coordinatorCommittedKafkaOffset = message.getCoordinatorInfo().getGlobalKafkaCommitOffsets().get(partition.partition());
      // Recover kafka committed offsets, treating the commit offset from the coordinator
      // as the source of truth
      if (coordinatorCommittedKafkaOffset != null && coordinatorCommittedKafkaOffset >= 0) {
        if (coordinatorCommittedKafkaOffset != committedKafkaOffset) {
          LOG.warn(String.format("Recovering the kafka offset for partition %s to offset %s instead of local offset %s",
              partition.partition(), coordinatorCommittedKafkaOffset, committedKafkaOffset));
          context.offset(partition, coordinatorCommittedKafkaOffset);
        }
        committedKafkaOffset = coordinatorCommittedKafkaOffset;
      }
    }
  }
}
