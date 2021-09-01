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

package org.apache.hudi.connect.core;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.connect.writers.ConnectTransactionServices;
import org.apache.hudi.connect.writers.TransactionServices;
import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the Coordinator that
 * coordinates the Hudi write transactions
 * across all the Kafka partitions for a single Kafka Topic.
 */
public class HudiTransactionCoordinator extends TransactionCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(HudiTransactionCoordinator.class);
  private static final String BOOTSTRAP_SERVERS_CFG = "bootstrap.servers";
  private static final String KAFKA_OFFSET_KEY = "kafka.commit.offsets";
  private static final String KAFKA_OFFSET_DELIMITER = ",";
  private static final String KAFKA_OFFSET_KV_DELIMITER = "=";
  private static final Long START_COMMIT_INIT_DELAY_MS = 100L;
  private static final Long RESTART_COMMIT_DELAY_MS = 500L;

  private final HudiConnectConfigs configs;
  private final TopicPartition partition;
  private final KafkaControlAgent kafkaControlClient;
  private final TransactionServices hudiTransactionServices;
  private final KafkaPartitionProvider kafkaPartitionProvider;

  private String currentCommitTime;
  private Map<Integer, List<WriteStatus>> partitionsWriteStatusReceived;
  private Map<Integer, Long> globalCommittedKafkaOffsets;
  private Map<Integer, Long> currentConsumedKafkaOffsets;
  private State currentState;
  private int numPartitions;

  public HudiTransactionCoordinator(HudiConnectConfigs configs,
                                    TopicPartition partition,
                                    KafkaControlAgent kafkaControlClient) throws HoodieException {
    this(configs,
        partition,
        kafkaControlClient,
        new ConnectTransactionServices(configs),
        KafkaConnectUtils::getLatestNumPartitions);
  }

  public HudiTransactionCoordinator(HudiConnectConfigs configs,
                                    TopicPartition partition,
                                    KafkaControlAgent kafkaControlClient,
                                    TransactionServices hudiTransactionServices,
                                    KafkaPartitionProvider kafkaPartitionProvider) {
    super(configs, partition);
    this.configs = configs;
    this.partition = partition;
    this.kafkaControlClient = kafkaControlClient;
    this.hudiTransactionServices = hudiTransactionServices;
    this.kafkaPartitionProvider = kafkaPartitionProvider;

    this.currentCommitTime = StringUtils.EMPTY_STRING;
    this.partitionsWriteStatusReceived = new HashMap<>();
    this.globalCommittedKafkaOffsets = new HashMap<>();
    this.currentConsumedKafkaOffsets = new HashMap<>();
    this.currentState = State.INIT;
  }

  @Override
  public void start() {
    super.start();
    // Read the globalKafkaOffsets from the last commit file
    kafkaControlClient.registerTransactionCoordinator(this);
    initializeGlobalCommittedKafkaOffsets();
    // Submit the first start commit
    submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
            partition.topic(),
            StringUtils.EMPTY_STRING),
        START_COMMIT_INIT_DELAY_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    super.stop();
    kafkaControlClient.deregisterTransactionCoordinator(this);
  }

  @Override
  public void publishControlEvent(ControlEvent message) {
    CoordinatorEvent.CoordinatorEventType type;
    if (message.getMsgType().equals(ControlEvent.MsgType.WRITE_STATUS)) {
      type = CoordinatorEvent.CoordinatorEventType.WRITE_STATUS;
    } else {
      LOG.warn("The Coordinator should not be receiving messages of type {}", message.getMsgType().name());
      return;
    }
    CoordinatorEvent event = new CoordinatorEvent(type,
        partition.topic(),
        message.getCommitTime());
    event.setMessage(message);
    submitEvent(event);
  }

  @Override
  public void processCoordinatorEvent(CoordinatorEvent event) {
    try {
      // Ignore NULL and STALE events, unless its one to start a new COMMIT
      if (event == null
          || (!event.getEventType().equals(CoordinatorEvent.CoordinatorEventType.START_COMMIT)
          && (!event.getCommitTime().equals(currentCommitTime)))) {
        return;
      }

      switch (event.getEventType()) {
        case START_COMMIT:
          startNewCommit();
          break;
        case END_COMMIT:
          endExistingCommit();
          break;
        case WRITE_STATUS:
          // Ignore stale write_status messages sent after
          if (event.getMessage() != null
              && currentState.equals(State.ENDED_COMMIT)) {
            onReceiveWriteStatus(event.getMessage());
          } else {
            LOG.warn("Could not process WRITE_STATUS due to missing message");
          }
          break;
        case ACK_COMMIT:
          submitAckCommit();
          break;
        case WRITE_STATUS_TIMEOUT:
          handleWriteStatusTimeout();
          break;
        default:
          throw new IllegalStateException("Partition Coordinator has received an illegal event type " + event.getEventType().name());
      }
    } catch (Exception exception) {
      LOG.warn("Error recieved while polling the event loop in Partition Coordinator", exception);
    }
  }

  private void startNewCommit() {
    numPartitions = kafkaPartitionProvider.getLatestNumPartitions(configs.getString(BOOTSTRAP_SERVERS_CFG), partition.topic());
    partitionsWriteStatusReceived.clear();
    try {
      currentCommitTime = hudiTransactionServices.startCommit();
      ControlEvent message = new ControlEvent.Builder(
          ControlEvent.MsgType.START_COMMIT,
          currentCommitTime,
          partition)
          .setCoodinatorInfo(
              new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
          .build();
      kafkaControlClient.publishMessage(message);
      currentState = State.STARTED_COMMIT;
      // schedule a timeout for ending the current commit
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.END_COMMIT,
              partition.topic(),
              currentCommitTime),
          configs.getCommitIntervalSecs(), TimeUnit.SECONDS);
    } catch (Exception exception) {
      LOG.error("Failed to start a new commit {}, will retry", currentCommitTime, exception);
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
              partition.topic(),
              StringUtils.EMPTY_STRING),
          RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void endExistingCommit() {
    try {
      ControlEvent message = new ControlEvent.Builder(
          ControlEvent.MsgType.END_COMMIT,
          currentCommitTime,
          partition)
          .setCoodinatorInfo(new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
          .build();
      kafkaControlClient.publishMessage(message);
    } catch (Exception exception) {
      LOG.warn("Could not send END_COMMIT message for partition {} and commitTime {}", partition, currentCommitTime, exception);
    }
    currentConsumedKafkaOffsets.clear();
    currentState = State.ENDED_COMMIT;

    // schedule a timeout for receiving all write statuses
    submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.WRITE_STATUS_TIMEOUT,
            partition.topic(),
            currentCommitTime),
        configs.getCoordinatorWriteTimeoutSecs(), TimeUnit.SECONDS);
  }

  private void onReceiveWriteStatus(ControlEvent message) {
    ControlEvent.ParticipantInfo participantInfo = message.getParticipantInfo();
    if (participantInfo.getOutcomeType().equals(ControlEvent.OutcomeType.WRITE_SUCCESS)) {
      int partition = message.senderPartition().partition();
      partitionsWriteStatusReceived.put(partition, participantInfo.writeStatuses());
      currentConsumedKafkaOffsets.put(partition, participantInfo.getKafkaCommitOffset());
    }
    if (partitionsWriteStatusReceived.size() >= numPartitions
        && currentState.equals(State.ENDED_COMMIT)) {
      // Commit the kafka offsets to the commit file
      try {
        List<WriteStatus> allWriteStatuses = new ArrayList<>();
        partitionsWriteStatusReceived.forEach((key, value) -> allWriteStatuses.addAll(value));
        // Commit the last write in Hudi, along with the latest kafka offset
        hudiTransactionServices.endCommit(currentCommitTime,
            allWriteStatuses,
            transformKafkaOffsets(currentConsumedKafkaOffsets));
        currentState = State.WRITE_STATUS_RCVD;
        globalCommittedKafkaOffsets.putAll(currentConsumedKafkaOffsets);
        submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.ACK_COMMIT,
            partition.topic(),
            currentCommitTime));
      } catch (Exception exception) {
        LOG.error("Fatal error while committing file", exception);
      }
    }
  }

  private void handleWriteStatusTimeout() {
    // If we are still stuck in ENDED_STATE
    if (currentState.equals(State.ENDED_COMMIT)) {
      currentState = State.WRITE_STATUS_TIMEDOUT;
      LOG.warn("Did not receive the Write Status from all partitions");
      // Submit the next start commit
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
              partition.topic(),
              StringUtils.EMPTY_STRING),
          RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void submitAckCommit() {
    try {
      ControlEvent message = new ControlEvent.Builder(
          ControlEvent.MsgType.ACK_COMMIT,
          currentCommitTime,
          partition)
          .setCoodinatorInfo(
              new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
          .build();
      kafkaControlClient.publishMessage(message);
    } catch (Exception exception) {
      LOG.warn("Could not send ACK_COMMIT message for partition {} and commitTime {}", partition, currentCommitTime);
    }
    currentState = State.ACKED_COMMIT;

    // Submit the next start commit
    submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
            partition.topic(),
            StringUtils.EMPTY_STRING),
        START_COMMIT_INIT_DELAY_MS, TimeUnit.MILLISECONDS);
  }

  private void initializeGlobalCommittedKafkaOffsets() {
    try {
      Map<String, String> commitMetadata = hudiTransactionServices.loadLatestCommitMetadata();
      String latestKafkaOffsets = commitMetadata.get(KAFKA_OFFSET_KEY);
      if (!StringUtils.isNullOrEmpty(latestKafkaOffsets)) {
        LOG.info("Retrieved Raw Kafka offsets from Hudi Commit File " + latestKafkaOffsets);
        globalCommittedKafkaOffsets = Arrays.stream(latestKafkaOffsets.split(KAFKA_OFFSET_DELIMITER))
            .map(entry -> entry.split(KAFKA_OFFSET_KV_DELIMITER))
            .collect(Collectors.toMap(entry -> Integer.parseInt(entry[0]), entry -> Long.parseLong(entry[1])));
        LOG.info("Initialized the kafka offset commits " + globalCommittedKafkaOffsets);
      }
    } catch (Exception exception) {
      throw new HoodieException("Could not deserialize the kafka commit offsets", exception);
    }
  }

  private Map<String, String> transformKafkaOffsets(Map<Integer, Long> kafkaOffsets) {
    try {
      String kafkaOffsetValue = kafkaOffsets.keySet().stream()
          .map(key -> key + KAFKA_OFFSET_KV_DELIMITER + kafkaOffsets.get(key))
          .collect(Collectors.joining(KAFKA_OFFSET_DELIMITER));
      return Collections.singletonMap(KAFKA_OFFSET_KEY, kafkaOffsetValue);
    } catch (Exception exception) {
      throw new HoodieException("Could not serialize the kafka commit offsets", exception);
    }
  }

  private enum State {
    INIT,
    STARTED_COMMIT,
    ENDED_COMMIT,
    WRITE_STATUS_RCVD,
    WRITE_STATUS_TIMEDOUT,
    ACKED_COMMIT,
  }

  public interface KafkaPartitionProvider {

    int getLatestNumPartitions(String bootstrapServers, String topicName);
  }
}
