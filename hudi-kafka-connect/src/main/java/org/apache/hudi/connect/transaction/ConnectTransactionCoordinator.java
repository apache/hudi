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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.connect.writers.ConnectTransactionServices;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.connect.writers.KafkaConnectTransactionServices;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Implementation of the Coordinator that
 * coordinates the Hudi write transactions
 * across all the Kafka partitions for a single Kafka Topic.
 */
public class ConnectTransactionCoordinator implements TransactionCoordinator, Runnable {

  public static final int COORDINATOR_KAFKA_PARTITION = 0;

  private static final Logger LOG = LoggerFactory.getLogger(ConnectTransactionCoordinator.class);
  private static final String BOOTSTRAP_SERVERS_CFG = "bootstrap.servers";
  private static final String KAFKA_OFFSET_KEY = "kafka.commit.offsets";
  private static final String KAFKA_OFFSET_DELIMITER = ",";
  private static final String KAFKA_OFFSET_KV_DELIMITER = "=";
  private static final Long START_COMMIT_INIT_DELAY_MS = 100L;
  private static final Long RESTART_COMMIT_DELAY_MS = 500L;
  private static final int COORDINATOR_EVENT_LOOP_TIMEOUT_MS = 1000;

  private final KafkaConnectConfigs configs;
  private final TopicPartition partition;
  private final KafkaControlAgent kafkaControlClient;
  private final ConnectTransactionServices transactionServices;
  private final KafkaPartitionProvider partitionProvider;
  private final Map<Integer, List<WriteStatus>> partitionsWriteStatusReceived;
  private final Map<Integer, Long> currentConsumedKafkaOffsets;
  private final AtomicBoolean hasStarted = new AtomicBoolean(false);
  private final BlockingQueue<CoordinatorEvent> events;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduler;

  private String currentCommitTime;
  private Map<Integer, Long> globalCommittedKafkaOffsets;
  private State currentState;
  private int numPartitions;

  public ConnectTransactionCoordinator(KafkaConnectConfigs configs,
                                       TopicPartition partition,
                                       KafkaControlAgent kafkaControlClient) throws HoodieException {
    this(configs,
        partition,
        kafkaControlClient,
        new KafkaConnectTransactionServices(configs),
        KafkaConnectUtils::getLatestNumPartitions);
  }

  public ConnectTransactionCoordinator(KafkaConnectConfigs configs,
                                       TopicPartition partition,
                                       KafkaControlAgent kafkaControlClient,
                                       ConnectTransactionServices transactionServices,
                                       KafkaPartitionProvider partitionProvider) {
    this.configs = configs;
    this.partition = partition;
    this.kafkaControlClient = kafkaControlClient;
    this.transactionServices = transactionServices;
    this.partitionProvider = partitionProvider;
    this.events = new LinkedBlockingQueue<>();
    scheduler = Executors.newSingleThreadScheduledExecutor();
    executorService = Executors.newSingleThreadExecutor();


    this.currentCommitTime = StringUtils.EMPTY_STRING;
    this.partitionsWriteStatusReceived = new HashMap<>();
    this.globalCommittedKafkaOffsets = new HashMap<>();
    this.currentConsumedKafkaOffsets = new HashMap<>();
    this.currentState = State.INIT;
  }

  @Override
  public void start() {
    if (hasStarted.compareAndSet(false, true)) {
      executorService.submit(this);
    }
    kafkaControlClient.registerTransactionCoordinator(this);
    LOG.info(String.format("Start Transaction Coordinator for topic %s partition %s",
        partition.topic(), partition.partition()));

    initializeGlobalCommittedKafkaOffsets();
    // Submit the first start commit
    submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
            partition.topic(),
            StringUtils.EMPTY_STRING),
        START_COMMIT_INIT_DELAY_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    kafkaControlClient.deregisterTransactionCoordinator(this);
    scheduler.shutdownNow();
    hasStarted.set(false);
    if (executorService != null) {
      boolean terminated = false;
      try {
        LOG.info("Shutting down executor service.");
        executorService.shutdown();
        LOG.info("Awaiting termination.");
        terminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        LOG.warn(
            "Unclean Kafka Control Manager executor service shutdown ");
        executorService.shutdownNow();
      }
    }
  }

  @Override
  public TopicPartition getPartition() {
    return partition;
  }

  @Override
  public void processControlEvent(ControlMessage message) {
    CoordinatorEvent.CoordinatorEventType type;
    if (message.getType().equals(ControlMessage.EventType.WRITE_STATUS)) {
      type = CoordinatorEvent.CoordinatorEventType.WRITE_STATUS;
    } else {
      LOG.warn("Illegal message type [{}] processee by coordinator", message.getType().name());
      return;
    }

    CoordinatorEvent event = new CoordinatorEvent(type,
        message.getTopicName(),
        message.getCommitTime());
    event.setMessage(message);
    submitEvent(event);
  }

  @Override
  public void run() {
    while (true) {
      try {
        CoordinatorEvent event = events.poll(COORDINATOR_EVENT_LOOP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (event != null) {
          processCoordinatorEvent(event);
        }
      } catch (InterruptedException exception) {
        LOG.warn("Error received while polling the event loop in Partition Coordinator", exception);
      }
    }
  }

  private void submitEvent(CoordinatorEvent event) {
    this.submitEvent(event, 0, TimeUnit.SECONDS);
  }

  private void submitEvent(CoordinatorEvent event, long delay, TimeUnit unit) {
    scheduler.schedule(() -> {
      events.add(event);
    }, delay, unit);
  }

  private void processCoordinatorEvent(CoordinatorEvent event) {
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
      LOG.warn("Error received while polling the event loop in Partition Coordinator", exception);
    }
  }

  private void startNewCommit() {
    numPartitions = partitionProvider.getLatestNumPartitions(configs.getString(BOOTSTRAP_SERVERS_CFG), partition.topic());
    partitionsWriteStatusReceived.clear();
    try {
      currentCommitTime = transactionServices.startCommit();
      kafkaControlClient.publishMessage(buildControlMessage(ControlMessage.EventType.START_COMMIT));
      currentState = State.STARTED_COMMIT;
      // schedule a timeout for ending the current commit
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.END_COMMIT,
              partition.topic(),
              currentCommitTime),
          configs.getCommitIntervalSecs(), TimeUnit.SECONDS);
    } catch (Exception exception) {
      LOG.error(String.format("Failed to start a new commit %s, will retry", currentCommitTime), exception);
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
              partition.topic(),
              StringUtils.EMPTY_STRING),
          RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void endExistingCommit() {
    try {
      kafkaControlClient.publishMessage(buildControlMessage(ControlMessage.EventType.END_COMMIT));
    } catch (Exception exception) {
      LOG.warn("Could not send END_COMMIT message for partition {} and commitTime {}",
          partition, currentCommitTime, exception);
    }
    currentConsumedKafkaOffsets.clear();
    currentState = State.ENDED_COMMIT;

    // schedule a timeout for receiving all write statuses
    submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.WRITE_STATUS_TIMEOUT,
            partition.topic(),
            currentCommitTime),
        configs.getCoordinatorWriteTimeoutSecs(), TimeUnit.SECONDS);
  }

  private void onReceiveWriteStatus(ControlMessage message) {
    ControlMessage.ParticipantInfo participantInfo = message.getParticipantInfo();
    int partitionId = message.getSenderPartition();
    partitionsWriteStatusReceived.put(partitionId, KafkaConnectUtils.getWriteStatuses(participantInfo));
    currentConsumedKafkaOffsets.put(partitionId, participantInfo.getKafkaOffset());
    if (partitionsWriteStatusReceived.size() >= numPartitions
        && currentState.equals(State.ENDED_COMMIT)) {
      // Commit the kafka offsets to the commit file
      try {
        List<WriteStatus> allWriteStatuses = new ArrayList<>();
        partitionsWriteStatusReceived.forEach((key, value) -> allWriteStatuses.addAll(value));

        long totalErrorRecords = (long) allWriteStatuses.stream().mapToDouble(WriteStatus::getTotalErrorRecords).sum();
        long totalRecords = (long) allWriteStatuses.stream().mapToDouble(WriteStatus::getTotalRecords).sum();
        boolean hasErrors = totalErrorRecords > 0;

        if (!hasErrors || configs.allowCommitOnErrors()) {
          boolean success = transactionServices.endCommit(currentCommitTime,
              allWriteStatuses,
              transformKafkaOffsets(currentConsumedKafkaOffsets));

          if (success) {
            LOG.info("Commit " + currentCommitTime + " successful!");
            currentState = State.WRITE_STATUS_RCVD;
            globalCommittedKafkaOffsets.putAll(currentConsumedKafkaOffsets);
            submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.ACK_COMMIT,
                message.getTopicName(),
                currentCommitTime));
            return;
          } else {
            LOG.error("Commit " + currentCommitTime + " failed!");
          }
        } else if (hasErrors) {
          LOG.error("Coordinator found errors when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
          LOG.error("Printing out the top 100 errors");
          allWriteStatuses.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
            LOG.error("Global error :", ws.getGlobalError());
            if (ws.getErrors().size() > 0) {
              ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " is " + value));
            }
          });
        }

        // Submit the next start commit, that will rollback the current commit.
        currentState = State.FAILED_COMMIT;
        LOG.warn("Current commit {} failed. Starting a new commit after recovery delay of {} {}",
            currentCommitTime, RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS.name());
        submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
                partition.topic(),
                StringUtils.EMPTY_STRING),
            RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS);
      } catch (Exception exception) {
        LOG.error("Fatal error while committing file", exception);
      }
    }
  }

  private void handleWriteStatusTimeout() {
    // If we are still stuck in ENDED_STATE
    if (currentState.equals(State.ENDED_COMMIT)) {
      currentState = State.WRITE_STATUS_TIMEDOUT;
      LOG.warn("Current commit {} failed after a write status timeout. Starting a new commit after recovery delay of {} {}",
          currentCommitTime, RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS.name());
      // Submit the next start commit
      submitEvent(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT,
              partition.topic(),
              StringUtils.EMPTY_STRING),
          RESTART_COMMIT_DELAY_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void submitAckCommit() {
    try {
      kafkaControlClient.publishMessage(buildControlMessage(ControlMessage.EventType.ACK_COMMIT));
    } catch (Exception exception) {
      LOG.error("Could not send ACK_COMMIT message for partition {} and commitTime {}", partition, currentCommitTime, exception);
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
      Map<String, String> commitMetadata = transactionServices.fetchLatestExtraCommitMetadata();
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
    FAILED_COMMIT,
    WRITE_STATUS_RCVD,
    WRITE_STATUS_TIMEDOUT,
    ACKED_COMMIT,
  }

  /**
   * Provides the current partitions of a Kafka Topic dynamically.
   */
  public interface KafkaPartitionProvider {
    int getLatestNumPartitions(String bootstrapServers, String topicName);
  }

  private ControlMessage buildControlMessage(ControlMessage.EventType eventType) {
    return ControlMessage.newBuilder()
        .setProtocolVersion(KafkaConnectConfigs.CURRENT_PROTOCOL_VERSION)
        .setType(eventType)
        .setTopicName(partition.topic())
        .setSenderType(ControlMessage.EntityType.COORDINATOR)
        .setSenderPartition(partition.partition())
        .setReceiverType(ControlMessage.EntityType.PARTICIPANT)
        .setCommitTime(currentCommitTime)
        .setCoordinatorInfo(
            ControlMessage.CoordinatorInfo.newBuilder()
                .putAllGlobalKafkaCommitOffsets(globalCommittedKafkaOffsets)
                .build()
        ).build();
  }
}
