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

import org.apache.hudi.connect.kafka.KafkaConnectControlAgent;
import org.apache.hudi.connect.transaction.ConnectTransactionCoordinator;
import org.apache.hudi.connect.transaction.ConnectTransactionParticipant;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link SinkTask} interface provided by
 * Kafka Connect. Implements methods to receive the Kafka records
 * from the assigned partitions and commit the Kafka offsets.
 * Also, handles re-assignments of partitions.
 */
public class HoodieSinkTask extends SinkTask {

  public static final String TASK_ID_CONFIG_NAME = "task.id";
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSinkTask.class);

  private final Map<TopicPartition, TransactionCoordinator> transactionCoordinators;
  private final Map<TopicPartition, TransactionParticipant> transactionParticipants;
  private KafkaConnectControlAgent controlKafkaClient;
  private KafkaConnectConfigs connectConfigs;

  private String taskId;
  private String connectorName;

  public HoodieSinkTask() {
    transactionCoordinators = new HashMap<>();
    transactionParticipants = new HashMap<>();
  }

  @Override
  public String version() {
    return HoodieSinkConnector.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    connectorName = props.get("name");
    taskId = props.get(TASK_ID_CONFIG_NAME);
    LOG.info(String.format("Starting Hudi Sink Task for %s connector %s with id %s with assignments %s",
        props, connectorName, taskId, context.assignment()));
    try {
      connectConfigs = KafkaConnectConfigs.newBuilder().withProperties(props).build();
      controlKafkaClient = KafkaConnectControlAgent.createKafkaControlManager(
          connectConfigs.getBootstrapServers(),
          connectConfigs.getControlTopicName());
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error.", e);
    } catch (ConnectException e) {
      LOG.error("Couldn't start HudiSinkConnector:", e);
      LOG.info("Shutting down HudiSinkConnector.");
      cleanup();
      // Always throw the original exception that prevent us from starting
      throw e;
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);

      TransactionParticipant transactionParticipant = transactionParticipants.get(tp);
      if (transactionParticipant != null) {
        transactionParticipant.buffer(record);
      }
    }

    for (TopicPartition partition : context.assignment()) {
      if (transactionParticipants.get(partition) == null) {
        throw new RetriableException("TransactionParticipant should be created for each assigned partition, "
            + "but has not been created for the topic/partition: " + partition.topic() + ":" + partition.partition());
      }
      try {
        transactionParticipants.get(partition).processRecords();
      } catch (HoodieIOException exception) {
        throw new RetriableException("Intermittent write errors for Hudi "
            + " for the topic/partition: " + partition.topic() + ":" + partition.partition()
            + " , ensuring kafka connect will retry ", exception);
      }
    }
  }

  @Override
  public void stop() {
    cleanup();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // No-op. The connector is managing the offsets.
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Although the connector manages offsets via commit files in Hudi, we still want to have Connect
    // commit the consumer offsets for records this task has consumed from its topic partitions and
    // committed to Hudi.
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    for (TopicPartition partition : context.assignment()) {
      TransactionParticipant worker = transactionParticipants.get(partition);
      if (worker != null && worker.getLastKafkaCommittedOffset() >= 0) {
        result.put(partition, new OffsetAndMetadata(worker.getLastKafkaCommittedOffset()));
      }
    }
    return result;
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOG.info("New partitions added " + partitions.toString());
    bootstrap(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOG.info("Existing partitions deleted " + partitions.toString());
    // Close any writers we have. We may get assigned the same partitions and end up duplicating
    // some effort since we'll have to reprocess those messages. It may be possible to hold on to
    // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
    // more complex due to potential failures and network partitions. For example, we may get
    // this close, then miss a few generations of group membership, during which
    // data may have continued to be processed and we'd have to restart from the recovery stage,
    // make sure we apply the WAL, and only reuse the temp file if the starting offset is still
    // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
    for (TopicPartition partition : partitions) {
      if (partition.partition() == ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION) {
        if (transactionCoordinators.containsKey(partition)) {
          transactionCoordinators.get(partition).stop();
          transactionCoordinators.remove(partition);
        }
      }
      TransactionParticipant worker = transactionParticipants.remove(partition);
      if (worker != null) {
        try {
          LOG.debug("Closing data writer due to task start failure.");
          worker.stop();
        } catch (Throwable t) {
          LOG.warn("Error closing and stopping data writer", t);
        }
      }
    }
  }

  private void bootstrap(Collection<TopicPartition> partitions) {
    LOG.info(String.format("Bootstrap task for connector %s with id %s with assignments %s part %s",
        connectorName, taskId, context.assignment(), partitions));
    for (TopicPartition partition : partitions) {
      try {
        // If the partition is 0, instantiate the Leader
        if (partition.partition() == ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION) {
          ConnectTransactionCoordinator coordinator = new ConnectTransactionCoordinator(
              connectConfigs,
              partition,
              controlKafkaClient);
          coordinator.start();
          transactionCoordinators.put(partition, coordinator);
        }
        ConnectTransactionParticipant worker = new ConnectTransactionParticipant(connectConfigs, partition, controlKafkaClient, context);
        transactionParticipants.put(partition, worker);
        worker.start();
      } catch (HoodieException exception) {
        LOG.error(String.format("Fatal error initializing task %s for partition %s", taskId, partition.partition()), exception);
      }
    }
  }

  private void cleanup() {
    for (TopicPartition partition : context.assignment()) {
      TransactionParticipant worker = transactionParticipants.get(partition);
      if (worker != null) {
        try {
          LOG.debug("Closing data writer due to task start failure.");
          worker.stop();
        } catch (Throwable t) {
          LOG.debug("Error closing and stopping data writer", t);
        }
      }
    }
    transactionParticipants.clear();
    transactionCoordinators.forEach((topic, transactionCoordinator) -> transactionCoordinator.stop());
    transactionCoordinators.clear();
  }
}
