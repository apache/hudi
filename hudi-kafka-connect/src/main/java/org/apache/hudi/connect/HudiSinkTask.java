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

import org.apache.hudi.connect.core.HudiTransactionCoordinator;
import org.apache.hudi.connect.core.HudiTransactionParticipant;
import org.apache.hudi.connect.kafka.HudiKafkaControlAgent;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
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
public class HudiSinkTask extends SinkTask {

  public static final String TASK_ID_CONFIG_NAME = "task.id";
  private static final Logger LOG = LoggerFactory.getLogger(HudiSinkTask.class);
  private static final int COORDINATOR_KAFKA_PARTITION = 0;

  private final Map<TopicPartition, HudiTransactionParticipant> hudiTransactionParticipants;
  private HudiTransactionCoordinator hudiTransactionCoordinator;
  private HudiKafkaControlAgent controlKafkaClient;
  private HudiConnectConfigs connectConfigs;

  private String taskId;
  private String connectorName;

  public HudiSinkTask() {
    hudiTransactionParticipants = new HashMap<>();
  }

  @Override
  public String version() {
    return HudiSinkConnector.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    connectorName = props.get("name");
    taskId = props.get(TASK_ID_CONFIG_NAME);
    LOG.info("Starting Hudi Sink Task for {} connector {} with id {} with assignments {}", props, connectorName, taskId, context.assignment());
    try {
      connectConfigs = HudiConnectConfigs.newBuilder().withProperties(props).build();
      controlKafkaClient = HudiKafkaControlAgent.createKafkaControlManager("localhost:9092", connectConfigs.getControlTopicName());
      bootstrap(context.assignment());
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
      hudiTransactionParticipants.get(tp).buffer(record);
    }

    for (TopicPartition partition : context.assignment()) {
      hudiTransactionParticipants.get(partition).processRecords();
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
      HudiTransactionParticipant worker = hudiTransactionParticipants.get(partition);
      if (worker != null) {
        worker.processRecords();
        if (worker.getLastKafkaCommittedOffset() >= 0) {
          result.put(partition, new OffsetAndMetadata(worker.getLastKafkaCommittedOffset()));
        }
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
      if (partition.partition() == COORDINATOR_KAFKA_PARTITION && hudiTransactionCoordinator != null) {
        hudiTransactionCoordinator.stop();
      }
      HudiTransactionParticipant worker = hudiTransactionParticipants.remove(partition);
      if (worker != null) {
        try {
          LOG.debug("Closing data writer due to task start failure.");
          worker.stop();
        } catch (Throwable t) {
          LOG.debug("Error closing and stopping data writer: {}", t.getMessage(), t);
        }
      }
    }
  }

  private void bootstrap(Collection<TopicPartition> partitions) {
    LOG.info("Bootstrap task for connector {} with id {} with assignments {} part {}",
        connectorName, taskId, context.assignment(), partitions);
    for (TopicPartition partition : partitions) {
      try {
        // If the partition is 0, instantiate the Leader
        if (partition.partition() == COORDINATOR_KAFKA_PARTITION) {
          hudiTransactionCoordinator = new HudiTransactionCoordinator(connectConfigs, partition, controlKafkaClient);
          hudiTransactionCoordinator.start();
        }
        HudiTransactionParticipant worker = new HudiTransactionParticipant(connectConfigs, partition, controlKafkaClient, context);
        hudiTransactionParticipants.put(partition, worker);
        worker.start();
      } catch (HoodieException exception) {
        LOG.error("Fatal error initializing task {} for partition {}", taskId, partition.partition(), exception);
      }
    }
  }

  private void cleanup() {
    for (TopicPartition partition : context.assignment()) {
      HudiTransactionParticipant worker = hudiTransactionParticipants.get(partition);
      if (worker != null) {
        try {
          LOG.debug("Closing data writer due to task start failure.");
          worker.stop();
        } catch (Throwable t) {
          LOG.debug("Error closing and stopping data writer: {}", t.getMessage(), t);
        }
      }
    }
    hudiTransactionParticipants.clear();
    if (hudiTransactionCoordinator != null) {
      hudiTransactionCoordinator.stop();
    }
  }
}
