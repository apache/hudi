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

package org.apache.hudi.connect.kafka;

import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class that manages the Kafka consumer and producer for
 * the Kafka Control Topic that ensures coordination across the
 * {@link TransactionCoordinator} and {@link TransactionParticipant}s.
 * Use a single instance per worker (single-threaded),
 * and register multiple tasks that can receive the control messages.
 */
public class KafkaConnectControlAgent implements KafkaControlAgent {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectControlAgent.class);
  private static final Object LOCK = new Object();
  private static final long KAFKA_POLL_TIMEOUT_MS = 100;
  private static final int EXEC_SHUTDOWN_TIMEOUT_MS = 5000;

  private static KafkaConnectControlAgent agent;
  private final String bootstrapServers;
  private final String controlTopicName;
  private final ExecutorService executorService;
  private final Map<String, TransactionCoordinator> topicCoordinators;
  // List of TransactionParticipants per Kafka Topic
  private final Map<String, ConcurrentLinkedQueue<TransactionParticipant>> partitionWorkers;
  private final KafkaControlProducer producer;
  private KafkaConsumer<String, byte[]> consumer;

  public KafkaConnectControlAgent(String bootstrapServers,
                                  String controlTopicName) {
    this.bootstrapServers = bootstrapServers;
    this.controlTopicName = controlTopicName;
    this.executorService = Executors.newSingleThreadExecutor();
    this.topicCoordinators = new HashMap<>();
    this.partitionWorkers = new HashMap<>();
    this.producer = new KafkaControlProducer(bootstrapServers, controlTopicName);
    start();
  }

  public static KafkaConnectControlAgent createKafkaControlManager(String bootstrapServers,
                                                                   String controlTopicName) {
    if (agent == null) {
      synchronized (LOCK) {
        if (agent == null) {
          agent = new KafkaConnectControlAgent(bootstrapServers, controlTopicName);
        }
      }
    }
    return agent;
  }

  @Override
  public void registerTransactionParticipant(TransactionParticipant worker) {
    if (!partitionWorkers.containsKey(worker.getPartition().topic())) {
      partitionWorkers.put(worker.getPartition().topic(), new ConcurrentLinkedQueue<>());
    }
    partitionWorkers.get(worker.getPartition().topic()).add(worker);
  }

  @Override
  public void deregisterTransactionParticipant(TransactionParticipant worker) {
    if (partitionWorkers.containsKey(worker.getPartition().topic())) {
      partitionWorkers.get(worker.getPartition().topic()).remove(worker);
    }
  }

  @Override
  public void registerTransactionCoordinator(TransactionCoordinator coordinator) {
    if (!topicCoordinators.containsKey(coordinator.getPartition().topic())) {
      topicCoordinators.put(coordinator.getPartition().topic(), coordinator);
    }
  }

  public void deregisterTransactionCoordinator(TransactionCoordinator coordinator) {
    topicCoordinators.remove(coordinator.getPartition().topic());
  }

  @Override
  public void publishMessage(ControlMessage message) {
    producer.publishMessage(message);
  }

  private void start() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Todo fetch the worker id or name instead of a uuid.
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "hudi-control-group" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    // Since we are using Kafka Control Topic as a RPC like interface,
    // we want consumers to only process messages that are sent after they come online
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());

    consumer.subscribe(Collections.singletonList(controlTopicName));

    executorService.submit(() -> {
      while (true) {
        ConsumerRecords<String, byte[]> records;
        records = consumer.poll(Duration.ofMillis(KAFKA_POLL_TIMEOUT_MS));
        for (ConsumerRecord<String, byte[]> record : records) {
          try {
            LOG.debug("Kafka consumerGroupId = {} topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                "", record.topic(), record.partition(), record.offset(), record.key(), record.value());
            ControlMessage message = ControlMessage.parseFrom(record.value());
            String senderTopic = message.getTopicName();

            if (message.getReceiverType().equals(ControlMessage.EntityType.PARTICIPANT)) {
              if (partitionWorkers.containsKey(senderTopic)) {
                for (TransactionParticipant partitionWorker : partitionWorkers.get(senderTopic)) {
                  partitionWorker.processControlEvent(message);
                }
              } else {
                LOG.error("Failed to send message for unregistered participants for topic {}", senderTopic);
              }
            } else if (message.getReceiverType().equals(ControlMessage.EntityType.COORDINATOR)) {
              if (topicCoordinators.containsKey(senderTopic)) {
                topicCoordinators.get(senderTopic).processControlEvent(message);
              }
            } else {
              LOG.warn("Sender type of Control Message unknown {}", message.getSenderType().name());
            }
          } catch (Exception e) {
            LOG.error("Fatal error while consuming a kafka record for topic = {} partition = {}", record.topic(), record.partition(), e);
          }
        }
        try {
          consumer.commitSync();
        } catch (CommitFailedException exception) {
          LOG.error("Fatal error while committing kafka control topic");
        }
      }
    });
  }

  public void stop() {
    producer.stop();
    consumer.close();
    if (executorService != null) {
      boolean terminated = false;
      try {
        LOG.info("Shutting down executor service.");
        executorService.shutdown();
        LOG.info("Awaiting termination.");
        terminated = executorService.awaitTermination(EXEC_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
}
