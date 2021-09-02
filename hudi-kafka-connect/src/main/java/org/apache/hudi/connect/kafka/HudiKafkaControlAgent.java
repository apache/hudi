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

import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.CoordinatorManager;
import org.apache.hudi.connect.core.TopicTransactionCoordinator;
import org.apache.hudi.connect.core.TransactionParticipant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
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
 * {@link TopicTransactionCoordinator} and {@link TransactionParticipant}s.
 * Use a single instance per worker (single-threaded),
 * and register multiple tasks that can receive the control messages.
 */
public class HudiKafkaControlAgent implements KafkaControlAgent {

  private static final Logger LOG = LoggerFactory.getLogger(HudiKafkaControlAgent.class);
  private static final Object LOCK = new Object();
  private static final long KAFKA_POLL_TIMEOUT_MS = 100;
  private static final int EXEC_SHUTDOWN_TIMEOUT_MS = 5000;

  private static HudiKafkaControlAgent kafkaCommunicationAgent;
  private final String bootstrapServers;
  private final String controlTopicName;
  private final ExecutorService executorService;
  private final CoordinatorManager transactionCoordinatorManager;
  // List of TransactionParticipants per Kafka Topic
  private final Map<String, ConcurrentLinkedQueue<TransactionParticipant>> partitionWorkers;
  private final KafkaControlProducer producer;

  private KafkaConsumer<String, ControlEvent> consumer;

  public HudiKafkaControlAgent(String bootstrapServers,
                               String controlTopicName,
                               CoordinatorManager transactionCoordinatorManager) {
    this.bootstrapServers = bootstrapServers;
    this.controlTopicName = controlTopicName;
    this.executorService = Executors.newSingleThreadExecutor();
    this.transactionCoordinatorManager = transactionCoordinatorManager;
    this.partitionWorkers = new HashMap<>();
    this.producer = new KafkaControlProducer(bootstrapServers, controlTopicName);
    start();
  }

  public static HudiKafkaControlAgent createKafkaControlManager(String bootstrapServers,
                                                                String controlTopicName,
                                                                CoordinatorManager transactionCoordinatorManager) {
    if (kafkaCommunicationAgent == null) {
      synchronized (LOCK) {
        if (kafkaCommunicationAgent == null) {
          kafkaCommunicationAgent = new HudiKafkaControlAgent(bootstrapServers, controlTopicName, transactionCoordinatorManager);
        }
      }
    }
    return kafkaCommunicationAgent;
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
  public void publishMessage(ControlEvent message) {
    producer.publishMessage(message);
  }

  private void start() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Todo fetch the worker id or name instead of a uuid.
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "hudi-control-group" + UUID.randomUUID().toString());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);

    // Since we are using Kafka Control Topic as a RPC like interface,
    // we want consumers to only process messages that are sent after they come online
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    consumer = new KafkaConsumer<>(props, new StringDeserializer(),
        new KafkaJsonDeserializer<>(ControlEvent.class));

    consumer.subscribe(Collections.singletonList(controlTopicName));

    executorService.submit(() -> {
      while (true) {
        ConsumerRecords<String, ControlEvent> records;
        records = consumer.poll(Duration.ofMillis(KAFKA_POLL_TIMEOUT_MS));
        for (ConsumerRecord<String, ControlEvent> record : records) {
          try {
            LOG.debug("Kafka consumerGroupId = {} topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                "", record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
            ControlEvent message = record.value();
            String senderTopic = message.senderPartition().topic();
            if (message.getSenderType().equals(ControlEvent.SenderType.COORDINATOR)) {
              if (partitionWorkers.containsKey(senderTopic)) {
                for (TransactionParticipant partitionWorker : partitionWorkers.get(senderTopic)) {
                  partitionWorker.publishControlEvent(message);
                }
              } else {
                LOG.warn("Failed to send message for unregistered participants for topic {}", senderTopic);
              }
            } else if (message.getSenderType().equals(ControlEvent.SenderType.PARTICIPANT)) {
              transactionCoordinatorManager.processControlEvent(message);
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

  /**
   * Deserializes the incoming Kafka records for the Control Topic.
   *
   * @param <T> represents the object that is sent over the Control Topic.
   */
  public static class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDeserializer.class);
    private final Class<T> type;

    KafkaJsonDeserializer(Class<T> type) {
      this.type = type;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
      ObjectMapper mapper = new ObjectMapper();
      T obj = null;
      try {
        obj = mapper.readValue(bytes, type);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
      return obj;
    }
  }
}
