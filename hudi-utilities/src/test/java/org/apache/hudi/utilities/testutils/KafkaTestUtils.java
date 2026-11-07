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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.util.StringUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.Tuple2;

public class KafkaTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTestUtils.class);
  private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.7.1");
  private static final long TOPIC_READY_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);
  private final List<String> createdTopics = new ArrayList<>();
  private ConfluentKafkaContainer kafkaContainer;

  public KafkaTestUtils setup() {
    kafkaContainer = new ConfluentKafkaContainer(KAFKA_IMAGE);
    kafkaContainer.start();
    return this;
  }

  public String brokerAddress() {
    if (kafkaContainer == null || !kafkaContainer.isRunning()) {
      throw new IllegalStateException("Kafka container is not running. Please start the container first.");
    }
    return kafkaContainer.getBootstrapServers();
  }

  public void createTopic(String topic) {
    createTopic(topic, 1);
  }

  public void createTopic(String topic, int numPartitions) throws TopicExistsException {
    createTopic(topic, numPartitions, null);
  }

  public void createTopic(String topic, int numPartitions, Properties properties) throws TopicExistsException {
    Properties adminProps = getAdminProps();

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      createdTopics.add(topic);
      NewTopic newTopic = new NewTopic(topic, numPartitions, (short) 1);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();
      waitForPartitionLeaders(adminClient, topic, numPartitions);
      if (properties != null) {
        adminClient.alterConfigs(
            Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topic),
                new Config(
                    properties.entrySet().stream()
                        .map(e -> new ConfigEntry(
                            e.getKey().toString(), e.getValue().toString()))
                        .collect(Collectors.toList())))
        ).all().get();
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (e.getCause() instanceof TopicExistsException) {
        throw (TopicExistsException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Topic creation returns before the broker has elected partition leaders. Producing in that window
   * gets NOT_LEADER_OR_FOLLOWER, and the idempotent producer's retries can end in
   * OUT_OF_ORDER_SEQUENCE_NUMBER, which drops records. Wait until every partition has a leader.
   */
  private static void waitForPartitionLeaders(AdminClient adminClient, String topic, int numPartitions)
      throws ExecutionException, InterruptedException {
    long deadline = System.nanoTime() + TOPIC_READY_TIMEOUT_NANOS;
    while (true) {
      try {
        TopicDescription description =
            adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get();
        if (description.partitions().size() == numPartitions
            && description.partitions().stream().allMatch(p -> p.leader() != null && !p.leader().isEmpty())) {
          return;
        }
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          throw e;
        }
      }
      if (System.nanoTime() - deadline > 0) {
        throw new IllegalStateException("Timed out waiting for partition leaders of topic " + topic);
      }
      Thread.sleep(100);
    }
  }

  private Properties getAdminProps() {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    return adminProps;
  }

  public void deleteTopics() {
    if (createdTopics.isEmpty()) {
      return;
    }
    try (AdminClient adminClient = AdminClient.create(getAdminProps())) {
      adminClient.deleteTopics(createdTopics).all().get();
      createdTopics.clear();
    } catch (Exception e) {
      LOG.warn("Failed to delete topics: {}", StringUtils.join(createdTopics, ","), e);
    }
  }

  public void sendMessages(String topic, String[] messages) {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps())) {
      List<Future<RecordMetadata>> acks = new ArrayList<>(messages.length);
      for (String message : messages) {
        acks.add(producer.send(new ProducerRecord<>(topic, message)));
      }
      producer.flush();
      awaitAcks(topic, acks);
    }
  }

  public void sendMessages(String topic, Tuple2<String, String>[] keyValuePairs) {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps())) {
      List<Future<RecordMetadata>> acks = new ArrayList<>(keyValuePairs.length);
      for (Tuple2<String, String> kv : keyValuePairs) {
        acks.add(producer.send(new ProducerRecord<>(topic, kv._1, kv._2)));
      }
      producer.flush();
      awaitAcks(topic, acks);
    }
  }

  private static void awaitAcks(String topic, List<Future<RecordMetadata>> acks) {
    for (Future<RecordMetadata> ack : acks) {
      try {
        ack.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while sending records to Kafka topic " + topic, e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to send a record to Kafka topic " + topic, e.getCause());
      }
    }
  }

  private Properties getProducerProps() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProps;
  }

  public void teardown() {
    if (kafkaContainer != null && kafkaContainer.isRunning()) {
      kafkaContainer.stop();
    }
  }
}
