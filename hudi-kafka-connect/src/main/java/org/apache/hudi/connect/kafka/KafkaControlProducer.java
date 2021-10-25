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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Kafka producer to send events to the
 * Control Topic that coordinates transactions
 * across Participants.
 */
public class KafkaControlProducer {

  private static final Logger LOG = LogManager.getLogger(KafkaControlProducer.class);

  private final String bootstrapServers;
  private final String controlTopicName;
  private Producer<String, byte[]> producer;

  public KafkaControlProducer(String bootstrapServers, String controlTopicName) {
    this.bootstrapServers = bootstrapServers;
    this.controlTopicName = controlTopicName;
    start();
  }

  private void start() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    producer = new KafkaProducer<>(
        props,
        new StringSerializer(),
        new ByteArraySerializer()
    );
  }

  public void stop() {
    producer.close();
  }

  public void publishMessage(ControlMessage message) {
    ProducerRecord<String, byte[]> record
        = new ProducerRecord<>(controlTopicName, message.getType().name(), message.toByteArray());
    producer.send(record);
  }
}
