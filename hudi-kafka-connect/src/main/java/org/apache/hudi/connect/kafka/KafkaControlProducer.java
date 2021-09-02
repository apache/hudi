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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka producer to send events to the
 * Control Topic that coordinates transactions
 * across Participants.
 */
public class KafkaControlProducer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaControlProducer.class);

  private final String bootstrapServers;
  private final String controlTopicName;
  private Producer<String, ControlEvent> producer;

  public KafkaControlProducer(String bootstrapServers, String controlTopicName) {
    this.bootstrapServers = bootstrapServers;
    this.controlTopicName = controlTopicName;
    start();
  }

  private void start() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);

    producer =
        new KafkaProducer<>(
            props,
            new StringSerializer(),
            new KafkaJsonSerializer()
        );
  }

  public void stop() {
    producer.close();
  }

  public void publishMessage(ControlEvent message) {
    ProducerRecord<String, ControlEvent> record
        = new ProducerRecord<>(controlTopicName, message.key(), message);
    producer.send(record);
  }

  public static class KafkaJsonSerializer implements Serializer<ControlEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSerializer.class);

    @Override
    public byte[] serialize(String topic, ControlEvent data) {
      byte[] retVal = null;
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

      try {
        retVal = objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        LOG.error("Fatal error during serialization of Kafka Control Message ", e);
      }
      return retVal;
    }
  }
}
