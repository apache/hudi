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

package org.apache.hudi.utilities.sources.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.hudi.common.config.TypedProperties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.schema.DummySchemaProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHoodieAvroKafkaDeserializer {

  private static KafkaAvroSerializer avroSerializer;
  private static SchemaRegistryClient client;
  private static KafkaAvroDeserializer avroDeserializer;
  private static String topic;

  @BeforeAll
  public static void init() {
    client = new MockSchemaRegistryClient();
    avroDeserializer = new KafkaAvroDeserializer(client);
    avroSerializer = new KafkaAvroSerializer(client);
    topic = "test_topic";
  }

  private GenericRecord createAvroRecord(Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("favorite_number", 13);
    avroRecord.put("favorite_color", "dummy_color");
    return avroRecord;
  }

  @Test
  public void testKafkaDeserializer() {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    typedProperties.setProperty(HoodieAvroKafkaDeserializer.SCHEMA_PROVIDER_CLASS_PROP, DummySchemaProvider.class.getName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(typedProperties);
    DummySchemaProvider schemaProvider = new DummySchemaProvider(typedProperties);
    byte[] bytes;
    GenericRecord record = createAvroRecord(schemaProvider.getSourceSchema());
    bytes = avroSerializer.serialize(topic, record);
    HoodieAvroKafkaDeserializer kafkaAvroDecoder = new HoodieAvroKafkaDeserializer(client, verifiableProperties);
    Assertions.assertEquals(record, avroDeserializer.deserialize(topic, bytes));
    Assertions.assertEquals(record, kafkaAvroDecoder.fromBytes(bytes));
  }
}
