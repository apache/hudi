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

package org.apache.hudi.utilities.serde;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Properties;

public class TestHoodieKafkaAvroDecoder extends UtilitiesTestBase {

  private static KafkaAvroSerializer avroSerializer;
  private static KafkaAvroDeserializer avroDeserializer;
  private static String topic;

  @BeforeAll
  public static void init() throws Exception {
    UtilitiesTestBase.initClass();
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    avroSerializer = new KafkaAvroSerializer(schemaRegistryClient, new HashMap(defaultConfig));
    topic = "test_topic";

    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_uber.avsc", dfs, dfsBasePath + "/source_uber.avsc");
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  private GenericRecord createAvroRecord(Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("_row_key", "testUser");
    avroRecord.put("timestamp", 0.0);
    avroRecord.put("rider", "dummy_rider");
    avroRecord.put("driver", "dummy_driver");
    avroRecord.put("fare", 50.0);
    avroRecord.put("_hoodie_is_deleted", false);
    return avroRecord;
  }

  @Test
  public void testKafkaDeserializer() {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty(FilebasedSchemaProvider.Config.SOURCE_SCHEMA_FILE_PROP, dfsBasePath + "/source_uber.avsc");
    VerifiableProperties verifiableProperties = new VerifiableProperties(typedProperties);
    FilebasedSchemaProvider schemaProvider = new FilebasedSchemaProvider(typedProperties, jsc);
    byte[] bytes;
    GenericRecord record = createAvroRecord(schemaProvider.getSourceSchema());
    bytes = avroSerializer.serialize(topic, record);
    HoodieKafkaAvroDecoder kafkaAvroDecoder = new HoodieKafkaAvroDecoder(verifiableProperties);
    Assertions.assertEquals(record, avroDeserializer.deserialize(topic, bytes));
    Assertions.assertEquals(record, kafkaAvroDecoder.deserialize(bytes));
  }
}
