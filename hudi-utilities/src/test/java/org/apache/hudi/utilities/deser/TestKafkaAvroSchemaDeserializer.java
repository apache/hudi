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

package org.apache.hudi.utilities.deser;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.hudi.utilities.sources.helpers.SchemaTestProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link KafkaAvroSchemaDeserializer}.
 */
public class TestKafkaAvroSchemaDeserializer extends UtilitiesTestBase {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final String topic;
  private final Schema origSchema = createUserSchema();
  private final Schema evolSchema = createExtendUserSchema();
  private Properties config = new Properties();

  public TestKafkaAvroSchemaDeserializer() {
    config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(config));
    topic = "test";
  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(userSchema);
  }

  private IndexedRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private Schema createExtendUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, "
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"], \"default\": null}]}";
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(userSchema);
  }

  private IndexedRecord createExtendUserRecord() {
    Schema schema = createExtendUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("age", 30);
    return avroRecord;
  }

  /**
   * Tests {@link KafkaAvroSchemaDeserializer#deserialize(Boolean, String, Boolean, byte[], Schema)}.
   */
  @Test
  public void testKafkaAvroSchemaDeserializer() {
    byte[] bytesOrigRecord;
    IndexedRecord avroRecord = createUserRecord();
    config.put(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, origSchema.toString());

    KafkaAvroSchemaDeserializer avroDeserializer = new KafkaAvroSchemaDeserializer(schemaRegistry, new HashMap(config));
    avroDeserializer.configure(new HashMap(config), false);
    bytesOrigRecord = avroSerializer.serialize(topic, avroRecord);
    // record is serialized in orig schema and deserialized using same schema.
    assertEquals(avroRecord, avroDeserializer.deserialize(false, topic, false, bytesOrigRecord, origSchema));

    IndexedRecord avroRecordWithAllField = createExtendUserRecord();
    byte[] bytesExtendedRecord = avroSerializer.serialize(topic, avroRecordWithAllField);

    SchemaTestProvider.schemaToReturn.set(evolSchema);
    config.put(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, evolSchema.toString());
    avroDeserializer = new KafkaAvroSchemaDeserializer(schemaRegistry, new HashMap(config));
    avroDeserializer.configure(new HashMap(config), false);
    // record is serialized w/ evolved schema, and deserialized w/ evolved schema
    IndexedRecord avroRecordWithAllFieldActual = (IndexedRecord) avroDeserializer.deserialize(false, topic, false, bytesExtendedRecord, evolSchema);
    assertEquals(avroRecordWithAllField, avroRecordWithAllFieldActual);
    assertEquals(avroRecordWithAllFieldActual.getSchema(), evolSchema);

    // read old record w/ evolved schema.
    IndexedRecord actualRec = (IndexedRecord) avroDeserializer.deserialize(false, topic, false, bytesOrigRecord, origSchema);
    // record won't be equal to original record as we read w/ evolved schema. "age" will be added w/ default value of null
    assertNotEquals(avroRecord, actualRec);
    GenericRecord genericRecord = (GenericRecord) actualRec;
    GenericRecord origGenRec = (GenericRecord) avroRecord;
    assertEquals(genericRecord.get("name").toString(), origGenRec.get("name").toString());
    assertEquals(actualRec.getSchema(), evolSchema);
    assertNull(genericRecord.get("age"));
  }

  protected TypedProperties getConvertToTypedProperties(Map<String, ?> configs) {
    TypedProperties typedProperties = new TypedProperties();
    for (Entry<String, ?> entry : configs.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
    return typedProperties;
  }
}
