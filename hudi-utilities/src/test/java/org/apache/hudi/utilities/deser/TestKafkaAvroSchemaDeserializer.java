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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.hudi.utilities.sources.helpers.SchemaTestProvider;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link KafkaAvroSchemaDeserializer}.
 */
public class TestKafkaAvroSchemaDeserializer {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final String topic;
  private final HoodieSchema origSchema = createUserSchema();
  private final HoodieSchema evolSchema = createExtendUserSchema();
  private final Properties config = new Properties();

  public TestKafkaAvroSchemaDeserializer() {
    config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(config));
    topic = "test";
  }

  private HoodieSchema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    return HoodieSchema.parse(userSchema);
  }

  private IndexedRecord createUserRecord() {
    HoodieSchema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema.toAvroSchema());
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private HoodieSchema createExtendUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, "
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"], \"default\": null}]}";
    return HoodieSchema.parse(userSchema);
  }

  private IndexedRecord createExtendUserRecord() {
    HoodieSchema schema = createExtendUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema.toAvroSchema());
    avroRecord.put("name", "testUser");
    avroRecord.put("age", 30);
    return avroRecord;
  }

  /**
   * Tests {@link KafkaAvroSchemaDeserializer#deserialize(String, Boolean, byte[], Schema)}.
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
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, false, bytesOrigRecord, origSchema.toAvroSchema()));

    IndexedRecord avroRecordWithAllField = createExtendUserRecord();
    byte[] bytesExtendedRecord = avroSerializer.serialize(topic, avroRecordWithAllField);

    SchemaTestProvider.schemaToReturn.set(evolSchema);
    config.put(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, evolSchema.toString());
    avroDeserializer = new KafkaAvroSchemaDeserializer(schemaRegistry, new HashMap(config));
    avroDeserializer.configure(new HashMap(config), false);
    // record is serialized w/ evolved schema, and deserialized w/ evolved schema
    IndexedRecord avroRecordWithAllFieldActual = (IndexedRecord) avroDeserializer.deserialize(topic, false, bytesExtendedRecord, evolSchema.toAvroSchema());
    assertEquals(avroRecordWithAllField, avroRecordWithAllFieldActual);
    assertEquals(HoodieSchema.fromAvroSchema(avroRecordWithAllFieldActual.getSchema()), evolSchema);

    // read old record w/ evolved schema.
    IndexedRecord actualRec = (IndexedRecord) avroDeserializer.deserialize(topic, false, bytesOrigRecord, origSchema.toAvroSchema());
    // record won't be equal to original record as we read w/ evolved schema. "age" will be added w/ default value of null
    assertNotEquals(avroRecord, actualRec);
    GenericRecord genericRecord = (GenericRecord) actualRec;
    GenericRecord origGenRec = (GenericRecord) avroRecord;
    assertEquals(genericRecord.get("name").toString(), origGenRec.get("name").toString());
    assertEquals(HoodieSchema.fromAvroSchema(actualRec.getSchema()), evolSchema);
    assertNull(genericRecord.get("age"));
  }

  private Schema loadSchemaFromResource(String resourcePath) throws IOException {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      return new Schema.Parser().parse(is);
    }
  }

  private static Schema getRecordTypeFromUnion(Schema unionSchema) {
    return unionSchema.getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No record type in union"));
  }

  private GenericRecord createCdcSourceRecord(Schema envelopeSchema) {
    Schema sourceSchema = envelopeSchema.getField("source").schema();
    GenericRecord source = new GenericData.Record(sourceSchema);
    source.put("version", "2.3.0.Final");
    source.put("connector", "mysql");
    source.put("name", "cdc");
    source.put("ts_ms", 1700000000000L);
    source.put("snapshot", "false");
    source.put("db", "testdb");
    source.put("sequence", null);
    source.put("table", "item");
    source.put("server_id", 1L);
    source.put("gtid", null);
    source.put("file", "binlog.000001");
    source.put("pos", 12345L);
    source.put("row", 0);
    source.put("thread", null);
    source.put("query", null);
    return source;
  }

  private GenericRecord createCdcValueRecord(Schema envelopeSchema) {
    Schema valueSchema = getRecordTypeFromUnion(envelopeSchema.getField("before").schema());
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("id", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
    value.put("account_id", 42);
    value.put("title", "Test Item");
    value.put("query", "test query");
    value.put("request_query_id", null);
    value.put("page", null);
    value.put("request_page_id", null);
    value.put("source_rank_id", null);
    value.put("property_id", 100);
    value.put("created", "2024-01-01T00:00:00Z");
    value.put("created_by", 1);
    value.put("updated", "2024-01-02T00:00:00Z");
    value.put("updated_by", 2);
    value.put("version_id", null);
    value.put("related_urls", null);
    value.put("assignee", 5);
    value.put("status", "IN_PROGRESS");
    value.put("tags", null);
    value.put("deleted", false);
    value.put("profile_id", null);
    return value;
  }

  private GenericRecord createCdcValueRecordBefore(Schema envelopeSchema) {
    Schema valueSchema = getRecordTypeFromUnion(envelopeSchema.getField("before").schema());
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("id", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
    value.put("account_id", 42);
    value.put("title", "Old Item Title");
    value.put("query", "old query");
    value.put("request_query_id", null);
    value.put("page", "https://example.com/old");
    value.put("request_page_id", null);
    value.put("source_rank_id", 10);
    value.put("property_id", 100);
    value.put("created", "2024-01-01T00:00:00Z");
    value.put("created_by", 1);
    value.put("updated", "2024-01-02T00:00:00Z");
    value.put("updated_by", 1);
    value.put("version_id", "v1");
    value.put("related_urls", "[\"https://example.com\"]");
    value.put("assignee", 5);
    value.put("status", "TO_DO");
    value.put("tags", null);
    value.put("deleted", false);
    value.put("profile_id", null);
    return value;
  }

  private GenericRecord createCdcEnvelopeRecord(Schema envelopeSchema, GenericRecord beforeRecord, GenericRecord afterRecord) {
    GenericRecord envelope = new GenericData.Record(envelopeSchema);
    envelope.put("before", beforeRecord);
    envelope.put("after", afterRecord);
    envelope.put("source", createCdcSourceRecord(envelopeSchema));
    envelope.put("op", "u");
    envelope.put("ts_ms", 1700000000000L);
    envelope.put("transaction", null);
    return envelope;
  }

  /**
   * Tests deserialization of a CDC (Debezium) envelope schema with schema evolution
   * across all deserialize method overloads. The old schema lacks 4 fields
   * (notes, search_engine_id, locale_id, language_id) in the nested Value record.
   * When deserializing old records with the evolved schema, those new fields should default to null.
   *
   * Exercises:
   * - deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema)
   * - deserialize(String topic, byte[] bytes)
   * - deserialize(String topic, byte[] bytes, Schema readerSchema)
   * - deserialize(String topic, Headers headers, byte[] bytes)
   */
  @Test
  public void testKafkaAvroSchemaDeserializerWithCdcEnvelopeEvolution() throws IOException {
    Schema cdcOldSchema = loadSchemaFromResource("schema/cdc_envelope_old.avsc");
    Schema cdcNewSchema = loadSchemaFromResource("schema/cdc_envelope_new.avsc");

    // Create and serialize records with old schema (no notes/search_engine_id/locale_id/language_id)
    GenericRecord oldBefore = createCdcValueRecordBefore(cdcOldSchema);
    GenericRecord oldAfter = createCdcValueRecord(cdcOldSchema);
    GenericRecord oldEnvelope = createCdcEnvelopeRecord(cdcOldSchema, oldBefore, oldAfter);
    byte[] bytesOldRecord = avroSerializer.serialize(topic, oldEnvelope);

    // Create and serialize records with evolved schema (new fields populated)
    GenericRecord newBefore = createCdcValueRecordBefore(cdcNewSchema);
    GenericRecord newAfter = createCdcValueRecord(cdcNewSchema);
    newAfter.put("notes", "[{\"note\": \"test\"}]");
    newAfter.put("search_engine_id", "[\"default\"]");
    newAfter.put("locale_id", 1);
    newAfter.put("language_id", 2);
    GenericRecord newEnvelope = createCdcEnvelopeRecord(cdcNewSchema, newBefore, newAfter);
    byte[] bytesNewRecord = avroSerializer.serialize(topic, newEnvelope);

    // Configure deserializer with evolved schema
    config.put(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, cdcNewSchema.toString());
    KafkaAvroSchemaDeserializer deserializer = new KafkaAvroSchemaDeserializer(schemaRegistry, new HashMap(config));
    deserializer.configure(new HashMap(config), false);

    // === 1. deserialize(String, Boolean, byte[], Schema) — the existing override ===
    IndexedRecord evolvedDeserialized = (IndexedRecord) deserializer.deserialize(topic, false, bytesOldRecord, cdcNewSchema);
    GenericRecord evolvedEnvelope = (GenericRecord) evolvedDeserialized;
    assertEquals(cdcNewSchema, evolvedEnvelope.getSchema());
    assertEquals("u", evolvedEnvelope.get("op").toString());
    // Validate before record
    GenericRecord beforeRecord = (GenericRecord) evolvedEnvelope.get("before");
    assertEquals("Old Item Title", beforeRecord.get("title").toString());
    assertEquals(42, beforeRecord.get("account_id"));
    assertEquals("TO_DO", beforeRecord.get("status").toString());
    assertNull(beforeRecord.get(20));
    assertNull(beforeRecord.get(21));
    assertNull(beforeRecord.get(22));
    assertNull(beforeRecord.get(23));
    // Validate after record
    GenericRecord afterRecord = (GenericRecord) evolvedEnvelope.get("after");
    assertEquals("Test Item", afterRecord.get("title").toString());
    assertNull(afterRecord.get("notes"));
    assertNull(afterRecord.get("search_engine_id"));
    assertNull(afterRecord.get("locale_id"));
    assertNull(afterRecord.get("language_id"));

    // Evolved record via same method — should round-trip exactly
    IndexedRecord newDeserialized = (IndexedRecord) deserializer.deserialize(topic, false, bytesNewRecord, cdcNewSchema);
    assertEquals(newEnvelope, newDeserialized);

    // === 2. deserialize(String, byte[]) ===
    GenericRecord topicBytesResult = (GenericRecord) deserializer.deserialize(topic, bytesOldRecord);
    GenericRecord tbBefore = (GenericRecord) topicBytesResult.get("before");
    assertEquals("Old Item Title", tbBefore.get("title").toString());
    assertNull(tbBefore.get(20));
    assertNull(tbBefore.get(21));
    assertNull(tbBefore.get(22));
    assertNull(tbBefore.get(23));

    // === 3. deserialize(String, byte[], Schema) ===
    GenericRecord topicBytesSchemaResult = (GenericRecord) deserializer.deserialize(topic, bytesOldRecord, cdcNewSchema);
    GenericRecord tbsBefore = (GenericRecord) topicBytesSchemaResult.get("before");
    assertEquals("Old Item Title", tbsBefore.get("title").toString());
    assertNull(tbsBefore.get(20));
    assertNull(tbsBefore.get(21));
    assertNull(tbsBefore.get(22));
    assertNull(tbsBefore.get(23));

    // === 4. deserialize(String, Headers, byte[]) ===
    RecordHeaders headers = new RecordHeaders();
    GenericRecord headersResult = (GenericRecord) deserializer.deserialize(topic, headers, bytesOldRecord);
    assertEquals(cdcNewSchema, headersResult.getSchema());
    GenericRecord hdrBefore = (GenericRecord) headersResult.get("before");
    assertEquals("Old Item Title", hdrBefore.get("title").toString());
    assertEquals(42, hdrBefore.get("account_id"));
    assertNull(hdrBefore.get(20));
    assertNull(hdrBefore.get(21));
    assertNull(hdrBefore.get(22));
    assertNull(hdrBefore.get(23));

    // New record via headers method
    GenericRecord newHdrResult = (GenericRecord) deserializer.deserialize(topic, headers, bytesNewRecord);
    GenericRecord newHdrAfter = (GenericRecord) newHdrResult.get("after");
    assertEquals("[{\"note\": \"test\"}]", newHdrAfter.get("notes").toString());
    assertEquals("[\"default\"]", newHdrAfter.get("search_engine_id").toString());
    assertEquals(1, newHdrAfter.get("locale_id"));
    assertEquals(2, newHdrAfter.get("language_id"));
  }

}
