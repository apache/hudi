/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestKeepValuesPartialMergingUtils {
  private KeepValuesPartialMergingUtils<IndexedRecord> keepValuesPartialMergingUtils;
  private RecordContext<IndexedRecord> mockRecordContext;
  private HoodieSchema fullSchema;
  private HoodieSchema partialSchema;
  private HoodieSchema readerSchema;
  private Schema avroFullSchema;
  private Schema avroPartialSchema;
  private Schema avroReaderSchema;

  @BeforeEach
  void setUp() {
    keepValuesPartialMergingUtils = new KeepValuesPartialMergingUtils<>();
    mockRecordContext = mock(RecordContext.class);

    // Create Avro schemas first
    avroFullSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroFullSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null)
    ));

    avroPartialSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroPartialSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));

    avroReaderSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroReaderSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null)
    ));

    // Wrap with HoodieSchema
    fullSchema = HoodieSchema.fromAvroSchema(avroFullSchema);
    partialSchema = HoodieSchema.fromAvroSchema(avroPartialSchema);
    readerSchema = HoodieSchema.fromAvroSchema(avroReaderSchema);
  }

  @Test
  void testGetCachedFieldNameToIdMapping() {
    Map<String, Integer> fieldNameToIdMapping = KeepValuesPartialMergingUtils.getCachedFieldNameToIdMapping(fullSchema);

    assertNotNull(fieldNameToIdMapping);
    assertEquals(4, fieldNameToIdMapping.size());
    assertEquals(0, fieldNameToIdMapping.get("id"));
    assertEquals(1, fieldNameToIdMapping.get("name"));
    assertEquals(2, fieldNameToIdMapping.get("age"));
    assertEquals(3, fieldNameToIdMapping.get("city"));

    // Test caching - should return the same map for the same schema
    Map<String, Integer> cachedMapping = KeepValuesPartialMergingUtils.getCachedFieldNameToIdMapping(fullSchema);
    assertSame(fieldNameToIdMapping, cachedMapping);
  }

  @Test
  void testGetCachedMergedSchema() {
    HoodieSchema mergedSchema = KeepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);

    assertNotNull(mergedSchema);
    assertEquals(4, mergedSchema.getFields().size());
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
    assertEquals("city", mergedSchema.getFields().get(3).name());

    // Test caching - should return the same schema for the same input
    HoodieSchema cachedSchema = KeepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);
    assertSame(mergedSchema, cachedSchema);
  }

  @Test
  void testGetCachedMergedSchemaWithDifferentFieldOrder() {
    // Create a schema with different field order
    Schema avroReorderedSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroReorderedSchema.setFields(Arrays.asList(
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null),
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));
    HoodieSchema reorderedSchema = HoodieSchema.fromAvroSchema(avroReorderedSchema);

    HoodieSchema mergedSchema = KeepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, reorderedSchema, readerSchema);

    // Should follow the reader schema order
    assertEquals(4, mergedSchema.getFields().size());
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
    assertEquals("city", mergedSchema.getFields().get(3).name());
  }

  @Test
  void testIsPartial() {
    // Test when schema is partial compared to merged schema
    assertTrue(KeepValuesPartialMergingUtils.isPartial(partialSchema, fullSchema));

    // Test when schema is not partial (same as merged schema)
    assertFalse(KeepValuesPartialMergingUtils.isPartial(fullSchema, fullSchema));

    // Test when schema has more fields than merged schema
    Schema avroExtendedSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroExtendedSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null),
        new Schema.Field("extra", Schema.create(Schema.Type.STRING), "Extra field", null)
    ));
    HoodieSchema extendedSchema = HoodieSchema.fromAvroSchema(avroExtendedSchema);
    assertTrue(KeepValuesPartialMergingUtils.isPartial(extendedSchema, fullSchema));
  }

  @Test
  void testMergePartialRecordsWithNonPartialNewer() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(avroFullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");

    GenericRecord newerRecord = new GenericData.Record(avroFullSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");
    newerRecord.put("age", 30);
    newerRecord.put("city", "Boston");

    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, null);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, null);

    // When newer schema is not partial, should return newer record as-is
    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, fullSchema, readerSchema, mockRecordContext);

    assertEquals(newer, result.getLeft());
    assertEquals(fullSchema, result.getRight());
  }

  @Test
  void testMergePartialRecordsWithPartialNewer() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(avroFullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");

    GenericRecord newerRecord = new GenericData.Record(avroPartialSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");

    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, null);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, null);

    // Mock record context behavior
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("id"))).thenReturn("1");
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("name"))).thenReturn("John");
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("age"))).thenReturn(25);
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("city"))).thenReturn("New York");

    when(mockRecordContext.getValue(eq(newerRecord), eq(partialSchema), eq("id"))).thenReturn("1");
    when(mockRecordContext.getValue(eq(newerRecord), eq(partialSchema), eq("name"))).thenReturn("Jane");

    GenericRecord mergedRecord = new GenericData.Record(avroReaderSchema);
    mergedRecord.put("id", "1");
    mergedRecord.put("name", "Jane");
    mergedRecord.put("age", 25);
    mergedRecord.put("city", "New York");

    when(mockRecordContext.constructEngineRecord(any(HoodieSchema.class), any(Object[].class))).thenReturn(mergedRecord);
    when(mockRecordContext.encodeSchema(any(HoodieSchema.class))).thenReturn(1);

    // When newer schema is partial, should merge records
    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockRecordContext);

    assertNotNull(result.getLeft());
    assertEquals("1", result.getLeft().getRecordKey());
    assertEquals(2L, result.getLeft().getOrderingValue());
    assertFalse(result.getLeft().isDelete());
  }

  @Test
  void testMergePartialRecordsWithDeleteRecord() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(avroFullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");

    GenericRecord newerRecord = new GenericData.Record(avroPartialSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");

    BufferedRecord<IndexedRecord> older = new BufferedRecord<>(
        "1", 1L, olderRecord, 1, null);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>(
        "1", 2L, newerRecord, 1, HoodieOperation.DELETE); // Delete record

    // Mock record context behavior
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("id"))).thenReturn("1");
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("name"))).thenReturn("John");
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("age"))).thenReturn(25);
    when(mockRecordContext.getValue(eq(olderRecord), eq(fullSchema), eq("city"))).thenReturn("New York");

    when(mockRecordContext.getValue(eq(newerRecord), eq(partialSchema), eq("id"))).thenReturn("1");
    when(mockRecordContext.getValue(eq(newerRecord), eq(partialSchema), eq("name"))).thenReturn("Jane");

    GenericRecord mergedRecord = new GenericData.Record(avroReaderSchema);
    mergedRecord.put("id", "1");
    mergedRecord.put("name", "Jane");
    mergedRecord.put("age", 25);
    mergedRecord.put("city", "New York");

    when(mockRecordContext.constructEngineRecord(any(HoodieSchema.class), any(Object[].class))).thenReturn(mergedRecord);
    when(mockRecordContext.encodeSchema(any(HoodieSchema.class))).thenReturn(1);

    // Test with delete record
    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockRecordContext);

    assertNotNull(result.getLeft());
    assertTrue(result.getLeft().isDelete());
  }

  @Test
  void testMergePartialRecordsWithEmptySchemas() {
    Schema avroEmptySchema = Schema.createRecord("EmptyRecord", "Empty record", "test", false);
    avroEmptySchema.setFields(Collections.emptyList());
    HoodieSchema emptySchema = HoodieSchema.fromAvroSchema(avroEmptySchema);

    GenericRecord olderRecord = new GenericData.Record(avroEmptySchema);
    GenericRecord newerRecord = new GenericData.Record(avroEmptySchema);

    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, null);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, null);

    when(mockRecordContext.constructEngineRecord(any(HoodieSchema.class), any(Object[].class))).thenReturn(newerRecord);
    when(mockRecordContext.encodeSchema(any(HoodieSchema.class))).thenReturn(1);

    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, emptySchema, newer, emptySchema, emptySchema, mockRecordContext);

    assertNotNull(result.getLeft());
  }

  @Test
  void testGetCachedMergedSchemaWithOverlappingFields() {
    // Create schemas with overlapping fields
    Schema avroSchema1 = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroSchema1.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));
    HoodieSchema schema1 = HoodieSchema.fromAvroSchema(avroSchema1);

    Schema avroSchema2 = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroSchema2.setFields(Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null)
    ));
    HoodieSchema schema2 = HoodieSchema.fromAvroSchema(avroSchema2);

    HoodieSchema mergedSchema = KeepValuesPartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);

    assertNotNull(mergedSchema);
    assertEquals(3, mergedSchema.getFields().size()); // id, name, age (city not in either schema)
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
  }

  @Test
  void testGetCachedMergedSchemaWithNoCommonFields() {
    // Create schemas with no common fields
    Schema avroSchema1 = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroSchema1.setFields(Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), "Field 1", null),
        new Schema.Field("field2", Schema.create(Schema.Type.INT), "Field 2", null)
    ));
    HoodieSchema schema1 = HoodieSchema.fromAvroSchema(avroSchema1);

    Schema avroSchema2 = Schema.createRecord("TestRecord", "Test record", "test", false);
    avroSchema2.setFields(Arrays.asList(
        new Schema.Field("field3", Schema.create(Schema.Type.BOOLEAN), "Field 3", null),
        new Schema.Field("field4", Schema.create(Schema.Type.LONG), "Field 4", null)
    ));
    HoodieSchema schema2 = HoodieSchema.fromAvroSchema(avroSchema2);

    HoodieSchema mergedSchema = KeepValuesPartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);
    assertNotNull(mergedSchema);
    assertEquals(0, mergedSchema.getFields().size()); // No fields from reader schema are in either schema
  }
}