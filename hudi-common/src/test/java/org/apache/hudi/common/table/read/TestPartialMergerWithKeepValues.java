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
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.ArgumentCaptor;

class TestPartialMergerWithKeepValues {
  private PartialMergerWithKeepValues<IndexedRecord> keepValuesPartialMergingUtils;
  private RecordContext<IndexedRecord> mockRecordContext;
  private HoodieSchema fullSchema;
  private HoodieSchema partialSchema;
  private HoodieSchema readerSchema;
  private Schema avroFullSchema;
  private Schema avroPartialSchema;
  private Schema avroReaderSchema;

  @BeforeEach
  void setUp() {
    keepValuesPartialMergingUtils = new PartialMergerWithKeepValues<>();
    mockRecordContext = mock(RecordContext.class);

    // Create HoodieSchema directly
    fullSchema = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null),
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), "Age field", null),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING), "City field", null)
    ));

    partialSchema = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null)
    ));

    readerSchema = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null),
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), "Age field", null),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING), "City field", null)
    ));

    // Get Avro schemas for creating GenericRecord instances
    avroFullSchema = fullSchema.toAvroSchema();
    avroPartialSchema = partialSchema.toAvroSchema();
    avroReaderSchema = readerSchema.toAvroSchema();
  }

  @Test
  void testGetCachedFieldNames() {
    Set<String> fieldNames = keepValuesPartialMergingUtils.getCachedFieldNames(fullSchema);

    assertNotNull(fieldNames);
    assertEquals(4, fieldNames.size());
    assertTrue(fieldNames.contains("id"));
    assertTrue(fieldNames.contains("name"));
    assertTrue(fieldNames.contains("age"));
    assertTrue(fieldNames.contains("city"));

    // Test caching - should return the same set for the same schema
    Set<String> cachedFieldNames = keepValuesPartialMergingUtils.getCachedFieldNames(fullSchema);
    assertSame(fieldNames, cachedFieldNames);
  }

  @Test
  void testGetCachedMergedSchema() {
    HoodieSchema mergedSchema = keepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);

    assertNotNull(mergedSchema);
    assertEquals(4, mergedSchema.getFields().size());
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
    assertEquals("city", mergedSchema.getFields().get(3).name());

    // Test caching - should return the same schema for the same input
    HoodieSchema cachedSchema = keepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);
    assertSame(mergedSchema, cachedSchema);
  }

  @Test
  void testGetCachedMergedSchemaWithDifferentFieldOrder() {
    // Create a schema with different field order
    HoodieSchema reorderedSchema = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), "Age field", null),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING), "City field", null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null)
    ));

    HoodieSchema mergedSchema = keepValuesPartialMergingUtils.getCachedMergedSchema(partialSchema, reorderedSchema, readerSchema);

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
    assertTrue(PartialMergerWithKeepValues.isPartial(partialSchema, fullSchema));

    // Test when schema is not partial (same as merged schema)
    assertFalse(PartialMergerWithKeepValues.isPartial(fullSchema, fullSchema));

    // Test when schema has more fields than merged schema
    HoodieSchema extendedSchema = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null),
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), "Age field", null),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING), "City field", null),
        HoodieSchemaField.of("extra", HoodieSchema.create(HoodieSchemaType.STRING), "Extra field", null)
    ));
    assertTrue(PartialMergerWithKeepValues.isPartial(extendedSchema, fullSchema));
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

    // Capture arguments to validate correct values are passed
    ArgumentCaptor<HoodieSchema> schemaCaptor = ArgumentCaptor.forClass(HoodieSchema.class);
    ArgumentCaptor<Object[]> valuesCaptor = ArgumentCaptor.forClass(Object[].class);
    ArgumentCaptor<HoodieSchema> encodeSchemaCaptor = ArgumentCaptor.forClass(HoodieSchema.class);

    when(mockRecordContext.constructEngineRecord(schemaCaptor.capture(), valuesCaptor.capture())).thenReturn(mergedRecord);
    when(mockRecordContext.encodeSchema(encodeSchemaCaptor.capture())).thenReturn(1);

    // When newer schema is partial, should merge records
    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockRecordContext);

    // Validate the merged schema passed to constructEngineRecord
    HoodieSchema capturedSchema = schemaCaptor.getValue();
    assertNotNull(capturedSchema);
    assertEquals(4, capturedSchema.getFields().size());
    assertEquals("id", capturedSchema.getFields().get(0).name());
    assertEquals("name", capturedSchema.getFields().get(1).name());
    assertEquals("age", capturedSchema.getFields().get(2).name());
    assertEquals("city", capturedSchema.getFields().get(3).name());

    // Validate the field values array passed to constructEngineRecord
    // Expected: ["1" (from newer), "Jane" (from newer), 25 (from older), "New York" (from older)]
    Object[] capturedValues = valuesCaptor.getValue();
    assertNotNull(capturedValues);
    assertEquals(4, capturedValues.length);
    assertEquals("1", capturedValues[0]);
    assertEquals("Jane", capturedValues[1]);
    assertEquals(25, capturedValues[2]);
    assertEquals("New York", capturedValues[3]);

    // Validate the schema passed to encodeSchema
    HoodieSchema capturedEncodeSchema = encodeSchemaCaptor.getValue();
    assertNotNull(capturedEncodeSchema);
    assertEquals(capturedSchema, capturedEncodeSchema);

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

    // Capture arguments to validate correct values are passed
    ArgumentCaptor<HoodieSchema> schemaCaptor = ArgumentCaptor.forClass(HoodieSchema.class);
    ArgumentCaptor<Object[]> valuesCaptor = ArgumentCaptor.forClass(Object[].class);
    ArgumentCaptor<HoodieSchema> encodeSchemaCaptor = ArgumentCaptor.forClass(HoodieSchema.class);

    when(mockRecordContext.constructEngineRecord(schemaCaptor.capture(), valuesCaptor.capture())).thenReturn(mergedRecord);
    when(mockRecordContext.encodeSchema(encodeSchemaCaptor.capture())).thenReturn(1);

    // Test with delete record
    Pair<BufferedRecord<IndexedRecord>, HoodieSchema> result = keepValuesPartialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockRecordContext);

    // Validate the merged schema passed to constructEngineRecord
    HoodieSchema capturedSchema = schemaCaptor.getValue();
    assertNotNull(capturedSchema);
    assertEquals(4, capturedSchema.getFields().size());
    assertEquals("id", capturedSchema.getFields().get(0).name());
    assertEquals("name", capturedSchema.getFields().get(1).name());
    assertEquals("age", capturedSchema.getFields().get(2).name());
    assertEquals("city", capturedSchema.getFields().get(3).name());

    // Validate the field values array passed to constructEngineRecord
    // Expected: ["1" (from newer), "Jane" (from newer), 25 (from older), "New York" (from older)]
    Object[] capturedValues = valuesCaptor.getValue();
    assertNotNull(capturedValues);
    assertEquals(4, capturedValues.length);
    assertEquals("1", capturedValues[0]);
    assertEquals("Jane", capturedValues[1]);
    assertEquals(25, capturedValues[2]);
    assertEquals("New York", capturedValues[3]);

    // Validate the schema passed to encodeSchema
    HoodieSchema capturedEncodeSchema = encodeSchemaCaptor.getValue();
    assertNotNull(capturedEncodeSchema);
    assertEquals(capturedSchema, capturedEncodeSchema);

    assertNotNull(result.getLeft());
    assertTrue(result.getLeft().isDelete());
  }

  @Test
  void testMergePartialRecordsWithEmptySchemas() {
    HoodieSchema emptySchema = HoodieSchema.createRecord("EmptyRecord", "Empty record", "test", false, Collections.emptyList());
    Schema avroEmptySchema = emptySchema.toAvroSchema();

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
    HoodieSchema schema1 = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "ID field", null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null)
    ));

    HoodieSchema schema2 = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "Name field", null),
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), "Age field", null)
    ));

    HoodieSchema mergedSchema = keepValuesPartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);

    assertNotNull(mergedSchema);
    assertEquals(3, mergedSchema.getFields().size()); // id, name, age (city not in either schema)
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
  }

  @Test
  void testGetCachedMergedSchemaWithNoCommonFields() {
    // Create schemas with no common fields
    HoodieSchema schema1 = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING), "Field 1", null),
        HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.INT), "Field 2", null)
    ));

    HoodieSchema schema2 = HoodieSchema.createRecord("TestRecord", "Test record", "test", false, Arrays.asList(
        HoodieSchemaField.of("field3", HoodieSchema.create(HoodieSchemaType.BOOLEAN), "Field 3", null),
        HoodieSchemaField.of("field4", HoodieSchema.create(HoodieSchemaType.LONG), "Field 4", null)
    ));

    HoodieSchema mergedSchema = keepValuesPartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);
    assertNotNull(mergedSchema);
    assertEquals(0, mergedSchema.getFields().size()); // No fields from reader schema are in either schema
  }
}