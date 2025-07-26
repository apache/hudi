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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
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

class TestPartialMergingUtils {
  private PartialMergingUtils<IndexedRecord> partialMergingUtils;
  private HoodieReaderContext<IndexedRecord> mockReaderContext;
  private Schema fullSchema;
  private Schema partialSchema;
  private Schema readerSchema;

  @BeforeEach
  void setUp() {
    partialMergingUtils = new PartialMergingUtils<>();
    mockReaderContext = mock(HoodieReaderContext.class);
    
    // Create test schemas
    fullSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    fullSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null)
    ));

    partialSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    partialSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));

    readerSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    readerSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null)
    ));
  }

  @Test
  void testGetCachedFieldIdToNameMapping() {
    Map<Integer, String> fieldIdToNameMapping = PartialMergingUtils.getCachedFieldIdToNameMapping(fullSchema);
    
    assertNotNull(fieldIdToNameMapping);
    assertEquals(4, fieldIdToNameMapping.size());
    assertEquals("id", fieldIdToNameMapping.get(0));
    assertEquals("name", fieldIdToNameMapping.get(1));
    assertEquals("age", fieldIdToNameMapping.get(2));
    assertEquals("city", fieldIdToNameMapping.get(3));
    
    // Test caching - should return the same map for the same schema
    Map<Integer, String> cachedMapping = PartialMergingUtils.getCachedFieldIdToNameMapping(fullSchema);
    assertSame(fieldIdToNameMapping, cachedMapping);
  }

  @Test
  void testGetCachedFieldNameToIdMapping() {
    Map<String, Integer> fieldNameToIdMapping = PartialMergingUtils.getCachedFieldNameToIdMapping(fullSchema);
    
    assertNotNull(fieldNameToIdMapping);
    assertEquals(4, fieldNameToIdMapping.size());
    assertEquals(0, fieldNameToIdMapping.get("id"));
    assertEquals(1, fieldNameToIdMapping.get("name"));
    assertEquals(2, fieldNameToIdMapping.get("age"));
    assertEquals(3, fieldNameToIdMapping.get("city"));
    
    // Test caching - should return the same map for the same schema
    Map<String, Integer> cachedMapping = PartialMergingUtils.getCachedFieldNameToIdMapping(fullSchema);
    assertSame(fieldNameToIdMapping, cachedMapping);
  }

  @Test
  void testGetCachedMergedSchema() {
    Schema mergedSchema = PartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);

    assertNotNull(mergedSchema);
    assertEquals(4, mergedSchema.getFields().size());
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
    assertEquals("city", mergedSchema.getFields().get(3).name());
    
    // Test caching - should return the same schema for the same input
    Schema cachedSchema = PartialMergingUtils.getCachedMergedSchema(partialSchema, fullSchema, readerSchema);
    assertSame(mergedSchema, cachedSchema);
  }

  @Test
  void testGetCachedMergedSchemaWithDifferentFieldOrder() {
    // Create a schema with different field order
    Schema reorderedSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    reorderedSchema.setFields(Arrays.asList(
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null),
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));

    Schema mergedSchema = PartialMergingUtils.getCachedMergedSchema(partialSchema, reorderedSchema, readerSchema);
    
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
    assertTrue(PartialMergingUtils.isPartial(partialSchema, fullSchema));
    
    // Test when schema is not partial (same as merged schema)
    assertFalse(PartialMergingUtils.isPartial(fullSchema, fullSchema));
    
    // Test when schema has more fields than merged schema
    Schema extendedSchema = Schema.createRecord("TestRecord", "Test record", "test", false);
    extendedSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City field", null),
        new Schema.Field("extra", Schema.create(Schema.Type.STRING), "Extra field", null)
    ));
    assertTrue(PartialMergingUtils.isPartial(extendedSchema, fullSchema));
  }

  @Test
  void testMergePartialRecordsWithNonPartialNewer() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(fullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");
    
    GenericRecord newerRecord = new GenericData.Record(fullSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");
    newerRecord.put("age", 30);
    newerRecord.put("city", "Boston");
    
    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, false);
    
    // When newer schema is not partial, should return newer record as-is
    Pair<BufferedRecord<IndexedRecord>, Schema> result = partialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, fullSchema, readerSchema, mockReaderContext);
    
    assertEquals(newer, result.getLeft());
    assertEquals(fullSchema, result.getRight());
  }

  @Test
  void testMergePartialRecordsWithPartialNewer() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(fullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");
    
    GenericRecord newerRecord = new GenericData.Record(partialSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");
    
    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, false);
    
    // Mock reader context behavior
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("id"))).thenReturn("1");
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("name"))).thenReturn("John");
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("age"))).thenReturn(25);
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("city"))).thenReturn("New York");
    
    when(mockReaderContext.getValue(eq(newerRecord), eq(partialSchema), eq("id"))).thenReturn("1");
    when(mockReaderContext.getValue(eq(newerRecord), eq(partialSchema), eq("name"))).thenReturn("Jane");
    
    GenericRecord mergedRecord = new GenericData.Record(readerSchema);
    mergedRecord.put("id", "1");
    mergedRecord.put("name", "Jane");
    mergedRecord.put("age", 25);
    mergedRecord.put("city", "New York");
    
    when(mockReaderContext.constructEngineRecord(eq(readerSchema), any(List.class))).thenReturn(mergedRecord);
    when(mockReaderContext.encodeAvroSchema(any(Schema.class))).thenReturn(1);
    
    // When newer schema is partial, should merge records
    Pair<BufferedRecord<IndexedRecord>, Schema> result = partialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockReaderContext);
    
    assertNotNull(result.getLeft());
    assertEquals("1", result.getLeft().getRecordKey());
    assertEquals(2L, result.getLeft().getOrderingValue());
    assertFalse(result.getLeft().isDelete());
    assertEquals(readerSchema, result.getRight());
  }

  @Test
  void testMergePartialRecordsWithDeleteRecord() {
    // Create test records
    GenericRecord olderRecord = new GenericData.Record(fullSchema);
    olderRecord.put("id", "1");
    olderRecord.put("name", "John");
    olderRecord.put("age", 25);
    olderRecord.put("city", "New York");
    
    GenericRecord newerRecord = new GenericData.Record(partialSchema);
    newerRecord.put("id", "1");
    newerRecord.put("name", "Jane");
    
    BufferedRecord<IndexedRecord> older = new BufferedRecord<>(
        "1", 1L, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>(
        "1", 2L, newerRecord, 1, true); // Delete record
    
    // Mock reader context behavior
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("id"))).thenReturn("1");
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("name"))).thenReturn("John");
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("age"))).thenReturn(25);
    when(mockReaderContext.getValue(eq(olderRecord), eq(fullSchema), eq("city"))).thenReturn("New York");
    
    when(mockReaderContext.getValue(eq(newerRecord), eq(partialSchema), eq("id"))).thenReturn("1");
    when(mockReaderContext.getValue(eq(newerRecord), eq(partialSchema), eq("name"))).thenReturn("Jane");
    
    GenericRecord mergedRecord = new GenericData.Record(readerSchema);
    mergedRecord.put("id", "1");
    mergedRecord.put("name", "Jane");
    mergedRecord.put("age", 25);
    mergedRecord.put("city", "New York");
    
    when(mockReaderContext.constructEngineRecord(eq(readerSchema), any(List.class))).thenReturn(mergedRecord);
    when(mockReaderContext.encodeAvroSchema(any(Schema.class))).thenReturn(1);
    
    // Test with delete record
    Pair<BufferedRecord<IndexedRecord>, Schema> result = partialMergingUtils.mergePartialRecords(
        older, fullSchema, newer, partialSchema, readerSchema, mockReaderContext);
    
    assertNotNull(result.getLeft());
    assertTrue(result.getLeft().isDelete());
  }

  @Test
  void testMergePartialRecordsWithEmptySchemas() {
    Schema emptySchema = Schema.createRecord("EmptyRecord", "Empty record", "test", false);
    emptySchema.setFields(Arrays.asList());
    
    GenericRecord olderRecord = new GenericData.Record(emptySchema);
    GenericRecord newerRecord = new GenericData.Record(emptySchema);
    
    BufferedRecord<IndexedRecord> older = new BufferedRecord<>("1", 1L, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newer = new BufferedRecord<>("1", 2L, newerRecord, 1, false);
    
    when(mockReaderContext.constructEngineRecord(eq(emptySchema), any(List.class))).thenReturn(newerRecord);
    when(mockReaderContext.encodeAvroSchema(any(Schema.class))).thenReturn(1);
    
    Pair<BufferedRecord<IndexedRecord>, Schema> result = partialMergingUtils.mergePartialRecords(
        older, emptySchema, newer, emptySchema, emptySchema, mockReaderContext);
    
    assertNotNull(result.getLeft());
    assertEquals(emptySchema, result.getRight());
  }

  @Test
  void testGetCachedMergedSchemaWithOverlappingFields() {
    // Create schemas with overlapping fields
    Schema schema1 = Schema.createRecord("TestRecord", "Test record", "test", false);
    schema1.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "ID field", null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null)
    ));
    
    Schema schema2 = Schema.createRecord("TestRecord", "Test record", "test", false);
    schema2.setFields(Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name field", null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age field", null)
    ));
    
    Schema mergedSchema = PartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);
    
    assertNotNull(mergedSchema);
    assertEquals(3, mergedSchema.getFields().size()); // id, name, age (city not in either schema)
    assertEquals("id", mergedSchema.getFields().get(0).name());
    assertEquals("name", mergedSchema.getFields().get(1).name());
    assertEquals("age", mergedSchema.getFields().get(2).name());
  }

  @Test
  void testGetCachedMergedSchemaWithNoCommonFields() {
    // Create schemas with no common fields
    Schema schema1 = Schema.createRecord("TestRecord", "Test record", "test", false);
    schema1.setFields(Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), "Field 1", null),
        new Schema.Field("field2", Schema.create(Schema.Type.INT), "Field 2", null)
    ));
    
    Schema schema2 = Schema.createRecord("TestRecord", "Test record", "test", false);
    schema2.setFields(Arrays.asList(
        new Schema.Field("field3", Schema.create(Schema.Type.BOOLEAN), "Field 3", null),
        new Schema.Field("field4", Schema.create(Schema.Type.LONG), "Field 4", null)
    ));
    
    Schema mergedSchema = PartialMergingUtils.getCachedMergedSchema(schema1, schema2, readerSchema);
    assertNotNull(mergedSchema);
    assertEquals(0, mergedSchema.getFields().size()); // No fields from reader schema are in either schema
  }
}
