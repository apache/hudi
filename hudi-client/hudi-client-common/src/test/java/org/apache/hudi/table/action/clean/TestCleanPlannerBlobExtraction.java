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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for blob path extraction in CleanPlanner.
 * Tests the getManagedBlobPaths method which recursively traverses schema structures
 * (RECORD, ARRAY, MAP) to extract managed external blob file paths.
 */
public class TestCleanPlannerBlobExtraction {

  private CleanPlanner cleanPlanner;
  private AvroRecordContext recordContext;

  @BeforeEach
  public void setUp() {
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(false);
    HoodieEngineContext context = new HoodieLocalEngineContext(storageConf);

    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    SyncableFileSystemView mockSliceView = mock(SyncableFileSystemView.class);

    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.getBasePath()).thenReturn(new StoragePath("/tmp/test"));
    when(mockTable.getSliceView()).thenReturn(mockSliceView);
    when(mockSliceView.getPendingCompactionOperations()).thenReturn(Stream.empty());
    when(mockSliceView.getPendingLogCompactionOperations()).thenReturn(Stream.empty());

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    cleanPlanner = new CleanPlanner(context, mockTable, config);
    recordContext = AvroRecordContext.getFieldAccessorInstance();
  }

  // ========================================
  // Schema Creation Helpers (using Avro directly)
  // ========================================

  private HoodieSchema createSimpleBlobRecordSchema() {
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("blob_field", HoodieSchema.createBlob(), null, null));
    return HoodieSchema.createRecord("TestRecord", null, null, fields);
  }

  // ========================================
  // Blob Record Creation Helpers
  // ========================================

  private GenericRecord createBlobReference(String path, boolean managed) {
    Schema refSchema = HoodieSchema.createBlob().getField("reference").get().schema().getNonNullType().toAvroSchema();
    GenericRecord reference = new GenericData.Record(refSchema);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, path);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, managed);
    return reference;
  }

  private GenericRecord createInlineBlob(byte[] data) {
    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "INLINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(data));
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);
    return blob;
  }

  private GenericRecord createManagedExternalBlob(String path) {
    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "OUT_OF_LINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, null);
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, createBlobReference(path, true));
    return blob;
  }

  private GenericRecord createUnmanagedExternalBlob(String path) {
    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "OUT_OF_LINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, null);
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, createBlobReference(path, false));
    return blob;
  }

  // ========================================
  // Assertion Helpers
  // ========================================

  private void assertPathsEqual(List<String> expected, List<String> actual) {
    List<String> sortedExpected = new ArrayList<>(expected);
    List<String> sortedActual = new ArrayList<>(actual);
    Collections.sort(sortedExpected);
    Collections.sort(sortedActual);
    assertEquals(sortedExpected, sortedActual);
  }

  // ========================================
  // Test 1: Basic Blob Types (4 tests)
  // ========================================

  @Test
  public void testInlineBlob_ReturnsEmpty() {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", createInlineBlob("test data".getBytes()));

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Inline blobs should not return paths");
  }

  @Test
  public void testManagedExternalBlob_ReturnsPath() {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", createManagedExternalBlob("/hoodie/blobs/managed.bin"));

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Collections.singletonList("/hoodie/blobs/managed.bin"), paths);
  }

  @Test
  public void testUnmanagedExternalBlob_ReturnsEmpty() {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", createUnmanagedExternalBlob("/external/unmanaged.bin"));

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Unmanaged external blobs should not return paths");
  }

  @Test
  public void testNullBlobValue_ReturnsEmpty() {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", null);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Null blob values should be handled gracefully");
  }

  // ========================================
  // Test 2: Multiple Top-Level Blobs (2 tests)
  // ========================================

  @Test
  public void testMultipleBlobFields_MixedTypes() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null,null));
    fields.add(HoodieSchemaField.of("blob1", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob2", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob3", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob1", createInlineBlob("data1".getBytes()));
    record.put("blob2", createManagedExternalBlob("/path/blob2.bin"));
    record.put("blob3", createUnmanagedExternalBlob("/external/blob3.bin"));

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/path/blob2.bin"), paths);
  }

  @Test
  public void testMultipleBlobFields_WithNull() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null,null));
    fields.add(HoodieSchemaField.of("blob1", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob2", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob3", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob1", createManagedExternalBlob("/path/blob1.bin"));
    record.put("blob2", null);
    record.put("blob3", createManagedExternalBlob("/path/blob3.bin"));

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/path/blob1.bin", "/path/blob3.bin"), paths);
  }

  // ========================================
  // Test 3: Nested Records (3 tests)
  // ========================================

  @Test
  public void testNestedRecord_SingleLevel() {
    // Create inner record schema with blob
    List<HoodieSchemaField> innerFields = new ArrayList<>();
    innerFields.add(HoodieSchemaField.of("data", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    innerFields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema innerSchema = HoodieSchema.createRecord("InnerRecord", null, null, false, innerFields);

    // Create outer record schema
    List<HoodieSchemaField> outerFields = new ArrayList<>();
    outerFields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    outerFields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(innerSchema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("OuterRecord", null, null, false, outerFields);

    // Create records
    GenericRecord innerRecord = new GenericData.Record(innerSchema.toAvroSchema());
    innerRecord.put("data", "nested data");
    innerRecord.put("blob_field", createManagedExternalBlob("/nested/level1.bin"));

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("nested", innerRecord);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Collections.singletonList("/nested/level1.bin"), paths);
  }

  @Test
  public void testNestedRecord_TwoLevels() {
    // Level 0: innermost record with blob
    List<HoodieSchemaField> level0Fields = new ArrayList<>();
    level0Fields.add(HoodieSchemaField.of("data", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level0Fields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema level0Schema = HoodieSchema.createRecord("Level0Record", null, null, false, level0Fields);

    // Level 1: middle record
    List<HoodieSchemaField> level1Fields = new ArrayList<>();
    level1Fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level1Fields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(level0Schema), null, null));
    HoodieSchema level1Schema = HoodieSchema.createRecord("Level1Record", null, null, false, level1Fields);

    // Level 2: outer record
    List<HoodieSchemaField> level2Fields = new ArrayList<>();
    level2Fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level2Fields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(level1Schema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("Level2Record", null, null, false, level2Fields);

    // Create nested records
    GenericRecord level0Record = new GenericData.Record(level0Schema.toAvroSchema());
    level0Record.put("data", "deep data");
    level0Record.put("blob_field", createManagedExternalBlob("/nested/level2.bin"));

    GenericRecord level1Record = new GenericData.Record(level1Schema.toAvroSchema());
    level1Record.put("id", "level1");
    level1Record.put("nested", level0Record);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "level2");
    record.put("nested", level1Record);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Collections.singletonList("/nested/level2.bin"), paths);
  }

  @Test
  public void testNestedRecord_NullRecord() {
    // Create inner record schema with blob
    List<HoodieSchemaField> innerFields = new ArrayList<>();
    innerFields.add(HoodieSchemaField.of("data", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    innerFields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema innerSchema = HoodieSchema.createRecord("InnerRecord", null, null, false, innerFields);

    // Create outer record schema
    List<HoodieSchemaField> outerFields = new ArrayList<>();
    outerFields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    outerFields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(innerSchema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("OuterRecord", null, null, false, outerFields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("nested", null);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Null nested records should be handled gracefully");
  }

  // ========================================
  // Test 4: Arrays (4 tests)
  // ========================================

  @Test
  public void testArrayOfBlobs_AllManaged() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blobs", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");

    List<GenericRecord> blobs = Arrays.asList(
        createManagedExternalBlob("/array/blob1.bin"),
        createManagedExternalBlob("/array/blob2.bin"),
        createManagedExternalBlob("/array/blob3.bin")
    );
    record.put("blobs", blobs);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/array/blob1.bin", "/array/blob2.bin", "/array/blob3.bin"), paths);
  }

  @Test
  public void testArrayOfBlobs_Mixed() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blobs", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");

    List<GenericRecord> blobs = Arrays.asList(
        createInlineBlob("data1".getBytes()),
        createManagedExternalBlob("/array/managed.bin"),
        createUnmanagedExternalBlob("/external/unmanaged.bin")
    );
    record.put("blobs", blobs);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/array/managed.bin"), paths);
  }

  @Test
  public void testArrayOfBlobs_EmptyArray() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blobs", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blobs", Collections.emptyList());

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Empty array should return no paths");
  }

  @Test
  public void testArrayOfRecords_WithBlobs() {
    // Create record schema with blob
    List<HoodieSchemaField> itemFields = new ArrayList<>();
    itemFields.add(HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    itemFields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema itemSchema = HoodieSchema.createRecord("ItemRecord", null, null, false, itemFields);

    // Create schema with array of records
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("items", HoodieSchema.createArray(itemSchema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    // Create records
    List<GenericRecord> items = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      GenericRecord item = new GenericData.Record(itemSchema.toAvroSchema());
      item.put("name", "item" + i);
      item.put("blob_field", createManagedExternalBlob("/array/record/blob" + i + ".bin"));
      items.add(item);
    }

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("items", items);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList(
        "/array/record/blob1.bin",
        "/array/record/blob2.bin",
        "/array/record/blob3.bin"
    ), paths);
  }

  // ========================================
  // Test 5: Maps (3 tests)
  // ========================================

  @Test
  public void testMapOfBlobs_AllManaged() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");

    Map<String, GenericRecord> blobMap = new HashMap<>();
    blobMap.put("key1", createManagedExternalBlob("/map/blob1.bin"));
    blobMap.put("key2", createManagedExternalBlob("/map/blob2.bin"));
    blobMap.put("key3", createManagedExternalBlob("/map/blob3.bin"));
    record.put("blob_map", blobMap);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/map/blob1.bin", "/map/blob2.bin", "/map/blob3.bin"), paths);
  }

  @Test
  public void testMapOfBlobs_Mixed() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");

    Map<String, GenericRecord> blobMap = new HashMap<>();
    blobMap.put("key1", createInlineBlob("data1".getBytes()));
    blobMap.put("key2", createManagedExternalBlob("/map/managed.bin"));
    blobMap.put("key3", createUnmanagedExternalBlob("/external/unmanaged.bin"));
    record.put("blob_map", blobMap);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList("/map/managed.bin"), paths);
  }

  @Test
  public void testMapOfBlobs_EmptyMap() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_map", Collections.emptyMap());

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertTrue(paths.isEmpty(), "Empty map should return no paths");
  }

  // ========================================
  // Test 6: Complex Nested Structures (2 tests)
  // ========================================

  @Test
  public void testComplexNesting_AllPathsExtracted() {
    // Create nested record with blob
    List<HoodieSchemaField> nestedFields = new ArrayList<>();
    nestedFields.add(HoodieSchemaField.of("data", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    nestedFields.add(HoodieSchemaField.of("nested_blob", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema nestedSchema = HoodieSchema.createRecord("NestedRecord", null, null, false, nestedFields);

    // Create main schema with top-level blob, nested record, array, and map
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("top_level_blob", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("nested_record", HoodieSchema.createNullable(nestedSchema), null, null));
    fields.add(HoodieSchemaField.of("blob_array", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("ComplexRecord", null, null, false, fields);

    // Create nested record
    GenericRecord nestedRecord = new GenericData.Record(nestedSchema.toAvroSchema());
    nestedRecord.put("data", "nested data");
    nestedRecord.put("nested_blob", createManagedExternalBlob("/complex/nested.bin"));

    // Create main record
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("top_level_blob", createManagedExternalBlob("/complex/top_level.bin"));
    record.put("nested_record", nestedRecord);

    List<GenericRecord> blobArray = Arrays.asList(
        createManagedExternalBlob("/complex/array1.bin"),
        createManagedExternalBlob("/complex/array2.bin")
    );
    record.put("blob_array", blobArray);

    Map<String, GenericRecord> blobMap = new HashMap<>();
    blobMap.put("key1", createManagedExternalBlob("/complex/map1.bin"));
    blobMap.put("key2", createManagedExternalBlob("/complex/map2.bin"));
    record.put("blob_map", blobMap);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList(
        "/complex/top_level.bin",
        "/complex/nested.bin",
        "/complex/array1.bin",
        "/complex/array2.bin",
        "/complex/map1.bin",
        "/complex/map2.bin"
    ), paths);
  }

  @Test
  public void testArray_ContainsMap_ContainsBlob() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null,  null));
    fields.add(HoodieSchemaField.of("array_of_maps", HoodieSchema.createArray(HoodieSchema.createMap(HoodieSchema.createBlob())), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    List<Map<String, GenericRecord>> arrayOfMaps = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      Map<String, GenericRecord> map = new HashMap<>();
      map.put("blob" + i, createManagedExternalBlob("/complex/array/map/blob" + i + ".bin"));
      arrayOfMaps.add(map);
    }

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("array_of_maps", arrayOfMaps);

    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);

    assertPathsEqual(Arrays.asList(
        "/complex/array/map/blob1.bin",
        "/complex/array/map/blob2.bin"
    ), paths);
  }

  // ========================================
  // Test 7: Edge Cases (3 tests)
  // ========================================

  @Test
  public void testEmptyRecord_ReturnsEmpty() {
    Schema schema = Schema.createRecord("EmptyRecord", null, null, false, Collections.emptyList());
    GenericRecord record = new GenericData.Record(schema);

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(schema);
    List<String> paths = cleanPlanner.getManagedBlobPaths(hoodieSchema, record, recordContext);

    assertTrue(paths.isEmpty(), "Empty record should return no paths");
  }

  @Test
  public void testLargeNestedStructure_Performance() {
    // Create item schema with blob
    List<HoodieSchemaField> itemFields = new ArrayList<>();
    itemFields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    itemFields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema itemSchema = HoodieSchema.createRecord("ItemRecord", null, null, false, itemFields);

    // Create schema with array of records
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, (Object) null));
    fields.add(HoodieSchemaField.of("items", HoodieSchema.createArray(itemSchema), null, (Object) null));
    HoodieSchema schema = HoodieSchema.createRecord("LargeRecord", null, null, false, fields);

    // Create 100 items with blobs
    List<GenericRecord> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord item = new GenericData.Record(itemSchema.toAvroSchema());
      item.put("id", "item" + i);
      item.put("blob_field", createManagedExternalBlob("/large/blob" + i + ".bin"));
      items.add(item);
    }

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("items", items);

    long startTime = System.currentTimeMillis();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    long duration = System.currentTimeMillis() - startTime;

    assertEquals(100, paths.size(), "Should extract all 100 blob paths");
    assertTrue(duration < 1000, "Extraction should complete in less than 1 second, took: " + duration + "ms");
  }
}
