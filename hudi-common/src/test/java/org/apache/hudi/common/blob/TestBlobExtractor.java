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

package org.apache.hudi.common.blob;

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.blob.BlobExtractor;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestBlobExtractor {

  private static Stream<Arguments> testBasicBlobs() {
    return Stream.of(
        Arguments.of(
            createInlineBlob("test data".getBytes()),
            Collections.emptyList()
        ),
        Arguments.of(
            createExternalBlob("/hoodie/blobs/managed.bin", true),
            Collections.singletonList("/hoodie/blobs/managed.bin")
        ),
        Arguments.of(
            createExternalBlob("/external/unmanaged.bin", false),
            Collections.emptyList()
        ),
        Arguments.of(
            null,
            Collections.emptyList()
        )
    );
  }

  @ParameterizedTest
  @MethodSource
  void testBasicBlobs(GenericRecord inputBlob, List<String> expectedPaths) {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", inputBlob);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  private static Stream<Arguments> testMultipleBlobFields() {
    return Stream.of(
        Arguments.of(
            createInlineBlob("data1".getBytes()),
            createExternalBlob("/path/blob2.bin", true),
            createExternalBlob("/external/blob3.bin", false),
            Collections.singletonList("/path/blob2.bin")
        ),
        Arguments.of(
            createExternalBlob("/path/blob1.bin", true),
            null,
            createExternalBlob("/path/blob3.bin", true),
            Arrays.asList("/path/blob1.bin", "/path/blob3.bin")
        )
    );
  }

  @ParameterizedTest
  @MethodSource
  void testMultipleBlobFields(GenericRecord blob1, GenericRecord blob2, GenericRecord blob3, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null,null));
    fields.add(HoodieSchemaField.of("blob1", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob2", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob3", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob1", blob1);
    record.put("blob2", blob2);
    record.put("blob3", blob3);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  @Test
  void testNestedRecord() {
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
    level0Record.put("blob_field", createExternalBlob("/nested/level2.bin", true));

    GenericRecord level1Record = new GenericData.Record(level1Schema.toAvroSchema());
    level1Record.put("id", "level1");
    level1Record.put("nested", level0Record);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "level2");
    record.put("nested", level1Record);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(Collections.singletonList("/nested/level2.bin"), paths);
  }

  private static Stream<Arguments> testArrayOfBlobs() {
    return Stream.of(
        Arguments.of(Arrays.asList(
                createExternalBlob("/array/blob1.bin", true),
                createExternalBlob("/array/blob2.bin", true),
                createExternalBlob("/array/blob3.bin", true)),
            Arrays.asList("/array/blob1.bin", "/array/blob2.bin", "/array/blob3.bin")),
        Arguments.of(Arrays.asList(
            createInlineBlob("data1".getBytes()),
            createExternalBlob("/array/managed.bin", true),
            createExternalBlob("/external/unmanaged.bin", false)
        ), Collections.singletonList("/array/managed.bin")),
        Arguments.of(Collections.emptyList(), Collections.emptyList()));
  }

  @ParameterizedTest
  @MethodSource
  void testArrayOfBlobs(List<GenericRecord> blobs, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blobs", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blobs", blobs);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  @Test
  void testArrayOfRecords() {
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
      item.put("blob_field", createExternalBlob("/array/record/blob" + i + ".bin", true));
      items.add(item);
    }

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("items", items);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(Arrays.asList("/array/record/blob1.bin", "/array/record/blob2.bin", "/array/record/blob3.bin"), paths);
  }

  private static Stream<Arguments> testMapOfBlobs() {
    Map<String, GenericRecord> blobMap1 = new HashMap<>();
    blobMap1.put("key1", createExternalBlob("/map/blob1.bin", true));
    blobMap1.put("key2", createExternalBlob("/map/blob2.bin", true));
    blobMap1.put("key3", createExternalBlob("/map/blob3.bin", true));

    Map<String, GenericRecord> blobMap2 = new HashMap<>();
    blobMap2.put("key1", createInlineBlob("data1".getBytes()));
    blobMap2.put("key2", createExternalBlob("/map/managed.bin", true));
    blobMap2.put("key3", createExternalBlob("/external/unmanaged.bin", false));
    return Stream.of(
        Arguments.of(blobMap1, Arrays.asList("/map/blob1.bin", "/map/blob2.bin", "/map/blob3.bin")),
        Arguments.of(blobMap2, Collections.singletonList("/map/managed.bin")),
        Arguments.of(Collections.emptyMap(), Collections.emptyList())
    );
  }

  @ParameterizedTest
  @MethodSource
  void testMapOfBlobs(Map<String, GenericRecord> blobMap, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_map", blobMap);

    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = BlobExtractor.getInstance().getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  private void assertPathsEqual(List<String> expected, List<String> actual) {
    assertEquals(expected.size(), actual.size(), "Expected and actual path lists should have the same size");
    assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  private HoodieSchema createSimpleBlobRecordSchema() {
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("blob_field", HoodieSchema.createBlob(), null, null));
    return HoodieSchema.createRecord("TestRecord", null, null, fields);
  }

  private static GenericRecord createInlineBlob(byte[] data) {
    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "INLINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(data));
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);
    return blob;
  }

  private static GenericRecord createExternalBlob(String path, boolean isManaged) {
    Schema refSchema = HoodieSchema.createBlob().getField("reference").get().schema().getNonNullType().toAvroSchema();
    GenericRecord reference = new GenericData.Record(refSchema);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, path);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, isManaged);

    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "OUT_OF_LINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, null);
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, reference);
    return blob;
  }
}
