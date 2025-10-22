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

package org.apache.hudi.io.storage;

import com.lancedb.lance.file.LanceFileReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSparkLanceWriter}.
 */
public class TestHoodieSparkLanceWriter {

  @TempDir
  File tempDir;

  private HoodieStorage storage;
  private SparkTaskContextSupplier taskContextSupplier;
  private String instantTime;

  @BeforeEach
  public void setUp() throws IOException {
    storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath());
    taskContextSupplier = new SparkTaskContextSupplier();
    instantTime = "20251201120000000";
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  @Test
  public void testWriteRowWithMetadataPopulation() throws Exception {
    // Schema WITH meta fields (writer expects this when populateMetaFields=true)
    StructType schema = createSchemaWithMetaFields();

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_with_metadata.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, true);

    // Create row with PLACEHOLDER meta fields + user data
    InternalRow row = createRowWithMetaFields(1, "Alice", 30L);
    HoodieKey key = new HoodieKey("key1", "partition1");

    writer.writeRowWithMetadata(key, row);
    writer.close();

    // Verify using LanceFileReader
    assertTrue(storage.exists(path), "Lance file should exist");

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {

      assertEquals(1, reader.numRows(), "Should have 1 record");

      // Schema should have 5 meta fields + 3 user fields = 8 fields
      assertEquals(8, reader.schema().getFields().size(), "Should have 8 fields");

      // Read and verify data
      ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE);
      assertTrue(arrowReader.loadNextBatch(), "Should load batch");
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      // Verify meta fields were populated
      VarCharVector commitTimeVector = (VarCharVector) root.getVector(COMMIT_TIME_METADATA_FIELD.getFieldName());
      assertNotNull(commitTimeVector);
      assertEquals(instantTime, new String(commitTimeVector.get(0)), "Commit time should match");

      VarCharVector recordKeyVector = (VarCharVector) root.getVector(RECORD_KEY_METADATA_FIELD.getFieldName());
      assertEquals("key1", new String(recordKeyVector.get(0)), "Record key should match");

      VarCharVector partitionPathVector = (VarCharVector) root.getVector(PARTITION_PATH_METADATA_FIELD.getFieldName());
      assertEquals("partition1", new String(partitionPathVector.get(0)), "Partition path should match");

      VarCharVector fileNameVector = (VarCharVector) root.getVector(FILENAME_METADATA_FIELD.getFieldName());
      assertEquals(path.getName(), new String(fileNameVector.get(0)), "File name should match");

      VarCharVector seqNoVector = (VarCharVector) root.getVector(COMMIT_SEQNO_METADATA_FIELD.getFieldName());
      String seqNo = new String(seqNoVector.get(0));
      assertTrue(seqNo.startsWith(instantTime), "Sequence number should start with instant time");

      // Verify user data fields
      IntVector idVector = (IntVector) root.getVector("id");
      assertEquals(1, idVector.get(0), "ID should match");

      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      assertEquals("Alice", new String(nameVector.get(0)), "Name should match");

      BigIntVector ageVector = (BigIntVector) root.getVector("age");
      assertEquals(30L, ageVector.get(0), "Age should match");

      arrowReader.close();
    }
  }

  @Test
  public void testWriteRowWithoutMetadataPopulation() throws Exception {
    // Schema WITHOUT meta fields
    StructType schema = createSchemaWithoutMetaFields();

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_without_metadata.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    // Create row with just user data (no meta fields)
    InternalRow row = createRow(1, "Bob", 25L);
    HoodieKey key = new HoodieKey("key2", "partition2");

    writer.writeRowWithMetadata(key, row);
    writer.close();

    // Verify using LanceFileReader
    assertTrue(storage.exists(path));

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {

      assertEquals(1, reader.numRows());

      // Schema should have ONLY 3 user fields (no meta fields)
      assertEquals(3, reader.schema().getFields().size(), "Should have only user fields");

      // Read and verify data
      ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE);
      assertTrue(arrowReader.loadNextBatch());
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      // Verify NO meta fields exist
      assertFalse(hasField(root, COMMIT_TIME_METADATA_FIELD.getFieldName()), "Should not have commit time field");
      assertFalse(hasField(root, RECORD_KEY_METADATA_FIELD.getFieldName()), "Should not have record key field");

      // Verify user data
      IntVector idVector = (IntVector) root.getVector("id");
      assertEquals(1, idVector.get(0));

      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      assertEquals("Bob", new String(nameVector.get(0)));

      arrowReader.close();
    }
  }

  @Test
  public void testWriteRowSimple() throws Exception {
    StructType schema = createSchemaWithoutMetaFields();

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_simple_write.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    InternalRow row = createRow(1, "Charlie", 35L);
    writer.writeRow("key3", row);
    writer.close();

    // Verify file exists and has correct record count
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      assertEquals(1, reader.numRows());
    }
  }

  @Test
  public void testSequenceIdGeneration() throws Exception {
    StructType schema = createSchemaWithMetaFields();

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_sequence_id.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, true);

    // Write multiple records
    for (int i = 0; i < 3; i++) {
      InternalRow row = createRowWithMetaFields(i, "User" + i, 20L + i);
      HoodieKey key = new HoodieKey("key" + i, "partition1");
      writer.writeRowWithMetadata(key, row);
    }
    writer.close();

    // Verify sequence IDs are unique and properly formatted
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {

      assertEquals(3, reader.numRows());

      ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE);
      arrowReader.loadNextBatch();
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      VarCharVector seqNoVector = (VarCharVector) root.getVector(COMMIT_SEQNO_METADATA_FIELD.getFieldName());

      List<String> seqNos = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        String seqNo = new String(seqNoVector.get(i));
        assertTrue(seqNo.startsWith(instantTime + "_"), "Sequence ID should start with instantTime");
        seqNos.add(seqNo);
      }

      // All sequence IDs should be unique
      assertEquals(3, seqNos.stream().distinct().count(), "All sequence IDs should be unique");

      arrowReader.close();
    }
  }

  @Test
  public void testBatchFlushing() throws Exception {
    StructType schema = createSchemaWithoutMetaFields();

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_batch_flush.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    // Write more than DEFAULT_BATCH_SIZE (1000) records
    int recordCount = 2500;
    for (int i = 0; i < recordCount; i++) {
      InternalRow row = createRow(i, "User" + i, 20L + i);
      writer.writeRow("key" + i, row);
    }
    writer.close();

    // Verify all records were written
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      assertEquals(recordCount, reader.numRows(), "All records should be written");
    }
  }

  @Test
  public void testPrimitiveTypes() throws Exception {
    StructType schema = new StructType()
        .add("int_field", DataTypes.IntegerType, false)
        .add("long_field", DataTypes.LongType, false)
        .add("float_field", DataTypes.FloatType, false)
        .add("double_field", DataTypes.DoubleType, false)
        .add("bool_field", DataTypes.BooleanType, false)
        .add("string_field", DataTypes.StringType, false)
        .add("binary_field", DataTypes.BinaryType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_primitives.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    GenericInternalRow row = new GenericInternalRow(new Object[]{
        42,                                    // int
        123456789L,                           // long
        3.14f,                                // float
        2.71828,                              // double
        true,                                 // boolean
        UTF8String.fromString("test"),        // string
        new byte[]{1, 2, 3, 4}               // binary
    });

    writer.writeRow("key1", row);
    writer.close();

    // Verify all types were written correctly
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {

      assertEquals(1, reader.numRows());
      assertEquals(7, reader.schema().getFields().size());

      ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE);
      arrowReader.loadNextBatch();
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      assertEquals(42, ((IntVector) root.getVector("int_field")).get(0));
      assertEquals(123456789L, ((BigIntVector) root.getVector("long_field")).get(0));
      assertEquals(3.14f, ((Float4Vector) root.getVector("float_field")).get(0), 0.001);
      assertEquals(2.71828, ((Float8Vector) root.getVector("double_field")).get(0), 0.00001);
      assertEquals(1, ((BitVector) root.getVector("bool_field")).get(0));
      assertEquals("test", new String(((VarCharVector) root.getVector("string_field")).get(0)));

      byte[] binary = ((VarBinaryVector) root.getVector("binary_field")).get(0);
      assertEquals(4, binary.length);

      arrowReader.close();
    }
  }

  @Test
  public void testNullValues() throws Exception {
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_nulls.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    // Write rows with null values
    writer.writeRow("key1", createRow(1, "Alice", 30L));
    writer.writeRow("key2", createRow(2, null, 25L));  // null name
    writer.writeRow("key3", createRow(3, "Charlie", null));  // null age
    writer.close();

    // Verify nulls are preserved
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {

      assertEquals(3, reader.numRows());

      ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE);
      arrowReader.loadNextBatch();
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      assertFalse(nameVector.isNull(0), "First name should not be null");
      assertTrue(nameVector.isNull(1), "Second name should be null");
      assertFalse(nameVector.isNull(2), "Third name should not be null");

      BigIntVector ageVector = (BigIntVector) root.getVector("age");
      assertFalse(ageVector.isNull(0), "First age should not be null");
      assertFalse(ageVector.isNull(1), "Second age should not be null");
      assertTrue(ageVector.isNull(2), "Third age should be null");

      arrowReader.close();
    }
  }

  @Test
  public void testWriteEmptyDataset() throws Exception {
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_empty.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);
    writer.close();  // Close without writing any rows

    // Lance doesn't create a file if no data is written
    assertFalse(storage.exists(path), "Lance file should not exist when no data is written");
  }

  @Test
  public void testWriteStructType() throws Exception {
    // Create schema with nested struct
    StructType addressSchema = new StructType()
        .add("street", DataTypes.StringType, true)
        .add("city", DataTypes.StringType, true)
        .add("zipcode", DataTypes.IntegerType, true);

    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("address", addressSchema, true);

    // Create test data with nested struct
    GenericInternalRow address1 = new GenericInternalRow(new Object[]{
        UTF8String.fromString("123 Main St"),
        UTF8String.fromString("New York"),
        10001
    });

    GenericInternalRow address2 = new GenericInternalRow(new Object[]{
        UTF8String.fromString("456 Oak Ave"),
        UTF8String.fromString("Los Angeles"),
        90001
    });

    List<InternalRow> rows = new ArrayList<>();
    rows.add(new GenericInternalRow(new Object[]{1, UTF8String.fromString("Alice"), address1}));
    rows.add(new GenericInternalRow(new Object[]{2, UTF8String.fromString("Bob"), address2}));

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_struct.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    for (InternalRow row : rows) {
      writer.writeRow("key" + rows.indexOf(row), row);
    }
    writer.close();

    assertTrue(storage.exists(path), "Lance file with struct type should exist");
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      assertEquals(rows.size(), reader.numRows(), "Row count should match");
      assertEquals(3, reader.schema().getFields().size(), "Should have 3 top-level fields (id, name, address)");
    }
  }

  // Helper methods

  private StructType createSchemaWithMetaFields() {
    return new StructType()
        .add(COMMIT_TIME_METADATA_FIELD.getFieldName(), DataTypes.StringType, false)
        .add(COMMIT_SEQNO_METADATA_FIELD.getFieldName(), DataTypes.StringType, false)
        .add(RECORD_KEY_METADATA_FIELD.getFieldName(), DataTypes.StringType, false)
        .add(PARTITION_PATH_METADATA_FIELD.getFieldName(), DataTypes.StringType, false)
        .add(FILENAME_METADATA_FIELD.getFieldName(), DataTypes.StringType, false)
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true);
  }

  private StructType createSchemaWithoutMetaFields() {
    return new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true);
  }

  private InternalRow createRowWithMetaFields(Object... userValues) {
    // Create row with PLACEHOLDER meta fields (will be updated by writer) + user data
    Object[] allValues = new Object[5 + userValues.length];

    // Meta fields - use empty strings as placeholders
    allValues[0] = UTF8String.fromString(""); // commit_time
    allValues[1] = UTF8String.fromString(""); // commit_seqno
    allValues[2] = UTF8String.fromString(""); // record_key
    allValues[3] = UTF8String.fromString(""); // partition_path
    allValues[4] = UTF8String.fromString(""); // file_name

    // Copy user values
    for (int i = 0; i < userValues.length; i++) {
      allValues[5 + i] = processValue(userValues[i]);
    }

    return new GenericInternalRow(allValues);
  }

  private InternalRow createRow(Object... values) {
    Object[] processedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      processedValues[i] = processValue(values[i]);
    }
    return new GenericInternalRow(processedValues);
  }

  private Object processValue(Object value) {
    if (value instanceof String) {
      return UTF8String.fromString((String) value);
    }
    return value;
  }

  private boolean hasField(VectorSchemaRoot root, String fieldName) {
    try {
      return root.getVector(fieldName) != null;
    } catch (Exception e) {
      return false;
    }
  }
}