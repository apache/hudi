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

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.io.storage.LanceTestUtils.createRow;
import static org.apache.hudi.io.storage.LanceTestUtils.createRowWithMetaFields;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSparkLanceReader}.
 */
public class TestHoodieSparkLanceReader {

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
  public void testReadPrimitiveTypes() throws Exception {
    // Create schema with primitive types
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true)
        .add("score", DataTypes.DoubleType, true)
        .add("active", DataTypes.BooleanType, true);

    // Create test data
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, "Alice", 30L, 95.5, true));
    expectedRows.add(createRow(2, "Bob", 25L, 87.3, false));
    expectedRows.add(createRow(3, "Charlie", 35L, 92.1, true));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_primitives.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows);

    // Verify record count
    assertEquals(expectedRows.size(), reader.getTotalRecords(), "Record count should match");

    // Verify schema
    assertNotNull(reader.getSchema(), "Schema should not be null");

    // Read all records
    List<InternalRow> actualRows = readAllRows(reader);

    // Verify record count
    assertEquals(expectedRows.size(), actualRows.size(), "Should read same number of records");

    // Verify each record
    for (int i = 0; i < expectedRows.size(); i++) {
      InternalRow expected = expectedRows.get(i);
      InternalRow actual = actualRows.get(i);

      assertEquals(expected.getInt(0), actual.getInt(0), "id field should match");
      assertEquals(expected.getUTF8String(1), actual.getUTF8String(1), "name field should match");
      assertEquals(expected.getLong(2), actual.getLong(2), "age field should match");
      assertEquals(expected.getDouble(3), actual.getDouble(3), 0.001, "score field should match");
      assertEquals(expected.getBoolean(4), actual.getBoolean(4), "active field should match");
    }

  }

  @Test
  public void testReadWithNulls() throws Exception {
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("value", DataTypes.DoubleType, true);

    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, "Alice", 100.0));
    expectedRows.add(createRow(2, null, 200.0));  // null name
    expectedRows.add(createRow(3, "Charlie", null));  // null value
    expectedRows.add(createRow(4, null, null));  // multiple nulls

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_nulls.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows);
    List<InternalRow> actualRows = readAllRows(reader);

    assertEquals(expectedRows.size(), actualRows.size(), "Should read same number of records");

    // Verify nulls are preserved
    assertTrue(actualRows.get(1).isNullAt(1), "Second row name should be null");
    assertFalse(actualRows.get(1).isNullAt(2), "Second row value should not be null");

    assertTrue(actualRows.get(2).isNullAt(2), "Third row value should be null");
    assertFalse(actualRows.get(2).isNullAt(1), "Third row name should not be null");

    assertTrue(actualRows.get(3).isNullAt(1), "Fourth row name should be null");
    assertTrue(actualRows.get(3).isNullAt(2), "Fourth row value should be null");

  }

  @Test
  public void testReadAllSupportedTypes() throws Exception {
    StructType schema = new StructType()
        .add("int_field", DataTypes.IntegerType, false)
        .add("long_field", DataTypes.LongType, false)
        .add("float_field", DataTypes.FloatType, false)
        .add("double_field", DataTypes.DoubleType, false)
        .add("bool_field", DataTypes.BooleanType, false)
        .add("string_field", DataTypes.StringType, false)
        .add("binary_field", DataTypes.BinaryType, false);

    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(new GenericInternalRow(new Object[]{
        42,                                    // int
        123456789L,                           // long
        3.14f,                                // float
        2.71828,                              // double
        true,                                 // boolean
        UTF8String.fromString("test"),        // string
        new byte[]{1, 2, 3, 4}               // binary
    }));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_all_types.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows);
    assertEquals(1, reader.getTotalRecords());

    List<InternalRow> actualRows = readAllRows(reader);

    assertEquals(1, actualRows.size());
    InternalRow actual = actualRows.get(0);

    assertEquals(42, actual.getInt(0), "int field should match");
    assertEquals(123456789L, actual.getLong(1), "long field should match");
    assertEquals(3.14f, actual.getFloat(2), 0.001, "float field should match");
    assertEquals(2.71828, actual.getDouble(3), 0.00001, "double field should match");
    assertTrue(actual.getBoolean(4), "bool field should match");
    assertEquals("test", actual.getUTF8String(5).toString(), "string field should match");

    byte[] expectedBytes = new byte[]{1, 2, 3, 4};
    byte[] actualBytes = actual.getBinary(6);
    assertEquals(expectedBytes.length, actualBytes.length, "binary field length should match");
    for (int i = 0; i < expectedBytes.length; i++) {
      assertEquals(expectedBytes[i], actualBytes[i], "binary field byte " + i + " should match");
    }

  }

  @Test
  public void testReadLargeDataset() throws Exception {
    // Test batch reading with more than 1000 records
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("value", DataTypes.LongType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_large.lance");
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false);

    int recordCount = 2500;
    for (int i = 0; i < recordCount; i++) {
      GenericInternalRow row = new GenericInternalRow(new Object[]{i, (long) i * 2});
      writer.writeRow("key" + i, row);
    }
    writer.close();

    // Read back
    HoodieSparkLanceReader reader = new HoodieSparkLanceReader(storage, path);
    assertEquals(recordCount, reader.getTotalRecords(), "Total record count should match");

    // Verify all records
    int count = 0;
    try (ClosableIterator<UnsafeRow> iterator = reader.getUnsafeRowIterator()) {
      while (iterator.hasNext()) {
        InternalRow row = iterator.next();
        assertEquals(count, row.getInt(0), "id should match");
        assertEquals(count * 2L, row.getLong(1), "value should match");
        count++;
      }
    }

    assertEquals(recordCount, count, "Should read all records");
  }

  @Test
  public void testGetRecordKeyIterator() throws Exception {
    // Create schema with all 5 Hudi metadata fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.IntegerType, true);

    // Create test data with placeholder metadata fields
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRowWithMetaFields("Alice", 30));
    expectedRows.add(createRowWithMetaFields("Bob", 25));
    expectedRows.add(createRowWithMetaFields("Charlie", 35));
    expectedRows.add(createRowWithMetaFields("David", 40));
    expectedRows.add(createRowWithMetaFields("Eve", 28));

    // Write with metadata population enabled
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_record_keys.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows, true);

    // Read record keys using getRecordKeyIterator
    List<String> actualKeys = new ArrayList<>();
    try (ClosableIterator<String> keyIterator = reader.getRecordKeyIterator()) {
      while (keyIterator.hasNext()) {
        actualKeys.add(keyIterator.next());
      }
    }

    // Verify all keys are returned in the same order
    assertEquals(5, actualKeys.size(), "Should return all 5 record keys");
    assertEquals("key0", actualKeys.get(0), "First key should match");
    assertEquals("key1", actualKeys.get(1), "Second key should match");
    assertEquals("key2", actualKeys.get(2), "Third key should match");
    assertEquals("key3", actualKeys.get(3), "Fourth key should match");
    assertEquals("key4", actualKeys.get(4), "Fifth key should match");
  }

  @Test
  public void testFilterRowKeysWithCandidates() throws Exception {
    // Create schema with all 5 Hudi metadata fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("value", DataTypes.IntegerType, true);

    // Create 10 records with placeholder metadata fields
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(createRowWithMetaFields(i * 10));
    }

    // Write with metadata population enabled
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_filter_candidates.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true);

    // Create candidate set with 3 specific keys
    Set<String> candidateKeys = new HashSet<>();
    candidateKeys.add("key2");
    candidateKeys.add("key5");
    candidateKeys.add("key7");

    // Filter row keys
    Set<Pair<String, Long>> result = reader.filterRowKeys(candidateKeys);

    // Verify result contains exactly 3 entries with correct positions
    assertEquals(3, result.size(), "Should return exactly 3 matching keys");

    assertTrue(result.contains(Pair.of("key2", 2L)), "Should contain key2 at position 2");
    assertTrue(result.contains(Pair.of("key5", 5L)), "Should contain key5 at position 5");
    assertTrue(result.contains(Pair.of("key7", 7L)), "Should contain key7 at position 7");
  }

  @Test
  public void testFilterRowKeysWithEmptySet() throws Exception {
    // Create schema with all 5 Hudi metadata fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("value", DataTypes.IntegerType, true);

    // Create 5 records with placeholder metadata fields
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(createRowWithMetaFields(i));
    }

    // Write with metadata population enabled
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_filter_empty.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true);

    // Filter with empty set - should return all keys
    Set<Pair<String, Long>> result = reader.filterRowKeys(Collections.emptySet());

    // Verify all 5 keys are returned with correct positions
    assertEquals(5, result.size(), "Empty filter should return all keys");
    assertTrue(result.contains(Pair.of("key0", 0L)), "Should contain key0 at position 0");
    assertTrue(result.contains(Pair.of("key1", 1L)), "Should contain key1 at position 1");
    assertTrue(result.contains(Pair.of("key2", 2L)), "Should contain key2 at position 2");
    assertTrue(result.contains(Pair.of("key3", 3L)), "Should contain key3 at position 3");
    assertTrue(result.contains(Pair.of("key4", 4L)), "Should contain key4 at position 4");
  }

  @Test
  public void testFilterRowKeysWithNull() throws Exception {
    // Create schema with all 5 Hudi metadata fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("value", DataTypes.IntegerType, true);

    // Create 5 records with placeholder metadata fields
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(createRowWithMetaFields(i));
    }

    // Write with metadata population enabled
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_filter_null.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true);

    // Filter with null - should return all keys
    Set<Pair<String, Long>> result = reader.filterRowKeys(null);

    // Verify all 5 keys are returned
    assertEquals(5, result.size(), "Null filter should return all keys");
    assertTrue(result.contains(Pair.of("key0", 0L)));
    assertTrue(result.contains(Pair.of("key1", 1L)));
    assertTrue(result.contains(Pair.of("key2", 2L)));
    assertTrue(result.contains(Pair.of("key3", 3L)));
    assertTrue(result.contains(Pair.of("key4", 4L)));
  }

  @Test
  public void testFilterRowKeysNoMatches() throws Exception {
    // Create schema with all 5 Hudi metadata fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("value", DataTypes.IntegerType, true);

    // Create 5 records with placeholder metadata fields
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(createRowWithMetaFields(i));
    }

    // Write with metadata population enabled
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_filter_no_match.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true);

    // Filter with candidates that don't exist
    Set<String> candidateKeys = new HashSet<>();
    candidateKeys.add("key999");
    candidateKeys.add("key888");
    candidateKeys.add("nonexistent");

    Set<Pair<String, Long>> result = reader.filterRowKeys(candidateKeys);

    // Verify result is empty
    assertEquals(0, result.size(), "Should return empty set when no keys match");
  }

  @Test
  public void testReadWithMetadataPopulation() throws Exception {
    // Create schema WITH all 5 Hudi metadata fields + user fields
    StructType schema = new StructType()
        .add(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false)
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true);

    // Create rows WITH placeholder metadata fields (empty strings) + user data
    List<InternalRow> rows = new ArrayList<>();
    rows.add(createRowWithMetaFields(1, "Alice", 30L));
    rows.add(createRowWithMetaFields(2, "Bob", 25L));
    rows.add(createRowWithMetaFields(3, "Charlie", 35L));

    // Write with populateMetaFields=true - writer should populate the placeholders
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_metadata_population.lance");
    HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true);

    // Verify record count
    assertEquals(3, reader.getTotalRecords(), "Should have 3 records");

    // Read all records and verify metadata fields were populated
    List<InternalRow> actualRows = readAllRows(reader);
    assertEquals(3, actualRows.size(), "Should read 3 records");

    // Verify first row has populated metadata fields
    InternalRow firstRow = actualRows.get(0);

    // Verify metadata fields are populated (not empty placeholders)
    assertFalse(firstRow.isNullAt(0), "Commit time should not be null");
    assertEquals(instantTime, firstRow.getUTF8String(0).toString(), "Commit time should match");

    assertFalse(firstRow.isNullAt(1), "Commit seqno should not be null");
    String seqNo = firstRow.getUTF8String(1).toString();
    assertTrue(seqNo.startsWith(instantTime), "Sequence number should start with instant time");

    assertFalse(firstRow.isNullAt(2), "Record key should not be null");
    // Record key is set to "key0" by writeAndCreateReader helper

    assertFalse(firstRow.isNullAt(3), "Partition path should not be null");
    assertFalse(firstRow.isNullAt(4), "File name should not be null");
    assertEquals(path.getName(), firstRow.getUTF8String(4).toString(), "File name should match");

    // Verify user data fields are intact (at positions 5, 6, 7)
    assertEquals(1, firstRow.getInt(5), "ID should match");
    assertEquals("Alice", firstRow.getUTF8String(6).toString(), "Name should match");
    assertEquals(30L, firstRow.getLong(7), "Age should match");

    // Verify all three rows have unique sequence numbers
    Set<String> seqNos = new HashSet<>();
    for (InternalRow row : actualRows) {
      seqNos.add(row.getUTF8String(1).toString());
    }
    assertEquals(3, seqNos.size(), "All sequence numbers should be unique");
  }

  /**
   * Helper method to read all rows from a reader into a list.
   * Makes copies of UnsafeRows to avoid reuse issues.
   */
  private List<InternalRow> readAllRows(HoodieSparkLanceReader reader) throws IOException {
    List<InternalRow> rows = new ArrayList<>();
    try (ClosableIterator<UnsafeRow> iterator = reader.getUnsafeRowIterator()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next().copy());
      }
    }
    return rows;
  }

  /**
   * Helper method to write rows to a Lance file and create a reader.
   */
  private HoodieSparkLanceReader writeAndCreateReader(StoragePath path, StructType schema, List<InternalRow> rows) throws IOException {
    return writeAndCreateReader(path, schema, rows, false);
  }
    
  private HoodieSparkLanceReader writeAndCreateReader(StoragePath path, StructType schema, List<InternalRow> rows, boolean populateMetaFields) throws IOException {
    HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, populateMetaFields);
    for (InternalRow row : rows) {
      HoodieKey key = new HoodieKey("key" + rows.indexOf(row), "default_partition");
      // Note writeRowWithMetadata implicitly handles case where populateMetaFields=false
      writer.writeRowWithMetadata(key, row);
    }
    writer.close();
    return new HoodieSparkLanceReader(storage, path);
  }
}