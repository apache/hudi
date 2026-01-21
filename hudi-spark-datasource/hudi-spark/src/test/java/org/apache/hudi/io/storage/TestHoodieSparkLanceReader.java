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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.SafeProjection;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      List<InternalRow> actualRows = readAllRows(reader, schema);

      assertEquals(expectedRows.size(), actualRows.size(), "Should read same number of records");

      // Verify nulls are preserved
      assertTrue(actualRows.get(1).isNullAt(1), "Second row name should be null");
      assertFalse(actualRows.get(1).isNullAt(2), "Second row value should not be null");

      assertFalse(actualRows.get(2).isNullAt(1), "Third row name should not be null");
      assertTrue(actualRows.get(2).isNullAt(2), "Third row value should be null");

      assertTrue(actualRows.get(3).isNullAt(1), "Fourth row name should be null");
      assertTrue(actualRows.get(3).isNullAt(2), "Fourth row value should be null");
    }
  }

  @Test
  public void testReadAllSupportedTypes() throws Exception {
    // Create schema with all supported primitive types
    StructType schema = new StructType()
        .add("int_field", DataTypes.IntegerType, false)
        .add("long_field", DataTypes.LongType, false)
        .add("float_field", DataTypes.FloatType, false)
        .add("double_field", DataTypes.DoubleType, false)
        .add("bool_field", DataTypes.BooleanType, false)
        .add("string_field", DataTypes.StringType, false)
        .add("binary_field", DataTypes.BinaryType, false);

    // Create test data with 3 records
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(42, 123456789L, 3.14f, 2.71828, true, "Alice", new byte[]{1, 2, 3, 4}));
    expectedRows.add(createRow(-100, 987654321L, 2.5f, 98.765, false, "Bob", new byte[]{10, 20, 30}));
    expectedRows.add(createRow(0, 0L, 0.0f, 0.0, true, "Charlie", new byte[]{99}));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_all_types.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Test
  public void testReadDecimalType() throws Exception {
    // Create schema with decimal types of different precision and scale
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("decimal_small", DataTypes.createDecimalType(10, 2), false)
        .add("decimal_large", DataTypes.createDecimalType(20, 5), false)
        .add("decimal_zero_scale", DataTypes.createDecimalType(10, 0), false);

    // Create test data with 3 records
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, Decimal.apply(new BigDecimal("999.99"), 10, 2),
        Decimal.apply(new BigDecimal("123456789.12345"), 20, 5), Decimal.apply(12345L, 10, 0)));
    expectedRows.add(createRow(2, Decimal.apply(new BigDecimal("-123.45"), 10, 2),
        Decimal.apply(new BigDecimal("-987654.54321"), 20, 5), Decimal.apply(-5000L, 10, 0)));
    expectedRows.add(createRow(3, Decimal.apply(0L, 10, 2),
        Decimal.apply(0L, 20, 5), Decimal.apply(0L, 10, 0)));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_decimal.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Test
  public void testReadTimestampAndDateTypes() throws Exception {
    // Create schema with timestamp and date types
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("timestamp_field", DataTypes.TimestampType, false)
        .add("date_field", DataTypes.DateType, false);

    // Create test data with 3 records
    // Timestamps are microseconds since Unix epoch, dates are days since Unix epoch
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, 1609459200000000L, 18628));  // 2021-01-01
    expectedRows.add(createRow(2, 1640995200000000L, 18993));  // 2022-01-01
    expectedRows.add(createRow(3, 0L, 0));  // 1970-01-01 (epoch)

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_timestamp_date.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Test
  public void testReadArrayType() throws Exception {
    // Create schema with array types
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("int_array", DataTypes.createArrayType(DataTypes.IntegerType, false), false)
        .add("string_array", DataTypes.createArrayType(DataTypes.StringType, false), false)
        .add("nullable_elements", DataTypes.createArrayType(DataTypes.StringType, true), true);

    // Create test data with 3 records
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, new Object[]{}, new Object[]{}, null));  // Empty arrays
    expectedRows.add(createRow(2, new Object[]{42}, new Object[]{"Alice"}, new Object[]{null}));  // Single element
    expectedRows.add(createRow(3, new Object[]{1, 2, 3, 4, 5}, new Object[]{"Bob", "Charlie", "David"}, new Object[]{"A", null, "B", null}));  // Multi-element

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_array.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Disabled
  @Test
  public void testReadMapType() throws Exception {
    // Currently map type is not supported in current lance version: https://github.com/lance-format/lance/issues/3620
    // To re-enable test once lance java file writer default supports map type

    // Create schema with map types
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("string_to_int_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false), false)
        .add("int_to_string_map", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType, false), false);

    // Create test data with 3 records
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1,
        new ArrayBasedMapData(new GenericArrayData(new Object[]{}), new GenericArrayData(new Object[]{})),
        new ArrayBasedMapData(new GenericArrayData(new Object[]{}), new GenericArrayData(new Object[]{}))));
    expectedRows.add(createRow(2,
        new ArrayBasedMapData(new GenericArrayData(new Object[]{"key1"}), new GenericArrayData(new Object[]{100})),
        new ArrayBasedMapData(new GenericArrayData(new Object[]{1}), new GenericArrayData(new Object[]{"value1"}))));
    expectedRows.add(createRow(3,
        new ArrayBasedMapData(new GenericArrayData(new Object[]{"alpha", "beta", "gamma"}), new GenericArrayData(new Object[]{10, 20, 30})),
        new ArrayBasedMapData(new GenericArrayData(new Object[]{100, 200, 300}), new GenericArrayData(new Object[]{"hundred", "two hundred", "three hundred"}))));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_map.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Test
  public void testReadStructType() throws Exception {
    // Create schema with nested struct
    StructType addressSchema = new StructType()
        .add("street", DataTypes.StringType, false)
        .add("city", DataTypes.StringType, false)
        .add("zipcode", DataTypes.IntegerType, false);

    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, false)
        .add("address", addressSchema, true);

    // Create test data with 3 records
    List<InternalRow> expectedRows = new ArrayList<>();
    expectedRows.add(createRow(1, "Alice", createRow("123 Main St", "New York", 10001)));
    expectedRows.add(createRow(2, "Bob", createRow("456 Oak Ave", "Los Angeles", 90001)));
    expectedRows.add(createRow(3, "Charlie", createRow("789 Pine Rd", "Chicago", 60601)));

    // Write and read back
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_struct.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows)) {
      assertNotNull(reader.getSchema(), "Schema should not be null");
      List<InternalRow> actualRows = readAllRows(reader, schema);
      assertEquals(expectedRows, actualRows);
    }
  }

  @Test
  public void testReadLargeDataset() throws Exception {
    // Test batch reading with more than 1000 records
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("value", DataTypes.LongType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_large.lance");
    int recordCount = 2500;
    try (HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, false)) {
      for (int i = 0; i < recordCount; i++) {
        GenericInternalRow row = new GenericInternalRow(new Object[]{i, (long) i * 2});
        writer.writeRow("key" + i, row);
      }
    }

    // Read back
    try (HoodieSparkLanceReader reader = new HoodieSparkLanceReader(path)) {
      assertEquals(recordCount, reader.getTotalRecords(), "Total record count should match");

      // Verify all records
      int count = 0;
      HoodieSchema readerSchema = reader.getSchema();
      try (ClosableIterator<HoodieRecord<InternalRow>> iterator = reader.getRecordIterator(readerSchema)) {
        while (iterator.hasNext()) {
          HoodieRecord<InternalRow> record = iterator.next();
          InternalRow row = record.getData();
          assertEquals(count, row.getInt(0), "id should match");
          assertEquals(count * 2L, row.getLong(1), "value should match");
          count++;
        }
      }

      assertEquals(recordCount, count, "Should read all records");
    }
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
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, expectedRows, true)) {
      // Read record keys using getRecordKeyIterator
      List<String> actualKeys = new ArrayList<>();
      try (ClosableIterator<String> keyIterator = reader.getRecordKeyIterator()) {
        while (keyIterator.hasNext()) {
          actualKeys.add(keyIterator.next());
        }
      }

      // Verify all keys are returned in the same order
      List<String> expectedKeys = Arrays.asList("key0", "key1", "key2", "key3", "key4");
      assertEquals(expectedKeys, actualKeys, "Record keys should match");
    }
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
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true)) {
      // Create candidate set with 3 specific keys
      Set<String> candidateKeys = new HashSet<>();
      candidateKeys.add("key2");
      candidateKeys.add("key5");
      candidateKeys.add("key7");

      // Filter row keys
      Set<Pair<String, Long>> result = reader.filterRowKeys(candidateKeys);

      // Verify result contains exactly the expected key-position pairs
      Set<Pair<String, Long>> expected = Set.of(
          Pair.of("key2", 2L),
          Pair.of("key5", 5L),
          Pair.of("key7", 7L)
      );
      assertEquals(expected, result, "Should contain exactly the expected key-position pairs");
    }
  }

  private static Stream<Arguments> filterRowKeysInputParams() {
    return Stream.of(
        Arguments.of(Collections.emptySet()),
        Arguments.of((Set<String>) null)
    );
  }

  @ParameterizedTest
  @MethodSource("filterRowKeysInputParams")
  public void testFilterRowKeysReturnsAllKeys(Set<String> candidateKeys) throws Exception {
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
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_filter.lance");
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true)) {
      // Filter with candidateKeys - should return all keys for null or empty set
      Set<Pair<String, Long>> result = reader.filterRowKeys(candidateKeys);

      // Verify all 5 keys are returned with correct positions
      Set<Pair<String, Long>> expected = Set.of(
          Pair.of("key0", 0L),
          Pair.of("key1", 1L),
          Pair.of("key2", 2L),
          Pair.of("key3", 3L),
          Pair.of("key4", 4L)
      );
      assertEquals(expected, result, "Filter should return all keys with correct positions");
    }
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
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true)) {
      // Filter with candidates that don't exist
      Set<String> candidateKeys = new HashSet<>();
      candidateKeys.add("key999");
      candidateKeys.add("key888");
      candidateKeys.add("nonexistent");

      Set<Pair<String, Long>> result = reader.filterRowKeys(candidateKeys);

      // Verify result is empty
      assertEquals(0, result.size(), "Should return empty set when no keys match");
    }
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
    try (HoodieSparkLanceReader reader = writeAndCreateReader(path, schema, rows, true)) {
      // Verify record count
      assertEquals(3, reader.getTotalRecords(), "Should have 3 records");

      // Read all records and verify metadata fields were populated
      List<InternalRow> actualRows = readAllRows(reader, schema);
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
  }

  /**
   * Helper method to read all rows from a reader into a list.
   * Converts UnsafeRow to GenericInternalRow by extracting field values using type-specific getters.
   */
  private List<InternalRow> readAllRows(HoodieSparkLanceReader reader, StructType schema) throws IOException {
    List<InternalRow> rows = new ArrayList<>();
    HoodieSchema hoodieSchema = reader.getSchema();

    try (ClosableIterator<HoodieRecord<InternalRow>> iterator = reader.getRecordIterator(hoodieSchema)) {
      while (iterator.hasNext()) {
        HoodieRecord<InternalRow> record = iterator.next();
        InternalRow unsafeRow = record.getData();
        rows.add(SafeProjection.create(schema).apply(unsafeRow));
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
    try (HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, schema, instantTime, taskContextSupplier, storage, populateMetaFields)) {
      for (int i = 0; i < rows.size(); i++) {
        HoodieKey key = new HoodieKey("key" + i, "default_partition");
        // Note writeRowWithMetadata implicitly handles case where populateMetaFields=false
        writer.writeRowWithMetadata(key, rows.get(i));
      }
    }
    return new HoodieSparkLanceReader(path);
  }

  @Test
  public void testReadWithRequestedSchema() throws Exception {
    StructType fullSchema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.IntegerType, true)
        .add("city", DataTypes.StringType, true);

    List<InternalRow> rows = new ArrayList<>();
    rows.add(createRow(1, "Alice", 30, "NYC"));
    rows.add(createRow(2, "Bob", 25, "SF"));
    rows.add(createRow(3, "Charlie", 35, "LA"));

    // Write Lance file with full schema
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/test_projection.lance");
    try (HoodieSparkLanceWriter writer = new HoodieSparkLanceWriter(
        path, fullSchema, instantTime, taskContextSupplier, storage, false)) {
      for (int i = 0; i < rows.size(); i++) {
        writer.writeRow("key" + i, rows.get(i));
      }
    }

    //  Create requested schema with only 2 fields
    StructType requestedSparkSchema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true);

    // Convert to HoodieSchema
    HoodieSchema fullHoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        fullSchema, "record", "", true);
    HoodieSchema requestedHoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        requestedSparkSchema, "record", "", true);

    // Read with schema projection
    try (HoodieSparkLanceReader reader = new HoodieSparkLanceReader(path)) {
      try (ClosableIterator<HoodieRecord<InternalRow>> recordIterator =
           reader.getRecordIterator(fullHoodieSchema, requestedHoodieSchema)) {

        int count = 0;
        while (recordIterator.hasNext()) {
          HoodieRecord<InternalRow> record = recordIterator.next();
          InternalRow row = record.getData();
          // Verify only 3 columns present 
          assertEquals(2, row.numFields(), "Should have only 2 projected fields");
          // Verify projected field values
          assertEquals(count + 1, row.getInt(0), "ID should match");
          assertFalse(row.isNullAt(1), "Name should not be null");
          count++;
        }
        assertEquals(3, count, "Should read all 3 records");
      }
    }
  }
}
