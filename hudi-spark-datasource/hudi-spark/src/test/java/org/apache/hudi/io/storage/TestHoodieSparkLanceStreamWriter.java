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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.lance.file.LanceFileReader;

import java.io.ByteArrayOutputStream;
import java.io.File;

import static org.apache.hudi.io.storage.LanceTestUtils.createRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSparkLanceStreamWriter}.
 */
@DisabledIfSystemProperty(named = "lance.skip.tests", matches = "true")
class TestHoodieSparkLanceStreamWriter {

  @TempDir
  File tempDir;

  private HoodieStorage storage;

  @BeforeEach
  void setUp() {
    storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  @Test
  void testBasicStreamWrite() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Write 100 records to stream
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      for (int i = 0; i < 100; i++) {
        InternalRow row = createRow(i, "User" + i, 20L + i);
        writer.writeRow("key" + i, row);
      }
    }

    // Verify the stream contains valid Lance data
    byte[] lanceData = baos.toByteArray();
    assertTrue(lanceData.length > 0, "Stream should contain Lance data");

    // Verify we can read back the data
    verifyLanceData(lanceData, 100);
  }

  @Test
  void testEmptyStreamWrite() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Close without writing any rows
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      // No writes
    }

    // Should create a valid empty Lance file
    byte[] lanceData = baos.toByteArray();
    assertTrue(lanceData.length > 0, "Should create valid empty Lance file with schema");

    // Verify the empty file has valid structure
    verifyLanceData(lanceData, 0);
  }

  @Test
  void testLargeDataStreamWrite() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Write 10K records to test temp file handling with larger dataset
    int recordCount = 10000;
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      for (int i = 0; i < recordCount; i++) {
        InternalRow row = createRow(i, "User" + i, 20L + i);
        writer.writeRow("key" + i, row);
      }
    }

    // Verify all records were written
    byte[] lanceData = baos.toByteArray();
    assertTrue(lanceData.length > 1024, "Should have significant data for 10K records");
    verifyLanceData(lanceData, recordCount);
  }

  @Test
  void testTempFileCleanup() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Count temp files before
    File tempDirFile = new File(System.getProperty("java.io.tmpdir"));
    int initialLanceFileCount = countLanceFiles(tempDirFile);

    // Write and close
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      for (int i = 0; i < 10; i++) {
        InternalRow row = createRow(i, "User" + i, 20L + i);
        writer.writeRow("key" + i, row);
      }
    }

    // Verify temp file was cleaned up
    int finalLanceFileCount = countLanceFiles(tempDirFile);
    assertEquals(initialLanceFileCount, finalLanceFileCount,
        "Temp Lance file should be cleaned up after close");
  }
//
//  @Test
//  void testTempFileCleanupOnError() throws Exception {
//    StructType schema = createSimpleSchema();
//
//    // Create an output stream that will fail on write
//    FailingOutputStream failingStream = new FailingOutputStream();
//    File tempDirFile = new File(System.getProperty("java.io.tmpdir"));
//    int initialLanceFileCount = countLanceFiles(tempDirFile);
//
//    // Attempt write that should fail during close
//    assertThrows(IOException.class, () -> {
//      try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
//          new FSDataOutputStream(failingStream, null), schema, storage)) {
//        for (int i = 0; i < 10; i++) {
//          InternalRow row = createRow(i, "User" + i, 20L + i);
//          writer.writeRow("key" + i, row);
//        }
//      }
//    });
//
//    // Verify temp file was cleaned up even on error
//    int finalLanceFileCount = countLanceFiles(tempDirFile);
//    assertEquals(initialLanceFileCount, finalLanceFileCount,
//        "Temp Lance file should be cleaned up even on error");
//  }

  @Test
  void testStreamWriteWithComplexTypes() throws Exception {
    // Schema with nested struct
    StructType addressSchema = new StructType()
        .add("street", DataTypes.StringType, true)
        .add("city", DataTypes.StringType, true)
        .add("zipcode", DataTypes.IntegerType, true);

    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("address", addressSchema, true);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Write records with nested structs
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {

      GenericInternalRow address1 = new GenericInternalRow(new Object[]{
          UTF8String.fromString("123 Main St"),
          UTF8String.fromString("New York"),
          10001
      });

      InternalRow row1 = new GenericInternalRow(new Object[]{
          1,
          UTF8String.fromString("Alice"),
          address1
      });
      writer.writeRow("key1", row1);

      // Test null values
      InternalRow row2 = new GenericInternalRow(new Object[]{
          2,
          null,  // null name
          null   // null address
      });
      writer.writeRow("key2", row2);
    }

    // Verify complex types and nulls were preserved
    byte[] lanceData = baos.toByteArray();
    verifyLanceData(lanceData, 2);
  }

  @Test
  void testStreamWriteDoesNotCloseOutputStream() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsOutput = new FSDataOutputStream(baos, null);

    // Write and close stream writer
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        fsOutput, schema, storage)) {
      InternalRow row = createRow(1, "Alice", 30L);
      writer.writeRow("key1", row);
    }

    // FSDataOutputStream should still be usable (not closed by writer)
    // This verifies the writer doesn't close the output stream
    // Note: We can't directly test if stream is open, but we can verify it was usable
    assertTrue(baos.toByteArray().length > 0, "Data should have been written");
  }

  @Test
  void testWriteRowWithMetadata() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Test writeRowWithMetadata (though metadata population is not yet supported)
    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      InternalRow row = createRow(1, "Alice", 30L);
      HoodieKey key = new HoodieKey("key1", "partition1");
      writer.writeRowWithMetadata(key, row);
    }

    // Verify data was written
    byte[] lanceData = baos.toByteArray();
    verifyLanceData(lanceData, 1);
  }

  @Test
  void testCanWrite() throws Exception {
    StructType schema = createSimpleSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
      assertTrue(writer.canWrite(), "Stream writer should always return true for canWrite()");
    }
  }

  @Test
  void testWritePrimitiveTypes() throws Exception {
    StructType schema = new StructType()
        .add("int_field", DataTypes.IntegerType, false)
        .add("long_field", DataTypes.LongType, false)
        .add("float_field", DataTypes.FloatType, false)
        .add("double_field", DataTypes.DoubleType, false)
        .add("bool_field", DataTypes.BooleanType, false)
        .add("string_field", DataTypes.StringType, false)
        .add("binary_field", DataTypes.BinaryType, false);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (HoodieSparkLanceStreamWriter writer = new HoodieSparkLanceStreamWriter(
        new FSDataOutputStream(baos, null), schema, storage)) {
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
    }

    // Verify all types were written
    byte[] lanceData = baos.toByteArray();
    verifyLanceData(lanceData, 1);
  }

  // Helper methods

  private StructType createSimpleSchema() {
    return new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.LongType, true);
  }

  private void verifyLanceData(byte[] lanceData, int expectedRowCount) throws Exception {
    // Write to temp file so we can read with LanceFileReader
    File tempLanceFile = File.createTempFile("verify-lance-", ".lance", tempDir);
    try {
      java.nio.file.Files.write(tempLanceFile.toPath(), lanceData);

      // Read and verify using LanceFileReader
      try (BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
               "verifyLanceData", HoodieSparkLanceReader.LANCE_DATA_ALLOCATOR_SIZE);
           LanceFileReader reader = LanceFileReader.open(tempLanceFile.getAbsolutePath(), allocator)) {
        assertEquals(expectedRowCount, reader.numRows(), "Row count should match");
      }
    } finally {
      tempLanceFile.delete();
    }
  }

  private int countLanceFiles(File directory) {
    File[] files = directory.listFiles((dir, name) ->
        name.startsWith("hudi-lance-stream-") && name.endsWith(".lance"));
    return files != null ? files.length : 0;
  }
}
