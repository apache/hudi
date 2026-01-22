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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLanceDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockContentLocation;
import org.apache.hudi.common.util.LanceUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SafeProjection;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.io.storage.LanceTestUtils.createRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIfSystemProperty(named = "lance.skip.tests", matches = "true")
class TestHoodieLanceDataBlock extends SparkClientFunctionalTestHarness {

  @TempDir
  File tempDir;

  private static final String RECORD_KEY_FIELD = HoodieRecord.RECORD_KEY_METADATA_FIELD;

  /**
   * Tests basic serialization and deserialization of Lance data blocks with byte[] content.
   */
  @Test
  void testSerializeAndDeserializeWithContent() throws IOException {
    StructType schema = createTestSchema();
    HoodieSchema hoodieSchema = convertToHoodieSchema(schema);

    List<InternalRow> rows = createTestRows();
    byte[] content = serializeRecordsToLanceBlock(rows, schema, hoodieSchema);
    
    Map<HeaderMetadataType, String> header = Collections.singletonMap(HeaderMetadataType.SCHEMA, hoodieSchema.toString());
    HoodieLogBlockContentLocation mockLocation = createMockContentLocation();

    HoodieLanceDataBlock dataBlock = new HoodieLanceDataBlock(
        () -> null,
        Option.of(content),
        false,
        mockLocation,
        Option.of(hoodieSchema),
        header,
        Collections.emptyMap(),
        RECORD_KEY_FIELD
    );

    // Deserialize and verify records can be read
    deserializeAndVerifyRecords(dataBlock, schema, rows);
  }

  /**
   * Tests deserialization with input stream (lazy reading).
   */
  @Test
  void testDeserializeWithInputStream() throws IOException {
    StructType schema = createTestSchema();
    HoodieSchema hoodieSchema = convertToHoodieSchema(schema);

    List<InternalRow> rows = createTestRows();
    byte[] content = serializeRecordsToLanceBlock(rows, schema, hoodieSchema);

    // Test seekable input stream
    SeekableDataInputStream inputStream = new ByteArraySeekableDataInputStream(
        new ByteBufferBackedInputStream(content)
    );

    Map<HeaderMetadataType, String> header = Collections.singletonMap(HeaderMetadataType.SCHEMA, hoodieSchema.toString());

    HoodieLogBlockContentLocation contentLocation = new HoodieLogBlockContentLocation(
        hoodieStorage(), null, 0, content.length, content.length);

    HoodieLanceDataBlock dataBlock = new HoodieLanceDataBlock(
        () -> inputStream,
        Option.empty(),
        true,
        contentLocation,
        Option.of(hoodieSchema),
        header,
        Collections.emptyMap(),
        RECORD_KEY_FIELD
    );

    deserializeAndVerifyRecords(dataBlock, schema, rows);
  }

  /**
   * Tests that temporary files are created and cleaned up properly.
   */
  @Test
  void testTempFileCleanup() throws IOException {
    StructType schema = createTestSchema();
    HoodieSchema hoodieSchema = convertToHoodieSchema(schema);

    List<InternalRow> rows = createTestRows();
    byte[] content = serializeRecordsToLanceBlock(rows, schema, hoodieSchema);

    Map<HeaderMetadataType, String> header = Collections.singletonMap(HeaderMetadataType.SCHEMA, hoodieSchema.toString());
    HoodieLogBlockContentLocation mockLocation = createMockContentLocation();
    HoodieLanceDataBlock dataBlock = new HoodieLanceDataBlock(
        () -> null,
        Option.of(content),
        false,
        mockLocation,
        Option.of(hoodieSchema),
        header,
        Collections.emptyMap(),
        RECORD_KEY_FIELD
    );

    // Read records and verify temp file exists during iteration
    try (ClosableIterator<HoodieRecord<Object>> iterator =
             dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.SPARK)) {
      assertTrue(iterator.hasNext(), "Iterator should have records");

      // Temp file should exist while iterator is open
      assertTrue(hasLanceFiles());
    }
    // After closing iterator, temp file should be cleaned up
    assertFalse(hasLanceFiles());
  }

  /**
   * Tests reading Lance data block with schema projection.
   */
  @Test
  void testSchemaProjection() throws IOException {
    StructType fullSchema = createTestSchema();
    HoodieSchema fullHoodieSchema = convertToHoodieSchema(fullSchema);

    // Create test records
    List<InternalRow> rows = createTestRows();
    byte[] content = serializeRecordsToLanceBlock(rows, fullSchema, fullHoodieSchema);

    // Create projected schema (only name and age fields)
    StructType projectedSchema = new StructType()
        .add("name", DataTypes.StringType, true)
        .add("age", DataTypes.IntegerType, true);
    HoodieSchema projectedHoodieSchema = convertToHoodieSchema(projectedSchema);
    Map<HeaderMetadataType, String> header = Collections.singletonMap(HeaderMetadataType.SCHEMA, fullHoodieSchema.toString());
    HoodieLogBlockContentLocation mockLocation = createMockContentLocation();
    HoodieLanceDataBlock dataBlock = new HoodieLanceDataBlock(
        () -> null,
        Option.of(content),
        false,
        mockLocation,
        Option.of(projectedHoodieSchema),
        header,
        Collections.emptyMap(),
        RECORD_KEY_FIELD
    );

    List<InternalRow> expectedRows = new ArrayList<>(rows.size());
    expectedRows.add(createRow("Alice", 30));
    expectedRows.add(createRow("Bob", 25));
    expectedRows.add(createRow("Charlie", 35));
    // Read with projection
    deserializeAndVerifyRecords(dataBlock, projectedSchema, expectedRows);
  }

  /**
   * Tests that block type is correctly identified.
   */
  @Test
  void testGetBlockType() {
    Map<HeaderMetadataType, String> header = Collections.singletonMap(HeaderMetadataType.SCHEMA, convertToHoodieSchema(createTestSchema()).toString());
    HoodieLogBlockContentLocation mockLocation = createMockContentLocation();
    HoodieLanceDataBlock dataBlock = new HoodieLanceDataBlock(
        () -> null,
        Option.empty(),
        false,
        mockLocation,
        Option.empty(),
        header,
        Collections.emptyMap(),
        RECORD_KEY_FIELD
    );

    assertEquals(HoodieDataBlock.HoodieLogBlockType.LANCE_DATA_BLOCK, dataBlock.getBlockType());
  }

  // Helper methods

  private StructType createTestSchema() {
    return new StructType()
        .add(new StructField("name", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("age", DataTypes.IntegerType, true, Metadata.empty()))
        .add(new StructField("score", DataTypes.DoubleType, true, Metadata.empty()));
  }

  private List<InternalRow> createTestRows() {
    List<InternalRow> rows = new ArrayList<>(3);
    rows.add(createRow("Alice", 30, 95.5));
    rows.add(createRow("Bob", 25, 87.3));
    rows.add(createRow("Charlie", 35, 92.1));
    return rows;
  }

  private HoodieSchema convertToHoodieSchema(StructType sparkSchema) {
    return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(sparkSchema, "testSchema");
  }

  private byte[] serializeRecordsToLanceBlock(List<InternalRow> rows, StructType schema, HoodieSchema hoodieSchema)
      throws IOException {
    List<HoodieRecord> records = new ArrayList<>();
    for (InternalRow row : rows) {
        records.add(new HoodieSparkRecord(row, schema));
    }
    // Use LanceUtils to serialize rows to Lance format
    ByteArrayOutputStream outputStream = new LanceUtils().serializeRecordsToLogBlock(hoodieStorage(), records, hoodieSchema, hoodieSchema, "name", Collections.emptyMap());
    return outputStream.toByteArray();
  }

  private HoodieLogBlockContentLocation createMockContentLocation() {
    StoragePath mockPath = new StoragePath(tempDir.getAbsolutePath(), "test.log");

    return new HoodieLogBlockContentLocation(
        hoodieStorage(),
        new HoodieLogFile(mockPath),
        0,
        1024,
        1024
    );
  }

  private boolean hasLanceFiles() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File[] lanceFiles = tmpDir.listFiles((dir, name) ->
        name.startsWith("lance-log-block-") && name.endsWith(".lance"));
    return lanceFiles != null && lanceFiles.length > 0;
  }

  private void deserializeAndVerifyRecords(HoodieLanceDataBlock dataBlock, StructType schema, List<InternalRow> rows) {
    try (ClosableIterator<HoodieRecord<InternalRow>> iterator = dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.SPARK)) {
      List<HoodieRecord<InternalRow>> records = new ArrayList<>();
      iterator.forEachRemaining(records::add);
      validateRowEquality(schema, rows, records);
    }
  }

  private void validateRowEquality(StructType schema, List<InternalRow> expectedRows, List<HoodieRecord<InternalRow>> actualRecords) {
    assertEquals(expectedRows.size(), actualRecords.size(), "Row count should match");
    for (int i = 0; i < expectedRows.size(); i++) {
      InternalRow expected = expectedRows.get(i);
      InternalRow actual = SafeProjection.create(schema).apply(actualRecords.get(i).getData());
      assertEquals(expected, actual);
    }
  }
}
