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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.io.storage.parquet.ParquetFileMetadataLoader;
import org.apache.hudi.io.storage.parquet.RowGroup;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieParquetKeyedLookupReader}.
 */
public class TestHoodieParquetKeyedLookupReader extends HoodieCommonTestHarness {
  private final String fileName = genParquetFileName();

  private static String genParquetFileName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public void writeData(List<GenericRecord> records, boolean enabled) throws IOException {
    final ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(
            new Path(basePath, fileName),
            new Configuration()))
        .withSchema(AVRO_SCHEMA)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withBloomFilterEnabled(enabled)
        .withPageSize(1024 * 1024)
        .withRowGroupSize(10 * 1024 * 1024)
        .enableDictionaryEncoding()
        .build();

    for (GenericRecord record : records) {
      writer.write(record);
    }
    writer.close();
  }

  private List<GenericRecord> generateData(int numRecords) {
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(dataGen.generateGenericRecord(
          String.valueOf(i), "0", "rider-" + i, "driver-" + i, 1L));
    }
    return records;
  }

  @BeforeEach
  public void setUp() throws IOException, ParseException {
    initPath();
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() {
    cleanupTestDataGenerator();
  }

  @Test
  public void testBloomFilterEnabled() throws IOException {
    writeData(generateData(10_000), true);

    ParquetFileMetadataLoader loader = new ParquetFileMetadataLoader(
        HadoopInputFile.fromPath(new Path(basePath, fileName), new Configuration()),
        (new ParquetFileMetadataLoader.Options.Builder().enableLoadBloomFilters()).build());
    loader.load();

    List<RowGroup> rowGroups = loader.getRowGroups();
    assertEquals(1, rowGroups.size());

    // Both keys are valid.
    List<String> testKeys = Arrays.asList("rider-0", "riders-1");
    boolean shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertFalse(shouldSkip);

    // One key is valid.
    testKeys = Arrays.asList("rider-10001", "riders-10002");
    shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertTrue(shouldSkip);

    // Both keys are invalid.
    testKeys = Arrays.asList("rider-1001", "riders-10002");
    shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertFalse(shouldSkip);
  }

  @Test
  public void testBloomFilterDisabled() throws IOException {
    writeData(generateData(10_000), true);

    ParquetFileMetadataLoader loader = new ParquetFileMetadataLoader(
        HadoopInputFile.fromPath(new Path(basePath, fileName), new Configuration()),
        (new ParquetFileMetadataLoader.Options.Builder()).build());
    loader.load();

    List<RowGroup> rowGroups = loader.getRowGroups();
    assertEquals(1, rowGroups.size());

    // Both keys are valid.
    List<String> testKeys = Arrays.asList("rider-0", "riders-1");
    boolean shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertFalse(shouldSkip);

    // One key is valid.
    testKeys = Arrays.asList("rider-10001", "riders-10002");
    shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertFalse(shouldSkip);

    // Both keys are invalid.
    testKeys = Arrays.asList("rider-1001", "riders-10002");
    shouldSkip = HoodieParquetKeyedLookupReader.shouldSkip(
        rowGroups.get(0), 4, new LinkedList<>(testKeys));
    assertFalse(shouldSkip);
  }
}
