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

package org.apache.hudi.io.storage.parquet;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.io.HeapSeekableInputStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;

/**
 * Tests {@link ParquetMetadataFileReader}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestParquetMetadataFileReader extends HoodieCommonTestHarness {
  private static final long BLOOM_FILTER_HEADER_SIZE_GUESS = 1024; // 1KB
  private static final long BLOOM_FILTER_SIZE_GUESS = 1024 * 1024; // 1 MB
  private ParquetMetadataFileReader fileReader;
  private final String fileName = genParquetFileName();

  private static String genParquetFileName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public void writeData(List<GenericRecord> records) throws IOException {
    final ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(
            new Path(basePath, fileName),
            new Configuration()))
        .withSchema(AVRO_SCHEMA)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withBloomFilterEnabled(true)
        .withPageSize(1024 * 1024)
        .withRowGroupSize(10 * 1024 * 1024)
        .enableDictionaryEncoding()
        .build();

    for (GenericRecord record : records) {
      writer.write(record);
    }
    writer.close();
  }

  @BeforeEach
  public void setUp() throws IOException, ParseException {
    initPath();
    initTestDataGenerator();
    writeData(dataGen.generateGenericRecords(10000));
  }

  @AfterEach
  public void tearDown() {
    cleanupTestDataGenerator();
  }

  @Test
  public void testReadColumnIndex() throws IOException {
    try (ParquetMetadataFileReader fileReader = new ParquetMetadataFileReader(
        HadoopInputFile.fromPath(new Path(basePath, fileName), new Configuration()),
        ParquetReadOptions.builder().build())) {
      List<BlockMetaData> rowGroups = fileReader.getRowGroups();
      for (BlockMetaData rowGroup : rowGroups) {
        List<ColumnChunkMetaData> chunks = rowGroup.getColumns();
        for (ColumnChunkMetaData chunk : chunks) {
          IndexReference ref = chunk.getColumnIndexReference();
          if (ref != null) {
            ByteBuffer metadataCache = ByteBuffer.allocate((int)(ref.getLength()));
            fileReader.setStreamPosition(ref.getOffset());
            fileReader.blockRead(metadataCache);
            metadataCache.flip();
            try (SeekableInputStream indexBytesStream = HeapSeekableInputStream.wrap(metadataCache.array())) {
              ColumnIndex columnIndex = fileReader.readColumnIndex(
                  fileReader,
                  indexBytesStream,
                  ref.getOffset(),
                  ref.getOffset() + ref.getLength(),
                  chunk);
              Assertions.assertNotNull((Object) columnIndex);
              Assertions.assertEquals(columnIndex.getMinValues().size(), columnIndex.getMaxValues().size());
            }
          }
        }
      }
    }
  }

  @Test
  public void testReadOffsetIndex() throws IOException {
    try (ParquetMetadataFileReader fileReader = new ParquetMetadataFileReader(
        HadoopInputFile.fromPath(new Path(basePath, fileName), new Configuration()),
        ParquetReadOptions.builder().build())) {
      List<BlockMetaData> rowGroups = fileReader.getRowGroups();
      for (BlockMetaData rowGroup : rowGroups) {
        List<ColumnChunkMetaData> chunks = rowGroup.getColumns();
        for (ColumnChunkMetaData chunk : chunks) {
          IndexReference ref = chunk.getOffsetIndexReference();
          if (ref != null) {
            ByteBuffer metadataCache = ByteBuffer.allocate((int)(ref.getLength()));
            fileReader.setStreamPosition(ref.getOffset());
            fileReader.blockRead(metadataCache);
            metadataCache.flip();
            try (SeekableInputStream indexBytesStream = HeapSeekableInputStream.wrap(metadataCache.array())) {
              OffsetIndex offsetIndex = fileReader.readOffsetIndex(
                  fileReader,
                  indexBytesStream,
                  ref.getOffset(),
                  ref.getOffset() + ref.getLength(),
                  chunk);
              Assertions.assertNotNull((Object) offsetIndex);
              Assertions.assertTrue(offsetIndex.getPageCount() > 0);
            }
          }
        }
      }
    }
  }

  @Test
  public void testReadBloomFilter() throws IOException {
    try (ParquetMetadataFileReader fileReader = new ParquetMetadataFileReader(
        HadoopInputFile.fromPath(new Path(basePath, fileName), new Configuration()),
        ParquetReadOptions.builder().build())) {
      List<BlockMetaData> rowGroups = fileReader.getRowGroups();
      for (BlockMetaData rowGroup : rowGroups) {
        List<ColumnChunkMetaData> chunks = rowGroup.getColumns();
        for (ColumnChunkMetaData chunk : chunks) {
          long bloomFilterOffset = chunk.getBloomFilterOffset();
          if (bloomFilterOffset >= 0) {
            ByteBuffer metadataCache = ByteBuffer.allocate((int)(BLOOM_FILTER_HEADER_SIZE_GUESS + BLOOM_FILTER_SIZE_GUESS));
            fileReader.setStreamPosition(bloomFilterOffset);
            fileReader.blockRead(metadataCache);
            metadataCache.flip();
            try (SeekableInputStream indexBytesStream = HeapSeekableInputStream.wrap(metadataCache.array())) {
              BloomFilter bloomFilter = fileReader.readBloomFilter(
                  fileReader,
                  indexBytesStream,
                  bloomFilterOffset,
                  bloomFilterOffset + BLOOM_FILTER_HEADER_SIZE_GUESS + BLOOM_FILTER_SIZE_GUESS,
                  chunk);
              Assertions.assertNotNull((Object) bloomFilter);
              Assertions.assertEquals("xxhash", bloomFilter.getHashStrategy().toString());
              Assertions.assertEquals("block", bloomFilter.getAlgorithm().toString());
              BlockSplitBloomFilter bloomFilterObject = (BlockSplitBloomFilter) bloomFilter;
              Assertions.assertTrue(bloomFilterObject.getBitsetSize() > 0);
            }
          } else {
            Assertions.fail("Failed to generate bloom filter index for testing");
          }
        }
      }
    }
  }
}
