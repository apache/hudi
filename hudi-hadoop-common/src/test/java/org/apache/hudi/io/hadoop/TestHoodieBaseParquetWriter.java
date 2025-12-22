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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBaseParquetWriter {

  private static class MockHoodieParquetWriter extends HoodieBaseParquetWriter<IndexedRecord> {

    long writtenRecordCount = 0L;
    long currentDataSize = 0L;

    public MockHoodieParquetWriter(StoragePath file,
                                   HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig)
        throws IOException {
      super(file, (HoodieParquetConfig) parquetConfig);
    }

    @Override
    public long getDataSize() {
      return currentDataSize;
    }

    @Override
    public long getWrittenRecordCount() {
      return writtenRecordCount;
    }

    public void setWrittenRecordCount(long writtenCount) {
      this.writtenRecordCount = writtenCount;
    }

    public void setCurrentDataSize(long currentDataSize) {
      this.currentDataSize = currentDataSize;
    }
  }

  @TempDir
  public java.nio.file.Path tempDir;

  @Test
  public void testCanWrite() throws IOException {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();

    Schema schema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema),
        schema, Option.of(filter), new Properties());

    long maxFileSize = 2 * 1024 * 1024;
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig =
        new HoodieParquetConfig<>(writeSupport, CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE, maxFileSize, conf, 0, true);

    StoragePath filePath = new StoragePath(
        new StoragePath(tempDir.toUri()), "test_fileSize.parquet");
    try (MockHoodieParquetWriter writer = new MockHoodieParquetWriter(filePath, parquetConfig)) {
      // doesn't start write, should return true
      assertTrue(writer.canWrite());
      // recordCountForNextSizeCheck should be DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK
      assertEquals(writer.getRecordCountForNextSizeCheck(), DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK);
      // 10 bytes per record
      writer.setCurrentDataSize(1000);
      writer.setWrittenRecordCount(writer.getRecordCountForNextSizeCheck());

      assertTrue(writer.canWrite());
      // Should check it with more DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK records
      assertEquals(writer.getRecordCountForNextSizeCheck(), writer.getWrittenRecordCount() + DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK);

      // 80 bytes per record
      writer.setCurrentDataSize(808000);
      writer.setWrittenRecordCount(writer.getRecordCountForNextSizeCheck());
      assertTrue(writer.canWrite());
      // Should check it half way, not DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK
      long avgRecordSize = writer.getDataSize() / writer.getWrittenRecordCount();
      long recordsDelta = (maxFileSize / avgRecordSize - writer.getWrittenRecordCount()) / 2;
      assertEquals(writer.getRecordCountForNextSizeCheck(), writer.getWrittenRecordCount() + recordsDelta);

      writer.setCurrentDataSize(maxFileSize);
      writer.setWrittenRecordCount(writer.getRecordCountForNextSizeCheck());
      assertFalse(writer.canWrite(),
          "The writer stops write new records while the file doesn't reach the max file size limit");
    }
  }
}
