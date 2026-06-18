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
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBaseParquetWriter {

  @Setter
  private static class MockHoodieParquetWriter extends HoodieBaseParquetWriter<IndexedRecord> {

    @Getter
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
  }

  @TempDir
  public java.nio.file.Path tempDir;

  @Test
  public void testCanWrite() throws IOException {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();

    HoodieSchema schema = HoodieTestDataGenerator.HOODIE_SCHEMA;
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()),
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

  @Test
  public void testRejectAlreadyShreddedVariantInput() {
    // A record read from an already-shredded base file (compaction/clustering) arrives with a
    // populated typed_value. The Avro write support cannot reconstruct the unshredded variant yet,
    // so it must fail fast rather than silently drop the payload. Guard tracked in
    // https://github.com/apache/hudi/issues/18931.
    HoodieSchema recordSchema = HoodieSchema.createRecord("guardTest", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("v", HoodieSchema.createVariant())));

    // Shredding disabled so no VariantShreddingProvider is required; the variant field is still tracked.
    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(recordSchema, props);
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(effectiveSchema.toAvroSchema()), recordSchema, Option.empty(), props);

    // Input whose variant column is already shredded (carries typed_value).
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    Schema shreddedVariantAvro =
        HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields).getAvroSchema();

    Schema inputAvro = Schema.createRecord("guardInput", null, null, false);
    inputAvro.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.INT), null, (Object) null),
        new Schema.Field("v", shreddedVariantAvro, null, (Object) null)));
    GenericRecord variantValue = new GenericData.Record(shreddedVariantAvro);
    variantValue.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(new byte[] {1}));
    GenericRecord input = new GenericData.Record(inputAvro);
    input.put("id", 1);
    input.put("v", variantValue);

    HoodieException ex = assertThrows(HoodieException.class, () -> writeSupport.write(input));
    assertTrue(ex.getMessage().contains("already-shredded") && ex.getMessage().contains("18931"),
        "Expected the read-then-reshred guard message, got: " + ex.getMessage());
  }
}
