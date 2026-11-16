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
import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.io.storage.hadoop.HoodieAvroHFileWriter;
import org.apache.hudi.io.storage.hadoop.HoodieAvroOrcWriter;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieFileWriterFactory}.
 */
public class TestHoodieAvroFileWriterFactory extends HoodieClientTestBase {

  @Test
  public void testGetFileWriter() throws IOException {
    // parquet file format.
    final String instantTime = "100";
    final StoragePath parquetPath = new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000.parquet");
    final HoodieWriteConfig cfg = getConfig();
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();
    HoodieFileWriter parquetWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        parquetPath, table.getStorage(), cfg.getStorageConfig(), HOODIE_SCHEMA, supplier, HoodieRecordType.AVRO);
    assertTrue(parquetWriter instanceof HoodieAvroParquetWriter);
    parquetWriter.close();

    // hfile format.
    final StoragePath hfilePath = new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000.hfile");
    HoodieFileWriter hfileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table.getStorage(), cfg.getStorageConfig(), HOODIE_SCHEMA, supplier, HoodieRecordType.AVRO);
    assertTrue(hfileWriter instanceof HoodieAvroHFileWriter);
    hfileWriter.close();

    // orc file format.
    final StoragePath orcPath = new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000.orc");
    HoodieFileWriter orcFileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        orcPath, table.getStorage(), cfg.getStorageConfig(), HOODIE_SCHEMA, supplier, HoodieRecordType.AVRO);
    assertTrue(orcFileWriter instanceof HoodieAvroOrcWriter);
    orcFileWriter.close();

    // other file format exception.
    final StoragePath logPath = new StoragePath(
        basePath + "/partition/path/f.b51192a8-574b-4a85-b246-bcfec03ac8bf_100.log.2_1-0-1");
    final Throwable thrown = assertThrows(UnsupportedOperationException.class, () -> {
      HoodieFileWriterFactory.getFileWriter(instantTime, logPath,
          table.getStorage(), cfg.getStorageConfig(), HOODIE_SCHEMA, supplier, HoodieRecordType.AVRO);
    }, "should fail since log storage writer is not supported yet.");
    assertTrue(thrown.getMessage().contains("format not supported yet."));
  }

  @ParameterizedTest
  @CsvSource({"SIMPLE, true", "GLOBAL_BLOOM, false"})
  void testNativeLogBloomPolicy(String indexType, boolean baseBloomEnabled) throws Exception {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"test\",\"fields\":["
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"}]}");
    HoodieWriteConfig config = getConfigBuilder().withSchema(schema.toString())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetBloomFilterEnable(baseBloomEnabled).build()).build();
    config.setValue("hoodie.index.type", indexType);
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("_hoodie_record_key", "key-1");
    for (String fileName : Arrays.asList("file-1_1-0-1_001.parquet",
        "file-1_1-0-1_002_1.log.parquet", "file-1_1-0-1_002_1.deletes.parquet")) {
      StoragePath path = new StoragePath(basePath, fileName);
      try (HoodieFileWriter writer = HoodieFileWriterFactory.getFileWriter(
          "002", path, storage, config, schema, new LocalTaskContextSupplier(), HoodieRecordType.AVRO)) {
        writer.writeRow("key-1", record);
      }
      Map<String, String> footer = ParquetUtils.readMetadata(storage, path).getFileMetaData().getKeyValueMetaData();
      boolean expectedBloom = fileName.equals("file-1_1-0-1_001.parquet");
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), fileName);
    }
  }

  @Test
  public void testEnableBloomFilter() {
    HoodieWriteConfig config = getConfig(IndexType.BLOOM);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.NONE, config));
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.COMMIT_TIME_ONLY, config));
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.FILE_NAME_ONLY, config));
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, config));

    config = getConfig(IndexType.SIMPLE);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));

    config = getConfig(IndexType.SIMPLE);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));

    config = getConfigBuilder(IndexType.BLOOM)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(false).build()).build();
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));

    config = getConfigBuilder(IndexType.SIMPLE)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(true).build()).build();
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));

    config = getConfigBuilder(IndexType.SIMPLE)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(false).build()).build();
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(MetaFieldsMode.ALL, config));
  }
}
