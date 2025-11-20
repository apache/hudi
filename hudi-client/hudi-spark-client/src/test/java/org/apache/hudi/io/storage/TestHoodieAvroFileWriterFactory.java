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
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.io.hadoop.HoodieAvroHFileWriter;
import org.apache.hudi.io.hadoop.HoodieAvroOrcWriter;
import org.apache.hudi.io.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;

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
        parquetPath, table.getStorage(), cfg.getStorageConfig(), HoodieSchema.fromAvroSchema(HoodieTestDataGenerator.AVRO_SCHEMA), supplier, HoodieRecordType.AVRO);
    assertTrue(parquetWriter instanceof HoodieAvroParquetWriter);
    parquetWriter.close();

    // hfile format.
    final StoragePath hfilePath = new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000.hfile");
    HoodieFileWriter hfileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table.getStorage(), cfg.getStorageConfig(), HoodieSchema.fromAvroSchema(HoodieTestDataGenerator.AVRO_SCHEMA), supplier, HoodieRecordType.AVRO);
    assertTrue(hfileWriter instanceof HoodieAvroHFileWriter);
    hfileWriter.close();

    // orc file format.
    final StoragePath orcPath = new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000.orc");
    HoodieFileWriter orcFileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        orcPath, table.getStorage(), cfg.getStorageConfig(), HoodieSchema.fromAvroSchema(HoodieTestDataGenerator.AVRO_SCHEMA), supplier, HoodieRecordType.AVRO);
    assertTrue(orcFileWriter instanceof HoodieAvroOrcWriter);
    orcFileWriter.close();

    // other file format exception.
    final StoragePath logPath = new StoragePath(
        basePath + "/partition/path/f.b51192a8-574b-4a85-b246-bcfec03ac8bf_100.log.2_1-0-1");
    final Throwable thrown = assertThrows(UnsupportedOperationException.class, () -> {
      HoodieFileWriterFactory.getFileWriter(instantTime, logPath,
          table.getStorage(), cfg.getStorageConfig(), HoodieSchema.fromAvroSchema(HoodieTestDataGenerator.AVRO_SCHEMA), supplier, HoodieRecordType.AVRO);
    }, "should fail since log storage writer is not supported yet.");
    assertTrue(thrown.getMessage().contains("format not supported yet."));
  }

  @Test
  public void testEnableBloomFilter() {
    HoodieWriteConfig config = getConfig(IndexType.BLOOM);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(true, config));
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(false, config));

    config = getConfig(IndexType.SIMPLE);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(true, config));

    config = getConfig(IndexType.SIMPLE);
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(true, config));

    config = getConfigBuilder(IndexType.BLOOM)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(false).build()).build();
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(true, config));

    config = getConfigBuilder(IndexType.SIMPLE)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(true).build()).build();
    assertTrue(HoodieFileWriterFactory.enableBloomFilter(true, config));

    config = getConfigBuilder(IndexType.SIMPLE)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetBloomFilterEnable(false).build()).build();
    assertFalse(HoodieFileWriterFactory.enableBloomFilter(true, config));
  }
}
