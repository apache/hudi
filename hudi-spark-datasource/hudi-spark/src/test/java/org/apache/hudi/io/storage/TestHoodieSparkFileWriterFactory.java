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

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link HoodieSparkFileWriterFactory}.
 */
class TestHoodieSparkFileWriterFactory {

  @TempDir
  Path tmpDir;

  @ParameterizedTest
  @ValueSource(strings = {"BLOOM", "SIMPLE"})
  void testNativeLogBloomPolicy(String indexType) throws Exception {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"test\",\"fields\":["
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"}]}");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tmpDir.toString()).withSchema(schema.toString())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetBloomFilterEnable(false).build()).build();
    config.setValue("hoodie.index.type", indexType);
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString("key-1")});
    for (String fileName : Arrays.asList("file-1_1-0-1_001.parquet",
        "file-1_1-0-1_002_1.log.parquet", "file-1_1-0-1_002_1.deletes.parquet")) {
      StoragePath path = new StoragePath(tmpDir.toString(), fileName);
      try (HoodieFileWriter writer = HoodieFileWriterFactory.getFileWriter(
          "002", path, storage, config, schema, new LocalTaskContextSupplier(), HoodieRecordType.SPARK)) {
        writer.writeRow("key-1", row);
      }
      Map<String, String> footer = ParquetUtils.readMetadata(storage, path).getFileMetaData().getKeyValueMetaData();
      boolean expectedBloom = fileName.equals("file-1_1-0-1_001.parquet") && indexType.contains("BLOOM");
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), fileName);
    }
  }
}
