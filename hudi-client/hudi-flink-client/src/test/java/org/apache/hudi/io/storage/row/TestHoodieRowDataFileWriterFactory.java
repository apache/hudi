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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link HoodieRowDataFileWriterFactory}.
 */
class TestHoodieRowDataFileWriterFactory {

  @TempDir
  Path tmpDir;

  @Test
  void testNativeLogBloomPolicy() throws Exception {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"test\",\"fields\":["
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"}]}");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tmpDir.toString()).build();
    config.setValue("hoodie.index.type", "GLOBAL_BLOOM");
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(storage);
    for (String fileName : Arrays.asList("file-1_1-0-1_001.parquet",
        "file-1_1-0-1_002_1.log.parquet", "file-1_1-0-1_002_1.deletes.parquet")) {
      StoragePath path = new StoragePath(tmpDir.toString(), fileName);
      try (HoodieRowDataParquetWriter writer = (HoodieRowDataParquetWriter) factory.newParquetFileWriter(
          "002", path, config, schema, new LocalTaskContextSupplier())) {
        writer.writeRow("key-1", GenericRowData.of(StringData.fromString("key-1")));
      }
      Map<String, String> footer = ParquetUtils.readMetadata(storage, path).getFileMetaData().getKeyValueMetaData();
      boolean expectedBloom = fileName.equals("file-1_1-0-1_001.parquet");
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), fileName);
      assertEquals(expectedBloom, footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), fileName);
    }
  }
}
