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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieLogBlockMetadataScanner {

  private static final int BUFFER_SIZE = 4096;

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  void testCdcLogFilesAreSkipped() throws IOException, URISyntaxException, InterruptedException {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tempDir.toString(), HoodieTableType.MERGE_ON_READ);
    HoodieStorage storage = metaClient.getStorage();
    StoragePath partitionPath = new StoragePath(tempDir.toString(), "partition");
    storage.createDirectory(partitionPath);

    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 10);

    HoodieLogFile dataLogFile = writeLogFile(storage, partitionPath, null,
        HoodieCommonTestHarness.getDataBlock(HoodieLogBlockType.AVRO_DATA_BLOCK, records, header));
    HoodieLogFile cdcLogFile = writeLogFile(storage, partitionPath, HoodieCDCUtils.CDC_LOGFILE_SUFFIX,
        HoodieCommonTestHarness.getDataBlock(HoodieLogBlockType.CDC_DATA_BLOCK, records, header));
    assertFalse(dataLogFile.isCDC());
    assertTrue(cdcLogFile.isCDC());

    HoodieLogBlockMetadataScanner scanner = new HoodieLogBlockMetadataScanner(
        metaClient, Arrays.asList(dataLogFile, cdcLogFile), BUFFER_SIZE, "100", Option.empty());

    assertEquals(1, scanner.getCurrentInstantLogBlocks().size());
    assertEquals(HoodieLogBlockType.AVRO_DATA_BLOCK, scanner.getCurrentInstantLogBlocks().peek().getBlockType());
  }

  private static HoodieLogFile writeLogFile(HoodieStorage storage, StoragePath partitionPath, String suffix,
                                            HoodieLogBlock block) throws IOException, InterruptedException {
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withSuffix(suffix)
        .withLogFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build()) {
      writer.appendBlock(block);
      return writer.getLogFile();
    }
  }
}
