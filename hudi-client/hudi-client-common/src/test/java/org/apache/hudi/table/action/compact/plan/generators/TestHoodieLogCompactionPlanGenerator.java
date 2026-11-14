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

package org.apache.hudi.table.action.compact.plan.generators;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieLogCompactionPlanGenerator extends HoodieCommonTestHarness {

  private static final String PARTITION = "partition";
  private static final String FILE_ID = "test-fileid1";

  @Test
  void testCdcLogFilesDoNotCountTowardsTheLogFileThreshold() throws Exception {
    HoodieLogCompactionPlanGenerator generator = newGenerator(2);
    FileSlice fileSlice = new FileSlice(PARTITION, "100", FILE_ID);
    fileSlice.addLogFile(writeLogFile(HoodieCDCUtils.CDC_LOGFILE_SUFFIX, "101"));
    fileSlice.addLogFile(writeLogFile(HoodieCDCUtils.CDC_LOGFILE_SUFFIX, "102"));
    fileSlice.addLogFile(writeLogFile(HoodieCDCUtils.CDC_LOGFILE_SUFFIX, "103"));

    assertFalse(generator.isFileSliceEligibleForLogCompaction(fileSlice, "103", Option.empty()));
  }

  @Test
  void testLogFilesCountTowardsTheLogFileThreshold() throws Exception {
    HoodieLogCompactionPlanGenerator generator = newGenerator(2);
    FileSlice fileSlice = new FileSlice(PARTITION, "100", FILE_ID);
    fileSlice.addLogFile(writeLogFile(null, "101"));
    fileSlice.addLogFile(writeLogFile(null, "102"));
    fileSlice.addLogFile(writeLogFile(HoodieCDCUtils.CDC_LOGFILE_SUFFIX, "102"));

    assertTrue(generator.isFileSliceEligibleForLogCompaction(fileSlice, "102", Option.empty()));
  }

  private HoodieLogCompactionPlanGenerator newGenerator(int logCompactionBlocksThreshold) throws IOException {
    initMetaClient();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withLogCompactionBlocksThreshold(logCompactionBlocksThreshold).build())
        .build();
    HoodieEngineContext context = new HoodieLocalEngineContext(getStorageConf());
    HoodieTable table = mock(HoodieTable.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getConfig()).thenReturn(config);
    return new HoodieLogCompactionPlanGenerator(table, context, config, null);
  }

  private HoodieLogFile writeLogFile(String suffix, String instantTime) throws IOException, URISyntaxException, InterruptedException {
    StoragePath partitionPath = new StoragePath(basePath, PARTITION);
    metaClient.getStorage().createDirectory(partitionPath);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, SchemaTestUtil.getSimpleSchema().toString());
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 5);
    HoodieLogBlockType blockType = suffix == null ? HoodieLogBlockType.AVRO_DATA_BLOCK : HoodieLogBlockType.CDC_DATA_BLOCK;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withSuffix(suffix)
        .withLogFileId(FILE_ID)
        .withInstantTime(instantTime)
        .withStorage(metaClient.getStorage())
        .build()) {
      writer.appendBlock(getDataBlock(blockType, records, header));
      return writer.getLogFile();
    }
  }
}
