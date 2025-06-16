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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieAppendHandle}.
 */
public class TestAppendHandle extends BaseTestHandle {

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testAppendHandleRLIStats(boolean populateMetaFields) {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withPopulateMetaFields(populateMetaFields)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(true).withStreamingWriteEnabled(true).build())
        .build();

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    // create parquet file
    createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    // generate update records
    instantTime = "001";
    List<HoodieRecord> records = dataGenerator.generateUniqueUpdates(instantTime, 50);
    HoodieAppendHandle handle = new HoodieAppendHandle(config, instantTime, table, partitionPath, fileId, records.iterator(), new LocalTaskContextSupplier());
    Map<String, HoodieRecord> recordMap = new HashMap<>();
    for (int i = 0; i < records.size(); i++) {
      recordMap.put(String.valueOf(i), records.get(i));
    }
    // write the update records
    handle.write(recordMap);
    WriteStatus writeStatus = handle.writeStatus;
    handle.close();

    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());
    // validate write status has all record delegates
    if (populateMetaFields) {
      assertEquals(records.size(), writeStatus.getWrittenRecordDelegates().size());
      for (HoodieRecordDelegate recordDelegate : writeStatus.getWrittenRecordDelegates()) {
        assertTrue(recordDelegate.getNewLocation().isPresent());
        assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
        assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
      }
    }
  }
}
