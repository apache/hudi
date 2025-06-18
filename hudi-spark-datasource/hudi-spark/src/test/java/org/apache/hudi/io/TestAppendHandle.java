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

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieAppendHandle}.
 */
public class TestAppendHandle extends BaseTestHandle {

  @Test
  public void testAppendHandleRLIStats() {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
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
    assertEquals(records.size(), writeStatus.getIndexStats().getWrittenRecordDelegates().size());
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      assertTrue(recordDelegate.getNewLocation().isPresent());
      assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
      assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
    }
  }

  @Test
  public void testAppendHandleSecondaryIndexStats() throws Exception {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withStreamingWriteEnabled(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexName("sec-rider")
            .withSecondaryIndexForColumn("rider")
            .build())
        .build();

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    HoodieTableMetadataWriter metadataWriter = SparkMetadataWriterFactory.create(storageConf, config, context, table.getMetaClient().getTableConfig());
    metadataWriter.close();

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
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieSparkTable.create(config, context, metaClient);
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
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // Since the MDT is not populated during the create, the updates would be considered as new records by the Append handle
    // Therefore only secondary index records for the 50 updates would appear here
    assertEquals(50, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());

    // Validate the secondary index stats returned
    Set<String> returnedRecordKeys = new HashSet<>();
    for (SecondaryIndexStats stat : writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get()) {
      // verify si stat marks record as not deleted
      assertFalse(stat.isDeleted());
      // verify the record key and secondary key is present
      assertTrue(StringUtils.nonEmpty(stat.getRecordKey()));
      assertTrue(StringUtils.nonEmpty(stat.getSecondaryKeyValue()));
      returnedRecordKeys.add(stat.getRecordKey());
    }
    // Ensure that all record keys are unique and match the initial insert size
    assertEquals(50, returnedRecordKeys.size());
  }
}
