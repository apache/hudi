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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieCreateHandle}.
 */
public class TestCreateHandle extends BaseTestHandle {

  @Test
  public void testCreateHandleRLIAndSIStats() throws Exception {
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

    HoodieTable table = HoodieSparkTable.createForReads(config, context, metaClient);
    HoodieTableMetadataWriter metadataWriter = SparkMetadataWriterFactory.create(storageConf, config, context, table.getMetaClient().getTableConfig());
    metadataWriter.close();

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieSparkTable.createForReads(config, context, metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();

    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());
    // validate write status has all record delegates
    assertEquals(records.size(), writeStatus.getIndexStats().getWrittenRecordDelegates().size());
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      assertTrue(recordDelegate.getNewLocation().isPresent());
      assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
      assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
    }

    // Validate the secondary index stats returned
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    assertEquals(100, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());

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
    assertEquals(100, returnedRecordKeys.size());
  }
}
