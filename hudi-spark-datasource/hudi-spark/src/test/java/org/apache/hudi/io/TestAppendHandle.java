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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieAppendHandle}.
 */
public class TestAppendHandle extends BaseTestHandle {

  @Test
  public void testAppendHandleRLIAndSIStats() throws Exception {
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
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String instantTime = InProcessTimeGenerator.createNewInstantTime();

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    // generate update and delete records
    instantTime = InProcessTimeGenerator.createNewInstantTime();
    int numUpdates = 20;
    List<HoodieRecord> records = dataGenerator.generateUniqueUpdates(instantTime, numUpdates);
    int numDeletes = generateDeleteRecords(records, dataGenerator, instantTime);
    assertTrue(numDeletes > 0);
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
    assertEquals(numDeletes, writeStatus.getStat().getNumDeletes());
    assertEquals(0, writeStatus.getTotalErrorRecords());
    // validate write status has all record delegates
    assertEquals(records.size(), writeStatus.getIndexStats().getWrittenRecordDelegates().size());
    int numDeletedRecordDelegates = 0;
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      if (!recordDelegate.getNewLocation().isPresent()) {
        numDeletedRecordDelegates++;
      } else {
        assertTrue(recordDelegate.getNewLocation().isPresent());
        assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
        assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
      }
    }
    assertEquals(numDeletes, numDeletedRecordDelegates);

    // validate SI stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // secondary index records size is same twice the number of updates and number of deletes
    // Two records for every update - delete old record and insert new record
    assertEquals(2 * numUpdates + numDeletes, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());
    validateSecondaryIndexStatsContent(writeStatus, numUpdates, numDeletes);
  }
}