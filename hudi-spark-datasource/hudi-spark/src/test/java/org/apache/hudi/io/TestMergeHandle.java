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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieMergeHandle}.
 */
public class TestMergeHandle extends BaseTestHandle {

  @Test
  public void testMergeHandleRLIAndSIStatsWithUpdatesAndDeletes() throws Exception {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withPopulateMetaFields(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withStreamingWriteEnabled(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexName("sec-rider")
            .withSecondaryIndexForColumn("rider")
            .build())
        .withKeyGenerator(KeyGeneratorForDataGeneratorRecords.class.getCanonicalName())
        .build();
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime = client.startCommit();
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    instantTime = "001";
    int numUpdates = 10;
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, numUpdates);
    int numDeletes = generateDeleteRecords(newRecords, dataGenerator, instantTime);
    assertTrue(numDeletes > 0);
    HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, instantTime, table, newRecords.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        new HoodieBaseFile(fileGroup.getAllBaseFiles().findFirst().get()), Option.empty());
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    WriteStatus writeStatus = mergeHandle.writeStatus;
    // verify stats after merge
    assertEquals(100 - numDeletes, writeStatus.getStat().getNumWrites());
    assertEquals(numUpdates, writeStatus.getStat().getNumUpdateWrites());
    assertEquals(numDeletes, writeStatus.getStat().getNumDeletes());

    // verify record index stats
    // numUpdates + numDeletes - new record index updates
    assertEquals(numUpdates + numDeletes, writeStatus.getIndexStats().getWrittenRecordDelegates().size());
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

    // verify secondary index stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // 2 * numUpdates si records for old secondary keys and new secondary keys related to updates
    // numDeletes secondary keys related to deletes
    assertEquals(2 * numUpdates + numDeletes, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());
    validateSecondaryIndexStatsContent(writeStatus, numUpdates, numDeletes);
  }
}
