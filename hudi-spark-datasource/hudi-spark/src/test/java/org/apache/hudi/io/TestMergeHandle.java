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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieMergeHandle}.
 */
public class TestMergeHandle extends BaseTestHandle {

  @Test
  public void testMergeHandleRLIStats() throws IOException {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withPopulateMetaFields(false)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(true).withStreamingWriteEnabled(true).build())
        .withKeyGenerator(KeyGeneratorForDataGeneratorRecords.class.getCanonicalName())
        .build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);
    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    // Create a parquet file
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    instantTime = "001";
    List<HoodieRecord> updates = dataGenerator.generateUniqueUpdates(instantTime, 10);
    HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, instantTime, table, updates.iterator(), partitionPath, fileId, table.getTaskContextSupplier(),
        new HoodieBaseFile(writeStatus.getStat().getPath()), Option.of(new KeyGeneratorForDataGeneratorRecords(config.getProps())));
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    writeStatus = mergeHandle.writeStatus;
    // verify stats after merge
    assertEquals(records.size(), writeStatus.getStat().getNumWrites());
    assertEquals(10, writeStatus.getStat().getNumUpdateWrites());
  }

  @Test
  public void testMergeHandleSecondaryIndexStatsWithUpdates() throws Exception {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withPopulateMetaFields(false)
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
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);
    HoodieTableMetadataWriter metadataWriter = SparkMetadataWriterFactory.create(storageConf, config, context, table.getMetaClient().getTableConfig());
    metadataWriter.close();

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    // Create a parquet file
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});

    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    instantTime = "001";
    List<HoodieRecord> updates = dataGenerator.generateUniqueUpdates(instantTime, 10);
    HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, instantTime, table, updates.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        new HoodieBaseFile(writeStatus.getStat().getPath()), Option.of(new KeyGeneratorForDataGeneratorRecords(config.getProps())));
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    writeStatus = mergeHandle.writeStatus;
    // verify stats after merge
    assertEquals(records.size(), writeStatus.getStat().getNumWrites());
    assertEquals(10, writeStatus.getStat().getNumUpdateWrites());
    // verify secondary index stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // 10 si records for old secondary keys and 10 for new secondary keys
    assertEquals(20, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());

    // Validate the secondary index stats returned
    Map<String, String> deletedRecordAndSecondaryKeys = new HashMap<>();
    Map<String, String> newRecordAndSecondaryKeys = new HashMap<>();
    for (SecondaryIndexStats stat : writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get()) {
      // verify si stat marks record as not deleted
      if (stat.isDeleted()) {
        deletedRecordAndSecondaryKeys.put(stat.getRecordKey(), stat.getSecondaryKeyValue());
      } else {
        newRecordAndSecondaryKeys.put(stat.getRecordKey(), stat.getSecondaryKeyValue());
      }
      // verify the record key and secondary key is present
      assertTrue(StringUtils.nonEmpty(stat.getRecordKey()));
      assertTrue(StringUtils.nonEmpty(stat.getSecondaryKeyValue()));
    }

    // Ensure that all record keys are unique and match the initial update size
    // There should be 10 delete and 10 new secondary index records
    assertEquals(10, deletedRecordAndSecondaryKeys.size());
    assertEquals(10, newRecordAndSecondaryKeys.size());
    assertEquals(deletedRecordAndSecondaryKeys.keySet(), newRecordAndSecondaryKeys.keySet());
    for (String recordKey : deletedRecordAndSecondaryKeys.keySet()) {
      // verify secondary key for deleted and new secondary index records is different
      assertNotEquals(deletedRecordAndSecondaryKeys.get(recordKey), newRecordAndSecondaryKeys.get(recordKey));
    }
  }

  @Test
  public void testMergeHandleSecondaryIndexStatsWithDeletes() throws Exception {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withPopulateMetaFields(false)
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
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);
    HoodieTableMetadataWriter metadataWriter = SparkMetadataWriterFactory.create(storageConf, config, context, table.getMetaClient().getTableConfig());
    metadataWriter.close();

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    // Create a parquet file
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});

    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    instantTime = "001";
    List<HoodieRecord> deletes = dataGenerator.generateUniqueDeleteRecords(instantTime, 10);
    HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, instantTime, table, deletes.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        new HoodieBaseFile(writeStatus.getStat().getPath()), Option.of(new KeyGeneratorForDataGeneratorRecords(config.getProps())));
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    writeStatus = mergeHandle.writeStatus;
    // verify stats after merge, since there are 10 delete records only 90 records will be written
    assertEquals(90, writeStatus.getStat().getNumWrites());
    assertEquals(10, writeStatus.getStat().getNumDeletes());
    // verify secondary index stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // 10 si records for old secondary keys and 10 for new secondary keys
    assertEquals(10, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());

    // Validate the secondary index stats returned
    Set<String> returnedRecordKeys = new HashSet<>();
    for (SecondaryIndexStats stat : writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get()) {
      // verify si stat marks the record as deleted
      assertTrue(stat.isDeleted());
      // verify the record key and secondary key is present
      assertTrue(StringUtils.nonEmpty(stat.getRecordKey()));
      assertTrue(StringUtils.nonEmpty(stat.getSecondaryKeyValue()));
      returnedRecordKeys.add(stat.getRecordKey());
    }
    // Ensure that all record keys are unique and match the deleted records size
    assertEquals(10, returnedRecordKeys.size());
  }
}
