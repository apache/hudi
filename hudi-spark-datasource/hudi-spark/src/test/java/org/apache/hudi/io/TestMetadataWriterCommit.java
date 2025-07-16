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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests Metadata writer APIs with streaming.
 */
public class TestMetadataWriterCommit extends BaseTestHandle {

  @Test
  public void testCreateHandleRLIStats() throws IOException {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withMetadataIndexColumnStats(false)
            .withSecondaryIndexEnabled(false)
            .withStreamingWriteEnabled(true)
            .build())
        .build();

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = InProcessTimeGenerator.createNewInstantTime();

    // create a parquet file and obtain corresponding write status
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    HoodieCommitMetadata commitMetadata = createCommitMetadata(writeStatus.getStat(), partitionPath);

    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    // create mdt writer
    HoodieBackedTableMetadataWriter mdtWriter = (HoodieBackedTableMetadataWriter) SparkMetadataWriterFactory.createWithStreamingWrites(storageConf, config,
        HoodieFailedWritesCleaningPolicy.LAZY, context, Option.empty());
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder().setBasePath(metaClient.getMetaPath() + "/metadata").setConf(storageConf).build();
    assertEquals(2, mdtMetaClient.getActiveTimeline().filterCompletedInstants().countInstants());

    // Create commit in MDT
    mdtWriter = (HoodieBackedTableMetadataWriter) SparkMetadataWriterFactory.createWithStreamingWrites(storageConf, config,
        HoodieFailedWritesCleaningPolicy.LAZY, context, Option.empty());
    mdtWriter.startCommit(instantTime);
    HoodieData<WriteStatus> mdtWriteStatus = mdtWriter.streamWriteToMetadataPartitions(HoodieJavaRDD.of(Collections.singletonList(writeStatus), context, 1), instantTime);
    List<HoodieWriteStat> mdtWriteStats = mdtWriteStatus.collectAsList().stream().map(WriteStatus::getStat).collect(Collectors.toList());
    mdtWriter.completeStreamingCommit(instantTime, context, mdtWriteStats, commitMetadata);
    // 3 bootstrap commits for 2 enabled partitions, 1 commit due to update
    assertEquals(3, mdtMetaClient.reloadActiveTimeline().filterCompletedInstants().countInstants());

    // verify commit metadata
    HoodieCommitMetadata mdtCommitMetadata = mdtMetaClient.getActiveTimeline().readCommitMetadata(mdtMetaClient.getActiveTimeline().lastInstant().get());
    // 2 partitions should be seen in the commit metadata - FILES and Record index
    assertEquals(2, mdtCommitMetadata.getPartitionToWriteStats().size());
    assertEquals(1, mdtCommitMetadata.getPartitionToWriteStats().get(FILES.getPartitionPath()).size());
    assertEquals(10, mdtCommitMetadata.getPartitionToWriteStats().get(RECORD_INDEX.getPartitionPath()).size());
    assertFalse(mdtCommitMetadata.getPartitionToWriteStats().containsKey(COLUMN_STATS.getPartitionPath()));

    // Create commit in MDT with col stats enabled
    config.getMetadataConfig().setValue(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS, "true");
    instantTime = InProcessTimeGenerator.createNewInstantTime();
    mdtWriter = (HoodieBackedTableMetadataWriter) SparkMetadataWriterFactory.createWithStreamingWrites(storageConf, config,
        HoodieFailedWritesCleaningPolicy.LAZY, context, Option.empty());
    mdtWriter.startCommit(instantTime);
    mdtWriteStatus = mdtWriter.streamWriteToMetadataPartitions(HoodieJavaRDD.of(Collections.singletonList(writeStatus), context, 1), instantTime);
    mdtWriteStats = mdtWriteStatus.collectAsList().stream().map(WriteStatus::getStat).collect(Collectors.toList());
    mdtWriter.completeStreamingCommit(instantTime, context, mdtWriteStats, commitMetadata);
    // 3 bootstrap commits for 3 enabled partitions, 2 commits due to update
    assertEquals(5, mdtMetaClient.reloadActiveTimeline().filterCompletedInstants().countInstants());

    // Verify commit metadata
    mdtCommitMetadata = mdtMetaClient.getActiveTimeline().readCommitMetadata(mdtMetaClient.getActiveTimeline().lastInstant().get());
    // 3 partitions should be seen in the commit metadata - FILES, Record index and Column stats
    assertEquals(3, mdtCommitMetadata.getPartitionToWriteStats().size());
    assertEquals(1, mdtCommitMetadata.getPartitionToWriteStats().get(FILES.getPartitionPath()).size());
    assertEquals(10, mdtCommitMetadata.getPartitionToWriteStats().get(RECORD_INDEX.getPartitionPath()).size());
    assertEquals(2, mdtCommitMetadata.getPartitionToWriteStats().get(COLUMN_STATS.getPartitionPath()).size());
  }

  @Test
  public void testCreateHandleSIStats() throws IOException {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    // commit is needed to populate schema of the table. We use a different partition path in the commit below than what is used
    // for actual test
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[1]});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime = writeClient.startCommit();
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 1);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    // init some args
    String fileId = UUID.randomUUID().toString();
    instantTime = InProcessTimeGenerator.createNewInstantTime();
    // enable metadata table with secondary index
    config = getConfigBuilder(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withMetadataIndexColumnStats(false)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexName("sec-rider")
            .withSecondaryIndexForColumn("rider")
            .withStreamingWriteEnabled(true)
            .build())
        .build();
    config.setSchema(TRIP_EXAMPLE_SCHEMA);

    // create mdt writer
    HoodieBackedTableMetadataWriter mdtWriter = (HoodieBackedTableMetadataWriter) SparkMetadataWriterFactory.createWithStreamingWrites(storageConf, config,
        HoodieFailedWritesCleaningPolicy.LAZY, context, Option.empty());
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder().setBasePath(metaClient.getMetaPath() + "/metadata").setConf(storageConf).build();
    // 3 bootstrapped MDT partitions - files, record index and secondary index
    assertEquals(3, mdtMetaClient.getActiveTimeline().filterCompletedInstants().countInstants());

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build();
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH, metaClient.getIndexDefinitionPath());
    table = HoodieSparkTable.create(config, context, metaClient);
    dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    // create a parquet file and obtain corresponding write status
    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    HoodieCommitMetadata commitMetadata = createCommitMetadata(writeStatus.getStat(), partitionPath);

    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    // Create commit in MDT
    mdtWriter = (HoodieBackedTableMetadataWriter) SparkMetadataWriterFactory.createWithStreamingWrites(storageConf, config,
        HoodieFailedWritesCleaningPolicy.LAZY, context, Option.empty());
    mdtWriter.startCommit(instantTime);
    HoodieData<WriteStatus> mdtWriteStatus = mdtWriter.streamWriteToMetadataPartitions(HoodieJavaRDD.of(Collections.singletonList(writeStatus), context, 1), instantTime);
    List<HoodieWriteStat> mdtWriteStats = mdtWriteStatus.collectAsList().stream().map(WriteStatus::getStat).collect(Collectors.toList());
    mdtWriter.completeStreamingCommit(instantTime, context, mdtWriteStats, commitMetadata);
    // 3 bootstrap commits for 3 enabled partitions, 1 commit due to update
    assertEquals(4, mdtMetaClient.reloadActiveTimeline().filterCompletedInstants().countInstants());

    // verify commit metadata
    HoodieCommitMetadata mdtCommitMetadata = mdtMetaClient.getActiveTimeline().readCommitMetadata(mdtMetaClient.getActiveTimeline().lastInstant().get());
    // 3 partitions should be seen in the commit metadata - FILES, Record index and Secondary Index
    assertEquals(3, mdtCommitMetadata.getPartitionToWriteStats().size());
    assertEquals(1, mdtCommitMetadata.getPartitionToWriteStats().get(FILES.getPartitionPath()).size());
    assertEquals(10, mdtCommitMetadata.getPartitionToWriteStats().get(RECORD_INDEX.getPartitionPath()).size());
    assertEquals(10, mdtCommitMetadata.getPartitionToWriteStats().get(SECONDARY_INDEX.getPartitionPath()
        + config.getMetadataConfig().getSecondaryIndexName()).size());
  }

  public static HoodieCommitMetadata createCommitMetadata(HoodieWriteStat writeStat, String partitionPath) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.addWriteStat(partitionPath, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }
}
