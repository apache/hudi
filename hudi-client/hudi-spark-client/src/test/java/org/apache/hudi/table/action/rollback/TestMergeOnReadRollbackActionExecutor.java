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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMergeOnReadRollbackActionExecutor extends HoodieClientRollbackTestBase {
  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    //just generate tow partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true})
  public void testMergeOnReadRollbackActionExecutor(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).withAutoCommit(false).build();
    twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers);
    List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
    List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
    firstPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, firstPartitionCommit2LogFiles.size());
    secondPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, secondPartitionCommit2LogFiles.size());
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    //2. rollback
    HoodieInstant rollBackInstant = new HoodieInstant(isUsingMarkers, HoodieTimeline.DELTA_COMMIT_ACTION, "002");
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, "003", rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers());
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        "003",
        rollBackInstant,
        true,
        false);
    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());

    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
      HoodieRollbackPartitionMetadata meta = entry.getValue();
      assertEquals(0, meta.getFailedDeleteFiles().size());
      assertEquals(0, meta.getSuccessDeleteFiles().size());
    }

    //4. assert file group after rollback, and compare to the rollbackstat
    // assert the first partition data and log file size
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());
    FileSlice firstPartitionRollBack1FileSlice = firstPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> firstPartitionRollBackLogFiles = firstPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, firstPartitionRollBackLogFiles.size());

    firstPartitionRollBackLogFiles.removeAll(firstPartitionCommit2LogFiles);
    assertEquals(1, firstPartitionRollBackLogFiles.size());

    // assert the second partition data and log file size
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());
    FileSlice secondPartitionRollBack1FileSlice = secondPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> secondPartitionRollBackLogFiles = secondPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, secondPartitionRollBackLogFiles.size());

    secondPartitionRollBackLogFiles.removeAll(secondPartitionCommit2LogFiles);
    assertEquals(1, secondPartitionRollBackLogFiles.size());

    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, "002").doesMarkerDirExist());
  }

  @Test
  public void testRollbackForCanIndexLogFile() throws IOException {
    cleanupResources();
    setUpDFS();
    //1. prepare data and assert data result
    //just generate one partitions
    dataGen = new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH});
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build()).withRollbackUsingMarkers(false).withAutoCommit(false).build();

    //1. prepare data
    new HoodieTestDataGenerator().writePartitionMetadata(fs, new String[]{DEFAULT_FIRST_PARTITION_PATH}, basePath);
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write 1 (only inserts)
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_FIRST_PARTITION_PATH);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime);
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(statuses.collect());
    client.commit(newCommitTime, statuses);

    // check fileSlice
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    SyncableFileSystemView fsView = getFileSystemViewWithUnCommittedSlices(table.getMetaClient());
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    assertEquals(1, (int) firstPartitionCommit2FileGroups.get(0).getAllFileSlices().count());
    assertFalse(firstPartitionCommit2FileGroups.get(0).getAllFileSlices().findFirst().get().getBaseFile().isPresent());
    assertEquals(1, firstPartitionCommit2FileGroups.get(0).getAllFileSlices().findFirst().get().getLogFiles().count());
    String generatedFileID = firstPartitionCommit2FileGroups.get(0).getFileGroupId().getFileId();

    // check hoodieCommitMeta
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
        table.getMetaClient().getCommitTimeline()
            .getInstantDetails(new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, "001"))
            .get(),
        HoodieCommitMetadata.class);
    List<HoodieWriteStat> firstPartitionWriteStat = commitMetadata.getPartitionToWriteStats().get(DEFAULT_FIRST_PARTITION_PATH);
    assertEquals(2, firstPartitionWriteStat.size());
    // we have an empty writeStat for all partition
    assert firstPartitionWriteStat.stream().anyMatch(wStat -> StringUtils.isNullOrEmpty(wStat.getFileId()));
    // we have one  non-empty writeStat which must contains update or insert
    assertEquals(1, firstPartitionWriteStat.stream().filter(wStat -> !StringUtils.isNullOrEmpty(wStat.getFileId())).count());
    firstPartitionWriteStat.stream().filter(wStat -> !StringUtils.isNullOrEmpty(wStat.getFileId())).forEach(wStat -> {
      assert wStat.getNumInserts() > 0;
    });

    // Write 2 (inserts)
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> updateRecords = Collections.singletonList(dataGen.generateUpdateRecord(records.get(0).getKey(), newCommitTime));
    List<HoodieRecord> insertRecordsInSamePartition = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_FIRST_PARTITION_PATH);
    List<HoodieRecord> insertRecordsInOtherPartition = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_SECOND_PARTITION_PATH);
    List<HoodieRecord> recordsToBeWrite = Stream.concat(Stream.concat(updateRecords.stream(), insertRecordsInSamePartition.stream()), insertRecordsInOtherPartition.stream())
        .collect(Collectors.toList());
    writeRecords = jsc.parallelize(recordsToBeWrite, 1);
    statuses = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, statuses);
    table = this.getHoodieTable(metaClient, cfg);
    commitMetadata = HoodieCommitMetadata.fromBytes(
        table.getMetaClient().getCommitTimeline()
            .getInstantDetails(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime))
            .get(),
        HoodieCommitMetadata.class);
    assert commitMetadata.getPartitionToWriteStats().containsKey(DEFAULT_FIRST_PARTITION_PATH);
    assert commitMetadata.getPartitionToWriteStats().containsKey(DEFAULT_SECOND_PARTITION_PATH);
    List<HoodieWriteStat> hoodieWriteStatOptionList = commitMetadata.getPartitionToWriteStats().get(DEFAULT_FIRST_PARTITION_PATH);
    // Both update and insert record should enter same existing fileGroup due to small file handling
    assertEquals(1, hoodieWriteStatOptionList.size());
    assertEquals(generatedFileID, hoodieWriteStatOptionList.get(0).getFileId());
    // check insert and update numbers
    assertEquals(2, hoodieWriteStatOptionList.get(0).getNumInserts());
    assertEquals(1, hoodieWriteStatOptionList.get(0).getNumUpdateWrites());

    List<HoodieWriteStat> secondHoodieWriteStatOptionList = commitMetadata.getPartitionToWriteStats().get(DEFAULT_SECOND_PARTITION_PATH);
    // All insert should enter one fileGroup
    assertEquals(1, secondHoodieWriteStatOptionList.size());
    String fileIdInPartitionTwo = secondHoodieWriteStatOptionList.get(0).getFileId();
    assertEquals(2, hoodieWriteStatOptionList.get(0).getNumInserts());

    // Rollback
    HoodieInstant rollBackInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002");
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, "003", rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers());
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        "003",
        rollBackInstant,
        true,
        false);

    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition data and log file size
    HoodieRollbackPartitionMetadata partitionMetadata = rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(partitionMetadata.getSuccessDeleteFiles().isEmpty());
    assertTrue(partitionMetadata.getFailedDeleteFiles().isEmpty());
    assertEquals(1, partitionMetadata.getRollbackLogFiles().size());

    // assert the second partition data and log file size
    partitionMetadata = rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH);
    assertEquals(1, partitionMetadata.getSuccessDeleteFiles().size());
    assertTrue(partitionMetadata.getFailedDeleteFiles().isEmpty());
    assertTrue(partitionMetadata.getRollbackLogFiles().isEmpty());
    assertEquals(1, partitionMetadata.getSuccessDeleteFiles().size());
  }

  /**
   * Test Cases for rolling back when there is no base file.
   */
  @Test
  public void testRollbackWhenFirstCommitFail() throws Exception {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withRollbackUsingMarkers(false)
        .withPath(basePath).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      client.startCommitWithTime("001");
      client.insert(jsc.emptyRDD(), "001");
      client.rollback("001");
    }
  }

  private void setUpDFS() throws IOException {
    initDFS();
    initSparkContexts();
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initFileSystem();
    initDFSMetaClient();
  }
}
