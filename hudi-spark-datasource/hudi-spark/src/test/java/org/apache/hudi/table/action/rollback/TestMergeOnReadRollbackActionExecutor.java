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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
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
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.Assertions;
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
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
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
    dataGen = new HoodieTestDataGenerator(
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initHoodieStorage();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMergeOnReadRollbackActionExecutor(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder()
        .withRollbackUsingMarkers(isUsingMarkers)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteTimelineClientRetry(true).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers, client);
    List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
    List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
    firstPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, firstPartitionCommit2LogFiles.size());
    secondPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, secondPartitionCommit2LogFiles.size());
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    //2. rollback
    String timestampToRollback = metaClient.reloadActiveTimeline().lastInstant().get().requestedTime();
    String rollbackTime = InProcessTimeGenerator.createNewInstantTime();
    HoodieInstant rollBackInstant = INSTANT_GENERATOR.createNewInstant(isUsingMarkers ? HoodieInstant.State.INFLIGHT : HoodieInstant.State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, timestampToRollback);
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, rollbackTime, rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers(), false);
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        rollbackTime,
        rollBackInstant,
        true,
        false);
    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());

    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
      HoodieRollbackPartitionMetadata meta = entry.getValue();
      assertEquals(0, meta.getFailedDeleteFiles().size());
      assertEquals(1, meta.getSuccessDeleteFiles().size());
    }

    //4. assert file group after rollback, and compare to the rollbackstat
    // assert the first partition data and log file size

    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());
    FileSlice firstPartitionRollBack1FileSlice = firstPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> firstPartitionRollBackLogFiles = firstPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(0, firstPartitionRollBackLogFiles.size());

    // assert the second partition data and log file size
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());
    FileSlice secondPartitionRollBack1FileSlice = secondPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> secondPartitionRollBackLogFiles = secondPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(0, secondPartitionRollBackLogFiles.size());

    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, timestampToRollback).doesMarkerDirExist());
    client.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMergeOnReadRollbackLogCompactActionExecutorWithListingStrategy(boolean isComplete) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder()
        .withRollbackUsingMarkers(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withLogCompactionBlocksThreshold(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteTimelineClientRetry(true).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, true, client);
      List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
      List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
      firstPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
      assertEquals(1, firstPartitionCommit2LogFiles.size());
      secondPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
      assertEquals(1, secondPartitionCommit2LogFiles.size());

      //2. log compact
      cfg = getConfigBuilder()
          .withCompactionConfig(HoodieCompactionConfig.newBuilder()
              .withLogCompactionBlocksThreshold(1)
              .withMaxNumDeltaCommitsBeforeCompaction(1)
              .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.NUM_COMMITS).build())
          .withRollbackUsingMarkers(false)
          .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withRemoteServerPort(cfg.getViewStorageConfig().getRemoteViewServerPort())
              .withRemoteTimelineClientRetry(true)
              .build())
          .build();

      String action = HoodieTimeline.LOG_COMPACTION_ACTION;
      if (isComplete) {
        action = HoodieTimeline.DELTA_COMMIT_ACTION;
      }
      String logCompactionTime = (String) client.scheduleLogCompaction(Option.empty()).get();
      HoodieWriteMetadata writeMetadata = client.logCompact(logCompactionTime);
      if (isComplete) {
        client.commitLogCompaction(logCompactionTime, writeMetadata, Option.empty());
      }

      //3. rollback log compact
      metaClient.reloadActiveTimeline();
      HoodieInstant rollBackInstant = INSTANT_GENERATOR.createNewInstant(!isComplete ? HoodieInstant.State.INFLIGHT : HoodieInstant.State.COMPLETED,
          action, logCompactionTime);
      HoodieTable table = this.getHoodieTable(metaClient, cfg);
      String rollbackTime = InProcessTimeGenerator.createNewInstantTime();
      BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
          new BaseRollbackPlanActionExecutor(context, cfg, table, rollbackTime, rollBackInstant, false,
              cfg.shouldRollbackUsingMarkers(), false);
      mergeOnReadRollbackPlanActionExecutor.execute().get();
      MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
          context,
          cfg,
          table,
          rollbackTime,
          rollBackInstant,
          true,
          false);
      //4. assert the rollback stat
      final HoodieRollbackMetadata execute = mergeOnReadRollbackActionExecutor.execute();
      Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = execute.getPartitionMetadata();
      assertEquals(isComplete ? 2 : 0, rollbackMetadata.size());

      for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
        HoodieRollbackPartitionMetadata meta = entry.getValue();
        assertEquals(0, meta.getFailedDeleteFiles().size());
        assertEquals(1, meta.getSuccessDeleteFiles().size());
      }

      //4. assert file group after rollback, and compare to the rollbackstat
      // assert the first partition data and log file size
      metaClient.reloadActiveTimeline();
      table = this.getHoodieTable(metaClient, cfg);
      List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
      assertEquals(1, firstPartitionRollBack1FileGroups.size());
      List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
      assertEquals(1, firstPartitionRollBack1FileSlices.size());
      FileSlice firstPartitionRollBack1FileSlice = firstPartitionRollBack1FileSlices.get(0);
      List<HoodieLogFile> firstPartitionRollBackLogFiles = firstPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals(1, firstPartitionRollBackLogFiles.size());

      // assert the second partition data and log file size
      List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
      assertEquals(1, secondPartitionRollBack1FileGroups.size());
      List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
      assertEquals(1, secondPartitionRollBack1FileSlices.size());
      FileSlice secondPartitionRollBack1FileSlice = secondPartitionRollBack1FileSlices.get(0);
      List<HoodieLogFile> secondPartitionRollBackLogFiles = secondPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals(1, secondPartitionRollBackLogFiles.size());

      assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, logCompactionTime).doesMarkerDirExist());
    }
  }

  @Test
  public void testMergeOnReadRestoreCompactionCommit() throws IOException, InterruptedException {
    boolean isUsingMarkers = false;
    HoodieWriteConfig cfg = getConfigBuilder()
        .withRollbackUsingMarkers(isUsingMarkers)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteTimelineClientRetry(true).build())
        .build();

    // 1. ingest data to partition 3.
    //just generate two partitions
    HoodieTestDataGenerator dataGenPartition3 = new HoodieTestDataGenerator(new String[] {DEFAULT_THIRD_PARTITION_PATH});
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(storage,
        new String[] {DEFAULT_THIRD_PARTITION_PATH}, basePath);
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = client.startCommit();
    List<HoodieRecord> records = dataGenPartition3.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statusList = client.upsert(writeRecords, newCommitTime).collect();
    client.commit(newCommitTime, jsc.parallelize(statusList));
    Assertions.assertNoWriteErrors(statusList);

    //2. Ingest inserts + upserts to partition 1 and 2. we will eventually rollback both these commits using restore flow.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers, client);
    List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
    List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
    firstPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, firstPartitionCommit2LogFiles.size());
    secondPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, secondPartitionCommit2LogFiles.size());
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    //3. rollback the update to partition1 and partition2
    String timestampToRollback = metaClient.reloadActiveTimeline().lastInstant().get().requestedTime();
    String rollbackTime = InProcessTimeGenerator.createNewInstantTime();
    HoodieInstant rollBackInstant = INSTANT_GENERATOR.createNewInstant(isUsingMarkers ? HoodieInstant.State.INFLIGHT : HoodieInstant.State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, timestampToRollback);
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, rollbackTime, rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers(), true);
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        rollbackTime,
        rollBackInstant,
        true,
        false);
    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());
    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, timestampToRollback).doesMarkerDirExist());

    // rollback first instant as well. this time since its part of the restore, entire file slice should be deleted and not just log files (for partition1 and partition2)
    timestampToRollback = metaClient.reloadActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime();
    rollbackTime = InProcessTimeGenerator.createNewInstantTime();
    HoodieInstant rollBackInstant1 = INSTANT_GENERATOR.createNewInstant(isUsingMarkers ? HoodieInstant.State.INFLIGHT : HoodieInstant.State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, timestampToRollback);
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor1 =
        new BaseRollbackPlanActionExecutor(context, cfg, table, rollbackTime, rollBackInstant1, false,
            cfg.shouldRollbackUsingMarkers(), true);
    mergeOnReadRollbackPlanActionExecutor1.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor1 = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        rollbackTime,
        rollBackInstant1,
        true,
        false);
    mergeOnReadRollbackActionExecutor1.execute().getPartitionMetadata();

    //assert there are no valid file groups in both partition1 and partition2
    assertEquals(0, table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).count());
    assertEquals(0, table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).count());
    // and only 3rd partition should have valid file groups.
    assertTrue(table.getFileSystemView().getAllFileGroups(DEFAULT_THIRD_PARTITION_PATH).count() > 0);
    client.close();
  }

  @Test
  public void testRollbackForCanIndexLogFile() throws IOException {
    //1. prepare data and assert data result
    //just generate one partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
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
            .build())
        .withRollbackUsingMarkers(false).build();

    //1. prepare data
    new HoodieTestDataGenerator().writePartitionMetadata(storage,
        new String[] {DEFAULT_FIRST_PARTITION_PATH}, basePath);
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write 1 (only inserts)
    String newCommitTime = client.startCommit();
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_FIRST_PARTITION_PATH);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statusesList = client.upsert(writeRecords, newCommitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(statusesList);
    client.commit(newCommitTime, jsc.parallelize(statusesList));

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
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime);
    HoodieCommitMetadata commitMetadata =
        table.getMetaClient().getCommitTimeline().readCommitMetadata(instant);
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
    newCommitTime = client.startCommit();
    List<HoodieRecord> updateRecords = Collections.singletonList(dataGen.generateUpdateRecord(records.get(0).getKey(), newCommitTime));
    List<HoodieRecord> insertRecordsInSamePartition = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_FIRST_PARTITION_PATH);
    List<HoodieRecord> insertRecordsInOtherPartition = dataGen.generateInsertsForPartition(newCommitTime, 2, DEFAULT_SECOND_PARTITION_PATH);
    List<HoodieRecord> recordsToBeWrite = Stream.concat(Stream.concat(updateRecords.stream(), insertRecordsInSamePartition.stream()), insertRecordsInOtherPartition.stream())
        .collect(Collectors.toList());
    writeRecords = jsc.parallelize(recordsToBeWrite, 1);
    statusesList = client.upsert(writeRecords, newCommitTime).collect();
    client.commit(newCommitTime, jsc.parallelize(statusesList));
    table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime);
    commitMetadata = table.getMetaClient().getCommitTimeline().readCommitMetadata(instant1);
    assertTrue(commitMetadata.getPartitionToWriteStats().containsKey(DEFAULT_FIRST_PARTITION_PATH));
    assertTrue(commitMetadata.getPartitionToWriteStats().containsKey(DEFAULT_SECOND_PARTITION_PATH));
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
    HoodieInstant rollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime);
    String rollbackTimestamp = InProcessTimeGenerator.createNewInstantTime();
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, rollbackTimestamp, rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers(), false);
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        rollbackTimestamp,
        rollBackInstant,
        true,
        false);

    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition data and log file size
    HoodieRollbackPartitionMetadata partitionMetadata = rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH);
    assertEquals(1, partitionMetadata.getSuccessDeleteFiles().size(), "Always use file deletion based rollback");
    assertTrue(partitionMetadata.getFailedDeleteFiles().isEmpty());
    assertTrue(partitionMetadata.getRollbackLogFiles().isEmpty());

    // assert the second partition data and log file size
    partitionMetadata = rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH);
    assertEquals(1, partitionMetadata.getSuccessDeleteFiles().size());
    assertTrue(partitionMetadata.getFailedDeleteFiles().isEmpty());
    assertTrue(partitionMetadata.getRollbackLogFiles().isEmpty());
    assertEquals(1, partitionMetadata.getSuccessDeleteFiles().size());
    client.close();
  }

  /**
   * Test Cases for rolling back when there is no base file.
   */
  @Test
  public void testRollbackWhenFirstCommitFail() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withRollbackUsingMarkers(false)
        .withPath(basePath).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String newCommitTime = client.startCommit();
      List<WriteStatus> statuses = client.insert(jsc.emptyRDD(), newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statuses));
      client.rollback(newCommitTime);
    }
  }
}
