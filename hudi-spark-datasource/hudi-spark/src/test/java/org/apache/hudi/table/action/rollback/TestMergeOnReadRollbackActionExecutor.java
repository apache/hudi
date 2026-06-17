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
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

  /**
   * Tests that rollback operations generate unique write tokens for log files, preventing collisions
   * during repeated rollback attempts.
   *
   * <p>This test validates the fix for write token generation in metadata table rollbacks. Previously,
   * rollback log files used the default UNKNOWN_WRITE_TOKEN ("1-0-1"), causing collisions when rollback
   * was retried. Now, each rollback generates explicit write tokens based on Spark task context
   * (format: {partitionId}-{stageId}-{attemptId}).
   *
   * <p>Test flow:
   * <ol>
   *   <li>Create initial commit with inserts to establish base files</li>
   *   <li>Create second commit with updates to generate log files (MOR table)</li>
   *   <li>Backup commit timeline files and marker directory for repeated rollback simulation</li>
   *   <li>Execute first rollback and validate write tokens are NOT "1-0-1"</li>
   *   <li>Restore commit state (timeline files + markers) to simulate rollback retry scenario</li>
   *   <li>Execute second rollback and validate unique write tokens prevent collisions</li>
   *   <li>Verify exactly one new rollback log file per file group from second attempt</li>
   * </ol>
   *
   * @param enableMetadataTable runs the test both with and without metadata table enabled to
   *                            ensure write-token generation is correct in both code paths
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRollbackWriteTokenGeneration(boolean enableMetadataTable) throws Exception {
    // 1. Setup: Create a table-version-6 MOR table so the rollback exercises RollbackHelperV1
    //    (which is what this test targets). On v8+ rollbacks delete files directly and don't
    //    produce rollback log files.
    Properties props = new Properties();
    props.put(HoodieTableConfig.VERSION.key(), HoodieTableVersion.SIX.versionCode());
    tearDown();
    initPath();
    initSparkContexts();
    dataGen = new HoodieTestDataGenerator(
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initHoodieStorage();
    initMetaClient(HoodieTableType.MERGE_ON_READ, props);

    HoodieWriteConfig cfg = getConfigBuilder()
        .withRollbackUsingMarkers(true)
        .withMarkersType(MarkerType.DIRECT.name())
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .build();

    HoodieTestDataGenerator.writePartitionMetadataDeprecated(
        storage, new String[] {DEFAULT_FIRST_PARTITION_PATH}, basePath);
    FileSystem fs = (FileSystem) storage.getFileSystem();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // Write 1: Initial inserts
    String commitTime1 = "001";
    WriteClientTestUtils.startCommitWithTime(client, commitTime1);
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(commitTime1, 100, DEFAULT_FIRST_PARTITION_PATH);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statusList = client.upsert(writeRecords, commitTime1).collect();
    Assertions.assertNoWriteErrors(statusList);
    client.commit(commitTime1, jsc.parallelize(statusList));

    // Write 2: Updates to same partition to create log files. Use multiple Spark partitions to
    // exercise multiple task contexts (so write tokens vary across tasks).
    String commitTime2 = "002";
    WriteClientTestUtils.startCommitWithTime(client, commitTime2);
    List<HoodieRecord> updateRecords = dataGen.generateUpdates(commitTime2, records);
    writeRecords = jsc.parallelize(updateRecords, 2);
    statusList = client.upsert(writeRecords, commitTime2).collect();
    Assertions.assertNoWriteErrors(statusList);
    // Intentionally leave commit 002 in inflight state so rollback exercises the inflight path.

    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    Map<String, List<String>> logFileNames = collectLogFileNamesByFileId(fs, DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(logFileNames.isEmpty());

    // Backup commit 002 timeline files + marker dir so the rollback retry below can replay the same input.
    Path commit2RequestedPath = new Path(metaClient.getMetaPath().toString(),
        commitTime2 + HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
    Path commit2InflightPath = new Path(metaClient.getMetaPath().toString(),
        commitTime2 + HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
    Path commit2MarkerDir = new Path(metaClient.getMarkerFolderPath(commitTime2));
    Path backupDir = new Path(basePath, ".backup_test");
    Path backupMarkerDir = new Path(backupDir, commitTime2);
    fs.mkdirs(backupDir);

    boolean requestedExists = fs.exists(commit2RequestedPath);
    boolean inflightExists = fs.exists(commit2InflightPath);
    boolean markerDirExists = fs.exists(commit2MarkerDir);

    if (requestedExists) {
      FileUtil.copy(fs, commit2RequestedPath, fs,
          new Path(backupDir, commitTime2 + HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION),
          false, fs.getConf());
    }
    if (inflightExists) {
      FileUtil.copy(fs, commit2InflightPath, fs,
          new Path(backupDir, commitTime2 + HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION),
          false, fs.getConf());
    }
    if (markerDirExists) {
      FileUtil.copy(fs, commit2MarkerDir, fs, backupMarkerDir, false, fs.getConf());
    }

    // 3. Rollback commit 002
    String rollbackTime = "003";
    HoodieInstant rollBackInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, commitTime2);
    BaseRollbackPlanActionExecutor rollbackPlanExecutor = new BaseRollbackPlanActionExecutor(
        context, cfg, table, rollbackTime, rollBackInstant, false, true, false);
    rollbackPlanExecutor.execute().get();

    MergeOnReadRollbackActionExecutor rollbackExecutor = new MergeOnReadRollbackActionExecutor(
        context, cfg, table, rollbackTime, rollBackInstant, true, false);
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = rollbackExecutor.execute().getPartitionMetadata();

    assertEquals(1, rollbackMetadata.size());
    HoodieRollbackPartitionMetadata partitionMetadata = rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(partitionMetadata.getRollbackLogFiles().isEmpty(), "Should have rollback log files");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = this.getHoodieTable(metaClient, cfg);

    // Validate write tokens on the rollback log files are per-task generated (not UNKNOWN_WRITE_TOKEN "1-0-1").
    List<FileSlice> rollbackFileSlices = table.getSliceView()
        .getLatestFileSlices(DEFAULT_FIRST_PARTITION_PATH)
        .collect(Collectors.toList());
    // FileSlice.getLogFiles() is sorted highest-version first (reverse comparator),
    // so index 0 is the latest log file produced by the rollback.
    List<HoodieLogFile> rollbackLogFiles = rollbackFileSlices.stream()
        .flatMap(slice -> {
          List<HoodieLogFile> logFiles = slice.getLogFiles().collect(Collectors.toList());
          return Collections.singleton(logFiles.get(0)).stream();
        })
        .collect(Collectors.toList());

    assertTrue(rollbackLogFiles.size() > 0, "Should have rollback log files with rollback instant time");
    for (HoodieLogFile logFile : rollbackLogFiles) {
      String writeToken = logFile.getLogWriteToken();
      assertFalse(writeToken.isEmpty(), "Write token should not be empty");
      assertTrue(writeToken.matches("\\d+-\\d+-\\d+"),
          String.format("Write token should match pattern partitionId-stageId-attemptId, but got: %s in file: %s",
              writeToken, logFile.getFileName()));
      assertNotEquals("1-0-1", writeToken);
    }

    Map<String, List<String>> logFileNamesPostRollback = collectLogFileNamesByFileId(fs, DEFAULT_FIRST_PARTITION_PATH);

    // Simulate rollback retry: remove rollback timeline files and restore commit 002 timeline + markers.
    HoodieInstant lastRollbackInstant = metaClient.getActiveTimeline().getRollbackTimeline().lastInstant().get();
    String latestRollbackCompletedFileName =
        INSTANT_FILE_NAME_GENERATOR.getFileName(lastRollbackInstant);
    fs.delete(new Path(metaClient.getMetaPath().toString(), latestRollbackCompletedFileName), false);
    fs.delete(new Path(metaClient.getMetaPath().toString(),
        rollbackTime + HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION), false);

    if (requestedExists) {
      FileUtil.copy(fs, new Path(backupDir, commitTime2 + HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION),
          fs, commit2RequestedPath, false, fs.getConf());
    }
    if (inflightExists) {
      FileUtil.copy(fs, new Path(backupDir, commitTime2 + HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION),
          fs, commit2InflightPath, false, fs.getConf());
    }
    if (markerDirExists) {
      FileUtil.copy(fs, backupMarkerDir, fs, commit2MarkerDir.getParent(), false, fs.getConf());
    }
    fs.delete(backupDir, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = this.getHoodieTable(metaClient, cfg);

    // Trigger second rollback - should create additional rollback log files with different write tokens.
    MergeOnReadRollbackActionExecutor rollbackExecutor2 = new MergeOnReadRollbackActionExecutor(
        context, cfg, table, rollbackTime, rollBackInstant, true, false);
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata2 = rollbackExecutor2.execute().getPartitionMetadata();

    assertEquals(1, rollbackMetadata2.size());
    HoodieRollbackPartitionMetadata partitionMetadata2 = rollbackMetadata2.get(DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(partitionMetadata2.getRollbackLogFiles().isEmpty(), "Should have rollback log files");

    metaClient = HoodieTableMetaClient.reload(metaClient);

    Map<String, List<String>> logFileNamesPost2ndRollback = collectLogFileNamesByFileId(fs, DEFAULT_FIRST_PARTITION_PATH);
    Map<String, Integer> filesFrom2ndRollback = new HashMap<>();
    logFileNamesPost2ndRollback.forEach((fileId, fileNames) -> {
      List<String> previousFiles = logFileNamesPostRollback.getOrDefault(fileId, Collections.emptyList());
      for (String fileName : fileNames) {
        if (!previousFiles.contains(fileName)) {
          filesFrom2ndRollback.merge(fileId, 1, Integer::sum);
          assertNotEquals("1-0-1", new HoodieLogFile(fileName).getLogWriteToken());
        }
      }
    });

    assertFalse(filesFrom2ndRollback.isEmpty(),
        "Second rollback should produce at least one new log file (no collision with first rollback)");
    assertEquals(logFileNames.size(), filesFrom2ndRollback.size());
    filesFrom2ndRollback.forEach((k, v) -> assertEquals(1, v));
    client.close();
  }

  /**
   * Lists all log files in the given partition and groups their file names by file ID.
   */
  private Map<String, List<String>> collectLogFileNamesByFileId(FileSystem fs, String partitionPath) throws IOException {
    Map<String, List<String>> logFilesByFileId = new HashMap<>();
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(
        new Path(metaClient.getBasePath().toString() + "/" + partitionPath), false);
    while (itr.hasNext()) {
      FileStatus fileStatus = itr.next();
      String fileName = fileStatus.getPath().getName();
      if (fileName.contains("log")) {
        String fileId = FSUtils.getFileId(fileName);
        logFilesByFileId.computeIfAbsent(fileId, k -> new ArrayList<>()).add(fileName);
      }
    }
    return logFilesByFileId;
  }
}
