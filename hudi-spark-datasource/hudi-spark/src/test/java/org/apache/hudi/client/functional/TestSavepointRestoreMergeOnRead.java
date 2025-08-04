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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.functional.TestHoodieFileSystemViews.assertForFSVEquality;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for MERGE_ON_READ table savepoint restore.
 */
@Tag("functional")
public class TestSavepointRestoreMergeOnRead extends HoodieClientTestBase {
  // Overrides setup to avoid creating a table with the default table version
  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    initTimelineService();
  }

  /**
   * Actions: DC1, DC2, DC3, savepoint DC3,(snapshot query) DC4, C5, DC6, DC7. restore to DC3.
   * Should roll back DC5 and DC6.
   * The latest file slice should be fully cleaned up, and rollback log appends for DC4 in first file slice.
   *
   * <p>For example file layout,
   * FG1:
   * BF1(DC1), LF1(DC2), LF2(DC3), LF3(DC4)
   * BF5(C5), LF1(DC6), LF2(DC7)
   * After restore, it becomes
   * BF1(DC1), LF1(DC2), LF2(DC3), LF3(DC4), LF4(RB DC4)
   *
   * <p>Expected behaviors:
   * snapshot query: total rec matches.
   * checking the row count by updating columns in (val4,val5,val6, val7).
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testCleaningDeltaCommits(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(4) // the 4th delta_commit triggers compaction
        .withInlineCompaction(false).build(), tableVersion);
    Map<String, Integer> commitToRowCount = new HashMap<>();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = client.startCommit();
        // Write 4 inserts with the 2td commit been rolled back
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.commit(newCommitTime, client.insert(writeRecords, newCommitTime));
        commitToRowCount.put(newCommitTime, numRecords);
        if (i == 3) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          baseRecordsToUpdate = records;
          client.savepoint("user1", "Savepoint for 3rd commit");
        }
      }

      assertRowNumberEqualsTo(30);

      // write another 3 delta commits
      String compactionCommit = null;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"));
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.commit(newCommitTime, client.upsert(writeRecords, newCommitTime), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
        if (i == 1) {
          Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
          assertTrue(compactionInstant.isPresent(), "A compaction plan should be scheduled");
          compactionCommit = compactionInstant.get();
          client.compact(compactionInstant.get());
        }
      }

      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(30);
      // ensure there are no data files matching the compaction commit that was rolled back.
      String finalCompactionCommit = compactionCommit;
      StoragePathFilter filter = (path) -> path.toString().contains(finalCompactionCommit);
      for (String pPath : dataGen.getPartitionPaths()) {
        assertEquals(0, storage.listDirectEntries(
            FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath),
            filter).size());
      }
    }
    validateFilesMetadata(hoodieWriteConfig);
    assertEquals(commitToRowCount, getRecordCountPerCommit());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  public void testRestoreToWithInflightDeltaCommit(HoodieTableVersion tableVersion) throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(4)
        .withInlineCompaction(false)
        .compactionSmallFileSize(0).build(), tableVersion);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // establish base files
      String firstCommitTime = client.startCommit();
      int numRecords = 10;
      List<HoodieRecord> initialRecords = dataGen.generateInserts(firstCommitTime, numRecords);
      client.commit(firstCommitTime, client.insert(jsc.parallelize(initialRecords, 1), firstCommitTime));
      // add updates that go to log files
      String secondCommitTime = client.startCommit();
      List<HoodieRecord> updatedRecords = dataGen.generateUniqueUpdates(secondCommitTime, numRecords);
      client.commit(secondCommitTime, client.upsert(jsc.parallelize(updatedRecords, 1), secondCommitTime));
      // create a savepoint
      client.savepoint("user1", "Savepoint for 2nd commit");
      // add a third delta commit
      String thirdCommitTime = client.startCommit();
      List<HoodieRecord> updatedRecordsToBeRolledBack = dataGen.generateUniqueUpdates(thirdCommitTime, numRecords);
      client.commit(thirdCommitTime, client.upsert(jsc.parallelize(updatedRecordsToBeRolledBack, 1), thirdCommitTime));
      // add a fourth delta commit but leave it in-flight
      String fourthCommitTime = client.startCommit();
      List<HoodieRecord> inFlightRecords = Stream.concat(dataGen.generateUniqueUpdates(fourthCommitTime, numRecords).stream(),
          dataGen.generateInserts(fourthCommitTime, numRecords).stream()).collect(Collectors.toList());
      // collect result to trigger file creation
      List<WriteStatus> writes = client.upsert(jsc.parallelize(inFlightRecords, 1), fourthCommitTime).collect();
      // restore to the savepoint
      client.restoreToSavepoint(secondCommitTime);
      // Validate metadata and records match expectations
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(Collections.singletonMap(secondCommitTime, numRecords), getRecordCountPerCommit());
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  public void testRestoreWithFileGroupCreatedWithDeltaCommits(HoodieTableVersion tableVersion) throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(4)
        .withInlineCompaction(true).build(), tableVersion);
    final int numRecords = 100;
    String firstCommit;
    String secondCommit;
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // 1st commit insert
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.commit(newCommitTime, client.insert(writeRecords, newCommitTime));
      firstCommit = newCommitTime;

      // 2nd commit with inserts and updates which will create new file slice due to small file handling.
      newCommitTime = client.startCommit();
      List<HoodieRecord> records2 = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 1);
      List<HoodieRecord> records3 = dataGen.generateInserts(newCommitTime, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 1);

      client.commit(newCommitTime, client.upsert(writeRecords2.union(writeRecords3), newCommitTime));
      secondCommit = newCommitTime;
      // add savepoint to 2nd commit
      client.savepoint(firstCommit, "test user","test comment");
    }
    assertRowNumberEqualsTo(130);
    // verify there are new base files created matching the 2nd commit timestamp.
    StoragePathFilter filter = (path) -> path.toString().contains(secondCommit);
    for (String pPath : dataGen.getPartitionPaths()) {
      assertEquals(1, storage.listDirectEntries(
              FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath), filter)
          .size());
    }

    // disable small file handling so that updates go to log files.
    hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4) // the 4th delta_commit triggers compaction
            .withInlineCompaction(true)
            .compactionSmallFileSize(0)
            .build())
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .build())
        .withAutoCommit(false)
        .build();

    // add 2 more updates which will create log files.
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.commit(newCommitTime, client.upsert(writeRecords, newCommitTime));

      newCommitTime = client.startCommit();
      records = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      writeRecords = jsc.parallelize(records, 1);
      client.commit(newCommitTime, client.upsert(writeRecords, newCommitTime));
    }
    assertRowNumberEqualsTo(130);

    // restore to 2nd commit.
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      client.restoreToSavepoint(firstCommit);
    }
    assertRowNumberEqualsTo(100);
    // verify that entire file slice created w/ base instant time of 2nd commit is completely rolledback.
    filter = (path) -> path.toString().contains(secondCommit);
    for (String pPath : dataGen.getPartitionPaths()) {
      assertEquals(0, storage.listDirectEntries(
              FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath), filter)
          .size());
    }
    // ensure files matching 1st commit is intact
    filter = (path) -> path.toString().contains(firstCommit);
    for (String pPath : dataGen.getPartitionPaths()) {
      assertEquals(1,
          storage.listDirectEntries(
              FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath),
              filter).size());
    }
    validateFilesMetadata(hoodieWriteConfig);
    Map<String, Integer> actualCommitToRowCount = getRecordCountPerCommit();
    assertEquals(Collections.singletonMap(firstCommit, numRecords), actualCommitToRowCount);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  public void testRestoreWithUpdatesToClusteredFileGroups(HoodieTableVersion tableVersion) throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(
        HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(4).withInlineCompaction(false).compactionSmallFileSize(0).build(),
        HoodieClusteringConfig.newBuilder().withAsyncClusteringMaxCommits(2).build(), tableVersion);
    final int numRecords = 20;
    String secondCommit;
    String clusteringCommit;
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // 1st commit insert
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.commit(newCommitTime, client.insert(writeRecords, newCommitTime));

      // 2nd commit with inserts and updates which will create new file slice due to small file handling.
      newCommitTime = client.startCommit();
      List<HoodieRecord> records2 = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 1);
      List<HoodieRecord> records3 = dataGen.generateInserts(newCommitTime, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 1);

      client.commit(newCommitTime, client.upsert(writeRecords2.union(writeRecords3), newCommitTime));
      secondCommit = newCommitTime;
      // add savepoint to 2nd commit
      client.savepoint(secondCommit, "test user","test comment");

      Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
      clusteringCommit = clusteringInstant.get();
      client.cluster(clusteringCommit);

      // add new log files on top of the clustered file group
      newCommitTime = client.startCommit();
      List<HoodieRecord> updatedRecords = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      writeClient.commit(newCommitTime, writeClient.upsert(jsc.parallelize(updatedRecords, 1), newCommitTime));
    }
    assertRowNumberEqualsTo(50);
    StoragePathFilter filter = path -> path.toString().contains(clusteringCommit);
    for (String pPath : dataGen.getPartitionPaths()) {
      // verify there is 1 base file and 1 log file created matching the clustering timestamp if table version is 6.
      // For table version 8 and above, the log file will not reference the clustering commit.
      assertEquals(tableVersion == HoodieTableVersion.SIX ? 2 : 1, storage.listDirectEntries(
              FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath), filter)
          .size());
    }

    // restore to 2nd commit and remove the clustering instant and delta commit that followed it
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      client.restoreToSavepoint(secondCommit);
    }
    // verify that entire file slice created w/ base instant time of clustering instant is completely rolled back.
    for (String pPath : dataGen.getPartitionPaths()) {
      assertEquals(0, storage.listDirectEntries(
              FSUtils.constructAbsolutePath(hoodieWriteConfig.getBasePath(), pPath), filter)
          .size());
    }
    validateFilesMetadata(hoodieWriteConfig);
    Map<String, Integer> actualCommitToRowCount = getRecordCountPerCommit();
    assertEquals(Collections.singletonMap(secondCommit, 50), actualCommitToRowCount);
  }

  /**
   * <p>Actions: DC1, DC2, DC3, savepoint DC3, DC4, C5.pending, DC6, DC7, restore
   * should roll back until DC3.
   *
   * <P>Expected behaviors: pending compaction after savepoint should also be cleaned,
   * the latest file slice should be fully delete, for DC4 a rollback log append should be made.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testCleaningPendingCompaction(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(4) // the 4th delta_commit triggers compaction
        .withInlineCompaction(false)
        .withScheduleInlineCompaction(false)
        .compactionSmallFileSize(0).build(), tableVersion);
    Map<String, Integer> commitToRowCount = new HashMap<>();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = client.startCommit();
        // Write 4 inserts with the 2td commit been rolled back
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(writeRecords, newCommitTime);
        client.commit(newCommitTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
        commitToRowCount.put(newCommitTime, numRecords);
        if (i == 3) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          baseRecordsToUpdate = records;
          client.savepoint("user1", "Savepoint for 3rd commit");
        }
      }

      assertRowNumberEqualsTo(30);

      // write another 3 delta commits
      for (int i = 1; i <= 3; i++) {
        upsertBatch(writeClient, baseRecordsToUpdate);
        if (i == 1) {
          Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
          assertTrue(compactionInstant.isPresent(), "A compaction plan should be scheduled");
          compactWithoutCommit(compactionInstant.get(), tableVersion);
        }
      }

      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertEquals(commitToRowCount, getRecordCountPerCommit());
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(tableVersion, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
    }
  }

  /**
   * Actions: DC1, DC2, DC3, C4, savepoint C4, DC5, C6(RB_DC5), DC7, restore
   *
   * <P>Expected behaviors: should roll back DC5, C6 and DC6.
   * No files will be cleaned up. Only rollback log appends.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testCleaningCompletedRollback(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(3) // the 3rd delta_commit triggers compaction
        .withInlineCompaction(false)
        .withScheduleInlineCompaction(false).build(), tableVersion);
    Map<String, Integer> commitToRowCount = new HashMap<>();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = client.startCommit();
        // Write 4 inserts with the 2td commit been rolled back
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(writeRecords, newCommitTime);
        client.commit(newCommitTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
        if (i == 2) {
          baseRecordsToUpdate = records;
        } else {
          commitToRowCount.put(newCommitTime, numRecords);
        }
      }

      // update to generate log files, then a valid compaction plan can be scheduled
      String updateCommitTime = upsertBatch(client, baseRecordsToUpdate);
      commitToRowCount.put(updateCommitTime, baseRecordsToUpdate.size());
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstant.isPresent(), "A compaction plan should be scheduled");
      HoodieWriteMetadata result = client.compact(compactionInstant.get());
      client.commitCompaction(compactionInstant.get(), (HoodieCommitMetadata) result.getCommitMetadata().get(), Option.empty());
      savepointCommit = compactionInstant.get();
      client.savepoint("user1", "Savepoint for 3td commit");

      assertRowNumberEqualsTo(20);
      // write a delta_commit but does not commit
      updateBatchWithoutCommit(client.createNewInstantTime(),
          Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"), tableVersion);
      // rollback the delta_commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(client.rollbackFailedWrites(metaClient), "The last delta_commit should be rolled back");

      // another update
      upsertBatch(client, baseRecordsToUpdate);

      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertEquals(commitToRowCount, getRecordCountPerCommit());
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(tableVersion, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
    }
  }

  @Test
  void rollbackWithAsyncServices_compactionCompletesDuringCommit() {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigWithCompactionAndConcurrencyControl(HoodieTableVersion.EIGHT);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      final int numRecords = 10;
      writeInitialCommitsForAsyncServicesTests(numRecords);
      String inflightCommit = client.startCommit();
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(inflightCommit, numRecords);
      JavaRDD<WriteStatus> writeStatus = writeClient.upsert(jsc.parallelize(records, 1), inflightCommit);

      // Run compaction while delta-commit is in-flight
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      HoodieWriteMetadata result = client.compact(compactionInstant.get());
      client.commitCompaction(compactionInstant.get(), (HoodieCommitMetadata) result.getCommitMetadata().get(), Option.empty());
      // commit the inflight delta commit
      client.commit(inflightCommit, writeStatus);

      client.savepoint(inflightCommit, "user1", "Savepoint for commit that completed after compaction");

      // write one more commit
      String newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, numRecords);
      writeStatus = client.insert(jsc.parallelize(records, 1), newCommitTime);
      client.commit(newCommitTime, writeStatus);

      // restore to savepoint
      client.restoreToSavepoint(inflightCommit);
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(Collections.singletonMap(inflightCommit, numRecords), getRecordCountPerCommit());
      // ensure the compaction instant is still present because it was completed before the target of the restore
      assertFalse(metaClient.reloadActiveTimeline().filterCompletedInstants().getInstantsAsStream()
          .anyMatch(hoodieInstant -> hoodieInstant.requestedTime().equals(compactionInstant.get())));
    }
  }

  @Test
  void rollbackWithAsyncServices_commitCompletesDuringCompaction() {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigWithCompactionAndConcurrencyControl(HoodieTableVersion.EIGHT);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      final int numRecords = 10;
      writeInitialCommitsForAsyncServicesTests(numRecords);
      String inflightCommit = client.startCommit();
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(inflightCommit, numRecords);
      JavaRDD<WriteStatus> writeStatus = client.upsert(jsc.parallelize(records, 1), inflightCommit);

      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      HoodieWriteMetadata result = client.compact(compactionInstant.get());
      // commit the inflight delta commit
      client.commit(inflightCommit, writeStatus);
      // commit the compaction instant after the delta commit
      client.commitCompaction(compactionInstant.get(), (HoodieCommitMetadata) result.getCommitMetadata().get(), Option.empty());

      client.savepoint(inflightCommit, "user1", "Savepoint for commit that completed during compaction");

      // write one more commit
      String newCommitTime = writeClient.startCommit();
      records = dataGen.generateInserts(newCommitTime, numRecords);
      writeStatus = client.insert(jsc.parallelize(records, 1), newCommitTime);
      client.commit(newCommitTime, writeStatus);

      // restore to savepoint
      client.restoreToSavepoint(inflightCommit);
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(Collections.singletonMap(inflightCommit, numRecords), getRecordCountPerCommit());
      // ensure the compaction instant is not present because it was completed after the target of the restore
      assertFalse(metaClient.reloadActiveTimeline().filterCompletedInstants().getInstantsAsStream()
          .anyMatch(hoodieInstant -> hoodieInstant.requestedTime().equals(compactionInstant.get())));
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void rollbackWithAsyncServices_commitStartsAndFinishesDuringCompaction(HoodieTableVersion tableVersion) {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigWithCompactionAndConcurrencyControl(tableVersion);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      final int numRecords = 10;
      writeInitialCommitsForAsyncServicesTests(numRecords);
      // Schedule a compaction
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      // Start a delta commit
      String inflightCommit = client.startCommit();
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(inflightCommit, numRecords);
      JavaRDD<WriteStatus> writeStatus = client.upsert(jsc.parallelize(records, 1), inflightCommit);

      HoodieWriteMetadata result = client.compact(compactionInstant.get());
      // commit the inflight delta commit
      client.commit(inflightCommit, writeStatus);
      // commit the compaction instant after the delta commit
      client.commitCompaction(compactionInstant.get(), (HoodieCommitMetadata) result.getCommitMetadata().get(), Option.empty());

      client.savepoint(inflightCommit, "user1", "Savepoint for commit that completed during compaction");

      // write one more commit
      String newCommitTime = writeClient.startCommit();
      records = dataGen.generateInserts(newCommitTime, numRecords);
      writeStatus = client.insert(jsc.parallelize(records, 1), newCommitTime);
      client.commit(newCommitTime, writeStatus);

      // restore to savepoint
      client.restoreToSavepoint(inflightCommit);
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(Collections.singletonMap(inflightCommit, numRecords), getRecordCountPerCommit());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().getInstantsAsStream()
          .anyMatch(hoodieInstant -> hoodieInstant.requestedTime().equals(compactionInstant.get())));
      assertEquals(tableVersion, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testMissingFileDoesNotFallRestore(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(4)
        .withInlineCompaction(false)
        .compactionSmallFileSize(0).build(), tableVersion);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // establish base files
      String firstCommitTime = client.startCommit();
      int numRecords = 10;
      List<HoodieRecord> initialRecords = dataGen.generateInserts(firstCommitTime, numRecords);
      client.commit(firstCommitTime, client.insert(jsc.parallelize(initialRecords, 1), firstCommitTime));
      // add updates that go to log files
      String secondCommitTime = client.startCommit();
      List<HoodieRecord> updatedRecords = dataGen.generateUniqueUpdates(secondCommitTime, numRecords);
      client.commit(secondCommitTime, client.upsert(jsc.parallelize(updatedRecords, 1), secondCommitTime));
      client.savepoint(secondCommitTime, "user1", "Savepoint for commit that completed during compaction");

      // add a third delta commit with log and new base files
      String thirdCommitTime = client.startCommit();
      List<HoodieRecord> upsertRecords = Stream.concat(dataGen.generateUniqueUpdates(thirdCommitTime, numRecords).stream(),
          dataGen.generateInserts(thirdCommitTime, numRecords).stream()).collect(Collectors.toList());
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(upsertRecords, 1), thirdCommitTime).collect();
      client.commit(thirdCommitTime, jsc.parallelize(writeStatuses, 1));

      // delete one base file and one log file to validate both cases are handled gracefully
      boolean deletedLogFile = false;
      boolean deletedBaseFile = false;
      for (WriteStatus writeStatus : writeStatuses) {
        StoragePath path = FSUtils.constructAbsolutePath(basePath, writeStatus.getStat().getPath());
        if (deletedLogFile && deletedBaseFile) {
          break;
        }
        if (FSUtils.isLogFile(path)) {
          deletedLogFile = true;
          storage.deleteFile(path);
        } else {
          deletedBaseFile = true;
          storage.deleteFile(path);
        }
      }
      client.restoreToSavepoint(secondCommitTime);
      validateFilesMetadata(hoodieWriteConfig);
      assertEquals(Collections.singletonMap(secondCommitTime, numRecords), getRecordCountPerCommit());
    }
  }

  private Map<String, Integer> writeInitialCommitsForAsyncServicesTests(int numRecords) {
    Map<String, Integer> commitToRowCount = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = i == 0 ? dataGen.generateInserts(newCommitTime, numRecords) : dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<WriteStatus> writeStatus = i == 0 ? writeClient.insert(jsc.parallelize(records, 1), newCommitTime) : writeClient.upsert(jsc.parallelize(records, 1), newCommitTime);
      writeClient.commit(newCommitTime, writeStatus);
      if (i == 2) {
        commitToRowCount.put(newCommitTime, numRecords);
      }
    }
    return commitToRowCount;
  }

  private HoodieWriteConfig getHoodieWriteConfigWithCompactionAndConcurrencyControl(HoodieTableVersion tableVersion) {
    HoodieWriteConfig config = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .withInlineCompaction(false)
            .withScheduleInlineCompaction(false)
            .build())
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .build())
        .withAutoCommit(false)
        .withProps(Collections.singletonMap(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"))
        .build();
    try {
      initMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
      return config;
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize HoodieTableMetaClient", e);
    }
  }

  private void validateFilesMetadata(HoodieWriteConfig writeConfig) {
    HoodieTableFileSystemView fileListingBasedView = FileSystemViewManager.createInMemoryFileSystemView(context, metaClient,
        HoodieMetadataConfig.newBuilder().enable(false).build());
    FileSystemViewStorageConfig viewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(writeConfig.getProps())
        .withStorageType(FileSystemViewStorageType.MEMORY).build();
    HoodieTableFileSystemView metadataBasedView = (HoodieTableFileSystemView) FileSystemViewManager
        .createViewManager(context, writeConfig.getMetadataConfig(), viewStorageConfig, writeConfig.getCommonConfig(),
            (SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata>) v1 ->
                HoodieTableMetadata.create(context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath()))
        .getFileSystemView(basePath);
    assertForFSVEquality(fileListingBasedView, metadataBasedView, true);
  }

  private String upsertBatch(SparkRDDWriteClient client, List<HoodieRecord> baseRecordsToUpdate) throws IOException {
    String newCommitTime = client.startCommit();
    List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"));
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.commit(newCommitTime, client.upsert(writeRecords, newCommitTime), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
    return newCommitTime;
  }

  private void compactWithoutCommit(String compactionInstantTime, HoodieTableVersion tableVersion) {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoCommit(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .build())
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      JavaRDD<WriteStatus> statuses = (JavaRDD<WriteStatus>) client.compact(compactionInstantTime).getWriteStatuses();
      assertNoWriteErrors(statuses.collect());
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private HoodieWriteConfig getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig compactionConfig, HoodieTableVersion tableVersion) throws IOException {
    return getHoodieWriteConfigAndInitializeTable(compactionConfig, HoodieClusteringConfig.newBuilder().build(), tableVersion);
  }

  private HoodieWriteConfig getHoodieWriteConfigAndInitializeTable(HoodieCompactionConfig compactionConfig, HoodieClusteringConfig clusteringConfig, HoodieTableVersion tableVersion)
      throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(compactionConfig)
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withAutoCommit(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .build();
    initMetaClient(HoodieTableType.MERGE_ON_READ, hoodieWriteConfig.getProps());
    return hoodieWriteConfig;
  }

  private Map<String, Integer> getRecordCountPerCommit() {
    Dataset<Row> dataset = sparkSession.read().format("hudi").load(basePath);
    List<Row> rows = dataset.collectAsList();
    int index = dataset.schema().fieldIndex(HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName());
    Map<String, Integer> actualCommitToRowCount = new HashMap<>();
    rows.forEach(row -> {
      actualCommitToRowCount.compute(row.getString(index), (k, v) -> (v == null) ? 1 : v + 1);
    });
    return actualCommitToRowCount;
  }
}
