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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for MERGE_ON_READ table savepoint restore.
 */
@Tag("functional")
public class TestSavepointRestoreMergeOnRead extends HoodieClientTestBase {

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
  @Test
  void testCleaningDeltaCommits() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4) // the 4th delta_commit triggers compaction
            .withInlineCompaction(true)
            .build())
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        // Write 4 inserts with the 2td commit been rolled back
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.insert(writeRecords, newCommitTime);
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
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"));
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);
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
  }

  @Test
  public void testRestoreWithFileGroupCreatedWithDeltaCommits() throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .withInlineCompaction(true)
            .build())
        .withRollbackUsingMarkers(true)
        .build();
    final int numRecords = 100;
    String firstCommit;
    String secondCommit;
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // 1st commit insert
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.startCommitWithTime(newCommitTime);
      client.insert(writeRecords, newCommitTime);
      firstCommit = newCommitTime;

      // 2nd commit with inserts and updates which will create new file slice due to small file handling.
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records2 = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 1);
      List<HoodieRecord> records3 = dataGen.generateInserts(newCommitTime, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 1);

      client.startCommitWithTime(newCommitTime);
      client.upsert(writeRecords2.union(writeRecords3), newCommitTime);
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
        .build();

    // add 2 more updates which will create log files.
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.startCommitWithTime(newCommitTime);
      client.upsert(writeRecords, newCommitTime);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateUniqueUpdates(newCommitTime, numRecords);
      writeRecords = jsc.parallelize(records, 1);
      client.startCommitWithTime(newCommitTime);
      client.upsert(writeRecords, newCommitTime);
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
  }

  /**
   * <p>Actions: DC1, DC2, DC3, savepoint DC3, DC4, C5.pending, DC6, DC7, restore
   * should roll back until DC3.
   *
   * <P>Expected behaviors: pending compaction after savepoint should also be cleaned,
   * the latest file slice should be fully delete, for DC4 a rollback log append should be made.
   */
  @Test
  void testCleaningPendingCompaction() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4) // the 4th delta_commit triggers compaction
            .withInlineCompaction(false)
            .withScheduleInlineCompaction(true)
            .build())
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        // Write 4 inserts with the 2td commit been rolled back
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.insert(writeRecords, newCommitTime);
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
          compactWithoutCommit(compactionInstant.get());
        }
      }

      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(30);
    }
  }

  /**
   * Actions: DC1, DC2, DC3, C4, savepoint C4, DC5, C6(RB_DC5), DC7, restore
   *
   * <P>Expected behaviors: should roll back DC5, C6 and DC6.
   * No files will be cleaned up. Only rollback log appends.
   */
  @Test
  void testCleaningCompletedRollback() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER) // eager cleaning
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(3) // the 3rd delta_commit triggers compaction
            .withInlineCompaction(false)
            .withScheduleInlineCompaction(true)
            .build())
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      final int numRecords = 10;
      List<HoodieRecord> baseRecordsToUpdate = null;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        // Write 4 inserts with the 2td commit been rolled back
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
        client.insert(writeRecords, newCommitTime);
        if (i == 2) {
          baseRecordsToUpdate = records;
        }
      }

      // update to generate log files, then a valid compaction plan can be scheduled
      upsertBatch(client, baseRecordsToUpdate);
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstant.isPresent(), "A compaction plan should be scheduled");
      client.compact(compactionInstant.get());
      savepointCommit = compactionInstant.get();
      client.savepoint("user1", "Savepoint for 3td commit");

      assertRowNumberEqualsTo(20);
      // write a delta_commit but does not commit
      updateBatchWithoutCommit(HoodieActiveTimeline.createNewInstantTime(), Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"));
      // rollback the delta_commit
      assertTrue(writeClient.rollbackFailedWrites(), "The last delta_commit should be rolled back");

      // another update
      upsertBatch(writeClient, baseRecordsToUpdate);

      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  private void upsertBatch(SparkRDDWriteClient client, List<HoodieRecord> baseRecordsToUpdate) throws IOException {
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, Objects.requireNonNull(baseRecordsToUpdate, "The records to update should not be null"));
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.upsert(writeRecords, newCommitTime);
  }

  private void compactWithoutCommit(String compactionInstantTime) {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withAutoCommit(false) // disable auto commit
        .withRollbackUsingMarkers(true)
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
}
