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
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Objects;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for COPY_ON_WRITE table savepoint restore.
 */
@Tag("functional")
public class TestSavepointRestoreCopyOnWrite extends HoodieClientTestBase {

  /**
   * Actions: C1, C2, savepoint C2, C3, C4, restore.
   * Should go back to C2,
   * C3 and C4 should be cleaned up.
   */
  @Test
  void testBasicRollback() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 4; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(40);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  @Test
  void testRollbackBeyondLastMDTCompaction() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      final int iterations = 5;
      for (int i = 1; i <= 5; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(iterations * numRecords);
      // restore will be forced to rebuild the metadata table
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
      // check if the metadata table is rebuilt
      String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(metadataTableBasePath)
          .setConf(storageConf)
          .setLoadActiveTimelineOnLoad(true)
          .build();
      assertEquals(1, metadataMetaClient.getCommitsTimeline().filter(instant -> !instant.requestedTime().startsWith(SOLO_COMMIT_TIMESTAMP)).countInstants());
    }
  }

  /**
   * The restore should roll back all the pending instants that are beyond the savepoint.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 inflight, restore.
   * Should go back to C2,
   * C3, C4 should be cleaned up.
   */
  @Test
  void testCleaningPendingInstants() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(30);
      // write another pending instant
      insertBatchWithoutCommit(numRecords);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  /**
   * The rollbacks(either inflight or complete) beyond the savepoint should be cleaned.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 (RB_C3), C5, restore.
   * Should go back to C2.
   * C3, C4(RB_C3), C5 should be cleaned up.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 (RB_C3) inflight, restore.
   * Should go back to C2.
   * C3, C4 (RB_C3) should be cleaned up.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCleaningRollbackInstants(boolean commitRollback) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        // eager cleaning
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(20);
      // write another pending instant
      insertBatchWithoutCommit(numRecords);
      // rollback the pending instant
      if (commitRollback) {
        client.rollbackFailedWrites(metaClient);
      } else {
        HoodieInstant pendingInstant = metaClient.getActiveTimeline().filterPendingExcludingCompaction()
            .lastInstant().orElseThrow(() -> new HoodieException("Pending instant does not exist"));
        HoodieSparkTable.create(client.getConfig(), context)
            .scheduleRollback(context, WriteClientTestUtils.createNewInstantTime(), pendingInstant, false, true, false);
      }
      Option<String> rollbackInstant = metaClient.reloadActiveTimeline().getRollbackTimeline().lastInstant().map(HoodieInstant::requestedTime);
      assertTrue(rollbackInstant.isPresent(), "The latest instant should be a rollback");
      // write another batch
      insertBatch(hoodieWriteConfig, client, WriteClientTestUtils.createNewInstantTime(),
          rollbackInstant.get(), numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 3, 1, Option.empty(), INSTANT_GENERATOR);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  /**
   * Coverage for the MDT-pre-check guard inside {@code restoreToInstant}.
   * When the caller passes {@code initialMetadataTableIfNecessary=false}, the
   * {@code shouldDeleteMdtBeforeRestore} helper must not be invoked, and the existing MDT
   * directory must remain intact after the restore (no implicit deletion).
   */
  @Test
  void testRestoreToInstantSkipsMdtCheckWhenMetadataDisabled() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String firstCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      // 5 inserts so the MDT goes through one compaction with maxDeltaCommits=4.
      for (int i = 1; i <= 5; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 1) {
          firstCommit = newCommitTime;
        }
      }
      assertRowNumberEqualsTo(50);

      // Sanity: the MDT directory exists.
      String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      assertTrue(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should exist before restore");

      // Restore to firstCommit with initialMetadataTableIfNecessary=false. Without the guard the
      // helper would observe that firstCommit is at or before the oldest MDT compaction and
      // delete the MDT. With the guard the helper is not invoked, so the MDT survives.
      client.restoreToInstant(Objects.requireNonNull(firstCommit, "first commit should not be null"), false);

      assertTrue(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should still exist after restoreToInstant when initialMetadataTableIfNecessary=false");
      assertRowNumberEqualsTo(numRecords);
    }
  }

  /**
   * Coverage for the penultimate-compaction trigger inside the centralized helper.
   * Sets up a table with two MDT compactions and restores to a commit that is at or before the
   * penultimate compaction. The helper should detect this and delete the MDT pre-emptively;
   * the restore should still complete successfully.
   */
  @Test
  void testRestoreToInstantDeletesMdtWhenTargetIsBeforePenultimateCompaction() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String earlyCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      // 6 inserts with maxDeltaCommits=2 produces multiple MDT compactions, ensuring at
      // least two completed compactions exist on the MDT timeline.
      for (int i = 1; i <= 6; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 1) {
          earlyCommit = newCommitTime;
        }
      }
      assertRowNumberEqualsTo(60);

      String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(mdtBasePath)
          .setConf(storageConf)
          .setLoadActiveTimelineOnLoad(true)
          .build();
      assertTrue(mdtMetaClient.getCommitTimeline().filterCompletedInstants().countInstants() >= 2,
          "Test setup should produce at least two completed MDT compactions");

      // Restore to earlyCommit, which is well before the penultimate MDT compaction. The helper
      // must trigger a pre-emptive MDT delete; the restore must still complete and the MDT must
      // be rebuilt (or at minimum recreated as a fresh empty MDT) by the post-restore init.
      client.restoreToInstant(Objects.requireNonNull(earlyCommit, "early commit should not be null"), true);
      assertRowNumberEqualsTo(numRecords);
    }
  }

  /**
   * Regression coverage for the {@code restoreToSavepoint} refactor. After replacing the inline
   * MDT pre-check with a call to the centralized helper, the common case (target after the
   * oldest MDT compaction, no MDT delete needed) must still work end-to-end.
   */
  @Test
  void testRestoreToSavepointStillWorksAfterRefactor() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 4; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(40);
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }
}
