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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Objects;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "NINE", "TEN"})
  void testRestoreWithMetadataCompactionAfterEveryDeltaCommit(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT))
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .build();
    initMetaClient(HoodieTableType.COPY_ON_WRITE, hoodieWriteConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      final int iterations = 3;
      for (int i = 1; i <= iterations; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 1) {
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 1st commit");
        }
      }
      assertRowNumberEqualsTo(iterations * numRecords);
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(numRecords);
      assertEquals(tableVersion, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
      String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(metadataTableBasePath)
          .setConf(storageConf)
          .setLoadActiveTimelineOnLoad(true)
          .build();
      HoodieTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
      assertFalse(metadataTimeline.getCommitTimeline().filterCompletedInstants().empty());
      // Table version 6 restores the metadata table in place and keeps the delta commit of the savepointed commit.
      // Later versions rebuild it, and a compaction follows the bootstrap commits before the metadata restore is planned.
      int expectedDeltaCommits = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 1 : 2;
      assertEquals(expectedDeltaCommits, metadataTimeline.getDeltaCommitTimeline()
          .filter(instant -> !instant.requestedTime().startsWith(SOLO_COMMIT_TIMESTAMP)).countInstants());
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "NINE", "TEN"})
  void testRestoreToSavepointOlderThanMetadataTable(HoodieTableVersion tableVersion) throws Exception {
    HoodieWriteConfig configWithoutMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieWriteConfig configWithMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(tableVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT))
            .build())
        .build();
    initMetaClient(HoodieTableType.COPY_ON_WRITE, configWithMetadata.getProps());
    final int numRecords = 10;
    String savepointCommit;
    String prevInstant;
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithoutMetadata)) {
      savepointCommit = WriteClientTestUtils.createNewInstantTime();
      insertBatch(configWithoutMetadata, client, savepointCommit, HoodieTimeline.INIT_INSTANT_TS, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords, 1, Option.empty(), INSTANT_GENERATOR);
      client.savepoint("user1", "Savepoint for 1st commit");
      prevInstant = WriteClientTestUtils.createNewInstantTime();
      insertBatch(configWithoutMetadata, client, prevInstant, savepointCommit, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 2, 1, Option.empty(), INSTANT_GENERATOR);
    }
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithMetadata)) {
      String newCommitTime = WriteClientTestUtils.createNewInstantTime();
      insertBatch(configWithMetadata, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 3, 1, Option.empty(), INSTANT_GENERATOR);
      assertRowNumberEqualsTo(numRecords * 3);
      client.restoreToSavepoint(savepointCommit);
      assertRowNumberEqualsTo(numRecords);
      assertEquals(tableVersion, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(configWithMetadata.getBasePath()))
          .setConf(storageConf)
          .setLoadActiveTimelineOnLoad(true)
          .build();
      assertFalse(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline()
          .containsInstant(newCommitTime));
      String commitAfterRestore = WriteClientTestUtils.createNewInstantTime();
      insertBatch(configWithMetadata, client, commitAfterRestore, savepointCommit, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 2, 1, Option.empty(), INSTANT_GENERATOR);
      assertRowNumberEqualsTo(numRecords * 2);
    }
  }

  /**
   * A metadata table built after the savepoint that holds only its initialization delta commits is kept and restored in
   * place.
   */
  @Test
  void testRestoreKeepsAMetadataTableBuiltAfterTheSavepointWithoutLaterDeltaCommits() throws Exception {
    HoodieWriteConfig configWithoutMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieWriteConfig configWithMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    final int numRecords = 10;
    String savepointCommit = WriteClientTestUtils.createNewInstantTime();
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithoutMetadata)) {
      insertBatch(configWithoutMetadata, client, savepointCommit, HoodieTimeline.INIT_INSTANT_TS, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords, 1, Option.empty(), INSTANT_GENERATOR);
      client.savepoint("user1", "Savepoint for 1st commit");
      insertBatch(configWithoutMetadata, client, WriteClientTestUtils.createNewInstantTime(), savepointCommit, numRecords,
          SparkRDDWriteClient::insert, false, true, numRecords, numRecords * 2, 1, Option.empty(), INSTANT_GENERATOR);
    }
    syncTableMetadata(configWithMetadata);

    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithMetadata)) {
      assertFalse(client.deleteMdtIfNecessaryBeforeRestore(savepointCommit));
      client.restoreToSavepoint(savepointCommit);
      assertRowNumberEqualsTo(numRecords);
    }
  }

  /**
   * Before table version 8 a metadata table built after the restore target has no base file before the target. When it
   * also holds a delta commit of a data table instant, a restore that keeps it is refused.
   */
  @Test
  void testRestoreToInstantRefusesAMetadataTableWithDeltaCommitsAndNoBaseFileBeforeTheTarget() throws Exception {
    HoodieWriteConfig configWithoutMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieWriteConfig configWithMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .build();
    initMetaClient(HoodieTableType.COPY_ON_WRITE, configWithMetadata.getProps());
    final int numRecords = 10;
    String savepointCommit = WriteClientTestUtils.createNewInstantTime();
    String secondCommit = WriteClientTestUtils.createNewInstantTime();
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithoutMetadata)) {
      insertBatch(configWithoutMetadata, client, savepointCommit, HoodieTimeline.INIT_INSTANT_TS, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords, 1, Option.empty(), INSTANT_GENERATOR);
      client.savepoint("user1", "Savepoint for 1st commit");
      insertBatch(configWithoutMetadata, client, secondCommit, savepointCommit, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 2, 1, Option.empty(), INSTANT_GENERATOR);
    }
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithMetadata)) {
      insertBatch(configWithMetadata, client, WriteClientTestUtils.createNewInstantTime(), secondCommit, numRecords,
          SparkRDDWriteClient::insert, false, true, numRecords, numRecords * 3, 1, Option.empty(), INSTANT_GENERATOR);
      HoodieRestoreException e = assertThrows(HoodieRestoreException.class, () -> client.restoreToInstant(savepointCommit, true));
      assertInstanceOf(HoodieMetadataException.class, e.getCause());
      assertTrue(e.getCause().getMessage().contains("Please delete metadata table and retry"), e.getCause().getMessage());
    }
  }

  @Test
  void testRestoreToTheLatestCommitRollsBackNothing() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
      }
      client.savepoint("user1", "Savepoint for the latest commit");
      client.restoreToSavepoint(prevInstant);
      assertRowNumberEqualsTo(numRecords * 2);
      HoodieTimeline timeline = HoodieTableMetaClient.reload(metaClient).getActiveTimeline();
      HoodieInstant restore = timeline.getRestoreTimeline().filterCompletedInstants().lastInstant().get();
      assertTrue(timeline.readRestoreMetadata(restore).getInstantsToRollback().isEmpty());
    }
  }

  @Test
  void testRestoreTargetWithoutInstantKeepsMetadataTable() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
      }
      String timeWithoutInstant = WriteClientTestUtils.createNewInstantTime();
      StoragePath metadataTablePath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath()));
      assertFalse(client.deleteMdtIfNecessaryBeforeRestore(timeWithoutInstant));
      assertThrows(HoodieException.class, () -> client.restoreToSavepoint(timeWithoutInstant));
      assertTrue(HoodieStorageUtils.getStorage(metadataTablePath, storageConf).exists(metadataTablePath));
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
   * Two-assertion test for the MDT pre-check guard:
   * <ol>
   *   <li>{@code deleteMdtIfNecessaryBeforeRestore(firstCommit)} returns {@code true} (and
   *       deletes the MDT) when the target is at or before the oldest MDT compaction,
   *       confirming the guard logic fires for this table setup.</li>
   *   <li>{@code restoreToInstant(firstCommit, false)} does NOT invoke the pre-check, so a
   *       caller that explicitly opts out of MDT integration is never surprised by an implicit
   *       MDT deletion.</li>
   * </ol>
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

      String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      assertTrue(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should exist before any pre-check");

      // Assertion 1: deleteMdtIfNecessaryBeforeRestore detects that firstCommit is at or before
      // the oldest MDT compaction, deletes the MDT, and returns true.
      boolean mdtDeleted = client.deleteMdtIfNecessaryBeforeRestore(
          Objects.requireNonNull(firstCommit, "first commit should not be null"));
      assertTrue(mdtDeleted,
          "deleteMdtIfNecessaryBeforeRestore should return true when target is at or before the oldest MDT compaction");
      assertFalse(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should have been deleted by deleteMdtIfNecessaryBeforeRestore");

      // Assertion 2: restoreToInstant with initialMetadataTableIfNecessary=false does NOT invoke
      // the pre-check — the MDT (now absent) is not touched. The restore proceeds without MDT.
      client.restoreToInstant(firstCommit, false);
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
