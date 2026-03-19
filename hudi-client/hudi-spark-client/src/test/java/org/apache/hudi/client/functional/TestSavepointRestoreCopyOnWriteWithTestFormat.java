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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for COPY_ON_WRITE table savepoint restore.
 */
@Tag("functional")
public class TestSavepointRestoreCopyOnWriteWithTestFormat extends HoodieClientTestBase {

  /**
   * Actions: C1, C2, savepoint C2, C3, C4 (inflight or complete), restore.
   * Should go back to C2,
   * C3 and C4 should be cleaned up.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBasicRollback(boolean commitC4) throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    initMetaClient(getTableType(), props);
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      List<String> commitTimestamps = new ArrayList<>();
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        commitTimestamps.add(newCommitTime);
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      // Add C4 - either complete or inflight based on parameter
      if (commitC4) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        commitTimestamps.add(newCommitTime);
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * 4, 1, Option.empty(), INSTANT_GENERATOR);
        assertRowNumberEqualsTo(40);
      } else {
        // Leave C4 as inflight
        String inflightCommit = insertBatchWithoutCommit(numRecords);
        commitTimestamps.add(inflightCommit);
        Assertions.assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(inflightCommit));
        assertRowNumberEqualsTo(30);
      }
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      Assertions.assertEquals(1, metaClient.reloadActiveTimeline().getRestoreTimeline().filterCompletedInstants().countInstants());
      commitTimestamps.subList(2, 4).forEach(instant -> Assertions.assertFalse(metaClient.getActiveTimeline().containsInstant(instant)));
      assertRowNumberEqualsTo(20);
    }
  }

  @Test
  void testRollbackBeyondLastMDTCompaction() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    initMetaClient(getTableType(), props);
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
      Assertions.assertEquals(1, metaClient.reloadActiveTimeline().getRestoreTimeline().filterCompletedInstants().countInstants());
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
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    initMetaClient(getTableType(), props);
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
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
      String inflightCommit = insertBatchWithoutCommit(numRecords);
      Assertions.assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(inflightCommit));
      // rollback the pending instant
      if (commitRollback) {
        client.rollbackFailedWrites(metaClient);
      } else {
        HoodieInstant pendingInstant = metaClient.getActiveTimeline().filterPendingExcludingCompaction()
            .lastInstant().orElseThrow(() -> new HoodieException("Pending instant does not exist"));
        HoodieSparkTable.create(client.getConfig(), context, client.getTransactionManager())
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
      Assertions.assertEquals(1, metaClient.reloadActiveTimeline().getRestoreTimeline().filterCompletedInstants().countInstants());
      assertRowNumberEqualsTo(20);
    }
  }
}
