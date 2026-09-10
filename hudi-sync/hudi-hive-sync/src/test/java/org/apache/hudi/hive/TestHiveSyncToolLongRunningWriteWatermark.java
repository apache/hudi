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

package org.apache.hudi.hive;

import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.testutils.HiveTestUtil.TABLE_NAME;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A partition written by a long-running INSERT_OVERWRITE must still be registered when the incremental
 * sync's watermark moved past the write's instant time while it was in flight. That only works when the
 * sync client persists and reads the commit completion-time watermark ({@code last_commit_completion_time_sync}),
 * which drives the "hollow instant" lookup in {@code TimelineUtils.getCommitsTimelineAfter}; a client that
 * tracks {@code last_commit_time_sync} alone silently and permanently drops the partition.
 */
public class TestHiveSyncToolLongRunningWriteWatermark {

  private static final String EXISTING_PARTITION_INSTANT = "100";
  private static final String LONG_RUNNING_INSERT_OVERWRITE_INSTANT = "101";
  private static final String EMPTY_COMMIT_DURING_WRITE = "102";
  private static final String EMPTY_COMMIT_AFTER_WRITE = "103";
  private static final String NEW_PARTITION = "2026/08/04";
  /** {@link org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor} maps {@link #NEW_PARTITION} to this datestr value. */
  private static final List<String> NEW_PARTITION_VALUES = Collections.singletonList("2026-08-04");

  private HiveSyncTool hiveSyncTool;
  private HoodieHiveSyncClient hiveClient;

  @BeforeEach
  void setUp() throws Exception {
    HiveTestUtil.setUp(Option.empty(), true);
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), "hms");
  }

  @AfterEach
  void teardown() throws Exception {
    closeHiveSyncTool();
    HiveTestUtil.clear();
  }

  @AfterAll
  static void cleanUpClass() throws IOException {
    HiveTestUtil.shutdown();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPartitionOfWriteCompletingAfterWatermarkAdvanced(boolean clientPersistsCompletionTime) throws Exception {
    // t=100: table with one partition, synced. Watermark = 100.
    HiveTestUtil.createCOWTable(EXISTING_PARTITION_INSTANT, 1, true);
    reSyncHiveTable(clientPersistsCompletionTime);
    assertEquals(1, hiveClient.getAllPartitions(TABLE_NAME).size());
    assertEquals(EXISTING_PARTITION_INSTANT, hiveClient.getLastCommitTimeSynced(TABLE_NAME).get());

    // t=101: INSERT_OVERWRITE into a brand-new partition starts (requested + inflight, files on storage)
    // and keeps running for a long time.
    HoodieReplaceCommitMetadata longRunningWrite =
        HiveTestUtil.startInsertOverwritePartition(NEW_PARTITION, LONG_RUNNING_INSERT_OVERWRITE_INSTANT);

    // t=102: a concurrent writer lands an empty commit (no partitions) while 101 is still in flight.
    HiveTestUtil.addEmptyCommit(EMPTY_COMMIT_DURING_WRITE);

    // The periodic sync runs now: 101 is pending so it is invisible, and the watermark jumps to 102.
    reSyncHiveTable(clientPersistsCompletionTime);
    assertEquals(1, hiveClient.getAllPartitions(TABLE_NAME).size(), "pending write must not be synced yet");
    assertEquals(EMPTY_COMMIT_DURING_WRITE, hiveClient.getLastCommitTimeSynced(TABLE_NAME).get());

    // 101 completes: its instant time is now BELOW the watermark, while its completion time (stamped into
    // the completed file's name at creation) is later than 102's.
    HiveTestUtil.createReplaceCommitFile(longRunningWrite, LONG_RUNNING_INSERT_OVERWRITE_INSTANT);
    assertTrue(HiveTestUtil.fileSystem.exists(new Path(HiveTestUtil.basePath, NEW_PARTITION)),
        "partition data is on storage");

    // Next sync cycle.
    reSyncHiveTable(clientPersistsCompletionTime);
    List<Partition> partitionsAfterCompletion = hiveClient.getAllPartitions(TABLE_NAME);

    // t=103: more unrelated commits keep arriving; every later sync advances the watermark further.
    HiveTestUtil.addEmptyCommit(EMPTY_COMMIT_AFTER_WRITE);
    reSyncHiveTable(clientPersistsCompletionTime);
    List<Partition> partitionsAfterLaterSyncs = hiveClient.getAllPartitions(TABLE_NAME);
    assertEquals(EMPTY_COMMIT_AFTER_WRITE, hiveClient.getLastCommitTimeSynced(TABLE_NAME).get());

    if (clientPersistsCompletionTime) {
      // Stock Hive client: the completion-time watermark rescues the hollow instant.
      assertEquals(2, partitionsAfterCompletion.size(), "hollow instant rescued by completion-time watermark");
      assertTrue(containsNewPartition(partitionsAfterCompletion), "the registered partition is the INSERT_OVERWRITE's");
      assertEquals(2, partitionsAfterLaterSyncs.size());
      assertTrue(containsNewPartition(partitionsAfterLaterSyncs));
    } else {
      // Instant-time-only client: the partition is on storage with a completed replacecommit, yet it is
      // never added - and the watermark keeps moving, so no future incremental sync will add it.
      assertEquals(1, partitionsAfterCompletion.size(), "partition of the late-completing write was skipped");
      assertFalse(containsNewPartition(partitionsAfterCompletion));
      assertEquals(1, partitionsAfterLaterSyncs.size(), "partition is permanently lost to incremental sync");
      assertFalse(containsNewPartition(partitionsAfterLaterSyncs));
      // read through a stock client to show the completion key was never written to the metastore
      try (HiveSyncTool stockTool = new HiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf())) {
        assertFalse(((HoodieHiveSyncClient) stockTool.syncClient).getLastCommitCompletionTimeSynced(TABLE_NAME).isPresent());
      }
    }
  }

  /** One cycle of a periodic sync job: a fresh tool (fresh timeline snapshot) per run. */
  private void reSyncHiveTable(boolean clientPersistsCompletionTime) {
    reInitHiveSyncClient(clientPersistsCompletionTime);
    hiveSyncTool.syncHoodieTable();
    reInitHiveSyncClient(clientPersistsCompletionTime);
  }

  private void reInitHiveSyncClient(boolean clientPersistsCompletionTime) {
    closeHiveSyncTool();
    hiveSyncTool = clientPersistsCompletionTime
        ? new HiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf())
        : new InstantTimeWatermarkHiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf());
    hiveClient = (HoodieHiveSyncClient) hiveSyncTool.syncClient;
  }

  private static boolean containsNewPartition(List<Partition> partitions) {
    return partitions.stream().anyMatch(partition -> NEW_PARTITION_VALUES.equals(partition.getValues()));
  }

  private void closeHiveSyncTool() {
    if (hiveSyncTool != null) {
      hiveSyncTool.close();
      hiveSyncTool = null;
    }
  }

  /** {@link HiveSyncTool} whose client tracks only the instant-time watermark. */
  private static class InstantTimeWatermarkHiveSyncTool extends HiveSyncTool {
    InstantTimeWatermarkHiveSyncTool(Properties props, Configuration hadoopConf) {
      super(props, hadoopConf);
    }

    @Override
    protected void initSyncClient(HiveSyncConfig config, HoodieTableMetaClient metaClient) {
      this.syncClient = new InstantTimeWatermarkSyncClient(config, metaClient);
    }
  }

  /** Writes only {@code last_commit_time_sync} and leaves {@code getLastCommitCompletionTimeSynced} at its default. */
  private static class InstantTimeWatermarkSyncClient extends HoodieHiveSyncClient {
    InstantTimeWatermarkSyncClient(HiveSyncConfig config, HoodieTableMetaClient metaClient) {
      super(config, metaClient);
    }

    @Override
    public Option<String> getLastCommitCompletionTimeSynced(String tableName) {
      return Option.empty();
    }

    @Override
    public void updateLastCommitTimeSynced(String tableName) {
      getActiveTimeline().lastInstant().ifPresent(last ->
          updateTableProperties(tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, last.requestedTime())));
    }
  }
}
