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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.TestHoodieClientBase;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for Async Compaction and Ingestion interaction.
 */
public class TestAsyncCompaction extends TestHoodieClientBase {

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  @Test
  public void testRollbackForInflightCompaction() throws Exception {
    // Rollback inflight compaction
    HoodieWriteConfig cfg = getConfig(false);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());

      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(),
          "Pending Compaction instant has expected instant time");
      assertEquals(State.REQUESTED, pendingCompactionInstant.getState(), "Pending Compaction instant has expected state");

      moveCompactionFromRequestedToInflight(compactionInstantTime, cfg);

      // Reload and rollback inflight compaction
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, hadoopConf);
      // hoodieTable.rollback(jsc,
      //    new HoodieInstant(true, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime), false);

      client.rollbackInflightCompaction(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime), hoodieTable);
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      pendingCompactionInstant = metaClient.getCommitsAndCompactionTimeline().filterPendingCompactionTimeline()
          .getInstants().findFirst().get();
      assertEquals("compaction", pendingCompactionInstant.getAction());
      assertEquals(State.REQUESTED, pendingCompactionInstant.getState());
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp());

      // We indirectly test for the race condition where a inflight instant was first deleted then created new. Every
      // time this happens, the pending compaction instant file in Hoodie Meta path becomes an empty file (Note: Hoodie
      // reads compaction plan from aux path which is untouched). TO test for regression, we simply get file status
      // and look at the file size
      FileStatus fstatus =
          metaClient.getFs().getFileStatus(new Path(metaClient.getMetaPath(), pendingCompactionInstant.getFileName()));
      assertTrue(fstatus.getLen() > 0);
    }
  }

  @Test
  public void testRollbackInflightIngestionWithPendingCompaction() throws Exception {
    // Rollback inflight ingestion when there is pending compaction
    HoodieWriteConfig cfg = getConfig(false);
    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "005";
    String inflightInstantTime = "006";
    String nextInflightInstantTime = "007";

    int numRecs = 2000;

    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(),
          "Pending Compaction instant has expected instant time");
      HoodieInstant inflightInstant =
          metaClient.getActiveTimeline().filterPendingExcludingCompaction().firstInstant().get();
      assertEquals(inflightInstantTime, inflightInstant.getTimestamp(), "inflight instant has expected instant time");

      // This should rollback
      client.startCommitWithTime(nextInflightInstantTime);

      // Validate
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      inflightInstant = metaClient.getActiveTimeline().filterPendingExcludingCompaction().firstInstant().get();
      assertEquals(inflightInstant.getTimestamp(), nextInflightInstantTime, "inflight instant has expected instant time");
      assertEquals(1, metaClient.getActiveTimeline()
              .filterPendingExcludingCompaction().getInstants().count(),
          "Expect only one inflight instant");
      // Expect pending Compaction to be present
      pendingCompactionInstant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(),
          "Pending Compaction instant has expected instant time");
    }
  }

  @Test
  public void testInflightCompaction() throws Exception {
    // There is inflight compaction. Subsequent compaction run must work correctly
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      String thirdInstantTime = "006";
      String fourthInstantTime = "007";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule and mark compaction instant as inflight
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);
      moveCompactionFromRequestedToInflight(compactionInstantTime, cfg);

      // Complete ingestions
      runNextDeltaCommits(client, readClient, Arrays.asList(thirdInstantTime, fourthInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));

      // execute inflight compaction
      executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
    }
  }

  @Test
  public void testScheduleIngestionBeforePendingCompaction() throws Exception {
    // Case: Failure case. Latest pending compaction instant time must be earlier than this instant time
    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);
    HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String failedInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    final List<HoodieRecord> initalRecords = dataGen.generateInserts(firstInstantTime, numRecs);
    final List<HoodieRecord> records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), initalRecords, cfg, true,
        new ArrayList<>());

    // Schedule compaction but do not run them
    scheduleCompaction(compactionInstantTime, client, cfg);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    HoodieInstant pendingCompactionInstant =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
    assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(), "Pending Compaction instant has expected instant time");

    assertThrows(IllegalArgumentException.class, () -> {
      runNextDeltaCommits(client, readClient, Arrays.asList(failedInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));
    }, "Latest pending compaction instant time must be earlier than this instant time");
  }

  @Test
  public void testScheduleCompactionAfterPendingIngestion() throws Exception {
    // Case: Failure case. Earliest ingestion inflight instant time must be later than compaction time

    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);
    HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String inflightInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

    metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    HoodieInstant inflightInstant =
        metaClient.getActiveTimeline().filterPendingExcludingCompaction().firstInstant().get();
    assertEquals(inflightInstantTime, inflightInstant.getTimestamp(), "inflight instant has expected instant time");

    assertThrows(IllegalArgumentException.class, () -> {
      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);
    }, "Earliest ingestion inflight instant time must be later than compaction time");
  }

  @Test
  public void testScheduleCompactionWithOlderOrSameTimestamp() throws Exception {
    // Case: Failure case. Earliest ingestion inflight instant time must be later than compaction time

    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);
    HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());

    final String firstInstantTime = "001";
    final String secondInstantTime = "004";
    final String compactionInstantTime = "002";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    assertThrows(IllegalArgumentException.class, () -> {
      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);
    }, "Compaction Instant to be scheduled cannot have older timestamp");

    // Schedule with timestamp same as that of committed instant
    assertThrows(IllegalArgumentException.class, () -> {
      // Schedule compaction but do not run them
      scheduleCompaction(secondInstantTime, client, cfg);
    }, "Compaction Instant to be scheduled cannot have same timestamp as committed instant");

    final String compactionInstantTime2 = "006";
    scheduleCompaction(compactionInstantTime2, client, cfg);
    assertThrows(IllegalArgumentException.class, () -> {
      // Schedule compaction with the same times as a pending compaction
      scheduleCompaction(secondInstantTime, client, cfg);
    }, "Compaction Instant to be scheduled cannot have same timestamp as a pending compaction");
  }

  @Test
  public void testCompactionAfterTwoDeltaCommits() throws Exception {
    // No Delta Commits after compaction request
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleAndExecuteCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, false);
    }
  }

  @Test
  public void testInterleavedCompaction() throws Exception {
    // Case: Two delta commits before and after compaction schedule
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      String thirdInstantTime = "006";
      String fourthInstantTime = "007";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);

      runNextDeltaCommits(client, readClient, Arrays.asList(thirdInstantTime, fourthInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));
      executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
    }
  }

  /**
   * HELPER METHODS FOR TESTING.
   **/

  private void validateDeltaCommit(String latestDeltaCommit,
      final Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToCompactionOperation,
      HoodieWriteConfig cfg) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    HoodieTable table = getHoodieTable(metaClient, cfg);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table);
    fileSliceList.forEach(fileSlice -> {
      Pair<String, HoodieCompactionOperation> opPair = fgIdToCompactionOperation.get(fileSlice.getFileGroupId());
      if (opPair != null) {
        assertEquals(fileSlice.getBaseInstantTime(), opPair.getKey(), "Expect baseInstant to match compaction Instant");
        assertTrue(fileSlice.getLogFiles().count() > 0,
            "Expect atleast one log file to be present where the latest delta commit was written");
        assertFalse(fileSlice.getBaseFile().isPresent(), "Expect no data-file to be present");
      } else {
        assertTrue(fileSlice.getBaseInstantTime().compareTo(latestDeltaCommit) <= 0,
            "Expect baseInstant to be less than or equal to latestDeltaCommit");
      }
    });
  }

  private List<HoodieRecord> runNextDeltaCommits(HoodieWriteClient client, final HoodieReadClient readClient, List<String> deltaInstants,
                                                 List<HoodieRecord> records, HoodieWriteConfig cfg, boolean insertFirst, List<String> expPendingCompactionInstants)
      throws Exception {

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    List<Pair<String, HoodieCompactionPlan>> pendingCompactions = readClient.getPendingCompactions();
    List<String> gotPendingCompactionInstants =
        pendingCompactions.stream().map(pc -> pc.getKey()).sorted().collect(Collectors.toList());
    assertEquals(expPendingCompactionInstants, gotPendingCompactionInstants);

    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToCompactionOperation =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);

    if (insertFirst) {
      // Use first instant for inserting records
      String firstInstant = deltaInstants.get(0);
      deltaInstants = deltaInstants.subList(1, deltaInstants.size());
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.startCommitWithTime(firstInstant);
      JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, firstInstant);
      List<WriteStatus> statusList = statuses.collect();

      if (!cfg.shouldAutoCommit()) {
        client.commit(firstInstant, statuses);
      }
      assertNoWriteErrors(statusList);
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      List<HoodieBaseFile> dataFilesToRead = getCurrentLatestDataFiles(hoodieTable, cfg);
      assertTrue(dataFilesToRead.stream().findAny().isPresent(),
          "should list the parquet files we wrote in the delta commit");
      validateDeltaCommit(firstInstant, fgIdToCompactionOperation, cfg);
    }

    int numRecords = records.size();
    for (String instantTime : deltaInstants) {
      records = dataGen.generateUpdates(instantTime, numRecords);
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      createNextDeltaCommit(instantTime, records, client, metaClient, cfg, false);
      validateDeltaCommit(instantTime, fgIdToCompactionOperation, cfg);
    }
    return records;
  }

  private void moveCompactionFromRequestedToInflight(String compactionInstantTime, HoodieWriteConfig cfg) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    HoodieInstant compactionInstant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compactionInstant);
    HoodieInstant instant = metaClient.getActiveTimeline().reload().filterPendingCompactionTimeline().getInstants()
        .filter(in -> in.getTimestamp().equals(compactionInstantTime)).findAny().get();
    assertTrue(instant.isInflight(), "Instant must be marked inflight");
  }

  private void scheduleCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieWriteConfig cfg)
      throws IOException {
    client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    HoodieInstant instant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().get();
    assertEquals(compactionInstantTime, instant.getTimestamp(), "Last compaction instant must be the one set");
  }

  private void scheduleAndExecuteCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieTable table,
      HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {
    scheduleCompaction(compactionInstantTime, client, cfg);
    executeCompaction(compactionInstantTime, client, table, cfg, expectedNumRecs, hasDeltaCommitAfterPendingCompaction);
  }

  private void executeCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieTable table,
      HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {

    client.compact(compactionInstantTime);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table);
    assertTrue(fileSliceList.stream().findAny().isPresent(), "Ensure latest file-slices are not empty");
    assertFalse(fileSliceList.stream()
            .anyMatch(fs -> !fs.getBaseInstantTime().equals(compactionInstantTime)),
        "Verify all file-slices have base-instant same as compaction instant");
    assertFalse(fileSliceList.stream().anyMatch(fs -> !fs.getBaseFile().isPresent()),
        "Verify all file-slices have data-files");

    if (hasDeltaCommitAfterPendingCompaction) {
      assertFalse(fileSliceList.stream().anyMatch(fs -> fs.getLogFiles().count() == 0),
          "Verify all file-slices have atleast one log-file");
    } else {
      assertFalse(fileSliceList.stream().anyMatch(fs -> fs.getLogFiles().count() > 0),
          "Verify all file-slices have no log-files");
    }

    // verify that there is a commit
    table = getHoodieTable(new HoodieTableMetaClient(hadoopConf, cfg.getBasePath(), true), cfg);
    HoodieTimeline timeline = table.getMetaClient().getCommitTimeline().filterCompletedInstants();
    String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
    assertEquals(latestCompactionCommitTime, compactionInstantTime,
        "Expect compaction instant time to be the latest commit time");
    assertEquals(expectedNumRecs,
        HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, "000").count(),
        "Must contain expected records");

  }

  private List<WriteStatus> createNextDeltaCommit(String instantTime, List<HoodieRecord> records,
      HoodieWriteClient client, HoodieTableMetaClient metaClient, HoodieWriteConfig cfg, boolean skipCommit) {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    client.startCommitWithTime(instantTime);

    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    List<WriteStatus> statusList = statuses.collect();
    assertNoWriteErrors(statusList);
    if (!cfg.shouldAutoCommit() && !skipCommit) {
      client.commit(instantTime, statuses);
    }

    Option<HoodieInstant> deltaCommit =
        metaClient.getActiveTimeline().reload().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
    if (skipCommit && !cfg.shouldAutoCommit()) {
      assertTrue(deltaCommit.get().getTimestamp().compareTo(instantTime) < 0,
          "Delta commit should not be latest instant");
    } else {
      assertTrue(deltaCommit.isPresent());
      assertEquals(instantTime, deltaCommit.get().getTimestamp(), "Delta commit should be latest instant");
    }
    return statusList;
  }

  private List<HoodieBaseFile> getCurrentLatestDataFiles(HoodieTable table, HoodieWriteConfig cfg) throws IOException {
    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(table.getMetaClient().getFs(), cfg.getBasePath());
    HoodieTableFileSystemView view =
        new HoodieTableFileSystemView(table.getMetaClient(), table.getCompletedCommitsTimeline(), allFiles);
    return view.getLatestBaseFiles().collect(Collectors.toList());
  }

  private List<FileSlice> getCurrentLatestFileSlices(HoodieTable table) {
    HoodieTableFileSystemView view = new HoodieTableFileSystemView(table.getMetaClient(),
        table.getMetaClient().getActiveTimeline().reload().getCommitsAndCompactionTimeline());
    return Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
        .flatMap(view::getLatestFileSlices).collect(Collectors.toList());
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
