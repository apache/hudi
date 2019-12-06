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

package org.apache.hudi;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.AvroUtils;
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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());

      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertTrue("Pending Compaction instant has expected instant time",
          pendingCompactionInstant.getTimestamp().equals(compactionInstantTime));
      assertTrue("Pending Compaction instant has expected state",
          pendingCompactionInstant.getState().equals(State.REQUESTED));

      moveCompactionFromRequestedToInflight(compactionInstantTime, client, cfg);

      // Reload and rollback inflight compaction
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
      hoodieTable.rollback(jsc, compactionInstantTime, false);

      client.rollbackInflightCompaction(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime), hoodieTable);
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
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

  private Path getInstantPath(HoodieTableMetaClient metaClient, String timestamp, String action, State state) {
    HoodieInstant instant = new HoodieInstant(state, action, timestamp);
    return new Path(metaClient.getMetaPath(), instant.getFileName());
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

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertTrue("Pending Compaction instant has expected instant time",
          pendingCompactionInstant.getTimestamp().equals(compactionInstantTime));
      HoodieInstant inflightInstant =
          metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().get();
      assertTrue("inflight instant has expected instant time",
          inflightInstant.getTimestamp().equals(inflightInstantTime));

      // This should rollback
      client.startCommitWithTime(nextInflightInstantTime);

      // Validate
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      inflightInstant = metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().get();
      assertTrue("inflight instant has expected instant time",
          inflightInstant.getTimestamp().equals(nextInflightInstantTime));
      assertTrue("Expect only one inflight instant",
          metaClient.getActiveTimeline().filterInflightsExcludingCompaction().getInstants().count() == 1);
      // Expect pending Compaction to be present
      pendingCompactionInstant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertTrue("Pending Compaction instant has expected instant time",
          pendingCompactionInstant.getTimestamp().equals(compactionInstantTime));
    }
  }

  @Test
  public void testInflightCompaction() throws Exception {
    // There is inflight compaction. Subsequent compaction run must work correctly
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {

      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      String thirdInstantTime = "006";
      String fourthInstantTime = "007";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule and mark compaction instant as inflight
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);
      moveCompactionFromRequestedToInflight(compactionInstantTime, client, cfg);

      // Complete ingestions
      runNextDeltaCommits(client, Arrays.asList(thirdInstantTime, fourthInstantTime), records, cfg, false,
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

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String failedInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    // Schedule compaction but do not run them
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    scheduleCompaction(compactionInstantTime, client, cfg);
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant pendingCompactionInstant =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
    assertTrue("Pending Compaction instant has expected instant time",
        pendingCompactionInstant.getTimestamp().equals(compactionInstantTime));

    boolean gotException = false;
    try {
      runNextDeltaCommits(client, Arrays.asList(failedInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));
    } catch (IllegalArgumentException iex) {
      // Latest pending compaction instant time must be earlier than this instant time. Should fail here
      gotException = true;
    }
    assertTrue("Latest pending compaction instant time must be earlier than this instant time", gotException);
  }

  @Test
  public void testScheduleCompactionAfterPendingIngestion() throws Exception {
    // Case: Failure case. Earliest ingestion inflight instant time must be later than compaction time

    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String inflightInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant inflightInstant =
        metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().get();
    assertTrue("inflight instant has expected instant time",
        inflightInstant.getTimestamp().equals(inflightInstantTime));

    boolean gotException = false;
    try {
      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);
    } catch (IllegalArgumentException iex) {
      // Earliest ingestion inflight instant time must be later than compaction time. Should fail here
      gotException = true;
    }
    assertTrue("Earliest ingestion inflight instant time must be later than compaction time", gotException);
  }

  @Test
  public void testScheduleCompactionWithOlderOrSameTimestamp() throws Exception {
    // Case: Failure case. Earliest ingestion inflight instant time must be later than compaction time

    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "002";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    boolean gotException = false;
    try {
      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);
    } catch (IllegalArgumentException iex) {
      gotException = true;
    }
    assertTrue("Compaction Instant to be scheduled cannot have older timestamp", gotException);

    // Schedule with timestamp same as that of committed instant
    gotException = false;
    String dupCompactionInstantTime = secondInstantTime;
    try {
      // Schedule compaction but do not run them
      scheduleCompaction(dupCompactionInstantTime, client, cfg);
    } catch (IllegalArgumentException iex) {
      gotException = true;
    }
    assertTrue("Compaction Instant to be scheduled cannot have same timestamp as committed instant", gotException);

    compactionInstantTime = "006";
    scheduleCompaction(compactionInstantTime, client, cfg);
    gotException = false;
    try {
      // Schedule compaction with the same times as a pending compaction
      scheduleCompaction(dupCompactionInstantTime, client, cfg);
    } catch (IllegalArgumentException iex) {
      gotException = true;
    }
    assertTrue("Compaction Instant to be scheduled cannot have same timestamp as a pending compaction", gotException);
  }

  @Test
  public void testCompactionAfterTwoDeltaCommits() throws Exception {
    // No Delta Commits after compaction request
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {

      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleAndExecuteCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, false);
    }
  }

  @Test
  public void testInterleavedCompaction() throws Exception {
    // Case: Two delta commits before and after compaction schedule
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getHoodieWriteClient(cfg, true);) {

      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      String thirdInstantTime = "006";
      String fourthInstantTime = "007";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);

      runNextDeltaCommits(client, Arrays.asList(thirdInstantTime, fourthInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));
      executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
    }
  }

  /**
   * HELPER METHODS FOR TESTING.
   **/

  private void validateDeltaCommit(String latestDeltaCommit,
      final Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToCompactionOperation,
      HoodieWriteConfig cfg) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable table = getHoodieTable(metaClient, cfg);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table, cfg);
    fileSliceList.forEach(fileSlice -> {
      Pair<String, HoodieCompactionOperation> opPair = fgIdToCompactionOperation.get(fileSlice.getFileGroupId());
      if (opPair != null) {
        assertTrue("Expect baseInstant to match compaction Instant",
            fileSlice.getBaseInstantTime().equals(opPair.getKey()));
        assertTrue("Expect atleast one log file to be present where the latest delta commit was written",
            fileSlice.getLogFiles().count() > 0);
        assertFalse("Expect no data-file to be present", fileSlice.getDataFile().isPresent());
      } else {
        assertTrue("Expect baseInstant to be less than or equal to latestDeltaCommit",
            fileSlice.getBaseInstantTime().compareTo(latestDeltaCommit) <= 0);
      }
    });
  }

  private List<HoodieRecord> runNextDeltaCommits(HoodieWriteClient client, List<String> deltaInstants,
      List<HoodieRecord> records, HoodieWriteConfig cfg, boolean insertFirst, List<String> expPendingCompactionInstants)
      throws Exception {

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingCompactions =
        CompactionUtils.getAllPendingCompactionPlans(metaClient);
    List<String> gotPendingCompactionInstants =
        pendingCompactions.stream().map(pc -> pc.getKey().getTimestamp()).sorted().collect(Collectors.toList());
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
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      List<HoodieDataFile> dataFilesToRead = getCurrentLatestDataFiles(hoodieTable, cfg);
      assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
          dataFilesToRead.stream().findAny().isPresent());
      validateDeltaCommit(firstInstant, fgIdToCompactionOperation, cfg);
    }

    int numRecords = records.size();
    for (String instantTime : deltaInstants) {
      records = dataGen.generateUpdates(instantTime, numRecords);
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      createNextDeltaCommit(instantTime, records, client, metaClient, cfg, false);
      validateDeltaCommit(instantTime, fgIdToCompactionOperation, cfg);
    }
    return records;
  }

  private void moveCompactionFromRequestedToInflight(String compactionInstantTime, HoodieWriteClient client,
      HoodieWriteConfig cfg) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant compactionInstant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    HoodieCompactionPlan workload = AvroUtils
        .deserializeCompactionPlan(metaClient.getActiveTimeline().getInstantAuxiliaryDetails(compactionInstant).get());
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compactionInstant);
    HoodieInstant instant = metaClient.getActiveTimeline().reload().filterPendingCompactionTimeline().getInstants()
        .filter(in -> in.getTimestamp().equals(compactionInstantTime)).findAny().get();
    assertTrue("Instant must be marked inflight", instant.isInflight());
  }

  private void scheduleCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieWriteConfig cfg)
      throws IOException {
    client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant instant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().get();
    assertEquals("Last compaction instant must be the one set", instant.getTimestamp(), compactionInstantTime);
  }

  private void scheduleAndExecuteCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieTable table,
      HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {
    scheduleCompaction(compactionInstantTime, client, cfg);
    executeCompaction(compactionInstantTime, client, table, cfg, expectedNumRecs, hasDeltaCommitAfterPendingCompaction);
  }

  private void executeCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieTable table,
      HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {

    client.compact(compactionInstantTime);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table, cfg);
    assertTrue("Ensure latest file-slices are not empty", fileSliceList.stream().findAny().isPresent());
    assertFalse("Verify all file-slices have base-instant same as compaction instant", fileSliceList.stream()
        .filter(fs -> !fs.getBaseInstantTime().equals(compactionInstantTime)).findAny().isPresent());
    assertFalse("Verify all file-slices have data-files",
        fileSliceList.stream().filter(fs -> !fs.getDataFile().isPresent()).findAny().isPresent());

    if (hasDeltaCommitAfterPendingCompaction) {
      assertFalse("Verify all file-slices have atleast one log-file",
          fileSliceList.stream().filter(fs -> fs.getLogFiles().count() == 0).findAny().isPresent());
    } else {
      assertFalse("Verify all file-slices have no log-files",
          fileSliceList.stream().filter(fs -> fs.getLogFiles().count() > 0).findAny().isPresent());
    }

    // verify that there is a commit
    table = getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath(), true), cfg);
    HoodieTimeline timeline = table.getMetaClient().getCommitTimeline().filterCompletedInstants();
    String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
    assertEquals("Expect compaction instant time to be the latest commit time", latestCompactionCommitTime,
        compactionInstantTime);
    Assert.assertEquals("Must contain expected records", expectedNumRecs,
        HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, "000").count());

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
      assertTrue("Delta commit should not be latest instant",
          deltaCommit.get().getTimestamp().compareTo(instantTime) < 0);
    } else {
      assertTrue(deltaCommit.isPresent());
      assertEquals("Delta commit should be latest instant", instantTime, deltaCommit.get().getTimestamp());
    }
    return statusList;
  }

  private List<HoodieDataFile> getCurrentLatestDataFiles(HoodieTable table, HoodieWriteConfig cfg) throws IOException {
    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(table.getMetaClient().getFs(), cfg.getBasePath());
    HoodieTableFileSystemView view =
        new HoodieTableFileSystemView(table.getMetaClient(), table.getCompletedCommitsTimeline(), allFiles);
    List<HoodieDataFile> dataFilesToRead = view.getLatestDataFiles().collect(Collectors.toList());
    return dataFilesToRead;
  }

  private List<FileSlice> getCurrentLatestFileSlices(HoodieTable table, HoodieWriteConfig cfg) throws IOException {
    HoodieTableFileSystemView view = new HoodieTableFileSystemView(table.getMetaClient(),
        table.getMetaClient().getActiveTimeline().reload().getCommitsAndCompactionTimeline());
    List<FileSlice> fileSliceList = Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).stream()
        .flatMap(partition -> view.getLatestFileSlices(partition)).collect(Collectors.toList());
    return fileSliceList;
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
