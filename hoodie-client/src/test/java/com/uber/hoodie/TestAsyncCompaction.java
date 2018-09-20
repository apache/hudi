/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

/**
 * Test Cases for Async Compaction and Ingestion interaction
 */
public class TestAsyncCompaction extends TestHoodieClientBase {

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true).withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024).withInlineCompaction(false)
                .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Override
  public void tearDown() throws IOException {
    super.tearDown();
  }

  @Test
  public void testRollbackInflightIngestionWithPendingCompaction() throws Exception {
    // Rollback inflight ingestion when there is pending compaction
    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "005";
    String inflightInstantTime = "006";
    String nextInflightInstantTime = "007";

    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

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

    //This should rollback
    client.startCommitWithTime(nextInflightInstantTime);

    //Validate
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    inflightInstant =
        metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().get();
    assertTrue("inflight instant has expected instant time",
        inflightInstant.getTimestamp().equals(nextInflightInstantTime));
    assertTrue("Expect only one inflight instant",
        metaClient.getActiveTimeline().filterInflightsExcludingCompaction().getInstants().count() == 1);
    //Expect pending Compaction to be present
    pendingCompactionInstant =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
    assertTrue("Pending Compaction instant has expected instant time",
        pendingCompactionInstant.getTimestamp().equals(compactionInstantTime));
  }

  @Test
  public void testInflightCompaction() throws Exception {
    // There is inflight compaction. Subsequent compaction run must work correctly
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "005";
    String thirdInstantTime = "006";
    String fourthInstantTime = "007";

    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

    // Schedule and mark compaction instant as inflight
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    scheduleCompaction(compactionInstantTime, client, cfg);
    moveCompactionFromRequestedToInflight(compactionInstantTime, client, cfg);

    // Complete ingestions
    runNextDeltaCommits(client, Arrays.asList(thirdInstantTime, fourthInstantTime),
        records, cfg, false, Arrays.asList(compactionInstantTime));

    // execute inflight compaction
    executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
  }

  @Test
  public void testScheduleIngestionBeforePendingCompaction() throws Exception {
    // Case: Failure case. Latest pending compaction instant time must be earlier than this instant time
    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String failedInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

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
      runNextDeltaCommits(client, Arrays.asList(failedInstantTime),
          records, cfg, false, Arrays.asList(compactionInstantTime));
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
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String inflightInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

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
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "002";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

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
    assertTrue("Compaction Instant to be scheduled cannot have same timestamp as committed instant",
        gotException);

    compactionInstantTime = "006";
    scheduleCompaction(compactionInstantTime, client, cfg);
    gotException = false;
    try {
      // Schedule compaction with the same times as a pending compaction
      scheduleCompaction(dupCompactionInstantTime, client, cfg);
    } catch (IllegalArgumentException iex) {
      gotException = true;
    }
    assertTrue("Compaction Instant to be scheduled cannot have same timestamp as a pending compaction",
        gotException);
  }

  @Test
  public void testCompactionAfterTwoDeltaCommits() throws Exception {
    // No Delta Commits after compaction request
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "005";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    scheduleAndExecuteCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, false);
  }

  @Test
  public void testInterleavedCompaction() throws Exception {
    //Case: Two delta commits before and after compaction schedule
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg, true);

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String compactionInstantTime = "005";
    String thirdInstantTime = "006";
    String fourthInstantTime = "007";

    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, Arrays.asList(firstInstantTime, secondInstantTime),
        records, cfg, true, new ArrayList<>());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    scheduleCompaction(compactionInstantTime, client, cfg);

    runNextDeltaCommits(client, Arrays.asList(thirdInstantTime, fourthInstantTime),
        records, cfg, false, Arrays.asList(compactionInstantTime));
    executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
  }

  /**
   * HELPER METHODS FOR TESTING
   **/

  private void validateDeltaCommit(String latestDeltaCommit,
      final Map<String, Pair<String, HoodieCompactionOperation>> fileIdToCompactionOperation,
      HoodieWriteConfig cfg) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table, cfg);
    fileSliceList.forEach(fileSlice -> {
      Pair<String, HoodieCompactionOperation> opPair = fileIdToCompactionOperation.get(fileSlice.getFileId());
      if (opPair != null) {
        System.out.println("FileSlice :" + fileSlice);
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
      List<HoodieRecord> records, HoodieWriteConfig cfg, boolean insertFirst,
      List<String> expPendingCompactionInstants) throws Exception {

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingCompactions =
        CompactionUtils.getAllPendingCompactionPlans(metaClient);
    List<String> gotPendingCompactionInstants =
        pendingCompactions.stream().map(pc -> pc.getKey().getTimestamp()).sorted().collect(Collectors.toList());
    assertEquals(expPendingCompactionInstants, gotPendingCompactionInstants);

    Map<String, Pair<String, HoodieCompactionOperation>> fileIdToCompactionOperation =
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
      TestHoodieClientBase.assertNoWriteErrors(statusList);
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
      List<HoodieDataFile> dataFilesToRead = getCurrentLatestDataFiles(hoodieTable, cfg);
      assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
          dataFilesToRead.stream().findAny().isPresent());
      validateDeltaCommit(firstInstant, fileIdToCompactionOperation, cfg);
    }

    int numRecords = records.size();
    for (String instantTime : deltaInstants) {
      records = dataGen.generateUpdates(instantTime, numRecords);
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      createNextDeltaCommit(instantTime, records, client, metaClient, cfg, false);
      validateDeltaCommit(instantTime, fileIdToCompactionOperation, cfg);
    }
    return records;
  }

  private void moveCompactionFromRequestedToInflight(String compactionInstantTime, HoodieWriteClient client,
      HoodieWriteConfig cfg) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant compactionInstant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    HoodieCompactionPlan workload = AvroUtils.deserializeCompactionPlan(
        metaClient.getActiveTimeline().getInstantAuxiliaryDetails(compactionInstant).get());
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compactionInstant);
    HoodieInstant instant = metaClient.getActiveTimeline().reload().filterPendingCompactionTimeline().getInstants()
        .filter(in -> in.getTimestamp().equals(compactionInstantTime)).findAny().get();
    assertTrue("Instant must be marked inflight", instant.isInflight());
  }

  private void scheduleCompaction(String compactionInstantTime, HoodieWriteClient client, HoodieWriteConfig cfg)
      throws IOException {
    client.scheduleCompactionAtInstant(compactionInstantTime, Optional.empty());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieInstant instant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().get();
    assertEquals("Last compaction instant must be the one set",
        instant.getTimestamp(), compactionInstantTime);
  }

  private void scheduleAndExecuteCompaction(String compactionInstantTime,
      HoodieWriteClient client, HoodieTable table, HoodieWriteConfig cfg, int expectedNumRecs,
      boolean hasDeltaCommitAfterPendingCompaction) throws IOException {
    scheduleCompaction(compactionInstantTime, client, cfg);
    executeCompaction(compactionInstantTime, client, table, cfg, expectedNumRecs, hasDeltaCommitAfterPendingCompaction);
  }

  private void executeCompaction(String compactionInstantTime,
      HoodieWriteClient client, HoodieTable table, HoodieWriteConfig cfg, int expectedNumRecs,
      boolean hasDeltaCommitAfterPendingCompaction) throws IOException {

    client.compact(compactionInstantTime);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table, cfg);
    assertTrue("Ensure latest file-slices are not empty", fileSliceList.stream().findAny().isPresent());
    assertFalse("Verify all file-slices have base-instant same as compaction instant",
        fileSliceList.stream().filter(fs -> !fs.getBaseInstantTime().equals(compactionInstantTime))
            .findAny().isPresent());
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
    table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath(), true), cfg, jsc);
    HoodieTimeline timeline = table.getMetaClient().getCommitTimeline().filterCompletedInstants();
    String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
    assertEquals("Expect compaction instant time to be the latest commit time",
        latestCompactionCommitTime, compactionInstantTime);
    assertEquals("Must contain expected records", expectedNumRecs,
        HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, "000").count());

  }

  private List<WriteStatus> createNextDeltaCommit(String instantTime, List<HoodieRecord> records,
      HoodieWriteClient client, HoodieTableMetaClient metaClient, HoodieWriteConfig cfg, boolean skipCommit) {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    client.startCommitWithTime(instantTime);

    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    List<WriteStatus> statusList = statuses.collect();
    TestHoodieClientBase.assertNoWriteErrors(statusList);
    if (!cfg.shouldAutoCommit() && !skipCommit) {
      client.commit(instantTime, statuses);
    }

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().reload().getDeltaCommitTimeline()
        .filterCompletedInstants().lastInstant();
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
    HoodieTableFileSystemView
        view = new HoodieTableFileSystemView(table.getMetaClient(), table.getCompletedCommitTimeline(), allFiles);
    List<HoodieDataFile> dataFilesToRead = view.getLatestDataFiles().collect(Collectors.toList());
    return dataFilesToRead;
  }

  private List<FileSlice> getCurrentLatestFileSlices(HoodieTable table, HoodieWriteConfig cfg) throws IOException {
    HoodieTableFileSystemView view = new HoodieTableFileSystemView(table.getMetaClient(),
        table.getMetaClient().getActiveTimeline().reload().getCommitsAndCompactionTimeline());
    List<FileSlice> fileSliceList =
        Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).stream().flatMap(partition ->
            view.getLatestFileSlices(partition)).collect(Collectors.toList());
    return fileSliceList;
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
