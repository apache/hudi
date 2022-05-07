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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for Async Compaction and Ingestion interaction.
 */
public class TestAsyncCompaction extends CompactionTestBase {

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit)
        .build();
  }

  @Test
  public void testRollbackForInflightCompaction() throws Exception {
    // Rollback inflight compaction
    HoodieWriteConfig cfg = getConfig(false);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
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

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();

      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(),
          "Pending Compaction instant has expected instant time");
      assertEquals(State.REQUESTED, pendingCompactionInstant.getState(), "Pending Compaction instant has expected state");

      moveCompactionFromRequestedToInflight(compactionInstantTime, cfg);

      // Reload and rollback inflight compaction
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      hoodieTable.rollbackInflightCompaction(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime));
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      // Schedule compaction but do not run them
      scheduleCompaction(compactionInstantTime, client, cfg);

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
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
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String failedInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    final List<HoodieRecord> initialRecords = dataGen.generateInserts(firstInstantTime, numRecs);
    final List<HoodieRecord> records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), initialRecords, cfg, true,
        new ArrayList<>());

    // Schedule compaction but do not run them
    scheduleCompaction(compactionInstantTime, client, cfg);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());

    String firstInstantTime = "001";
    String secondInstantTime = "004";
    String inflightInstantTime = "005";
    String compactionInstantTime = "006";
    int numRecs = 2000;

    List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
    records = runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
        new ArrayList<>());

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    createNextDeltaCommit(inflightInstantTime, records, client, metaClient, cfg, true);

    metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
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
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleAndExecuteCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, false);
    }
  }

  @Test
  public void testInterleavedCompaction() throws Exception {
    // Case: Two delta commits before and after compaction schedule
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
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

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);

      runNextDeltaCommits(client, readClient, Arrays.asList(thirdInstantTime, fourthInstantTime), records, cfg, false,
          Arrays.asList(compactionInstantTime));
      executeCompaction(compactionInstantTime, client, hoodieTable, cfg, numRecs, true);
    }
  }

  @Test
  public void testCompactionOnReplacedFiles() throws Exception {
    // Schedule a compaction. Replace those file groups and ensure compaction completes successfully.
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      String firstInstantTime = "001";
      String secondInstantTime = "004";
      String compactionInstantTime = "005";
      String replaceInstantTime = "006";
      String fourthInstantTime = "007";

      int numRecs = 2000;

      List<HoodieRecord> records = dataGen.generateInserts(firstInstantTime, numRecs);
      runNextDeltaCommits(client, readClient, Arrays.asList(firstInstantTime, secondInstantTime), records, cfg, true,
          new ArrayList<>());

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      scheduleCompaction(compactionInstantTime, client, cfg);
      metaClient.reloadActiveTimeline();
      HoodieInstant pendingCompactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant().get();
      assertEquals(compactionInstantTime, pendingCompactionInstant.getTimestamp(), "Pending Compaction instant has expected instant time");

      Set<HoodieFileGroupId> fileGroupsBeforeReplace = getAllFileGroups(hoodieTable, dataGen.getPartitionPaths());
      // replace by using insertOverwrite
      JavaRDD<HoodieRecord> replaceRecords = jsc.parallelize(dataGen.generateInserts(replaceInstantTime, numRecs), 1);
      client.startCommitWithTime(replaceInstantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      client.insertOverwrite(replaceRecords, replaceInstantTime);

      metaClient.reloadActiveTimeline();
      hoodieTable = getHoodieTable(metaClient, cfg);
      Set<HoodieFileGroupId> newFileGroups = getAllFileGroups(hoodieTable, dataGen.getPartitionPaths());
      // make sure earlier file groups are not visible
      assertEquals(0, newFileGroups.stream().filter(fg -> fileGroupsBeforeReplace.contains(fg)).count());

      // compaction should run with associated file groups are replaced
      executeCompactionWithReplacedFiles(compactionInstantTime, client, hoodieTable, cfg, dataGen.getPartitionPaths(), fileGroupsBeforeReplace);
    }
  }

  private Set<HoodieFileGroupId> getAllFileGroups(HoodieTable table, String[] partitions) {
    return Arrays.stream(partitions).flatMap(partition -> table.getSliceView().getLatestFileSlices(partition)
        .map(fg -> fg.getFileGroupId())).collect(Collectors.toSet());
  }
}
