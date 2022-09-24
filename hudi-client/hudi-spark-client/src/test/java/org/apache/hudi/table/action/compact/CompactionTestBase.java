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
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactionTestBase extends HoodieClientTestBase {

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(autoCommit)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).orcMaxFileSize(1024 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  /**
   * HELPER METHODS FOR TESTING.
   **/
  protected void validateDeltaCommit(String latestDeltaCommit, final Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToCompactionOperation,
                                     HoodieWriteConfig cfg) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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

  protected List<HoodieRecord> runNextDeltaCommits(SparkRDDWriteClient client, final SparkRDDReadClient readClient, List<String> deltaInstants,
                                                   List<HoodieRecord> records, HoodieWriteConfig cfg, boolean insertFirst, List<String> expPendingCompactionInstants)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
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
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      HoodieTable hoodieTable = getHoodieTable(metaClient, cfg);
      List<HoodieBaseFile> dataFilesToRead = getCurrentLatestBaseFiles(hoodieTable);
      assertTrue(dataFilesToRead.stream().findAny().isPresent(),
          "should list the base files we wrote in the delta commit");
      validateDeltaCommit(firstInstant, fgIdToCompactionOperation, cfg);
    }

    int numRecords = records.size();
    for (String instantTime : deltaInstants) {
      records = dataGen.generateUpdates(instantTime, numRecords);
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      createNextDeltaCommit(instantTime, records, client, metaClient, cfg, false);
      validateDeltaCommit(instantTime, fgIdToCompactionOperation, cfg);
    }
    return records;
  }

  protected void moveCompactionFromRequestedToInflight(String compactionInstantTime, HoodieWriteConfig cfg) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    HoodieInstant compactionInstant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compactionInstant);
    HoodieInstant instant = metaClient.getActiveTimeline().reload().filterPendingCompactionTimeline().getInstants()
        .filter(in -> in.getTimestamp().equals(compactionInstantTime)).findAny().get();
    assertTrue(instant.isInflight(), "Instant must be marked inflight");
  }

  protected void scheduleCompaction(String compactionInstantTime, SparkRDDWriteClient client, HoodieWriteConfig cfg) {
    client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    HoodieInstant instant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().get();
    assertEquals(compactionInstantTime, instant.getTimestamp(), "Last compaction instant must be the one set");
  }

  protected void scheduleAndExecuteCompaction(String compactionInstantTime, SparkRDDWriteClient client, HoodieTable table,
                                            HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {
    scheduleCompaction(compactionInstantTime, client, cfg);
    executeCompaction(compactionInstantTime, client, table, cfg, expectedNumRecs, hasDeltaCommitAfterPendingCompaction);
  }

  protected void executeCompaction(String compactionInstantTime, SparkRDDWriteClient client, HoodieTable table,
                                 HoodieWriteConfig cfg, int expectedNumRecs, boolean hasDeltaCommitAfterPendingCompaction) throws IOException {

    client.compact(compactionInstantTime);
    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, compactionInstantTime).doesMarkerDirExist());
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
    table = getHoodieTable(HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).setLoadActiveTimelineOnLoad(true).build(), cfg);
    HoodieTimeline timeline = table.getMetaClient().getCommitTimeline().filterCompletedInstants();
    String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
    assertEquals(latestCompactionCommitTime, compactionInstantTime,
        "Expect compaction instant time to be the latest commit time");
    assertEquals(expectedNumRecs,
        HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of("000")),
        "Must contain expected records");

  }

  protected void executeCompactionWithReplacedFiles(String compactionInstantTime, SparkRDDWriteClient client, HoodieTable table,
                                   HoodieWriteConfig cfg, String[] partitions, Set<HoodieFileGroupId> replacedFileIds) throws IOException {

    client.compact(compactionInstantTime);
    List<FileSlice> fileSliceList = getCurrentLatestFileSlices(table);
    assertTrue(fileSliceList.stream().findAny().isPresent(), "Ensure latest file-slices are not empty");
    assertFalse(fileSliceList.stream()
            .anyMatch(fs -> replacedFileIds.contains(fs.getFileGroupId())),
        "Compacted files should not show up in latest slices");

    // verify that there is a commit
    table = getHoodieTable(HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).setLoadActiveTimelineOnLoad(true).build(), cfg);
    HoodieTimeline timeline = table.getMetaClient().getCommitTimeline().filterCompletedInstants();
    // verify compaction commit is visible in timeline
    assertTrue(timeline.filterCompletedInstants().getInstants()
        .filter(instant -> compactionInstantTime.equals(instant.getTimestamp())).findFirst().isPresent());
    for (String partition: partitions) {
      table.getSliceView().getLatestFileSlicesBeforeOrOn(partition, compactionInstantTime, true).forEach(fs -> {
        // verify that all log files are merged
        assertEquals(0, fs.getLogFiles().count());
        assertTrue(fs.getBaseFile().isPresent());
      });
    }
  }

  protected List<WriteStatus> createNextDeltaCommit(String instantTime, List<HoodieRecord> records, SparkRDDWriteClient client,
                                                    HoodieTableMetaClient metaClient, HoodieWriteConfig cfg, boolean skipCommit) {
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

  protected List<HoodieBaseFile> getCurrentLatestBaseFiles(HoodieTable table) throws IOException {
    FileStatus[] allBaseFiles = HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles();
    HoodieTableFileSystemView view =
        getHoodieTableFileSystemView(table.getMetaClient(), table.getCompletedCommitsTimeline(), allBaseFiles);
    return view.getLatestBaseFiles().collect(Collectors.toList());
  }

  protected List<FileSlice> getCurrentLatestFileSlices(HoodieTable table) {
    HoodieTableFileSystemView view = new HoodieTableFileSystemView(table.getMetaClient(),
        table.getMetaClient().getActiveTimeline().reload().getWriteTimeline());
    return Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
        .flatMap(view::getLatestFileSlices).collect(Collectors.toList());
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
