/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LogReaderUtils}.
 */
public class TestLogReaderUtils extends SparkClientFunctionalTestHarness {

  @Test
  public void testGetAllLogFilesWithMaxCommit() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(basePath())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .compactionSmallFileSize(0)
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // First commit - insert data
      String firstCommit = "001";
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      List<HoodieRecord> records1 = dataGen.generateInserts(firstCommit, 100);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 1);
      JavaRDD<WriteStatus> statusesRdd1 = client.insert(writeRecords1, firstCommit);
      List<WriteStatus> statuses1 = statusesRdd1.collect();
      assertNoWriteErrors(statuses1);
      client.commit(firstCommit, statusesRdd1);

      // Second commit - update data to create log files
      String secondCommit = "002";
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      List<HoodieRecord> records2 = dataGen.generateUpdates(secondCommit, 50);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 1);
      JavaRDD<WriteStatus> statusesRdd2 = client.upsert(writeRecords2, secondCommit);
      List<WriteStatus> statuses2 = statusesRdd2.collect();
      assertNoWriteErrors(statuses2);
      assertLogFilesProduced(statuses2);
      client.commit(secondCommit, statusesRdd2);

      // Third commit - more updates
      String thirdCommit = "003";
      WriteClientTestUtils.startCommitWithTime(client, thirdCommit);
      List<HoodieRecord> records3 = dataGen.generateUpdates(thirdCommit, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 1);
      JavaRDD<WriteStatus> statusesRdd3 = client.upsert(writeRecords3, thirdCommit);
      List<WriteStatus> statuses3 = statusesRdd3.collect();
      assertNoWriteErrors(statuses3);
      assertLogFilesProduced(statuses3);
      client.commit(thirdCommit, statusesRdd3);

      // Reload metaClient to get latest timeline
      metaClient = HoodieTableMetaClient.reload(metaClient);

      // Get file system view
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(),
          metaClient,
          metaClient.getActiveTimeline().filterCompletedInstants());

      // Get all partitions
      List<String> partitions = Arrays.asList(dataGen.getPartitionPaths());

      // Test: Get all log files up to the second commit
      Map<HoodieLogFile, List<String>> logFilesWithMaxCommit = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient,
          fsView,
          partitions,
          secondCommit,
          context());

      // Verify results
      assertNotNull(logFilesWithMaxCommit);
      assertFalse(logFilesWithMaxCommit.isEmpty());

      // For a MOR table with updates, we should have log files
      // The method should filter out log files created after secondCommit
      for (Map.Entry<HoodieLogFile, List<String>> entry : logFilesWithMaxCommit.entrySet()) {
        HoodieLogFile logFile = entry.getKey();
        List<String> commitTimes = entry.getValue();

        assertNotNull(logFile);
        assertFalse(commitTimes.isEmpty(), "Each log file should have at least one associated commit time");

        // Verify all commit times are <= secondCommit
        for (String commitTime : commitTimes) {
          assertTrue(commitTime.compareTo(thirdCommit) < 0,
              "Commit time should be before third commit: " + commitTime);
        }
      }

      // Test with third commit - should include more log files
      Map<HoodieLogFile, List<String>> allLogFiles = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient,
          fsView,
          partitions,
          thirdCommit,
          context());

      assertNotNull(allLogFiles);
      // Should have same or more entries when including more commits
      assertTrue(allLogFiles.size() >= logFilesWithMaxCommit.size(),
          "Should have same or more log files when including third commit");
    }
  }

  @Test
  public void testGetAllLogFilesWithMaxCommitEmptyPartitions() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(),
        metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    // Test with empty partitions list
    Map<HoodieLogFile, List<String>> result = LogReaderUtils.getAllLogFilesWithMaxCommit(
        metaClient,
        fsView,
        Arrays.asList(),
        "000",
        context());

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map for empty partitions");
  }

  @Test
  public void testGetAllLogFilesWithArchivedCommit() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

    // Configure archival to archive after 4 commits, keeping minimum 2
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .compactionSmallFileSize(0)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(1)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 4)
            .build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(false)
            .build())
        .withEmbeddedTimelineServerEnabled(true)
        .forTable("test-trip-table")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // First commit - insert data (creates base files)
      String firstCommit = "001";
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      List<HoodieRecord> records1 = dataGen.generateInserts(firstCommit, 100);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 1);
      JavaRDD<WriteStatus> statusesRdd1 = client.insert(writeRecords1, firstCommit);
      List<WriteStatus> statuses1 = statusesRdd1.collect();
      assertNoWriteErrors(statuses1);
      client.commit(firstCommit, statusesRdd1);

      // Second commit - update data to create log files
      String secondCommit = "002";
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      List<HoodieRecord> records2 = dataGen.generateUpdates(secondCommit, 50);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 1);
      JavaRDD<WriteStatus> statusesRdd2 = client.upsert(writeRecords2, secondCommit);
      List<WriteStatus> statuses2 = statusesRdd2.collect();
      assertNoWriteErrors(statuses2);
      assertLogFilesProduced(statuses2);
      client.commit(secondCommit, statusesRdd2);

      // Third through sixth commits - more updates to trigger archival
      String thirdCommit = "003";
      WriteClientTestUtils.startCommitWithTime(client, thirdCommit);
      JavaRDD<WriteStatus> statusesRdd3 = client.upsert(jsc().parallelize(dataGen.generateUpdates(thirdCommit, 30), 1), thirdCommit);
      assertNoWriteErrors(statusesRdd3.collect());
      client.commit(thirdCommit, statusesRdd3);

      String fourthCommit = "004";
      WriteClientTestUtils.startCommitWithTime(client, fourthCommit);
      JavaRDD<WriteStatus> statusesRdd4 = client.upsert(jsc().parallelize(dataGen.generateUpdates(fourthCommit, 20), 1), fourthCommit);
      assertNoWriteErrors(statusesRdd4.collect());
      client.commit(fourthCommit, statusesRdd4);

      String fifthCommit = "005";
      WriteClientTestUtils.startCommitWithTime(client, fifthCommit);
      JavaRDD<WriteStatus> statusesRdd5 = client.upsert(jsc().parallelize(dataGen.generateUpdates(fifthCommit, 20), 1), fifthCommit);
      assertNoWriteErrors(statusesRdd5.collect());
      client.commit(fifthCommit, statusesRdd5);

      String sixthCommit = "006";
      WriteClientTestUtils.startCommitWithTime(client, sixthCommit);
      JavaRDD<WriteStatus> statusesRdd6 = client.upsert(jsc().parallelize(dataGen.generateUpdates(sixthCommit, 20), 1), sixthCommit);
      assertNoWriteErrors(statusesRdd6.collect());
      client.commit(sixthCommit, statusesRdd6);

      // Reload metaClient and trigger archival
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      TimelineArchiverV2 archiver = new TimelineArchiverV2(config, table);
      archiver.archiveIfRequired(context());

      // Reload metaClient after archival
      metaClient = HoodieTableMetaClient.reload(metaClient);

      // Verify that the second commit (which produced log files) has been archived
      assertFalse(metaClient.getActiveTimeline().containsInstant(secondCommit),
          "Second commit should be archived");

      // Get file system view using the active timeline
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(),
          metaClient,
          metaClient.getActiveTimeline().filterCompletedInstants());

      List<String> partitions = Arrays.asList(dataGen.getPartitionPaths());

      // Get all log files up to the sixth commit - should include log files from archived second commit
      Map<HoodieLogFile, List<String>> logFilesWithMaxCommit = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient,
          fsView,
          partitions,
          sixthCommit,
          context());

      // Verify results
      assertNotNull(logFilesWithMaxCommit);
      assertFalse(logFilesWithMaxCommit.isEmpty(),
          "Should return log files even though some commits are archived");

      // Verify that log files contain commit times from archived timeline
      boolean hasArchivedCommitTime = false;
      for (List<String> commitTimes : logFilesWithMaxCommit.values()) {
        assertFalse(commitTimes.isEmpty(), "Each log file should have at least one associated commit time");
        for (String commitTime : commitTimes) {
          if (metaClient.getActiveTimeline().isBeforeTimelineStarts(commitTime)) {
            hasArchivedCommitTime = true;
          }
        }
      }

      assertTrue(hasArchivedCommitTime,
          "Should have log files with commit times from archived timeline");
    }
  }

  private void assertLogFilesProduced(List<WriteStatus> statuses) {
    assertTrue(statuses.stream()
            .anyMatch(status -> FSUtils.isLogFile(new StoragePath(status.getStat().getPath()))),
        "Expected log files to be produced from upsert");
  }
}
