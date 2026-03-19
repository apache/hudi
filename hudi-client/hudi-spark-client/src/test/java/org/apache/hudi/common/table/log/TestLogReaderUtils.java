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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
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
    // Create a MERGE_ON_READ table to generate log files
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(basePath())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // First commit - insert data
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      List<HoodieRecord> records1 = dataGen.generateInserts(firstCommit, 100);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 1);
      List<WriteStatus> statuses1 = client.insert(writeRecords1, firstCommit).collect();
      assertNoWriteErrors(statuses1);

      // Second commit - update data to create log files
      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      List<HoodieRecord> records2 = dataGen.generateUpdates(secondCommit, 50);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 1);
      List<WriteStatus> statuses2 = client.upsert(writeRecords2, secondCommit).collect();
      assertNoWriteErrors(statuses2);
      assertLogFilesProduced(metaClient, secondCommit);

      // Third commit - more updates
      String thirdCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, thirdCommit);
      List<HoodieRecord> records3 = dataGen.generateUpdates(thirdCommit, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 1);
      List<WriteStatus> statuses3 = client.upsert(writeRecords3, thirdCommit).collect();
      assertNoWriteErrors(statuses3);
      assertLogFilesProduced(metaClient, thirdCommit);

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
    // Create a MERGE_ON_READ table
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
    // Create a MERGE_ON_READ table to generate log files
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

    // Configure archival to archive after 4 commits, keeping minimum 2
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(basePath())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 4)
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // First commit - insert data (creates base files)
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      List<HoodieRecord> records1 = dataGen.generateInserts(firstCommit, 100);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 1);
      List<WriteStatus> statuses1 = client.insert(writeRecords1, firstCommit).collect();
      assertNoWriteErrors(statuses1);

      // Second commit - update data to create log files
      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      List<HoodieRecord> records2 = dataGen.generateUpdates(secondCommit, 50);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 1);
      List<WriteStatus> statuses2 = client.upsert(writeRecords2, secondCommit).collect();
      assertNoWriteErrors(statuses2);
      assertLogFilesProduced(metaClient, secondCommit);

      // Third through sixth commits - more updates to trigger archival
      String thirdCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, thirdCommit);
      List<WriteStatus> statuses3 = client.upsert(jsc().parallelize(dataGen.generateUpdates(thirdCommit, 30), 1), thirdCommit).collect();
      assertNoWriteErrors(statuses3);

      String fourthCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, fourthCommit);
      List<WriteStatus> statuses4 = client.upsert(jsc().parallelize(dataGen.generateUpdates(fourthCommit, 20), 1), fourthCommit).collect();
      assertNoWriteErrors(statuses4);

      String fifthCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, fifthCommit);
      List<WriteStatus> statuses5 = client.upsert(jsc().parallelize(dataGen.generateUpdates(fifthCommit, 20), 1), fifthCommit).collect();
      assertNoWriteErrors(statuses5);

      String sixthCommit = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, sixthCommit);
      List<WriteStatus> statuses6 = client.upsert(jsc().parallelize(dataGen.generateUpdates(sixthCommit, 20), 1), sixthCommit).collect();
      assertNoWriteErrors(statuses6);

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

  private void assertLogFilesProduced(HoodieTableMetaClient metaClient, String commitTime) throws Exception {
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitMetadata commitMetadata = reloadedMetaClient.getActiveTimeline()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .readCommitMetadata(reloadedMetaClient.getActiveTimeline()
            .getCommitsTimeline()
            .filterCompletedInstants()
            .filter(instant -> instant.requestedTime().equals(commitTime))
            .firstInstant().get());
    assertTrue(commitMetadata.getWriteStats().stream()
            .anyMatch(stat -> FSUtils.isLogFile(stat.getPath())),
        "Expected log files to be produced from upsert");
  }
}
