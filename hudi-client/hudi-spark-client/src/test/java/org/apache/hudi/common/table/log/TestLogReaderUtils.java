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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
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
      JavaRDD<WriteStatus> insertRdd = client.insert(jsc().parallelize(dataGen.generateInserts(firstCommit, 100), 1), firstCommit);
      assertNoWriteErrors(insertRdd.collect());
      client.commit(firstCommit, insertRdd);

      // Second and third commits - upserts to create log files
      String[] upsertCommits = {"002", "003"};
      String secondCommit = upsertCommits[0];
      String thirdCommit = upsertCommits[1];
      for (String commitTime : upsertCommits) {
        WriteClientTestUtils.startCommitWithTime(client, commitTime);
        JavaRDD<WriteStatus> upsertRdd = client.upsert(jsc().parallelize(dataGen.generateUpdates(commitTime, 50), 1), commitTime);
        List<WriteStatus> statuses = upsertRdd.collect();
        assertNoWriteErrors(statuses);
        assertLogFilesProduced(statuses);
        client.commit(commitTime, upsertRdd);
      }

      metaClient = HoodieTableMetaClient.reload(metaClient);

      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(),
          metaClient,
          metaClient.getActiveTimeline().filterCompletedInstants());

      List<String> partitions = Arrays.asList(dataGen.getPartitionPaths());

      // Test: Get all log files up to the second commit
      Map<HoodieLogFile, List<String>> logFilesWithMaxCommit = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient, fsView, partitions, secondCommit, context());

      assertNotNull(logFilesWithMaxCommit);
      assertFalse(logFilesWithMaxCommit.isEmpty());

      for (Map.Entry<HoodieLogFile, List<String>> entry : logFilesWithMaxCommit.entrySet()) {
        assertNotNull(entry.getKey());
        List<String> commitTimes = entry.getValue();
        assertFalse(commitTimes.isEmpty(), "Each log file should have at least one associated commit time");
        for (String commitTime : commitTimes) {
          assertTrue(commitTime.compareTo(thirdCommit) < 0,
              "Commit time should be before third commit: " + commitTime);
        }
      }

      // Test with third commit - should include same or more log files
      Map<HoodieLogFile, List<String>> allLogFiles = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient, fsView, partitions, thirdCommit, context());

      assertNotNull(allLogFiles);
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

    Map<HoodieLogFile, List<String>> result = LogReaderUtils.getAllLogFilesWithMaxCommit(
        metaClient, fsView, Arrays.asList(), "000", context());

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map for empty partitions");
  }

  @Test
  public void testGetAllLogFilesWithArchivedCommit() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());

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
      JavaRDD<WriteStatus> insertRdd = client.insert(jsc().parallelize(dataGen.generateInserts(firstCommit, 100), 1), firstCommit);
      assertNoWriteErrors(insertRdd.collect());
      client.commit(firstCommit, insertRdd);

      // Upsert commits to create log files and trigger archival (002-006)
      String[] upsertCommits = {"002", "003", "004", "005", "006"};
      for (String commitTime : upsertCommits) {
        WriteClientTestUtils.startCommitWithTime(client, commitTime);
        JavaRDD<WriteStatus> rdd = client.upsert(jsc().parallelize(dataGen.generateUpdates(commitTime, 20), 1), commitTime);
        List<WriteStatus> statuses = rdd.collect();
        assertNoWriteErrors(statuses);
        assertLogFilesProduced(statuses);
        client.commit(commitTime, rdd);
      }

      String sixthCommit = upsertCommits[upsertCommits.length - 1];

      // Trigger archival
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      new TimelineArchiverV2(config, table).archiveIfRequired(context());
      metaClient = HoodieTableMetaClient.reload(metaClient);

      assertFalse(metaClient.getActiveTimeline().containsInstant(upsertCommits[0]),
          "Second commit should be archived");

      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(),
          metaClient,
          metaClient.getActiveTimeline().filterCompletedInstants());

      List<String> partitions = Arrays.asList(dataGen.getPartitionPaths());

      Map<HoodieLogFile, List<String>> logFilesWithMaxCommit = LogReaderUtils.getAllLogFilesWithMaxCommit(
          metaClient, fsView, partitions, sixthCommit, context());

      assertNotNull(logFilesWithMaxCommit);
      assertFalse(logFilesWithMaxCommit.isEmpty(),
          "Should return log files even though some commits are archived");

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
