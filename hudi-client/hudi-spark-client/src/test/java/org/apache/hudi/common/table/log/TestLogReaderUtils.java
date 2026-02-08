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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
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
      String firstCommit = client.createNewInstantTime(true);
      List<HoodieRecord> records1 = dataGen.generateInserts(firstCommit, 100);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 1);
      List<WriteStatus> statuses1 = client.insert(writeRecords1, firstCommit).collect();
      assertNoWriteErrors(statuses1);

      // Second commit - update data to create log files
      String secondCommit = client.createNewInstantTime(true);
      List<HoodieRecord> records2 = dataGen.generateUpdates(secondCommit, 50);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 1);
      List<WriteStatus> statuses2 = client.upsert(writeRecords2, secondCommit).collect();
      assertNoWriteErrors(statuses2);

      // Third commit - more updates
      String thirdCommit = client.createNewInstantTime(true);
      List<HoodieRecord> records3 = dataGen.generateUpdates(thirdCommit, 30);
      JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 1);
      List<WriteStatus> statuses3 = client.upsert(writeRecords3, thirdCommit).collect();
      assertNoWriteErrors(statuses3);

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
}
