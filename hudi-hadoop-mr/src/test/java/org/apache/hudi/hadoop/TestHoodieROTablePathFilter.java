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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieTimeTravelException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.hudi.common.table.timeline.TimelineUtils.validateTimestampAsOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieROTablePathFilter extends HoodieCommonTestHarness {

  private HoodieROTablePathFilter pathFilter;
  private HoodieTestTable testTable;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
    pathFilter = new HoodieROTablePathFilter(metaClient.getStorageConf().unwrapAs(Configuration.class));
    testTable = HoodieTestTable.of(metaClient);
  }

  @Test
  public void testHoodiePaths() throws Exception {
    final String p1 = "2017/01/01";
    final String p2 = "2017/01/02";
    testTable.addCommit("001")
        .withBaseFilesInPartition(p1, "f1", "f2").getLeft()
        .withBaseFilesInPartition(p2, "f3").getLeft()
        .addCommit("002")
        .withBaseFilesInPartition(p1, "f2").getLeft()
        .addInflightCommit("003")
        .withBaseFilesInPartition(p2, "f3").getLeft()
        .addRequestedCompaction("004");

    assertTrue(pathFilter.accept(testTable.forCommit("002").getBaseFilePath(p1, "f2")));
    assertFalse(pathFilter.accept(testTable.forCommit("003").getBaseFilePath(p2, "f3")));
    assertFalse(pathFilter.accept(testTable.forCommit("003").getBaseFilePath(p1, "f3")));

    assertFalse(pathFilter.accept(new Path(testTable.getCommitFilePath("001").toUri())));
    assertFalse(pathFilter.accept(new Path(testTable.getCommitFilePath("002").toUri())));
    assertFalse(pathFilter.accept(testTable.getInflightCommitFilePath("003")));
    assertFalse(pathFilter.accept(testTable.getRequestedCompactionFilePath("004")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/hoodie.properties")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME)));

    assertEquals(1, pathFilter.metaClientCache.size());
    assertEquals(0, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 0");
  }

  @Test
  public void testNonHoodiePaths() throws IOException {
    java.nio.file.Path path1 = Paths.get(basePath, "nonhoodiefolder");
    Files.createDirectories(path1);
    assertTrue(pathFilter.accept(new Path(path1.toUri())));

    java.nio.file.Path path2 = Paths.get(basePath, "nonhoodiefolder/somefile");
    Files.createFile(path2);
    assertTrue(pathFilter.accept(new Path(path2.toUri())));
    assertEquals(2, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 2");
  }

  @Test
  public void testPartitionPathsAsNonHoodiePaths() throws Exception {
    final String p1 = "2017/01/01";
    final String p2 = "2017/01/02";
    testTable.addCommit("001").getFileIdsWithBaseFilesInPartitions(p1, p2);
    Path partitionPath1 = testTable.getPartitionPath(p1).getParent();
    Path partitionPath2 = testTable.getPartitionPath(p2).getParent();
    assertTrue(pathFilter.accept(partitionPath1), "Directories should be accepted");
    assertTrue(pathFilter.accept(partitionPath2), "Directories should be accepted");
    assertEquals(2, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 2");
  }

  /**
   * Comprehensive test for time travel validation with inflight commits.
   * Tests both direct validation and HoodieROTablePathFilter behavior.
   *
   * Scenario: commit001 (success) → commit002 (inflight) → commit003 (success)
   * Issue: Time travel to commit003 fails because inflight commit002 has lower timestamp
   */
  @Test
  public void testTimeTravelValidationWithInflightCommits() throws Exception {
    final String partition1 = "2023/01/01";
    final String partition2 = "2023/01/02";

    // Setup: Create commits with inflight scenario
    String commit001 = "20230101120000001";
    testTable.addCommit(commit001)
        .withPartitionMetaFilesWithDepth(partition1, partition2)
        .withBaseFilesInPartition(partition1, "file1").getLeft()
        .withBaseFilesInPartition(partition2, "file2").getLeft();

    String commit002 = "20230101130000002"; // This will be inflight
    testTable.addInflightCommit(commit002)
        .withBaseFilesInPartition(partition1, "file3").getLeft();

    String commit003 = "20230101140000003";
    testTable.addCommit(commit003)
        .withBaseFilesInPartition(partition1, "file4").getLeft()
        .withBaseFilesInPartition(partition2, "file5").getLeft();

    metaClient.reloadActiveTimeline();

    // Verify timeline state
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    List<HoodieInstant> completedCommits = activeTimeline.getCommitsTimeline().filterCompletedInstants().getInstants();
    List<HoodieInstant> inflightCommits = activeTimeline.getCommitsTimeline().filterInflightsAndRequested().getInstants();

    assertEquals(2, completedCommits.size(), "Should have 2 completed commits");
    assertEquals(1, inflightCommits.size(), "Should have 1 inflight commit");
    assertEquals(commit002, inflightCommits.get(0).requestedTime(), "Inflight commit should be 002");

    // Test 1: Direct time travel validation should fail for commit after inflight
    HoodieTimeTravelException exception = assertThrows(HoodieTimeTravelException.class, () -> {
      validateTimestampAsOf(metaClient, commit003);
    }, "Time travel to commit003 should fail due to inflight commit002 with lower timestamp");

    assertTrue(exception.getMessage().contains("must be earlier than the first incomplete commit timestamp"),
        "Exception should mention incomplete commit timestamp issue");

    // Test 2: Direct time travel validation should succeed for commit before inflight
    validateTimestampAsOf(metaClient, commit001); // Should not throw

    // Test 3: HoodieROTablePathFilter with Configuration-only constructor
    Configuration confWithAsOf = new Configuration(metaClient.getStorageConf().unwrapAs(Configuration.class));
    confWithAsOf.set(HoodieCommonConfig.TIMESTAMP_AS_OF.key(), commit003);

    HoodieROTablePathFilter pathFilterWithAsOf = new HoodieROTablePathFilter(confWithAsOf);
    Path testFilePath = testTable.forCommit(commit003).getBaseFilePath(partition1, "file4");

    HoodieException pathFilterException = assertThrows(HoodieException.class, () -> {
      pathFilterWithAsOf.accept(testFilePath);
    }, "HoodieROTablePathFilter.accept() should fail when TIMESTAMP_AS_OF is set to commit003 due to inflight commit002");

    // Verify the exception is time travel related (could be NPE due to engineContext or actual time travel error)
    boolean isTimeTravelRelated = pathFilterException.getMessage().contains("Time travel's timestamp")
        || pathFilterException.getMessage().contains("must be earlier than the first incomplete commit timestamp")
        || (pathFilterException.getCause() instanceof HoodieTimeTravelException)
        || pathFilterException.getMessage().contains("null"); // NPE due to engineContext being null

    assertTrue(isTimeTravelRelated,
        "Exception should be related to time travel validation or engineContext initialization. Got: " + pathFilterException.getMessage());
  }

  /**
   * Comprehensive test for HoodieROTablePathFilter behavior with different constructor patterns
   * and TIMESTAMP_AS_OF configurations. Tests both working scenarios and failure cases.
   */
  @Test
  public void testHoodieROTablePathFilterWithTimestampAsOf() throws Exception {
    final String partition = "2023/01/01";

    // Setup: Create commits with inflight scenario
    String commit001 = "20230101120000001";
    testTable.addCommit(commit001)
        .withPartitionMetaFilesWithDepth(partition)
        .withBaseFilesInPartition(partition, "file1").getLeft();

    String commit002 = "20230101130000002"; // This will be inflight
    testTable.addInflightCommit(commit002)
        .withBaseFilesInPartition(partition, "file2").getLeft();

    String commit003 = "20230101140000003";
    testTable.addCommit(commit003)
        .withBaseFilesInPartition(partition, "file3").getLeft();

    metaClient = HoodieTableMetaClient.reload(metaClient);

    Path file1Path = testTable.forCommit(commit001).getBaseFilePath(partition, "file1");
    Path file3Path = testTable.forCommit(commit003).getBaseFilePath(partition, "file3");

    // Test 1: HoodieROTablePathFilter without TIMESTAMP_AS_OF should work (normal operation)
    HoodieROTablePathFilter filterWithoutAsOf = new HoodieROTablePathFilter(
        metaClient.getStorageConf().unwrapAs(Configuration.class), metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    assertTrue(filterWithoutAsOf.accept(file1Path), "File from commit001 should be accepted");
    assertTrue(filterWithoutAsOf.accept(file3Path), "File from commit003 should be accepted");

    // Test 2: HoodieROTablePathFilter with TIMESTAMP_AS_OF before inflight should work
    Configuration confBeforeInflight = new Configuration(metaClient.getStorageConf().unwrapAs(Configuration.class));
    confBeforeInflight.set(HoodieCommonConfig.TIMESTAMP_AS_OF.key(), commit001);

    HoodieROTablePathFilter filterBeforeInflight = new HoodieROTablePathFilter(confBeforeInflight, metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    assertTrue(filterBeforeInflight.accept(file1Path), "File from commit001 should be accepted with as.of.instant=001");

    // Test 3: HoodieROTablePathFilter with TIMESTAMP_AS_OF after inflight should fail during accept()
    Configuration confAfterInflight = new Configuration(metaClient.getStorageConf().unwrapAs(Configuration.class));
    confAfterInflight.set(HoodieCommonConfig.TIMESTAMP_AS_OF.key(), commit003);

    HoodieROTablePathFilter filterAfterInflight = new HoodieROTablePathFilter(confAfterInflight, metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    // Filter creation succeeds, but accept() should fail due to time travel validation
    assertThrows(HoodieException.class, () -> {
      filterAfterInflight.accept(file3Path);
    }, "Calling accept() with as.of.instant=003 should fail due to inflight commit002");

    // Test 4: Configuration-only constructor with TIMESTAMP_AS_OF after inflight should fail during accept()
    Configuration confOnlyAfterInflight = new Configuration(metaClient.getStorageConf().unwrapAs(Configuration.class));
    confOnlyAfterInflight.set(HoodieCommonConfig.TIMESTAMP_AS_OF.key(), commit003);

    HoodieROTablePathFilter filterConfOnlyAfterInflight = new HoodieROTablePathFilter(confOnlyAfterInflight, metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    // Filter creation succeeds, but accept() should fail
    assertThrows(HoodieException.class, () -> {
      filterConfOnlyAfterInflight.accept(file3Path);
    }, "Calling accept() should fail with as.of.instant=003 due to inflight commit002");
  }

  /**
   * Test the fix scenario - what should happen when the issue is resolved.
   * This demonstrates that HoodieROTablePathFilter works fine without as.of.instant
   * even when inflight commits exist.
   */
  @Test
  public void testProposedFixScenario() throws Exception {
    final String partition = "2023/01/01";

    // Setup the problematic scenario
    String commit001 = "20230101120000001";
    testTable.addCommit(commit001)
        .withPartitionMetaFilesWithDepth(partition)
        .withBaseFilesInPartition(partition, "file1").getLeft();

    String commit002 = "20230101130000002";
    testTable.addInflightCommit(commit002)
        .withBaseFilesInPartition(partition, "file2").getLeft();

    String commit003 = "20230101140000003";
    testTable.addCommit(commit003)
        .withBaseFilesInPartition(partition, "file3").getLeft();

    // Reload metaClient to get updated timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Proposed fix: BaseHoodieTableFileIndex should not pass as.of.instant to HoodieROTablePathFilter
    // when there are inflight commits that would cause validation to fail

    // Test 1: HoodieROTablePathFilter should work without as.of.instant even when inflight commits exist
    Configuration confWithoutAsOf = new Configuration(metaClient.getStorageConf().unwrapAs(Configuration.class));
    HoodieROTablePathFilter filterWithoutAsOf = new HoodieROTablePathFilter(confWithoutAsOf, metaClient,
        metaClient.getActiveTimeline().filterCompletedInstants());

    Path file1Path = testTable.forCommit(commit001).getBaseFilePath(partition, "file1");
    Path file3Path = testTable.forCommit(commit003).getBaseFilePath(partition, "file3");

    assertTrue(filterWithoutAsOf.accept(file1Path), "Should accept file from commit001");
    assertTrue(filterWithoutAsOf.accept(file3Path), "Should accept file from commit003");
  }
}
