/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.split;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link DefaultHoodieSplitDiscover}.
 */
public class TestDefaultHoodieSplitDiscover extends HoodieCommonTestHarness {

  @BeforeEach
  void init() {
    initPath();
  }

  @Test
  void testDiscoverSplitsWithNoNewInstants() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    metaClient.reloadActiveTimeline();
    HoodieTimeline commitsTimeline = metaClient.getActiveTimeline()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    String lastInstant = commitsTimeline.lastInstant().get().getCompletionTime();

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // Query with the last instant - should return empty or minimal splits
    HoodieContinuousSplitBatch result = discover.discoverSplits(lastInstant);

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
  }

  @Test
  void testDiscoverSplitsWithNewInstants() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    // Insert initial data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    metaClient.reloadActiveTimeline();
    HoodieTimeline commitsTimeline = metaClient.getActiveTimeline()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    HoodieInstant firstInstant = commitsTimeline.firstInstant().get();

    // Insert more data to create new instants
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    metaClient.reloadActiveTimeline();

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // Discover splits after the first instant
    HoodieContinuousSplitBatch result = discover.discoverSplits(firstInstant.getCompletionTime());

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
    assertNotNull(result.getOffset(), "To instant should not be null");
  }

  @Test
  void testDiscoverSplitsFromEarliest() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // Discover splits from null (earliest)
    HoodieContinuousSplitBatch result = discover.discoverSplits(null);

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDiscoverSplitsWithDifferentTableTypes(HoodieTableType tableType) throws Exception {
    metaClient = HoodieTestUtils.init(basePath, tableType);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    HoodieContinuousSplitBatch result = discover.discoverSplits(null);

    assertNotNull(result, "Result should not be null for table type: " + tableType);
    assertNotNull(result.getSplits(), "Splits should not be null for table type: " + tableType);
  }

  @Test
  void testDiscoverSplitsReturnsCorrectInstantRange() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    // Insert multiple commits
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    metaClient.reloadActiveTimeline();
    HoodieTimeline commitsTimeline = metaClient.getActiveTimeline()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    HoodieInstant firstInstant = commitsTimeline.firstInstant().get();
    String firstCompletionTime = firstInstant.getCompletionTime();

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    HoodieContinuousSplitBatch result = discover.discoverSplits(firstCompletionTime);

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getOffset(), "To instant should not be null");
  }

  @Test
  void testDiscoverSplitsWithSkipOptions() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieScanContext scanContext = createScanContextWithSkipOptions(conf, true, true, false);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    HoodieContinuousSplitBatch result = discover.discoverSplits(scanContext.getStartInstant());

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
  }

  @Test
  void testDiscoverSplitsConstructor() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    assertNotNull(discover, "Discover instance should not be null");
  }

  @Test
  void testDiscoverSplitsWithNonExistentTable() throws Exception {
    // Use a path that doesn't have a Hudi table initialized
    String nonExistentPath = basePath + "/non_existent_table";
    Configuration conf = TestConfigurations.getDefaultConf(nonExistentPath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // Should return empty batch when table doesn't exist
    HoodieContinuousSplitBatch result = discover.discoverSplits(null);

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
    // Empty batch should have no splits
  }

  @Test
  void testLazyMetaClientInitialization() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // First call should initialize metaClient and discover splits
    HoodieContinuousSplitBatch result1 = discover.discoverSplits(null);
    assertNotNull(result1, "First result should not be null");

    // Second call should reuse the same metaClient
    HoodieContinuousSplitBatch result2 = discover.discoverSplits(null);
    assertNotNull(result2, "Second result should not be null");
  }

  @Test
  void testDiscoverSplitsHandlesNullMetaClientGracefully() throws Exception {
    // Use a directory that exists but is not a Hudi table
    String emptyPath = basePath + "/empty_dir";
    new File(emptyPath).mkdirs();

    Configuration conf = TestConfigurations.getDefaultConf(emptyPath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    HoodieScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext);

    // Should handle null metaClient gracefully and return empty batch
    HoodieContinuousSplitBatch result = discover.discoverSplits("20230101000000");

    assertNotNull(result, "Result should not be null even with null metaClient");
    assertNotNull(result.getSplits(), "Splits should not be null");
  }

  @Test
  void testLagInfoComesFromTheCommitsThatWroteTheFileGroup() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    // the second commit only writes par1
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT.subList(0, 2), conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    List<HoodieInstant> commits = getCompletedCommits();
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));
    Collection<HoodieSourceSplit> splits = discover.discoverSplits(commits.get(0).getCompletionTime()).getSplits();

    Map<String, HoodieSourceSplitLagInfo> lagInfos = discover.getLagInfo(splits);

    assertFalse(splits.isEmpty());
    Map<String, Long> expectedRecords = getWrittenRecords(commits.subList(1, 3));
    for (HoodieSourceSplit split : splits) {
      HoodieSourceSplitLagInfo lagInfo = lagInfos.get(split.splitId());
      boolean par1 = split.getPartitionPath().equals("par1");
      assertEquals(par1 ? Arrays.asList(commits.get(1).requestedTime(), commits.get(2).requestedTime())
          : Collections.singletonList(commits.get(2).requestedTime()), lagInfo.getReadCommits());
      assertEquals((par1 ? commits.get(1) : commits.get(2)).getCompletionTime(), lagInfo.getOldestCommitCompletionTime());
      assertEquals(expectedRecords.get(split.getFileId()), lagInfo.getEstimatedRecords());
      assertEquals(new File(new StoragePath(split.getBasePath().get()).toUri().getPath()).length(), lagInfo.getEstimatedBytes());
    }
  }

  @Test
  void testLagInfoOfSplitsOfTheSameFileGroupFromSeveralBatches() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_COMMITS_LIMIT, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    List<HoodieInstant> commits = getCompletedCommits();
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));
    HoodieContinuousSplitBatch firstBatch = discover.discoverSplits(commits.get(0).getCompletionTime());
    HoodieContinuousSplitBatch secondBatch = discover.discoverSplits(firstBatch.getOffset());
    List<HoodieSourceSplit> splits = new ArrayList<>(firstBatch.getSplits());
    splits.addAll(secondBatch.getSplits());

    Map<String, HoodieSourceSplitLagInfo> lagInfos = discover.getLagInfo(splits);

    for (HoodieSourceSplit split : firstBatch.getSplits()) {
      assertEquals(Collections.singletonList(commits.get(1).requestedTime()), lagInfos.get(split.splitId()).getReadCommits());
    }
    for (HoodieSourceSplit split : secondBatch.getSplits()) {
      assertEquals(Collections.singletonList(commits.get(2).requestedTime()), lagInfos.get(split.splitId()).getReadCommits());
    }
  }

  @Test
  void testLagInfoOfSnapshotSplitsReadTheirLatestCommit() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    HoodieInstant commit = getCompletedCommits().get(0);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));
    Collection<HoodieSourceSplit> splits = discover.discoverSplits(null).getSplits();

    Map<String, HoodieSourceSplitLagInfo> lagInfos = discover.getLagInfo(splits);

    assertFalse(splits.isEmpty());
    for (HoodieSourceSplit split : splits) {
      assertEquals(new HoodieSourceSplitLagInfo(Collections.singletonList(commit.requestedTime()), commit.getCompletionTime(), 0L, 0L),
          lagInfos.get(split.splitId()));
    }
  }

  @Test
  void testGetUndiscoveredCommitsAfterACommitsLimitedDiscovery() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_COMMITS_LIMIT, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    List<HoodieInstant> commits = getCompletedCommits();
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));
    HoodieContinuousSplitBatch result = discover.discoverSplits(commits.get(0).getCompletionTime());

    List<String> undiscovered = discover.getUndiscoveredCommits(result.getOffset(), false);

    assertEquals(commits.get(1).getCompletionTime(), result.getOffset());
    assertEquals(Collections.singletonList(commits.get(2).getCompletionTime()), undiscovered);
  }

  @Test
  void testGetUndiscoveredCommitsReloadsTheTimeline() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));
    String offset = getCompletedCommits().get(0).getCompletionTime();
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    List<String> undiscovered = discover.getUndiscoveredCommits(offset, true);

    assertEquals(Collections.singletonList(getCompletedCommits().get(1).getCompletionTime()), undiscovered);
  }

  @Test
  void testGetUndiscoveredCommitsWithoutOffsetReturnsNothing() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(createScanContext(conf));

    List<String> undiscovered = discover.getUndiscoveredCommits(null, true);

    assertTrue(undiscovered.isEmpty());
  }

  // Helper methods

  private List<HoodieInstant> getCompletedCommits() {
    metaClient.reloadActiveTimeline();
    return metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  /**
   * Returns the inserts plus updates written by the commits, keyed by file id.
   */
  private Map<String, Long> getWrittenRecords(List<HoodieInstant> commits) throws Exception {
    Map<String, Long> records = new HashMap<>();
    for (HoodieInstant commit : commits) {
      HoodieCommitMetadata metadata = metaClient.getActiveTimeline().readCommitMetadata(commit);
      for (HoodieWriteStat writeStat : metadata.getWriteStats()) {
        records.merge(writeStat.getFileId(), writeStat.getNumInserts() + writeStat.getNumUpdateWrites(), Long::sum);
      }
    }
    return records;
  }

  private HoodieScanContext createScanContext(Configuration conf) throws Exception {
    return createScanContextWithSkipOptions(conf, false, false, false);
  }

  private HoodieScanContext createScanContextWithSkipOptions(
      Configuration conf,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite) throws Exception {
    return HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant(conf.get(FlinkOptions.READ_START_COMMIT))
        .endInstant("")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .cdcEnabled(false)
        .build();
  }
}
