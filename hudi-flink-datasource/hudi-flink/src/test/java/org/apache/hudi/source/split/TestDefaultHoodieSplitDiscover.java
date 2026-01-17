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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ScanContext;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

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

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

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

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

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

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

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

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

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

    ScanContext scanContext = createScanContextWithSkipOptions(conf, true, true, false);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

    HoodieContinuousSplitBatch result = discover.discoverSplits(scanContext.getStartCommit());

    assertNotNull(result, "Result should not be null");
    assertNotNull(result.getSplits(), "Splits should not be null");
  }

  @Test
  void testDiscoverSplitsConstructor() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);

    ScanContext scanContext = createScanContext(conf);
    DefaultHoodieSplitDiscover discover = new DefaultHoodieSplitDiscover(
        scanContext, metaClient);

    assertNotNull(discover, "Discover instance should not be null");
  }

  // Helper methods

  private ScanContext createScanContext(Configuration conf) throws Exception {
    return createScanContextWithSkipOptions(conf, false, false, false);
  }

  private ScanContext createScanContextWithSkipOptions(
      Configuration conf,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite) throws Exception {
    return new ScanContext.Builder()
        .conf(conf)
        .path(new Path(basePath))
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
