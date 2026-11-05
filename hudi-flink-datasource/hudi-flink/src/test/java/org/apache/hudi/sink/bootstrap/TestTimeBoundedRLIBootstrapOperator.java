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

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TimeBoundedRLIBootstrapOperator}.
 */
public class TestTimeBoundedRLIBootstrapOperator {

  @TempDir
  File tempFile;

  @Test
  void testSkipPreloadWhenBootstrapDaysIsNonPositive() throws Exception {
    Configuration conf = getTimeBoundedRLIConf();
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS, 0);
    StreamerUtil.initTableIfNotExists(conf);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new TimeBoundedRLIBootstrapOperator(conf), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
  }

  @Test
  void testSkipPreloadForFreshTableWithoutMetadataTable() throws Exception {
    Configuration conf = getTimeBoundedRLIConf();
    StreamerUtil.initTableIfNotExists(conf);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new TimeBoundedRLIBootstrapOperator(conf), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
  }

  @Test
  void testFailFastWhenMetadataTableIsMarkedAvailableButCannotBeLoaded() throws Exception {
    Configuration conf = getTimeBoundedRLIConf();
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    metaClient.getTableConfig().setMetadataPartitionState(metaClient, MetadataPartitionType.FILES.getPartitionPath(), true);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new TimeBoundedRLIBootstrapOperator(conf), 1, 1, 0)) {
      RuntimeException error = assertThrows(RuntimeException.class, harness::open);

      assertEquals("Can not initialize the table metadata", error.getMessage());
    }
  }

  @Test
  void testFilterPartitionsInWindowWithDefaultDayFormat() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    TimeBoundedRLIBootstrapOperator operator = new TimeBoundedRLIBootstrapOperator(conf);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DAY);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    int bootstrapDays = 3;

    String todayPartition = today.format(formatter);
    String oneDayAgoPartition = today.minusDays(1).format(formatter);
    String cutoffPartition = today.minusDays(bootstrapDays).format(formatter);
    String justInsideWindowPartition = today.minusDays(bootstrapDays - 1).format(formatter);
    String futurePartition = today.plusDays(1).format(formatter);
    String unparsablePartition = "not-a-date";

    List<String> partitionPaths = Arrays.asList(
        todayPartition, oneDayAgoPartition, cutoffPartition, justInsideWindowPartition, futurePartition, unparsablePartition);

    List<String> result = operator.filterPartitionsInWindow(partitionPaths, bootstrapDays);

    assertEquals(3, result.size());
    assertTrue(result.contains(todayPartition));
    assertTrue(result.contains(oneDayAgoPartition));
    assertTrue(result.contains(justInsideWindowPartition));
    assertFalse(result.contains(cutoffPartition));
    assertFalse(result.contains(futurePartition));
    assertFalse(result.contains(unparsablePartition));
  }

  @Test
  void testFilterPartitionsInWindowWithHiveStylePartitioning() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    TimeBoundedRLIBootstrapOperator operator = new TimeBoundedRLIBootstrapOperator(conf);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DAY);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    int bootstrapDays = 3;

    String todayPartition = "dt=" + today.format(formatter);
    String outsideWindowPartition = "dt=" + today.minusDays(bootstrapDays).format(formatter);

    List<String> result = operator.filterPartitionsInWindow(Arrays.asList(todayPartition, outsideWindowPartition), bootstrapDays);

    assertEquals(1, result.size());
    assertTrue(result.contains(todayPartition));
  }

  @Test
  void testFilterPartitionsInWindowWithCustomPartitionFormat() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    TimeBoundedRLIBootstrapOperator operator = new TimeBoundedRLIBootstrapOperator(conf);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    int bootstrapDays = 1;

    String todayPartition = today.format(formatter);
    String outsideWindowPartition = today.minusDays(2).format(formatter);

    List<String> result = operator.filterPartitionsInWindow(Arrays.asList(todayPartition, outsideWindowPartition), bootstrapDays);

    assertEquals(1, result.size());
    assertTrue(result.contains(todayPartition));
  }

  @Test
  void testFilterPartitionsInWindowWithLocalTimezone() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.WRITE_UTC_TIMEZONE, false);
    TimeBoundedRLIBootstrapOperator operator = new TimeBoundedRLIBootstrapOperator(conf);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DAY);
    LocalDate today = LocalDate.now();
    int bootstrapDays = 1;

    String todayPartition = today.format(formatter);

    List<String> result = operator.filterPartitionsInWindow(Arrays.asList(todayPartition), bootstrapDays);

    assertEquals(1, result.size());
    assertTrue(result.contains(todayPartition));
  }

  @Test
  void testShouldLoadBucketMatchesPartitionIndexFunc() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    TimeBoundedRLIBootstrapOperator operator = new TimeBoundedRLIBootstrapOperator(conf);

    int parallelism = 4;
    Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
    operator.partitionIndexFunc = partitionIndexFunc;

    String partitionPath = "20260101";
    int fileGroupCount = 5;

    for (int fileGroupIdx = 0; fileGroupIdx < fileGroupCount; fileGroupIdx++) {
      int expectedTask = partitionIndexFunc.apply(fileGroupCount, partitionPath, fileGroupIdx);
      for (int taskID = 0; taskID < parallelism; taskID++) {
        assertEquals(expectedTask == taskID, operator.shouldLoadBucket(partitionPath, fileGroupCount, fileGroupIdx, taskID));
      }
    }
  }

  private Configuration getTimeBoundedRLIConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS, 3);
    return conf;
  }
}
