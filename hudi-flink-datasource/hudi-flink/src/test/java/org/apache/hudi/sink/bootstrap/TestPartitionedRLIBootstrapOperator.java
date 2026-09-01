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
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PartitionedRLIBootstrapOperator}.
 */
public class TestPartitionedRLIBootstrapOperator {

  @TempDir
  File tempFile;

  @Test
  void testSkipPreloadWhenBootstrapDaysIsZero() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS, 0);
    StreamerUtil.initTableIfNotExists(conf);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new PartitionedRLIBootstrapOperator(conf), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
  }

  @Test
  void testSkipPreloadForFreshTableWithoutMetadataTable() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    StreamerUtil.initTableIfNotExists(conf);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new PartitionedRLIBootstrapOperator(conf), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
  }

  @Test
  void testFailFastWhenMetadataTableIsMarkedAvailableButCannotBeLoaded() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    metaClient.getTableConfig().setMetadataPartitionState(metaClient, MetadataPartitionType.FILES.getPartitionPath(), true);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new PartitionedRLIBootstrapOperator(conf), 1, 1, 0)) {
      RuntimeException error = assertThrows(RuntimeException.class, harness::open);

      assertEquals("Can not initialize the table metadata", error.getMessage());
    }
  }

  @Test
  void testFilterPartitionsInWindowKeepsOnlyRecentDatePartitions() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);

    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(conf);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);

    String todayPartition = today.format(formatter);
    String withinWindowPartition = today.minusDays(2).format(formatter);
    String cutoffBoundaryPartition = today.minusDays(7).format(formatter);
    String outsideWindowPartition = today.minusDays(10).format(formatter);

    List<String> partitionsInWindow = operator.filterPartitionsInWindow(
        Arrays.asList(todayPartition, withinWindowPartition, cutoffBoundaryPartition, outsideWindowPartition),
        7);

    assertEquals(2, partitionsInWindow.size());
    assertTrue(partitionsInWindow.contains(todayPartition));
    assertTrue(partitionsInWindow.contains(withinWindowPartition));
  }

  @Test
  void testFilterPartitionsInWindowSkipsNonDatePartitionPaths() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);

    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(conf);

    List<String> partitionsInWindow = operator.filterPartitionsInWindow(
        Arrays.asList("not-a-date", "americas"), 7);

    assertEquals(0, partitionsInWindow.size());
  }

  @Test
  void testFilterPartitionsInWindowHandlesHiveStylePartitioning() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);

    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(conf);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    String hiveStylePartition = "dt=" + today.format(formatter);

    List<String> partitionsInWindow = operator.filterPartitionsInWindow(Arrays.asList(hiveStylePartition), 7);

    assertEquals(1, partitionsInWindow.size());
    assertTrue(partitionsInWindow.contains(hiveStylePartition));
  }

  @Test
  void testSkipPreloadWhenRecordIndexPartitionNotAvailable() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    StreamerUtil.initTableIfNotExists(conf);

    HoodieBackedTableMetadata mockMetadata = mock(HoodieBackedTableMetadata.class);
    when(mockMetadata.enabled()).thenReturn(true);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(operatorWithMockedMetadata(conf, mockMetadata), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
    verify(mockMetadata, never()).getBucketizedFileGroupsForPartitionedRLI(any());
  }

  @Test
  void testPreloadEmitsRecordsOnlyForPartitionsWithinWindow() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    metaClient.getTableConfig().setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), true);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    String withinWindowPartition = today.format(formatter);
    String outsideWindowPartition = today.minusDays(30).format(formatter);

    Map<String, List<FileSlice>> partitionedFileGroups = new HashMap<>();
    partitionedFileGroups.put(withinWindowPartition, Collections.singletonList(fileSlice(withinWindowPartition, "f1")));
    partitionedFileGroups.put(outsideWindowPartition, Collections.singletonList(fileSlice(outsideWindowPartition, "f2")));

    HoodieBackedTableMetadata mockMetadata = mock(HoodieBackedTableMetadata.class);
    when(mockMetadata.enabled()).thenReturn(true);
    when(mockMetadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX)).thenReturn(partitionedFileGroups);
    when(mockMetadata.readRecordIndexLocations(any(SerializableFunctionUnchecked.class))).thenReturn(HoodieListPairData.eager(Collections.singletonList(
        Pair.of("key1", new HoodieRecordGlobalLocation(withinWindowPartition, "001", "f1")))));

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(operatorWithMockedMetadata(conf, mockMetadata), 1, 1, 0)) {
      harness.open();

      List<HoodieFlinkInternalRow> output = harness.extractOutputValues();
      assertEquals(1, output.size());
      assertEquals("key1", output.get(0).getRecordKey());
      assertEquals(withinWindowPartition, output.get(0).getPartitionPath());
      assertEquals("f1", output.get(0).getFileId());
    }
    verify(mockMetadata, times(1)).readRecordIndexLocations(any(SerializableFunctionUnchecked.class));
  }

  @Test
  void testPreloadFiltersFileSlicesForSingleTaskParallelism() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    metaClient.getTableConfig().setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), true);

    String partitionPath = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY));
    List<FileSlice> fileSlices = Arrays.asList(fileSlice(partitionPath, "f0"), fileSlice(partitionPath, "f1"));
    Map<String, List<FileSlice>> partitionedFileGroups = new HashMap<>();
    partitionedFileGroups.put(partitionPath, fileSlices);

    HoodieBackedTableMetadata mockMetadata = mock(HoodieBackedTableMetadata.class);
    when(mockMetadata.enabled()).thenReturn(true);
    when(mockMetadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX)).thenReturn(partitionedFileGroups);
    when(mockMetadata.readRecordIndexLocations(any(SerializableFunctionUnchecked.class))).thenReturn(HoodieListPairData.eager(Collections.emptyList()));

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(operatorWithMockedMetadata(conf, mockMetadata), 1, 1, 0)) {
      harness.open();
    }

    // With a single task handling all parallelism buckets, every file slice in the partition is passed through.
    ArgumentCaptor<SerializableFunctionUnchecked<List<FileSlice>, List<FileSlice>>> filterCaptor = ArgumentCaptor.forClass(SerializableFunctionUnchecked.class);
    verify(mockMetadata).readRecordIndexLocations(filterCaptor.capture());
    List<FileSlice> filtered = filterCaptor.getValue().apply(fileSlices);

    assertEquals(2, filtered.size());
  }

  @Test
  void testShouldLoadBucketRoundRobinsFileGroupsAcrossTasks() {
    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(getPartitionedRLIConf());

    assertTrue(operator.shouldLoadBucket(0, 2, 0));
    assertFalse(operator.shouldLoadBucket(1, 2, 0));
    assertFalse(operator.shouldLoadBucket(0, 2, 1));
    assertTrue(operator.shouldLoadBucket(1, 2, 1));
  }

  @Test
  void testFilterPartitionsInWindowExcludesFutureDatedPartitions() throws Exception {
    Configuration conf = getPartitionedRLIConf();
    conf.set(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);

    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(conf);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    String futurePartition = LocalDate.now(ZoneOffset.UTC).plusDays(1).format(formatter);

    List<String> partitionsInWindow = operator.filterPartitionsInWindow(Arrays.asList(futurePartition), 7);

    assertEquals(0, partitionsInWindow.size());
  }

  @Test
  void testFilterPartitionsInWindowUsesDefaultDayFormatWhenUnset() throws Exception {
    Configuration conf = getPartitionedRLIConf();

    PartitionedRLIBootstrapOperator operator = new PartitionedRLIBootstrapOperator(conf);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DAY);
    String todayPartition = LocalDate.now().format(formatter);

    List<String> partitionsInWindow = operator.filterPartitionsInWindow(Arrays.asList(todayPartition), 7);

    assertEquals(1, partitionsInWindow.size());
    assertTrue(partitionsInWindow.contains(todayPartition));
  }

  private static FileSlice fileSlice(String partitionPath, String fileId) {
    return new FileSlice(new HoodieFileGroupId(partitionPath, fileId), "001");
  }

  private PartitionedRLIBootstrapOperator operatorWithMockedMetadata(Configuration conf, HoodieBackedTableMetadata mockMetadata) {
    return new PartitionedRLIBootstrapOperator(conf) {
      @Override
      HoodieBackedTableMetadata createTableMetadata(HoodieTableMetaClient metaClient) {
        return mockMetadata;
      }
    };
  }

  private Configuration getPartitionedRLIConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());
    return conf;
  }
}
