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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadataWriter {

  private static Stream<Arguments> testArgumentsForEnabledPartitions() {
    return Arrays.stream(MetadataPartitionType.values()).flatMap(
        partitionType -> Arrays.stream(
                new HoodieTableVersion[] {HoodieTableVersion.EIGHT, HoodieTableVersion.SIX})
            .flatMap(tableVersion -> Arrays.stream(
                new Arguments[] {
                    Arguments.of(partitionType, tableVersion, true),
                    Arguments.of(partitionType, tableVersion, false)})));
  }

  @ParameterizedTest
  @MethodSource("testArgumentsForEnabledPartitions")
  public void testEnabledPartitions(MetadataPartitionType partitionType,
                                    HoodieTableVersion tableVersion,
                                    boolean isTablePartitioned) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the configuration enabling given partition type, but the meta client not having it available (yet to initialize the partition)
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(partitionType)).thenReturn(false);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    Mockito.when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(isTablePartitioned);

    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder();
    int expectedEnabledPartitions;
    boolean tableVersionEightOrAbove = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT);
    boolean isEnabled;

    switch (partitionType) {
      case EXPRESSION_INDEX:
        metadataConfigBuilder.enable(true).withExpressionIndexEnabled(true);
        expectedEnabledPartitions = tableVersionEightOrAbove ? (isTablePartitioned ? 4 : 3) : 2;
        isEnabled = tableVersionEightOrAbove;
        break;
      case SECONDARY_INDEX:
        metadataConfigBuilder.enable(true).withEnableRecordIndex(true).withSecondaryIndexEnabled(true);
        expectedEnabledPartitions = tableVersionEightOrAbove ? (isTablePartitioned ? 5 : 4) : 3;
        isEnabled = tableVersionEightOrAbove;
        break;
      case PARTITION_STATS:
        metadataConfigBuilder.enable(true).withMetadataIndexPartitionStats(true);
        isEnabled = tableVersionEightOrAbove && isTablePartitioned;
        expectedEnabledPartitions = isEnabled ? 3 : 2;
        break;
      case BLOOM_FILTERS:
        metadataConfigBuilder.enable(true).withMetadataIndexBloomFilter(true);
        expectedEnabledPartitions = tableVersionEightOrAbove ? (isTablePartitioned ? 4 : 3) : 3;
        isEnabled = true;
        break;
      case RECORD_INDEX:
        metadataConfigBuilder.enable(true).withEnableRecordIndex(true);
        expectedEnabledPartitions = tableVersionEightOrAbove ? (isTablePartitioned ? 5 : 4) : 3;
        isEnabled = true;
        break;
      default:
        metadataConfigBuilder.enable(true);
        // by default, FILES, COLUMN_STATS, PARTITION_STATS (for table version 8 and partitioned table) are enabled
        expectedEnabledPartitions = tableVersionEightOrAbove && isTablePartitioned ? 3 : 2;
        isEnabled = true;
        break;
    }

    HoodieBackedTableMetadataWriter<?> metadataWriter = Mockito.mock(
        HoodieBackedTableMetadataWriter.class, Mockito.CALLS_REAL_METHODS);
    List<MetadataPartitionType> enabledPartitions =
        metadataWriter.getEnabledPartitions(metadataConfigBuilder.build(), metaClient);

    // Verify partition type is enabled due to config
    assertEquals(expectedEnabledPartitions, enabledPartitions.size());
    assertEquals(isEnabled,
        enabledPartitions.contains(partitionType)
            || MetadataPartitionType.ALL_PARTITIONS.equals(partitionType));
  }

  @Test
  void testPartitionAvailableByMetaClientOnly() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having RECORD_INDEX available but config not enabling it
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(true);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(false).build();

    HoodieBackedTableMetadataWriter<?> metadataWriter = Mockito.mock(
        HoodieBackedTableMetadataWriter.class, Mockito.CALLS_REAL_METHODS);
    List<MetadataPartitionType> enabledPartitions = metadataWriter.getEnabledPartitions(metadataConfig, metaClient);

    // Verify RECORD_INDEX and FILES is enabled due to availability, and COLUMN_STATS and PARTITION_STATS by default
    assertEquals(4, enabledPartitions.size(), "RECORD_INDEX, FILES, COL_STATS, PARTITION_STATS should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.RECORD_INDEX), "RECORD_INDEX should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.COLUMN_STATS), "COLUMN_STATS should be enabled by default");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.PARTITION_STATS), "PARTITION_STATS should be enabled by default");
  }

  @Test
  void testNoPartitionsEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Neither config nor availability allows any partitions
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(Mockito.any())).thenReturn(false);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(false).build();

    HoodieBackedTableMetadataWriter<?> metadataWriter = Mockito.mock(
        HoodieBackedTableMetadataWriter.class, Mockito.CALLS_REAL_METHODS);
    List<MetadataPartitionType> enabledPartitions =
        metadataWriter.getEnabledPartitions(metadataConfig, metaClient);

    // Verify no partitions are enabled
    assertTrue(enabledPartitions.isEmpty(), "No partitions should be enabled");
  }

  @Test
  void testExpressionIndexPartitionEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having EXPRESSION_INDEX available
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(true);
    HoodieIndexDefinition expressionIndexDefinition = TestMetadataPartitionType.createIndexDefinition(
        MetadataPartitionType.EXPRESSION_INDEX, "dummy", "column_stats", "lower",
        Collections.singletonList("name"), null);
    HoodieIndexMetadata expressionIndexMetadata = new HoodieIndexMetadata(Collections.singletonMap("expr_index_dummy", expressionIndexDefinition));
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.of(expressionIndexMetadata));
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.EXPRESSION_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();

    HoodieBackedTableMetadataWriter<?> metadataWriter = Mockito.mock(
        HoodieBackedTableMetadataWriter.class, Mockito.CALLS_REAL_METHODS);
    List<MetadataPartitionType> enabledPartitions = metadataWriter.getEnabledPartitions(metadataConfig, metaClient);

    // Verify EXPRESSION_INDEX and FILES is enabled due to availability, and COLUMN_STATS and PARTITION_STATS by default
    assertEquals(4, enabledPartitions.size(), "EXPRESSION_INDEX, FILES, COL_STATS and SECONDARY_INDEX should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.EXPRESSION_INDEX), "EXPRESSION_INDEX should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.COLUMN_STATS), "COLUMN_STATS should be enabled by default");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.PARTITION_STATS), "PARTITION_STATS should be enabled by default");
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,true,false,true",
      "false,true,false,true",
      "true,false,false,true",
      "false,false,false,false",
      "false,false,true,false",
  })
  void runPendingTableServicesOperations(boolean hasPendingCompaction, boolean hasPendingLogCompaction, boolean requiresRefresh, boolean ranService) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    if (requiresRefresh) {
      when(metaClient.reloadActiveTimeline()).thenReturn(initialTimeline);
    } else {
      when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    }
    if (hasPendingCompaction) {
      when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    }
    if (hasPendingLogCompaction) {
      when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);
    }
    HoodieActiveTimeline expectedResult;
    if (ranService) {
      HoodieActiveTimeline timelineReloadedAfterServicesRun = mock(HoodieActiveTimeline.class);
      when(metaClient.reloadActiveTimeline()).thenReturn(timelineReloadedAfterServicesRun);
      expectedResult = timelineReloadedAfterServicesRun;
    } else {
      expectedResult = initialTimeline;
    }
    assertSame(expectedResult, HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(metaClient, writeClient, requiresRefresh));

    verify(writeClient, times(hasPendingCompaction ? 1 : 0)).runAnyPendingCompactions();
    verify(writeClient, times(hasPendingLogCompaction ? 1 : 0)).runAnyPendingLogCompactions();
    int expectedTimelineReloads = (requiresRefresh ? 1 : 0) + (ranService ? 1 : 0);
    verify(metaClient, times(expectedTimelineReloads)).reloadActiveTimeline();
  }

  @Test
  void rollbackFailedWrites_reloadsTimelineOnWritesRolledBack() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(true);
    try (MockedStatic<HoodieTableMetaClient> mockedStatic = mockStatic(HoodieTableMetaClient.class)) {
      HoodieTableMetaClient reloadedClient = mock(HoodieTableMetaClient.class);
      mockedStatic.when(() -> HoodieTableMetaClient.reload(mockMetaClient)).thenReturn(reloadedClient);
      assertSame(reloadedClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(writeConfig, mockWriteClient, mockMetaClient));
    }
  }

  @Test
  void rollbackFailedWrites_avoidsTimelineReload() {
    HoodieWriteConfig eagerWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(false);
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(eagerWriteConfig, mockWriteClient, mockMetaClient));

    HoodieWriteConfig lazyWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(lazyWriteConfig, mockWriteClient, mockMetaClient));
  }
}
