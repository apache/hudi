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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexesMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MetadataPartitionType}.
 */
public class TestMetadataPartitionType {

  @ParameterizedTest
  @EnumSource(MetadataPartitionType.class)
  public void testPartitionEnabledByConfigOnly(MetadataPartitionType partitionType) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the configuration enabling given partition type, but the meta client not having it available (yet to initialize the partition)
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(partitionType)).thenReturn(false);
    Mockito.when(metaClient.getIndexesMetadata()).thenReturn(Option.empty());
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder();
    int expectedEnabledPartitions;
    switch (partitionType) {
      case FILES:
      case FUNCTIONAL_INDEX:
      case SECONDARY_INDEX:
        metadataConfigBuilder.enable(true);
        expectedEnabledPartitions = 1;
        break;
      case COLUMN_STATS:
        metadataConfigBuilder.enable(true).withMetadataIndexColumnStats(true);
        expectedEnabledPartitions = 2;
        break;
      case BLOOM_FILTERS:
        metadataConfigBuilder.enable(true).withMetadataIndexBloomFilter(true);
        expectedEnabledPartitions = 2;
        break;
      case RECORD_INDEX:
        metadataConfigBuilder.enable(true).withEnableRecordIndex(true);
        expectedEnabledPartitions = 2;
        break;
      case PARTITION_STATS:
        metadataConfigBuilder.enable(true).withMetadataIndexPartitionStats(true).withColumnStatsIndexForColumns("partitionCol");
        expectedEnabledPartitions = 2;
        break;
      default:
        throw new IllegalArgumentException("Unknown partition type: " + partitionType);
    }

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfigBuilder.build().getProps(), metaClient);

    // Verify partition type is enabled due to config
    if (partitionType == MetadataPartitionType.FUNCTIONAL_INDEX || partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      assertEquals(1, enabledPartitions.size(), "FUNCTIONAL_INDEX should be enabled by SQL, only FILES is enabled in this case.");
      assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES));
    } else {
      assertEquals(expectedEnabledPartitions, enabledPartitions.size());
      assertTrue(enabledPartitions.contains(partitionType));
    }
  }

  @Test
  public void testPartitionAvailableByMetaClientOnly() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having RECORD_INDEX available but config not enabling it
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    Mockito.when(metaClient.getIndexesMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(false).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig.getProps(), metaClient);

    // Verify RECORD_INDEX and FILES is enabled due to availability
    assertEquals(2, enabledPartitions.size(), "RECORD_INDEX and FILES should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.RECORD_INDEX), "RECORD_INDEX should be enabled by availability");
  }

  @Test
  public void testNoPartitionsEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Neither config nor availability allows any partitions
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(metaClient.getIndexesMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(Mockito.any())).thenReturn(false);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(false).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig.getProps(), metaClient);

    // Verify no partitions are enabled
    assertTrue(enabledPartitions.isEmpty(), "No partitions should be enabled");
  }

  @Test
  public void testFunctionalIndexPartitionEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having FUNCTIONAL_INDEX available
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    HoodieIndexesMetadata functionalIndexMetadata =
        new HoodieIndexesMetadata(Collections.singletonMap("func_index_dummy", new HoodieIndexDefinition("func_index_dummy", null, null, null, null)));
    Mockito.when(metaClient.getIndexesMetadata()).thenReturn(Option.of(functionalIndexMetadata));
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FUNCTIONAL_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig.getProps(), metaClient);

    // Verify FUNCTIONAL_INDEX and FILES is enabled due to availability
    assertEquals(2, enabledPartitions.size(), "FUNCTIONAL_INDEX and FILES should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FUNCTIONAL_INDEX), "FUNCTIONAL_INDEX should be enabled by availability");
  }

  @Test
  public void testGetMetadataPartitionsNeedingWriteStatusTracking() {
    List<MetadataPartitionType> trackingPartitions = MetadataPartitionType.getMetadataPartitionsNeedingWriteStatusTracking();
    assertTrue(trackingPartitions.contains(MetadataPartitionType.RECORD_INDEX), "RECORD_INDEX should need write status tracking");
    assertEquals(1, trackingPartitions.size(), "Only one partition should need write status tracking");
  }
}
