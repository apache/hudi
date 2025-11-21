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
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MetadataPartitionType}.
 */
public class TestMetadataPartitionType {

  private static Stream<Arguments> partitionTypeAndPartitionPairs() {
    return Arrays.stream(MetadataPartitionType.values())
        .flatMap(type -> {
          if (type == MetadataPartitionType.PARTITION_STATS) {
            // PARTITION_STATS can only be enabled if the table is partitioned
            return Stream.of(Arguments.of(type, true));
          }
          return Stream.of(Arguments.of(type, true), Arguments.of(type, false));
        });
  }

  @ParameterizedTest
  @MethodSource("partitionTypeAndPartitionPairs")
  public void testPartitionEnabledByConfigOnly(MetadataPartitionType partitionType, boolean isTablePartitioned) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the configuration enabling given partition type, but the meta client not having it available (yet to initialize the partition)
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(isTablePartitioned);
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(partitionType)).thenReturn(false);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder();
    int expectedEnabledPartitions;
    switch (partitionType) {
      case EXPRESSION_INDEX:
        metadataConfigBuilder.enable(true).withExpressionIndexEnabled(true);
        expectedEnabledPartitions = 3;
        break;
      case SECONDARY_INDEX:
        metadataConfigBuilder.enable(true).withEnableGlobalRecordLevelIndex(true).withSecondaryIndexEnabled(true).withSecondaryIndexForColumn("col1");
        expectedEnabledPartitions = 4;
        break;
      case BLOOM_FILTERS:
        metadataConfigBuilder.enable(true).withMetadataIndexBloomFilter(true);
        expectedEnabledPartitions = 3;
        break;
      case RECORD_INDEX:
        metadataConfigBuilder.enable(true).withEnableGlobalRecordLevelIndex(true);
        expectedEnabledPartitions = 3;
        break;
      default:
        metadataConfigBuilder.enable(true);
        expectedEnabledPartitions = 2; // by default, FILES, COLUMN_STATS are enabled
        break;
    }
    if (isTablePartitioned) {
      expectedEnabledPartitions++; // PARTITION_STATS is enabled by default if table is partitioned
    }

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfigBuilder.build(), metaClient);

    // Verify partition type is enabled due to config
    assertEquals(expectedEnabledPartitions, enabledPartitions.size());
    assertTrue(enabledPartitions.contains(partitionType) || MetadataPartitionType.ALL_PARTITIONS.equals(partitionType));
  }

  @Test
  public void testPartitionAvailableByMetaClientOnly() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having RECORD_INDEX available but config not enabling it
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(true);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withEnableGlobalRecordLevelIndex(false).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig, metaClient);

    // Verify RECORD_INDEX and FILES is enabled due to availability, and COLUMN_STATS and PARTITION_STATS by default
    assertEquals(4, enabledPartitions.size(), "RECORD_INDEX, FILES, COL_STATS, PARTITION_STATS should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.RECORD_INDEX), "RECORD_INDEX should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.COLUMN_STATS), "COLUMN_STATS should be enabled by default");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.PARTITION_STATS), "PARTITION_STATS should be enabled by default");
  }

  @Test
  public void testNoPartitionsEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Neither config nor availability allows any partitions
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(Mockito.any())).thenReturn(false);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(false).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig, metaClient);

    // Verify no partitions are enabled
    assertTrue(enabledPartitions.isEmpty(), "No partitions should be enabled");
  }

  @Test
  public void testExpressionIndexPartitionEnabled() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);

    // Simulate the meta client having EXPRESSION_INDEX available
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(true);
    Mockito.when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES)).thenReturn(true);
    HoodieIndexDefinition expressionIndexDefinition = createIndexDefinition(MetadataPartitionType.EXPRESSION_INDEX, "dummy", "column_stats", "lower", Collections.singletonList("name"), null);
    HoodieIndexMetadata expressionIndexMetadata = new HoodieIndexMetadata(Collections.singletonMap("expr_index_dummy", expressionIndexDefinition));
    Mockito.when(metaClient.getIndexMetadata()).thenReturn(Option.of(expressionIndexMetadata));
    Mockito.when(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.EXPRESSION_INDEX)).thenReturn(true);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();

    List<MetadataPartitionType> enabledPartitions = MetadataPartitionType.getEnabledPartitions(metadataConfig, metaClient);

    // Verify EXPRESSION_INDEX and FILES is enabled due to availability, and COLUMN_STATS and PARTITION_STATS by default
    assertEquals(4, enabledPartitions.size(), "EXPRESSION_INDEX, FILES, COL_STATS and SECONDARY_INDEX should be available");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.FILES), "FILES should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.EXPRESSION_INDEX), "EXPRESSION_INDEX should be enabled by availability");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.COLUMN_STATS), "COLUMN_STATS should be enabled by default");
    assertTrue(enabledPartitions.contains(MetadataPartitionType.PARTITION_STATS), "PARTITION_STATS should be enabled by default");
  }

  @Test
  public void testGetMetadataPartitionsNeedingWriteStatusTracking() {
    List<MetadataPartitionType> trackingPartitions = MetadataPartitionType.getMetadataPartitionsNeedingWriteStatusTracking();
    assertTrue(trackingPartitions.contains(MetadataPartitionType.RECORD_INDEX), "RECORD_INDEX should need write status tracking");
    assertEquals(1, trackingPartitions.size(), "Only one partition should need write status tracking");
  }

  @Test
  public void testFromPartitionPath() {
    assertEquals(MetadataPartitionType.FILES, MetadataPartitionType.fromPartitionPath("files"));
    assertEquals(MetadataPartitionType.EXPRESSION_INDEX, MetadataPartitionType.fromPartitionPath("expr_index_dummy"));
    assertEquals(MetadataPartitionType.SECONDARY_INDEX, MetadataPartitionType.fromPartitionPath("secondary_index_dummy"));
    assertEquals(MetadataPartitionType.COLUMN_STATS, MetadataPartitionType.fromPartitionPath("column_stats"));
    assertEquals(MetadataPartitionType.BLOOM_FILTERS, MetadataPartitionType.fromPartitionPath("bloom_filters"));
    assertEquals(MetadataPartitionType.RECORD_INDEX, MetadataPartitionType.fromPartitionPath("record_index"));
    assertEquals(MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.fromPartitionPath("partition_stats"));
    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.fromPartitionPath("unknown"));
  }

  @Test
  public void testGetMetadataPartitionRecordType() {
    assertEquals(1, MetadataPartitionType.ALL_PARTITIONS.getRecordType());
    assertEquals(2, MetadataPartitionType.FILES.getRecordType());
    assertEquals(3, MetadataPartitionType.COLUMN_STATS.getRecordType());
    assertEquals(4, MetadataPartitionType.BLOOM_FILTERS.getRecordType());
    assertEquals(5, MetadataPartitionType.RECORD_INDEX.getRecordType());
    assertEquals(6, MetadataPartitionType.PARTITION_STATS.getRecordType());
    assertEquals(7, MetadataPartitionType.SECONDARY_INDEX.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(MetadataPartitionType.class)
  public void testGetNonExpressionIndexPath(MetadataPartitionType partitionType) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    String expressionIndexName = "expr_index_dummyExpressionIndex";
    String secondaryIndexName = "secondary_index_dummySecondaryIndex";
    HoodieIndexMetadata indexMetadata = getIndexMetadata(expressionIndexName, secondaryIndexName);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    
    // Mock getIndexForMetadataPartition for both index names
    when(metaClient.getIndexForMetadataPartition(expressionIndexName))
        .thenReturn(Option.of(indexMetadata.getIndexDefinitions().get(expressionIndexName)));
    when(metaClient.getIndexForMetadataPartition(secondaryIndexName))
        .thenReturn(Option.of(indexMetadata.getIndexDefinitions().get(secondaryIndexName)));
    
    if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      assertEquals(expressionIndexName, partitionType.getPartitionPath(metaClient, expressionIndexName));
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      assertEquals(secondaryIndexName, partitionType.getPartitionPath(metaClient, secondaryIndexName));
    } else {
      assertEquals(partitionType.getPartitionPath(), partitionType.getPartitionPath(metaClient, null));
    }
  }

  private static HoodieIndexMetadata getIndexMetadata(String expressionIndexName, String secondaryIndexName) {
    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    HoodieIndexDefinition expressionIndexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(expressionIndexName)
        .withIndexType("column_stats")
        .withIndexFunction("lower")
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), expressionIndexName))
        .withSourceFields(Collections.singletonList("name"))
        .build();
    indexDefinitions.put(expressionIndexName, expressionIndexDefinition);
    HoodieIndexDefinition secondaryIndexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(secondaryIndexName)
        .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), secondaryIndexName))
        .withIndexFunction(null)
        .withSourceFields(Collections.singletonList("name"))
        .build();
    indexDefinitions.put(secondaryIndexName, secondaryIndexDefinition);
    return new HoodieIndexMetadata(indexDefinitions);
  }

  @Test
  public void testExceptionForMissingExpressionIndexMetadata() {
    MetadataPartitionType partitionType = MetadataPartitionType.EXPRESSION_INDEX;
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(metaClient.getIndexForMetadataPartition("testIndex")).thenReturn(Option.empty());

    assertThrows(IllegalArgumentException.class,
        () -> partitionType.getPartitionPath(metaClient, "testIndex"));
  }

  @Test
  public void testIndexNameWithoutPrefix() {
    for (MetadataPartitionType partitionType : MetadataPartitionType.getValidValues()) {
      String userIndexName = MetadataPartitionType.isExpressionOrSecondaryIndex(partitionType.getPartitionPath()) ? "idx" : "";
      HoodieIndexDefinition indexDefinition = createIndexDefinition(partitionType, userIndexName, partitionType.name(), null, null, null);
      assertEquals(partitionType.getIndexNameWithoutPrefix(indexDefinition), userIndexName);
    }

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition indexDefinition = createIndexDefinition(MetadataPartitionType.RECORD_INDEX, "", MetadataPartitionType.RECORD_INDEX.name(), null, null, null);
      MetadataPartitionType.EXPRESSION_INDEX.getIndexNameWithoutPrefix(indexDefinition);
    });
  }

  private HoodieIndexDefinition createIndexDefinition(MetadataPartitionType partitionType, String userIndexName, String indexType, String indexFunction, List<String> sourceFields,
                                                      Map<String, String> indexOptions) {
    String indexSuffix = StringUtils.nonEmpty(userIndexName) ? userIndexName : "";
    String indexName = partitionType.getPartitionPath() + indexSuffix;
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType(indexType)
        .withIndexFunction(indexFunction)
        .withSourceFields(sourceFields)
        .withIndexOptions(indexOptions)
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), indexName))
        .build();
  }

  @Test
  public void testIsExpressionOrSecondaryIndex() {
    assertTrue(MetadataPartitionType.isExpressionOrSecondaryIndex("expr_index_"));
    assertTrue(MetadataPartitionType.isExpressionOrSecondaryIndex("expr_index_idx"));
    assertTrue(MetadataPartitionType.isExpressionOrSecondaryIndex("secondary_index_"));
    assertTrue(MetadataPartitionType.isExpressionOrSecondaryIndex("secondary_index_idx"));

    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.isExpressionOrSecondaryIndex("expr_index"),
        "No MetadataPartitionType for partition path: expr_index");
    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.isExpressionOrSecondaryIndex("expr_indexidx"),
        "No MetadataPartitionType for partition path: expr_indexidx");
    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.isExpressionOrSecondaryIndex("secondary_index"),
        "No MetadataPartitionType for partition path: secondary_index");
    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.isExpressionOrSecondaryIndex("secondary_indexidx"),
        "No MetadataPartitionType for partition path: secondary_indexidx");

    for (MetadataPartitionType partitionType : MetadataPartitionType.getValidValues()) {
      if (partitionType != MetadataPartitionType.EXPRESSION_INDEX && partitionType != MetadataPartitionType.SECONDARY_INDEX) {
        assertFalse(MetadataPartitionType.isExpressionOrSecondaryIndex(partitionType.getPartitionPath()));
      }
    }
  }
}
