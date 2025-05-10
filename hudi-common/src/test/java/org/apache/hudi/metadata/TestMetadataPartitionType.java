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

import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
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

  private static Stream<Arguments> testArguments() {
    return Arrays.stream(new Arguments[] {
        Arguments.of(FILES, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(FILES, HoodieTableVersion.SIX, false, true),
        Arguments.of(COLUMN_STATS, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(COLUMN_STATS, HoodieTableVersion.SIX, false, true),
        Arguments.of(BLOOM_FILTERS, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(BLOOM_FILTERS, HoodieTableVersion.SIX, false, true),
        Arguments.of(RECORD_INDEX, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(RECORD_INDEX, HoodieTableVersion.SIX, false, true),
        Arguments.of(EXPRESSION_INDEX, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(EXPRESSION_INDEX, HoodieTableVersion.SIX, false, false),
        Arguments.of(SECONDARY_INDEX, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(SECONDARY_INDEX, HoodieTableVersion.SIX, false, false),
        Arguments.of(PARTITION_STATS, HoodieTableVersion.EIGHT, true, true),
        Arguments.of(PARTITION_STATS, HoodieTableVersion.EIGHT, false, false),
        Arguments.of(PARTITION_STATS, HoodieTableVersion.SIX, true, false),
        Arguments.of(PARTITION_STATS, HoodieTableVersion.SIX, false, false),
    });
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  void testMetadataPartitionProperties(MetadataPartitionType partitionType,
                                       HoodieTableVersion tableVersion,
                                       boolean isTablePartitioned,
                                       boolean expectedSupported) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    Mockito.when(tableConfig.isTablePartitioned()).thenReturn(isTablePartitioned);

    assertEquals(expectedSupported, partitionType.isMetadataPartitionSupported(metaClient));
  }

  @Test
  public void testGetMetadataPartitionsNeedingWriteStatusTracking() {
    List<MetadataPartitionType> trackingPartitions = MetadataPartitionType.getMetadataPartitionsNeedingWriteStatusTracking();
    assertTrue(trackingPartitions.contains(RECORD_INDEX), "RECORD_INDEX should need write status tracking");
    assertEquals(1, trackingPartitions.size(), "Only one partition should need write status tracking");
  }

  @Test
  public void testFromPartitionPath() {
    assertEquals(FILES, MetadataPartitionType.fromPartitionPath("files"));
    assertEquals(EXPRESSION_INDEX, MetadataPartitionType.fromPartitionPath("expr_index_dummy"));
    assertEquals(SECONDARY_INDEX, MetadataPartitionType.fromPartitionPath("secondary_index_dummy"));
    assertEquals(COLUMN_STATS, MetadataPartitionType.fromPartitionPath("column_stats"));
    assertEquals(BLOOM_FILTERS, MetadataPartitionType.fromPartitionPath("bloom_filters"));
    assertEquals(RECORD_INDEX, MetadataPartitionType.fromPartitionPath("record_index"));
    assertEquals(PARTITION_STATS, MetadataPartitionType.fromPartitionPath("partition_stats"));
    assertThrows(IllegalArgumentException.class, () -> MetadataPartitionType.fromPartitionPath("unknown"));
  }

  @Test
  public void testGetMetadataPartitionRecordType() {
    assertEquals(1, MetadataPartitionType.ALL_PARTITIONS.getRecordType());
    assertEquals(2, FILES.getRecordType());
    assertEquals(3, COLUMN_STATS.getRecordType());
    assertEquals(4, BLOOM_FILTERS.getRecordType());
    assertEquals(5, RECORD_INDEX.getRecordType());
    assertEquals(6, PARTITION_STATS.getRecordType());
    assertEquals(7, SECONDARY_INDEX.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(MetadataPartitionType.class)
  public void testGetNonExpressionIndexPath(MetadataPartitionType partitionType) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    String expressionIndexName = "dummyExpressionIndex";
    String secondaryIndexName = "dummySecondaryIndex";
    HoodieIndexMetadata indexMetadata = getIndexMetadata(expressionIndexName, secondaryIndexName);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    if (partitionType == EXPRESSION_INDEX) {
      assertEquals(expressionIndexName, partitionType.getPartitionPath(metaClient, expressionIndexName));
    } else if (partitionType == SECONDARY_INDEX) {
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
        .withSourceFields(Collections.singletonList("name"))
        .build();
    indexDefinitions.put(expressionIndexName, expressionIndexDefinition);
    HoodieIndexDefinition secondaryIndexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(secondaryIndexName)
        .withIndexType(null)
        .withIndexFunction(null)
        .withSourceFields(Collections.singletonList("name"))
        .build();
    indexDefinitions.put(secondaryIndexName, secondaryIndexDefinition);
    return new HoodieIndexMetadata(indexDefinitions);
  }

  @Test
  public void testExceptionForMissingExpressionIndexMetadata() {
    MetadataPartitionType partitionType = EXPRESSION_INDEX;
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    assertThrows(IllegalArgumentException.class,
        () -> partitionType.getPartitionPath(metaClient, "testIndex"));
  }

  @Test
  public void testIndexNameWithoutPrefix() {
    for (MetadataPartitionType partitionType : MetadataPartitionType.values()) {
      String userIndexName = MetadataPartitionType.isExpressionOrSecondaryIndex(partitionType.getPartitionPath()) ? "idx" : "";
      HoodieIndexDefinition indexDefinition = createIndexDefinition(partitionType, userIndexName, null, null, null, null);
      assertEquals(partitionType.getIndexNameWithoutPrefix(indexDefinition), userIndexName);
    }

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition indexDefinition = createIndexDefinition(RECORD_INDEX, "", null, null, null, null);
      EXPRESSION_INDEX.getIndexNameWithoutPrefix(indexDefinition);
    });
  }

  protected static HoodieIndexDefinition createIndexDefinition(MetadataPartitionType partitionType, String userIndexName, String indexType, String indexFunction, List<String> sourceFields,
                                                      Map<String, String> indexOptions) {
    String indexSuffix = StringUtils.nonEmpty(userIndexName) ? userIndexName : "";
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(partitionType.getPartitionPath() + indexSuffix)
        .withIndexType(indexType)
        .withIndexFunction(indexFunction)
        .withSourceFields(sourceFields)
        .withIndexOptions(indexOptions)
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

    for (MetadataPartitionType partitionType : MetadataPartitionType.values()) {
      if (partitionType != EXPRESSION_INDEX && partitionType != SECONDARY_INDEX) {
        assertFalse(MetadataPartitionType.isExpressionOrSecondaryIndex(partitionType.getPartitionPath()));
      }
    }
  }
}
