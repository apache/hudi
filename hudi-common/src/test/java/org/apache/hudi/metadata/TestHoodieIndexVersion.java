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

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.metadata.indexversion.AllPartitionsIndexVersion;
import org.apache.hudi.metadata.indexversion.BloomFiltersIndexVersion;
import org.apache.hudi.metadata.indexversion.ColumnStatsIndexVersion;
import org.apache.hudi.metadata.indexversion.ExpressionIndexVersion;
import org.apache.hudi.metadata.indexversion.FilesIndexVersion;
import org.apache.hudi.metadata.indexversion.HoodieIndexVersion;
import org.apache.hudi.metadata.indexversion.PartitionStatsIndexVersion;
import org.apache.hudi.metadata.indexversion.RecordIndexVersion;
import org.apache.hudi.metadata.indexversion.SecondaryIndexVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndexVersion {

  @Test
  public void testEnumValues() {
    // Test all enum values are properly defined
    assertEquals(MetadataPartitionType.ALL_PARTITIONS, AllPartitionsIndexVersion.V1.getPartitionType());
    assertEquals(1, AllPartitionsIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("0.14.0"), AllPartitionsIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.PARTITION_STATS, PartitionStatsIndexVersion.V1.getPartitionType());
    assertEquals(1, PartitionStatsIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("0.14.0"), PartitionStatsIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.FILES, FilesIndexVersion.V1.getPartitionType());
    assertEquals(1, FilesIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("0.14.0"), FilesIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.RECORD_INDEX, RecordIndexVersion.V1.getPartitionType());
    assertEquals(1, RecordIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("1.0.0"), RecordIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.COLUMN_STATS, ColumnStatsIndexVersion.V1.getPartitionType());
    assertEquals(1, ColumnStatsIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("1.0.0"), ColumnStatsIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.BLOOM_FILTERS, BloomFiltersIndexVersion.V1.getPartitionType());
    assertEquals(1, BloomFiltersIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("1.0.0"), BloomFiltersIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.SECONDARY_INDEX, SecondaryIndexVersion.V1.getPartitionType());
    assertEquals(1, SecondaryIndexVersion.V1.versionCode());
    assertEquals(Arrays.asList("1.0.0"), SecondaryIndexVersion.V1.getReleaseVersions());

    assertEquals(MetadataPartitionType.SECONDARY_INDEX, SecondaryIndexVersion.V2.getPartitionType());
    assertEquals(2, SecondaryIndexVersion.V2.versionCode());
    assertEquals(Arrays.asList("1.1.0"), SecondaryIndexVersion.V2.getReleaseVersions());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getCurrentVersionTestCases")
  public void testGetCurrentVersionWithString(String testName, HoodieTableVersion tableVersion, 
                                             String partitionPath, HoodieIndexVersion expectedVersion) {
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(tableVersion, partitionPath);
    assertEquals(expectedVersion, result);
  }

  private static Stream<Arguments> getCurrentVersionTestCases() {
    return Stream.of(
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, "record_index", RecordIndexVersion.V1),
        Arguments.of("RECORD_INDEX", HoodieTableVersion.NINE, "record_index", RecordIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, "column_stats", ColumnStatsIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.NINE, "column_stats", ColumnStatsIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "bloom_filters", BloomFiltersIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "bloom_filters", BloomFiltersIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "expr_index_idx1", ExpressionIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "expr_index_idx1", ExpressionIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "secondary_index_idx1", SecondaryIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "secondary_index_idx1", SecondaryIndexVersion.V2),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, "files", FilesIndexVersion.V1),
        Arguments.of("FILES", HoodieTableVersion.NINE, "files", FilesIndexVersion.V1),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.EIGHT, "files", FilesIndexVersion.V1),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.NINE, "files", FilesIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, "partition_stats", PartitionStatsIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.NINE, "partition_stats", PartitionStatsIndexVersion.V1)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getCurrentVersionWithEnumTestCases")
  public void testGetCurrentVersionWithEnum(String testName, HoodieTableVersion tableVersion, 
                                           MetadataPartitionType partitionType, HoodieIndexVersion expectedVersion) {
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(tableVersion, partitionType);
    assertEquals(expectedVersion, result);
  }

  private static Stream<Arguments> getCurrentVersionWithEnumTestCases() {
    return Stream.of(
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, MetadataPartitionType.RECORD_INDEX, RecordIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.COLUMN_STATS, ColumnStatsIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, MetadataPartitionType.BLOOM_FILTERS, BloomFiltersIndexVersion.V1),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, MetadataPartitionType.FILES, FilesIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.PARTITION_STATS, PartitionStatsIndexVersion.V1),
        Arguments.of("ALL_PARTITIONS", HoodieTableVersion.EIGHT, MetadataPartitionType.ALL_PARTITIONS, AllPartitionsIndexVersion.V1)
    );
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion8() {
    // Table version 8 should return SECONDARY_INDEX_ONE
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.EIGHT, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(SecondaryIndexVersion.V1, result);
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion9() {
    // Table version 9 should return SECONDARY_INDEX_TWO
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.NINE, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(SecondaryIndexVersion.V2, result);
  }

  @Test
  public void testGetCurrentVersionUnknownPartitionType() {
    // Test with an unknown partition type that doesn't exist in the enum
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.EIGHT, "unknown_partition_type");
    });
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("isValidIndexDefinitionTestCases")
  public void testIsValidIndexDefinition(String testName, HoodieTableVersion tableVersion, 
                                        HoodieIndexDefinition indexDef, boolean expectedResult) {
    boolean result = HoodieIndexVersion.isValidIndexDefinition(tableVersion, indexDef);
    assertEquals(expectedResult, result);
  }

  private static Stream<Arguments> isValidIndexDefinitionTestCases() {
    return Stream.of(
        // Table version 8, missing version attribute is allowed
        Arguments.of("Table version 8 with null version", 
            HoodieTableVersion.EIGHT, 
            createIndexDefinition("column_stats", null), 
            true),
        
        // Table version 8, SI only v1 is allowed
        Arguments.of("Table version 8 with SI v1", 
            HoodieTableVersion.EIGHT, 
            createIndexDefinition("secondary_index_idx_test", SecondaryIndexVersion.V1), 
            true),
        
        // Table version 8, SI v2 is not allowed
        Arguments.of("Table version 8 with SI v2", 
            HoodieTableVersion.EIGHT, 
            createIndexDefinition("secondary_index_idx_test", SecondaryIndexVersion.V2), 
            false),
        
        // Table version 9, SI must have non-null version
        Arguments.of("Table version 9 with SI null version", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", null), 
            false),
        
        // Table version 9, SI must be v2 or above
        Arguments.of("Table version 9 with SI v1", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", SecondaryIndexVersion.V1), 
            false),
        
        // Table version 9, SI v2 is allowed
        Arguments.of("Table version 9 with SI v2", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", SecondaryIndexVersion.V2), 
            true),
        
        // Table version 9, non-SI with null version is allowed
        Arguments.of("Table version 9 with non-SI null version", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("column_stats", null), 
            true),
        
        // Table version 9, non-SI with any version is allowed
        Arguments.of("Table version 9 with non-SI v1", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("column_stats", ColumnStatsIndexVersion.V1), 
            true)
    );
  }

  private static HoodieIndexDefinition createIndexDefinition(String indexName, HoodieIndexVersion version) {
    HoodieIndexDefinition.Builder builder = HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType(MetadataPartitionType.fromPartitionPath(indexName).name())
        .withSourceFields(Collections.singletonList("test_field"));
    
    if (version != null) {
      builder.withVersion(version);
    }
    
    return builder.build();
  }

  @Test
  public void testComparisonMethods() {
    // Test greaterThan
    assertTrue(SecondaryIndexVersion.V2.greaterThan(SecondaryIndexVersion.V1));
    assertFalse(SecondaryIndexVersion.V1.greaterThan(SecondaryIndexVersion.V2));
    assertFalse(SecondaryIndexVersion.V1.greaterThan(SecondaryIndexVersion.V1));

    // Test greaterThanOrEquals
    assertTrue(SecondaryIndexVersion.V2.greaterThanOrEquals(SecondaryIndexVersion.V1));
    assertTrue(SecondaryIndexVersion.V1.greaterThanOrEquals(SecondaryIndexVersion.V1));
    assertFalse(SecondaryIndexVersion.V1.greaterThanOrEquals(SecondaryIndexVersion.V2));

    // Test lowerThan
    assertTrue(SecondaryIndexVersion.V1.lowerThan(SecondaryIndexVersion.V2));
    assertFalse(SecondaryIndexVersion.V2.lowerThan(SecondaryIndexVersion.V1));
    assertFalse(SecondaryIndexVersion.V1.lowerThan(SecondaryIndexVersion.V1));

    // Test lowerThanOrEquals
    assertTrue(SecondaryIndexVersion.V1.lowerThanOrEquals(SecondaryIndexVersion.V2));
    assertTrue(SecondaryIndexVersion.V1.lowerThanOrEquals(SecondaryIndexVersion.V1));
    assertFalse(SecondaryIndexVersion.V2.lowerThanOrEquals(SecondaryIndexVersion.V1));
  }

  @Test
  public void testComparisonMethodsDifferentPartitionTypes() {
    // Test that comparison methods throw exception for different partition types
    assertThrows(IllegalArgumentException.class, () -> {
      SecondaryIndexVersion.V1.greaterThan(ColumnStatsIndexVersion.V1);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      SecondaryIndexVersion.V1.greaterThanOrEquals(ColumnStatsIndexVersion.V1);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      SecondaryIndexVersion.V1.lowerThan(ColumnStatsIndexVersion.V1);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      SecondaryIndexVersion.V1.lowerThanOrEquals(ColumnStatsIndexVersion.V1);
    });
  }

  @Test
  public void testVersionCanBeAssignedToPartitionType() {
    // Test valid partition type
    SecondaryIndexVersion.V1.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.SECONDARY_INDEX);
    // Test invalid partition type
    assertThrows(IllegalArgumentException.class, () -> {
      SecondaryIndexVersion.V1.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.COLUMN_STATS);
    });
    assertThrows(IllegalArgumentException.class, () -> {
      ColumnStatsIndexVersion.V1.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.EXPRESSION_INDEX);
    });
  }

  @Test
  public void testToString() {
    assertEquals("SECONDARY_INDEX_ONE", SecondaryIndexVersion.V1.toString());
    assertEquals("SECONDARY_INDEX_TWO", SecondaryIndexVersion.V2.toString());
    assertEquals("COLUMN_STATS_ONE", ColumnStatsIndexVersion.V1.toString());
    assertEquals("RECORD_INDEX_ONE", RecordIndexVersion.V1.toString());
  }
} 