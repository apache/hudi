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
    assertEquals(MetadataPartitionType.ALL_PARTITIONS, HoodieIndexVersion.ALL_PARTITIONS_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.ALL_PARTITIONS_ONE.versionCode());
    assertEquals(Arrays.asList("0.14.0"), HoodieIndexVersion.ALL_PARTITIONS_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.PARTITION_STATS, HoodieIndexVersion.PARTITION_STATS_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.PARTITION_STATS_ONE.versionCode());
    assertEquals(Arrays.asList("0.14.0"), HoodieIndexVersion.PARTITION_STATS_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.FILES, HoodieIndexVersion.FILES_INDEX_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.FILES_INDEX_ONE.versionCode());
    assertEquals(Arrays.asList("0.14.0"), HoodieIndexVersion.FILES_INDEX_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.RECORD_INDEX, HoodieIndexVersion.RECORD_INDEX_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.RECORD_INDEX_ONE.versionCode());
    assertEquals(Arrays.asList("1.0.0"), HoodieIndexVersion.RECORD_INDEX_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.COLUMN_STATS, HoodieIndexVersion.COLUMN_STATS_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.COLUMN_STATS_ONE.versionCode());
    assertEquals(Arrays.asList("1.0.0"), HoodieIndexVersion.COLUMN_STATS_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.BLOOM_FILTERS, HoodieIndexVersion.BLOOM_FILTERS_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.BLOOM_FILTERS_ONE.versionCode());
    assertEquals(Arrays.asList("1.0.0"), HoodieIndexVersion.BLOOM_FILTERS_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.EXPRESSION_INDEX, HoodieIndexVersion.EXPRESSION_INDEX_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.EXPRESSION_INDEX_ONE.versionCode());
    assertEquals(Arrays.asList("1.0.0"), HoodieIndexVersion.EXPRESSION_INDEX_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.SECONDARY_INDEX, HoodieIndexVersion.SECONDARY_INDEX_ONE.getPartitionType());
    assertEquals(1, HoodieIndexVersion.SECONDARY_INDEX_ONE.versionCode());
    assertEquals(Arrays.asList("1.0.0"), HoodieIndexVersion.SECONDARY_INDEX_ONE.getReleaseVersions());

    assertEquals(MetadataPartitionType.SECONDARY_INDEX, HoodieIndexVersion.SECONDARY_INDEX_TWO.getPartitionType());
    assertEquals(2, HoodieIndexVersion.SECONDARY_INDEX_TWO.versionCode());
    assertEquals(Arrays.asList("1.1.0"), HoodieIndexVersion.SECONDARY_INDEX_TWO.getReleaseVersions());
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
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, "record_index", HoodieIndexVersion.RECORD_INDEX_ONE),
        Arguments.of("RECORD_INDEX", HoodieTableVersion.NINE, "record_index", HoodieIndexVersion.RECORD_INDEX_ONE),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, "column_stats", HoodieIndexVersion.COLUMN_STATS_ONE),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.NINE, "column_stats", HoodieIndexVersion.COLUMN_STATS_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "bloom_filters", HoodieIndexVersion.BLOOM_FILTERS_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "bloom_filters", HoodieIndexVersion.BLOOM_FILTERS_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "expr_index_idx1", HoodieIndexVersion.EXPRESSION_INDEX_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "expr_index_idx1", HoodieIndexVersion.EXPRESSION_INDEX_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "secondary_index_idx1", HoodieIndexVersion.SECONDARY_INDEX_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "secondary_index_idx1", HoodieIndexVersion.SECONDARY_INDEX_TWO),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, "files", HoodieIndexVersion.FILES_INDEX_ONE),
        Arguments.of("FILES", HoodieTableVersion.NINE, "files", HoodieIndexVersion.FILES_INDEX_ONE),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.EIGHT, "files", HoodieIndexVersion.FILES_INDEX_ONE),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.NINE, "files", HoodieIndexVersion.FILES_INDEX_ONE),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, "partition_stats", HoodieIndexVersion.PARTITION_STATS_ONE),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.NINE, "partition_stats", HoodieIndexVersion.PARTITION_STATS_ONE)
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
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, MetadataPartitionType.RECORD_INDEX, HoodieIndexVersion.RECORD_INDEX_ONE),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.COLUMN_STATS, HoodieIndexVersion.COLUMN_STATS_ONE),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, MetadataPartitionType.BLOOM_FILTERS, HoodieIndexVersion.BLOOM_FILTERS_ONE),
        Arguments.of("EXPRESSION_INDEX", HoodieTableVersion.EIGHT, MetadataPartitionType.EXPRESSION_INDEX, HoodieIndexVersion.EXPRESSION_INDEX_ONE),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, MetadataPartitionType.FILES, HoodieIndexVersion.FILES_INDEX_ONE),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.PARTITION_STATS, HoodieIndexVersion.PARTITION_STATS_ONE),
        Arguments.of("ALL_PARTITIONS", HoodieTableVersion.EIGHT, MetadataPartitionType.ALL_PARTITIONS, HoodieIndexVersion.ALL_PARTITIONS_ONE)
    );
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion8() {
    // Table version 8 should return SECONDARY_INDEX_ONE
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.EIGHT, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE, result);
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion9() {
    // Table version 9 should return SECONDARY_INDEX_TWO
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.NINE, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO, result);
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
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.SECONDARY_INDEX_ONE), 
            true),
        
        // Table version 8, SI v2 is not allowed
        Arguments.of("Table version 8 with SI v2", 
            HoodieTableVersion.EIGHT, 
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.SECONDARY_INDEX_TWO), 
            false),
        
        // Table version 9, SI must have non-null version
        Arguments.of("Table version 9 with SI null version", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", null), 
            false),
        
        // Table version 9, SI must be v2 or above
        Arguments.of("Table version 9 with SI v1", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.SECONDARY_INDEX_ONE), 
            false),
        
        // Table version 9, SI v2 is allowed
        Arguments.of("Table version 9 with SI v2", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.SECONDARY_INDEX_TWO), 
            true),
        
        // Table version 9, non-SI with null version is allowed
        Arguments.of("Table version 9 with non-SI null version", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("column_stats", null), 
            true),
        
        // Table version 9, non-SI with any version is allowed
        Arguments.of("Table version 9 with non-SI v1", 
            HoodieTableVersion.NINE, 
            createIndexDefinition("column_stats", HoodieIndexVersion.COLUMN_STATS_ONE), 
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
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_TWO.greaterThan(HoodieIndexVersion.SECONDARY_INDEX_ONE));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThan(HoodieIndexVersion.SECONDARY_INDEX_TWO));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThan(HoodieIndexVersion.SECONDARY_INDEX_ONE));

    // Test greaterThanOrEquals
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_TWO.greaterThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE));
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO));

    // Test lowerThan
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThan(HoodieIndexVersion.SECONDARY_INDEX_TWO));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_TWO.lowerThan(HoodieIndexVersion.SECONDARY_INDEX_ONE));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThan(HoodieIndexVersion.SECONDARY_INDEX_ONE));

    // Test lowerThanOrEquals
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO));
    assertTrue(HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE));
    assertFalse(HoodieIndexVersion.SECONDARY_INDEX_TWO.lowerThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE));
  }

  @Test
  public void testComparisonMethodsDifferentPartitionTypes() {
    // Test that comparison methods throw exception for different partition types
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThan(HoodieIndexVersion.COLUMN_STATS_ONE);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.SECONDARY_INDEX_ONE.greaterThanOrEquals(HoodieIndexVersion.COLUMN_STATS_ONE);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThan(HoodieIndexVersion.COLUMN_STATS_ONE);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.SECONDARY_INDEX_ONE.lowerThanOrEquals(HoodieIndexVersion.COLUMN_STATS_ONE);
    });
  }

  @Test
  public void testVersionCanBeAssignedToPartitionType() {
    // Test valid partition type
    HoodieIndexVersion.SECONDARY_INDEX_ONE.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.SECONDARY_INDEX);
    // Test invalid partition type
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.SECONDARY_INDEX_ONE.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.COLUMN_STATS);
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexVersion.COLUMN_STATS_ONE.ensureVersionCanBeAssignedToIndexType(MetadataPartitionType.EXPRESSION_INDEX);
    });
  }

  @Test
  public void testToString() {
    assertEquals("SECONDARY_INDEX_ONE", HoodieIndexVersion.SECONDARY_INDEX_ONE.toString());
    assertEquals("SECONDARY_INDEX_TWO", HoodieIndexVersion.SECONDARY_INDEX_TWO.toString());
    assertEquals("COLUMN_STATS_ONE", HoodieIndexVersion.COLUMN_STATS_ONE.toString());
    assertEquals("RECORD_INDEX_ONE", HoodieIndexVersion.RECORD_INDEX_ONE.toString());
  }
} 