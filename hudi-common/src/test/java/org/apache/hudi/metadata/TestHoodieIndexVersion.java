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

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndexVersion {

  @ParameterizedTest(name = "{0}")
  @MethodSource("getCurrentVersionTestCases")
  public void testGetCurrentVersionWithString(String testName, HoodieTableVersion tableVersion, 
                                             String partitionPath, HoodieIndexVersion expectedVersion) {
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(tableVersion, partitionPath);
    assertEquals(expectedVersion, result);
  }

  private static Stream<Arguments> getCurrentVersionTestCases() {
    return Stream.of(
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, "record_index", HoodieIndexVersion.V1),
        Arguments.of("RECORD_INDEX", HoodieTableVersion.NINE, "record_index", HoodieIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, "column_stats", HoodieIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.NINE, "column_stats", HoodieIndexVersion.V2),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "bloom_filters", HoodieIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "bloom_filters", HoodieIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "expr_index_idx1", HoodieIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "expr_index_idx1", HoodieIndexVersion.V2),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, "secondary_index_idx1", HoodieIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.NINE, "secondary_index_idx1", HoodieIndexVersion.V2),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, "files", HoodieIndexVersion.V1),
        Arguments.of("FILES", HoodieTableVersion.NINE, "files", HoodieIndexVersion.V1),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.EIGHT, "files", HoodieIndexVersion.V1),
        Arguments.of("EXPRESSION INDEX", HoodieTableVersion.NINE, "files", HoodieIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, "partition_stats", HoodieIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.NINE, "partition_stats", HoodieIndexVersion.V2)
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
        Arguments.of("RECORD_INDEX", HoodieTableVersion.EIGHT, MetadataPartitionType.RECORD_INDEX, HoodieIndexVersion.V1),
        Arguments.of("COLUMN_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.COLUMN_STATS, HoodieIndexVersion.V1),
        Arguments.of("BLOOM_FILTERS", HoodieTableVersion.EIGHT, MetadataPartitionType.BLOOM_FILTERS, HoodieIndexVersion.V1),
        Arguments.of("EXPRESSION_INDEX", HoodieTableVersion.EIGHT, MetadataPartitionType.EXPRESSION_INDEX, HoodieIndexVersion.V1),
        Arguments.of("FILES", HoodieTableVersion.EIGHT, MetadataPartitionType.FILES, HoodieIndexVersion.V1),
        Arguments.of("PARTITION_STATS", HoodieTableVersion.EIGHT, MetadataPartitionType.PARTITION_STATS, HoodieIndexVersion.V1),
        Arguments.of("ALL_PARTITIONS", HoodieTableVersion.EIGHT, MetadataPartitionType.ALL_PARTITIONS, HoodieIndexVersion.V1)
    );
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion8() {
    // Table version 8 should return V1
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.EIGHT, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(HoodieIndexVersion.V1, result);
  }

  @Test
  public void testGetCurrentVersionSecondaryIndexTableVersion9() {
    // Table version 9 should return V2
    HoodieIndexVersion result = HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.NINE, MetadataPartitionType.SECONDARY_INDEX);
    assertEquals(HoodieIndexVersion.V2, result);
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
        // Table version 8, SI only v1 is allowed
        Arguments.of("Table version 8 with SI v1",
            HoodieTableVersion.EIGHT,
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.V1),
            true),
        
        // Table version 8, SI v2 is not allowed
        Arguments.of("Table version 8 with SI v2",
            HoodieTableVersion.EIGHT,
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.V2),
            false),

        // Table version 9, SI can be v1 or v2
        Arguments.of("Table version 9 with SI v1",
            HoodieTableVersion.NINE,
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.V1),
            true),
        
        // Table version 9, SI v2 is allowed
        Arguments.of("Table version 9 with SI v2",
            HoodieTableVersion.NINE,
            createIndexDefinition("secondary_index_idx_test", HoodieIndexVersion.V2),
            true),

        // Table version 9, non-SI with any version is allowed
        Arguments.of("Table version 9 with non-SI v1",
            HoodieTableVersion.NINE,
            createIndexDefinition("column_stats", HoodieIndexVersion.V1),
            true),

        // Table version 9, non-SI with no version is not allowed
        Arguments.of("Table version 9 with null version",
            HoodieTableVersion.NINE,
            createIndexDefinition("column_stats", null),
            false),

        // Table version 9, SI with no version is not allowed
        Arguments.of("Table version 9 with v1",
            HoodieTableVersion.NINE,
            createIndexDefinition("secondary_index_idx_test", null),
            false)
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
    assertTrue(HoodieIndexVersion.V2.greaterThan(HoodieIndexVersion.V1));
    assertFalse(HoodieIndexVersion.V1.greaterThan(HoodieIndexVersion.V2));
    assertFalse(HoodieIndexVersion.V1.greaterThan(HoodieIndexVersion.V1));

    // Test greaterThanOrEquals
    assertTrue(HoodieIndexVersion.V2.greaterThanOrEquals(HoodieIndexVersion.V1));
    assertTrue(HoodieIndexVersion.V1.greaterThanOrEquals(HoodieIndexVersion.V1));
    assertFalse(HoodieIndexVersion.V1.greaterThanOrEquals(HoodieIndexVersion.V2));

    // Test lowerThan
    assertTrue(HoodieIndexVersion.V1.lowerThan(HoodieIndexVersion.V2));
    assertFalse(HoodieIndexVersion.V2.lowerThan(HoodieIndexVersion.V1));
    assertFalse(HoodieIndexVersion.V1.lowerThan(HoodieIndexVersion.V1));

    // Test lowerThanOrEquals
    assertTrue(HoodieIndexVersion.V1.lowerThanOrEquals(HoodieIndexVersion.V2));
    assertTrue(HoodieIndexVersion.V1.lowerThanOrEquals(HoodieIndexVersion.V1));
    assertFalse(HoodieIndexVersion.V2.lowerThanOrEquals(HoodieIndexVersion.V1));
  }

  @Test
  public void testToString() {
    assertEquals("V1", HoodieIndexVersion.V1.toString());
    assertEquals("V2", HoodieIndexVersion.V2.toString());
  }

  @Test
  public void testBuilderWithNullValuesThrowsException() {
    // Test null index name
    assertThrows(NullPointerException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexName(null)
          .withIndexType("COLUMN_STATS")
          .withSourceFields(Collections.singletonList("test_field"))
          .withVersion(HoodieIndexVersion.V1)
          .build();
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexType("COLUMN_STATS")
          .withSourceFields(Collections.singletonList("test_field"))
          .withVersion(HoodieIndexVersion.V1)
          .build();
    });

    // Test null index type
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexName("test_index")
          .withIndexType(null)
          .withSourceFields(Collections.singletonList("test_field"))
          .withVersion(HoodieIndexVersion.V1)
          .build();
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexName("test_index")
          .withSourceFields(Collections.singletonList("test_field"))
          .withVersion(HoodieIndexVersion.V1)
          .build();
    });

    // Test null version
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexName("test_index")
          .withIndexType("COLUMN_STATS")
          .withSourceFields(Collections.singletonList("test_field"))
          .withVersion(null)
          .build();
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieIndexDefinition.newBuilder()
          .withIndexName("test_index")
          .withIndexType("COLUMN_STATS")
          .withSourceFields(Collections.singletonList("test_field"))
          .build();
    });
  }
} 