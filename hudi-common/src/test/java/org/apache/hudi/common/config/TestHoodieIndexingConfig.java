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

package org.apache.hudi.common.config;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieIndexingConfig}.
 */
class TestHoodieIndexingConfig {

  @Test
  void testBuilderSetsProvidedValues() {
    HoodieIndexingConfig config = HoodieIndexingConfig.newBuilder()
        .withIndexName("idx_bloom")
        .withIndexType(MetadataPartitionType.BLOOM_FILTERS.name())
        .withIndexFunction("lower")
        .build();

    assertEquals("idx_bloom", config.getIndexName());
    assertEquals(MetadataPartitionType.BLOOM_FILTERS.name(), config.getIndexType());
    assertEquals("lower", config.getIndexFunction());
  }

  @Test
  void testBuildAppliesDefaultIndexType() {
    HoodieIndexingConfig config = HoodieIndexingConfig.newBuilder()
        .withIndexName("column_stats")
        .build();

    // INDEX_TYPE defaults to COLUMN_STATS, INDEX_NAME and INDEX_FUNCTION have no defaults.
    assertEquals(MetadataPartitionType.COLUMN_STATS.name(), config.getIndexType());
    assertEquals("column_stats", config.getIndexName());
    assertNull(config.getIndexFunction());
  }

  @Test
  void testIsIndexUsingHelpers() {
    HoodieIndexingConfig bloom = HoodieIndexingConfig.newBuilder()
        .withIndexName("idx_bloom")
        .withIndexType(MetadataPartitionType.BLOOM_FILTERS.name())
        .build();
    assertTrue(bloom.isIndexUsingBloomFilter());
    assertFalse(bloom.isIndexUsingColumnStats());
    assertFalse(bloom.isIndexUsingRecordIndex());

    HoodieIndexingConfig columnStats = HoodieIndexingConfig.newBuilder()
        .withIndexName("column_stats")
        .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
        .build();
    assertTrue(columnStats.isIndexUsingColumnStats());

    HoodieIndexingConfig recordIndex = HoodieIndexingConfig.newBuilder()
        .withIndexName("idx_record")
        .withIndexType(MetadataPartitionType.RECORD_INDEX.name())
        .build();
    assertTrue(recordIndex.isIndexUsingRecordIndex());
  }

  @Test
  void testFromPropertiesCarriesOverValues() {
    Properties props = new Properties();
    props.setProperty(HoodieIndexingConfig.INDEX_NAME.key(), "column_stats");
    props.setProperty(HoodieIndexingConfig.INDEX_FUNCTION.key(), "identity");

    HoodieIndexingConfig config = HoodieIndexingConfig.newBuilder()
        .fromProperties(props)
        .build();

    assertEquals("column_stats", config.getIndexName());
    assertEquals("identity", config.getIndexFunction());
  }

  @Test
  void testCopyProducesEqualConfig() {
    HoodieIndexingConfig source = HoodieIndexingConfig.newBuilder()
        .withIndexName("idx_bloom")
        .withIndexType(MetadataPartitionType.BLOOM_FILTERS.name())
        .withIndexFunction("lower")
        .build();

    HoodieIndexingConfig copy = HoodieIndexingConfig.copy(source);
    assertEquals(source.getIndexName(), copy.getIndexName());
    assertEquals(source.getIndexType(), copy.getIndexType());
    assertEquals(source.getIndexFunction(), copy.getIndexFunction());
  }

  @Test
  void testMergeLetsSecondConfigOverride() {
    HoodieIndexingConfig first = HoodieIndexingConfig.newBuilder()
        .withIndexName("idx_original")
        .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
        .build();
    HoodieIndexingConfig second = HoodieIndexingConfig.newBuilder()
        .withIndexType(MetadataPartitionType.BLOOM_FILTERS.name())
        .withIndexFunction("upper")
        .build();

    HoodieIndexingConfig merged = HoodieIndexingConfig.merge(first, second);
    assertEquals("idx_original", merged.getIndexName());
    assertEquals(MetadataPartitionType.BLOOM_FILTERS.name(), merged.getIndexType());
    assertEquals("upper", merged.getIndexFunction());
  }

  @Test
  void testFromIndexDefinition() {
    HoodieIndexDefinition definition = HoodieIndexDefinition.newBuilder()
        .withIndexName("column_stats")
        .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
        .withIndexFunction("identity")
        .build();

    HoodieIndexingConfig config = HoodieIndexingConfig.fromIndexDefinition(definition);
    assertEquals("column_stats", config.getIndexName());
    assertEquals(MetadataPartitionType.COLUMN_STATS.name(), config.getIndexType());
    assertEquals("identity", config.getIndexFunction());
  }

  @Test
  void testGenerateAndValidateChecksum() {
    Properties props = new Properties();
    props.setProperty(HoodieIndexingConfig.INDEX_NAME.key(), "column_stats");
    props.setProperty(HoodieIndexingConfig.INDEX_TYPE.key(), MetadataPartitionType.COLUMN_STATS.name());

    long checksum = HoodieIndexingConfig.generateChecksum(props);
    // Checksum must be deterministic for the same inputs.
    assertEquals(checksum, HoodieIndexingConfig.generateChecksum(props));

    props.setProperty(HoodieIndexingConfig.INDEX_DEFINITION_CHECKSUM.key(), String.valueOf(checksum));
    assertTrue(HoodieIndexingConfig.validateChecksum(props));

    props.setProperty(HoodieIndexingConfig.INDEX_DEFINITION_CHECKSUM.key(), String.valueOf(checksum + 1));
    assertFalse(HoodieIndexingConfig.validateChecksum(props));
  }

  @Test
  void testGenerateChecksumRequiresIndexName() {
    Properties props = new Properties();
    props.setProperty(HoodieIndexingConfig.INDEX_TYPE.key(), MetadataPartitionType.COLUMN_STATS.name());
    assertThrows(IllegalArgumentException.class, () -> HoodieIndexingConfig.generateChecksum(props));
  }

  @Test
  void testDefaultExpressionIndexRangeMetadataStorageLevel() {
    HoodieIndexingConfig config = HoodieIndexingConfig.newBuilder()
        .withIndexName("column_stats")
        .build();
    assertEquals("MEMORY_AND_DISK_SER", config.getExpressionIndexRangeMetadataStorageLevel());
  }
}
