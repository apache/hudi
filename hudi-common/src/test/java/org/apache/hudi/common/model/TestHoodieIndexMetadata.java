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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieIndexMetadata}.
 */
public class TestHoodieIndexMetadata {
  @Test
  void testSerDeWithIgnoredFields() throws Exception {
    String indexName = MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath() + "idx";
    HoodieIndexDefinition def = HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType("column_stats")
        .withIndexFunction("identity")
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), indexName))
        .withSourceFields(Arrays.asList("a", "b", "c"))
        .withIndexOptions(Collections.emptyMap())
        .build();
    assertThat(def.getSourceFieldsKey(), is("a.b.c"));
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(Collections.singletonMap(indexName, def));
    String serialized = indexMetadata.toJson();
    assertFalse(serialized.contains("sourceFieldsKey"), "The field 'sourceFieldsKey' should be ignored in serialization");
    HoodieIndexMetadata deserialized = HoodieIndexMetadata.fromJson(serialized);
    Map<String, HoodieIndexDefinition> indexDefinitionMap = deserialized.getIndexDefinitions();
    assertThat(indexDefinitionMap.size(), is(1));
    assertThat(indexDefinitionMap.values().iterator().next().getSourceFieldsKey(), is("a.b.c"));
  }

  @Test
  void testHasIndex() {
    // Create test index definitions
    Map<String, HoodieIndexDefinition> indexes = new HashMap<>();
    HoodieIndexDefinition colStatsIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName("column_stats")
        .withIndexType("column_stats")
        .withSourceFields(Arrays.asList("col1", "col2"))
        .build();
    HoodieIndexDefinition secondaryIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName("secondary_index_price")
        .withIndexType("secondary_index")
        .withSourceFields(Collections.singletonList("price"))
        .build();
    
    indexes.put("column_stats", colStatsIndex);
    indexes.put("secondary_index_price", secondaryIndex);
    
    HoodieIndexMetadata metadata = new HoodieIndexMetadata(indexes);
    
    // Test hasIndex
    assertTrue(metadata.hasIndex("column_stats"));
    assertTrue(metadata.hasIndex("secondary_index_price"));
    assertFalse(metadata.hasIndex("non_existent_index"));
    assertFalse(metadata.hasIndex(null));
    
    // Test with empty metadata
    HoodieIndexMetadata emptyMetadata = new HoodieIndexMetadata();
    assertFalse(emptyMetadata.hasIndex("any_index"));
  }

  @Test
  void testGetIndex() {
    Map<String, HoodieIndexDefinition> indexes = new HashMap<>();
    HoodieIndexDefinition colStatsIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName("column_stats")
        .withIndexType("column_stats")
        .withSourceFields(Arrays.asList("col1", "col2"))
        .build();
    HoodieIndexDefinition secondaryIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName("secondary_index_price")
        .withIndexType("secondary_index")
        .withSourceFields(Collections.singletonList("price"))
        .build();
    
    indexes.put("column_stats", colStatsIndex);
    indexes.put("secondary_index_price", secondaryIndex);
    
    HoodieIndexMetadata metadata = new HoodieIndexMetadata(indexes);
    
    // Test getIndex for existing indexes
    Option<HoodieIndexDefinition> colStatsOpt = metadata.getIndex("column_stats");
    assertTrue(colStatsOpt.isPresent());
    assertEquals("column_stats", colStatsOpt.get().getIndexName());
    assertEquals("column_stats", colStatsOpt.get().getIndexType());
    
    Option<HoodieIndexDefinition> secondaryOpt = metadata.getIndex("secondary_index_price");
    assertTrue(secondaryOpt.isPresent());
    assertEquals("secondary_index_price", secondaryOpt.get().getIndexName());
    assertEquals("secondary_index", secondaryOpt.get().getIndexType());
    
    // Test getIndex for non-existent index
    Option<HoodieIndexDefinition> nonExistentOpt = metadata.getIndex("non_existent_index");
    assertFalse(nonExistentOpt.isPresent());
    
    // Test getIndex with null
    Option<HoodieIndexDefinition> nullOpt = metadata.getIndex(null);
    assertFalse(nullOpt.isPresent());
    
    // Test with empty metadata
    HoodieIndexMetadata emptyMetadata = new HoodieIndexMetadata();
    Option<HoodieIndexDefinition> emptyOpt = emptyMetadata.getIndex("any_index");
    assertFalse(emptyOpt.isPresent());
  }
}
