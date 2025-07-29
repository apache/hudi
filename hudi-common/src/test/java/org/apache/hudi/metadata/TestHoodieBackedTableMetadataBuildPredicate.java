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

import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicate;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HoodieBackedTableMetadata.buildPredicate method.
 */
public class TestHoodieBackedTableMetadataBuildPredicate {

  @Test
  public void testBuildPredicateForSecondaryIndexPartition() {
    // Test case 1: Secondary index partition with isFullKey = true
    // Should always use prefix matching regardless of isFullKey
    String partitionName = MetadataPartitionType.SECONDARY_INDEX.getPartitionPath();
    List<String> sortedKeys = Arrays.asList("key1", "key2", "key3");
    
    Exception exception = assertThrows(IllegalArgumentException.class, () -> HoodieBackedTableMetadata.buildPredicate(partitionName, sortedKeys, true));
    // Verify it uses startsWithAny for secondary index
    assertTrue(exception.getMessage().contains("Secondary index should never use full-key lookup"));
    
    // Test case 2: Secondary index partition with isFullKey = false
    Predicate predicatePrefixKey = HoodieBackedTableMetadata.buildPredicate(partitionName, sortedKeys, false);
    assertTrue(predicatePrefixKey.getOperator().equals(Expression.Operator.STARTS_WITH));
  }

  @Test
  public void testBuildPredicateForAllMetadataPartitionTypes() {
    // Test all metadata partition types to ensure proper handling
    List<String> testKeys = Arrays.asList("key1", "key2");
    
    for (MetadataPartitionType partitionType : MetadataPartitionType.values()) {
      if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
        continue;
      }
      String partitionPath = partitionType.getPartitionPath();
      
      // Test with isFullKey = true
      Predicate predicateFullKey = HoodieBackedTableMetadata.buildPredicate(partitionPath, testKeys, true);
      assertNotNull(predicateFullKey, "Predicate should not be null for partition: " + partitionPath);
      
      // Test with isFullKey = false
      Predicate predicatePrefixKey = HoodieBackedTableMetadata.buildPredicate(partitionPath, testKeys, false);
      assertNotNull(predicatePrefixKey, "Predicate should not be null for partition: " + partitionPath);

      // Non-secondary index should use IN for full key, STARTS_WITH for prefix
      assertTrue(predicateFullKey.getOperator().equals(Expression.Operator.IN),
          "Non-secondary index should use IN for isFullKey=true");
      assertTrue(predicatePrefixKey.getOperator().equals(Expression.Operator.STARTS_WITH),
          "Non-secondary index should use STARTS_WITH for isFullKey=false");
    }
  }
}