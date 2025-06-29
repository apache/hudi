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

import org.apache.hudi.common.function.SerializableFunction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTableMetadataUtil {

  @Test
  public void testGetRecordKeyToFileGroupIndexFunctionOptimized() throws Exception {
    String partitionName = "secondary_index_test_index";
    HoodieIndexVersion version = HoodieIndexVersion.V2;
    
    // Test with secondary key format
    String compositeKey = "secondaryKey$recordKey";
    boolean useSecondaryKeyForHashing = true;

    SerializableFunction<String, SerializableFunction<Integer, Integer>> optimizedFunction =
        HoodieTableMetadataUtil.getRecordKeyToFileGroupIndexFunction(partitionName, version, useSecondaryKeyForHashing);
    
    int result1 = optimizedFunction.apply(compositeKey).apply(10);
    int result2 = optimizedFunction.apply("anotherSecondaryKey$anotherRecordKey").apply(10);
    
    // Both should hash the secondary key portion
    assertNotEquals(result1, result2);
    
    // Test with regular key format
    useSecondaryKeyForHashing = false;
    optimizedFunction = HoodieTableMetadataUtil.getRecordKeyToFileGroupIndexFunction(partitionName, version, useSecondaryKeyForHashing);
    
    int result3 = optimizedFunction.apply("simpleKey").apply(10);
    int result4 = optimizedFunction.apply("anotherSimpleKey").apply(10);
    
    // Both should hash the full key
    assertNotEquals(result3, result4);
  }

  @Test
  public void testShouldUseSecondaryKeyForHashing() {
    String secondaryIndexPartition = "secondary_index_test_index";
    String regularPartition = "record_index";
    HoodieIndexVersion version = HoodieIndexVersion.V2;
    
    // Test secondary index partition with composite key
    assertTrue(HoodieTableMetadataUtil.shouldUseSecondaryKeyForHashing(
        secondaryIndexPartition, version, "secondaryKey$recordKey"));
    
    // Test secondary index partition with simple key (should not use secondary key)
    assertFalse(HoodieTableMetadataUtil.shouldUseSecondaryKeyForHashing(
        secondaryIndexPartition, version, "simpleKey"));
    
    // Test regular partition (should not use secondary key regardless of key format)
    assertFalse(HoodieTableMetadataUtil.shouldUseSecondaryKeyForHashing(
        regularPartition, version, "secondaryKey$recordKey"));
    assertFalse(HoodieTableMetadataUtil.shouldUseSecondaryKeyForHashing(
        regularPartition, version, "simpleKey"));
    
    // Test with older version (should not use secondary key)
    assertFalse(HoodieTableMetadataUtil.shouldUseSecondaryKeyForHashing(
        secondaryIndexPartition, HoodieIndexVersion.V1, "secondaryKey$recordKey"));
  }
} 