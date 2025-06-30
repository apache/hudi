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

import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
} 