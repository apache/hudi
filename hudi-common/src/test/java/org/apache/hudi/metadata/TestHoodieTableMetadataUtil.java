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

import org.apache.hudi.common.function.SerializableBiFunction;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.constructSecondaryIndexKey;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieTableMetadataUtil {

  @Test
  public void testGetRecordKeyToFileGroupIndexFunction() {
    int numFileGroups = 10;
    String recordKey = "recordKey$";
    String secondaryKey = "secondaryKey$";
    // Raw key used for read path
    SecondaryIndexPrefixRawKey rawKey1 = new SecondaryIndexPrefixRawKey(secondaryKey);
    // Composite key used for write path
    String compositeKey = constructSecondaryIndexKey(secondaryKey, recordKey);

    SerializableBiFunction<String, Integer, Integer> hashOnSecKeyOnly =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(true);
    SerializableBiFunction<String, Integer, Integer> hashOnFullKey =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(false);

    // On write path we use hashOnSecKeyOnly
    int result1 = hashOnSecKeyOnly.apply(compositeKey, numFileGroups);
    // On read path, we use hashOnFullKey
    int result2 = hashOnFullKey.apply(rawKey1.encode(), numFileGroups);

    // Both should hash the secondary key portion so read and write paths are consistent.
    assertEquals(result1, result2);
  }
}
