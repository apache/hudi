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
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.escapeSpecialChars;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestHoodieTableMetadataUtil {

  @Test
  public void testGetRecordKeyToFileGroupIndexFunction() {
    // Test with secondary key format
    String compositeKey = "secondaryKey$recordKey";

    SerializableBiFunction<String, Integer, Integer> hashOnSecKeyOnly =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(true);
    SerializableBiFunction<String, Integer, Integer> hashOnFullKey =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(false);

    int result1 = hashOnSecKeyOnly.apply(compositeKey, 10);
    int result2 = hashOnSecKeyOnly.apply("anotherSecondaryKey$anotherRecordKey", 10);

    // Both should hash the secondary key portion
    assertNotEquals(result1, result2);

    // Test with regular key format
    int result3 = hashOnFullKey.apply("simpleKey", 10);
    int result4 = hashOnFullKey.apply("anotherSimpleKey", 10);

    // Both should hash the full key
    assertNotEquals(result3, result4);

    // Hash on Sec key only <=> Hash full key if key equals to UNESCAPED sec key
    assertEquals(hashOnSecKeyOnly.apply("secKey$recKey", 10), hashOnFullKey.apply("secKey", 10));
    assertEquals(hashOnSecKeyOnly.apply(constructSecondaryIndexKey("seckey", "reckey"), 10), hashOnFullKey.apply("seckey", 10));
    assertEquals(hashOnSecKeyOnly.apply(constructSecondaryIndexKey("$", "reckey"), 10), hashOnFullKey.apply(escapeSpecialChars("$"), 10));
  }
}
