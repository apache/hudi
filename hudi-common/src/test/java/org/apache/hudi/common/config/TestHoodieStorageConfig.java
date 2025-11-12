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

package org.apache.hudi.common.config;

import org.apache.hudi.common.bloom.BloomFilterTypeCode;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestHoodieStorageConfig {
  @Test
  void testHoodieStorageConfig() {
    String bloomFilterType = BloomFilterTypeCode.SIMPLE.name();
    assertNotEquals(BLOOM_FILTER_TYPE.defaultValue().toUpperCase(),
        bloomFilterType.toUpperCase());
    Properties props = new Properties();
    props.put(BLOOM_FILTER_TYPE.key(), bloomFilterType);
    HoodieStorageConfig config = HoodieStorageConfig.newBuilder()
        .fromProperties(props).build();
    assertEquals(bloomFilterType, config.getBloomFilterType());
  }

  @Test
  void testBloomFilterBuilderWithAllValues() {
    String expectedFilterType = BloomFilterTypeCode.SIMPLE.name();
    int expectedNumEntries = 80000;
    double expectedFpp = 0.000000005;
    int expectedDynamicMaxEntries = 150000;

    HoodieStorageConfig storageConfig = HoodieStorageConfig.newBuilder()
        .withBloomFilterType(expectedFilterType)
        .withBloomFilterNumEntries(expectedNumEntries)
        .withBloomFilterFpp(expectedFpp)
        .withBloomFilterDynamicMaxEntries(expectedDynamicMaxEntries).build();

    // Verify values are set correctly
    assertEquals(expectedFilterType, storageConfig.getString(BLOOM_FILTER_TYPE));
    assertEquals(expectedNumEntries, storageConfig.getInt(BLOOM_FILTER_NUM_ENTRIES_VALUE));
    assertEquals(expectedFpp, storageConfig.getDouble(BLOOM_FILTER_FPP_VALUE));
    assertEquals(expectedDynamicMaxEntries, storageConfig.getInt(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES));
  }

  @Test
  void testBloomFilterBuilderWithDefaultValues() {
    HoodieStorageConfig storageConfig = HoodieStorageConfig.newBuilder().build();
    // Verify values are set correctly
    assertEquals(BLOOM_FILTER_TYPE.defaultValue(), storageConfig.getString(BLOOM_FILTER_TYPE));
    assertEquals(BLOOM_FILTER_NUM_ENTRIES_VALUE.defaultValue(), storageConfig.getString(BLOOM_FILTER_NUM_ENTRIES_VALUE));
    assertEquals(BLOOM_FILTER_FPP_VALUE.defaultValue(), storageConfig.getString(BLOOM_FILTER_FPP_VALUE));
    assertEquals(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue(), storageConfig.getString(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES));
  }
}
