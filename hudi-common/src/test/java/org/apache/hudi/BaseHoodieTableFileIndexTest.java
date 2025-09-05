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

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieMetadataConfig;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class BaseHoodieTableFileIndexTest {

  @Test
  public void testGetMetadataConfigReturnsFieldValue() throws Exception {
    // Create a mock of BaseHoodieTableFileIndex
    BaseHoodieTableFileIndex fileIndex = mock(BaseHoodieTableFileIndex.class, 
        org.mockito.Mockito.CALLS_REAL_METHODS);
    
    // Create a test metadata config
    HoodieMetadataConfig testConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexBloomFilter(true)
        .withMetadataIndexColumnStats(true)
        .build();
    
    // Use reflection to set the private metadataConfig field
    Field metadataConfigField = BaseHoodieTableFileIndex.class.getDeclaredField("metadataConfig");
    metadataConfigField.setAccessible(true);
    metadataConfigField.set(fileIndex, testConfig);
    
    // Test the getMetadataConfig method
    HoodieMetadataConfig result = fileIndex.getMetadataConfig();
    
    assertNotNull(result, "Metadata config should not be null");
    assertSame(testConfig, result, "Should return the same metadata config instance");
    assertEquals(true, result.isEnabled(), "Metadata should be enabled");
    assertEquals(true, result.isBloomFilterIndexEnabled(), "Bloom filter index should be enabled");
    assertEquals(true, result.isColumnStatsIndexEnabled(), "Column stats index should be enabled");
  }
}