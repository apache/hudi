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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieMetadataConfig}.
 */
class TestHoodieMetadataConfig {
  @Test
  void testGetRecordPreparationParallelism() {
    // Test default value
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().build();
    assertEquals(0, config.getRecordPreparationParallelism());

    // Test custom value
    Properties props = new Properties();
    props.put(HoodieMetadataConfig.RECORD_PREPARATION_PARALLELISM.key(), "100");
    HoodieMetadataConfig configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertEquals(100, configWithCustomValue.getRecordPreparationParallelism());

    // Test zero value
    Properties propsZero = new Properties();
    propsZero.put(HoodieMetadataConfig.RECORD_PREPARATION_PARALLELISM.key(), "0");
    HoodieMetadataConfig configWithZeroValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(propsZero)
        .build();
    assertEquals(0, configWithZeroValue.getRecordPreparationParallelism());

    // Test negative value
    Properties propsNegative = new Properties();
    propsNegative.put(HoodieMetadataConfig.RECORD_PREPARATION_PARALLELISM.key(), "-50");
    HoodieMetadataConfig configWithNegativeValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(propsNegative)
        .build();
    assertEquals(-50, configWithNegativeValue.getRecordPreparationParallelism());
  }

  @Test
  void testStreamingWritesCoalesceDivisorForDataTableWrites() {
    // Test default value
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().build();
    assertEquals(5000, config.getStreamingWritesCoalesceDivisorForDataTableWrites());

    // Test custom value
    Properties props = new Properties();
    props.put(HoodieMetadataConfig.STREAMING_WRITE_DATATABLE_WRITE_STATUSES_COALESCE_DIVISOR.key(), "1");
    HoodieMetadataConfig configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertEquals(1, configWithCustomValue.getStreamingWritesCoalesceDivisorForDataTableWrites());

    Properties propsZero = new Properties();
    propsZero.put(HoodieMetadataConfig.STREAMING_WRITE_DATATABLE_WRITE_STATUSES_COALESCE_DIVISOR.key(), "10000");
    HoodieMetadataConfig configWithZeroValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(propsZero)
        .build();
    assertEquals(10000, configWithZeroValue.getStreamingWritesCoalesceDivisorForDataTableWrites());
  }

}
