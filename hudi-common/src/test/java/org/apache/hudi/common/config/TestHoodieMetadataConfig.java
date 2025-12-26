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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  void testGlobalRLI() {
    // Test default value
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().build();
    assertFalse(config.isGlobalRecordLevelIndexEnabled());

    //set older config property.
    Properties props = new Properties();
    props.put("hoodie.metadata.record.index.enable", "true");
    HoodieMetadataConfig configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertTrue(configWithCustomValue.isGlobalRecordLevelIndexEnabled());

    // set latest config property
    props = new Properties();
    props.put(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertTrue(configWithCustomValue.isGlobalRecordLevelIndexEnabled());
  }

  @Test
  void testRecordIndexMaxFileGroupSizeBytes() {
    // Test default value (1GB)
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().build();
    assertEquals(1024L * 1024L * 1024L, config.getRecordIndexMaxFileGroupSizeBytes());

    // Test custom value using builder method
    long customSize = 2L * 1024L * 1024L * 1024L; // 2GB
    HoodieMetadataConfig configWithBuilder = HoodieMetadataConfig.newBuilder()
        .withRecordIndexMaxFileGroupSizeBytes(customSize)
        .build();
    assertEquals(customSize, configWithBuilder.getRecordIndexMaxFileGroupSizeBytes());

    // Test custom value via Properties
    Properties props = new Properties();
    props.put(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), String.valueOf(customSize));
    HoodieMetadataConfig configWithProperties = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertEquals(customSize, configWithProperties.getRecordIndexMaxFileGroupSizeBytes());

    // Test value larger than Integer.MAX_VALUE to ensure long is properly handled
    long largeSize = 3L * 1024L * 1024L * 1024L; // 3GB (exceeds Integer.MAX_VALUE which is ~2.1GB)
    Properties propsLarge = new Properties();
    propsLarge.put(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), String.valueOf(largeSize));
    HoodieMetadataConfig configWithLargeValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(propsLarge)
        .build();
    assertEquals(largeSize, configWithLargeValue.getRecordIndexMaxFileGroupSizeBytes());

    // Verify that the value is indeed larger than Integer.MAX_VALUE
    assertTrue(largeSize > Integer.MAX_VALUE, "Test value should exceed Integer.MAX_VALUE to validate long type");
  }


  @Test
  public void testCleanerRollbackParallelism() {
    // Test default value
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().build();
    assertEquals(512, config.getCleanerParallelism());
    assertEquals(512, config.getRollbackParallelism());
    assertEquals(512, config.getFinalizeWritesParallelism());

    // Test custom value
    Properties props = new Properties();
    props.put(HoodieMetadataConfig.CLEANER_PARALLELISM.key(), "100");
    props.put(HoodieMetadataConfig.ROLLBACK_PARALLELISM.key(), "100");
    props.put(HoodieMetadataConfig.FINALIZE_WRITES_PARALLELISM.key(), "100");
    HoodieMetadataConfig configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertEquals(100, configWithCustomValue.getCleanerParallelism());
    assertEquals(100, configWithCustomValue.getRollbackParallelism());
    assertEquals(100, configWithCustomValue.getFinalizeWritesParallelism());

    // Test zero value
    Properties propsZero = new Properties();
    props = new Properties();
    props.put(HoodieMetadataConfig.CLEANER_PARALLELISM.key(), "0");
    props.put(HoodieMetadataConfig.ROLLBACK_PARALLELISM.key(), "0");
    props.put(HoodieMetadataConfig.FINALIZE_WRITES_PARALLELISM.key(), "0");
    configWithCustomValue = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
    assertEquals(0, configWithCustomValue.getCleanerParallelism());
    assertEquals(0, configWithCustomValue.getRollbackParallelism());
    assertEquals(0, configWithCustomValue.getFinalizeWritesParallelism());
  }
}
