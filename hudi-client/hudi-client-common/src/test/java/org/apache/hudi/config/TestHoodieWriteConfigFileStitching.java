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

package org.apache.hudi.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for file stitching binary copy schema evolution configuration.
 */
public class TestHoodieWriteConfigFileStitching {

  @Test
  public void testFileStitchingBinaryCopySchemaEvolutionConfig() {
    // Test default value (should be false)
    HoodieWriteConfig config1 = HoodieWriteConfig.newBuilder()
        .withPath("/test/path")
        .build();
    assertFalse(config1.isBinaryCopySchemaEvolutionEnabled(),
        "File stitching binary copy schema evolution should be disabled by default");

    // Test explicitly setting to false
    Properties props = new Properties();
    props.setProperty(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE.key(), "false");
    HoodieWriteConfig config2 = HoodieWriteConfig.newBuilder()
        .withPath("/test/path")
        .withProps(props)
        .build();
    assertFalse(config2.isBinaryCopySchemaEvolutionEnabled(),
        "File stitching binary copy schema evolution should be disabled when explicitly set to false");

    // Test explicitly setting to true
    props.setProperty(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE.key(), "true");
    HoodieWriteConfig config3 = HoodieWriteConfig.newBuilder()
        .withPath("/test/path")
        .withProps(props)
        .build();
    assertTrue(config3.isBinaryCopySchemaEvolutionEnabled(),
        "File stitching binary copy schema evolution should be enabled when explicitly set to true");
  }

  @Test
  public void testClusteringConfigProperty() {
    // Test that the clustering config property has the correct default value
    assertEquals(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE.key(),
        HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE.key(),
        "Config key should match expected value");
    
    assertFalse(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE.defaultValue(),
        "Default value should be false to disable schema evolution");
  }
}