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

package com.uber.hudi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hudi.config.HoodieWriteConfig;

/**
 * Tests to ensure that the defaults of the various HUDI configs are always consistent with
 * what is expected at Uber across various HUDI versions.
 */
public class TestConfigDefaultsForUber {

  @Test
  public void testConfigDefaults() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/").build();

    // Compaction config defaults
    assertEquals(0, config.getParquetSmallFileLimit());
    assertEquals(96, config.getMinCommitsToKeep());
    assertEquals(128, config.getMaxCommitsToKeep());
    assertEquals(24, config.getCleanerCommitsRetained());

    // Memory config defaults
    assertEquals(1.0, config.getWriteStatusFailureFraction(), 0.0);
  }
}
