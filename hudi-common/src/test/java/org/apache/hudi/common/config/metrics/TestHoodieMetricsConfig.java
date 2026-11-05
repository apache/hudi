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

package org.apache.hudi.common.config.metrics;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.config.HoodieCommonConfig.META_SYNC_BASE_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestHoodieMetricsConfig {
  @Test
  void testReturnsBasePathWhenSetViaBuilder() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
        .withPath("primary/base/path")
        .build();
    assertEquals("primary/base/path", config.getBasePath());
  }

  @Test
  void testReturnsMetaSyncPathWhenBasePathIsNotSet() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder().build();
    config.setValue(META_SYNC_BASE_PATH_KEY, "base/path/set/during/sync");
    assertEquals("base/path/set/during/sync", config.getBasePath());
  }

  /**
   * Turning metrics on is consent to report, not consent to start collecting a new class of metric.
   * Record index lookup collection registers a Spark accumulator per table and runs per shard read, so
   * an operator has to ask for it by name rather than inherit it on upgrade.
   */
  @Test
  void recordIndexLookupMetricsStayOffUnlessAskedForByName() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder().on(true).build();
    assertFalse(config.getBoolean(HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE),
        "enabling metrics must not silently enrol a table in record index lookup collection");
  }

  @Test
  void testReturnsNullWhenNeitherBasePathNorMetaSyncIsSet() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder().build();
    assertNull(config.getBasePath());
  }
}
