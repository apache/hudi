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

package org.apache.hudi.metrics;

import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.storage.HoodieStorage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestMetrics {
  private final HoodieStorage hoodieStorage = mock(HoodieStorage.class);

  @Test
  void metricsInstanceCached() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .withPath("/tmp/path1")
        .withReporterType("INMEMORY")
        .build();
    Metrics metrics1 = Metrics.getInstance(metricsConfig, hoodieStorage);
    Metrics metrics2 = Metrics.getInstance(metricsConfig, hoodieStorage);
    assertSame(metrics1, metrics2);
    metrics1.shutdown();
  }

  @Test
  void recreateMetricsForBasePath() {
    String path = "/tmp/path2";
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .withPath(path)
        .withReporterType("INMEMORY")
        .build();
    Metrics metrics1 = Metrics.getInstance(metricsConfig, hoodieStorage);
    metrics1.shutdown();
    assertFalse(Metrics.isInitialized(path));
    // Getting an instance after shutting down the previous one should result in a new instance
    Metrics metrics2 = Metrics.getInstance(metricsConfig, hoodieStorage);
    assertTrue(Metrics.isInitialized(path));
    assertNotSame(metrics1, metrics2);
  }
}
