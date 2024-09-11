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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPrometheusReporter {

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @AfterEach
  void shutdownMetrics() {
    if (metrics != null) {
      metrics.shutdown();
    }
  }

  @Test
  public void testRegisterGauge() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS);
    when(metricsConfig.getPrometheusPort()).thenReturn(9090);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    assertDoesNotThrow(() -> {
      new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });
  }
}
