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

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieConsoleMetrics {

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @BeforeEach
  public void start() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CONSOLE);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    metrics = hoodieMetrics.getMetrics();
  }

  @AfterEach
  public void stop() {
    metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    metrics.registerGauge("metric1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges().get("metric1").getValue().toString());
  }
}
