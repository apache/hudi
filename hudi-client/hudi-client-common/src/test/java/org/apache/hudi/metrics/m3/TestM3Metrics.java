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

package org.apache.hudi.metrics.m3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestM3Metrics {

  @Mock
  HoodieWriteConfig config;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @BeforeEach
  public void start() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.M3);
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
  }

  @Test
  public void testRegisterGauge() {
    when(config.getM3ServerHost()).thenReturn("localhost");
    when(config.getM3ServerPort()).thenReturn(NetworkTestUtils.nextFreePort());
    when(config.getTableName()).thenReturn("raw_table");
    when(config.getM3Env()).thenReturn("dev");
    when(config.getM3Service()).thenReturn("hoodie");
    when(config.getM3Tags()).thenReturn("tag1=value1,tag2=value2");
    when(config.getMetricReporterMetricsNamePrefix()).thenReturn("");
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();
    metrics.registerGauge("metric1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges().get("metric1").getValue().toString());
    metrics.shutdown();
  }

  @Test
  public void testEmptyM3Tags() {
    when(config.getM3ServerHost()).thenReturn("localhost");
    when(config.getM3ServerPort()).thenReturn(NetworkTestUtils.nextFreePort());
    when(config.getTableName()).thenReturn("raw_table");
    when(config.getM3Env()).thenReturn("dev");
    when(config.getM3Service()).thenReturn("hoodie");
    when(config.getM3Tags()).thenReturn("");
    when(config.getMetricReporterMetricsNamePrefix()).thenReturn("");
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();
    metrics.registerGauge("metric1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges().get("metric1").getValue().toString());
    metrics.shutdown();
  }

  @Test
  public void testInvalidM3Tags() {
    when(config.getTableName()).thenReturn("raw_table");
    when(config.getMetricReporterMetricsNamePrefix()).thenReturn("");
    assertThrows(RuntimeException.class, () -> {
      hoodieMetrics = new HoodieMetrics(config);
    });
  }
}
