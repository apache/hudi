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

package org.apache.hudi.metrics.datadog;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDatadogMetricsReporter {

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @Mock
  MetricRegistry registry;

  @AfterEach
  void shutdownMetrics() {
    if (metrics != null) {
      metrics.shutdown();
    }
  }

  @Test
  public void instantiationShouldFailWhenNoApiKey() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);

    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(metricsConfig.getDatadogApiKey()).thenReturn("");
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());

    Throwable t = assertThrows(IllegalStateException.class, () -> {
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });
    assertEquals("Datadog cannot be initialized: API key is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldFailWhenNoMetricPrefix() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);

    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(metricsConfig.getDatadogApiKey()).thenReturn("foo");
    when(metricsConfig.getDatadogMetricPrefix()).thenReturn("");
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    Throwable t = assertThrows(IllegalStateException.class, () -> {
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });
    assertEquals("Datadog cannot be initialized: Metric prefix is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldSucceed() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);

    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(metricsConfig.getDatadogApiSite()).thenReturn(ApiSite.EU);
    when(metricsConfig.getDatadogApiKey()).thenReturn("foo");
    when(metricsConfig.getDatadogApiKeySkipValidation()).thenReturn(true);
    when(metricsConfig.getDatadogMetricPrefix()).thenReturn("bar");
    when(metricsConfig.getDatadogMetricHost()).thenReturn("foo");
    when(metricsConfig.getDatadogMetricTags()).thenReturn(Arrays.asList("baz", "foo"));
    when(metricsConfig.getDatadogReportPeriodSeconds()).thenReturn(10);
    when(metricsConfig.getMetricReporterMetricsNamePrefix()).thenReturn("");
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    assertDoesNotThrow(() -> {
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });
  }
}
