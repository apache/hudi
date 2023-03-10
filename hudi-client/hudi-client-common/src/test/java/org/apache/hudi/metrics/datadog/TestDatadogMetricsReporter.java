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

import org.apache.hudi.config.HoodieWriteConfig;
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
  HoodieWriteConfig config;
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
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("table1");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(config.getDatadogApiKey()).thenReturn("");
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());

    Throwable t = assertThrows(IllegalStateException.class, () -> {
      hoodieMetrics = new HoodieMetrics(config);
      metrics = hoodieMetrics.getMetrics();
    });
    assertEquals("Datadog cannot be initialized: API key is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldFailWhenNoMetricPrefix() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("table1");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(config.getDatadogApiKey()).thenReturn("foo");
    when(config.getDatadogMetricPrefix()).thenReturn("");
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    Throwable t = assertThrows(IllegalStateException.class, () -> {
      hoodieMetrics = new HoodieMetrics(config);
      metrics = hoodieMetrics.getMetrics();
    });
    assertEquals("Datadog cannot be initialized: Metric prefix is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldSucceed() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("table1");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(config.getDatadogApiSite()).thenReturn(ApiSite.EU);
    when(config.getDatadogApiKey()).thenReturn("foo");
    when(config.getDatadogApiKeySkipValidation()).thenReturn(true);
    when(config.getDatadogMetricPrefix()).thenReturn("bar");
    when(config.getDatadogMetricHost()).thenReturn("foo");
    when(config.getDatadogMetricTags()).thenReturn(Arrays.asList("baz", "foo"));
    when(config.getDatadogReportPeriodSeconds()).thenReturn(10);
    when(config.getMetricReporterMetricsNamePrefix()).thenReturn("");
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    assertDoesNotThrow(() -> {
      hoodieMetrics = new HoodieMetrics(config);
      metrics = hoodieMetrics.getMetrics();
    });
  }
}
