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
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDatadogMetricsReporter {

  @Mock
  HoodieWriteConfig config;

  @Mock
  MetricRegistry registry;

  @AfterEach
  void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void instantiationShouldFailWhenNoApiKey() {
    when(config.getDatadogApiKey()).thenReturn("");
    Throwable t = assertThrows(IllegalStateException.class, () -> {
      new DatadogMetricsReporter(config, registry);
    });
    assertEquals("Datadog cannot be initialized: API key is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldFailWhenNoMetricPrefix() {
    when(config.getDatadogApiKey()).thenReturn("foo");
    when(config.getDatadogMetricPrefix()).thenReturn("");
    Throwable t = assertThrows(IllegalStateException.class, () -> {
      new DatadogMetricsReporter(config, registry);
    });
    assertEquals("Datadog cannot be initialized: Metric prefix is null or empty.", t.getMessage());
  }

  @Test
  public void instantiationShouldSucceed() {
    when(config.getDatadogApiSite()).thenReturn(ApiSite.EU);
    when(config.getDatadogApiKey()).thenReturn("foo");
    when(config.getDatadogApiKeySkipValidation()).thenReturn(true);
    when(config.getDatadogMetricPrefix()).thenReturn("bar");
    when(config.getDatadogMetricHost()).thenReturn("foo");
    when(config.getDatadogMetricTags()).thenReturn(Arrays.asList("baz", "foo"));
    assertDoesNotThrow(() -> {
      new DatadogMetricsReporter(config, registry);
    });
  }
}
