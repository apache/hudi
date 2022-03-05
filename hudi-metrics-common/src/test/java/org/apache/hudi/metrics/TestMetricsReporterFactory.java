/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.config.HoodieMetricsConfig;
import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;
import org.apache.hudi.metrics.datadog.DatadogHttpClient;
import org.apache.hudi.metrics.datadog.DatadogMetricsReporter;
import org.apache.hudi.metrics.prometheus.PrometheusReporter;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestMetricsReporterFactory {

  @Mock
  HoodieMetricsConfig config;

  @Mock
  HoodieMetricRegistry registry;

  @Test
  public void metricsReporterFactoryShouldReturnReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof InMemoryMetricsReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnJMXReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(config.getJmxHost()).thenReturn("localhost");
    when(config.getJmxPort()).thenReturn(String.valueOf(NetworkTestUtils.nextFreePort()));
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof JmxMetricsReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnGraphiteReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.GRAPHITE);
    when(config.getGraphiteServerHost()).thenReturn("localhost");
    when(config.getGraphiteServerPort()).thenReturn(NetworkTestUtils.nextFreePort());
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof MetricsGraphiteReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnPrometheusReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof PrometheusReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnConsoleReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.CONSOLE);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof ConsoleMetricsReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnDatadogReporter() {
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.DATADOG);
    when(config.getDatadogApiSite()).thenReturn(DatadogHttpClient.ApiSite.EU);
    when(config.getDatadogApiKey()).thenReturn("foo");
    when(config.getDatadogApiKeySkipValidation()).thenReturn(true);
    when(config.getDatadogMetricPrefix()).thenReturn("bar");
    when(config.getDatadogMetricHost()).thenReturn("foo");
    when(config.getDatadogMetricTags()).thenReturn(Arrays.asList("baz", "foo"));
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof DatadogMetricsReporter);
  }

  @Test
  public void metricsReporterFactoryShouldReturnUserDefinedReporter() {
    when(config.getMetricReporterClassName()).thenReturn(DummyMetricsReporter.class.getName());

    TypedProperties props = new TypedProperties();
    props.setProperty("testKey", "testValue");

    when(config.getProps()).thenReturn(props);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry);
    assertTrue(reporter instanceof CustomizableMetricsReporter);
    assertEquals(props, ((DummyMetricsReporter) reporter).getProps());
    assertEquals(registry, ((DummyMetricsReporter) reporter).getRegistry());
  }

  @Test
  public void metricsReporterFactoryShouldThrowExceptionWhenMetricsReporterClassIsIllegal() {
    when(config.getMetricReporterClassName()).thenReturn(IllegalTestMetricsReporter.class.getName());
    when(config.getProps()).thenReturn(new TypedProperties());
    assertThrows(HoodieException.class, () -> MetricsReporterFactory.createReporter(config, registry));
  }

  public static class DummyMetricsReporter extends CustomizableMetricsReporter {

    public DummyMetricsReporter(Properties props, HoodieMetricRegistry registry) {
      super(props, registry);
    }

    @Override
    public void start() {}

    @Override
    public void report() {}

    @Override
    public Closeable getReporter() {
      return null;
    }

    @Override
    public void stop() {}
  }

  public static class IllegalTestMetricsReporter {

    public IllegalTestMetricsReporter(Properties props, MetricRegistry registry) {}
  }
}


