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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;
import org.apache.hudi.metrics.prometheus.PrometheusReporter;
import org.apache.hudi.metrics.prometheus.PushGatewayMetricsReporter;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestMetricsReporterFactory {

  @Mock
  HoodieMetricsConfig metricsConfig;

  @Mock
  MetricRegistry registry;

  public static Object[][] params() {
    return new Object[][] {
        {MetricsReporterType.INMEMORY, InMemoryMetricsReporter.class},
        {MetricsReporterType.CONSOLE, ConsoleMetricsReporter.class},
        {MetricsReporterType.PROMETHEUS, PrometheusReporter.class},
        {MetricsReporterType.PROMETHEUS_PUSHGATEWAY, PushGatewayMetricsReporter.class},
        {MetricsReporterType.SLF4J, Slf4jMetricsReporter.class}};
  }

  @ParameterizedTest
  @MethodSource("params")
  public void metricsReporterFactoryShouldReturnReporter(MetricsReporterType type, Class expectClazz) {
    when(metricsConfig.getMetricsReporterType()).thenReturn(type);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(metricsConfig, registry).get();
    assertEquals(reporter.getClass(), expectClazz);
  }

  @Test
  void metricsReporterFactoryShouldReturnCloudWatchReporter() {
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);

    MetricsReporter reporterMock = mock(MetricsReporter.class);
    try (MockedStatic<ReflectionUtils> mockedStatic = Mockito.mockStatic(ReflectionUtils.class)) {
      mockedStatic.when(() ->
          ReflectionUtils.loadClass(
              eq("org.apache.hudi.aws.metrics.cloudwatch.CloudWatchMetricsReporter"),
              any(Class[].class),
              eq(metricsConfig),
              eq(registry)
          )
      ).thenReturn(reporterMock);

      MetricsReporter actualReporter = MetricsReporterFactory.createReporter(metricsConfig, registry).get();
      assertSame(reporterMock, actualReporter);
    }
  }

  @Test
  void metricsReporterFactoryShouldReturnUserDefinedReporter() {
    TypedProperties props = new TypedProperties();
    props.setProperty("testKey", "testValue");

    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
        .fromProperties(props)
        .withReporterType(MetricsReporterType.CUSTOM.name())
        .withReporterClass(DummyMetricsReporter.class.getName())
        .build();

    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry).get();
    assertTrue(reporter instanceof CustomizableMetricsReporter);
    assertEquals("testValue", ((DummyMetricsReporter) reporter).getProps().getProperty("testKey"));
    assertEquals(registry, ((DummyMetricsReporter) reporter).getRegistry());
  }

  @Test
  void metricsReporterFactoryShouldThrowExceptionWhenMetricsReporterClassIsIllegal() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
        .withReporterType(MetricsReporterType.CUSTOM.name())
        .withReporterClass(IllegalTestMetricsReporter.class.getName())
        .build();
    assertThrows(HoodieException.class, () -> MetricsReporterFactory.createReporter(config, registry));
  }

  @Test
  void customTypeWithoutClassNameThrows() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
        .withReporterType(MetricsReporterType.CUSTOM.name())
        .build();
    HoodieException ex = assertThrows(HoodieException.class,
        () -> MetricsReporterFactory.createReporter(config, registry));
    assertTrue(ex.getMessage().contains(HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME.key()));
  }

  @Test
  void customReporterWithConfigConstructorIsLoaded() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
        .withReporterType(MetricsReporterType.CUSTOM.name())
        .withReporterClass(ConfigConstructorReporter.class.getName())
        .build();
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry).get();
    assertTrue(reporter instanceof ConfigConstructorReporter);
    assertSame(config, ((ConfigConstructorReporter) reporter).getConfig());
    assertSame(registry, ((ConfigConstructorReporter) reporter).getRegistry());
  }

  @Test
  void customReporterWithNoArgConstructorIsLoaded() {
    HoodieMetricsConfig config = HoodieMetricsConfig.newBuilder()
            .withReporterType(MetricsReporterType.CUSTOM.name())
            .withReporterClass(NoArgConstructorReporter.class.getName())
        .build();
    MetricsReporter reporter = MetricsReporterFactory.createReporter(config, registry).get();
    assertTrue(reporter instanceof NoArgConstructorReporter);
  }

  public static class DummyMetricsReporter extends CustomizableMetricsReporter {

    public DummyMetricsReporter(Properties props, MetricRegistry registry) {
      super(props, registry);
    }

    @Override
    public void start() {
      // no-op
    }

    @Override
    public void report() {
      // no-op
    }

    @Override
    public void stop() {
      // no-op
    }
  }

  public static class IllegalTestMetricsReporter {

    public IllegalTestMetricsReporter(Properties props, MetricRegistry registry) {
    }
  }

  /** Uses the (HoodieMetricsConfig, MetricRegistry) constructor — no Properties constructor. */
  public static class ConfigConstructorReporter extends MetricsReporter {

    private final HoodieMetricsConfig config;
    private final MetricRegistry registry;

    public ConfigConstructorReporter(HoodieMetricsConfig config, MetricRegistry registry) {
      this.config = config;
      this.registry = registry;
    }

    public HoodieMetricsConfig getConfig() {
      return config;
    }

    public MetricRegistry getRegistry() {
      return registry;
    }

    @Override
    public void start() {
    }

    @Override
    public void report() {
    }

    @Override
    public void stop() {
    }
  }

  /** Uses no-arg constructor — no Properties or HoodieMetricsConfig constructor. */
  public static class NoArgConstructorReporter extends MetricsReporter {

    @Override
    public void start() {
    }

    @Override
    public void report() {
    }

    @Override
    public void stop() {
    }
  }
}
