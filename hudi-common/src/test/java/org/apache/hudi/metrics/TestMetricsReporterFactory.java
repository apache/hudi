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

import java.lang.reflect.InvocationTargetException;
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
              eq(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS),
              any(Class[].class),
              eq(metricsConfig),
              eq(registry)
          )
      ).thenReturn(reporterMock);

      MetricsReporter actualReporter = MetricsReporterFactory.createReporter(metricsConfig, registry).get();
      assertSame(reporterMock, actualReporter);
    }
  }

  /**
   * {@code hudi-aws} is deliberately absent from this module's test classpath, which is exactly the
   * situation a user hits on an engine bundle that does not shade that module: the reflectively loaded
   * CloudWatch reporter cannot be found. The failure must name the missing class and how to fix it,
   * not just report that some class could not be loaded.
   */
  @Test
  void metricsReporterFactoryShouldExplainHowToEnableCloudWatchWhenHudiAwsIsMissing() {
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);

    HoodieException exception = assertThrows(HoodieException.class,
        () -> MetricsReporterFactory.createReporter(metricsConfig, registry));

    String message = exception.getMessage();
    assertTrue(message.contains(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS),
        () -> "The failure should name the missing reporter class, but was: " + message);
    assertTrue(message.contains("hudi-aws-bundle"),
        () -> "The failure should name the bundle that provides the reporter, but was: " + message);
    assertTrue(message.contains(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()),
        () -> "The failure should name the config to change, but was: " + message);
    // Every string above also appears in the constructor-mismatch message, so without this the two branches
    // could be merged or reordered and both tests would still pass.
    assertTrue(message.contains("was not found on the classpath"),
        () -> "A missing class must not be reported as a constructor mismatch, but was: " + message);
  }

  /**
   * The classpath-based test above exercises the real failure but passes for any resolution failure.
   * This pins the mapping itself: a ClassNotFoundException cause is what triggers the rewrite.
   */
  @Test
  void metricsReporterFactoryRewritesClassNotFoundIntoAnActionableMessage() {
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);
    try (MockedStatic<ReflectionUtils> mockedStatic = Mockito.mockStatic(ReflectionUtils.class)) {
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS), any(Class[].class), eq(metricsConfig), eq(registry)))
          .thenThrow(new HoodieException("Unable to load class " + MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS,
              new ClassNotFoundException(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS)));

      HoodieException exception = assertThrows(HoodieException.class,
          () -> MetricsReporterFactory.createReporter(metricsConfig, registry));
      assertTrue(exception.getMessage().contains("hudi-aws-bundle"),
          () -> "Expected the remedy to be named, but was: " + exception.getMessage());
      assertTrue(exception.getMessage().contains("was not found on the classpath"),
          () -> "Expected this branch's own phrase, not one shared with the mismatch branch, but was: "
              + exception.getMessage());
    }
  }

  /**
   * A jar built against an older Hudi does not reach this branch - resolving its constructors fails first
   * with {@link NoClassDefFoundError}, covered below. What reaches here is a classpath carrying a stale or
   * duplicate copy, so that is the remedy the message has to give.
   */
  @Test
  void metricsReporterFactoryExplainsAConstructorMismatch() {
    HoodieException exception = captureCloudWatchFailure(
        new HoodieException("Unable to instantiate class " + MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS,
            new NoSuchMethodException("<init>")));

    String message = exception.getMessage();
    assertTrue(message.contains("constructor"),
        () -> "The failure should say the constructor did not match, but was: " + message);
    assertTrue(message.contains("stale or duplicate copy"),
        () -> "The failure should name a stale duplicate as the cause, but was: " + message);
    assertTrue(message.contains(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS),
        () -> "The failure should name the reporter class, but was: " + message);
    assertTrue(message.contains(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()),
        () -> "The failure should name the config to change, but was: " + message);
    assertTrue(message.contains(HoodieMetricsConfig.class.getName())
            && message.contains(MetricRegistry.class.getName()),
        () -> "Fully qualified parameter types are what distinguish the requested constructor from the "
            + "declared one, but was: " + message);
    // Positive rather than an assertFalse on the other branch's prose: a vacuous assertFalse never fails.
    assertTrue(message.contains("was found on the classpath but"),
        () -> "A class that resolved must not be reported as missing, but was: " + message);
    assertEquals(NoSuchMethodException.class, exception.getCause().getCause().getClass(),
        "The original NoSuchMethodException must stay in the chain - it is the evidence #12902 needed");
  }

  /**
   * The clean version-skew case, and the one the mismatch message used to claim. {@code getConstructor}
   * resolves the parameter types of every public constructor, so a jar built against an older Hudi dies on a
   * type that has since moved. That is an {@link Error}, so {@code ReflectionUtils} never wraps it and it
   * reaches the factory uncaught.
   */
  @Test
  void metricsReporterFactoryExplainsAVanishedParameterType() {
    HoodieException exception = captureCloudWatchFailure(
        new NoClassDefFoundError("org/apache/hudi/config/metrics/HoodieMetricsConfig"));

    String message = exception.getMessage();
    assertTrue(message.contains("built against a different Hudi version"),
        () -> "The failure should name version skew, but was: " + message);
    assertTrue(message.contains("org/apache/hudi/config/metrics/HoodieMetricsConfig"),
        () -> "The failure should name the type that vanished, but was: " + message);
    assertEquals(NoClassDefFoundError.class, exception.getCause().getClass(),
        "The Error must stay in the chain - its message is the evidence of skew");
  }

  /**
   * The gap every mocked test above shares: none of them proves that real reflection produces the shapes the
   * production code branches on. This drives the translation through the real {@code ReflectionUtils} with a
   * fixture whose only public constructor does not match, and asserts on the actual throwable.
   */
  @Test
  void metricsReporterFactoryTranslatesARealReflectionFailure() {
    HoodieException exception = assertThrows(HoodieException.class,
        () -> MetricsReporterFactory.createCloudWatchReporter(
            MismatchedReporter.class.getName(), metricsConfig, registry));

    String message = exception.getMessage();
    assertTrue(message.contains("stale or duplicate copy"),
        () -> "A real non-matching constructor should reach the mismatch branch, but was: " + message);
    assertEquals(NoSuchMethodException.class, exception.getCause().getCause().getClass(),
        "and the real NoSuchMethodException should be chained");
  }

  /** Public, with a single constructor that deliberately does not match (HoodieMetricsConfig, MetricRegistry). */
  public static class MismatchedReporter {
    public MismatchedReporter(String somethingElse) {
      // never called; exists so getConstructor has a public constructor to reject
    }
  }

  /**
   * The other direction, and the branch most likely to regress: a failure that is neither a missing class
   * nor a missing constructor must pass through untouched, so an error raised by the reporter's own
   * constructor is never rewritten into a classpath diagnosis.
   */
  @Test
  void metricsReporterFactoryLeavesOtherFailuresUntouched() {
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);
    try (MockedStatic<ReflectionUtils> mockedStatic = Mockito.mockStatic(ReflectionUtils.class)) {
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS), any(Class[].class), eq(metricsConfig), eq(registry)))
          .thenThrow(new HoodieException("Unable to instantiate class " + MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS,
              new InvocationTargetException(new IllegalStateException("no AWS region configured"))));

      HoodieException exception = assertThrows(HoodieException.class,
          () -> MetricsReporterFactory.createReporter(metricsConfig, registry));
      assertEquals("Unable to instantiate class " + MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS,
          exception.getMessage(),
          "A failure that is neither a missing class nor a missing constructor must not be rewritten");
    }
  }

  @Test
  void metricsReporterFactoryShouldReturnUserDefinedReporter() {
    when(metricsConfig.getMetricReporterClassName()).thenReturn(DummyMetricsReporter.class.getName());

    TypedProperties props = new TypedProperties();
    props.setProperty("testKey", "testValue");

    when(metricsConfig.getProps()).thenReturn(props);
    MetricsReporter reporter = MetricsReporterFactory.createReporter(metricsConfig, registry).get();
    assertTrue(reporter instanceof CustomizableMetricsReporter);
    assertEquals(props, ((DummyMetricsReporter) reporter).getProps());
    assertEquals(registry, ((DummyMetricsReporter) reporter).getRegistry());
  }

  @Test
  void metricsReporterFactoryShouldThrowExceptionWhenMetricsReporterClassIsIllegal() {
    when(metricsConfig.getMetricReporterClassName()).thenReturn(IllegalTestMetricsReporter.class.getName());
    when(metricsConfig.getProps()).thenReturn(new TypedProperties());
    assertThrows(HoodieException.class, () -> MetricsReporterFactory.createReporter(metricsConfig, registry));
  }

  private HoodieException captureCloudWatchFailure(Throwable reflectionFailure) {
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);
    try (MockedStatic<ReflectionUtils> mockedStatic = Mockito.mockStatic(ReflectionUtils.class)) {
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(MetricsReporterFactory.CLOUDWATCH_REPORTER_CLASS), any(Class[].class), eq(metricsConfig), eq(registry)))
          .thenThrow(reflectionFailure);
      return assertThrows(HoodieException.class,
          () -> MetricsReporterFactory.createReporter(metricsConfig, registry));
    }
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
}
