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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;
import org.apache.hudi.metrics.datadog.DatadogMetricsReporter;
import org.apache.hudi.metrics.m3.M3MetricsReporter;
import org.apache.hudi.metrics.prometheus.PrometheusReporter;
import org.apache.hudi.metrics.prometheus.PushGatewayMetricsReporter;

import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * Factory class for creating MetricsReporter.
 */
@Slf4j
public class MetricsReporterFactory {

  @VisibleForTesting
  static final String CLOUDWATCH_REPORTER_CLASS =
      "org.apache.hudi.aws.metrics.cloudwatch.CloudWatchMetricsReporter";

  public static Option<MetricsReporter> createReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    String reporterClassName = metricsConfig.getMetricReporterClassName();

    if (!StringUtils.isNullOrEmpty(reporterClassName)) {
      Object instance = ReflectionUtils.loadClass(
          reporterClassName, new Class<?>[] {Properties.class, MetricRegistry.class}, metricsConfig.getProps(), registry);
      if (!(instance instanceof CustomizableMetricsReporter)) {
        throw new HoodieException(metricsConfig.getMetricReporterClassName()
            + " is not a subclass of CustomizableMetricsReporter");
      }
      return Option.of((MetricsReporter) instance);
    }

    MetricsReporterType type = metricsConfig.getMetricsReporterType();
    MetricsReporter reporter = null;
    if (type == null) {
      log.warn("Metric creation failed. {} is not configured",
          HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key());
      return Option.empty();
    }

    switch (type) {
      case GRAPHITE:
        reporter = new MetricsGraphiteReporter(metricsConfig, registry);
        break;
      case INMEMORY:
        reporter = new InMemoryMetricsReporter();
        break;
      case JMX:
        reporter = new JmxMetricsReporter(metricsConfig, registry);
        break;
      case DATADOG:
        reporter = new DatadogMetricsReporter(metricsConfig, registry);
        break;
      case PROMETHEUS_PUSHGATEWAY:
        reporter = new PushGatewayMetricsReporter(metricsConfig, registry);
        break;
      case PROMETHEUS:
        reporter = new PrometheusReporter(metricsConfig, registry);
        break;
      case CONSOLE:
        reporter = new ConsoleMetricsReporter(registry);
        break;
      case CLOUDWATCH:
        reporter = createCloudWatchReporter(metricsConfig, registry);
        break;
      case M3:
        reporter = new M3MetricsReporter(metricsConfig, registry);
        break;
      case SLF4J:
        reporter = new Slf4jMetricsReporter(registry);
        break;
      default:
        log.error("Reporter type[{}] is not supported.", type);
        break;
    }
    return Option.ofNullable(reporter);
  }

  /**
   * The CloudWatch reporter ships in the optional {@code hudi-aws} module and so is loaded reflectively.
   * Reflection collapses several unrelated failures into the same opaque {@link HoodieException}. Three of
   * them have distinct remedies - the module is absent, it was built against a Hudi that has since moved a
   * class, or the classpath carries a stale duplicate - so translate those three, and leave every other
   * failure untouched.
   */
  private static MetricsReporter createCloudWatchReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    return createCloudWatchReporter(CLOUDWATCH_REPORTER_CLASS, metricsConfig, registry);
  }

  @VisibleForTesting
  static MetricsReporter createCloudWatchReporter(String reporterClass, HoodieMetricsConfig metricsConfig,
                                                 MetricRegistry registry) {
    try {
      return (MetricsReporter) ReflectionUtils.loadClass(reporterClass,
          new Class[] {HoodieMetricsConfig.class, MetricRegistry.class}, metricsConfig, registry);
    } catch (NoClassDefFoundError e) {
      // Class#getConstructor resolves the parameter types of every public constructor, not just the one
      // asked for, so a jar built against an older Hudi fails here on a type that has since moved - not
      // with a missing-constructor error. NoClassDefFoundError is an Error, so ReflectionUtils never wraps
      // it and it arrives here uncaught. Its message names the type that vanished, which is the strongest
      // evidence of skew available.
      throw new HoodieException(String.format(
          "Cannot report metrics to CloudWatch: %s was found on the classpath but was built against a "
              + "different Hudi version - resolving its constructors needs %s, which this Hudi no longer "
              + "provides. Use a hudi-aws-bundle of the same version as the engine bundle, or set %s to a "
              + "different reporter type.",
          reporterClass, e.getMessage(), HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()), e);
    } catch (HoodieException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new HoodieException(String.format(
            "Cannot report metrics to CloudWatch: %s was not found on the classpath. It ships in the "
                + "optional hudi-aws module, which not every engine bundle includes. Add the "
                + "hudi-aws-bundle jar matching your Hudi version to the classpath, or set %s to a "
                + "different reporter type.",
            reporterClass, HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()), e);
      }
      if (e.getCause() instanceof NoSuchMethodException) {
        // The class resolved and so did every constructor's parameter types, yet none matched. A jar built
        // against an older Hudi fails earlier, in the NoClassDefFoundError branch above, so what reaches
        // here is a classpath carrying a stale or duplicate copy of this class or of its parameter types.
        // Fully qualified names, because the package move and the shaded-codahale relocation are exactly
        // what distinguishes the declared constructor from the requested one - under simple names both read
        // as (HoodieMetricsConfig, MetricRegistry) and the error looks wrong.
        throw new HoodieException(String.format(
            "Cannot report metrics to CloudWatch: %s was found on the classpath but does not declare a "
                + "(%s, %s) constructor. Some jar on the classpath is supplying a stale or duplicate copy "
                + "of this class or of its parameter types. Check for more than one Hudi version on the "
                + "classpath - a leftover hudi-common or hudi-client-common is the usual culprit - and "
                + "align every Hudi artifact, including the engine bundle, to one version. Or set %s to a "
                + "different reporter type.",
            reporterClass, HoodieMetricsConfig.class.getName(),
            MetricRegistry.class.getName(), HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()), e);
      }
      throw e;
    }
  }
}
