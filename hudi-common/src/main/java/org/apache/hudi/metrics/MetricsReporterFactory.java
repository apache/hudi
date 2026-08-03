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
   * Not every engine bundle shades that module, in which case class loading fails without pointing at a
   * remedy. Translate that into an actionable error.
   */
  private static MetricsReporter createCloudWatchReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    try {
      return (MetricsReporter) ReflectionUtils.loadClass(CLOUDWATCH_REPORTER_CLASS,
          new Class[] {HoodieMetricsConfig.class, MetricRegistry.class}, metricsConfig, registry);
    } catch (HoodieException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new HoodieException(String.format(
            "Cannot report metrics to CloudWatch: %s was not found on the classpath. It ships in the "
                + "optional hudi-aws module, which not every engine bundle includes. Add the "
                + "hudi-aws-bundle jar matching your Hudi version to the classpath, or set %s to a "
                + "different reporter type.",
            CLOUDWATCH_REPORTER_CLASS, HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key()), e);
      }
      throw e;
    }
  }
}
