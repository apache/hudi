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

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.config.HoodieMetricsConfig;
import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Factory class for creating MetricsReporter.
 */
public class MetricsReporterFactory {

  private static final Logger LOG = LogManager.getLogger(MetricsReporterFactory.class);

  public static MetricsReporter createReporter(HoodieMetricsConfig config, HoodieMetricRegistry registry) {
    String reporterClassName = config.getMetricReporterClassName();
    if (!StringUtils.isNullOrEmpty(reporterClassName)) {
      Object instance = ReflectionUtils.loadClass(
          reporterClassName, new Class<?>[] {Properties.class, HoodieMetricRegistry.class}, config.getProps(), registry);
      if (!(instance instanceof CustomizableMetricsReporter)) {
        throw new HoodieException(config.getMetricReporterClassName()
            + " is not a subclass of CustomizableMetricsReporter");
      }
      return (MetricsReporter) instance;
    }

    MetricsReporterType type = config.getMetricsReporterType();
    switch (type) {
      case GRAPHITE:
        reporterClassName = "org.apache.hudi.metrics.MetricsGraphiteReporter";
        break;
      case INMEMORY:
        reporterClassName = "org.apache.hudi.metrics.InMemoryMetricsReporter";
        break;
      case JMX:
        reporterClassName = "org.apache.hudi.metrics.JmxMetricsReporter";
        break;
      case DATADOG:
        reporterClassName = "org.apache.hudi.metrics.datadog.DatadogMetricsReporter";
        break;
      case PROMETHEUS_PUSHGATEWAY:
        reporterClassName = "org.apache.hudi.metrics.prometheus.PushGatewayMetricsReporter";
        break;
      case PROMETHEUS:
        reporterClassName = "org.apache.hudi.metrics.prometheus.PrometheusReporter";
        break;
      case CONSOLE:
        reporterClassName = "org.apache.hudi.metrics.ConsoleMetricsReporter";
        break;
      case CLOUDWATCH:
        reporterClassName = "org.apache.hudi.aws.cloudwatch.CloudWatchMetricsReporter";
        break;
      default:
        LOG.error("Reporter type[" + type + "] is not supported.");
        break;
    }

    Object reporter = ReflectionUtils.loadClass(
        reporterClassName, new Class<?>[] {HoodieMetricsConfig.class, HoodieMetricRegistry.class}, config, registry);
    if (!(reporter instanceof MetricsReporter)) {
      throw new HoodieException(reporterClassName
          + " is not a subclass of MetricsReporter or does not provide a constructor"
          + " that accepts HoodieMetricsConfig and HoodieMetricsRegistry");
    }
    return (MetricsReporter) reporter;
  }
}
