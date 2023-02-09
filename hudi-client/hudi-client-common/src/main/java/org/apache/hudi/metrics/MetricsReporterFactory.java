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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.cloudwatch.CloudWatchMetricsReporter;
import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;
import org.apache.hudi.metrics.datadog.DatadogMetricsReporter;
import org.apache.hudi.metrics.prometheus.PrometheusReporter;
import org.apache.hudi.metrics.prometheus.PushGatewayMetricsReporter;

import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Factory class for creating MetricsReporter.
 */
public class MetricsReporterFactory {

  private static final Logger LOG = LogManager.getLogger(MetricsReporterFactory.class);

  public static MetricsReporter createReporter(HoodieWriteConfig config, MetricRegistry registry) {
    String reporterClassName = config.getMetricReporterClassName();

    if (!StringUtils.isNullOrEmpty(reporterClassName)) {
      Object instance = ReflectionUtils.loadClass(
          reporterClassName, new Class<?>[] {Properties.class, MetricRegistry.class}, config.getProps(), registry);
      if (!(instance instanceof CustomizableMetricsReporter)) {
        throw new HoodieException(config.getMetricReporterClassName()
            + " is not a subclass of CustomizableMetricsReporter");
      }
      return (MetricsReporter) instance;
    }

    MetricsReporterType type = config.getMetricsReporterType();
    MetricsReporter reporter = null;
    switch (type) {
      case GRAPHITE:
        reporter = new MetricsGraphiteReporter(config, registry);
        break;
      case INMEMORY:
        reporter = new InMemoryMetricsReporter();
        break;
      case JMX:
        reporter = new JmxMetricsReporter(config, registry);
        break;
      case DATADOG:
        reporter = new DatadogMetricsReporter(config, registry);
        break;
      case PROMETHEUS_PUSHGATEWAY:
        reporter = new PushGatewayMetricsReporter(config, registry);
        break;
      case PROMETHEUS:
        reporter = new PrometheusReporter(config, registry);
        break;
      case CONSOLE:
        reporter = new ConsoleMetricsReporter(registry);
        break;
      case CLOUDWATCH:
        reporter = new CloudWatchMetricsReporter(config, registry);
        break;
      default:
        LOG.error("Reporter type[" + type + "] is not supported.");
        break;
    }
    return reporter;
  }
}
