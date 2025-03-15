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

package org.apache.hudi.aws.metrics.cloudwatch;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Hudi Amazon CloudWatch metrics reporter. Responsible for reading Hoodie metrics configurations and hooking up with
 * {@link org.apache.hudi.metrics.Metrics}. Internally delegates reporting tasks to {@link CloudWatchReporter}.
 */
public class CloudWatchMetricsReporter extends MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMetricsReporter.class);

  private final MetricRegistry registry;
  private final HoodieMetricsConfig metricsConfig;
  private final CloudWatchReporter reporter;

  public CloudWatchMetricsReporter(HoodieWriteConfig writeConfig, MetricRegistry registry) {
    this(writeConfig.getMetricsConfig(), registry);
  }

  CloudWatchMetricsReporter(HoodieWriteConfig writeConfig, MetricRegistry registry, CloudWatchReporter reporter) {
    this(writeConfig.getMetricsConfig(), registry, reporter);
  }

  public CloudWatchMetricsReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    this.metricsConfig = metricsConfig;
    this.registry = registry;
    this.reporter = createCloudWatchReporter();
  }

  CloudWatchMetricsReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry, CloudWatchReporter reporter) {
    this.metricsConfig = metricsConfig;
    this.registry = registry;
    this.reporter = reporter;
  }

  private CloudWatchReporter createCloudWatchReporter() {
    return CloudWatchReporter.forRegistry(registry)
        .prefixedWith(metricsConfig.getCloudWatchMetricPrefix())
        .namespace(metricsConfig.getCloudWatchMetricNamespace())
        .maxDatumsPerRequest(metricsConfig.getCloudWatchMaxDatumsPerRequest())
        .build(metricsConfig.getProps());
  }

  @Override
  public void start() {
    LOG.info("Starting CloudWatch Metrics Reporter.");
    reporter.start(metricsConfig.getCloudWatchReportPeriodSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    reporter.report();
  }

  @Override
  public void stop() {
    LOG.info("Stopping CloudWatch Metrics Reporter.");
    reporter.stop();
  }
}
