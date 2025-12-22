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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricUtils;
import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PushGatewayMetricsReporter extends MetricsReporter {

  private final PushGatewayReporter pushGatewayReporter;
  private final int periodSeconds;
  private final boolean deleteShutdown;
  private final String configuredJobName;
  private final Map<String, String> configuredLabels;
  private final boolean randomSuffix;

  public PushGatewayMetricsReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {

    String serverHost = metricsConfig.getPushGatewayHost();
    int serverPort = metricsConfig.getPushGatewayPort();
    periodSeconds = metricsConfig.getPushGatewayReportPeriodSeconds();
    deleteShutdown = metricsConfig.getPushGatewayDeleteOnShutdown();
    configuredJobName = metricsConfig.getPushGatewayJobName();
    configuredLabels = Collections.unmodifiableMap(parseLabels(metricsConfig.getPushGatewayLabels()));
    randomSuffix = metricsConfig.getPushGatewayRandomJobNameSuffix();

    pushGatewayReporter = new PushGatewayReporter(
        registry,
        MetricFilter.ALL,
        TimeUnit.SECONDS,
        TimeUnit.SECONDS,
        getJobName(),
        configuredLabels,
        serverHost,
        serverPort,
        deleteShutdown);
  }

  @Override
  public void start() {
    pushGatewayReporter.start(periodSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    pushGatewayReporter.report(null, null, null, null, null);
  }

  @Override
  public void stop() {
    pushGatewayReporter.stop();
  }

  public Map<String, String> getLabels() {
    return configuredLabels;
  }

  private String getJobName() {
    if (randomSuffix) {
      Random random = new Random();
      return configuredJobName + random.nextLong();
    }
    return configuredJobName;
  }

  private static Map<String, String> parseLabels(String labels) {
    if (StringUtils.isNullOrEmpty(labels)) {
      return Collections.emptyMap();
    }

    return MetricUtils.getLabelsAsMap(labels);
  }
}
