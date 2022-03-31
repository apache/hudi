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

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Map;

/**
 * This is the main class of the metrics system.
 */
public class Metrics {

  private static final Logger LOG = LogManager.getLogger(Metrics.class);

  private static volatile boolean initialized = false;
  private static Metrics instance = null;

  private final MetricRegistry registry;
  private MetricsReporter reporter;
  private final String commonMetricPrefix;

  private Metrics(HoodieWriteConfig metricConfig) {
    registry = new MetricRegistry();
    commonMetricPrefix = metricConfig.getMetricReporterMetricsNamePrefix();
    reporter = MetricsReporterFactory.createReporter(metricConfig, registry);
    if (reporter == null) {
      throw new RuntimeException("Cannot initialize Reporter.");
    }
    reporter.start();

    Runtime.getRuntime().addShutdownHook(new Thread(this::reportAndCloseReporter));
  }

  private void reportAndCloseReporter() {
    try {
      LOG.info("Reporting and closing metrics");
      registerHoodieCommonMetrics();
      reporter.report();
      if (getReporter() != null) {
        getReporter().close();
      }
    } catch (Exception e) {
      LOG.warn("Error while closing reporter", e);
    }
  }

  private void reportAndFlushMetrics() {
    try {
      LOG.info("Reporting and flushing all metrics");
      this.registerHoodieCommonMetrics();
      this.reporter.report();
      this.registry.getNames().forEach(this.registry::remove);
    } catch (Exception e) {
      LOG.error("Error while reporting and flushing metrics", e);
    }
  }

  private void registerHoodieCommonMetrics() {
    registerGauges(Registry.getAllMetrics(true, true), Option.of(commonMetricPrefix));
  }

  public static Metrics getInstance() {
    assert initialized;
    return instance;
  }

  public static synchronized void init(HoodieWriteConfig metricConfig) {
    if (initialized) {
      return;
    }
    try {
      instance = new Metrics(metricConfig);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    initialized = true;
  }

  public static synchronized void shutdown() {
    if (!initialized) {
      return;
    }
    instance.reportAndCloseReporter();
    initialized = false;
  }

  public static synchronized void flush() {
    if (!Metrics.initialized) {
      return;
    }
    instance.reportAndFlushMetrics();
  }

  public static void registerGauges(Map<String, Long> metricsMap, Option<String> prefix) {
    String metricPrefix = prefix.isPresent() ? prefix.get() + "." : "";
    metricsMap.forEach((k, v) -> registerGauge(metricPrefix + k, v));
  }

  public static void registerGauge(String metricName, final long value) {
    try {
      MetricRegistry registry = Metrics.getInstance().getRegistry();
      HoodieGauge guage = (HoodieGauge) registry.gauge(metricName, () -> new HoodieGauge<>(value));
      guage.setValue(value);
    } catch (Exception e) {
      // Here we catch all exception, so the major upsert pipeline will not be affected if the
      // metrics system has some issues.
      LOG.error("Failed to send metrics: ", e);
    }
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public Closeable getReporter() {
    return reporter.getReporter();
  }
}
