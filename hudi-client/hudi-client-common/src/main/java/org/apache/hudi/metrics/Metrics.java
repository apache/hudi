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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the main class of the metrics system.
 */
public class Metrics {

  private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

  private static final Map<String, Metrics> METRICS_INSTANCE_PER_BASEPATH = new HashMap<>();

  private final MetricRegistry registry;
  private final List<MetricsReporter> reporters;
  private final String commonMetricPrefix;
  private boolean initialized = false;

  public Metrics(HoodieWriteConfig metricConfig) {
    registry = new MetricRegistry();
    commonMetricPrefix = metricConfig.getMetricReporterMetricsNamePrefix();
    reporters = new ArrayList<>();
    Option<MetricsReporter> defaultReporter = MetricsReporterFactory.createReporter(metricConfig, registry);
    defaultReporter.ifPresent(reporters::add);
    if (StringUtils.nonEmpty(metricConfig.getMetricReporterFileBasedConfigs())) {
      reporters.addAll(addAdditionalMetricsExporters(metricConfig));
    }
    if (reporters.size() == 0) {
      throw new RuntimeException("Cannot initialize Reporters.");
    }
    reporters.forEach(MetricsReporter::start);

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    this.initialized = true;
  }

  private void registerHoodieCommonMetrics() {
    registerGauges(Registry.getAllMetrics(true, true), Option.of(commonMetricPrefix));
  }

  public static synchronized Metrics getInstance(HoodieWriteConfig metricConfig) {
    String basePath = metricConfig.getBasePath();
    if (METRICS_INSTANCE_PER_BASEPATH.containsKey(basePath)) {
      return METRICS_INSTANCE_PER_BASEPATH.get(basePath);
    }

    Metrics metrics = new Metrics(metricConfig);
    METRICS_INSTANCE_PER_BASEPATH.put(basePath, metrics);
    return metrics;
  }

  public static synchronized void shutdownAllMetrics() {
    METRICS_INSTANCE_PER_BASEPATH.values().forEach(Metrics::shutdown);
  }

  private List<MetricsReporter> addAdditionalMetricsExporters(HoodieWriteConfig metricConfig) {
    List<MetricsReporter> reporterList = new ArrayList<>();
    List<String> propPathList = StringUtils.split(metricConfig.getMetricReporterFileBasedConfigs(), ",");
    try (FileSystem fs = FSUtils.getFs(propPathList.get(0), new Configuration())) {
      for (String propPath : propPathList) {
        HoodieWriteConfig secondarySourceConfig = HoodieWriteConfig.newBuilder().fromInputStream(
            fs.open(new Path(propPath))).withPath(metricConfig.getBasePath()).build();
        Option<MetricsReporter> reporter = MetricsReporterFactory.createReporter(secondarySourceConfig, registry);
        if (reporter.isPresent()) {
          reporterList.add(reporter.get());
        } else {
          LOG.error(String.format("Could not create reporter using properties path %s base path %s",
              propPath, metricConfig.getBasePath()));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to add MetricsExporters", e);
    }
    LOG.info("total additional metrics reporters added =" + reporterList.size());
    return reporterList;
  }

  public synchronized void shutdown() {
    try {
      registerHoodieCommonMetrics();
      reporters.forEach(MetricsReporter::report);
      LOG.info("Stopping the metrics reporter...");
      reporters.forEach(MetricsReporter::stop);
    } catch (Exception e) {
      LOG.warn("Error while closing reporter", e);
    } finally {
      initialized = false;
    }
  }

  public synchronized void flush() {
    try {
      LOG.info("Reporting and flushing all metrics");
      registerHoodieCommonMetrics();
      reporters.forEach(MetricsReporter::report);
      registry.getNames().forEach(this.registry::remove);
      registerHoodieCommonMetrics();
    } catch (Exception e) {
      LOG.error("Error while reporting and flushing metrics", e);
    }
  }

  public void registerGauges(Map<String, Long> metricsMap, Option<String> prefix) {
    String metricPrefix = prefix.isPresent() ? prefix.get() + "." : "";
    metricsMap.forEach((k, v) -> registerGauge(metricPrefix + k, v));
  }

  public void registerGauge(String metricName, final long value) {
    try {
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

  public static boolean isInitialized(String basePath) {
    if (METRICS_INSTANCE_PER_BASEPATH.containsKey(basePath)) {
      return METRICS_INSTANCE_PER_BASEPATH.get(basePath).initialized;
    }
    return false;
  }
}
