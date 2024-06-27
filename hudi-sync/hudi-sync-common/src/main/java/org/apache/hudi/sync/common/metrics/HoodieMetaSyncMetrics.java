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

package org.apache.hudi.sync.common.metrics;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieMetaSyncMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetaSyncMetrics.class);
  // Metrics are shut down by the shutdown hook added in the Metrics class
  private Metrics metrics;
  private HoodieMetricsConfig metricsConfig;

  private final String syncToolName;

  private static String recreateAndSyncFailureCounterName;
  private static String recreateAndSyncTimerName;

  private Timer recreateAndSyncTimer;
  private Counter recreateAndSyncFailureCounter;

  public HoodieMetaSyncMetrics(HoodieSyncConfig config, String syncToolName) {
    this.metricsConfig = config.getMetricsConfig();
    this.syncToolName = syncToolName;
    if (metricsConfig.isMetricsOn()) {
      metrics = Metrics.getInstance(metricsConfig);
      recreateAndSyncTimerName = getMetricsName("timer", "meta_sync.recreate_table");
      recreateAndSyncFailureCounterName = getMetricsName("counter", "meta_sync.recreate_table.failure");
    }
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public Timer.Context getRecreateAndSyncTimer() {
    if (metricsConfig.isMetricsOn() && recreateAndSyncTimer == null) {
      recreateAndSyncTimer = createTimer(recreateAndSyncTimerName);
    }
    return recreateAndSyncTimer == null ? null : recreateAndSyncTimer.time();
  }

  private Timer createTimer(String name) {
    return metricsConfig.isMetricsOn() ? metrics.getRegistry().timer(name) : null;
  }

  public void incrementRecreateAndSyncFailureCounter() {
    recreateAndSyncFailureCounter = getCounter(recreateAndSyncFailureCounter, recreateAndSyncFailureCounterName);
    recreateAndSyncFailureCounter.inc();
  }

  public void updateRecreateAndSyncDurationInMs(long durationInNs) {
    if (metricsConfig.isMetricsOn()) {
      long durationInMs = getDurationInMs(durationInNs);
      LOG.info("Sending recreate and sync metrics {}", durationInMs);
      metrics.registerGauge(getMetricsName("meta_sync", "recreate_table_duration_ms"), durationInMs);
    }
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  private long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }

  @VisibleForTesting
  public String getMetricsName(String action, String metric) {
    if (StringUtils.isNullOrEmpty(metricsConfig.getMetricReporterMetricsNamePrefix())) {
      return String.format("%s.%s.%s", action, metric, syncToolName);
    } else {
      return String.format("%s.%s.%s.%s", metricsConfig.getMetricReporterMetricsNamePrefix(), action, metric, syncToolName);
    }
  }

  public Counter getCounter(Counter counter, String name) {
    if (counter == null) {
      return metrics.getRegistry().counter(name);
    }
    return counter;
  }
}
