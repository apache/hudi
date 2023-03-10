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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;

import com.codahale.metrics.Timer;

public class HoodieDeltaStreamerMetrics extends HoodieIngestionMetrics {

  private Metrics metrics;

  private String overallTimerName;
  private String hiveSyncTimerName;
  private String metaSyncTimerName;
  private transient Timer overallTimer;
  private transient Timer hiveSyncTimer;
  private transient Timer metaSyncTimer;

  public HoodieDeltaStreamerMetrics(HoodieWriteConfig writeConfig) {
    super(writeConfig);
    if (writeConfig.isMetricsOn()) {
      metrics = Metrics.getInstance(writeConfig);
      this.overallTimerName = getMetricsName("timer", "deltastreamer");
      this.hiveSyncTimerName = getMetricsName("timer", "deltastreamerHiveSync");
      this.metaSyncTimerName = getMetricsName("timer", "deltastreamerMetaSync");
    }
  }

  public Timer.Context getOverallTimerContext() {
    if (writeConfig.isMetricsOn() && overallTimer == null) {
      overallTimer = createTimer(overallTimerName);
    }
    return overallTimer == null ? null : overallTimer.time();
  }

  public Timer.Context getHiveSyncTimerContext() {
    if (writeConfig.isMetricsOn() && hiveSyncTimer == null) {
      hiveSyncTimer = createTimer(hiveSyncTimerName);
    }
    return hiveSyncTimer == null ? null : hiveSyncTimer.time();
  }

  public Timer.Context getMetaSyncTimerContext() {
    if (writeConfig.isMetricsOn() && metaSyncTimer == null) {
      metaSyncTimer = createTimer(metaSyncTimerName);
    }
    return metaSyncTimer == null ? null : metaSyncTimer.time();
  }

  private Timer createTimer(String name) {
    return writeConfig.isMetricsOn() ? metrics.getRegistry().timer(name) : null;
  }

  private String getMetricsName(String action, String metric) {
    return String.format("%s.%s.%s", writeConfig.getMetricReporterMetricsNamePrefix(), action, metric);
  }

  public void updateDeltaStreamerMetrics(long durationInNs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "duration"), getDurationInMs(durationInNs));
    }
  }

  public void updateDeltaStreamerMetaSyncMetrics(String syncClassShortName, long syncNs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", syncClassShortName), getDurationInMs(syncNs));
    }
  }

  public void updateDeltaStreamerSyncMetrics(long syncEpochTimeInMs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "lastSync"), syncEpochTimeInMs);
    }
  }

  public void updateNumSuccessfulSyncs(long numSuccessfulSyncs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "numSuccessfulSyncs"), numSuccessfulSyncs);
    }
  }

  public void updateNumFailedSyncs(long numFailedSyncs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "numFailedSyncs"), numFailedSyncs);
    }
  }

  public void updateNumConsecutiveFailures(int numConsecutiveFailures) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "numConsecutiveFailures"), numConsecutiveFailures);
    }
  }

  public void updateTotalSourceBytesAvailableForIngest(long totalSourceBytesAvailable) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "totalSourceBytesAvailable"), totalSourceBytesAvailable);
    }
  }

  public void updateTotalSyncDurationMs(long totalSyncDurationMs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "totalSyncDurationMs"), totalSyncDurationMs);
    }
  }

  public void updateActualSyncDurationMs(long actualSyncDurationMs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "actualSyncDurationMs"), actualSyncDurationMs);
    }
  }

  /**
   * Update heartbeat from deltastreamer ingestion job when active for a table.
   *
   * @param heartbeatTimestampMs the timestamp in milliseconds at which heartbeat is emitted.
   */
  public void updateDeltaStreamerHeartbeatTimestamp(long heartbeatTimestampMs) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", "heartbeatTimestampMs"), heartbeatTimestampMs);
    }
  }

  public void updateDeltaStreamerSourceDelayCount(String sourceMetricName, long sourceDelayCount) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", sourceMetricName), sourceDelayCount);
    }
  }

  public void updateDeltaStreamerSourceNewMessageCount(String sourceMetricName, long sourceNewMessageCount) {
    if (writeConfig.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("deltastreamer", sourceMetricName), sourceNewMessageCount);
    }
  }

  @Override
  public void shutdown() {
    if (metrics != null) {
      metrics.shutdown();
    }
  }

  private static long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }
}
