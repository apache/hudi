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

import com.codahale.metrics.Timer;

import java.io.Serializable;

public class HoodieDeltaStreamerMetrics implements Serializable {

  private HoodieWriteConfig config;
  private String tableName;

  public String overallTimerName = null;
  public String hiveSyncTimerName = null;
  public String metaSyncTimerName = null;
  private transient Timer overallTimer = null;
  public transient Timer hiveSyncTimer = null;
  public transient Timer metaSyncTimer = null;

  public HoodieDeltaStreamerMetrics(HoodieWriteConfig config) {
    this.config = config;
    this.tableName = config.getTableName();
    if (config.isMetricsOn()) {
      Metrics.init(config);
      this.overallTimerName = getMetricsName("timer", "deltastreamer");
      this.hiveSyncTimerName = getMetricsName("timer", "deltastreamerHiveSync");
      this.metaSyncTimerName = getMetricsName("timer", "deltastreamerMetaSync");
    }
  }

  public Timer.Context getOverallTimerContext() {
    if (config.isMetricsOn() && overallTimer == null) {
      overallTimer = createTimer(overallTimerName);
    }
    return overallTimer == null ? null : overallTimer.time();
  }

  public Timer.Context getHiveSyncTimerContext() {
    if (config.isMetricsOn() && hiveSyncTimer == null) {
      hiveSyncTimer = createTimer(hiveSyncTimerName);
    }
    return hiveSyncTimer == null ? null : hiveSyncTimer.time();
  }

  public Timer.Context getMetaSyncTimerContext() {
    if (config.isMetricsOn() && metaSyncTimer == null) {
      metaSyncTimer = createTimer(metaSyncTimerName);
    }
    return metaSyncTimer == null ? null : metaSyncTimer.time();
  }

  private Timer createTimer(String name) {
    return config.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
  }

  String getMetricsName(String action, String metric) {
    return config == null ? null : String.format("%s.%s.%s", config.getMetricReporterMetricsNamePrefix(), action, metric);
  }

  public void updateDeltaStreamerMetrics(long durationInNs) {
    if (config.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", "duration"), getDurationInMs(durationInNs));
    }
  }

  public void updateDeltaStreamerMetaSyncMetrics(String syncClassShortName, long syncNs) {
    if (config.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", syncClassShortName), getDurationInMs(syncNs));
    }
  }

  public void updateDeltaStreamerKafkaDelayCountMetrics(long kafkaDelayCount) {
    if (config.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", "kafkaDelayCount"), kafkaDelayCount);
    }
  }

  public void updateDeltaStreamerSyncMetrics(long syncEpochTimeInMs) {
    if (config.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", "lastSync"), syncEpochTimeInMs);
    }
  }

  public void updateDeltaStreamerKafkaAvroMessagesToProcess(long totalNewMsgs) {
    if (config.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", "kafka_avro_messages_in"), totalNewMsgs);
    }
  }

  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }
}
