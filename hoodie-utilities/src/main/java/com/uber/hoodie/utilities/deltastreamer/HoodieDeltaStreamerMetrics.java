package com.uber.hoodie.utilities.deltastreamer;

import static com.uber.hoodie.metrics.Metrics.registerGauge;

import com.codahale.metrics.Timer;
import com.uber.hoodie.config.HoodieClientConfig;
import com.uber.hoodie.metrics.Metrics;

public class HoodieDeltaStreamerMetrics {

  private HoodieClientConfig config = null;
  private String tableName = null;

  public String overallTimerName = null;
  public String hiveSyncTimerName = null;
  private Timer overallTimer = null;
  public Timer hiveSyncTimer = null;

  public HoodieDeltaStreamerMetrics(HoodieClientConfig config) {
    this.config = config;
    this.tableName = config.getTableName();
    if (config.isMetricsOn()) {
      Metrics.init(config);
      this.overallTimerName = getMetricsName("timer", "deltastreamer");
      this.hiveSyncTimerName = getMetricsName("timer", "deltastreamerHiveSync");
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

  private Timer createTimer(String name) {
    return config.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
  }

  String getMetricsName(String action, String metric) {
    return config == null ? null : String.format("%s.%s.%s", tableName, action, metric);
  }

  public void updateDeltaStreamerMetrics(long durationInNs, long hiveSyncNs) {
    if (config.isMetricsOn()) {
      registerGauge(getMetricsName("deltastreamer", "duration"), getDurationInMs(durationInNs));
      registerGauge(getMetricsName("deltastreamer", "hiveSyncDuration"), getDurationInMs(hiveSyncNs));
    }
  }

  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }
}
