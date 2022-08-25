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

package org.apache.hudi.client.transaction.lock.metrics;

import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.Metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

public class HoodieLockMetrics {

  private final HoodieWriteConfig writeConfig;
  private final boolean isMetricsEnabled;
  private final int keepLastNtimes = 100;
  private final transient HoodieTimer lockDurationTimer = HoodieTimer.create();
  private final transient HoodieTimer lockApiRequestDurationTimer = HoodieTimer.create();
  private transient Counter lockAttempts;
  private transient Counter succesfulLockAttempts;
  private transient Counter failedLockAttempts;
  private transient Timer lockDuration;
  private transient Timer lockApiRequestDuration;

  public HoodieLockMetrics(HoodieWriteConfig writeConfig) {
    this.isMetricsEnabled = writeConfig.isMetricsOn();
    this.writeConfig = writeConfig;

    if (writeConfig.isMetricsOn()) {
      Metrics.init(writeConfig);
      MetricRegistry registry = Metrics.getInstance().getRegistry();

      lockAttempts = registry.counter(getMetricsName("acquire.attempts.count"));
      succesfulLockAttempts = registry.counter(getMetricsName("acquire.success.count"));
      failedLockAttempts = registry.counter(getMetricsName("acquire.failure.count"));

      lockDuration = createTimerForMetrics(registry, "acquire.duration");
      lockApiRequestDuration = createTimerForMetrics(registry, "request.latency");
    }
  }

  private String getMetricsName(String metric) {
    return writeConfig == null ? null : String.format("%s.%s.%s", writeConfig.getMetricReporterMetricsNamePrefix(), "lock", metric);
  }

  private Timer createTimerForMetrics(MetricRegistry registry, String metric) {
    String metricName = getMetricsName(metric);
    if (registry.getMetrics().get(metricName) == null) {
      lockDuration = new Timer(new SlidingWindowReservoir(keepLastNtimes));
      registry.register(metricName, lockDuration);
      return lockDuration;
    }
    return (Timer) registry.getMetrics().get(metricName);
  }

  public void startLockApiTimerContext() {
    if (isMetricsEnabled) {
      lockApiRequestDurationTimer.startTimer();
    }
  }

  public void updateLockAcquiredMetric() {
    if (isMetricsEnabled) {
      long duration = lockApiRequestDurationTimer.endTimer();
      lockApiRequestDuration.update(duration, TimeUnit.MILLISECONDS);
      lockAttempts.inc();
    }
  }

  public void startLockHeldTimerContext() {
    if (isMetricsEnabled) {
      succesfulLockAttempts.inc();
      lockDurationTimer.startTimer();
    }
  }

  public void updateLockNotAcquiredMetric() {
    if (isMetricsEnabled) {
      long duration = lockApiRequestDurationTimer.endTimer();
      lockApiRequestDuration.update(duration, TimeUnit.MILLISECONDS);
      failedLockAttempts.inc();
    }
  }

  public void updateLockHeldTimerMetrics() {
    if (isMetricsEnabled && lockDurationTimer != null) {
      long lockDurationInMs = lockDurationTimer.endTimer();
      lockDuration.update(lockDurationInMs, TimeUnit.MILLISECONDS);
    }
  }
}
