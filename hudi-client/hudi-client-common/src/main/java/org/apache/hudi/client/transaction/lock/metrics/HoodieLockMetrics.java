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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.storage.HoodieStorage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HoodieLockMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLockMetrics.class);

  public static final String LOCK_ACQUIRE_ATTEMPTS_COUNTER_NAME = "lock.acquire.attempts";
  public static final String LOCK_ACQUIRE_SUCCESS_COUNTER_NAME = "lock.acquire.success";
  public static final String LOCK_ACQUIRE_FAILURES_COUNTER_NAME = "lock.acquire.failure";
  public static final String LOCK_RELEASE_SUCCESS_COUNTER_NAME = "lock.release.success";
  public static final String LOCK_ACQUIRE_DURATION_TIMER_NAME = "lock.acquire.duration";
  public static final String LOCK_REQUEST_LATENCY_TIMER_NAME = "lock.request.latency";
  private final HoodieWriteConfig writeConfig;
  private final boolean isMetricsEnabled;
  private final int keepLastNtimes = 100;
  private final transient HoodieTimer lockDurationTimer = HoodieTimer.create();
  private final transient HoodieTimer lockApiRequestDurationTimer = HoodieTimer.create();
  private transient Counter lockAttempts;
  private transient Counter successfulLockAttempts;
  private transient Counter failedLockAttempts;
  private transient Counter lockReleaseSuccess;
  private transient Timer lockDuration;
  private transient Timer lockApiRequestDuration;
  private static final Object REGISTRY_LOCK = new Object();
  private Metrics metrics;

  public HoodieLockMetrics(HoodieWriteConfig writeConfig, HoodieStorage storage) {
    this.isMetricsEnabled = writeConfig.isLockingMetricsEnabled();
    this.writeConfig = writeConfig;

    if (isMetricsEnabled) {
      metrics = Metrics.getInstance(writeConfig.getMetricsConfig(), storage);
      MetricRegistry registry = metrics.getRegistry();

      lockAttempts = registry.counter(getMetricsName(LOCK_ACQUIRE_ATTEMPTS_COUNTER_NAME));
      successfulLockAttempts = registry.counter(getMetricsName(LOCK_ACQUIRE_SUCCESS_COUNTER_NAME));
      failedLockAttempts = registry.counter(getMetricsName(LOCK_ACQUIRE_FAILURES_COUNTER_NAME));
      lockReleaseSuccess = registry.counter(getMetricsName(LOCK_RELEASE_SUCCESS_COUNTER_NAME));
      lockDuration = createTimerForMetrics(registry, LOCK_ACQUIRE_DURATION_TIMER_NAME);
      lockApiRequestDuration = createTimerForMetrics(registry, LOCK_REQUEST_LATENCY_TIMER_NAME);
    }
  }

  private String getMetricsName(String metric) {
    return writeConfig == null ? null : String.format("%s.%s", writeConfig.getMetricReporterMetricsNamePrefix(), metric);
  }

  private Timer createTimerForMetrics(MetricRegistry registry, String metric) {
    String metricName = getMetricsName(metric);
    if (registry.getMetrics().get(metricName) == null) {
      synchronized (REGISTRY_LOCK) {
        if (registry.getMetrics().get(metricName) == null) {
          lockDuration = new Timer(new SlidingWindowReservoir(keepLastNtimes));
          registry.register(metricName, lockDuration);
          return lockDuration;
        }
      }
    }
    return (Timer) registry.getMetrics().get(metricName);
  }

  public void startLockApiTimerContext() {
    if (isMetricsEnabled) {
      lockApiRequestDurationTimer.startTimer();
    }
  }

  private static void updateMetric(HoodieTimer timer, Timer metric, String lockName) {
    Option<Long> durationMs = timer.tryEndTimer();
    if (durationMs.isPresent()) {
      metric.update(durationMs.get(), TimeUnit.MILLISECONDS);
    } else {
      LOG.info("Unable to get lock {} duration", lockName);
    }
  }

  public void updateLockAcquiredMetric() {
    if (isMetricsEnabled) {
      updateMetric(lockApiRequestDurationTimer, lockApiRequestDuration, "acquired");
      lockAttempts.inc();
      successfulLockAttempts.inc();
      lockDurationTimer.startTimer();
    }
  }

  public void updateLockNotAcquiredMetric() {
    if (isMetricsEnabled) {
      updateMetric(lockApiRequestDurationTimer, lockApiRequestDuration, "acquired");
      failedLockAttempts.inc();
    }
  }

  public void updateLockHeldTimerMetrics() {
    if (isMetricsEnabled && lockDurationTimer != null) {
      updateMetric(lockDurationTimer, lockDuration, "held");
    }
  }

  public void updateLockReleaseSuccessMetric() {
    if (isMetricsEnabled) {
      lockReleaseSuccess.inc();
    }
  }
}
