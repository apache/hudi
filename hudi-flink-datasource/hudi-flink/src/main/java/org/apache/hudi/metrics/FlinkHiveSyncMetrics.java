/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;

/**
 * Metrics for the Hive sync that {@link org.apache.hudi.sink.StreamWriteOperatorCoordinator} runs
 * on the JobManager after each commit.
 *
 * <p>A sync runs on the coordinator's Hive sync executor after a checkpoint commit, and on its event
 * executor after the commit that ends a bounded input, so each metric may be updated from either
 * thread.
 */
public class FlinkHiveSyncMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;

  static final String HIVE_SYNC_SUCCESS_COUNT = "hiveSyncSuccessCount";
  static final String HIVE_SYNC_FAILURE_COUNT = "hiveSyncFailureCount";
  static final String HIVE_SYNC_DURATION_MS = "hiveSyncDurationMs";
  static final String HIVE_SYNC_LAST_SUCCESS_TIME_MS = "hiveSyncLastSuccessTimeMs";

  /** Number of syncs that completed. */
  private final Counter syncSuccessCount = new ThreadSafeSimpleCounter();

  /**
   * Number of syncs that threw. The sync executor only logs a failure, so without this counter a
   * table that has stopped syncing leaves no trace outside the JobManager log.
   */
  private final Counter syncFailureCount = new ThreadSafeSimpleCounter();

  /**
   * Duration of each sync in milliseconds, failed ones included: a sync that fails after retrying
   * an unreachable metastore is the slow case this is meant to show.
   */
  private final Histogram syncDurationMs;

  /** Wall-clock time of the last completed sync in epoch milliseconds, or 0 until the first one. */
  private volatile long lastSyncSuccessTimeMs = 0L;

  public FlinkHiveSyncMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.syncDurationMs = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.counter(HIVE_SYNC_SUCCESS_COUNT, syncSuccessCount);
    metricGroup.counter(HIVE_SYNC_FAILURE_COUNT, syncFailureCount);
    metricGroup.histogram(HIVE_SYNC_DURATION_MS, syncDurationMs);
    metricGroup.gauge(HIVE_SYNC_LAST_SUCCESS_TIME_MS, () -> lastSyncSuccessTimeMs);
  }

  public void markSyncSucceeded(long durationMs) {
    syncDurationMs.update(durationMs);
    syncSuccessCount.inc();
    lastSyncSuccessTimeMs = System.currentTimeMillis();
  }

  public void markSyncFailed(long durationMs) {
    syncDurationMs.update(durationMs);
    syncFailureCount.inc();
  }
}
