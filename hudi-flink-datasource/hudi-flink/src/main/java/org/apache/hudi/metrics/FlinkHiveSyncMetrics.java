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

import org.apache.hudi.hive.HiveSyncStats;

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
  static final String HIVE_SYNC_INIT_DURATION_MS = "hiveSyncInitDurationMs";
  static final String HIVE_SYNC_SCHEMA_READ_DURATION_MS = "hiveSyncSchemaReadDurationMs";
  static final String HIVE_SYNC_PARTITION_SCAN_DURATION_MS = "hiveSyncPartitionScanDurationMs";
  static final String HIVE_SYNC_REMAINING_DURATION_MS = "hiveSyncRemainingDurationMs";
  static final String HIVE_SYNC_PARTITIONS_ADDED_COUNT = "hiveSyncPartitionsAddedCount";
  static final String HIVE_SYNC_SCHEMA_EVOLVED_COUNT = "hiveSyncSchemaEvolvedCount";

  /** Number of syncs that completed. */
  private final Counter syncSuccessCount = new ThreadSafeSimpleCounter();

  /**
   * Number of syncs that threw, or that did nothing because the metastore client could not be created
   * and {@code hive_sync.ignore_exceptions} swallowed the error. The sync executor only logs a
   * failure, so without this counter a table that has stopped syncing leaves no trace outside the
   * JobManager log.
   */
  private final Counter syncFailureCount = new ThreadSafeSimpleCounter();

  /**
   * Duration of each sync in milliseconds, failed ones included: a sync that fails after retrying
   * an unreachable metastore is the slow case this is meant to show.
   */
  private final Histogram syncDurationMs = newHistogram();

  /** Wall-clock time of the last completed sync in epoch milliseconds, or 0 until the first one. */
  private volatile long lastSyncSuccessTimeMs = 0L;

  /**
   * Time to build the sync tool, which creates the Hudi meta client and connects to the metastore.
   * Recorded when building it succeeds.
   */
  private final Histogram initDurationMs = newHistogram();

  /** Time a sync spent reading the table schema from storage, for syncs that read it. */
  private final Histogram schemaReadDurationMs = newHistogram();

  /**
   * Time a sync spent finding the partitions to sync, from the timeline or by listing storage, for
   * syncs that looked for them. This grows with the number of commits a sync has to catch up on.
   */
  private final Histogram partitionScanDurationMs = newHistogram();

  /**
   * Time a sync spent on everything but reading the schema and finding partitions: mostly metastore
   * calls, plus the local work between them, such as comparing the partitions and schemas found.
   */
  private final Histogram remainingDurationMs = newHistogram();

  /**
   * Number of partitions added to the metastore. A MERGE_ON_READ table adds the same partitions to
   * each of its Hive tables, and they are counted once. It staying flat while commits land means
   * the syncs register nothing.
   */
  private final Counter partitionsAddedCount = new ThreadSafeSimpleCounter();

  /**
   * Number of syncs that found the storage schema changed and pushed it to the metastore, counted
   * once per sync however many Hive tables the table is registered as.
   */
  private final Counter schemaEvolvedCount = new ThreadSafeSimpleCounter();

  public FlinkHiveSyncMetrics(MetricGroup metricGroup) {
    super(metricGroup);
  }

  @Override
  public void registerMetrics() {
    metricGroup.counter(HIVE_SYNC_SUCCESS_COUNT, syncSuccessCount);
    metricGroup.counter(HIVE_SYNC_FAILURE_COUNT, syncFailureCount);
    metricGroup.histogram(HIVE_SYNC_DURATION_MS, syncDurationMs);
    metricGroup.gauge(HIVE_SYNC_LAST_SUCCESS_TIME_MS, () -> lastSyncSuccessTimeMs);
    metricGroup.histogram(HIVE_SYNC_INIT_DURATION_MS, initDurationMs);
    metricGroup.histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS, schemaReadDurationMs);
    metricGroup.histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS, partitionScanDurationMs);
    metricGroup.histogram(HIVE_SYNC_REMAINING_DURATION_MS, remainingDurationMs);
    metricGroup.counter(HIVE_SYNC_PARTITIONS_ADDED_COUNT, partitionsAddedCount);
    metricGroup.counter(HIVE_SYNC_SCHEMA_EVOLVED_COUNT, schemaEvolvedCount);
  }

  public void updateInitDuration(long durationMs) {
    initDurationMs.update(durationMs);
  }

  /**
   * Records what a sync did, after it completed or failed.
   */
  public void updateSyncStats(HiveSyncStats stats) {
    stats.getSchemaReadMs().ifPresent(schemaReadDurationMs::update);
    stats.getPartitionScanMs().ifPresent(partitionScanDurationMs::update);
    stats.getRemainingMs().ifPresent(remainingDurationMs::update);
    partitionsAddedCount.inc(stats.getPartitionsAdded());
    if (stats.isSchemaEvolved()) {
      schemaEvolvedCount.inc();
    }
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

  private static Histogram newHistogram() {
    return new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }
}
