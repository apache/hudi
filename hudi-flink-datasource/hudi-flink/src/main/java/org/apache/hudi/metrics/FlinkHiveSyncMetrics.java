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

import org.apache.hudi.hive.HoodieHiveSyncStats;

import com.codahale.metrics.SlidingWindowReservoir;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Map;

/**
 * Metrics for the Hive sync that {@link org.apache.hudi.sink.StreamWriteOperatorCoordinator} runs
 * on the JobManager after each commit.
 *
 * <p>A sync runs on the coordinator's Hive sync executor after a checkpoint commit, and on its event
 * executor after the commit that ends a bounded input, so each metric may be updated from either
 * thread.
 *
 * <p>The breakdown of each sync, from the init duration to the schema evolved count, is only
 * registered when {@code hoodie.datasource.hive_sync.detailed_metrics.enabled} is set.
 *
 * <p>Obtain an instance with {@link #register}. Flink keeps one coordinator metric group per operator
 * for the life of the job, but creates a new coordinator whenever it rebuilds the execution graph,
 * as the adaptive scheduler does on every restart and rescale. That group ignores a metric registered
 * under a name it already holds, so metrics owned by each coordinator would stay frozen at the first
 * coordinator's values. Every coordinator of the same operator in the same job therefore shares one
 * instance.
 */
public class FlinkHiveSyncMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;

  /**
   * The instance shared by the coordinators of each operator, keyed by job and operator ID.
   *
   * <p>Values are weak: between coordinators, the only strong reference to an instance is the
   * {@link #HIVE_SYNC_LAST_SUCCESS_TIME_MS} gauge registered on the operator's group, so an instance
   * lives as long as that group reports it and its entry goes once the job's group is closed. That
   * gauge must keep referencing the instance, and the cache must not be bounded by size.
   */
  private static final Cache<String, FlinkHiveSyncMetrics> SHARED_METRICS = Caffeine.newBuilder().weakValues().build();

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
   * Recorded whether building it succeeds or fails, so the time spent failing to reach the
   * metastore shows here.
   */
  private final Histogram initDurationMs;

  /** Time a sync spent reading the table schema from storage, for syncs that read it. */
  private final Histogram schemaReadDurationMs;

  /**
   * Time a sync spent finding the partitions to sync, from the timeline or by listing storage, for
   * syncs that looked for them. This grows with the number of commits a sync has to catch up on.
   */
  private final Histogram partitionScanDurationMs;

  /**
   * Time a sync spent on everything but reading the schema and finding partitions: mostly metastore
   * calls, plus the local work between them, such as comparing the partitions and schemas found.
   */
  private final Histogram remainingDurationMs;

  /**
   * Number of partitions added to the metastore, counting for each sync the most added to any one of
   * its Hive tables. A MERGE_ON_READ table normally adds the same partitions to each of its Hive
   * tables, and they are counted once. It staying flat while commits land means the syncs register
   * nothing.
   */
  private final Counter partitionsAddedCount = new ThreadSafeSimpleCounter();

  /**
   * Number of syncs that found the storage schema changed and pushed it to the metastore, counted
   * once per sync however many Hive tables the table is registered as.
   */
  private final Counter schemaEvolvedCount = new ThreadSafeSimpleCounter();

  private final boolean detailedMetricsEnabled;

  /**
   * @param detailedMetricsEnabled whether to register and update the breakdown of each sync, which the sync records
   *     only when its detailed metrics are enabled
   */
  FlinkHiveSyncMetrics(MetricGroup metricGroup, boolean detailedMetricsEnabled) {
    super(metricGroup);
    this.detailedMetricsEnabled = detailedMetricsEnabled;
    // Detailed histograms are only accessed when detailed metrics are enabled.
    this.initDurationMs = detailedMetricsEnabled ? newHistogram() : null;
    this.schemaReadDurationMs = detailedMetricsEnabled ? newHistogram() : null;
    this.partitionScanDurationMs = detailedMetricsEnabled ? newHistogram() : null;
    this.remainingDurationMs = detailedMetricsEnabled ? newHistogram() : null;
  }

  /**
   * Returns the metrics of the coordinator that owns {@code metricGroup}, registered on it.
   *
   * <p>A coordinator that replaces an earlier one of the same operator and job gets the earlier
   * one's instance, so its syncs update the metrics the group already reports, and the counts carry
   * on across the restart. The instance is still registered on {@code metricGroup}, which covers a
   * group that is not the one it was first registered on, such as the group of a JobMaster
   * recreated for the same job.
   *
   * <p>A group that does not identify its job and operator, as in tests, gets an instance of its own.
   */
  public static FlinkHiveSyncMetrics register(MetricGroup metricGroup, boolean detailedMetricsEnabled) {
    Map<String, String> variables = metricGroup.getAllVariables();
    String jobId = variables.get(ScopeFormat.SCOPE_JOB_ID);
    String operatorId = variables.get(ScopeFormat.SCOPE_OPERATOR_ID);
    FlinkHiveSyncMetrics metrics = jobId == null || operatorId == null
        ? new FlinkHiveSyncMetrics(metricGroup, detailedMetricsEnabled)
        : SHARED_METRICS.get(jobId + "/" + operatorId,
            key -> new FlinkHiveSyncMetrics(metricGroup, detailedMetricsEnabled));
    metrics.registerMetrics(metricGroup);
    return metrics;
  }

  @Override
  public void registerMetrics() {
    registerMetrics(metricGroup);
  }

  private void registerMetrics(MetricGroup group) {
    group.counter(HIVE_SYNC_SUCCESS_COUNT, syncSuccessCount);
    group.counter(HIVE_SYNC_FAILURE_COUNT, syncFailureCount);
    group.histogram(HIVE_SYNC_DURATION_MS, syncDurationMs);
    group.gauge(HIVE_SYNC_LAST_SUCCESS_TIME_MS, () -> lastSyncSuccessTimeMs);
    if (detailedMetricsEnabled) {
      group.histogram(HIVE_SYNC_INIT_DURATION_MS, initDurationMs);
      group.histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS, schemaReadDurationMs);
      group.histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS, partitionScanDurationMs);
      group.histogram(HIVE_SYNC_REMAINING_DURATION_MS, remainingDurationMs);
      group.counter(HIVE_SYNC_PARTITIONS_ADDED_COUNT, partitionsAddedCount);
      group.counter(HIVE_SYNC_SCHEMA_EVOLVED_COUNT, schemaEvolvedCount);
    }
  }

  public void updateInitDuration(long durationMs) {
    if (!detailedMetricsEnabled) {
      return;
    }
    initDurationMs.update(durationMs);
  }

  /**
   * Records what a sync did, after it completed or failed.
   */
  public void updateSyncMetrics(HoodieHiveSyncStats syncMetrics) {
    if (!detailedMetricsEnabled) {
      return;
    }
    syncMetrics.getSchemaReadMs().ifPresent(schemaReadDurationMs::update);
    syncMetrics.getPartitionScanMs().ifPresent(partitionScanDurationMs::update);
    syncMetrics.getRemainingMs().ifPresent(remainingDurationMs::update);
    partitionsAddedCount.inc(syncMetrics.getPartitionsAdded());
    if (syncMetrics.isSchemaEvolved()) {
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
