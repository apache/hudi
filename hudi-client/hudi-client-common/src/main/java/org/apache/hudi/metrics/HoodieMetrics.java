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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.Timer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Wrapper for metrics-related operations.
 */
public class HoodieMetrics {

  private static final Logger LOG = LogManager.getLogger(HoodieMetrics.class);
  // Some timers
  public String rollbackTimerName = null;
  public String cleanTimerName = null;
  public String commitTimerName = null;
  public String deltaCommitTimerName = null;
  public String replaceCommitTimerName = null;
  public String finalizeTimerName = null;
  public String compactionTimerName = null;
  public String indexTimerName = null;
  private String conflictResolutionTimerName = null;
  private HoodieWriteConfig config;
  private String tableName;
  private Timer rollbackTimer = null;
  private Timer cleanTimer = null;
  private Timer commitTimer = null;
  private Timer deltaCommitTimer = null;
  private Timer finalizeTimer = null;
  private Timer compactionTimer = null;
  private Timer clusteringTimer = null;
  private Timer indexTimer = null;
  private Timer conflictResolutionTimer = null;

  public HoodieMetrics(HoodieWriteConfig config) {
    this.config = config;
    this.tableName = config.getTableName();
    if (config.isMetricsOn()) {
      Metrics.init(config);
      this.rollbackTimerName = getMetricsName("timer", HoodieTimeline.ROLLBACK_ACTION);
      this.cleanTimerName = getMetricsName("timer", HoodieTimeline.CLEAN_ACTION);
      this.commitTimerName = getMetricsName("timer", HoodieTimeline.COMMIT_ACTION);
      this.deltaCommitTimerName = getMetricsName("timer", HoodieTimeline.DELTA_COMMIT_ACTION);
      this.replaceCommitTimerName = getMetricsName("timer", HoodieTimeline.REPLACE_COMMIT_ACTION);
      this.finalizeTimerName = getMetricsName("timer", "finalize");
      this.compactionTimerName = getMetricsName("timer", HoodieTimeline.COMPACTION_ACTION);
      this.indexTimerName = getMetricsName("timer", "index");
      this.conflictResolutionTimerName = getMetricsName("timer", "conflict_resolution");
    }
  }

  private Timer createTimer(String name) {
    return config.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
  }

  public Timer.Context getRollbackCtx() {
    if (config.isMetricsOn() && rollbackTimer == null) {
      rollbackTimer = createTimer(rollbackTimerName);
    }
    return rollbackTimer == null ? null : rollbackTimer.time();
  }

  public Timer.Context getCompactionCtx() {
    if (config.isMetricsOn() && compactionTimer == null) {
      compactionTimer = createTimer(commitTimerName);
    }
    return compactionTimer == null ? null : compactionTimer.time();
  }

  public Timer.Context getClusteringCtx() {
    if (config.isMetricsOn() && clusteringTimer == null) {
      clusteringTimer = createTimer(replaceCommitTimerName);
    }
    return clusteringTimer == null ? null : clusteringTimer.time();
  }

  public Timer.Context getCleanCtx() {
    if (config.isMetricsOn() && cleanTimer == null) {
      cleanTimer = createTimer(cleanTimerName);
    }
    return cleanTimer == null ? null : cleanTimer.time();
  }

  public Timer.Context getCommitCtx() {
    if (config.isMetricsOn() && commitTimer == null) {
      commitTimer = createTimer(commitTimerName);
    }
    return commitTimer == null ? null : commitTimer.time();
  }

  public Timer.Context getFinalizeCtx() {
    if (config.isMetricsOn() && finalizeTimer == null) {
      finalizeTimer = createTimer(finalizeTimerName);
    }
    return finalizeTimer == null ? null : finalizeTimer.time();
  }

  public Timer.Context getDeltaCommitCtx() {
    if (config.isMetricsOn() && deltaCommitTimer == null) {
      deltaCommitTimer = createTimer(deltaCommitTimerName);
    }
    return deltaCommitTimer == null ? null : deltaCommitTimer.time();
  }

  public Timer.Context getIndexCtx() {
    if (config.isMetricsOn() && indexTimer == null) {
      indexTimer = createTimer(indexTimerName);
    }
    return indexTimer == null ? null : indexTimer.time();
  }

  public Timer.Context getConflictResolutionCtx() {
    if (config.isMetricsOn() && conflictResolutionTimer == null) {
      conflictResolutionTimer = createTimer(conflictResolutionTimerName);
    }
    return conflictResolutionTimer == null ? null : conflictResolutionTimer.time();
  }

  public void updateMetricsForEmptyData(String actionType) {
    if (!config.isMetricsOn() || !config.getMetricsReporterType().equals(MetricsReporterType.PROMETHEUS_PUSHGATEWAY)) {
      // No-op if metrics are not of type PROMETHEUS_PUSHGATEWAY.
      return;
    }
    Metrics.registerGauge(getMetricsName(actionType, "totalPartitionsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalFilesInsert"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalFilesUpdate"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalInsertRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalBytesWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalScanTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalCreateTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalUpsertTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesCompacted"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesSize"), 0);
  }

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
      String actionType) {
    updateCommitTimingMetrics(commitEpochTimeInMs, durationInMs, metadata, actionType);
    if (config.isMetricsOn()) {
      long totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
      long totalFilesInsert = metadata.fetchTotalFilesInsert();
      long totalFilesUpdate = metadata.fetchTotalFilesUpdated();
      long totalRecordsWritten = metadata.fetchTotalRecordsWritten();
      long totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
      long totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
      long totalBytesWritten = metadata.fetchTotalBytesWritten();
      long totalTimeTakenByScanner = metadata.getTotalScanTime();
      long totalTimeTakenForInsert = metadata.getTotalCreateTime();
      long totalTimeTakenForUpsert = metadata.getTotalUpsertTime();
      long totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
      long totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
      long totalLogFilesSize = metadata.getTotalLogFilesSize();
      Metrics.registerGauge(getMetricsName(actionType, "totalPartitionsWritten"), totalPartitionsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalFilesInsert"), totalFilesInsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalFilesUpdate"), totalFilesUpdate);
      Metrics.registerGauge(getMetricsName(actionType, "totalRecordsWritten"), totalRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), totalUpdateRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalInsertRecordsWritten"), totalInsertRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalBytesWritten"), totalBytesWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalScanTime"), totalTimeTakenByScanner);
      Metrics.registerGauge(getMetricsName(actionType, "totalCreateTime"), totalTimeTakenForInsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalUpsertTime"), totalTimeTakenForUpsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), totalCompactedRecordsUpdated);
      Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesCompacted"), totalLogFilesCompacted);
      Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesSize"), totalLogFilesSize);
    }
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
      String actionType) {
    if (config.isMetricsOn()) {
      Pair<Option<Long>, Option<Long>> eventTimePairMinMax = metadata.getMinAndMaxEventTime();
      if (eventTimePairMinMax.getLeft().isPresent()) {
        long commitLatencyInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getLeft().get();
        Metrics.registerGauge(getMetricsName(actionType, "commitLatencyInMs"), commitLatencyInMs);
      }
      if (eventTimePairMinMax.getRight().isPresent()) {
        long commitFreshnessInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getRight().get();
        Metrics.registerGauge(getMetricsName(actionType, "commitFreshnessInMs"), commitFreshnessInMs);
      }
      Metrics.registerGauge(getMetricsName(actionType, "commitTime"), commitEpochTimeInMs);
      Metrics.registerGauge(getMetricsName(actionType, "duration"), durationInMs);
    }
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending rollback metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
      Metrics.registerGauge(getMetricsName("rollback", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("rollback", "numFilesDeleted"), numFilesDeleted);
    }
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending clean metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
      Metrics.registerGauge(getMetricsName("clean", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("clean", "numFilesDeleted"), numFilesDeleted);
    }
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending finalize write metrics (duration=%d, numFilesFinalized=%d)", durationInMs,
          numFilesFinalized));
      Metrics.registerGauge(getMetricsName("finalize", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("finalize", "numFilesFinalized"), numFilesFinalized);
    }
  }

  public void updateIndexMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending index metrics (%s.duration, %d)", action, durationInMs));
      Metrics.registerGauge(getMetricsName("index", String.format("%s.duration", action)), durationInMs);
    }
  }

  public void updateConflictResolutionMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending index metrics (%s.duration, %d)", action, durationInMs));
      Metrics.registerGauge(getMetricsName("conflict_resolution", String.format("%s.duration", action)), durationInMs);
    }
  }

  String getMetricsName(String action, String metric) {
    return config == null ? null : String.format("%s.%s.%s", config.getMetricReporterMetricsNamePrefix(), action, metric);
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }

  public void emitConflictResolutionSuccessful() {
    if (config.isMetricsOn()) {
      LOG.info("Sending conflict resolution success metric");
      Metrics.registerGauge(getMetricsName("resolve", "conflict.success.count"), 1);
    }
  }

  public void emitConflictResolutionFailed() {
    if (config.isMetricsOn()) {
      LOG.info("Sending conflict resolution failed metric");
      Metrics.registerGauge(getMetricsName("resolve", "conflict.failure.count"), 1);
    }
  }
}
