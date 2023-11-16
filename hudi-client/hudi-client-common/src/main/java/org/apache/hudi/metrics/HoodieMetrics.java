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
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for metrics-related operations.
 */
public class HoodieMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetrics.class);

  public static final String TOTAL_PARTITIONS_WRITTEN_STR = "totalPartitionsWritten";
  public static final String TOTAL_FILES_INSERT_STR = "totalFilesInsert";
  public static final String TOTAL_FILES_UPDATE_STR = "totalFilesUpdate";
  public static final String TOTAL_RECORDS_WRITTEN_STR = "totalRecordsWritten";
  public static final String TOTAL_UPDATE_RECORDS_WRITTEN_STR = "totalUpdateRecordsWritten";
  public static final String TOTAL_INSERT_RECORDS_WRITTEN_STR = "totalInsertRecordsWritten";
  public static final String TOTAL_BYTES_WRITTEN_STR = "totalBytesWritten";
  public static final String TOTAL_SCAN_TIME_STR = "totalScanTime";
  public static final String TOTAL_CREATE_TIME_STR = "totalCreateTime";
  public static final String TOTAL_UPSERT_TIME_STR = "totalUpsertTime";
  public static final String TOTAL_COMPACTED_RECORDS_UPDATED_STR = "totalCompactedRecordsUpdated";
  public static final String TOTAL_LOG_FILES_COMPACTED_STR = "totalLogFilesCompacted";
  public static final String TOTAL_LOG_FILES_SIZE_STR = "totalLogFilesSize";
  public static final String TOTAL_RECORDS_DELETED = "totalRecordsDeleted";
  public static final String TOTAL_CORRUPTED_LOG_BLOCKS_STR = "totalCorruptedLogBlocks";
  public static final String TOTAL_ROLLBACK_LOG_BLOCKS_STR = "totalRollbackLogBlocks";
  public static final String DURATION_STR = "duration";
  public static final String DELETE_FILES_NUM_STR = "numFilesDeleted";
  public static final String DELETE_INSTANTS_NUM_STR = "numInstantsArchived";
  public static final String FINALIZED_FILES_NUM_STR = "numFilesFinalized";
  public static final String CONFLICT_RESOLUTION_STR = "conflict_resolution";
  public static final String COMMIT_LATENCY_IN_MS_STR = "commitLatencyInMs";
  public static final String COMMIT_FRESHNESS_IN_MS_STR = "commitFreshnessInMs";
  public static final String COMMIT_TIME_STR = "commitTime";
  public static final String SUCCESS_EXTENSION = ".success";
  public static final String FAILURE_EXTENSION = ".failure";

  public static final String TIMER_ACTION = "timer";
  public static final String COUNTER_ACTION = "counter";
  public static final String ARCHIVE_ACTION = "archive";
  public static final String FINALIZE_ACTION = "finalize";
  public static final String INDEX_ACTION = "index";
  
  private Metrics metrics;
  // Some timers
  public String rollbackTimerName = null;
  public String cleanTimerName = null;
  public String archiveTimerName = null;
  public String commitTimerName = null;
  public String logCompactionTimerName = null;
  public String deltaCommitTimerName = null;
  public String replaceCommitTimerName = null;
  public String finalizeTimerName = null;
  public String compactionTimerName = null;
  public String indexTimerName = null;
  private String conflictResolutionTimerName = null;
  private String conflictResolutionSuccessCounterName = null;
  private String conflictResolutionFailureCounterName = null;
  private String compactionRequestedCounterName = null;
  private String compactionCompletedCounterName = null;
  private HoodieWriteConfig config;
  private String tableName;
  private Timer rollbackTimer = null;
  private Timer cleanTimer = null;
  private Timer archiveTimer = null;
  private Timer commitTimer = null;
  private Timer deltaCommitTimer = null;
  private Timer finalizeTimer = null;
  private Timer compactionTimer = null;
  private Timer logCompactionTimer = null;
  private Timer clusteringTimer = null;
  private Timer indexTimer = null;
  private Timer conflictResolutionTimer = null;
  private Counter conflictResolutionSuccessCounter = null;
  private Counter conflictResolutionFailureCounter = null;
  private Counter compactionRequestedCounter = null;
  private Counter compactionCompletedCounter = null;

  public HoodieMetrics(HoodieWriteConfig config) {
    this.config = config;
    this.tableName = config.getTableName();
    if (config.isMetricsOn()) {
      metrics = Metrics.getInstance(config);
      this.rollbackTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.ROLLBACK_ACTION);
      this.cleanTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.CLEAN_ACTION);
      this.archiveTimerName = getMetricsName(TIMER_ACTION, ARCHIVE_ACTION);
      this.commitTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.COMMIT_ACTION);
      this.deltaCommitTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION);
      this.replaceCommitTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION);
      this.finalizeTimerName = getMetricsName(TIMER_ACTION, FINALIZE_ACTION);
      this.compactionTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.COMPACTION_ACTION);
      this.logCompactionTimerName = getMetricsName(TIMER_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION);
      this.indexTimerName = getMetricsName(TIMER_ACTION, INDEX_ACTION);
      this.conflictResolutionTimerName = getMetricsName(TIMER_ACTION, CONFLICT_RESOLUTION_STR);
      this.conflictResolutionSuccessCounterName = getMetricsName(COUNTER_ACTION, CONFLICT_RESOLUTION_STR + SUCCESS_EXTENSION);
      this.conflictResolutionFailureCounterName = getMetricsName(COUNTER_ACTION, CONFLICT_RESOLUTION_STR + FAILURE_EXTENSION);
      this.compactionRequestedCounterName = getMetricsName(COUNTER_ACTION, HoodieTimeline.COMPACTION_ACTION + HoodieTimeline.REQUESTED_EXTENSION);
      this.compactionCompletedCounterName = getMetricsName(COUNTER_ACTION, HoodieTimeline.COMPACTION_ACTION + HoodieTimeline.COMPLETED_EXTENSION);
    }
  }

  private Timer createTimer(String name) {
    return config.isMetricsOn() ? metrics.getRegistry().timer(name) : null;
  }

  public Metrics getMetrics() {
    return metrics;
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

  public Timer.Context getLogCompactionCtx() {
    if (config.isMetricsOn() && logCompactionTimer == null) {
      logCompactionTimer = createTimer(commitTimerName);
    }
    return logCompactionTimer == null ? null : logCompactionTimer.time();
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

  public Timer.Context getArchiveCtx() {
    if (config.isMetricsOn() && archiveTimer == null) {
      archiveTimer = createTimer(archiveTimerName);
    }
    return archiveTimer == null ? null : archiveTimer.time();
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
    if (config.isLockingMetricsEnabled() && conflictResolutionTimer == null) {
      conflictResolutionTimer = createTimer(conflictResolutionTimerName);
    }
    return conflictResolutionTimer == null ? null : conflictResolutionTimer.time();
  }

  public void updateMetricsForEmptyData(String actionType) {
    if (!config.isMetricsOn() || !config.getMetricsReporterType().equals(MetricsReporterType.PROMETHEUS_PUSHGATEWAY)) {
      // No-op if metrics are not of type PROMETHEUS_PUSHGATEWAY.
      return;
    }
    metrics.registerGauge(getMetricsName(actionType, TOTAL_PARTITIONS_WRITTEN_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_FILES_INSERT_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_FILES_UPDATE_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_RECORDS_WRITTEN_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_UPDATE_RECORDS_WRITTEN_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_INSERT_RECORDS_WRITTEN_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_RECORDS_DELETED), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_BYTES_WRITTEN_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_SCAN_TIME_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_CREATE_TIME_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_UPSERT_TIME_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_COMPACTED_RECORDS_UPDATED_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_LOG_FILES_COMPACTED_STR), 0);
    metrics.registerGauge(getMetricsName(actionType, TOTAL_LOG_FILES_SIZE_STR), 0);
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
      long totalRecordsDeleted = metadata.getTotalRecordsDeleted();
      long totalBytesWritten = metadata.fetchTotalBytesWritten();
      long totalTimeTakenByScanner = metadata.getTotalScanTime();
      long totalTimeTakenForInsert = metadata.getTotalCreateTime();
      long totalTimeTakenForUpsert = metadata.getTotalUpsertTime();
      long totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
      long totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
      long totalLogFilesSize = metadata.getTotalLogFilesSize();
      metrics.registerGauge(getMetricsName(actionType, TOTAL_PARTITIONS_WRITTEN_STR), totalPartitionsWritten);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_FILES_INSERT_STR), totalFilesInsert);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_FILES_UPDATE_STR), totalFilesUpdate);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_RECORDS_WRITTEN_STR), totalRecordsWritten);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_UPDATE_RECORDS_WRITTEN_STR), totalUpdateRecordsWritten);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_INSERT_RECORDS_WRITTEN_STR), totalInsertRecordsWritten);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_BYTES_WRITTEN_STR), totalBytesWritten);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_SCAN_TIME_STR), totalTimeTakenByScanner);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_CREATE_TIME_STR), totalTimeTakenForInsert);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_UPSERT_TIME_STR), totalTimeTakenForUpsert);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_COMPACTED_RECORDS_UPDATED_STR), totalCompactedRecordsUpdated);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_LOG_FILES_COMPACTED_STR), totalLogFilesCompacted);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_LOG_FILES_SIZE_STR), totalLogFilesSize);
      metrics.registerGauge(getMetricsName(actionType, TOTAL_RECORDS_DELETED), totalRecordsDeleted);
      if (config.isCompactionLogBlockMetricsOn()) {
        long totalCorruptedLogBlocks = metadata.getTotalCorruptLogBlocks();
        long totalRollbackLogBlocks = metadata.getTotalRollbackLogBlocks();
        metrics.registerGauge(getMetricsName(actionType, TOTAL_CORRUPTED_LOG_BLOCKS_STR), totalCorruptedLogBlocks);
        metrics.registerGauge(getMetricsName(actionType, TOTAL_ROLLBACK_LOG_BLOCKS_STR), totalRollbackLogBlocks);
      }
    }
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
      String actionType) {
    if (config.isMetricsOn()) {
      Pair<Option<Long>, Option<Long>> eventTimePairMinMax = metadata.getMinAndMaxEventTime();
      if (eventTimePairMinMax.getLeft().isPresent()) {
        long commitLatencyInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getLeft().get();
        metrics.registerGauge(getMetricsName(actionType, COMMIT_LATENCY_IN_MS_STR), commitLatencyInMs);
      }
      if (eventTimePairMinMax.getRight().isPresent()) {
        long commitFreshnessInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getRight().get();
        metrics.registerGauge(getMetricsName(actionType, COMMIT_FRESHNESS_IN_MS_STR), commitFreshnessInMs);
      }
      metrics.registerGauge(getMetricsName(actionType, COMMIT_TIME_STR), commitEpochTimeInMs);
      metrics.registerGauge(getMetricsName(actionType, DURATION_STR), durationInMs);
    }
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending rollback metrics (%s=%d, %s=%d)", DURATION_STR, durationInMs,
              DELETE_FILES_NUM_STR, numFilesDeleted));
      metrics.registerGauge(getMetricsName(HoodieTimeline.ROLLBACK_ACTION, DURATION_STR), durationInMs);
      metrics.registerGauge(getMetricsName(HoodieTimeline.ROLLBACK_ACTION, DELETE_FILES_NUM_STR), numFilesDeleted);
    }
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending clean metrics (%s=%d, %s=%d)", DURATION_STR, durationInMs,
              DELETE_FILES_NUM_STR, numFilesDeleted));
      metrics.registerGauge(getMetricsName(HoodieTimeline.CLEAN_ACTION, DURATION_STR), durationInMs);
      metrics.registerGauge(getMetricsName(HoodieTimeline.CLEAN_ACTION, DELETE_FILES_NUM_STR), numFilesDeleted);
    }
  }

  public void updateArchiveMetrics(long durationInMs, int numInstantsArchived) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending archive metrics (%s=%d, %s=%d)", DURATION_STR, durationInMs,
              DELETE_INSTANTS_NUM_STR, numInstantsArchived));
      metrics.registerGauge(getMetricsName(ARCHIVE_ACTION, DURATION_STR), durationInMs);
      metrics.registerGauge(getMetricsName(ARCHIVE_ACTION, DELETE_INSTANTS_NUM_STR), numInstantsArchived);
    }
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending finalize write metrics (%s=%d, %s=%d)", DURATION_STR, durationInMs,
          FINALIZED_FILES_NUM_STR, numFilesFinalized));
      metrics.registerGauge(getMetricsName(FINALIZE_ACTION, DURATION_STR), durationInMs);
      metrics.registerGauge(getMetricsName(FINALIZE_ACTION, FINALIZED_FILES_NUM_STR), numFilesFinalized);
    }
  }

  public void updateIndexMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending index metrics (%s.%s, %d)", action, DURATION_STR, durationInMs));
      metrics.registerGauge(getMetricsName(INDEX_ACTION, String.format("%s.%s", action, DURATION_STR)), durationInMs);
    }
  }

  @VisibleForTesting
  public String getMetricsName(String action, String metric) {
    return config == null ? null : String.format("%s.%s.%s", config.getMetricReporterMetricsNamePrefix(), action, metric);
  }

  public void updateClusteringFileCreationMetrics(long durationInMs) {
    reportMetrics(HoodieTimeline.REPLACE_COMMIT_ACTION, "fileCreationTime", durationInMs);
  }

  /**
   * Given a commit action, metrics name and value this method reports custom metrics.
   */
  public void reportMetrics(String commitAction, String metricName, long value) {
    metrics.registerGauge(getMetricsName(commitAction, metricName), value);
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }

  public void emitConflictResolutionSuccessful() {
    if (config.isLockingMetricsEnabled()) {
      LOG.info("Sending conflict resolution success metric");
      conflictResolutionSuccessCounter = getCounter(conflictResolutionSuccessCounter, conflictResolutionSuccessCounterName);
      conflictResolutionSuccessCounter.inc();
    }
  }

  public void emitConflictResolutionFailed() {
    if (config.isLockingMetricsEnabled()) {
      LOG.info("Sending conflict resolution failure metric");
      conflictResolutionFailureCounter = getCounter(conflictResolutionFailureCounter, conflictResolutionFailureCounterName);
      conflictResolutionFailureCounter.inc();
    }
  }

  public void emitCompactionRequested() {
    if (config.isMetricsOn()) {
      compactionRequestedCounter = getCounter(compactionRequestedCounter, compactionRequestedCounterName);
      compactionRequestedCounter.inc();
    }
  }

  public void emitCompactionCompleted() {
    if (config.isMetricsOn()) {
      compactionCompletedCounter = getCounter(compactionCompletedCounter, compactionCompletedCounterName);
      compactionCompletedCounter.inc();
    }
  }

  private Counter getCounter(Counter counter, String name) {
    if (counter == null) {
      return metrics.getRegistry().counter(name);
    }
    return counter;
  }
}
