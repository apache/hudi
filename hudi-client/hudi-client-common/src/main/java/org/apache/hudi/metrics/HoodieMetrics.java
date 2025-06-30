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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH;

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
  public static final String EARLIEST_PENDING_CLUSTERING_INSTANT_STR = "earliestInflightClusteringInstant";
  public static final String EARLIEST_PENDING_COMPACTION_INSTANT_STR = "earliestInflightCompactionInstant";
  public static final String EARLIEST_PENDING_CLEAN_INSTANT_STR = "earliestInflightCleanInstant";
  public static final String EARLIEST_PENDING_ROLLBACK_INSTANT_STR = "earliestInflightRollbackInstant";
  public static final String LATEST_COMPLETED_CLUSTERING_INSTANT_STR = "latestCompletedClusteringInstant";
  public static final String LATEST_COMPLETED_COMPACTION_INSTANT_STR = "latestCompletedCompactionInstant";
  public static final String LATEST_COMPLETED_CLEAN_INSTANT_STR = "latestCompletedCleanInstant";
  public static final String LATEST_COMPLETED_ROLLBACK_INSTANT_STR = "latestCompletedRollbackInstant";
  public static final String PENDING_CLUSTERING_INSTANT_COUNT_STR = "pendingClusteringInstantCount";
  public static final String PENDING_COMPACTION_INSTANT_COUNT_STR = "pendingCompactionInstantCount";
  public static final String PENDING_CLEAN_INSTANT_COUNT_STR = "pendingCleanInstantCount";
  public static final String PENDING_ROLLBACK_INSTANT_COUNT_STR = "pendingRollbackInstantCount";
  public static final String SUCCESS_EXTENSION = ".success";
  public static final String FAILURE_EXTENSION = ".failure";

  public static final String TIMER_METRIC = "timer";
  public static final String COUNTER_METRIC = "counter";
  public static final String ARCHIVE_ACTION = "archive";
  public static final String FINALIZE_ACTION = "finalize";
  public static final String INDEX_ACTION = "index";
  public static final String SOURCE_READ_AND_INDEX_ACTION = "source_read_and_index";

  public static final String COUNTER_METRIC_EXTENSION = "." + COUNTER_METRIC;
  public static final String SUCCESS_COUNTER = "success" + COUNTER_METRIC_EXTENSION;
  public static final String FAILURE_COUNTER = "failure" + COUNTER_METRIC_EXTENSION;

  private Metrics metrics;
  // Some timers
  public String rollbackTimerName = null;
  public String cleanTimerName = null;
  public String archiveTimerName = null;
  public String commitTimerName = null;
  public String logCompactionTimerName = null;
  public String deltaCommitTimerName = null;
  public String clusterCommitTimerName = null;
  public String finalizeTimerName = null;
  public String compactionTimerName = null;
  public String indexTimerName = null;
  public String sourceReadAndIndexTimerName = null;
  private String conflictResolutionTimerName = null;
  private String conflictResolutionSuccessCounterName = null;
  private String conflictResolutionFailureCounterName = null;
  private String compactionRequestedCounterName = null;
  private String compactionCompletedCounterName = null;
  private final HoodieWriteConfig config;
  private final String tableName;
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
  private Timer sourceReadAndIndexTimer = null;
  private Timer conflictResolutionTimer = null;
  private Counter conflictResolutionSuccessCounter = null;
  private Counter conflictResolutionFailureCounter = null;
  private Counter compactionRequestedCounter = null;
  private Counter compactionCompletedCounter = null;

  public HoodieMetrics(HoodieWriteConfig config, HoodieStorage storage) {
    this.config = config;
    this.tableName = config.getTableName();
    if (config.isMetricsOn()) {
      metrics = Metrics.getInstance(config.getMetricsConfig(), storage);
      this.rollbackTimerName = getMetricsName(HoodieTimeline.ROLLBACK_ACTION, TIMER_METRIC);
      this.cleanTimerName = getMetricsName(HoodieTimeline.CLEAN_ACTION, TIMER_METRIC);
      this.archiveTimerName = getMetricsName(ARCHIVE_ACTION, TIMER_METRIC);
      this.commitTimerName = getMetricsName(HoodieTimeline.COMMIT_ACTION, TIMER_METRIC);
      this.deltaCommitTimerName = getMetricsName(HoodieTimeline.DELTA_COMMIT_ACTION, TIMER_METRIC);
      this.clusterCommitTimerName = getMetricsName(HoodieTimeline.CLUSTERING_ACTION, TIMER_METRIC);
      this.finalizeTimerName = getMetricsName(FINALIZE_ACTION, TIMER_METRIC);
      this.compactionTimerName = getMetricsName(HoodieTimeline.COMPACTION_ACTION, TIMER_METRIC);
      this.logCompactionTimerName = getMetricsName(HoodieTimeline.LOG_COMPACTION_ACTION, TIMER_METRIC);
      this.indexTimerName = getMetricsName(INDEX_ACTION, TIMER_METRIC);
      this.sourceReadAndIndexTimerName = getMetricsName(SOURCE_READ_AND_INDEX_ACTION, TIMER_METRIC);
      this.conflictResolutionTimerName = getMetricsName(CONFLICT_RESOLUTION_STR, TIMER_METRIC);
      this.conflictResolutionSuccessCounterName = getMetricsName(CONFLICT_RESOLUTION_STR, SUCCESS_COUNTER);
      this.conflictResolutionFailureCounterName = getMetricsName(CONFLICT_RESOLUTION_STR, FAILURE_COUNTER);
      this.compactionRequestedCounterName = getMetricsName(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.REQUESTED_COMPACTION_SUFFIX + COUNTER_METRIC_EXTENSION);
      this.compactionCompletedCounterName = getMetricsName(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.COMPLETED_COMPACTION_SUFFIX + COUNTER_METRIC_EXTENSION);
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
      clusteringTimer = createTimer(clusterCommitTimerName);
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

  public Timer.Context getSourceReadAndIndexTimerCtx() {
    if (config.isMetricsOn() && sourceReadAndIndexTimer == null) {
      sourceReadAndIndexTimer = createTimer(sourceReadAndIndexTimerName);
    }
    return sourceReadAndIndexTimer == null ? null : sourceReadAndIndexTimer.time();
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
      LOG.debug("Sending finalize write metrics ({}={}, {}={})", DURATION_STR, durationInMs,
          FINALIZED_FILES_NUM_STR, numFilesFinalized);
      metrics.registerGauge(getMetricsName(FINALIZE_ACTION, DURATION_STR), durationInMs);
      metrics.registerGauge(getMetricsName(FINALIZE_ACTION, FINALIZED_FILES_NUM_STR), numFilesFinalized);
    }
  }

  public void updateIndexMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.debug("Sending index metrics ({}.{}, {})", action, DURATION_STR, durationInMs);
      metrics.registerGauge(getMetricsName(INDEX_ACTION, String.format("%s.%s", action, DURATION_STR)), durationInMs);
    }
  }

  public void updateSourceReadAndIndexMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.debug("Sending {} metrics ({}.duration, {})", SOURCE_READ_AND_INDEX_ACTION, action, durationInMs);
      metrics.registerGauge(getMetricsName(SOURCE_READ_AND_INDEX_ACTION, String.format("%s.duration", action)), durationInMs);
    }
  }

  @VisibleForTesting
  public String getMetricsName(String action, String metric) {
    if (config == null) {
      return null;
    }
    if (StringUtils.isNullOrEmpty(config.getMetricReporterMetricsNamePrefix())) {
      return String.format("%s.%s", action, metric);
    } else {
      return String.format("%s.%s.%s", config.getMetricReporterMetricsNamePrefix(), action, metric);
    }
  }

  public void updateClusteringFileCreationMetrics(long durationInMs) {
    reportMetrics(HoodieTimeline.CLUSTERING_ACTION, "fileCreationTime", durationInMs);
  }

  public void updateTableServiceInstantMetrics(final HoodieActiveTimeline activeTimeline) {
    updateEarliestPendingInstant(activeTimeline, EARLIEST_PENDING_CLUSTERING_INSTANT_STR, HoodieTimeline.CLUSTERING_ACTION);
    updateEarliestPendingInstant(activeTimeline, EARLIEST_PENDING_COMPACTION_INSTANT_STR, HoodieTimeline.COMPACTION_ACTION);
    updateEarliestPendingInstant(activeTimeline, EARLIEST_PENDING_CLEAN_INSTANT_STR, HoodieTimeline.CLEAN_ACTION);
    updateEarliestPendingInstant(activeTimeline, EARLIEST_PENDING_ROLLBACK_INSTANT_STR, HoodieTimeline.ROLLBACK_ACTION);

    updateLatestCompletedInstant(activeTimeline, LATEST_COMPLETED_CLUSTERING_INSTANT_STR, HoodieTimeline.REPLACE_COMMIT_ACTION);
    updateLatestCompletedInstant(activeTimeline, LATEST_COMPLETED_COMPACTION_INSTANT_STR, HoodieTimeline.COMMIT_ACTION);
    updateLatestCompletedInstant(activeTimeline, LATEST_COMPLETED_CLEAN_INSTANT_STR, HoodieTimeline.CLEAN_ACTION);
    updateLatestCompletedInstant(activeTimeline, LATEST_COMPLETED_ROLLBACK_INSTANT_STR, HoodieTimeline.ROLLBACK_ACTION);

    updatePendingInstantCount(activeTimeline, PENDING_CLUSTERING_INSTANT_COUNT_STR, HoodieTimeline.CLUSTERING_ACTION);
    updatePendingInstantCount(activeTimeline, PENDING_COMPACTION_INSTANT_COUNT_STR, HoodieTimeline.COMPACTION_ACTION);
    updatePendingInstantCount(activeTimeline, PENDING_CLEAN_INSTANT_COUNT_STR, HoodieTimeline.CLEAN_ACTION);
    updatePendingInstantCount(activeTimeline, PENDING_ROLLBACK_INSTANT_COUNT_STR, HoodieTimeline.ROLLBACK_ACTION);
  }

  /**
   * Use EarliestPendingInstant to judge which instant execution plan is the current table service blocked in.
   *
   * @param activeTimeline
   * @param metricName
   * @param action
   */
  private void updateEarliestPendingInstant(final HoodieActiveTimeline activeTimeline,
                                            final String metricName,
                                            final String action) {
    Set<String> validActions = CollectionUtils.createSet(action);
    HoodieTimeline filteredInstants = activeTimeline.filterInflightsAndRequested().filter(instant -> validActions.contains(instant.getAction()));
    Option<HoodieInstant> hoodieInstantOption = filteredInstants.firstInstant();
    if (hoodieInstantOption.isPresent()) {
      updateTimestampMetric(metricName, action, hoodieInstantOption);
    }
  }

  private void updateTimestampMetric(String metricName, String action, Option<HoodieInstant> hoodieInstantOption) {
    String requestedTime = hoodieInstantOption.get().requestedTime();
    if (requestedTime.length() > MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH) {
      // If requested instant is in MDT with table version six, it can contain suffix
      requestedTime = requestedTime.substring(0, MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH);
    }
    updateMetric(action, metricName, Long.parseLong(requestedTime));
  }

  /**
   * Use LatestCompletedInstant to observe the latest execution progress of the table service.
   *
   * @param activeTimeline
   * @param metricName
   * @param action
   */
  private void updateLatestCompletedInstant(final HoodieActiveTimeline activeTimeline,
                                            final String metricName,
                                            String action) {
    switch (metricName) {
      case LATEST_COMPLETED_COMPACTION_INSTANT_STR:
        action = HoodieActiveTimeline.COMMIT_ACTION;
        break;
      case LATEST_COMPLETED_CLUSTERING_INSTANT_STR:
        action = HoodieActiveTimeline.REPLACE_COMMIT_ACTION;
        break;
      default:
        // do nothing
    }
    Set<String> validActions = CollectionUtils.createSet(action);
    HoodieTimeline filteredInstants = activeTimeline.filterCompletedInstants().filter(instant -> validActions.contains(instant.getAction()));
    Option<HoodieInstant> hoodieInstantOption = filteredInstants.lastInstant();
    if (hoodieInstantOption.isPresent()) {
      updateTimestampMetric(metricName, action, hoodieInstantOption);
    }
  }

  /**
   * Use PendingInstantCount to judge how many execution plans are waiting to be executed.
   *
   * @param activeTimeline
   * @param metricName
   * @param action
   */
  private void updatePendingInstantCount(final HoodieActiveTimeline activeTimeline,
                                         final String metricName,
                                         final String action) {
    Set<String> validActions = CollectionUtils.createSet(action);
    HoodieTimeline filteredInstants = activeTimeline.filterInflightsAndRequested().filter(instant -> validActions.contains(instant.getAction()));
    updateMetric(action, metricName, filteredInstants.countInstants());
  }

  private void updateMetric(final String action, final String metricName, final long metricValue) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Updating timeline instant related metrics (%s=%d)", metricName, metricValue));
      metrics.registerGauge(getMetricsName(action, metricName), metricValue);
    }
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

  public void emitMetadataEnablementMetrics(boolean isMetadataEnabled, boolean isMetadataColStatsEnabled, boolean isMetadataBloomFilterEnabled,
                                            boolean isMetadataRliEnabled) {
    if (config.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("metadata", "isEnabled"), isMetadataEnabled ? 1 : 0);
      metrics.registerGauge(getMetricsName("metadata", "isColSatsEnabled"), isMetadataColStatsEnabled ? 1 : 0);
      metrics.registerGauge(getMetricsName("metadata", "isBloomFilterEnabled"), isMetadataBloomFilterEnabled ? 1 : 0);
      metrics.registerGauge(getMetricsName("metadata", "isRliEnabled"), isMetadataRliEnabled ? 1 : 0);
    }
  }

  public void emitIndexTypeMetrics(int indexTypeOrdinal) {
    if (config.isMetricsOn()) {
      metrics.registerGauge(getMetricsName("index", "type"), indexTypeOrdinal);
    }
  }

  private Counter getCounter(Counter counter, String name) {
    if (counter == null) {
      return metrics.getRegistry().counter(name);
    }
    return counter;
  }
}
