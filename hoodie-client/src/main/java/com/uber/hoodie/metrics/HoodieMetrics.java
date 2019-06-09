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

package com.uber.hoodie.metrics;

import static com.uber.hoodie.metrics.Metrics.registerGauge;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Wrapper for metrics-related operations.
 */
public class HoodieMetrics {

  private static Logger logger = LogManager.getLogger(HoodieMetrics.class);
  // Some timers
  public String rollbackTimerName = null;
  public String cleanTimerName = null;
  public String commitTimerName = null;
  public String deltaCommitTimerName = null;
  public String finalizeTimerName = null;
  public String compactionTimerName = null;
  private HoodieWriteConfig config = null;
  private String tableName = null;
  private Timer rollbackTimer = null;
  private Timer cleanTimer = null;
  private Timer commitTimer = null;
  private Timer deltaCommitTimer = null;
  private Timer finalizeTimer = null;
  private Timer compactionTimer = null;

  public HoodieMetrics(HoodieWriteConfig config, String tableName) {
    this.config = config;
    this.tableName = tableName;
    if (config.isMetricsOn()) {
      Metrics.init(config);
      this.rollbackTimerName = getMetricsName("timer", HoodieTimeline.ROLLBACK_ACTION);
      this.cleanTimerName = getMetricsName("timer", HoodieTimeline.CLEAN_ACTION);
      this.commitTimerName = getMetricsName("timer", HoodieTimeline.COMMIT_ACTION);
      this.deltaCommitTimerName = getMetricsName("timer", HoodieTimeline.DELTA_COMMIT_ACTION);
      this.finalizeTimerName = getMetricsName("timer", "finalize");
      this.compactionTimerName = getMetricsName("timer", HoodieTimeline.COMPACTION_ACTION);
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

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs,
      HoodieCommitMetadata metadata, String actionType) {
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
      registerGauge(getMetricsName(actionType, "duration"), durationInMs);
      registerGauge(getMetricsName(actionType, "totalPartitionsWritten"), totalPartitionsWritten);
      registerGauge(getMetricsName(actionType, "totalFilesInsert"), totalFilesInsert);
      registerGauge(getMetricsName(actionType, "totalFilesUpdate"), totalFilesUpdate);
      registerGauge(getMetricsName(actionType, "totalRecordsWritten"), totalRecordsWritten);
      registerGauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), totalUpdateRecordsWritten);
      registerGauge(getMetricsName(actionType, "totalInsertRecordsWritten"), totalInsertRecordsWritten);
      registerGauge(getMetricsName(actionType, "totalBytesWritten"), totalBytesWritten);
      registerGauge(getMetricsName(actionType, "commitTime"), commitEpochTimeInMs);
      registerGauge(getMetricsName(actionType, "totalScanTime"), totalTimeTakenByScanner);
      registerGauge(getMetricsName(actionType, "totalCreateTime"), totalTimeTakenForInsert);
      registerGauge(getMetricsName(actionType, "totalUpsertTime"), totalTimeTakenForUpsert);
      registerGauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), totalCompactedRecordsUpdated);
      registerGauge(getMetricsName(actionType, "totalLogFilesCompacted"), totalLogFilesCompacted);
      registerGauge(getMetricsName(actionType, "totalLogFilesSize"), totalLogFilesSize);
    }
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    if (config.isMetricsOn()) {
      logger.info(String
          .format("Sending rollback metrics (duration=%d, numFilesDeleted=%d)", durationInMs,
              numFilesDeleted));
      registerGauge(getMetricsName("rollback", "duration"), durationInMs);
      registerGauge(getMetricsName("rollback", "numFilesDeleted"), numFilesDeleted);
    }
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    if (config.isMetricsOn()) {
      logger.info(String
          .format("Sending clean metrics (duration=%d, numFilesDeleted=%d)", durationInMs,
              numFilesDeleted));
      registerGauge(getMetricsName("clean", "duration"), durationInMs);
      registerGauge(getMetricsName("clean", "numFilesDeleted"), numFilesDeleted);
    }
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    if (config.isMetricsOn()) {
      logger.info(String
          .format("Sending finalize write metrics (duration=%d, numFilesFinalized=%d)",
              durationInMs, numFilesFinalized));
      registerGauge(getMetricsName("finalize", "duration"), durationInMs);
      registerGauge(getMetricsName("finalize", "numFilesFinalized"), numFilesFinalized);
    }
  }

  @VisibleForTesting
  String getMetricsName(String action, String metric) {
    return config == null ? null : String.format("%s.%s.%s", tableName, action, metric);
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }
}
