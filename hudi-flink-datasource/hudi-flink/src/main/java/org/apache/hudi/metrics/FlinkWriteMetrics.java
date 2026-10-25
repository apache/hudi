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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.text.ParseException;

/**
 * Common flink write commit metadata metrics.
 */
@Slf4j
public class FlinkWriteMetrics extends HoodieFlinkMetrics {

  protected final String actionType;

  private long totalPartitionsWritten;
  private long totalFilesInsert;
  private long totalFilesUpdate;
  private long totalRecordsWritten;
  private long totalUpdateRecordsWritten;
  private long totalInsertRecordsWritten;
  private long totalBytesWritten;
  private long totalScanTime;
  private long totalCompactedRecordsUpdated;
  private long totalLogFilesCompacted;
  private long totalLogFilesSize;
  private long commitEpochTimeInMs;
  private long durationInMs;

  public FlinkWriteMetrics(MetricGroup metricGroup, String actionType) {
    super(metricGroup);
    this.actionType = actionType;
  }

  @Override
  public void registerMetrics() {
    // register commit gauge
    metricGroup.gauge(getMetricsName(actionType, "totalPartitionsWritten"), () -> totalPartitionsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalFilesInsert"), () -> totalFilesInsert);
    metricGroup.gauge(getMetricsName(actionType, "totalFilesUpdate"), () -> totalFilesUpdate);
    metricGroup.gauge(getMetricsName(actionType, "totalRecordsWritten"), () -> totalRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), () -> totalUpdateRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalInsertRecordsWritten"), () -> totalInsertRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalBytesWritten"), () -> totalBytesWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalScanTime"), () -> totalScanTime);
    metricGroup.gauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), () -> totalCompactedRecordsUpdated);
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesCompacted"), () -> totalLogFilesCompacted);
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesSize"), () -> totalLogFilesSize);
    metricGroup.gauge(getMetricsName(actionType, "commitTime"), () -> commitEpochTimeInMs);
    metricGroup.gauge(getMetricsName(actionType, "duration"), () -> durationInMs);
  }

  public void updateCommitMetrics(String instantTime, HoodieCommitMetadata metadata) {
    long commitEpochTimeInMs;
    try {
      commitEpochTimeInMs = HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTime).getTime();
    } catch (ParseException e) {
      log.warn("Invalid input issued instant: {}", instantTime);
      return;
    }
    updateCommitMetrics(commitEpochTimeInMs, System.currentTimeMillis() - commitEpochTimeInMs, metadata);
  }

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata) {
    updateCommitTimingMetrics(commitEpochTimeInMs, durationInMs);
    totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
    totalFilesInsert = metadata.fetchTotalFilesInsert();
    totalFilesUpdate = metadata.fetchTotalFilesUpdated();
    totalRecordsWritten = metadata.fetchTotalRecordsWritten();
    totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
    totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
    totalBytesWritten = metadata.fetchTotalBytesWritten();
    totalScanTime = metadata.getTotalScanTime();
    totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
    totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
    totalLogFilesSize = metadata.getTotalLogFilesSize();
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs) {
    this.commitEpochTimeInMs = commitEpochTimeInMs;
    this.durationInMs = durationInMs;
  }

  protected String getMetricsName(String action, String metric) {
    return String.format("%s.%s", action, metric);
  }

}
