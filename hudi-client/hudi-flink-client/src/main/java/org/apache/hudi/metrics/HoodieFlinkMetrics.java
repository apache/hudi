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

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieFlinkMetrics {

  private static final Logger LOG = LogManager.getLogger(HoodieFlinkMetrics.class);

  public Histogram bucketFlushHistogram;
  public Histogram deltaBucketFlushHistogram;
  public Histogram compactionHistogram;
  public Histogram bootstrapHistogram;

  private long totalPartitionsWritten;
  private long totalFilesInsert;
  private long totalFilesUpdate;
  private long totalRecordsWritten;
  private long totalUpdateRecordsWritten;
  private long totalInsertRecordsWritten;
  private long totalBytesWritten;
  private long totalScanTime;
  private long totalCreateTime;
  private long totalUpsertTime;
  private long totalCompactedRecordsUpdated;
  private long totalLogFilesCompacted;
  private long totalLogFilesSize;
  private long commitLatencyInMs;
  private long commitFreshnessInMs;
  private long commitEpochTimeInMs;
  private long durationInMs;

  private long rollbackDurationInMs;
  private long rollbackNumFilesDeleted;

  private long cleanDurationInMs;
  private long cleanNumFilesDeleted;

  private long finalizeDurationInMs;
  private long finalizeNumFilesDeleted;

  private final MetricGroup metricGroup;
  private final String actionType;

  public HoodieFlinkMetrics(String actionType, MetricGroup metricGroup) {
    this.metricGroup = metricGroup;
    this.actionType = actionType;

    registerMetric();
  }

  private void registerMetric() {
    // register histogram
    bucketFlushHistogram =  metricGroup.histogram(getMetricsName("histogram", "bucket_flush_cost"), createDropwizardHistogramWrapper());
    deltaBucketFlushHistogram =  metricGroup.histogram(getMetricsName("histogram", "delta_bucket_flush_cost"), createDropwizardHistogramWrapper());
    compactionHistogram =  metricGroup.histogram(getMetricsName("histogram", "compaction_cost"), createDropwizardHistogramWrapper());
    bootstrapHistogram =  metricGroup.histogram(getMetricsName("histogram", "bootstrap_cost"), createDropwizardHistogramWrapper());

    // register commit gauge
    metricGroup.gauge(getMetricsName(actionType, "totalPartitionsWritten"), () -> totalPartitionsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalFilesInsert"), () -> totalFilesInsert);
    metricGroup.gauge(getMetricsName(actionType, "totalFilesUpdate"), () -> totalFilesUpdate);
    metricGroup.gauge(getMetricsName(actionType, "totalRecordsWritten"), () -> totalRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), () -> totalUpdateRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalInsertRecordsWritten"), () -> totalInsertRecordsWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalBytesWritten"), () -> totalBytesWritten);
    metricGroup.gauge(getMetricsName(actionType, "totalScanTime"), () -> totalScanTime);
    metricGroup.gauge(getMetricsName(actionType, "totalCreateTime"), () -> totalCreateTime);
    metricGroup.gauge(getMetricsName(actionType, "totalUpsertTime"), () -> totalUpsertTime);
    metricGroup.gauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), () -> totalCompactedRecordsUpdated);
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesCompacted"), () -> totalLogFilesCompacted);
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesSize"), () -> totalLogFilesSize);
    metricGroup.gauge(getMetricsName(actionType, "commitLatencyInMs"), () -> commitLatencyInMs);
    metricGroup.gauge(getMetricsName(actionType, "commitFreshnessInMs"), () -> commitFreshnessInMs);
    metricGroup.gauge(getMetricsName(actionType, "commitTime"), () -> commitEpochTimeInMs);
    metricGroup.gauge(getMetricsName(actionType, "duration"), () -> durationInMs);

    // register rollback gauge
    metricGroup.gauge(getMetricsName("rollback", "duration"), () -> rollbackDurationInMs);
    metricGroup.gauge(getMetricsName("rollback", "numFilesDeleted"), () -> rollbackNumFilesDeleted);

    // register clean gauge
    metricGroup.gauge(getMetricsName("clean", "duration"), () -> cleanDurationInMs);
    metricGroup.gauge(getMetricsName("clean", "numFilesDeleted"), () -> cleanNumFilesDeleted);

    // register finalize gauge
    metricGroup.gauge(getMetricsName("finalize", "duration"), () -> finalizeDurationInMs);
    metricGroup.gauge(getMetricsName("finalize", "numFilesDeleted"), () -> finalizeNumFilesDeleted);
  }

  String getMetricsName(String action, String metric) {
    return String.format("%s.%s", action, metric);
  }

  public Histogram getBucketFlushHistogram() {
    return bucketFlushHistogram;
  }

  public Histogram getDeltaBucketFlushHistogram() {
    return deltaBucketFlushHistogram;
  }

  public Histogram getCompactionHistogram() {
    return compactionHistogram;
  }

  public Histogram getBootstrapHistogram() {
    return bootstrapHistogram;
  }

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata) {
    updateCommitTimingMetrics(commitEpochTimeInMs, durationInMs, metadata);
    totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
    totalFilesInsert = metadata.fetchTotalFilesInsert();
    totalFilesUpdate = metadata.fetchTotalFilesUpdated();
    totalRecordsWritten = metadata.fetchTotalRecordsWritten();
    totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
    totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
    totalBytesWritten = metadata.fetchTotalBytesWritten();
    totalScanTime = metadata.getTotalScanTime();
    totalCreateTime = metadata.getTotalCreateTime();
    totalUpsertTime = metadata.getTotalUpsertTime();
    totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
    totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
    totalLogFilesSize = metadata.getTotalLogFilesSize();
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata) {
    Pair<Option<Long>, Option<Long>> eventTimePairMinMax = metadata.getMinAndMaxEventTime();
    if (eventTimePairMinMax.getLeft().isPresent()) {
      this.commitLatencyInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getLeft().get();
    }
    if (eventTimePairMinMax.getRight().isPresent()) {
      this.commitFreshnessInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getRight().get();
    }
    this.commitEpochTimeInMs = commitEpochTimeInMs;
    this.durationInMs = durationInMs;
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    LOG.info(
        String.format("Sending rollback metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
    this.rollbackDurationInMs = durationInMs;
    this.rollbackNumFilesDeleted = numFilesDeleted;
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    LOG.info(
        String.format("Sending clean metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
    this.cleanDurationInMs = durationInMs;
    this.cleanNumFilesDeleted = numFilesDeleted;
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    LOG.info(String.format("Sending finalize write metrics (duration=%d, numFilesFinalized=%d)", durationInMs,
        numFilesFinalized));
    this.finalizeDurationInMs = durationInMs;
    this.finalizeNumFilesDeleted = numFilesFinalized;
  }

  private DropwizardHistogramWrapper createDropwizardHistogramWrapper() {
    return new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)));
  }
}
