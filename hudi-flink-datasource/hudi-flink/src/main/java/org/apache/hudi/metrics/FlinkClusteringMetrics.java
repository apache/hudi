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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;

/**
 * Metrics for flink clustering.
 */
public class FlinkClusteringMetrics extends FlinkWriteMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkClusteringMetrics.class);

  /**
   * Key for clustering timer.
   */
  private static final String CLUSTERING_KEY = "clustering";

  /**
   * Number of pending clustering instants.
   *
   * @see ClusteringPlanOperator
   */
  private long pendingClusteringCount;

  /**
   * Duration between the earliest pending clustering instant time and now in seconds.
   *
   *  @see ClusteringPlanOperator
   */
  private long clusteringDelay;

  /**
   * Cost for consuming a clustering operation in milliseconds.
   *
   * @see ClusteringOperator
   */
  private long clusteringCost;

  public FlinkClusteringMetrics(MetricGroup metricGroup) {
    super(metricGroup, CLUSTERING_KEY);
  }

  @Override
  public void registerMetrics() {
    super.registerMetrics();
    metricGroup.gauge(getMetricsName(actionType, "pendingClusteringCount"), () -> pendingClusteringCount);
    metricGroup.gauge(getMetricsName(actionType, "clusteringDelay"), () -> clusteringDelay);
    metricGroup.gauge(getMetricsName(actionType, "clusteringCost"), () -> clusteringCost);
  }

  public void setPendingClusteringCount(long pendingClusteringCount) {
    this.pendingClusteringCount = pendingClusteringCount;
  }

  public void setFirstPendingClusteringInstant(Option<HoodieInstant> firstPendingClusteringInstant) {
    try {
      if (!firstPendingClusteringInstant.isPresent()) {
        this.clusteringDelay = 0L;
      } else {
        Instant start = HoodieInstantTimeGenerator.parseDateFromInstantTime((firstPendingClusteringInstant.get()).requestedTime()).toInstant();
        this.clusteringDelay = Duration.between(start, Instant.now()).getSeconds();
      }
    } catch (ParseException e) {
      LOG.warn("Invalid input clustering instant: {}", firstPendingClusteringInstant);
    }
  }

  public void startClustering() {
    startTimer(CLUSTERING_KEY);
  }

  public void endClustering() {
    this.clusteringCost = stopTimer(CLUSTERING_KEY);
  }

}
