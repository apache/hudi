/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import org.apache.flink.metrics.MetricGroup;

import java.util.Set;

public class FlinkTimeLineMetrics extends HoodieFlinkMetrics {

  public static final String EARLIEST_INFLIGHT_CLUSTERING_INSTANT = "earliest_inflight_clustering_instant";
  public static final String LATEST_COMPLETED_CLUSTERING_INSTANT = "latest_completed_clustering_instant";
  public static final String PENDING_CLUSTERING_COUNT = "clustering_pendingClusteringCount";

  private long earliestInflightClusteringInstant;
  private long latestCompletedClusteringInstant;
  private long clusteringPendingClusteringCount;

  private static final String CLUSTERTING_ACTION = "replacecommit";

  public FlinkTimeLineMetrics(MetricGroup metricGroup) {
    super(metricGroup);
  }

  public void registerMetrics() {
    if (metricGroup == null) {
      return;
    }
    metricGroup.gauge(EARLIEST_INFLIGHT_CLUSTERING_INSTANT, () -> earliestInflightClusteringInstant);
    metricGroup.gauge(LATEST_COMPLETED_CLUSTERING_INSTANT, () -> latestCompletedClusteringInstant);
    metricGroup.gauge(PENDING_CLUSTERING_COUNT, () -> clusteringPendingClusteringCount);
  }

  public void updateTimeLineMetrics(HoodieActiveTimeline activeTimeline) {
    if (metricGroup == null) {
      return;
    }
    getAndUpdateEarliestInflightActionAndCountPendingMetric(activeTimeline);
    getAndUpdateLatestCompletedActionMetric(activeTimeline);
  }

  public void getAndUpdateEarliestInflightActionAndCountPendingMetric(HoodieActiveTimeline activeTimeline) {
    Set<String> validActions = CollectionUtils.createSet(CLUSTERTING_ACTION);
    HoodieTimeline filteredInstant = activeTimeline.filterInflightsAndRequested()
        .filter(instant -> validActions.contains(instant.getAction()));
    clusteringPendingClusteringCount = Long.valueOf(filteredInstant.countInstants());
    Option<HoodieInstant> hoodieInstantOption = filteredInstant.firstInstant();
    if (hoodieInstantOption.isPresent()) {
      earliestInflightClusteringInstant = Long.valueOf(hoodieInstantOption.get().requestedTime());
    }
  }

  public void getAndUpdateLatestCompletedActionMetric(HoodieActiveTimeline activeTimeline) {
    Set<String> validActions = CollectionUtils.createSet(CLUSTERTING_ACTION);
    HoodieTimeline filteredInstant = activeTimeline.filterCompletedInstants()
        .filter(instant -> validActions.contains(instant.getAction()));
    Option<HoodieInstant> hoodieInstantOption = filteredInstant.lastInstant();
    if (hoodieInstantOption.isPresent()) {
      latestCompletedClusteringInstant = Long.valueOf(hoodieInstantOption.get().requestedTime());
    }
  }
}
