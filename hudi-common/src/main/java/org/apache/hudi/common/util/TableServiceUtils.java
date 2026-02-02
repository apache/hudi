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

package org.apache.hudi.common.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for table service operations (compaction, clustering, log compaction).
 */
public class TableServiceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableServiceUtils.class);

  /**
   * Find a stale inflight instant that has exceeded the max processing time.
   * This is used to detect failed table service jobs that should be retried.
   *
   * <p>Supports the following action types:
   * <ul>
   *   <li>{@link HoodieTimeline#COMPACTION_ACTION}</li>
   *   <li>{@link HoodieTimeline#CLUSTERING_ACTION}</li>
   *   <li>{@link HoodieTimeline#LOG_COMPACTION_ACTION}</li>
   * </ul>
   *
   * @param metaClient          the table meta client
   * @param actionType          the action type (compaction, clustering, logcompaction)
   * @param maxProcessingTimeMs max time in milliseconds before considering an instant stale.
   *                            If &lt;= 0, the check is disabled and empty is returned.
   * @return the first stale inflight instant found, or empty if none
   */
  public static Option<HoodieInstant> findStaleInflightInstant(
      HoodieTableMetaClient metaClient,
      String actionType,
      long maxProcessingTimeMs) {
    if (maxProcessingTimeMs <= 0) {
      return Option.empty();
    }

    List<HoodieInstant> inflightInstants = getInflightInstants(metaClient, actionType);
    return findFirstStaleInstant(inflightInstants, maxProcessingTimeMs);
  }

  /**
   * Get all inflight instants for the specified action type.
   *
   * @param metaClient the table meta client
   * @param actionType the action type
   * @return list of inflight instants
   */
  private static List<HoodieInstant> getInflightInstants(HoodieTableMetaClient metaClient, String actionType) {
    HoodieTimeline pendingTimeline = getPendingTimeline(metaClient, actionType);

    switch (actionType) {
      case HoodieTimeline.COMPACTION_ACTION:
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        // For compaction and log compaction, filter inflight instants directly
        return pendingTimeline.getInstants().stream()
            .filter(instant -> instant.getState() == HoodieInstant.State.INFLIGHT)
            .collect(Collectors.toList());

      case HoodieTimeline.CLUSTERING_ACTION:
        // For clustering, use ClusteringUtils to properly resolve inflight instants
        // (handles both CLUSTERING_ACTION and legacy REPLACE_COMMIT_ACTION)
        return pendingTimeline.getInstants().stream()
            .map(instant -> ClusteringUtils.getInflightClusteringInstant(
                instant.requestedTime(),
                metaClient.getActiveTimeline(),
                metaClient.getInstantGenerator()))
            .filter(Option::isPresent)
            .map(Option::get)
            .collect(Collectors.toList());

      default:
        throw new IllegalArgumentException("Unsupported action type for stale instant detection: " + actionType);
    }
  }

  /**
   * Get the pending timeline for the specified action type.
   *
   * @param metaClient the table meta client
   * @param actionType the action type
   * @return the pending timeline
   */
  private static HoodieTimeline getPendingTimeline(HoodieTableMetaClient metaClient, String actionType) {
    switch (actionType) {
      case HoodieTimeline.COMPACTION_ACTION:
        return metaClient.getActiveTimeline().filterPendingCompactionTimeline();
      case HoodieTimeline.CLUSTERING_ACTION:
        return metaClient.getActiveTimeline().filterPendingReplaceOrClusteringTimeline();
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        return metaClient.getActiveTimeline().filterPendingLogCompactionTimeline();
      default:
        throw new IllegalArgumentException("Unsupported action type: " + actionType);
    }
  }

  /**
   * Find the first instant that has exceeded the max processing time.
   *
   * @param inflightInstants    list of inflight instants to check
   * @param maxProcessingTimeMs max processing time threshold in milliseconds
   * @return the first stale instant found, or empty if none
   */
  private static Option<HoodieInstant> findFirstStaleInstant(
      List<HoodieInstant> inflightInstants,
      long maxProcessingTimeMs) {
    long currentTime = System.currentTimeMillis();

    for (HoodieInstant instant : inflightInstants) {
      String requestedTime = instant.requestedTime();
      if (requestedTime == null) {
        continue;
      }

      Option<Date> startTimeOpt = TimelineUtils.parseDateFromInstantTimeSafely(requestedTime);
      if (startTimeOpt.isPresent() && isStale(startTimeOpt.get().getTime(), maxProcessingTimeMs, currentTime)) {
        return Option.of(instant);
      }
    }
    return Option.empty();
  }

  /**
   * Check if an instant start time is stale based on max processing time.
   *
   * @param startTimeMs         the instant start time in milliseconds
   * @param maxProcessingTimeMs the max processing time threshold in milliseconds
   * @param currentTimeMs       the current time in milliseconds
   * @return true if the instant is stale, false otherwise
   */
  public static boolean isStale(long startTimeMs, long maxProcessingTimeMs, long currentTimeMs) {
    return startTimeMs + maxProcessingTimeMs < currentTimeMs;
  }

  /**
   * Check if an instant start time is stale based on max processing time, using current system time.
   *
   * @param startTimeMs         the instant start time in milliseconds
   * @param maxProcessingTimeMs the max processing time threshold in milliseconds
   * @return true if the instant is stale, false otherwise
   */
  public static boolean isStale(long startTimeMs, long maxProcessingTimeMs) {
    return isStale(startTimeMs, maxProcessingTimeMs, System.currentTimeMillis());
  }
}
