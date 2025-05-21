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

package org.apache.hudi.client.transaction;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Handles conflict detection for the plans of various table services.
 * Cleaning and archiving are not included in this class as they do not require conflict detection.
 */
public class TableServicePlanConflictDetection {
  private final ConflictResolutionStrategy conflictResolutionStrategy;
  private final HoodieTableMetaClient metaClient;
  private final String lastKnownCompletionTime;

  /**
   * Constructors a new instance to detect conflicts for a table service plan.
   * Note that this should be used within a transaction to prevent race conditions. The meta client used must be constructed or reloaded within the transaction.
   * @param conflictResolutionStrategy The strategy to use for conflict resolution for the writer.
   * @param metaClient The meta client for the table. This must be constructed or reloaded during the transaction.
   * @param lastKnownCompletionTime The last completion time of an instant on the timeline when the planner started.
   */
  public TableServicePlanConflictDetection(ConflictResolutionStrategy conflictResolutionStrategy, HoodieTableMetaClient metaClient, String lastKnownCompletionTime) {
    this.conflictResolutionStrategy = conflictResolutionStrategy;
    this.metaClient = metaClient;
    this.lastKnownCompletionTime = lastKnownCompletionTime;
  }

  /**
   * Checks if there are any potential conflicts with actions that have happened since the planner start instant.
   * The compaction requires a check for conflicts with any commits for ingesting new data since the compaction plan is expected to contain all the log files for the slice it is compacting.
   * It also requires checks for new plans for clustering or potentially another compaction on the timeline to avoid multiple services operating on the same file groups.
   * @param compactionPlan the requested plan
   * @param requestedInstantTime the requested instant time that will be used when persisting the plan to the timeline
   * @return true if there is a conflict with the requested plan, false otherwise
   */
  public boolean hasConflict(HoodieCompactionPlan compactionPlan, String requestedInstantTime) {
    ConcurrentOperation compactionOperation = new ConcurrentOperation(compactionPlan, requestedInstantTime);
    return hasConflict(compactionOperation);
  }

  /**
   * Checks if there are any potential conflicts with actions that have happened since the planner start instant.
   * The clustering requires a check for conflicts with any commits for ingesting new data since the clustering plan will also contain log files,
   * and all the log files for the slice it is clustering must be in the plan.
   * It also requires checks for new plans for compaction or potentially another clustering on the timeline to avoid multiple services operating on the same file groups.
   * @param clusteringPlan the requested plan
   * @param requestedInstantTime the requested instant time that will be used when persisting the plan to the timeline
   * @return true if there is a conflict with the requested plan, false otherwise
   */
  public boolean hasConflict(HoodieClusteringPlan clusteringPlan, String requestedInstantTime) {
    ConcurrentOperation clusteringOperation = new ConcurrentOperation(clusteringPlan, requestedInstantTime);
    return hasConflict(clusteringOperation);
  }

  private boolean hasConflict(ConcurrentOperation plannedOperation) {
    return generateCandidateInstants().anyMatch(instant -> {
      try {
        ConcurrentOperation otherOperation = new ConcurrentOperation(instant, metaClient);
        return conflictResolutionStrategy.hasConflict(plannedOperation, otherOperation);
      } catch (IOException ex) {
        throw new HoodieIOException("Unable to parse instant details for conflict detection", ex);
      }
    });
  }

  private Stream<HoodieInstant> generateCandidateInstants() {
    Stream<HoodieInstant> commitsCompletedDuringPlanning = metaClient
        .getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsModifiedAfterByCompletionTime(lastKnownCompletionTime)
        .getInstantsAsStream();
    Stream<HoodieInstant> newClusteringPlans = metaClient
        .getActiveTimeline()
        .filterPendingClusteringTimeline()
        .getInstantsAsStream();
    Stream<HoodieInstant> newCompactionPlans = metaClient
        .getActiveTimeline()
        .filterPendingCompactionTimeline()
        .getInstantsAsStream();
    return Stream.concat(Stream.concat(newClusteringPlans, newCompactionPlans), commitsCompletedDuringPlanning);
  }
}
