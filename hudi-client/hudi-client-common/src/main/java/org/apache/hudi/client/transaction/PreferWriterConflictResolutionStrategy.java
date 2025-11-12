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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * This class extends the base implementation of conflict resolution strategy.
 * It gives preference to non-blocking ingestion over table services in case of conflicts.
 */
public class PreferWriterConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PreferWriterConflictResolutionStrategy.class);

  /**
   * For tableservices like replacecommit and compaction commits this method also returns ingestion inflight commits.
   */
  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant) {
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    if (ClusteringUtils.isClusteringInstant(activeTimeline, currentInstant, metaClient.getInstantGenerator())
        || COMPACTION_ACTION.equals(currentInstant.getAction())) {
      return getCandidateInstantsForTableServicesCommits(activeTimeline, currentInstant);
    } else {
      return getCandidateInstantsForNonTableServicesCommits(activeTimeline, currentInstant);
    }
  }

  private Stream<HoodieInstant> getCandidateInstantsForNonTableServicesCommits(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant) {

    // To find out which instants are conflicting, we apply the following logic
    // Get all the completed instants timeline only for commits that have happened
    // since the last successful write based on the transition times.
    // We need to check for write conflicts since they may have mutated the same files
    // that are being newly created by the current write.
    List<HoodieInstant> completedCommitsInstants = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
        .filterCompletedInstants()
        .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
        .getInstantsOrderedByCompletionTime()
        .collect(Collectors.toList());
    LOG.info("Instants that may have conflict with {} are {}", currentInstant, completedCommitsInstants);
    return completedCommitsInstants.stream();
  }

  /**
   * To find which instants are conflicting, we apply the following logic
   * Get both completed instants and ingestion inflight commits that have happened since the last successful write.
   * We need to check for write conflicts since they may have mutated the same files
   * that are being newly created by the current write.
   */
  private Stream<HoodieInstant> getCandidateInstantsForTableServicesCommits(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant) {
    // Fetch list of completed commits.
    Stream<HoodieInstant> completedCommitsStream =
        activeTimeline
            .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
            .filterCompletedInstants()
            .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
            .getInstantsAsStream();

    // Fetch list of ingestion inflight commits.
    Stream<HoodieInstant> inflightIngestionCommitsStream =
        activeTimeline
            .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION))
            .filterInflights()
            .getInstantsAsStream();

    // Merge and sort the instants and return.
    List<HoodieInstant> instantsToConsider = Stream.concat(completedCommitsStream, inflightIngestionCommitsStream)
        .sorted(Comparator.comparing(o -> o.getCompletionTime()))
        .collect(Collectors.toList());
    LOG.info("Instants that may have conflict with {} are {}", currentInstant, instantsToConsider);
    return instantsToConsider.stream();
  }

  @Override
  public boolean isPreCommitRequired() {
    return true;
  }
}
