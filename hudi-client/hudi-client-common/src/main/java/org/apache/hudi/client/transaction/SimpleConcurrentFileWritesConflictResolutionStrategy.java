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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * This class is a basic implementation of a conflict resolution strategy for concurrent writes {@link ConflictResolutionStrategy}.
 */
public class SimpleConcurrentFileWritesConflictResolutionStrategy
    implements ConflictResolutionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleConcurrentFileWritesConflictResolutionStrategy.class);

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant) {
    if (metaClient.getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      return getCandidateInstantsV8AndAbove(metaClient, currentInstant, lastSuccessfulInstant);
    } else {
      return getCandidateInstantsPreV8(metaClient, currentInstant, lastSuccessfulInstant);
    }
  }

  /**
   * To find which instants are conflicting for table versions 8 and above, we apply the following logic:
   * <ul>
   *   <li>Get completed instants timeline only for commits that have happened since the last successful write.</li>
   *   <li>Get any completed replace commit that happened since the last successful write and any pending replace commit.</li>
   * </ul>
   * @param metaClient table meta client
   * @param currentInstant the instant for the write this client is attempting to commit
   * @param lastSuccessfulInstant the last successful write before this commit started
   * @return a stream of instants that are candidates for conflict resolution
   */
  private Stream<HoodieInstant> getCandidateInstantsV8AndAbove(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                               Option<HoodieInstant> lastSuccessfulInstant) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    boolean isMoRTable = metaClient.getTableType() == HoodieTableType.MERGE_ON_READ;
    Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
        .getCommitsTimeline()
        .filterCompletedInstants()
        .filter(instant -> !isMoRTable || !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
        .findInstantsAfter(lastSuccessfulInstant.isPresent() ? lastSuccessfulInstant.get().requestedTime() : HoodieTimeline.INIT_INSTANT_TS)
        .getInstantsAsStream();

    Stream<HoodieInstant> clusteringAndReplaceCommitInstants = activeTimeline
        .filterPendingReplaceOrClusteringTimeline()
        .filter(instant -> isClusteringOrRecentlyRequestedInstant(activeTimeline, metaClient, currentInstant).test(instant))
        .getInstantsAsStream();

    return Stream.concat(completedCommitsInstantStream, clusteringAndReplaceCommitInstants);
  }

  /**
   * To find which instants are conflicting for table versions below 8, we apply the following logic:
   * <ul>
   *   <li>Get completed instants timeline only for commits that have happened since the last successful write.</li>
   *   <li>Get any scheduled or completed compaction that have started and/or finished after the current instant.</li>
   *   <li>Get any completed replace commit that happened since the last successful write and any pending replace commit.</li>
   * </ul>
   * @param metaClient table meta client
   * @param currentInstant the instant for the write this client is attempting to commit
   * @param lastSuccessfulInstant the last successful write before this commit started
   * @return a stream of instants that are candidates for conflict resolution
   */
  private Stream<HoodieInstant> getCandidateInstantsPreV8(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                          Option<HoodieInstant> lastSuccessfulInstant) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    Stream<HoodieInstant> completedCommitsInstantStream = getCommitsCompletedSinceLastCommit(lastSuccessfulInstant, activeTimeline);

    Stream<HoodieInstant> compactionAndClusteringPendingTimeline = activeTimeline
        .filterPendingReplaceClusteringAndCompactionTimeline()
        .filter(instant -> isClusteringOrRecentlyRequestedInstant(activeTimeline, metaClient, currentInstant).test(instant))
        .getInstantsAsStream();
    return Stream.concat(completedCommitsInstantStream, compactionAndClusteringPendingTimeline);
  }

  private static Stream<HoodieInstant> getCommitsCompletedSinceLastCommit(Option<HoodieInstant> lastSuccessfulInstant, HoodieActiveTimeline activeTimeline) {
    return activeTimeline
        .getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsAfter(lastSuccessfulInstant.map(HoodieInstant::requestedTime).orElse(HoodieTimeline.INIT_INSTANT_TS))
        .getInstantsAsStream();
  }

  private Predicate<HoodieInstant> isClusteringOrRecentlyRequestedInstant(HoodieActiveTimeline activeTimeline, HoodieTableMetaClient metaClient, HoodieInstant currentInstant) {
    return instant -> ClusteringUtils.isClusteringInstant(activeTimeline, instant, metaClient.getInstantGenerator())
        || (!HoodieTimeline.CLUSTERING_ACTION.equals(instant.getAction()) && compareTimestamps(instant.requestedTime(), GREATER_THAN, currentInstant.requestedTime()));
  }

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // TODO : UUID's can clash even for insert/insert, handle that case.
    Set<Pair<String, String>> partitionAndFileIdsSetForFirstInstant = thisOperation.getMutatedPartitionAndFileIds();
    Set<Pair<String, String>> partitionAndFileIdsSetForSecondInstant = otherOperation.getMutatedPartitionAndFileIds();
    Set<Pair<String, String>> intersection = new HashSet<>(partitionAndFileIdsSetForFirstInstant);
    intersection.retainAll(partitionAndFileIdsSetForSecondInstant);
    if (!intersection.isEmpty()) {
      LOG.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting file ids " + intersection);
      return true;
    }
    return false;
  }

  @Override
  public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
      ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // A completed COMPACTION action eventually shows up as a COMMIT action on the timeline.
    // We need to ensure we handle this during conflict resolution and not treat the commit from a
    // compaction operation as a regular commit. Regular commits & deltacommits are candidates for conflict.
    // Since the REPLACE action with CLUSTER operation does not support concurrent updates, we have
    // to consider it as conflict if we see overlapping file ids. Once concurrent updates are
    // supported for CLUSTER (https://issues.apache.org/jira/browse/HUDI-1042),
    // add that to the below check so that concurrent updates do not conflict.
    if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
      if (compareTimestamps(otherOperation.getInstantTimestamp(), LESSER_THAN, thisOperation.getInstantTimestamp())) {
        return thisOperation.getCommitMetadataOption();
      }
    } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
      // Since log compaction is a rewrite operation, it can be committed along with other delta commits.
      // The ordering of the commits is taken care by AbstractHoodieLogRecordReader scan method.
      // Conflict arises only if the log compaction commit has a lesser timestamp compared to compaction commit.
      return thisOperation.getCommitMetadataOption();
    }
    // just abort the current write if conflicts are found
    throw new HoodieWriteConflictException(new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes between first operation = " + thisOperation
        + ", second operation = " + otherOperation));
  }

  @Override
  public boolean isPreCommitRequired() {
    return false;
  }

}
