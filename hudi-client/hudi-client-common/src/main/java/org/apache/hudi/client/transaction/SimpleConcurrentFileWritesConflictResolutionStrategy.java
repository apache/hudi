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

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.WriteOperationType.CLUSTER;
import static org.apache.hudi.common.model.WriteOperationType.LOG_COMPACT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * This class is a basic implementation of a conflict resolution strategy for concurrent writes {@link ConflictResolutionStrategy}
 * based on start_timestamp, and need to record the pending instants before write.
 */
public class SimpleConcurrentFileWritesConflictResolutionStrategy
    implements ConflictResolutionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleConcurrentFileWritesConflictResolutionStrategy.class);

  private final HoodieWriteConfig writeConfig;

  public SimpleConcurrentFileWritesConflictResolutionStrategy(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<Set<String>> pendingInstantsBeforeWritten) {
    // Divide the process of obtaining candidate instants into two steps:
    //   1) get completed instants after current instant starts
    //   2) pick from instants that are still pending
    return Stream.concat(getCompletedInstantsAfterCurrent(metaClient, currentInstant, pendingInstantsBeforeWritten),
        pickFromCurrentPendingInstants(metaClient, currentInstant));
  }

  /**
   * Get completed instants after current instant starts based on start_timestamp:
   *   1) pick start_timestamp > current start_timestamp && have completed instants
   *   2) pending instants that were recorded before write, and have been converted to completed
   *
   * @param metaClient meta client
   * @param currentInstant current instant
   * @param pendingInstantsBeforeWrite pending instant recorded before write
   * @return selected {@link HoodieInstant} stream
   */
  public Stream<HoodieInstant> getCompletedInstantsAfterCurrent(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                                Option<Set<String>> pendingInstantsBeforeWrite) {
    return metaClient.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
        .filterCompletedInstants()
        .filter(i -> compareTimestamps(i.getTimestamp(), GREATER_THAN, currentInstant.getTimestamp())
            || (pendingInstantsBeforeWrite.isPresent() && pendingInstantsBeforeWrite.get().contains(i.getTimestamp())))
        .getInstantsAsStream();
  }

  /**
   * Pick from instants that are still pending, need to compare the priority of the two operations.
   *
   * @param metaClient meta client
   * @param currentInstant current instant
   * @return selected {@link HoodieInstant} stream
   */
  public Stream<HoodieInstant> pickFromCurrentPendingInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant) {

    // Whether to pick the pending instant to candidate instants:
    // +-----------------+----------------------------------------+---------------------------------------+------------+
    // | current\pending | ingestion                              |  clustering                           | compaction |
    // |-----------------+----------------------------------------+---------------------------------------+------------+
    // | ingestion       | no                                     | no if #isIngestionPrimaryClustering() | yes        |
    // |-----------------+----------------------------------------+---------------------------------------+------------+
    // | clustering      | yes if #isIngestionPrimaryClustering() | no                                    | no         |
    // +-----------------+----------------------------------------+---------------------------------------+------------+

    Set<String> actionsToPick = new HashSet<>();

    if (REPLACE_COMMIT_ACTION.equals(currentInstant.getAction())
        && ClusteringUtils.isPendingClusteringInstant(metaClient, currentInstant)) {
      if (isIngestionPrimaryClustering()) {
        actionsToPick.add(COMMIT_ACTION);
        actionsToPick.add(DELTA_COMMIT_ACTION);
      }
    } else {
      if (!isIngestionPrimaryClustering()) {
        actionsToPick.add(REPLACE_COMMIT_ACTION);
      }
      actionsToPick.add(COMPACTION_ACTION);
    }

    Set<String> tableServices = Stream.of(REPLACE_COMMIT_ACTION, COMPACTION_ACTION).collect(Collectors.toSet());

    return metaClient.getActiveTimeline()
        .getTimelineOfActions(actionsToPick)
        .filterInflightsAndRequested()
        // If target is table service, we can exclude the plan scheduled before current start,
        // because conflict has been resolved at writing
        .filter(i -> !tableServices.contains(i.getAction())
            || compareTimestamps(i.getTimestamp(), GREATER_THAN, currentInstant.getTimestamp()))
        .getInstantsAsStream();
  }

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // TODO : UUID's can clash even for insert/insert, handle that case.
    Set<Pair<String, String>> partitionAndFileIdsSetForFirstInstant = thisOperation.getMutatedPartitionAndFileIds();
    Set<Pair<String, String>> partitionAndFileIdsSetForSecondInstant = otherOperation.getMutatedPartitionAndFileIds();

    if (HoodieIndex.IndexType.BUCKET.name().equalsIgnoreCase(writeConfig.getIndexType().name())) {
      partitionAndFileIdsSetForFirstInstant = partitionAndFileIdsSetForFirstInstant
          .stream()
          .map(x -> Pair.of(x.getLeft(), BucketIdentifier.bucketIdStrFromFileId(x.getRight())))
          .collect(Collectors.toSet());
      partitionAndFileIdsSetForSecondInstant = partitionAndFileIdsSetForSecondInstant
          .stream()
          .map(x -> Pair.of(x.getLeft(), BucketIdentifier.bucketIdStrFromFileId(x.getRight())))
          .collect(Collectors.toSet());
    }

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
  public void resolveConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // A completed COMPACTION action eventually shows up as a COMMIT action on the timeline.
    // We need to ensure we handle this during conflict resolution and not treat the commit from a
    // compaction operation as a regular commit. Regular commits & deltacommits are candidates for conflict.
    // Since the REPLACE action with CLUSTER operation does not support concurrent updates, we have
    // to consider it as conflict if we see overlapping file ids. Once concurrent updates are
    // supported for CLUSTER (https://issues.apache.org/jira/browse/HUDI-1042),
    // add that to the below check so that concurrent updates do not conflict.
    if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
      if (HoodieTimeline.compareTimestamps(otherOperation.getInstantTimestamp(), HoodieTimeline.LESSER_THAN, thisOperation.getInstantTimestamp())) {
        return;
      }
    } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
      // Since log compaction is a rewrite operation, it can be committed along with other delta commits.
      // The ordering of the commits is taken care by AbstractHoodieLogRecordReader scan method.
      // Conflict arises only if the log compaction commit has a lesser timestamp compared to compaction commit.
      return;
    }
    // just abort the current write if conflicts are found
    throw new HoodieWriteConflictException(new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes"));
  }

  @Override
  public boolean isPendingInstantsBeforeWriteRequired() {
    return true;
  }

  public boolean isConflictResolveRequired(WriteOperationType operationType) {
    if (operationType == null) {
      return false;
    }

    if (WriteOperationType.isDataChange(operationType)) {
      return true;
    } else if (operationType.equals(CLUSTER)) {
      return isIngestionPrimaryClustering();
    } else {
      return operationType.equals(LOG_COMPACT);
    }
  }

  /**
   * Whether ingestion takes precedence over clustering
   */
  public boolean isIngestionPrimaryClustering() {
    return writeConfig.getClusteringUpdatesStrategyClass()
        .equals("org.apache.hudi.client.clustering.update.strategy.SparkAllowUpdateStrategy");
  }
}
