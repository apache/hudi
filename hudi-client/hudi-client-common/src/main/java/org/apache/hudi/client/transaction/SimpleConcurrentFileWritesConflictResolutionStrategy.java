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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * This class is a basic implementation of a conflict resolution strategy for concurrent writes {@link ConflictResolutionStrategy}.
 */
public class SimpleConcurrentFileWritesConflictResolutionStrategy
    implements ConflictResolutionStrategy {

  private static final Logger LOG = LogManager.getLogger(SimpleConcurrentFileWritesConflictResolutionStrategy.class);

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant,
                                                 Option<HoodieInstant> lastSuccessfulInstant) {

    // To find which instants are conflicting, we apply the following logic
    // 1. Get completed instants timeline only for commits that have happened since the last successful write.
    // 2. Get any scheduled or completed compaction or clustering operations that have started and/or finished
    // after the current instant. We need to check for write conflicts since they may have mutated the same files
    // that are being newly created by the current write.
    Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
        .getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsAfter(lastSuccessfulInstant.isPresent() ? lastSuccessfulInstant.get().getTimestamp() : HoodieTimeline.INIT_INSTANT_TS)
        .getInstants();

    Stream<HoodieInstant> compactionAndClusteringPendingTimeline = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(REPLACE_COMMIT_ACTION, COMPACTION_ACTION))
        .findInstantsAfter(currentInstant.getTimestamp())
        .filterInflightsAndRequested()
        .getInstants();
    return Stream.concat(completedCommitsInstantStream, compactionAndClusteringPendingTimeline);
  }

  @Override
  public boolean hasMarkerConflict(WriteMarkers writeMarkers, HoodieWriteConfig config, FileSystem fs, String partitionPath, String dataFileName) {
    return false;
  }

  @Override
  public boolean hasCommitConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // TODO : UUID's can clash even for insert/insert, handle that case.
    Set<String> fileIdsSetForFirstInstant = thisOperation.getMutatedFileIds();
    Set<String> fileIdsSetForSecondInstant = otherOperation.getMutatedFileIds();
    Set<String> intersection = new HashSet<>(fileIdsSetForFirstInstant);
    intersection.retainAll(fileIdsSetForSecondInstant);
    if (!intersection.isEmpty()) {
      LOG.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting file ids " + intersection);
      return true;
    }
    return false;
  }

  @Override
  public Option<HoodieCommitMetadata> resolveCommitConflict(HoodieTable table,
      ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // A completed COMPACTION action eventually shows up as a COMMIT action on the timeline.
    // We need to ensure we handle this during conflict resolution and not treat the commit from a
    // compaction operation as a regular commit. Regular commits & deltacommits are candidates for conflict.
    // Since the REPLACE action with CLUSTER operation does not support concurrent updates, we have
    // to consider it as conflict if we see overlapping file ids. Once concurrent updates are
    // supported for CLUSTER (https://issues.apache.org/jira/browse/HUDI-1042),
    // add that to the below check so that concurrent updates do not conflict.
    if (otherOperation.getOperationType() == WriteOperationType.COMPACT
        && HoodieTimeline.compareTimestamps(otherOperation.getInstantTimestamp(), HoodieTimeline.LESSER_THAN, thisOperation.getInstantTimestamp())) {
      return thisOperation.getCommitMetadataOption();
    }
    // just abort the current write if conflicts are found
    throw new HoodieWriteConflictException(new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes"));
  }

  @Override
  public Option<HoodieCommitMetadata> resolveMarkerConflict(WriteMarkers writeMarkers, String partitionPath, String dataFileName) throws HoodieWriteConflictException {
    throw new HoodieWriteConflictException(new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes"));
  }

}
