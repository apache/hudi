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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TimelineUtils provides a common way to query incremental meta-data changes for a hoodie table.
 *
 * This is useful in multiple places including:
 * 1) HiveSync - this can be used to query partitions that changed since previous sync.
 * 2) Incremental reads - InputFormats can use this API to query
 */
public class TimelineUtils {

  /**
   * Returns partitions that have new data strictly after commitTime.
   * Does not include internal operations such as clean in the timeline.
   */
  public static List<String> getPartitionsWritten(HoodieTimeline timeline) {
    HoodieTimeline timelineToSync = timeline.getCommitsAndCompactionTimeline();
    return getAffectedPartitions(timelineToSync);
  }

  /**
   * Returns partitions that have been modified including internal operations such as clean in the passed timeline.
   */
  public static List<String> getAffectedPartitions(HoodieTimeline timeline) {
    return timeline.filterCompletedInstants().getInstants().flatMap(s -> {
      switch (s.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(s).get(), HoodieCommitMetadata.class);
            return commitMetadata.getPartitionToWriteStats().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions written at " + s, e);
          }
        case HoodieTimeline.CLEAN_ACTION:
          try {
            HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(s).get());
            return cleanMetadata.getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions cleaned at " + s, e);
          }
        case HoodieTimeline.ROLLBACK_ACTION:
          try {
            return TimelineMetadataUtils.deserializeHoodieRollbackMetadata(timeline.getInstantDetails(s).get()).getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions rolledback at " + s, e);
          }
        case HoodieTimeline.RESTORE_ACTION:
          try {
            HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeAvroMetadata(timeline.getInstantDetails(s).get(), HoodieRestoreMetadata.class);
            return restoreMetadata.getHoodieRestoreMetadata().values().stream()
                .flatMap(Collection::stream)
                .flatMap(rollbackMetadata -> rollbackMetadata.getPartitionMetadata().keySet().stream());
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions restored at " + s, e);
          }
        case HoodieTimeline.SAVEPOINT_ACTION:
          try {
            return TimelineMetadataUtils.deserializeHoodieSavepointMetadata(timeline.getInstantDetails(s).get()).getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions savepoint at " + s, e);
          }
        case HoodieTimeline.COMPACTION_ACTION:
          // compaction is not a completed instant.  So no need to consider this action.
          return Stream.empty();
        default:
          throw new HoodieIOException("unknown action in timeline " + s.getAction());
      }

    }).distinct().filter(s -> !s.isEmpty()).collect(Collectors.toList());
  }
}
