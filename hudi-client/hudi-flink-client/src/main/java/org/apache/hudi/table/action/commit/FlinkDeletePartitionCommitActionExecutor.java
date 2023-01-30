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

package org.apache.hudi.table.action.commit;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDeletePartitionException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class FlinkDeletePartitionCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends FlinkInsertOverwriteCommitActionExecutor<T> {

  private final List<String> partitions;

  public FlinkDeletePartitionCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config,
                                                  HoodieTable<?, ?, ?, ?> table,
                                                  String instantTime,
                                                  List<String> partitions) {
    super(context, null, config, table, instantTime, null, WriteOperationType.DELETE_PARTITION);
    this.partitions = partitions;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    checkPreconditions();

    try {
      HoodieTimer timer = new HoodieTimer().startTimer();
      context.setJobStatus(this.getClass().getSimpleName(), "Gather all file ids from all deleting partitions.");
      Map<String, List<String>> partitionToReplaceFileIds =
          context.parallelize(partitions).distinct().collectAsList()
              .stream().collect(Collectors.toMap(partitionPath -> partitionPath, this::getAllExistingFileIds));
      HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();
      result.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
      result.setIndexUpdateDuration(Duration.ofMillis(timer.endTimer()));
      result.setWriteStatuses(Collections.emptyList());

      // created requested
      HoodieInstant dropPartitionsInstant = new HoodieInstant(REQUESTED, REPLACE_COMMIT_ACTION, instantTime);
      if (!table.getMetaClient().getFs().exists(new Path(table.getMetaClient().getMetaPath(),
          dropPartitionsInstant.getFileName()))) {
        HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
            .setOperationType(WriteOperationType.DELETE_PARTITION.name())
            .setExtraMetadata(extraMetadata.orElse(Collections.emptyMap()))
            .build();
        table.getMetaClient().getActiveTimeline().saveToPendingReplaceCommit(dropPartitionsInstant,
            TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
      }

      this.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())),
          instantTime);
      this.commitOnAutoCommit(result);
      return result;
    } catch (Exception e) {
      throw new HoodieDeletePartitionException("Failed to drop partitions for commit time " + instantTime, e);
    }
  }

  private List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }

  /**
   * Check if there are any pending table service actions (requested + inflight) on a table affecting the partitions to
   * be dropped.
   * <p>
   * This check is to prevent a drop-partition from proceeding should a partition have a table service action in
   * the pending stage. If this is allowed to happen, the filegroup that is an input for a table service action, might
   * also be a candidate for being replaced. As such, when the table service action and drop-partition commits are
   * committed, there will be two commits replacing a single filegroup.
   * <p>
   * For example, a timeline might have an execution order as such:
   * 000.replacecommit.requested (clustering filegroup_1 + filegroup_2 -> filegroup_3)
   * 001.replacecommit.requested, 001.replacecommit.inflight, 0001.replacecommit (drop_partition to replace filegroup_1)
   * 000.replacecommit.inflight (clustering is executed now)
   * 000.replacecommit (clustering completed)
   * For an execution order as shown above, 000.replacecommit and 001.replacecommit will both flag filegroup_1 to be replaced.
   * This will cause downstream duplicate key errors when a map is being constructed.
   */
  private void checkPreconditions() {
    List<String> instantsOfOffendingPendingTableServiceAction = new ArrayList<>();
    // ensure that there are no pending inflight clustering/compaction operations involving this partition
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();

    // separating the iteration of pending compaction operations from clustering as they return different stream types
    Stream.concat(fileSystemView.getPendingCompactionOperations(), fileSystemView.getPendingLogCompactionOperations())
        .filter(op -> partitions.contains(op.getRight().getPartitionPath()))
        .forEach(op -> instantsOfOffendingPendingTableServiceAction.add(op.getLeft()));

    fileSystemView.getFileGroupsInPendingClustering()
        .filter(fgIdInstantPair -> partitions.contains(fgIdInstantPair.getLeft().getPartitionPath()))
        .forEach(x -> instantsOfOffendingPendingTableServiceAction.add(x.getRight().getTimestamp()));

    if (instantsOfOffendingPendingTableServiceAction.size() > 0) {
      throw new HoodieDeletePartitionException("Failed to drop partitions. "
          + "Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: " + partitions + ". "
          + "Instant(s) of offending pending table service action: "
          + instantsOfOffendingPendingTableServiceAction.stream().distinct().collect(Collectors.toList()));
    }
  }
}
