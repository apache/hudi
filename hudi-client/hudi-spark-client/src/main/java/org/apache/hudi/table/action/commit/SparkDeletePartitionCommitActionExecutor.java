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

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.exception.HoodieDeletePartitionException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.Path;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class SparkDeletePartitionCommitActionExecutor<T>
    extends SparkInsertOverwriteCommitActionExecutor<T> {

  private List<String> partitions;
  public SparkDeletePartitionCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config, HoodieTable table,
                                                  String instantTime, List<String> partitions) {
    super(context, config, table, instantTime,null, WriteOperationType.DELETE_PARTITION);
    this.partitions = partitions;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    List<String> instantsOfOffendingPendingTableServiceAction = new ArrayList<>();
    // ensure that there are no pending inflight clustering/compaction operations involving this partition
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();

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

    try {
      HoodieTimer timer = HoodieTimer.start();
      context.setJobStatus(this.getClass().getSimpleName(), "Gather all file ids from all deleting partitions.");
      Map<String, List<String>> partitionToReplaceFileIds =
          HoodieJavaPairRDD.getJavaPairRDD(context.parallelize(partitions).distinct()
              .mapToPair(partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
      HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
      result.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
      result.setIndexUpdateDuration(Duration.ofMillis(timer.endTimer()));
      result.setWriteStatuses(context.emptyHoodieData());

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
}
