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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.exception.HoodieDeletePartitionPendingTableServiceException;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class for helper functions when performing a delete partition operation.
 */
public class DeletePartitionUtils {

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
   * This will cause  downstream duplicate key errors when a map is being constructed.
   *
   * @param table Table to perform validation on
   * @param partitionsToDrop List of partitions to drop
   */
  public static void checkForPendingTableServiceActions(HoodieTable table, List<String> partitionsToDrop) {
    List<String> instantsOfOffendingPendingTableServiceAction = new ArrayList<>();
    // ensure that there are no pending inflight clustering/compaction operations involving this partition
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();

    // separating the iteration of pending compaction operations from clustering as they return different stream types
    Stream.concat(fileSystemView.getPendingCompactionOperations(), fileSystemView.getPendingLogCompactionOperations())
        .filter(op -> partitionsToDrop.contains(op.getRight().getPartitionPath()))
        .forEach(op -> instantsOfOffendingPendingTableServiceAction.add(op.getLeft()));

    fileSystemView.getFileGroupsInPendingClustering()
        .filter(fgIdInstantPair -> partitionsToDrop.contains(fgIdInstantPair.getLeft().getPartitionPath()))
        .forEach(x -> instantsOfOffendingPendingTableServiceAction.add(x.getRight().requestedTime()));

    if (instantsOfOffendingPendingTableServiceAction.size() > 0) {
      throw new HoodieDeletePartitionPendingTableServiceException("Failed to drop partitions. "
          + "Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: " + partitionsToDrop + ". "
          + "Instant(s) of offending pending table service action: "
          + instantsOfOffendingPendingTableServiceAction.stream().distinct().collect(Collectors.toList()));
    }
  }

}
