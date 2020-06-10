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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseMergeOnReadRollbackActionExecutor<T extends HoodieRecordPayload, I, K, O, P> extends BaseRollbackActionExecutor<T, I, K, O, P> {

  public BaseMergeOnReadRollbackActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               BaseHoodieTable<T, I, K, O, P> table,
                                               String instantTime,
                                               HoodieInstant commitInstant,
                                               boolean deleteInstants) {
    super(context, config, table, instantTime, commitInstant, deleteInstants);
  }

  public BaseMergeOnReadRollbackActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               BaseHoodieTable<T, I, K, O, P> table,
                                               String instantTime,
                                               HoodieInstant commitInstant,
                                               boolean deleteInstants,
                                               boolean skipTimelinePublish) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish);
  }

  /**
   * Generate all rollback requests that we need to perform for rolling back this action without actually performing
   * rolling back.
   *
   * @param context           HoodieEngineContext
   * @param instantToRollback Instant to Rollback
   * @return list of rollback requests
   * @throws IOException
   */
  protected abstract List<RollbackRequest> generateRollbackRequests(HoodieEngineContext context, HoodieInstant instantToRollback)
      throws IOException;

  protected List<RollbackRequest> generateAppendRollbackBlocksAction(String partitionPath, HoodieInstant rollbackInstant,
                                                                     HoodieCommitMetadata commitMetadata) {
    ValidationUtils.checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the parquet and not the requested, hence get the
    // baseCommit always by listing the file slice
    Map<String, String> fileIdToBaseCommitTimeForLogMap = table.getSliceView().getLatestFileSlices(partitionPath)
        .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));
    return commitMetadata.getPartitionToWriteStats().get(partitionPath).stream().filter(wStat -> {

      // Filter out stats without prevCommit since they are all inserts
      boolean validForRollback = (wStat != null) && (!wStat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT))
          && (wStat.getPrevCommit() != null) && fileIdToBaseCommitTimeForLogMap.containsKey(wStat.getFileId());

      if (validForRollback) {
        // For sanity, log instant time can never be less than base-commit on which we are rolling back
        ValidationUtils
            .checkArgument(HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId()),
                HoodieTimeline.LESSER_THAN_OR_EQUALS, rollbackInstant.getTimestamp()));
      }

      return validForRollback && HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(
          // Base Ts should be strictly less. If equal (for inserts-to-logs), the caller employs another option
          // to delete and we should not step on it
          wStat.getFileId()), HoodieTimeline.LESSER_THAN, rollbackInstant.getTimestamp());
    }).map(wStat -> {
      String baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
      return RollbackRequest.createRollbackRequestWithAppendRollbackBlockAction(partitionPath, wStat.getFileId(),
          baseCommitTime, rollbackInstant);
    }).collect(Collectors.toList());
  }

}
