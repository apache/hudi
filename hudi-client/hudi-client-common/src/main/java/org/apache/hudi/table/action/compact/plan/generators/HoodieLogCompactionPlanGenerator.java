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

package org.apache.hudi.table.action.compact.plan.generators;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieCompactionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.LogCompactionExecutionHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HoodieLogCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends BaseHoodieCompactionPlanGenerator<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogCompactionPlanGenerator.class);

  public HoodieLogCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations) {
    HoodieCompactionStrategy compactionStrategy = HoodieCompactionStrategy.newBuilder()
        .setStrategyParams(getStrategyParams())
        .setCompactorClassName(LogCompactionExecutionHelper.class.getName())
        .build();
    return HoodieCompactionPlan.newBuilder()
        .setOperations(operations)
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION)
        .setStrategy(compactionStrategy)
        .setPreserveHoodieMetadata(true)
        .build();
  }

  @Override
  protected boolean filterFileSlice(FileSlice fileSlice, String lastCompletedInstantTime,
                                    Set<HoodieFileGroupId> pendingFileGroupIds, Option<InstantRange> instantRange) {
    return isFileSliceEligibleForLogCompaction(fileSlice, lastCompletedInstantTime, instantRange)
        && super.filterFileSlice(fileSlice, lastCompletedInstantTime, pendingFileGroupIds, instantRange);
  }

  @Override
  protected boolean filterLogCompactionOperations() {
    return true;
  }

  /**
   * Can schedule logcompaction if log files count is greater than 4 or total log blocks is greater than 4.
   * @param fileSlice File Slice under consideration.
   * @return Boolean value that determines whether log compaction will be scheduled or not.
   */
  private boolean isFileSliceEligibleForLogCompaction(FileSlice fileSlice, String maxInstantTime,
                                                      Option<InstantRange> instantRange) {
    LOG.info("Checking if fileId " + fileSlice.getFileId() + " and partition "
        + fileSlice.getPartitionPath() + " eligible for log compaction.");
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(hoodieTable.getMetaClient().getBasePath())
        .withLogFilePaths(fileSlice.getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(file -> file.getPath().toString())
            .collect(Collectors.toList()))
        .withLatestInstantTime(maxInstantTime)
        .withInstantRange(instantRange)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withOptimizedLogBlocksScan(true)
        .withRecordMerger(writeConfig.getRecordMerger())
        .withTableMetaClient(metaClient)
        .build();
    scanner.scan(true);
    int totalBlocks = scanner.getCurrentInstantLogBlocks().size();
    LOG.info("Total blocks seen are " + totalBlocks + ", log blocks threshold is "
        + writeConfig.getLogCompactionBlocksThreshold());

    // If total blocks in the file slice is > blocks threshold value(default value is 5).
    // Log compaction can be scheduled.
    return totalBlocks >= writeConfig.getLogCompactionBlocksThreshold();
  }
}
