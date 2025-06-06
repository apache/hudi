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
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogBlockMetadataScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.LogCompactionExecutionHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HoodieLogCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends BaseHoodieCompactionPlanGenerator<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogCompactionPlanGenerator.class);
  private final HoodieCompactionStrategy compactionStrategy;

  public HoodieLogCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                          BaseTableServicePlanActionExecutor executor) {
    super(table, engineContext, writeConfig, executor);
    this.compactionStrategy = HoodieCompactionStrategy.newBuilder()
        .setStrategyParams(getStrategyParams())
        .setCompactorClassName(LogCompactionExecutionHelper.class.getName())
        .build();
  }

  @Override
  protected HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations, Pair<List<String>, List<String>> partitionPair) {
    return HoodieCompactionPlan.newBuilder()
        .setOperations(operations)
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION)
        .setStrategy(compactionStrategy)
        .setMissingSchedulePartitions(partitionPair.getRight())
        .setPreserveHoodieMetadata(true)
        .build();
  }

  @Override
  protected List<String> getPartitions() {
    return executor.getPartitions(compactionStrategy, TableServiceType.LOG_COMPACT);
  }

  @Override
  protected boolean filterFileSlice(FileSlice fileSlice, String lastCompletedInstantTime,
                                    Set<HoodieFileGroupId> pendingFileGroupIds, Option<InstantRange> instantRange) {
    return super.filterFileSlice(fileSlice, lastCompletedInstantTime, pendingFileGroupIds, instantRange) && isFileSliceEligibleForLogCompaction(fileSlice, lastCompletedInstantTime, instantRange);
  }

  @Override
  protected boolean filterLogCompactionOperations() {
    return true;
  }

  /**
   * Can schedule logcompaction if log files count or total log blocks is greater than the configured threshold.
   * @param fileSlice File Slice under consideration.
   * @param instantRange Range of valid instants.
   * @return Boolean value that determines whether log compaction will be scheduled or not.
   */
  private boolean isFileSliceEligibleForLogCompaction(FileSlice fileSlice, String maxInstantTime,
                                                      Option<InstantRange> instantRange) {
    LOG.info("Checking if fileId {} and partition {} eligible for log compaction.", fileSlice.getFileId(), fileSlice.getPartitionPath());
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    long numLogFiles = fileSlice.getLogFiles().count();
    if (numLogFiles >= writeConfig.getLogCompactionBlocksThreshold()) {
      LOG.info("Total logs files ({}) is greater than log blocks threshold is {}", numLogFiles, writeConfig.getLogCompactionBlocksThreshold());
      return true;
    }
    HoodieLogBlockMetadataScanner scanner = new HoodieLogBlockMetadataScanner(metaClient, fileSlice.getLogFiles()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(file -> file.getPath().toString())
        .collect(Collectors.toList()),
        writeConfig.getMaxDFSStreamBufferSize(),
        maxInstantTime,
        instantRange);
    int totalBlocks = scanner.getCurrentInstantLogBlocks().size();
    LOG.info("Total blocks seen are {}, log blocks threshold is {}", totalBlocks, writeConfig.getLogCompactionBlocksThreshold());

    // If total blocks in the file slice is > blocks threshold value(default value is 5).
    // Log compaction can be scheduled.
    return totalBlocks >= writeConfig.getLogCompactionBlocksThreshold();
  }
}
