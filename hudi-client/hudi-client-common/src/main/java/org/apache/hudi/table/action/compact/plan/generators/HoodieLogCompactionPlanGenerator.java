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
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.LogCompactionExecutionStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class HoodieLogCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends BaseHoodieCompactionPlanGenerator {

  private static final Logger LOG = LogManager.getLogger(HoodieLogCompactionPlanGenerator.class);

  public HoodieLogCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Generate a new log compaction plan for scheduling.
   * @return Log Compaction Plan
   * @throws java.io.IOException when encountering errors
   */
  @Override
  public HoodieCompactionPlan generateCompactionPlan() {

    // While scheduling log compaction (i.e. minor compaction) make sure only one log compaction is scheduled for a latest file Slice.
    // Major compaction anyway will take care of creating a new base file, so if there is a pending compaction then log compaction
    // need not be scheduled for previous file slice.
    // Therefore, log compaction will only be scheduled for latest file slice or always for last file slice.
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) this.hoodieTable.getSliceView();

    // Accumulator to keep track of total log files for a table
    HoodieAccumulator totalLogFiles = this.engineContext.newAccumulator();
    // Accumulator to keep track of total log file slices for a table
    HoodieAccumulator totalFileSlices = this.engineContext.newAccumulator();

    HoodieTableMetaClient metaClient = this.hoodieTable.getMetaClient();

    // Filter partition paths.
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(this.engineContext, writeConfig.getMetadataConfig(),
        metaClient.getBasePath());

    // Compaction Strategy should be SpecificPartitionCompactionStrategy to run a logcompaction on a specified partition.
    partitionPaths = writeConfig.getCompactionStrategy().filterPartitionPaths(writeConfig, partitionPaths);

    // Collect all pending compaction file groups
    Set<HoodieFileGroupId> fgIdsInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet());

    // Collect all pending log compaction file groups
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getPendingLogCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet()));

    // Collect all pending clustering file groups
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering()
        .map(Pair::getLeft).collect(Collectors.toSet()));

    String maxInstantTime = hoodieTable.getMetaClient()
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();

    // Here two different filters are applied before scheduling log compaction.
    // 1. Exclude all the file groups which are either part of a pending compaction or clustering plans.
    // 2. Check if FileSlices are meeting the criteria for LogCompaction.
    List<HoodieCompactionOperation> operations = engineContext.flatMap(partitionPaths, partitionPath -> fileSystemView
        .getLatestFileSlices(partitionPath)
        .filter(fileSlice -> !fgIdsInPendingCompactionAndClustering.contains(fileSlice.getFileGroupId()))
        .filter(fileSlice -> isFileSliceEligibleForLogCompaction(fileSlice, maxInstantTime))
        .map(fileSlice -> {
          List<HoodieLogFile> logFiles =
              fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(toList());
          totalLogFiles.add(logFiles.size());
          totalFileSlices.add(1);
          Option<HoodieBaseFile> dataFile = fileSlice.getBaseFile();
          return new CompactionOperation(dataFile, fileSlice.getPartitionPath(), logFiles,
              writeConfig.getCompactionStrategy().captureMetrics(writeConfig, fileSlice));
        })
        .filter(c -> !c.getDeltaFileNames().isEmpty()), partitionPaths.size()).stream()
        .map(CompactionUtils::buildHoodieCompactionOperation).collect(Collectors.toList());

    if (operations.isEmpty()) {
      LOG.warn("After filtering, Nothing to log compact for " + metaClient.getBasePath());
      return null;
    }

    LOG.info("Total of " + operations.size() + " log compaction operations are retrieved");
    LOG.info("Total number of latest file slices " + totalFileSlices.value());
    LOG.info("Total number of log files " + totalLogFiles.value());

    HoodieCompactionStrategy compactionStrategy = HoodieCompactionStrategy.newBuilder()
        .setStrategyParams(getStrategyParams())
        .setCompactorClassName(LogCompactionExecutionStrategy.class.getName())
        .build();
    HoodieCompactionPlan compactionPlan = HoodieCompactionPlan.newBuilder()
        .setOperations(operations)
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION)
        .setStrategy(compactionStrategy)
        .setPreserveHoodieMetadata(true)
        .build();
    LOG.info("Compaction plan created " + compactionPlan);
    return compactionPlan;
  }

  /**
   * Can schedule logcompaction if log files count is greater than 4 or total log blocks is greater than 4.
   * @param fileSlice File Slice under consideration.
   * @return Boolean value that determines whether log compaction will be scheduled or not.
   */
  private boolean isFileSliceEligibleForLogCompaction(FileSlice fileSlice, String maxInstantTime) {
    LOG.info("Checking if fileId " + fileSlice.getFileId() + " and partition "
        + fileSlice.getPartitionPath() + " eligible for log compaction.");
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withFileSystem(metaClient.getFs())
        .withBasePath(hoodieTable.getMetaClient().getBasePath())
        .withLogFilePaths(fileSlice.getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(file -> file.getPath().toString())
            .collect(Collectors.toList()))
        .withLatestInstantTime(maxInstantTime)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withUseScanV2(true)
        .build();
    scanner.scanInternal(Option.empty(), true);
    int totalBlocks = scanner.getCurrentInstantLogBlocks().size();
    LOG.info("Total blocks seen are " + totalBlocks);

    // If total blocks in the file slice is > blocks threshold value(default value is 5).
    // Log compaction can be scheduled.
    return totalBlocks > writeConfig.getLogCompactionBlocksThreshold();
  }

}
