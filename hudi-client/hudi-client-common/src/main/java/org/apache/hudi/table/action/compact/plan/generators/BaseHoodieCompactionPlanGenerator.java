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
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public abstract class BaseHoodieCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> implements Serializable {
  private static final Logger LOG = LogManager.getLogger(BaseHoodieCompactionPlanGenerator.class);

  protected final HoodieTable<T, I, K, O> hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final transient HoodieEngineContext engineContext;

  public BaseHoodieCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.hoodieTable = table;
    this.writeConfig = writeConfig;
    this.engineContext = engineContext;
  }

  public HoodieCompactionPlan generateCompactionPlan() throws IOException {
    // Accumulator to keep track of total log files for a table
    HoodieAccumulator totalLogFiles = engineContext.newAccumulator();
    // Accumulator to keep track of total log file slices for a table
    HoodieAccumulator totalFileSlices = engineContext.newAccumulator();

    // TODO : check if maxMemory is not greater than JVM or executor memory
    // TODO - rollback any compactions in flight
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(engineContext, writeConfig.getMetadataConfig(), metaClient.getBasePath());

    // filter the partition paths if needed to reduce list status
    partitionPaths = filterPartitionPathsByStrategy(writeConfig, partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }
    LOG.info("Looking for files to compact in " + partitionPaths + " partitions");
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Looking for files to compact: " + writeConfig.getTableName());

    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) this.hoodieTable.getSliceView();
    Set<HoodieFileGroupId> fgIdsInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet());

    // Exclude files in pending clustering from compaction.
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getLeft).collect(Collectors.toSet()));

    if (filterLogCompactionOperations()) {
      fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getPendingLogCompactionOperations()
          .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
          .collect(Collectors.toList()));
    }

    String lastCompletedInstantTime = hoodieTable.getMetaClient()
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();

    List<HoodieCompactionOperation> operations = engineContext.flatMap(partitionPaths, partitionPath -> fileSystemView
        .getLatestFileSlices(partitionPath)
        .filter(slice -> filterFileSlice(slice, lastCompletedInstantTime, fgIdsInPendingCompactionAndClustering))
        .map(s -> {
          List<HoodieLogFile> logFiles =
              s.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(toList());
          totalLogFiles.add(logFiles.size());
          totalFileSlices.add(1L);
          // Avro generated classes are not inheriting Serializable. Using CompactionOperation POJO
          // for Map operations and collecting them finally in Avro generated classes for storing
          // into meta files.6
          Option<HoodieBaseFile> dataFile = s.getBaseFile();
          return new CompactionOperation(dataFile, partitionPath, logFiles,
              writeConfig.getCompactionStrategy().captureMetrics(writeConfig, s));
        }), partitionPaths.size()).stream()
        .map(CompactionUtils::buildHoodieCompactionOperation).collect(toList());

    LOG.info("Total of " + operations.size() + " compaction operations are retrieved");
    LOG.info("Total number of latest files slices " + totalFileSlices.value());
    LOG.info("Total number of log files " + totalLogFiles.value());
    LOG.info("Total number of file slices " + totalFileSlices.value());

    if (operations.isEmpty()) {
      LOG.warn("No operations are retrieved for " + metaClient.getBasePath());
      return null;
    }

    // Filter the compactions with the passed in filter. This lets us choose most effective compactions only
    HoodieCompactionPlan compactionPlan = getCompactionPlan(metaClient, operations);
    ValidationUtils.checkArgument(
        compactionPlan.getOperations().stream().noneMatch(
            op -> fgIdsInPendingCompactionAndClustering.contains(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))),
        "Bad Compaction Plan. FileId MUST NOT have multiple pending compactions. "
            + "Please fix your strategy implementation. FileIdsWithPendingCompactions :" + fgIdsInPendingCompactionAndClustering
            + ", Selected workload :" + compactionPlan);
    if (compactionPlan.getOperations().isEmpty()) {
      LOG.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
    }
    return compactionPlan;
  }

  protected abstract HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations);

  protected abstract boolean filterLogCompactionOperations();

  protected List<String> filterPartitionPathsByStrategy(HoodieWriteConfig writeConfig, List<String> partitionPaths) {
    return partitionPaths;
  }

  protected boolean filterFileSlice(FileSlice fileSlice, String lastCompletedInstantTime, Set<HoodieFileGroupId> pendingFileGroupIds) {
    return fileSlice.getLogFiles().count() > 0 && !pendingFileGroupIds.contains(fileSlice.getFileGroupId());
  }

  protected Map<String, String> getStrategyParams() {
    return Collections.emptyMap();
  }

}
