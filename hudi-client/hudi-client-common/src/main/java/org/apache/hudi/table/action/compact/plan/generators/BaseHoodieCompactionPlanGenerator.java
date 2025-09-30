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
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.CompactHelpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public abstract class BaseHoodieCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieCompactionPlanGenerator.class);

  protected final HoodieTable<T, I, K, O> hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final transient HoodieEngineContext engineContext;
  protected final BaseTableServicePlanActionExecutor executor;

  public BaseHoodieCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                           BaseTableServicePlanActionExecutor executor) {
    this.hoodieTable = table;
    this.writeConfig = writeConfig;
    this.engineContext = engineContext;
    this.executor = executor;
  }

  @Nullable
  public HoodieCompactionPlan generateCompactionPlan(String compactionInstant) throws IOException {
    // Accumulator to keep track of total log files for a table
    HoodieAccumulator totalLogFiles = engineContext.newAccumulator();
    // Accumulator to keep track of total file slices for a table
    HoodieAccumulator totalFileSlices = engineContext.newAccumulator();

    // TODO : check if maxMemory is not greater than JVM or executor memory
    // TODO - rollback any compactions in flight
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    CompletionTimeQueryView completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient);
    List<String> partitionPaths = getPartitions();

    int allPartitionSize = partitionPaths.size();

    // filter the partition paths if needed to reduce list status
    Pair<List<String>, List<String>> partitionPair = filterPartitionPathsByStrategy(partitionPaths);
    partitionPaths = partitionPair.getLeft();

    LOG.info("Strategy: {} matched {} partition paths from all {} partitions for table {}",
        writeConfig.getCompactionStrategy().getClass().getSimpleName(), partitionPaths.size(), allPartitionSize,
        hoodieTable.getConfig().getBasePath());
    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }
    // avoid logging all partitions in table by default
    LOG.info("Looking for files to compact in {} partitions for table {}", partitionPaths.size(), hoodieTable.getConfig().getBasePath());
    LOG.debug("Partitions scanned for compaction: {} for table {}", partitionPaths, hoodieTable.getConfig().getBasePath());
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Looking for files to compact: " + writeConfig.getTableName());

    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) this.hoodieTable.getSliceView();
    // Exclude file groups under compaction.
    Set<HoodieFileGroupId> fgIdsInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet());

    // Exclude files in pending clustering from compaction.
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getLeft).collect(Collectors.toSet()));

    // Exclude files in pending log compaction.
    if (filterLogCompactionOperations()) {
      fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getPendingLogCompactionOperations()
          .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
          .collect(Collectors.toList()));
    }

    String lastCompletedInstantTime = hoodieTable.getMetaClient()
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().requestedTime();
    LOG.info("Last completed instant time {} for table {}", lastCompletedInstantTime, hoodieTable.getConfig().getBasePath());
    Option<InstantRange> instantRange = CompactHelpers.getInstance().getInstantRange(metaClient);

    List<HoodieCompactionOperation> operations = engineContext.flatMap(partitionPaths, partitionPath -> fileSystemView
        .getLatestFileSlicesStateless(partitionPath)
        .filter(slice -> filterFileSlice(slice, lastCompletedInstantTime, fgIdsInPendingCompactionAndClustering, instantRange))
        .map(s -> {
          List<HoodieLogFile> logFiles = s.getLogFiles()
              // ==============================================================
              // IMPORTANT
              // ==============================================================
              // Currently, our filesystem view could return a file slice with pending log files there,
              // these files should be excluded from the plan, let's say we have such a sequence of actions

              // t10: a delta commit starts,
              // t20: the compaction is scheduled and the t10 delta commit is still pending, and the "fg_10.log" is included in the plan
              // t25: the delta commit 10 finishes,
              // t30: the compaction execution starts, now the reader considers the log file "fg_10.log" as valid.

              // for both OCC and NB-CC, this is in-correct.
              .filter(logFile -> completionTimeQueryView.isCompletedBefore(compactionInstant, logFile.getDeltaCommitTime()))
              .sorted(HoodieLogFile.getLogFileComparator()).collect(toList());
          if (logFiles.isEmpty()) {
            // compaction is not needed if there is no log file.
            return null;
          }
          totalLogFiles.add(logFiles.size());
          totalFileSlices.add(1L);
          // Avro generated classes are not inheriting Serializable. Using CompactionOperation POJO
          // for Map operations and collecting them finally in Avro generated classes for storing
          // into meta files.
          Option<HoodieBaseFile> dataFile = s.getBaseFile();
          return new CompactionOperation(dataFile, partitionPath, logFiles,
              writeConfig.getCompactionStrategy().captureMetrics(writeConfig, s));
        }).filter(Objects::nonNull), partitionPaths.size()).stream()
        .map(CompactionUtils::buildHoodieCompactionOperation).collect(toList());

    LOG.info("Total of {} compaction operations are retrieved for table {}", operations.size(), hoodieTable.getConfig().getBasePath());
    LOG.info("Total number of log files {} for table {}", totalLogFiles.value(), hoodieTable.getConfig().getBasePath());
    LOG.info("Total number of file slices {} for table {}", totalFileSlices.value(), hoodieTable.getConfig().getBasePath());

    if (operations.isEmpty()) {
      LOG.info("No operations are retrieved for {} for table {}", metaClient.getBasePath(), hoodieTable.getConfig().getBasePath());
      return null;
    }

    if (totalLogFiles.value() <= 0) {
      LOG.info("No log files are retrieved for {} for table {}", metaClient.getBasePath(), hoodieTable.getConfig().getBasePath());
      return null;
    }

    // Filter the compactions with the passed in filter. This lets us choose most effective compactions only
    HoodieCompactionPlan compactionPlan = getCompactionPlan(metaClient, operations, partitionPair);
    ValidationUtils.checkArgument(
        compactionPlan.getOperations().stream().noneMatch(
            op -> fgIdsInPendingCompactionAndClustering.contains(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))),
        () -> "Bad Compaction Plan. FileId MUST NOT have multiple pending compactions. "
            + "Please fix your strategy implementation. FileIdsWithPendingCompactions :" + fgIdsInPendingCompactionAndClustering
            + ", Selected workload :" + compactionPlan);
    if (compactionPlan.getOperations().isEmpty()) {
      LOG.info("After filtering, Nothing to compact for {} for table {}", metaClient.getBasePath(), hoodieTable.getConfig().getBasePath());
    }
    compactionPlan.setExtraMetadata(getExtraMetadata(operations, compactionPlan));
    return compactionPlan;
  }

  protected abstract List<String> getPartitions();

  protected abstract HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations, Pair<List<String>,List<String>> partitionPair);

  protected abstract boolean filterLogCompactionOperations();

  protected Pair<List<String>, List<String>> filterPartitionPathsByStrategy(List<String> partitionPaths) {
    return Pair.of(partitionPaths, Collections.emptyList());
  }

  protected boolean filterFileSlice(FileSlice fileSlice, String lastCompletedInstantTime,
                                    Set<HoodieFileGroupId> pendingFileGroupIds, Option<InstantRange> instantRange) {
    return fileSlice.getLogFiles().count() > 0 && !pendingFileGroupIds.contains(fileSlice.getFileGroupId());
  }

  protected Map<String, String> getStrategyParams() {
    return Collections.emptyMap();
  }

  protected Map<String, String> getExtraMetadata(List<HoodieCompactionOperation> operationsBeforeApplyingStrategy, HoodieCompactionPlan compactionPlan) {
    return Collections.emptyMap();
  }
}
