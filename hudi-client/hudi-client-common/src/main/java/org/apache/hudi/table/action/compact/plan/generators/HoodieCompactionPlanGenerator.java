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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class HoodieCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends BaseHoodieCompactionPlanGenerator {

  private static final Logger LOG = LogManager.getLogger(HoodieCompactionPlanGenerator.class);

  public HoodieCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Generate a new compaction plan for scheduling.
   * @return Compaction Plan
   * @throws java.io.IOException when encountering errors
   */
  @Override
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
    partitionPaths = writeConfig.getCompactionStrategy().filterPartitionPaths(writeConfig, partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }

    LOG.info("Compaction looking for files to compact in " + partitionPaths + " partitions");
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Looking for files to compact");

    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) this.hoodieTable.getSliceView();
    Set<HoodieFileGroupId> fgIdsInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet());
    // Exclude pending log compaction operations.
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getPendingLogCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId()).collect(Collectors.toSet()));

    // Exclude files in pending clustering from compaction.
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getLeft).collect(Collectors.toSet()));

    List<HoodieCompactionOperation> operations = engineContext.flatMap(partitionPaths, partitionPath -> fileSystemView
        .getLatestFileSlices(partitionPath)
        .filter(slice -> !fgIdsInPendingCompactionAndClustering.contains(slice.getFileGroupId()))
        .map(s -> {
          List<HoodieLogFile> logFiles =
              s.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(toList());
          totalLogFiles.add(logFiles.size());
          totalFileSlices.add(1L);
          // Avro generated classes are not inheriting Serializable. Using CompactionOperation POJO
          // for Map operations and collecting them finally in Avro generated classes for storing
          // into meta files.
          Option<HoodieBaseFile> dataFile = s.getBaseFile();
          return new CompactionOperation(dataFile, partitionPath, logFiles,
              writeConfig.getCompactionStrategy().captureMetrics(writeConfig, s));
        })
        .filter(c -> !c.getDeltaFileNames().isEmpty()), partitionPaths.size()).stream()
        .map(CompactionUtils::buildHoodieCompactionOperation).collect(toList());

    LOG.info("Total of " + operations.size() + " compactions are retrieved");
    LOG.info("Total number of latest files slices " + totalFileSlices.value());
    LOG.info("Total number of log files " + totalLogFiles.value());
    LOG.info("Total number of file slices " + totalFileSlices.value());
    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    HoodieCompactionPlan compactionPlan = writeConfig.getCompactionStrategy().generateCompactionPlan(writeConfig, operations,
        CompactionUtils.getAllPendingCompactionPlans(metaClient).stream().map(Pair::getValue).collect(toList()));
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
}
