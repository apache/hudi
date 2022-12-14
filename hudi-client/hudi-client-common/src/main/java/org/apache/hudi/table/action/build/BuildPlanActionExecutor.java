/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.build;

import org.apache.hudi.avro.model.HoodieBuildPlan;
import org.apache.hudi.avro.model.HoodieBuildTask;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create plan for build action, the main metadata for plan is organized
 * as follows:
 * @formatter:off
 *   {
 *     tasks: [
 *        (baseFilePathA, [secondaryIndexM, secondaryIndexN, ...]),
 *        (baseFilePathB, [secondaryIndexN, secondaryIndexY, ...]),
 *        ...
 *     ],
 *     ...
 *   }
 * @formatter:on
 *
 * For every base file, there is one build action task, and the task knows
 * which secondary indexes should be generated, so we can read the file
 * once to build all the relevant secondary indexes.
 *
 * todo: scenarios as follows can be optimized:
 * t1: write commit
 * t2: generate build plan => buildTaskSetA
 * t3: write commit
 * t4: generate build plan => buildTaskSetB
 *
 * Some tasks in buildTaskSetA may out of date for there are new file slices
 * generated in the same file group when scheduling build at t4
 */
public class BuildPlanActionExecutor<T extends HoodieRecordPayload, I, K, O>
    extends BaseActionExecutor<T, I, K, O, Option<HoodieBuildPlan>> {
  private static final Logger LOG = LoggerFactory.getLogger(BuildPlanActionExecutor.class);

  private static final int BUILD_PLAN_VERSION_1 = 1;
  public static final int CURRENT_BUILD_PLAN_VERSION = BUILD_PLAN_VERSION_1;

  private final Option<Map<String, String>> extraMetadata;

  public BuildPlanActionExecutor(
      HoodieEngineContext context,
      HoodieWriteConfig config,
      HoodieTable<T, I, K, O> table,
      String instantTime,
      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  @Override
  public Option<HoodieBuildPlan> execute() {
    Option<HoodieBuildPlan> buildPlan = createBuildPlan();
    if (buildPlan.isPresent()) {
      HoodieInstant buildInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.BUILD_ACTION, instantTime);
      try {
        table.getActiveTimeline().saveToPendingBuildAction(buildInstant,
            TimelineMetadataUtils.serializeBuildPlan(buildPlan.get()));
      } catch (IOException e) {
        throw new HoodieIOException("Generate build plan failed", e);
      }
    }

    return buildPlan;
  }

  private Option<HoodieBuildPlan> createBuildPlan() {
    Option<List<HoodieSecondaryIndex>> secondaryIndexes =
        SecondaryIndexUtils.getSecondaryIndexes(table.getMetaClient());
    if (!secondaryIndexes.isPresent() || CollectionUtils.isNullOrEmpty(secondaryIndexes.get())) {
      LOG.info("No secondary index defined for this table: {}",
          table.getMetaClient().getTableConfig().getTableName());
      return Option.empty();
    }

    List<String> partitionPaths = FSUtils.getAllPartitionPaths(
        context, table.getConfig().getMetadataConfig(), table.getMetaClient().getBasePathV2().toString());
    String partitionSelectedStr = table.getConfig().getBuildPartitionSelected();
    if (!StringUtils.isNullOrEmpty(partitionSelectedStr)) {
      List<String> partitionSelected = Arrays.asList(partitionSelectedStr.trim().split(","));
      partitionPaths = partitionPaths.stream().filter(partitionSelected::contains).collect(Collectors.toList());
    }

    if (CollectionUtils.isNullOrEmpty(partitionPaths)) {
      LOG.info("No partition needs to build secondary index");
      return Option.empty();
    }

    // Notice: here we won't check whether there are new commits since last build action
    // for new indexes may be added during this time

    List<HoodieBuildTask> buildTasks = partitionPaths.stream()
        .flatMap(partitionPath ->
            secondaryIndexes.get().stream().flatMap(secondaryIndex ->
                getEligibleFileSlices(partitionPath, secondaryIndex)
                    .map(fileSlice -> Pair.of(secondaryIndex, fileSlice))))
        .collect(Collectors.groupingBy(Pair::getRight,
            Collectors.mapping(Pair::getLeft, Collectors.toList())))
        .entrySet().stream()
        .map(entry -> {
          // Convert List<HoodieSecondaryIndex> to json string
          String indexMetas = SecondaryIndexUtils.toJsonString(entry.getValue());

          String partitionPath = entry.getKey().getPartitionPath();
          String baseFileName = entry.getKey().getBaseFile().get().getFileName();
          String filePath;
          if (StringUtils.isNullOrEmpty(partitionPath)) {
            filePath = new Path(baseFileName).toString();
          } else {
            filePath = new Path(partitionPath, baseFileName).toString();
          }
          return HoodieBuildTask.newBuilder()
              .setPartition(entry.getKey().getPartitionPath())
              .setBaseFilePath(filePath)
              .setIndexMetas(indexMetas)
              .setVersion(CURRENT_BUILD_PLAN_VERSION)
              .build();
        }).collect(Collectors.toList());

    // No new commits since last build and no indexes be added during this time
    if (CollectionUtils.isNullOrEmpty(buildTasks)) {
      return Option.empty();
    }

    HoodieBuildPlan buildPlan = HoodieBuildPlan.newBuilder()
        .setTasks(buildTasks)
        .setExtraMetadata(extraMetadata.orElse(Collections.emptyMap()))
        .setVersion(CURRENT_BUILD_PLAN_VERSION)
        .build();
    return Option.ofNullable(buildPlan);
  }

  private Stream<FileSlice> getEligibleFileSlices(
      String partitionPath, HoodieSecondaryIndex secondaryIndex) {
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();
    Stream<String> pendingBaseFilePaths = fileSystemView.getPendingSecondaryIndexBaseFiles()
        .filter(pair -> pair.getLeft().equals(secondaryIndex))
        .flatMap(pair -> pair.getValue().keySet().stream());

    Stream<String> completedBaseFilePaths = fileSystemView.getSecondaryIndexBaseFiles()
        .filter(pair -> pair.getLeft().equals(secondaryIndex))
        .flatMap(pair -> pair.getRight().keySet().stream());

    Set<String> baseFilePaths = Stream.concat(pendingBaseFilePaths, completedBaseFilePaths)
        .collect(Collectors.toSet());

    // The file slices which have base file and base file not exists in
    // pending/completed secondary index are eligible.
    Predicate<FileSlice> predicate = fileSlice -> {
      if (fileSlice.getBaseFile().isPresent()) {
        String baseFilePath;
        if (StringUtils.isNullOrEmpty(fileSlice.getPartitionPath())) {
          baseFilePath = fileSlice.getBaseFile().get().getFileName();
        } else {
          baseFilePath = fileSlice.getPartitionPath() + "/" + fileSlice.getBaseFile().get().getFileName();
        }

        return !baseFilePaths.contains(baseFilePath);
      }

      return false;
    };

    return table.getSliceView().getLatestFileSlices(partitionPath).filter(predicate);
  }
}
