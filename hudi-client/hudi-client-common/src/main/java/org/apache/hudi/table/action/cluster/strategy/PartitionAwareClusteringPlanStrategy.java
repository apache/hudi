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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scheduling strategy with restriction that clustering groups can only contain files from same partition.
 */
public abstract class PartitionAwareClusteringPlanStrategy<T,I,K,O> extends ClusteringPlanStrategy<T,I,K,O> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareClusteringPlanStrategy.class);

  public PartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Create Clustering group based on files eligible for clustering in the partition.
   */
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    HoodieWriteConfig writeConfig = getWriteConfig();

    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();

    // Sort fileSlices before dividing, which makes dividing more compact
    List<FileSlice> sortedFileSlices = new ArrayList<>(fileSlices);
    sortedFileSlices.sort((o1, o2) -> (int)
        ((o2.getBaseFile().isPresent() ? o2.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())
            - (o1.getBaseFile().isPresent() ? o1.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())));

    long totalSizeSoFar = 0;

    for (FileSlice currentSlice : sortedFileSlices) {
      long currentSize = currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize();
      // check if max size is reached and create new group, if needed.
      if (totalSizeSoFar + currentSize > writeConfig.getClusteringMaxBytesInGroup() && !currentGroup.isEmpty()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding one clustering group " + totalSizeSoFar + " max bytes: "
            + writeConfig.getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups);
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;

        // if fileSliceGroups's size reach the max group, stop loop
        if (fileSliceGroups.size() >= writeConfig.getClusteringMaxNumGroups()) {
          LOG.info("Having generated the maximum number of groups : " + writeConfig.getClusteringMaxNumGroups());
          break;
        }
      }

      // Add to the current file-group
      currentGroup.add(currentSlice);
      // assume each file group size is ~= parquet.max.file.size
      totalSizeSoFar += currentSize;
    }

    if (!currentGroup.isEmpty()) {
      if (currentGroup.size() > 1 || writeConfig.shouldClusteringSingleGroup()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding final clustering group " + totalSizeSoFar + " max bytes: "
            + writeConfig.getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups);
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
      }
    }

    return fileSliceGroups.stream().map(fileSliceGroup ->
        HoodieClusteringGroup.newBuilder()
            .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
            .setNumOutputFileGroups(fileSliceGroup.getRight())
            .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
            .build());
  }

  /**
   * Return list of partition paths to be considered for clustering.
   */
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    List<String> filteredPartitions = ClusteringPlanPartitionFilter.filter(partitionPaths, getWriteConfig());
    LOG.debug("Filtered to the following partitions: " + filteredPartitions);
    return filteredPartitions;
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan() {
    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for {}", metaClient.getBasePath());
    HoodieWriteConfig config = getWriteConfig();

    String partitionSelected = config.getClusteringPartitionSelected();
    LOG.info("Scheduling clustering partitionSelected: {}", partitionSelected);
    List<String> partitionPaths;

    if (StringUtils.isNullOrEmpty(partitionSelected)) {
      // get matched partitions if set
      partitionPaths = getRegexPatternMatchedPartitions(config, FSUtils.getAllPartitionPaths(
          getEngineContext(), metaClient.getStorage(), config.getMetadataConfig(), metaClient.getBasePath()));
      // filter the partition paths if needed to reduce list status
    } else {
      partitionPaths = Arrays.asList(partitionSelected.split(","));
    }

    partitionPaths = filterPartitionPaths(partitionPaths);
    LOG.info("Scheduling clustering partitionPaths: {}", partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    List<HoodieClusteringGroup> clusteringGroups = getEngineContext()
        .flatMap(
            partitionPaths,
            partitionPath -> {
              List<FileSlice> fileSlicesEligible = getFileSlicesEligibleForClustering(partitionPath).collect(Collectors.toList());
              return buildClusteringGroupsForPartition(partitionPath, fileSlicesEligible).limit(getWriteConfig().getClusteringMaxNumGroups());
            },
            partitionPaths.size())
        .stream()
        .limit(getWriteConfig().getClusteringMaxNumGroups())
        .collect(Collectors.toList());

    if (clusteringGroups.isEmpty()) {
      LOG.warn("No data available to cluster");
      return Option.empty();
    }

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(getWriteConfig().getClusteringExecutionStrategyClass())
        .setStrategyParams(getStrategyParams())
        .build();

    return Option.of(HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(getExtraMetadata())
        .setVersion(getPlanVersion())
        .setPreserveHoodieMetadata(true)
        .build());
  }

  public List<String> getRegexPatternMatchedPartitions(HoodieWriteConfig config, List<String> partitionPaths) {
    String pattern = config.getClusteringPartitionFilterRegexPattern();
    if (!StringUtils.isNullOrEmpty(pattern)) {
      partitionPaths = partitionPaths.stream()
          .filter(partition -> Pattern.matches(pattern, partition))
          .collect(Collectors.toList());
    }
    return partitionPaths;
  }

  protected int getNumberOfOutputFileGroups(long groupSize, long targetFileSize) {
    return (int) Math.ceil(groupSize / (double) targetFileSize);
  }
}
