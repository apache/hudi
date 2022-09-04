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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanFilter;
import org.apache.hudi.table.action.cluster.ClusteringPlanFilterMode;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scheduling strategy with restriction that clustering groups can only contain files from same partition.
 */
public abstract class PartitionAwareClusteringPlanStrategy<T extends HoodieRecordPayload,I,K,O> extends ClusteringPlanStrategy<T,I,K,O> {
  private static final Logger LOG = LogManager.getLogger(PartitionAwareClusteringPlanStrategy.class);

  public PartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Create Clustering group based on files eligible for clustering in the partition.
   */
  protected abstract Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath,
                                                                                     List<FileSlice> fileSlices);

  /**
   * Return list of partition paths to be considered for clustering.
   */
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    List<String> filteredPartitions = ClusteringPlanPartitionFilter.filter(partitionPaths, getWriteConfig());
    LOG.debug("Filtered to the following partitions: " + filteredPartitions);
    return filteredPartitions;
  }

  /**
   * Return list of FileSlices to be considered for clustering.
   */
  protected List<FileSlice> filterFileSlices(List<FileSlice> fileSlices, Option<HoodieDefaultTimeline> toFilterTimeline, ClusteringPlanFilterMode filterMode) {
    List<FileSlice> filteredFileSlices = ClusteringPlanFilter.filter(fileSlices, toFilterTimeline, filterMode);
    LOG.debug("Filtered to the following fileSlices: " + filteredFileSlices);
    return filteredFileSlices;
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan() {
    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for " + metaClient.getBasePath());
    HoodieWriteConfig config = getWriteConfig();
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(getEngineContext(), config.getMetadataConfig(), metaClient.getBasePath());

    // get matched partitions if set
    partitionPaths = getMatchedPartitions(config, partitionPaths);
    // filter the partition paths if needed to reduce list status
    partitionPaths = filterPartitionPaths(partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    Option<HoodieDefaultTimeline> toFilterTimeline = Option.empty();
    Option<HoodieInstant> latestCompletedInstant = Option.empty();
    // if filtering based on file slices are enabled, we need to pass the toFilter timeline below to assist in filtering.
    if (config.getClusteringPlanFilterMode() != ClusteringPlanFilterMode.NONE) {
      Option<HoodieInstant> latestScheduledReplaceCommit = metaClient.getActiveTimeline()
          .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
          .filter(instant -> instant.isRequested()).lastInstant();
      if (latestScheduledReplaceCommit.isPresent()) {
        HoodieClusteringPlan clusteringPlan = ClusteringUtils.getClusteringPlan(
                metaClient, HoodieTimeline.getReplaceCommitRequestedInstant(latestScheduledReplaceCommit.get().getTimestamp()))
            .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
                "Unable to read clustering plan for instant: " + latestScheduledReplaceCommit.get().getTimestamp()));
        if (!StringUtils.isNullOrEmpty(clusteringPlan.getLatestCompletedInstant())) {
          toFilterTimeline = Option.of((HoodieDefaultTimeline)
              metaClient.getActiveTimeline().filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, clusteringPlan.getLatestCompletedInstant())));
        }
      } else {
        // if last clustering was archived
        toFilterTimeline = Option.of(metaClient.getActiveTimeline());
      }
      latestCompletedInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    }

    Option<HoodieDefaultTimeline> finalToFilterTimeline = toFilterTimeline;
    List<HoodieClusteringGroup> clusteringGroups = getEngineContext()
        .flatMap(
            partitionPaths,
            partitionPath -> {
              List<FileSlice> fileSlicesEligible = getFileSlicesEligibleForClustering(partitionPath).collect(Collectors.toList());
              List<FileSlice> filteredFileSlices = filterFileSlices(fileSlicesEligible, finalToFilterTimeline, config.getClusteringPlanFilterMode());
              return buildClusteringGroupsForPartition(partitionPath, filteredFileSlices).limit(getWriteConfig().getClusteringMaxNumGroups());
            },
            partitionPaths.size())
        .stream()
        .limit(getWriteConfig().getClusteringMaxNumGroups())
        .collect(Collectors.toList());

    if (clusteringGroups.isEmpty()) {
      LOG.info("No data available to cluster");
      return Option.empty();
    }

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(getWriteConfig().getClusteringExecutionStrategyClass())
        .setStrategyParams(getStrategyParams())
        .build();

    HoodieClusteringPlan.Builder clusteringPlanBuilder = HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(getExtraMetadata())
        .setVersion(getPlanVersion())
        .setPreserveHoodieMetadata(getWriteConfig().isPreserveHoodieCommitMetadataForClustering());

    return Option.of(latestCompletedInstant.map(instant ->
        clusteringPlanBuilder.setLatestCompletedInstant(instant.getTimestamp()).build()).orElse(clusteringPlanBuilder.build()));
  }

  public List<String> getMatchedPartitions(HoodieWriteConfig config, List<String> partitionPaths) {
    String partitionSelected = config.getClusteringPartitionSelected();
    if (!StringUtils.isNullOrEmpty(partitionSelected)) {
      return Arrays.asList(partitionSelected.split(","));
    } else {
      return getRegexPatternMatchedPartitions(config, partitionPaths);
    }
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
}
