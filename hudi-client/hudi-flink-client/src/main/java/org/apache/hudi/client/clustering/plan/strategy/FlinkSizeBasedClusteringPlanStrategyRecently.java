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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;

/**
 * Only take care of partitions related to active timeline, instead of do full partition listing.
 */
public class FlinkSizeBasedClusteringPlanStrategyRecently<T> extends FlinkSizeBasedClusteringPlanStrategy<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSizeBasedClusteringPlanStrategy.class);
  public FlinkSizeBasedClusteringPlanStrategyRecently(HoodieTable table,
                                                      HoodieEngineContext engineContext,
                                                      HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    if (!table.getConfig().getTableType().equals(HoodieTableType.COPY_ON_WRITE)) {
      throw new UnsupportedOperationException("FlinkSizeBasedClusteringPlanStrategyRecently only support cow table for now.");
    }
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan(ClusteringPlanActionExecutor executor, Lazy<List<String>> partitions) {
    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for {}", metaClient.getBasePath());

    List<String> partitionPaths = getPartitionPathInActiveTimeline(hoodieTable);

    partitionPaths = filterPartitionPaths(getWriteConfig(), partitionPaths).getLeft();

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    List<HoodieClusteringGroup> clusteringGroups = getEngineContext()
            .flatMap(
                    partitionPaths, partitionPath -> {
                    List<FileSlice> fileSlicesEligible = getFileSlicesEligibleForClustering(partitionPath).collect(Collectors.toList());
                    return buildClusteringGroupsForPartition(partitionPath, fileSlicesEligible).getLeft().limit(getWriteConfig().getClusteringMaxNumGroups());
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

    return Option.of(HoodieClusteringPlan.newBuilder()
            .setStrategy(strategy)
            .setInputGroups(clusteringGroups)
            .setExtraMetadata(getExtraMetadata())
            .setVersion(getPlanVersion())
            .setPreserveHoodieMetadata(true)
            .build());
  }

  /**
   * Only take care of partitions related to active timeline, instead of do full partition listing.
   * @param hoodieTable
   * @return
   */
  private List<String> getPartitionPathInActiveTimeline(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    HashSet<String> partitions = new HashSet<>();
    HoodieTimeline cowCommitTimeline = hoodieTable.getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION)).filterCompletedInstants();
    cowCommitTimeline.getInstants().forEach(instant -> {
      try {
        HoodieCommitMetadata metadata = cowCommitTimeline.readCommitMetadata(instant);
        partitions.addAll(metadata.getWritePartitionPaths());
      } catch (IOException e) {
        // ignore Exception here
        LOG.warn("Failed to get details from commit metadata for instant [{}].", instant, e);
      }
    });

    LOG.info("Partitions related to active timeline: {}", partitions);
    return new ArrayList<>(partitions);
  }
}