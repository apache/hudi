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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.HoodieFlinkMergeOnReadTable;
import org.apache.hudi.table.action.cluster.strategy.PartitionAwareClusteringPlanStrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering Strategy based on following.
 * 1) Creates clustering groups based on max size allowed per group.
 * 2) Excludes files that are greater than 'small.file.limit' from clustering plan.
 */
public class FlinkSizeBasedClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends PartitionAwareClusteringPlanStrategy<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(FlinkSizeBasedClusteringPlanStrategy.class);

  public FlinkSizeBasedClusteringPlanStrategy(HoodieFlinkCopyOnWriteTable<T> table,
                                              HoodieFlinkEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public FlinkSizeBasedClusteringPlanStrategy(HoodieFlinkMergeOnReadTable<T> table,
                                              HoodieFlinkEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    HoodieWriteConfig writeConfig = getWriteConfig();

    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();
    long totalSizeSoFar = 0;

    for (FileSlice currentSlice : fileSlices) {
      // check if max size is reached and create new group, if needed.
      // in now, every clustering group out put is 1 file group.
      if (totalSizeSoFar >= writeConfig.getClusteringTargetFileMaxBytes() && !currentGroup.isEmpty()) {
        LOG.info("Adding one clustering group " + totalSizeSoFar + " max bytes: "
            + writeConfig.getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size());
        fileSliceGroups.add(Pair.of(currentGroup, 1));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;
      }

      // Add to the current file-group
      currentGroup.add(currentSlice);
      // assume each file group size is ~= parquet.max.file.size
      totalSizeSoFar += currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize();
    }

    if (!currentGroup.isEmpty()) {
      fileSliceGroups.add(Pair.of(currentGroup, 1));
    }

    return fileSliceGroups.stream().map(fileSliceGroup ->
        HoodieClusteringGroup.newBuilder()
            .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
            .setNumOutputFileGroups(fileSliceGroup.getRight())
            .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
            .build());
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      params.put(PLAN_STRATEGY_SORT_COLUMNS.key(), getWriteConfig().getClusteringSortColumns());
    }
    return params;
  }

  @Override
  protected Stream<FileSlice> getFileSlicesEligibleForClustering(final String partition) {
    return super.getFileSlicesEligibleForClustering(partition)
        // Only files that have basefile size smaller than small file size are eligible.
        .filter(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L) < getWriteConfig().getClusteringSmallFileLimit());
  }

}
