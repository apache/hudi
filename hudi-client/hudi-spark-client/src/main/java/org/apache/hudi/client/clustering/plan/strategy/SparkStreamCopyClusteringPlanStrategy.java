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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering Strategy that uses binary stream copy for execution.
 * This strategy automatically sets the SparkStreamCopyClusteringExecutionStrategy
 * as the execution strategy when building the clustering plan.
 */
public class SparkStreamCopyClusteringPlanStrategy<T>
    extends SparkSizeBasedClusteringPlanStrategy<T> {
  
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamCopyClusteringPlanStrategy.class);
  
  private static final String SPARK_STREAM_COPY_CLUSTERING_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.SparkStreamCopyClusteringExecutionStrategy";
  
  public SparkStreamCopyClusteringPlanStrategy(HoodieTable table,
                                               HoodieEngineContext engineContext,
                                               HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }
  
  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      params.put(PLAN_STRATEGY_SORT_COLUMNS.key(), getWriteConfig().getClusteringSortColumns());
    }
    // Add any additional parameters specific to stream copy strategy
    return params;
  }
  
  @Override
  protected Pair<Stream<HoodieClusteringGroup>, Boolean> buildClusteringGroupsForPartition(
      String partitionPath, List<FileSlice> fileSlices) {
    
    // When schema evolution is disabled, use schema-aware grouping
    // When schema evolution is enabled, use the default size-only grouping
    if (!getWriteConfig().isBinaryCopySchemaEvolutionEnabled()) {
      LOG.info("Schema evolution disabled, using schema-aware grouping for partition: {}", partitionPath);
      return buildClusteringGroupsForPartitionWithSchemaGrouping(partitionPath, fileSlices);
    } else {
      LOG.info("Schema evolution enabled, using size-only grouping for partition: {}", partitionPath);
      return super.buildClusteringGroupsForPartition(partitionPath, fileSlices);
    }
  }
  
  /**
   * Create Clustering group based on files eligible for clustering with schema-aware grouping.
   * When schema evolution is disabled, files are grouped by schema hash first, then by size.
   * This ensures only files with identical schemas are clustered together.
   */
  private Pair<Stream<HoodieClusteringGroup>, Boolean> buildClusteringGroupsForPartitionWithSchemaGrouping(
      String partitionPath, List<FileSlice> fileSlices) {
    HoodieWriteConfig writeConfig = getWriteConfig();
    
    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();
    
    // Sort fileSlices before dividing
    List<FileSlice> sortedFileSlices = new ArrayList<>(fileSlices);
    sortedFileSlices.sort((o1, o2) -> (int)
        ((o2.getBaseFile().isPresent() ? o2.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())
            - (o1.getBaseFile().isPresent() ? o1.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())));
    
    long totalSizeSoFar = 0;
    boolean partialScheduled = false;
    Integer currentGroupSchemaHash = null; // Track schema hash for current group
    
    for (FileSlice currentSlice : sortedFileSlices) {
      long currentSize = currentSlice.getBaseFile().isPresent() 
          ? currentSlice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize();
      
      // Get schema hash from the file for schema-based grouping
      Integer currentFileSchemaHash = getFileSchemaHash(currentSlice);
      
      // Check if we need to create a new group due to schema mismatch
      boolean schemaMismatch = currentGroupSchemaHash != null && !currentGroupSchemaHash.equals(currentFileSchemaHash);
      
      // Check if max size is reached OR schema is different, and create new group if needed
      if ((totalSizeSoFar + currentSize > writeConfig.getClusteringMaxBytesInGroup() || schemaMismatch) 
          && !currentGroup.isEmpty()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding clustering group - size: {} max bytes: {} num input slices: {} output groups: {}{}",
            totalSizeSoFar, writeConfig.getClusteringMaxBytesInGroup(), currentGroup.size(), numOutputGroups,
            schemaMismatch ? " (schema change detected)" : "");
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;
        currentGroupSchemaHash = null;
        
        // If fileSliceGroups size reaches the max group, stop loop
        if (fileSliceGroups.size() >= writeConfig.getClusteringMaxNumGroups()) {
          LOG.info("Having generated the maximum number of groups: {}", writeConfig.getClusteringMaxNumGroups());
          partialScheduled = true;
          break;
        }
      }
      
      // Add to the current file-group
      currentGroup.add(currentSlice);
      // Set schema hash for the group if not set
      if (currentGroupSchemaHash == null) {
        currentGroupSchemaHash = currentFileSchemaHash;
      }
      totalSizeSoFar += currentSize;
    }
    
    if (!currentGroup.isEmpty()) {
      if (currentGroup.size() > 1 || writeConfig.shouldClusteringSingleGroup()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding final clustering group - size: {} max bytes: {} num input slices: {} output groups: {}",
            totalSizeSoFar, writeConfig.getClusteringMaxBytesInGroup(), currentGroup.size(), numOutputGroups);
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
      }
    }
    
    return Pair.of(fileSliceGroups.stream().map(fileSliceGroup ->
        HoodieClusteringGroup.newBuilder()
            .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
            .setNumOutputFileGroups(fileSliceGroup.getRight())
            .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
            .build()), partialScheduled);
  }
  
  /**
   * Get the schema hash for a file slice to enable schema-based grouping.
   */
  private Integer getFileSchemaHash(FileSlice fileSlice) {
    if (fileSlice.getBaseFile().isPresent()) {
      String filePath = fileSlice.getBaseFile().get().getPath();
      try {
        // Use centralized ParquetUtils method to read schema hash
        return ParquetUtils.readSchemaHash(
            getHoodieTable().getStorage(),
            new StoragePath(filePath));
      } catch (Exception e) {
        LOG.warn("Failed to read schema hash from file: {}", filePath, e);
        // Return a default hash if we can't read the schema
        return 0;
      }
    }
    // Return default hash for files without base file
    return 0;
  }
  
  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan(ClusteringPlanActionExecutor executor, Lazy<List<String>> partitions) {
    Option<HoodieClusteringPlan> planOption = super.generateClusteringPlan(executor, partitions);
    
    if (planOption.isPresent()) {
      HoodieClusteringPlan plan = planOption.get();
      
      // Override the execution strategy to use SparkStreamCopyClusteringExecutionStrategy
      HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
          .setStrategyClassName(SPARK_STREAM_COPY_CLUSTERING_EXECUTION_STRATEGY)
          .setStrategyParams(getStrategyParams())
          .build();
      
      LOG.info("Setting execution strategy to SparkStreamCopyClusteringExecutionStrategy for stream copy clustering");
      
      return Option.of(HoodieClusteringPlan.newBuilder()
          .setStrategy(strategy)
          .setInputGroups(plan.getInputGroups())
          .setExtraMetadata(getExtraMetadata())
          .setVersion(getPlanVersion())
          .setPreserveHoodieMetadata(true)
          .setMissingSchedulePartitions(plan.getMissingSchedulePartitions())
          .build());
    }
    
    return planOption;
  }
}