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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.utils.FileSliceMetricUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pluggable implementation for scheduling clustering and creating ClusteringPlan.
 */
public abstract class ClusteringPlanStrategy<T,I,K,O> implements Serializable {
  private static final Logger LOG = LogManager.getLogger(ClusteringPlanStrategy.class);

  public static final int CLUSTERING_PLAN_VERSION_1 = 1;

  private final HoodieTable<T,I,K,O> hoodieTable;
  private final transient HoodieEngineContext engineContext;
  private final HoodieWriteConfig writeConfig;

  /**
   * Check if the given class is deprecated.
   * If it is, then try to convert it to suitable one and update the write config accordingly.
   * @param config write config
   * @return class name of clustering plan strategy
   */
  public static String checkAndGetClusteringPlanStrategy(HoodieWriteConfig config) {
    String className = config.getClusteringPlanStrategyClass();
    String sparkSizeBasedClassName = HoodieClusteringConfig.SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
    String sparkSelectedPartitionsClassName = "org.apache.hudi.client.clustering.plan.strategy.SparkSelectedPartitionsClusteringPlanStrategy";
    String sparkRecentDaysClassName = "org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy";
    String javaSelectedPartitionClassName = "org.apache.hudi.client.clustering.plan.strategy.JavaRecentDaysClusteringPlanStrategy";
    String javaSizeBasedClassName = HoodieClusteringConfig.JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;

    String logStr = "The clustering plan '%s' is deprecated. Please set the plan as '%s' and set '%s' as '%s' to achieve the same behaviour";
    if (sparkRecentDaysClassName.equals(className)) {
      config.setValue(HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME, ClusteringPlanPartitionFilterMode.RECENT_DAYS.name());
      LOG.warn(String.format(logStr, className, sparkSizeBasedClassName, HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME.key(), ClusteringPlanPartitionFilterMode.RECENT_DAYS.name()));
      return sparkSizeBasedClassName;
    } else if (sparkSelectedPartitionsClassName.equals(className)) {
      config.setValue(HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME, ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name());
      LOG.warn(String.format(logStr, className, sparkSizeBasedClassName, HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME.key(), ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name()));
      return sparkSizeBasedClassName;
    } else if (javaSelectedPartitionClassName.equals(className)) {
      config.setValue(HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME, ClusteringPlanPartitionFilterMode.RECENT_DAYS.name());
      LOG.warn(String.format(logStr, className, javaSizeBasedClassName, HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME.key(), ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name()));
      return javaSizeBasedClassName;
    }
    return className;
  }

  public ClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    this.hoodieTable = table;
    this.engineContext = engineContext;
  }

  /**
   * Generate metadata for grouping eligible files and create a plan. Note that data is not moved around
   * as part of this step.
   *
   * If there is no data available to cluster, return None.
   */
  public abstract Option<HoodieClusteringPlan> generateClusteringPlan();

  /**
   * Return file slices eligible for clustering. FileIds in pending clustering/compaction are not eligible for clustering.
   */
  protected Stream<FileSlice> getFileSlicesEligibleForClustering(String partition) {
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) getHoodieTable().getSliceView();
    Set<HoodieFileGroupId> fgIdsInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
        .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
        .collect(Collectors.toSet());
    fgIdsInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getKey).collect(Collectors.toSet()));

    return hoodieTable.getSliceView().getLatestFileSlices(partition)
        // file ids already in clustering are not eligible
        .filter(slice -> !fgIdsInPendingCompactionAndClustering.contains(slice.getFileGroupId()));
  }

  /**
   * Get parameters specific to strategy. These parameters are passed from 'schedule clustering' step to
   * 'execute clustering' step. 'execute clustering' step is typically async. So these params help with passing any required
   * context from schedule to run step.
   */
  protected abstract Map<String, String> getStrategyParams();

  /**
   * Returns any specific parameters to be stored as part of clustering metadata.
   */
  protected Map<String, String> getExtraMetadata() {
    return Collections.emptyMap();
  }

  /**
   * Version to support future changes for plan.
   */
  protected int getPlanVersion() {
    return CLUSTERING_PLAN_VERSION_1;
  }

  /**
   * Transform {@link FileSlice} to {@link HoodieSliceInfo}.
   */
  protected static List<HoodieSliceInfo> getFileSliceInfo(List<FileSlice> slices) {
    return slices.stream().map(slice -> new HoodieSliceInfo().newBuilder()
        .setPartitionPath(slice.getPartitionPath())
        .setFileId(slice.getFileId())
        .setDataFilePath(slice.getBaseFile().map(BaseFile::getPath).orElse(StringUtils.EMPTY_STRING))
        .setDeltaFilePaths(slice.getLogFiles().map(f -> f.getPath().toString()).collect(Collectors.toList()))
        .setBootstrapFilePath(slice.getBaseFile().map(bf -> bf.getBootstrapBaseFile().map(bbf -> bbf.getPath()).orElse(StringUtils.EMPTY_STRING)).orElse(StringUtils.EMPTY_STRING))
        .build()).collect(Collectors.toList());
  }

  /**
   * Generate metrics for the data to be clustered.
   */
  protected Map<String, Double> buildMetrics(List<FileSlice> fileSlices) {
    Map<String, Double> metrics = new HashMap<>();
    FileSliceMetricUtils.addFileSliceCommonMetrics(fileSlices, metrics, getWriteConfig().getParquetMaxFileSize());
    return metrics;
  }

  protected HoodieTable<T, I, K, O> getHoodieTable() {
    return this.hoodieTable;
  }

  protected HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }

  protected HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }
}
