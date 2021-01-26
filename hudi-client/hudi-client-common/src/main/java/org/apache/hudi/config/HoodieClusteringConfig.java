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

package org.apache.hudi.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
public class HoodieClusteringConfig extends DefaultHoodieConfig {

  // Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy
  public static final String CLUSTERING_PLAN_STRATEGY_CLASS = "hoodie.clustering.plan.strategy.class";
  public static final String DEFAULT_CLUSTERING_PLAN_STRATEGY_CLASS =
      "org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy";

  // Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy
  public static final String CLUSTERING_EXECUTION_STRATEGY_CLASS = "hoodie.clustering.execution.strategy.class";
  public static final String DEFAULT_CLUSTERING_EXECUTION_STRATEGY_CLASS =
      "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy";

  // Turn on inline clustering - clustering will be run after write operation is complete.
  public static final String INLINE_CLUSTERING_PROP = "hoodie.clustering.inline";
  private static final String DEFAULT_INLINE_CLUSTERING = "false";

  // Config to control frequency of clustering
  public static final String INLINE_CLUSTERING_MAX_COMMIT_PROP = "hoodie.clustering.inline.max.commits";
  private static final String DEFAULT_INLINE_CLUSTERING_NUM_COMMITS = "4";
  
  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.plan.strategy.";

  // Number of partitions to list to create ClusteringPlan.
  public static final String CLUSTERING_TARGET_PARTITIONS = CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.lookback.partitions";
  public static final String DEFAULT_CLUSTERING_TARGET_PARTITIONS = String.valueOf(2);

  // Files smaller than the size specified here are candidates for clustering.
  public static final String CLUSTERING_PLAN_SMALL_FILE_LIMIT = CLUSTERING_STRATEGY_PARAM_PREFIX + "small.file.limit";
  public static final String DEFAULT_CLUSTERING_PLAN_SMALL_FILE_LIMIT = String.valueOf(600 * 1024 * 1024L); // 600MB

  // Each clustering operation can create multiple groups. Total amount of data processed by clustering operation
  // is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS).
  // Max amount of data to be included in one group
  public static final String CLUSTERING_MAX_BYTES_PER_GROUP = CLUSTERING_STRATEGY_PARAM_PREFIX + "max.bytes.per.group";
  public static final String DEFAULT_CLUSTERING_MAX_GROUP_SIZE = String.valueOf(2 * 1024 * 1024 * 1024L);

  // Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism.
  public static final String CLUSTERING_MAX_NUM_GROUPS = CLUSTERING_STRATEGY_PARAM_PREFIX + "max.num.groups";
  public static final String DEFAULT_CLUSTERING_MAX_NUM_GROUPS = "30";

  // Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups.
  public static final String CLUSTERING_TARGET_FILE_MAX_BYTES = CLUSTERING_STRATEGY_PARAM_PREFIX + "target.file.max.bytes";
  public static final String DEFAULT_CLUSTERING_TARGET_FILE_MAX_BYTES = String.valueOf(1 * 1024 * 1024 * 1024L); // 1GB
  
  // Constants related to clustering that may be used by more than 1 strategy.
  public static final String CLUSTERING_SORT_COLUMNS_PROPERTY = HoodieClusteringConfig.CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns";

  // When file groups is in clustering, need to handle the update to these file groups. Default strategy just reject the update
  public static final String CLUSTERING_UPDATES_STRATEGY_PROP = "hoodie.clustering.updates.strategy";
  public static final String DEFAULT_CLUSTERING_UPDATES_STRATEGY = "org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy";

  // Async clustering
  public static final String ASYNC_CLUSTERING_ENABLE_OPT_KEY = "hoodie.clustering.async.enabled";
  public static final String DEFAULT_ASYNC_CLUSTERING_ENABLE_OPT_VAL = "false";

  public HoodieClusteringConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder withClusteringPlanStrategyClass(String clusteringStrategyClass) {
      props.setProperty(CLUSTERING_PLAN_STRATEGY_CLASS, clusteringStrategyClass);
      return this;
    }

    public Builder withClusteringExecutionStrategyClass(String runClusteringStrategyClass) {
      props.setProperty(CLUSTERING_EXECUTION_STRATEGY_CLASS, runClusteringStrategyClass);
      return this;
    }

    public Builder withClusteringTargetPartitions(int clusteringTargetPartitions) {
      props.setProperty(CLUSTERING_TARGET_PARTITIONS, String.valueOf(clusteringTargetPartitions));
      return this;
    }

    public Builder withClusteringPlanSmallFileLimit(long clusteringSmallFileLimit) {
      props.setProperty(CLUSTERING_PLAN_SMALL_FILE_LIMIT, String.valueOf(clusteringSmallFileLimit));
      return this;
    }
    
    public Builder withClusteringSortColumns(String sortColumns) {
      props.setProperty(CLUSTERING_SORT_COLUMNS_PROPERTY, sortColumns);
      return this;
    }

    public Builder withClusteringMaxBytesInGroup(long clusteringMaxGroupSize) {
      props.setProperty(CLUSTERING_MAX_BYTES_PER_GROUP, String.valueOf(clusteringMaxGroupSize));
      return this;
    }

    public Builder withClusteringMaxNumGroups(int maxNumGroups) {
      props.setProperty(CLUSTERING_MAX_NUM_GROUPS, String.valueOf(maxNumGroups));
      return this;
    }

    public Builder withClusteringTargetFileMaxBytes(long targetFileSize) {
      props.setProperty(CLUSTERING_TARGET_FILE_MAX_BYTES, String.valueOf(targetFileSize));
      return this;
    }

    public Builder withInlineClustering(Boolean inlineClustering) {
      props.setProperty(INLINE_CLUSTERING_PROP, String.valueOf(inlineClustering));
      return this;
    }

    public Builder withInlineClusteringNumCommits(int numCommits) {
      props.setProperty(INLINE_CLUSTERING_MAX_COMMIT_PROP, String.valueOf(numCommits));
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withClusteringUpdatesStrategy(String updatesStrategyClass) {
      props.setProperty(CLUSTERING_UPDATES_STRATEGY_PROP, updatesStrategyClass);
      return this;
    }

    public Builder withAsyncClustering(Boolean asyncClustering) {
      props.setProperty(ASYNC_CLUSTERING_ENABLE_OPT_KEY, String.valueOf(asyncClustering));
      return this;
    }

    public HoodieClusteringConfig build() {
      HoodieClusteringConfig config = new HoodieClusteringConfig(props);

      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_PLAN_STRATEGY_CLASS),
          CLUSTERING_PLAN_STRATEGY_CLASS, DEFAULT_CLUSTERING_PLAN_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_EXECUTION_STRATEGY_CLASS),
          CLUSTERING_EXECUTION_STRATEGY_CLASS, DEFAULT_CLUSTERING_EXECUTION_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_MAX_BYTES_PER_GROUP), CLUSTERING_MAX_BYTES_PER_GROUP,
          DEFAULT_CLUSTERING_MAX_GROUP_SIZE);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_MAX_NUM_GROUPS), CLUSTERING_MAX_NUM_GROUPS,
          DEFAULT_CLUSTERING_MAX_NUM_GROUPS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_TARGET_FILE_MAX_BYTES), CLUSTERING_TARGET_FILE_MAX_BYTES,
          DEFAULT_CLUSTERING_TARGET_FILE_MAX_BYTES);
      setDefaultOnCondition(props, !props.containsKey(INLINE_CLUSTERING_PROP), INLINE_CLUSTERING_PROP,
          DEFAULT_INLINE_CLUSTERING);
      setDefaultOnCondition(props, !props.containsKey(INLINE_CLUSTERING_MAX_COMMIT_PROP), INLINE_CLUSTERING_MAX_COMMIT_PROP,
          DEFAULT_INLINE_CLUSTERING_NUM_COMMITS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_TARGET_PARTITIONS), CLUSTERING_TARGET_PARTITIONS,
          DEFAULT_CLUSTERING_TARGET_PARTITIONS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_PLAN_SMALL_FILE_LIMIT), CLUSTERING_PLAN_SMALL_FILE_LIMIT,
          DEFAULT_CLUSTERING_PLAN_SMALL_FILE_LIMIT);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_UPDATES_STRATEGY_PROP), CLUSTERING_UPDATES_STRATEGY_PROP, 
          DEFAULT_CLUSTERING_UPDATES_STRATEGY);
      setDefaultOnCondition(props, !props.containsKey(ASYNC_CLUSTERING_ENABLE_OPT_KEY), ASYNC_CLUSTERING_ENABLE_OPT_KEY,
          DEFAULT_ASYNC_CLUSTERING_ENABLE_OPT_VAL);
      return config;
    }
  }
}
