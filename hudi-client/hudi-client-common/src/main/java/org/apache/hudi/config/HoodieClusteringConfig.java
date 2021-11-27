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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
@ConfigClassProperty(name = "Clustering Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control the clustering table service in hudi, "
        + "which optimizes the storage layout for better query performance by sorting and sizing data files.")
public class HoodieClusteringConfig extends HoodieConfig {

  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.plan.strategy.";
  public static final String SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy";
  public static final String JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy";
  public static final String SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy";
  public static final String JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy";

  // Any Space-filling curves optimize(z-order/hilbert) params can be saved with this prefix
  public static final String LAYOUT_OPTIMIZE_PARAM_PREFIX = "hoodie.layout.optimize.";

  public static final ConfigProperty<String> DAYBASED_LOOKBACK_PARTITIONS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.lookback.partitions")
      .defaultValue("2")
      .sinceVersion("0.7.0")
      .withDocumentation("Number of partitions to list to create ClusteringPlan");

  public static final ConfigProperty<String> PLAN_STRATEGY_SMALL_FILE_LIMIT = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "small.file.limit")
      .defaultValue(String.valueOf(600 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Files smaller than the size specified here are candidates for clustering");

  public static final ConfigProperty<String> PLAN_STRATEGY_CLASS_NAME = ConfigProperty
      .key("hoodie.clustering.plan.strategy.class")
      .defaultValue(SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY)
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan "
          + "i.e select what file groups are being clustered. Default strategy, looks at the clustering small file size limit (determined by "
          + PLAN_STRATEGY_SMALL_FILE_LIMIT.key() + ") to pick the small file slices within partitions for clustering.");

  public static final ConfigProperty<String> EXECUTION_STRATEGY_CLASS_NAME = ConfigProperty
      .key("hoodie.clustering.execution.strategy.class")
      .defaultValue(SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY)
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the "
          + " clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while "
          + " meeting the configured target file sizes.");

  public static final ConfigProperty<String> INLINE_CLUSTERING = ConfigProperty
      .key("hoodie.clustering.inline")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Turn on inline clustering - clustering will be run after each write operation is complete");

  public static final ConfigProperty<String> INLINE_CLUSTERING_MAX_COMMITS = ConfigProperty
      .key("hoodie.clustering.inline.max.commits")
      .defaultValue("4")
      .sinceVersion("0.7.0")
      .withDocumentation("Config to control frequency of clustering planning");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_MAX_COMMITS = ConfigProperty
      .key("hoodie.clustering.async.max.commits")
      .defaultValue("4")
      .sinceVersion("0.9.0")
      .withDocumentation("Config to control frequency of async clustering");

  public static final ConfigProperty<String> PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.skipfromlatest.partitions")
      .defaultValue("0")
      .sinceVersion("0.9.0")
      .withDocumentation("Number of partitions to skip from latest when choosing partitions to create ClusteringPlan");

  public static final ConfigProperty<String> PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.bytes.per.group")
      .defaultValue(String.valueOf(2 * 1024 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Each clustering operation can create multiple output file groups. Total amount of data processed by clustering operation"
          + " is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS)."
          + " Max amount of data to be included in one group");

  public static final ConfigProperty<String> PLAN_STRATEGY_MAX_GROUPS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.num.groups")
      .defaultValue("30")
      .sinceVersion("0.7.0")
      .withDocumentation("Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism");

  public static final ConfigProperty<String> PLAN_STRATEGY_TARGET_FILE_MAX_BYTES = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "target.file.max.bytes")
      .defaultValue(String.valueOf(1024 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups");

  public static final ConfigProperty<String> PLAN_STRATEGY_SORT_COLUMNS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns")
      .noDefaultValue()
      .sinceVersion("0.7.0")
      .withDocumentation("Columns to sort the data by when clustering");

  public static final ConfigProperty<String> UPDATES_STRATEGY = ConfigProperty
      .key("hoodie.clustering.updates.strategy")
      .defaultValue("org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy")
      .sinceVersion("0.7.0")
      .withDocumentation("Determines how to handle updates, deletes to file groups that are under clustering."
          + " Default strategy just rejects the update");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_ENABLE = ConfigProperty
      .key("hoodie.clustering.async.enabled")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Enable running of clustering service, asynchronously as inserts happen on the table.");

  public static final ConfigProperty<Boolean> PRESERVE_COMMIT_METADATA = ConfigProperty
      .key("hoodie.clustering.preserve.commit.metadata")
      .defaultValue(true)
      .sinceVersion("0.9.0")
      .withDocumentation("When rewriting data, preserves existing hoodie_commit_time");

  /**
   * Using space-filling curves to optimize the layout of table to boost query performance.
   * The table data which sorted by space-filling curve has better aggregation;
   * combine with min-max filtering, it can achieve good performance improvement.
   *
   * Notice:
   * when we use this feature, we need specify the sort columns.
   * The more columns involved in sorting, the worse the aggregation, and the smaller the query performance improvement.
   * Choose the filter columns which commonly used in query sql as sort columns.
   * It is recommend that 2 ~ 4 columns participate in sorting.
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_ENABLE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "enable")
      .defaultValue(false)
      .sinceVersion("0.10.0")
      .withDocumentation("Enable use z-ordering/space-filling curves to optimize the layout of table to boost query performance. "
          + "This parameter takes precedence over clustering strategy set using " + EXECUTION_STRATEGY_CLASS_NAME.key());

  public static final ConfigProperty LAYOUT_OPTIMIZE_STRATEGY = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "strategy")
      .defaultValue("z-order")
      .sinceVersion("0.10.0")
      .withDocumentation("Type of layout optimization to be applied, current only supports `z-order` and `hilbert` curves.");

  /**
   * There exists two method to build z-curve.
   * one is directly mapping sort cols to z-value to build z-curve;
   * we can find this method in Amazon DynamoDB https://aws.amazon.com/cn/blogs/database/tag/z-order/
   * the other one is Boundary-based Interleaved Index method which we proposed. simply call it sample method.
   * Refer to rfc-28 for specific algorithm flow.
   * Boundary-based Interleaved Index method has better generalization, but the build speed is slower than direct method.
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_CURVE_BUILD_METHOD = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "curve.build.method")
      .defaultValue("direct")
      .sinceVersion("0.10.0")
      .withDocumentation("Controls how data is sampled to build the space filling curves. two methods: `direct`,`sample`."
          + "The direct method is faster than the sampling, however sample method would produce a better data layout.");
  /**
   * Doing sample for table data is the first step in Boundary-based Interleaved Index method.
   * larger sample number means better optimize result, but more memory consumption
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "build.curve.sample.size")
      .defaultValue("200000")
      .sinceVersion("0.10.0")
      .withDocumentation("when setting" + LAYOUT_OPTIMIZE_CURVE_BUILD_METHOD.key() + " to `sample`, the amount of sampling to be done."
          + "Large sample size leads to better results, at the expense of more memory usage.");

  /**
   * The best way to use Z-order/Space-filling curves is to cooperate with Data-Skipping
   * with data-skipping query engine can greatly reduce the number of table files to be read.
   * otherwise query engine can only do row-group skipping for files (parquet/orc)
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_DATA_SKIPPING_ENABLE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "data.skipping.enable")
      .defaultValue(true)
      .sinceVersion("0.10.0")
      .withDocumentation("Enable data skipping by collecting statistics once layout optimization is complete.");

  public static final ConfigProperty<Boolean> ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT = ConfigProperty
      .key("hoodie.clustering.rollback.pending.replacecommit.on.conflict")
      .defaultValue(false)
      .sinceVersion("0.10.0")
      .withDocumentation("If updates are allowed to file groups pending clustering, then set this config to rollback failed or pending clustering instants. "
          + "Pending clustering will be rolled back ONLY IF there is conflict between incoming upsert and filegroup to be clustered. "
          + "Please exercise caution while setting this config, especially when clustering is done very frequently. This could lead to race condition in "
          + "rare scenarios, for example, when the clustering completes after instants are fetched but before rollback completed.");

  /**
   * @deprecated Use {@link #PLAN_STRATEGY_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_PLAN_STRATEGY_CLASS = PLAN_STRATEGY_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_PLAN_STRATEGY_CLASS = PLAN_STRATEGY_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #EXECUTION_STRATEGY_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_EXECUTION_STRATEGY_CLASS = EXECUTION_STRATEGY_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #EXECUTION_STRATEGY_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_EXECUTION_STRATEGY_CLASS = EXECUTION_STRATEGY_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #INLINE_CLUSTERING} and its methods instead
   */
  @Deprecated
  public static final String INLINE_CLUSTERING_PROP = INLINE_CLUSTERING.key();
  /**
   * @deprecated Use {@link #INLINE_CLUSTERING} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_INLINE_CLUSTERING = INLINE_CLUSTERING.defaultValue();
  /**
   * @deprecated Use {@link #INLINE_CLUSTERING_MAX_COMMITS} and its methods instead
   */
  @Deprecated
  public static final String INLINE_CLUSTERING_MAX_COMMIT_PROP = INLINE_CLUSTERING_MAX_COMMITS.key();
  /**
   * @deprecated Use {@link #INLINE_CLUSTERING_MAX_COMMITS} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_INLINE_CLUSTERING_NUM_COMMITS = INLINE_CLUSTERING_MAX_COMMITS.defaultValue();
  /**
   * @deprecated Use {@link #DAYBASED_LOOKBACK_PARTITIONS} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_TARGET_PARTITIONS = DAYBASED_LOOKBACK_PARTITIONS.key();
  /**
   * @deprecated Use {@link #DAYBASED_LOOKBACK_PARTITIONS} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_TARGET_PARTITIONS = DAYBASED_LOOKBACK_PARTITIONS.defaultValue();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_SMALL_FILE_LIMIT} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_PLAN_SMALL_FILE_LIMIT = PLAN_STRATEGY_SMALL_FILE_LIMIT.key();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_SMALL_FILE_LIMIT} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_PLAN_SMALL_FILE_LIMIT = PLAN_STRATEGY_SMALL_FILE_LIMIT.defaultValue();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_MAX_BYTES_PER_GROUP = PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP.key();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_MAX_GROUP_SIZE = PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP.defaultValue();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_MAX_GROUPS} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_MAX_NUM_GROUPS = PLAN_STRATEGY_MAX_GROUPS.key();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_MAX_GROUPS} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_MAX_NUM_GROUPS = PLAN_STRATEGY_MAX_GROUPS.defaultValue();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_TARGET_FILE_MAX_BYTES} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_TARGET_FILE_MAX_BYTES = PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_TARGET_FILE_MAX_BYTES} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_TARGET_FILE_MAX_BYTES = PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.defaultValue();
  /**
   * @deprecated Use {@link #PLAN_STRATEGY_SORT_COLUMNS} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_SORT_COLUMNS_PROPERTY = PLAN_STRATEGY_SORT_COLUMNS.key();
  /**
   * @deprecated Use {@link #UPDATES_STRATEGY} and its methods instead
   */
  @Deprecated
  public static final String CLUSTERING_UPDATES_STRATEGY_PROP = UPDATES_STRATEGY.key();
  /**
   * @deprecated Use {@link #UPDATES_STRATEGY} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLUSTERING_UPDATES_STRATEGY = UPDATES_STRATEGY.defaultValue();
  /**
   * @deprecated Use {@link #ASYNC_CLUSTERING_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String ASYNC_CLUSTERING_ENABLE_OPT_KEY = ASYNC_CLUSTERING_ENABLE.key();
  /** @deprecated Use {@link #ASYNC_CLUSTERING_ENABLE} and its methods instead */
  @Deprecated
  public static final String DEFAULT_ASYNC_CLUSTERING_ENABLE_OPT_VAL = ASYNC_CLUSTERING_ENABLE.defaultValue();

  // NOTE: This ctor is required for appropriate deserialization
  public HoodieClusteringConfig() {
    super();
  }

  public boolean isAsyncClusteringEnabled() {
    return getBooleanOrDefault(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE);
  }

  public boolean isInlineClusteringEnabled() {
    return getBooleanOrDefault(HoodieClusteringConfig.INLINE_CLUSTERING);
  }

  public static HoodieClusteringConfig from(TypedProperties props) {
    return  HoodieClusteringConfig.newBuilder().fromProperties(props).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieClusteringConfig clusteringConfig = new HoodieClusteringConfig();
    private EngineType engineType = EngineType.SPARK;

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.clusteringConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder withClusteringPlanStrategyClass(String clusteringStrategyClass) {
      clusteringConfig.setValue(PLAN_STRATEGY_CLASS_NAME, clusteringStrategyClass);
      return this;
    }

    public Builder withClusteringExecutionStrategyClass(String runClusteringStrategyClass) {
      clusteringConfig.setValue(EXECUTION_STRATEGY_CLASS_NAME, runClusteringStrategyClass);
      return this;
    }

    public Builder withClusteringTargetPartitions(int clusteringTargetPartitions) {
      clusteringConfig.setValue(DAYBASED_LOOKBACK_PARTITIONS, String.valueOf(clusteringTargetPartitions));
      return this;
    }

    public Builder withClusteringSkipPartitionsFromLatest(int clusteringSkipPartitionsFromLatest) {
      clusteringConfig.setValue(PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, String.valueOf(clusteringSkipPartitionsFromLatest));
      return this;
    }

    public Builder withClusteringPlanSmallFileLimit(long clusteringSmallFileLimit) {
      clusteringConfig.setValue(PLAN_STRATEGY_SMALL_FILE_LIMIT, String.valueOf(clusteringSmallFileLimit));
      return this;
    }
    
    public Builder withClusteringSortColumns(String sortColumns) {
      clusteringConfig.setValue(PLAN_STRATEGY_SORT_COLUMNS, sortColumns);
      return this;
    }

    public Builder withClusteringMaxBytesInGroup(long clusteringMaxGroupSize) {
      clusteringConfig.setValue(PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP, String.valueOf(clusteringMaxGroupSize));
      return this;
    }

    public Builder withClusteringMaxNumGroups(int maxNumGroups) {
      clusteringConfig.setValue(PLAN_STRATEGY_MAX_GROUPS, String.valueOf(maxNumGroups));
      return this;
    }

    public Builder withClusteringTargetFileMaxBytes(long targetFileSize) {
      clusteringConfig.setValue(PLAN_STRATEGY_TARGET_FILE_MAX_BYTES, String.valueOf(targetFileSize));
      return this;
    }

    public Builder withInlineClustering(Boolean inlineClustering) {
      clusteringConfig.setValue(INLINE_CLUSTERING, String.valueOf(inlineClustering));
      return this;
    }

    public Builder withInlineClusteringNumCommits(int numCommits) {
      clusteringConfig.setValue(INLINE_CLUSTERING_MAX_COMMITS, String.valueOf(numCommits));
      return this;
    }

    public Builder withAsyncClusteringMaxCommits(int numCommits) {
      clusteringConfig.setValue(ASYNC_CLUSTERING_MAX_COMMITS, String.valueOf(numCommits));
      return this;
    }

    public Builder fromProperties(Properties props) {
      // TODO this should cherry-pick only clustering properties
      this.clusteringConfig.getProps().putAll(props);
      return this;
    }

    public Builder withClusteringUpdatesStrategy(String updatesStrategyClass) {
      clusteringConfig.setValue(UPDATES_STRATEGY, updatesStrategyClass);
      return this;
    }

    public Builder withAsyncClustering(Boolean asyncClustering) {
      clusteringConfig.setValue(ASYNC_CLUSTERING_ENABLE, String.valueOf(asyncClustering));
      return this;
    }

    public Builder withPreserveHoodieCommitMetadata(Boolean preserveHoodieCommitMetadata) {
      clusteringConfig.setValue(PRESERVE_COMMIT_METADATA, String.valueOf(preserveHoodieCommitMetadata));
      return this;
    }

    public Builder withRollbackPendingClustering(Boolean rollbackPendingClustering) {
      clusteringConfig.setValue(ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT, String.valueOf(rollbackPendingClustering));
      return this;
    }

    public Builder withSpaceFillingCurveDataOptimizeEnable(Boolean enable) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withDataOptimizeStrategy(String strategy) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_STRATEGY, strategy);
      return this;
    }

    public Builder withDataOptimizeBuildCurveStrategy(String method) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_CURVE_BUILD_METHOD, method);
      return this;
    }

    public Builder withDataOptimizeBuildCurveSampleNumber(int sampleNumber) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE, String.valueOf(sampleNumber));
      return this;
    }

    public Builder withDataOptimizeDataSkippingEnable(boolean dataSkipping) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_DATA_SKIPPING_ENABLE, String.valueOf(dataSkipping));
      return this;
    }

    public HoodieClusteringConfig build() {
      clusteringConfig.setDefaultValue(
          PLAN_STRATEGY_CLASS_NAME, getDefaultPlanStrategyClassName(engineType));
      clusteringConfig.setDefaultValue(
          EXECUTION_STRATEGY_CLASS_NAME, getDefaultExecutionStrategyClassName(engineType));
      clusteringConfig.setDefaults(HoodieClusteringConfig.class.getName());
      return clusteringConfig;
    }

    private String getDefaultPlanStrategyClassName(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
        case FLINK:
        case JAVA:
          return JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    private String getDefaultExecutionStrategyClassName(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY;
        case FLINK:
        case JAVA:
          return JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }

  /**
   * strategy types for build z-ordering/space-filling curves.
   */
  public enum BuildCurveStrategyType {
    DIRECT("direct"),
    SAMPLE("sample");
    private final String value;

    BuildCurveStrategyType(String value) {
      this.value = value;
    }

    public static BuildCurveStrategyType fromValue(String value) {
      switch (value.toLowerCase(Locale.ROOT)) {
        case "direct":
          return DIRECT;
        case "sample":
          return SAMPLE;
        default:
          throw new HoodieException("Invalid value of Type.");
      }
    }
  }

  /**
   * strategy types for optimize layout for hudi data.
   */
  public enum BuildLayoutOptimizationStrategy {
    ZORDER("z-order"),
    HILBERT("hilbert");
    private final String value;

    BuildLayoutOptimizationStrategy(String value) {
      this.value = value;
    }

    public String toCustomString() {
      return value;
    }

    public static BuildLayoutOptimizationStrategy fromValue(String value) {
      switch (value.toLowerCase(Locale.ROOT)) {
        case "z-order":
          return ZORDER;
        case "hilbert":
          return HILBERT;
        default:
          throw new HoodieException("Invalid value of Type.");
      }
    }
  }
}
