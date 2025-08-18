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
import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
  public static final String FLINK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.FlinkSizeBasedClusteringPlanStrategy";
  public static final String FLINK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy";
  public static final String SPARK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy";
  public static final String JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY =
      "org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy";
  public static final String SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy";
  public static final String SPARK_CONSISTENT_BUCKET_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy";
  public static final String SINGLE_SPARK_JOB_CONSISTENT_HASHING_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.SingleSparkJobConsistentHashingExecutionStrategy";
  public static final String JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY =
      "org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy";
  public static final String PLAN_PARTITION_FILTER_MODE =
      "hoodie.clustering.plan.partition.filter.mode";

  // Any Space-filling curves optimize(z-order/hilbert) params can be saved with this prefix
  private static final String LAYOUT_OPTIMIZE_PARAM_PREFIX = "hoodie.layout.optimize.";

  public static final ConfigProperty<String> DAYBASED_LOOKBACK_PARTITIONS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.lookback.partitions")
      .defaultValue("2")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Number of partitions to list to create ClusteringPlan");

  public static final ConfigProperty<String> PARTITION_FILTER_BEGIN_PARTITION = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "cluster.begin.partition")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Begin partition used to filter partition (inclusive), only effective when the filter mode '"
          + PLAN_PARTITION_FILTER_MODE + "' is " + ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name());

  public static final ConfigProperty<String> PARTITION_FILTER_END_PARTITION = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "cluster.end.partition")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("End partition used to filter partition (inclusive), only effective when the filter mode '"
          + PLAN_PARTITION_FILTER_MODE + "' is " + ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name());

  public static final ConfigProperty<String> PLAN_STRATEGY_SMALL_FILE_LIMIT = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "small.file.limit")
      .defaultValue(String.valueOf(300 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Files smaller than the size in bytes specified here are candidates for clustering");

  public static final ConfigProperty<String> PARTITION_REGEX_PATTERN = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "partition.regex.pattern")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Filter clustering partitions that matched regex pattern");

  public static final ConfigProperty<String> PARTITION_SELECTED = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "partition.selected")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Partitions to run clustering");

  public static final ConfigProperty<String> PLAN_STRATEGY_CLASS_NAME = ConfigProperty
      .key("hoodie.clustering.plan.strategy.class")
      .defaultValue(SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY)
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan "
          + "i.e select what file groups are being clustered. Default strategy, looks at the clustering small file size limit (determined by "
          + PLAN_STRATEGY_SMALL_FILE_LIMIT.key() + ") to pick the small file slices within partitions for clustering.");

  public static final ConfigProperty<String> EXECUTION_STRATEGY_CLASS_NAME = ConfigProperty
      .key("hoodie.clustering.execution.strategy.class")
      .defaultValue(SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY)
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the "
          + " clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while "
          + " meeting the configured target file sizes.");

  public static final ConfigProperty<String> INLINE_CLUSTERING = ConfigProperty
      .key("hoodie.clustering.inline")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Turn on inline clustering - clustering will be run after each write operation is complete")
      .withAlternatives("hoodie.datasource.clustering.inline.enable");

  public static final ConfigProperty<String> INLINE_CLUSTERING_MAX_COMMITS = ConfigProperty
      .key("hoodie.clustering.inline.max.commits")
      .defaultValue("4")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Config to control frequency of clustering planning");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_MAX_COMMITS = ConfigProperty
      .key("hoodie.clustering.async.max.commits")
      .defaultValue("4")
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("Config to control frequency of async clustering");

  public static final ConfigProperty<Integer> CLUSTERING_MAX_PARALLELISM = ConfigProperty
      .key("hoodie.clustering.max.parallelism")
      .defaultValue(15)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum number of parallelism jobs submitted in clustering operation. "
          + "If the resource is sufficient(Like Spark engine has enough idle executors), increasing this "
          + "value will let the clustering job run faster, while it will give additional pressure to the "
          + "execution engines to manage more concurrent running jobs.");

  public static final ConfigProperty<Integer> CLUSTERING_GROUP_READ_PARALLELISM = ConfigProperty
      .key("hoodie.clustering.group.read.parallelism")
      .defaultValue(20)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Maximum number of parallelism when Spark read records from clustering group.");

  public static final ConfigProperty<String> PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.skipfromlatest.partitions")
      .defaultValue("0")
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("Number of partitions to skip from latest when choosing partitions to create ClusteringPlan");

  public static final ConfigProperty<ClusteringPlanPartitionFilterMode> PLAN_PARTITION_FILTER_MODE_NAME = ConfigProperty
      .key(PLAN_PARTITION_FILTER_MODE)
      .defaultValue(ClusteringPlanPartitionFilterMode.NONE)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation(ClusteringPlanPartitionFilterMode.class);

  public static final ConfigProperty<String> PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.bytes.per.group")
      .defaultValue(String.valueOf(2 * 1024 * 1024 * 1024L))
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Each clustering operation can create multiple output file groups. Total amount of data processed by clustering operation"
          + " is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS)."
          + " Max amount of data to be included in one group");

  public static final ConfigProperty<String> PLAN_STRATEGY_MAX_GROUPS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.num.groups")
      .defaultValue("30")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism");

  public static final ConfigProperty<String> PLAN_STRATEGY_TARGET_FILE_MAX_BYTES = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "target.file.max.bytes")
      .defaultValue(String.valueOf(1024 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups");

  public static final ConfigProperty<Boolean> PLAN_STRATEGY_SINGLE_GROUP_CLUSTERING_ENABLED = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "single.group.clustering.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Whether to generate clustering plan when there is only one file group involved, by default true");

  public static final ConfigProperty<String> PLAN_STRATEGY_SORT_COLUMNS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Columns to sort the data by when clustering");

  public static final ConfigProperty<String> UPDATES_STRATEGY = ConfigProperty
      .key("hoodie.clustering.updates.strategy")
      .defaultValue("org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Determines how to handle updates, deletes to file groups that are under clustering."
          + " Default strategy just rejects the update");

  public static final ConfigProperty<String> SCHEDULE_INLINE_CLUSTERING = ConfigProperty
      .key("hoodie.clustering.schedule.inline")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("When set to true, clustering service will be attempted for inline scheduling after each write. Users have to ensure "
          + "they have a separate job to run async clustering(execution) for the one scheduled by this writer. Users can choose to set both "
          + "`hoodie.clustering.inline` and `hoodie.clustering.schedule.inline` to false and have both scheduling and execution triggered by any async process, on which "
          + "case `hoodie.clustering.async.enabled` is expected to be set to true. But if `hoodie.clustering.inline` is set to false, and `hoodie.clustering.schedule.inline` "
          + "is set to true, regular writers will schedule clustering inline, but users are expected to trigger async job for execution. If `hoodie.clustering.inline` is set "
          + "to true, regular writers will do both scheduling and execution inline for clustering");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_ENABLE = ConfigProperty
      .key("hoodie.clustering.async.enabled")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Enable running of clustering service, asynchronously as inserts happen on the table.")
      .withAlternatives("hoodie.datasource.clustering.async.enable");

  /**
   * @deprecated this setting has no effect. Please refer to clustering configuration, as well as
   * {@link #LAYOUT_OPTIMIZE_STRATEGY} config to enable advanced record layout optimization strategies
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_ENABLE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .deprecatedAfter("0.11.0")
      .withDocumentation("This setting has no effect. Please refer to clustering configuration, as well as "
          + "LAYOUT_OPTIMIZE_STRATEGY config to enable advanced record layout optimization strategies");

  /**
   * Determines ordering strategy in for records layout optimization.
   * Currently, following strategies are supported
   * <ul>
   *   <li>Linear: simply orders records lexicographically</li>
   *   <li>Z-order: orders records along Z-order spatial-curve</li>
   *   <li>Hilbert: orders records along Hilbert's spatial-curve</li>
   * </ul>
   *
   * NOTE: "z-order", "hilbert" strategies may consume considerably more compute, than "linear".
   *       Make sure to perform small-scale local testing for your dataset before applying globally.
   */
  public static final ConfigProperty<String> LAYOUT_OPTIMIZE_STRATEGY = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "strategy")
      .defaultValue(LayoutOptimizationStrategy.LINEAR.name())
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation(LayoutOptimizationStrategy.class);

  /**
   * NOTE: This setting only has effect if {@link #LAYOUT_OPTIMIZE_STRATEGY} value is set to
   *       either "z-order" or "hilbert" (ie leveraging space-filling curves)
   *
   * Currently, two methods to order records along the curve are supported "build" and "sample":
   *
   * <ul>
   *   <li>Direct: entails that spatial curve will be built in full, "filling in" all of the individual
   *   points corresponding to each individual record</li>
   *   <li>Sample: leverages boundary-base interleaved index method (described in more details in
   *   Amazon DynamoDB blog [1])</li>
   * </ul>
   *
   * NOTE: Boundary-based interleaved Index method has better generalization,
   *       but is slower than direct method.
   *
   * Please refer to RFC-28 for specific elaboration on both flows.
   *
   * [1] https://aws.amazon.com/cn/blogs/database/tag/z-order/
   */
  public static final ConfigProperty<String> LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "curve.build.method")
      .defaultValue(SpatialCurveCompositionStrategyType.DIRECT.name())
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation(SpatialCurveCompositionStrategyType.class);

  /**
   * NOTE: This setting only has effect if {@link #LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD} value
   *       is set to "sample"
   *
   * Determines target sample size used by the Boundary-based Interleaved Index method.
   * Larger sample size entails better layout optimization outcomes, at the expense of higher memory
   * footprint.
   */
  public static final ConfigProperty<String> LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "build.curve.sample.size")
      .defaultValue("200000")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("Determines target sample size used by the Boundary-based Interleaved Index method "
          + "of building space-filling curve. Larger sample size entails better layout optimization outcomes, "
          + "at the expense of higher memory footprint.");

  /**
   * @deprecated this setting has no effect
   */
  public static final ConfigProperty LAYOUT_OPTIMIZE_DATA_SKIPPING_ENABLE = ConfigProperty
      .key(LAYOUT_OPTIMIZE_PARAM_PREFIX + "data.skipping.enable")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .deprecatedAfter("0.11.0")
      .withDocumentation("Enable data skipping by collecting statistics once layout optimization is complete.");

  public static final ConfigProperty<Boolean> ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT = ConfigProperty
      .key("hoodie.clustering.rollback.pending.replacecommit.on.conflict")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("If updates are allowed to file groups pending clustering, then set this config to rollback failed or pending clustering instants. "
          + "Pending clustering will be rolled back ONLY IF there is conflict between incoming upsert and filegroup to be clustered. "
          + "Please exercise caution while setting this config, especially when clustering is done very frequently. This could lead to race condition in "
          + "rare scenarios, for example, when the clustering completes after instants are fetched but before rollback completed.");

  public static final ConfigProperty<Boolean> FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "binary.copy.schema.evolution.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Enable schema evolution support for binary file stitching during clustering. "
          + "When enabled, allows clustering of files with different but compatible schemas (e.g., files with added columns). "
          + "When disabled (default), only files with identical schemas will be clustered together, providing better performance "
          + "but requiring schema consistency across all files in a clustering group.");

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

    public Builder withSingleGroupClusteringEnabled(Boolean enabled) {
      clusteringConfig.setValue(PLAN_STRATEGY_SINGLE_GROUP_CLUSTERING_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode mode) {
      clusteringConfig.setValue(PLAN_PARTITION_FILTER_MODE_NAME.key(), mode.toString());
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

    public Builder withClusteringPartitionRegexPattern(String pattern) {
      clusteringConfig.setValue(PARTITION_REGEX_PATTERN, pattern);
      return this;
    }

    public Builder withClusteringPartitionSelected(String partitionSelected) {
      clusteringConfig.setValue(PARTITION_SELECTED, partitionSelected);
      return this;
    }

    public Builder withClusteringSkipPartitionsFromLatest(int clusteringSkipPartitionsFromLatest) {
      clusteringConfig.setValue(PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, String.valueOf(clusteringSkipPartitionsFromLatest));
      return this;
    }

    public Builder withClusteringPartitionFilterBeginPartition(String begin) {
      clusteringConfig.setValue(PARTITION_FILTER_BEGIN_PARTITION, begin);
      return this;
    }

    public Builder withClusteringPartitionFilterEndPartition(String end) {
      clusteringConfig.setValue(PARTITION_FILTER_END_PARTITION, end);
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

    public Builder withScheduleInlineClustering(Boolean scheduleInlineClustering) {
      clusteringConfig.setValue(SCHEDULE_INLINE_CLUSTERING, String.valueOf(scheduleInlineClustering));
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

    public Builder withRollbackPendingClustering(Boolean rollbackPendingClustering) {
      clusteringConfig.setValue(ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT, String.valueOf(rollbackPendingClustering));
      return this;
    }

    public Builder withFileStitchingBinaryCopySchemaEvolutionEnabled(Boolean enabled) {
      clusteringConfig.setValue(FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withDataOptimizeStrategy(String strategy) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_STRATEGY, strategy);
      return this;
    }

    public Builder withDataOptimizeBuildCurveStrategy(String method) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD, method);
      return this;
    }

    public Builder withDataOptimizeBuildCurveSampleNumber(int sampleNumber) {
      clusteringConfig.setValue(LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE, String.valueOf(sampleNumber));
      return this;
    }

    public HoodieClusteringConfig build() {
      setDefaults();
      validate();

      return clusteringConfig;
    }

    private void setDefaults() {
      clusteringConfig.setDefaultValue(PLAN_STRATEGY_CLASS_NAME, getDefaultPlanStrategyClassName(engineType));
      clusteringConfig.setDefaultValue(EXECUTION_STRATEGY_CLASS_NAME, getDefaultExecutionStrategyClassName(engineType));
      clusteringConfig.setDefaults(HoodieClusteringConfig.class.getName());
    }

    private void validate() {
      boolean inlineCluster = clusteringConfig.getBoolean(HoodieClusteringConfig.INLINE_CLUSTERING);
      boolean inlineClusterSchedule = clusteringConfig.getBoolean(HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING);
      ValidationUtils.checkArgument(!(inlineCluster && inlineClusterSchedule), String.format("Either of inline clustering (%s) or "
              + "schedule inline clustering (%s) can be enabled. Both can't be set to true at the same time. %s,%s", HoodieClusteringConfig.INLINE_CLUSTERING.key(),
          HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING.key(), inlineCluster, inlineClusterSchedule));

      if (isConsistentHashingBucketIndex()) {
        String planStrategy = clusteringConfig.getString(PLAN_STRATEGY_CLASS_NAME);
        if (engineType == EngineType.FLINK) {
          ValidationUtils.checkArgument(planStrategy.equalsIgnoreCase(FLINK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY),
              "Consistent hashing bucket index only supports clustering plan strategy : " + FLINK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY);
        } else {
          ValidationUtils.checkArgument(
              planStrategy.equalsIgnoreCase(SPARK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY),
              "Consistent hashing bucket index only supports clustering plan strategy : " + SPARK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY);
          String clusteringConfigString = clusteringConfig.getString(EXECUTION_STRATEGY_CLASS_NAME);
          ValidationUtils.checkArgument(
              clusteringConfigString.equals(SPARK_CONSISTENT_BUCKET_EXECUTION_STRATEGY) || clusteringConfigString.equals(SINGLE_SPARK_JOB_CONSISTENT_HASHING_EXECUTION_STRATEGY),
              "Consistent hashing bucket index only supports clustering execution strategy : " + SPARK_CONSISTENT_BUCKET_EXECUTION_STRATEGY + " or "
                  + SINGLE_SPARK_JOB_CONSISTENT_HASHING_EXECUTION_STRATEGY);
        }
      }
    }

    private boolean isConsistentHashingBucketIndex() {
      return clusteringConfig.contains(HoodieIndexConfig.INDEX_TYPE.key())
          && clusteringConfig.contains(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key())
          && clusteringConfig.getString(HoodieIndexConfig.INDEX_TYPE.key()).equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name())
          && clusteringConfig.getString(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key()).equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
    }

    private String getDefaultPlanStrategyClassName(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return isConsistentHashingBucketIndex() ? SPARK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY : SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
        case FLINK:
          return isConsistentHashingBucketIndex() ? FLINK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY : FLINK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
        case JAVA:
          return JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    private String getDefaultExecutionStrategyClassName(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return isConsistentHashingBucketIndex() ? SINGLE_SPARK_JOB_CONSISTENT_HASHING_EXECUTION_STRATEGY : SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY;
        case FLINK:
        case JAVA:
          return JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }

  /**
   * Type of strategy for building Z-order/Hilbert space-filling curves.
   */
  @EnumDescription("This configuration only has effect if `hoodie.layout.optimize.strategy` is "
      + "set to either \"z-order\" or \"hilbert\" (i.e. leveraging space-filling curves). This "
      + "configuration controls the type of a strategy to use for building the space-filling "
      + "curves, tackling specifically how the Strings are ordered based on the curve. "
      + "Since we truncate the String to 8 bytes for ordering, there are two issues: (1) it "
      + "can lead to poor aggregation effect, (2) the truncation of String longer than 8 bytes "
      + "loses the precision, if the Strings are different but the 8-byte prefix is the same. "
      + "The boundary-based interleaved index method (\"SAMPLE\") has better generalization, "
      + "solving the two problems above, but is slower than direct method (\"DIRECT\"). "
      + "User should benchmark the write and query performance before tweaking this in "
      + "production, if this is actually a problem. Please refer to RFC-28 for more details.")
  public enum SpatialCurveCompositionStrategyType {

    @EnumFieldDescription("This strategy builds the spatial curve in full, filling in all of "
        + "the individual points corresponding to each individual record, which requires less "
        + "compute.")
    DIRECT,

    @EnumFieldDescription("This strategy leverages boundary-base interleaved index method "
        + "(described in more details in Amazon DynamoDB blog "
        + "https://aws.amazon.com/cn/blogs/database/tag/z-order/) and produces a better layout "
        + "compared to DIRECT strategy.  It requires more compute and is slower.")
    SAMPLE
  }

  /**
   * Layout optimization strategies such as Z-order/Hilbert space-curves, etc
   */
  @EnumDescription("Determines ordering strategy for records layout optimization.")
  public enum LayoutOptimizationStrategy {
    @EnumFieldDescription("Orders records lexicographically")
    LINEAR,

    @EnumFieldDescription("Orders records along Z-order spatial-curve.")
    ZORDER,

    @EnumFieldDescription("Orders records along Hilbert's spatial-curve.")
    HILBERT
  }

  public static LayoutOptimizationStrategy resolveLayoutOptimizationStrategy(String cfgVal) {
    if (cfgVal.equalsIgnoreCase("z-order")) {
      return LayoutOptimizationStrategy.ZORDER;
    }
    return LayoutOptimizationStrategy.valueOf(cfgVal.toUpperCase());
  }
}
