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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.client.clustering.plan.strategy.FlinkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.storage.StoragePath;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.util.StreamerUtil.buildProperties;
import static org.apache.hudi.util.StreamerUtil.createMetaClient;
import static org.apache.hudi.util.StreamerUtil.readConfig;

/**
 * Configurations for Hoodie Flink clustering.
 */
public class FlinkClusteringConfig extends Configuration {

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  // ------------------------------------------------------------------------
  //  Hudi Write Options
  // ------------------------------------------------------------------------

  @Parameter(names = {"--path"}, description = "Base path for the target hoodie table.", required = true)
  public String path;

  // ------------------------------------------------------------------------
  //  Clustering Options
  // ------------------------------------------------------------------------
  @Parameter(names = {"--clustering-delta-commits"}, description = "Max delta commits needed to trigger clustering, default 1 commit")
  public Integer clusteringDeltaCommits = 1;

  @Parameter(names = {"--clustering-tasks"}, description = "Parallelism of tasks that do actual clustering, default is -1")
  public Integer clusteringTasks = -1;

  @Parameter(names = {"--clean-policy"},
      description = "Clean policy to manage the Hudi table. Available option: KEEP_LATEST_COMMITS, KEEP_LATEST_FILE_VERSIONS, KEEP_LATEST_BY_HOURS."
          + "Default is KEEP_LATEST_COMMITS.")
  public String cleanPolicy = HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name();

  @Parameter(names = {"--clean-retain-commits"},
      description = "Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).\n"
          + "This also directly translates into how much you can incrementally pull on this table, default 10")
  public Integer cleanRetainCommits = 10;

  @Parameter(names = {"--clean-retain-hours"},
      description = "Number of hours for which commits need to be retained. This config provides a more flexible option as"
          + "compared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group,"
          + " corresponding to commits with commit times older than the configured number of hours to be retained are cleaned. default 24")
  public Integer cleanRetainHours = 24;

  @Parameter(names = {"--clean-retain-file-versions"},
      description = "Number of file versions to retain. Each file group will be retained for this number of version. default 5")
  public Integer cleanRetainFileVersions = 5;

  @Parameter(names = {"--archive-min-commits"},
      description = "Min number of commits to keep before archiving older commits into a sequential log, default 20.")
  public Integer archiveMinCommits = 20;

  @Parameter(names = {"--archive-max-commits"},
      description = "Max number of commits to keep before archiving older commits into a sequential log, default 30.")
  public Integer archiveMaxCommits = 30;

  @Parameter(names = {"--schedule", "-sc"}, description = "Schedule the clustering plan in this job.\n"
      + "Default is false")
  public Boolean schedule = false;

  @Parameter(names = {"--instant-time", "-it"}, description = "Clustering Instant time")
  public String clusteringInstantTime = null;

  @Parameter(names = {"--clean-async-enabled"}, description = "Whether to cleanup the old commits immediately on new commits, disabled by default")
  public Boolean cleanAsyncEnable = false;

  @Parameter(names = {"--plan-strategy-class"}, description = "Config to provide a strategy class to generator clustering plan")
  public String planStrategyClass = FlinkSizeBasedClusteringPlanStrategy.class.getName();

  @Parameter(names = {"--plan-partition-filter-mode"}, description = "Partition filter mode used in the creation of clustering plan")
  public String planPartitionFilterMode = "NONE";

  @Parameter(names = {"--target-file-max-bytes"}, description = "Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB")
  public Long targetFileMaxBytes = 1024 * 1024 * 1024L;

  @Parameter(names = {"--small-file-limit"}, description = "Files smaller than the size specified here are candidates for clustering, default 600 MB")
  public Long smallFileLimit = 600L;

  @Parameter(names = {"--skip-from-latest-partitions"}, description = "Number of partitions to skip from latest when choosing partitions to create ClusteringPlan, default 0")
  public Integer skipFromLatestPartitions = 0;

  @Parameter(names = {"--sort-columns"}, description = "Columns to sort the data by when clustering.")
  public String sortColumns = "";

  @Parameter(names = {"--sort-memory"}, description = "Sort memory in MB, default 128MB.")
  public Integer sortMemory = 128;

  @Parameter(names = {"--max-num-groups"}, description = "Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism. default 30")
  public Integer maxNumGroups = 30;

  @Parameter(names = {"--target-partitions"}, description = "Number of partitions to list to create ClusteringPlan, default 2")
  public Integer targetPartitions = 2;

  @Parameter(names = {"--cluster-begin-partition"}, description = "Begin partition used to filter partition (inclusive)")
  public String clusterBeginPartition = "";

  @Parameter(names = {"--cluster-end-partition"}, description = "End partition used to filter partition (inclusive)")
  public String clusterEndPartition = "";

  @Parameter(names = {"--partition-regex-pattern"}, description = "Filter clustering partitions that matched regex pattern")
  public String partitionRegexPattern = "";

  @Parameter(names = {"--partition-selected"}, description = "Partitions to run clustering")
  public String partitionSelected = "";

  public static final String SEQ_FIFO = "FIFO";
  public static final String SEQ_LIFO = "LIFO";
  @Parameter(names = {"--seq"}, description = "Clustering plan execution sequence, two options are supported:\n"
      + "1). FIFO: execute the oldest plan first, by default FIFO;\n"
      + "2). LIFO: execute the latest plan first")
  public String clusteringSeq = SEQ_FIFO;

  @Parameter(names = {"--service"}, description = "Flink Clustering runs in service mode, disable by default")
  public Boolean serviceMode = false;

  @Parameter(names = {"--min-clustering-interval-seconds"},
      description = "Min clustering interval of async clustering service, default 10 minutes")
  public Integer minClusteringIntervalSeconds = 600;

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--props\") can also be passed through command line using this parameter.")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
      + "hoodie and hadoop etc.")
  public String propsFilePath = "";

  public static TypedProperties getProps(FlinkClusteringConfig cfg) {
    return cfg.propsFilePath.isEmpty() ? buildProperties(cfg.configs) : readConfig(
        HadoopConfigurations.getHadoopConf(cfg),
        new StoragePath(cfg.propsFilePath),
        cfg.configs).getProps();
  }

  /**
   * Transforms a {@code FlinkClusteringConfig.config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  public static Configuration toFlinkConfig(FlinkClusteringConfig config) {
    Map<String, String> propsMap = new HashMap<String, String>((Map) getProps(config));
    org.apache.flink.configuration.Configuration conf = fromMap(propsMap);

    conf.set(FlinkOptions.PATH, config.path);
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, config.archiveMaxCommits);
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, config.archiveMinCommits);
    conf.set(FlinkOptions.CLEAN_POLICY, config.cleanPolicy);
    conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, config.cleanRetainCommits);
    conf.set(FlinkOptions.CLEAN_RETAIN_HOURS, config.cleanRetainHours);
    conf.set(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS, config.cleanRetainFileVersions);
    conf.set(FlinkOptions.CLUSTERING_DELTA_COMMITS, config.clusteringDeltaCommits);
    conf.set(FlinkOptions.CLUSTERING_TASKS, config.clusteringTasks);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS, config.planStrategyClass);
    conf.set(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME, config.planPartitionFilterMode);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES, config.targetFileMaxBytes);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT, config.smallFileLimit);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, config.skipFromLatestPartitions);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION, config.clusterBeginPartition);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION, config.clusterEndPartition);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN, config.partitionRegexPattern);
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_SELECTED, config.partitionSelected);
    conf.set(FlinkOptions.CLUSTERING_SORT_COLUMNS, config.sortColumns);
    conf.set(FlinkOptions.WRITE_SORT_MEMORY, config.sortMemory);
    conf.set(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS, config.maxNumGroups);
    conf.set(FlinkOptions.CLUSTERING_TARGET_PARTITIONS, config.targetPartitions);
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, config.cleanAsyncEnable);

    // use synchronous clustering always
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, config.schedule);

    // bulk insert conf
    HoodieTableConfig tableConfig = createMetaClient(conf).getTableConfig();
    conf.set(FlinkOptions.URL_ENCODE_PARTITIONING, Boolean.parseBoolean(tableConfig.getUrlEncodePartitioning()));
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable()));

    return conf;
  }
}
