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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.Configuration;

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
  @Parameter(names = {"--clustering-delta-commits"}, description = "Max delta commits needed to trigger clustering, default 4 commits", required = false)
  public Integer clusteringDeltaCommits = 1;

  @Parameter(names = {"--clustering-tasks"}, description = "Parallelism of tasks that do actual clustering, default is -1", required = false)
  public Integer clusteringTasks = -1;

  @Parameter(names = {"--compaction-max-memory"}, description = "Max memory in MB for compaction spillable map, default 100MB.", required = false)
  public Integer compactionMaxMemory = 100;

  @Parameter(names = {"--clean-retain-commits"},
      description = "Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).\n"
          + "This also directly translates into how much you can incrementally pull on this table, default 10",
      required = false)
  public Integer cleanRetainCommits = 10;

  @Parameter(names = {"--archive-min-commits"},
      description = "Min number of commits to keep before archiving older commits into a sequential log, default 20.",
      required = false)
  public Integer archiveMinCommits = 20;

  @Parameter(names = {"--archive-max-commits"},
      description = "Max number of commits to keep before archiving older commits into a sequential log, default 30.",
      required = false)
  public Integer archiveMaxCommits = 30;

  @Parameter(names = {"--schedule", "-sc"}, description = "Not recommended. Schedule the clustering plan in this job.\n"
      + "There is a risk of losing data when scheduling clustering outside the writer job.\n"
      + "Scheduling clustering in the writer job and only let this job do the clustering execution is recommended.\n"
      + "Default is true", required = false)
  public Boolean schedule = true;

  @Parameter(names = {"--clean-async-enabled"}, description = "Whether to cleanup the old commits immediately on new commits, enabled by default", required = false)
  public Boolean cleanAsyncEnable = false;

  @Parameter(names = {"--plan-strategy-class"}, description = "Config to provide a strategy class to generator clustering plan", required = false)
  public String planStrategyClass = FlinkSizeBasedClusteringPlanStrategy.class.getName();

  @Parameter(names = {"--plan-partition-filter-mode"}, description = "Partition filter mode used in the creation of clustering plan", required = false)
  public String planPartitionFilterMode = "NONE";

  @Parameter(names = {"--target-file-max-bytes"}, description = "Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB", required = false)
  public Integer targetFileMaxBytes = 1024 * 1024 * 1024;

  @Parameter(names = {"--small-file-limit"}, description = "Files smaller than the size specified here are candidates for clustering, default 600 MB", required = false)
  public Integer smallFileLimit = 600;

  @Parameter(names = {"--skip-from-latest-partitions"}, description = "Number of partitions to skip from latest when choosing partitions to create ClusteringPlan, default 0", required = false)
  public Integer skipFromLatestPartitions = 0;

  @Parameter(names = {"--sort-columns"}, description = "Columns to sort the data by when clustering.", required = false)
  public String sortColumns = "";

  @Parameter(names = {"--max-num-groups"}, description = "Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism. default 30", required = false)
  public Integer maxNumGroups = 30;

  @Parameter(names = {"--target-partitions"}, description = "Number of partitions to list to create ClusteringPlan, default 2", required = false)
  public Integer targetPartitions = 2;

  public static final String SEQ_FIFO = "FIFO";
  public static final String SEQ_LIFO = "LIFO";
  @Parameter(names = {"--seq"}, description = "Clustering plan execution sequence, two options are supported:\n"
      + "1). FIFO: execute the oldest plan first;\n"
      + "2). LIFO: execute the latest plan first, by default FIFO", required = false)
  public String clusteringSeq = SEQ_FIFO;

  @Parameter(names = {"--service"}, description = "Flink Clustering runs in service mode, disable by default")
  public Boolean serviceMode = false;

  @Parameter(names = {"--min-clustering-interval-seconds"},
      description = "Min clustering interval of async clustering service, default 10 minutes")
  public Integer minClusteringIntervalSeconds = 600;

  /**
   * Transforms a {@code FlinkClusteringConfig.config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  public static Configuration toFlinkConfig(FlinkClusteringConfig config) {
    Configuration conf = new Configuration();

    conf.setString(FlinkOptions.PATH, config.path);
    conf.setInteger(FlinkOptions.ARCHIVE_MAX_COMMITS, config.archiveMaxCommits);
    conf.setInteger(FlinkOptions.ARCHIVE_MIN_COMMITS, config.archiveMinCommits);
    conf.setInteger(FlinkOptions.CLEAN_RETAIN_COMMITS, config.cleanRetainCommits);
    conf.setInteger(FlinkOptions.COMPACTION_MAX_MEMORY, config.compactionMaxMemory);
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, config.clusteringDeltaCommits);
    conf.setInteger(FlinkOptions.CLUSTERING_TASKS, config.clusteringTasks);
    conf.setString(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS, config.planStrategyClass);
    conf.setString(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME, config.planPartitionFilterMode);
    conf.setInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES, config.targetFileMaxBytes);
    conf.setInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT, config.smallFileLimit);
    conf.setInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, config.skipFromLatestPartitions);
    conf.setString(FlinkOptions.CLUSTERING_SORT_COLUMNS, config.sortColumns);
    conf.setInteger(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS, config.maxNumGroups);
    conf.setInteger(FlinkOptions.CLUSTERING_TARGET_PARTITIONS, config.targetPartitions);
    conf.setBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED, config.cleanAsyncEnable);

    // use synchronous clustering always
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, config.schedule);

    // bulk insert conf
    HoodieTableConfig tableConfig = StreamerUtil.createMetaClient(conf).getTableConfig();
    conf.setBoolean(FlinkOptions.URL_ENCODE_PARTITIONING, Boolean.parseBoolean(tableConfig.getUrlEncodePartitioning()));
    conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable()));

    return conf;
  }
}
