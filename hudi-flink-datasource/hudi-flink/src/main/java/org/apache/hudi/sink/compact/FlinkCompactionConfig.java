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

package org.apache.hudi.sink.compact;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategy;
import org.apache.hudi.storage.StoragePath;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.util.StreamerUtil.buildProperties;
import static org.apache.hudi.util.StreamerUtil.readConfig;

/**
 * Configurations for Hoodie Flink compaction.
 */
public class FlinkCompactionConfig extends Configuration {

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  // ------------------------------------------------------------------------
  //  Hudi Write Options
  // ------------------------------------------------------------------------

  @Parameter(names = {"--path"}, description = "Base path for the target hoodie table.", required = true)
  public String path;

  // ------------------------------------------------------------------------
  //  Compaction Options
  // ------------------------------------------------------------------------

  public static final String NUM_COMMITS = "num_commits";
  public static final String TIME_ELAPSED = "time_elapsed";
  public static final String NUM_AND_TIME = "num_and_time";
  public static final String NUM_OR_TIME = "num_or_time";
  @Parameter(names = {"--compaction-trigger-strategy"},
      description = "Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits;\n"
          + "'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction;\n"
          + "'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;\n"
          + "'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.\n"
          + "Default is 'num_commits'")
  public String compactionTriggerStrategy = NUM_COMMITS;

  @Parameter(names = {"--disable-file-group-reader"}, description = "Whether to disable file group reader based compaction, false by default")
  public Boolean fileGroupReaderDisabled = false;

  @Parameter(names = {"--compaction-delta-commits"}, description = "Max delta commits needed to trigger compaction, default 1 commit")
  public Integer compactionDeltaCommits = 1;

  @Parameter(names = {"--compaction-delta-seconds"}, description = "Max delta seconds time needed to trigger compaction, default 1 hour")
  public Integer compactionDeltaSeconds = 3600;

  @Parameter(names = {"--clean-async-enabled"}, description = "Whether to cleanup the old commits immediately on new commits, enabled by default")
  public Boolean cleanAsyncEnable = false;

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

  @Parameter(names = {"--compaction-max-memory"}, description = "Max memory in MB for compaction spillable map, default 100MB.")
  public Integer compactionMaxMemory = 100;

  @Parameter(names = {"--compaction-target-io"}, description = "Target IO per compaction (both read and write) for batching compaction, default 512000M.")
  public Long compactionTargetIo = 512000L;

  @Parameter(names = {"--compaction-tasks"}, description = "Parallelism of tasks that do actual compaction, default is -1")
  public Integer compactionTasks = -1;

  @Parameter(names = {"--schedule", "-sc"}, description = "Not recommended. Schedule the compaction plan in this job.\n"
      + "There is a risk of losing data when scheduling compaction outside the writer job.\n"
      + "Scheduling compaction in the writer job and only let this job do the compaction execution is recommended.\n"
      + "Default is false")
  public Boolean schedule = false;

  public static final String SEQ_FIFO = "FIFO";
  public static final String SEQ_LIFO = "LIFO";
  @Parameter(names = {"--seq"}, description = "Compaction plan execution sequence, two options are supported:\n"
      + "1). FIFO: execute the oldest plan first, by default FIFO;\n"
      + "2). LIFO: execute the latest plan first")
  public String compactionSeq = SEQ_FIFO;

  @Parameter(names = {"--service"}, description = "Flink Compaction runs in service mode, disable by default")
  public Boolean serviceMode = false;

  @Parameter(names = {"--min-compaction-interval-seconds"},
      description = "Min compaction interval of async compaction service, default 10 minutes")
  public Integer minCompactionIntervalSeconds = 600;

  @Parameter(names = {"--plan-select-strategy"}, description = "The strategy define how to select compaction plan to compact.\n"
      + "1). num_instants: select plans by specific number of instants, it's the default strategy with 1 instant at a time;\n"
      + "3). all: Select all pending compaction plan;\n"
      + "4). instants: Select the compaction plan by specific instants")
  public String compactionPlanSelectStrategy = CompactionPlanStrategy.NUM_INSTANTS;

  @Parameter(names = {"--max-num-plans"}, description = "Max number of compaction plan would be selected in compaction."
      + "It's only effective for MultiCompactionPlanSelectStrategy.")
  public Integer maxNumCompactionPlans = 1;

  @Parameter(names = {"--target-instants"}, description = "Specify the compaction plan instants to compact,\n"
      + "Multiple instants are supported by comma separated instant time.\n"
      + "It's only effective for 'instants' plan selection strategy.")
  public String compactionPlanInstant;

  @Parameter(names = {"--spillable_map_path"}, description = "Default file path prefix for spillable map.")
  public String spillableMapPath = FileIOUtils.getDefaultSpillableMapBasePath();

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--props\") can also be passed through command line using this parameter.")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
      + "hoodie and hadoop etc.")
  public String propsFilePath = "";

  public static TypedProperties getProps(FlinkCompactionConfig cfg) {
    return cfg.propsFilePath.isEmpty() ? buildProperties(cfg.configs) : readConfig(
        HadoopConfigurations.getHadoopConf(cfg),
        new StoragePath(cfg.propsFilePath),
        cfg.configs).getProps();
  }

  /**
   * Transforms a {@code HoodieFlinkCompaction.config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  public static org.apache.flink.configuration.Configuration toFlinkConfig(FlinkCompactionConfig config) {
    Map<String, String> propsMap = new HashMap<String, String>((Map) getProps(config));
    org.apache.flink.configuration.Configuration conf = fromMap(propsMap);

    conf.set(FlinkOptions.PATH, config.path);
    conf.set(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, config.compactionTriggerStrategy);
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, config.archiveMaxCommits);
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, config.archiveMinCommits);
    conf.set(FlinkOptions.CLEAN_POLICY, config.cleanPolicy);
    conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, config.cleanRetainCommits);
    conf.set(FlinkOptions.CLEAN_RETAIN_HOURS, config.cleanRetainHours);
    conf.set(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS, config.cleanRetainFileVersions);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, config.compactionDeltaCommits);
    conf.set(FlinkOptions.COMPACTION_DELTA_SECONDS, config.compactionDeltaSeconds);
    conf.set(FlinkOptions.COMPACTION_MAX_MEMORY, config.compactionMaxMemory);
    // used as compaction memory by file group reader based compaction
    conf.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, config.compactionMaxMemory);
    conf.set(FlinkOptions.COMPACTION_TARGET_IO, config.compactionTargetIo);
    conf.set(FlinkOptions.COMPACTION_TASKS, config.compactionTasks);
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, config.cleanAsyncEnable);
    // use synchronous compaction always
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, config.schedule);
    // Map memory
    conf.setString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), config.spillableMapPath);
    // set file group reader
    conf.setString(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), !config.fileGroupReaderDisabled + "");

    return conf;
  }
}
