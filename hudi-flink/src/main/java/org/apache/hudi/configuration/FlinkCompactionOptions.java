/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;

/**
 * Flink sql indexing related config.
 */
@ConfigClassProperty(name = "Flink Compaction Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink compaction behavior on Hudi tables are listed below.")
public class FlinkCompactionOptions {

  public static final ConfigOption<Boolean> COMPACTION_SCHEDULE_ENABLED = ConfigOptions
      .key("compaction.schedule.enabled")
      .booleanType()
      .defaultValue(true) // default true for MOR write
      .withDescription("Schedule the compaction plan, enabled by default for MOR");

  public static final ConfigOption<Boolean> COMPACTION_ASYNC_ENABLED = ConfigOptions
      .key("compaction.async.enabled")
      .booleanType()
      .defaultValue(true) // default true for MOR write
      .withDescription("Async Compaction, enabled by default for MOR");

  public static final ConfigOption<Integer> COMPACTION_TASKS = ConfigOptions
      .key("compaction.tasks")
      .intType()
      .defaultValue(10) // default WRITE_TASKS * COMPACTION_DELTA_COMMITS * 0.5 (assumes two commits generate one bucket)
      .withDescription("Parallelism of tasks that do actual compaction, default is 10");

  public static final String NUM_COMMITS = "num_commits";
  public static final String TIME_ELAPSED = "time_elapsed";
  public static final String NUM_AND_TIME = "num_and_time";
  public static final String NUM_OR_TIME = "num_or_time";
  public static final ConfigOption<String> COMPACTION_TRIGGER_STRATEGY = ConfigOptions
      .key("compaction.trigger.strategy")
      .stringType()
      .defaultValue(NUM_COMMITS) // default true for MOR write
      .withDescription("Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits;\n"
          + "'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction;\n"
          + "'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;\n"
          + "'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.\n"
          + "Default is 'num_commits'");

  public static final ConfigOption<Integer> COMPACTION_DELTA_COMMITS = ConfigOptions
      .key("compaction.delta_commits")
      .intType()
      .defaultValue(5)
      .withDescription("Max delta commits needed to trigger compaction, default 5 commits");

  public static final ConfigOption<Integer> COMPACTION_DELTA_SECONDS = ConfigOptions
      .key("compaction.delta_seconds")
      .intType()
      .defaultValue(3600) // default 1 hour
      .withDescription("Max delta seconds time needed to trigger compaction, default 1 hour");

  public static final ConfigOption<Integer> COMPACTION_MAX_MEMORY = ConfigOptions
      .key("compaction.max_memory")
      .intType()
      .defaultValue(100) // default 100 MB
      .withDescription("Max memory in MB for compaction spillable map, default 100MB");

  public static final ConfigOption<Long> COMPACTION_TARGET_IO = ConfigOptions
      .key("compaction.target_io")
      .longType()
      .defaultValue(5120L) // default 5 GB
      .withDescription("Target IO per compaction (both read and write), default 5 GB");

  public static final ConfigOption<Boolean> CLEAN_ASYNC_ENABLED = ConfigOptions
      .key("clean.async.enabled")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to cleanup the old commits immediately on new commits, enabled by default");

  public static final ConfigOption<Integer> CLEAN_RETAIN_COMMITS = ConfigOptions
      .key("clean.retain_commits")
      .intType()
      .defaultValue(10)// default 10 commits
      .withDescription("Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).\n"
          + "This also directly translates into how much you can incrementally pull on this table, default 10");

  public static final ConfigOption<Integer> ARCHIVE_MAX_COMMITS = ConfigOptions
      .key("archive.max_commits")
      .intType()
      .defaultValue(30)// default max 30 commits
      .withDescription("Max number of commits to keep before archiving older commits into a sequential log, default 30");

  public static final ConfigOption<Integer> ARCHIVE_MIN_COMMITS = ConfigOptions
      .key("archive.min_commits")
      .intType()
      .defaultValue(20)// default min 20 commits
      .withDescription("Min number of commits to keep before archiving older commits into a sequential log, default 20");
}
