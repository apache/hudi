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
import org.apache.hudi.common.model.HoodieAvroRecordMerge;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.action.clean.CleaningTriggerStrategy;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Compaction related config.
 */
@Immutable
@ConfigClassProperty(name = "Compaction Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control compaction "
        + "(merging of log files onto a new base files) as well as  "
        + "cleaning (reclamation of older/unused file groups/slices).")
public class HoodieCompactionConfig extends HoodieConfig {

  public static final ConfigProperty<String> AUTO_ARCHIVE = ConfigProperty
      .key("hoodie.archive.automatic")
      .defaultValue("true")
      .withDocumentation("When enabled, the archival table service is invoked immediately after each commit,"
          + " to archive commits if we cross a maximum value of commits."
          + " It's recommended to enable this, to ensure number of active commits is bounded.");

  public static final ConfigProperty<String> ASYNC_ARCHIVE = ConfigProperty
      .key("hoodie.archive.async")
      .defaultValue("false")
      .sinceVersion("0.11.0")
      .withDocumentation("Only applies when " + AUTO_ARCHIVE.key() + " is turned on. "
          + "When turned on runs archiver async with writing, which can speed up overall write performance.");

  public static final ConfigProperty<String> AUTO_CLEAN = ConfigProperty
      .key("hoodie.clean.automatic")
      .defaultValue("true")
      .withDocumentation("When enabled, the cleaner table service is invoked immediately after each commit,"
          + " to delete older file slices. It's recommended to enable this, to ensure metadata and data storage"
          + " growth is bounded.");

  public static final ConfigProperty<String> ASYNC_CLEAN = ConfigProperty
      .key("hoodie.clean.async")
      .defaultValue("false")
      .withDocumentation("Only applies when " + AUTO_CLEAN.key() + " is turned on. "
          + "When turned on runs cleaner async with writing, which can speed up overall write performance.");

  public static final ConfigProperty<String> CLEANER_COMMITS_RETAINED = ConfigProperty
      .key("hoodie.cleaner.commits.retained")
      .defaultValue("10")
      .withDocumentation("Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits "
          + "(scheduled). This also directly translates into how much data retention the table supports for incremental queries.");

  public static final ConfigProperty<String> CLEANER_HOURS_RETAINED = ConfigProperty.key("hoodie.cleaner.hours.retained")
          .defaultValue("24")
          .withDocumentation("Number of hours for which commits need to be retained. This config provides a more flexible option as"
          + "compared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group,"
                  + " corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.");

  public static final ConfigProperty<String> CLEANER_POLICY = ConfigProperty
      .key("hoodie.cleaner.policy")
      .defaultValue(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
      .withDocumentation("Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space."
          + " By default, cleaner spares the file slices written by the last N commits, determined by  " + CLEANER_COMMITS_RETAINED.key()
          + " Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had"
          + "   a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time");

  public static final ConfigProperty<String> INLINE_COMPACT = ConfigProperty
      .key("hoodie.compact.inline")
      .defaultValue("false")
      .withDocumentation("When set to true, compaction service is triggered after each write. While being "
          + " simpler operationally, this adds extra latency on the write path.");

  public static final ConfigProperty<String> SCHEDULE_INLINE_COMPACT = ConfigProperty
      .key("hoodie.compact.schedule.inline")
      .defaultValue("false")
      .withDocumentation("When set to true, compaction service will be attempted for inline scheduling after each write. Users have to ensure "
          + "they have a separate job to run async compaction(execution) for the one scheduled by this writer. Users can choose to set both "
          + "`hoodie.compact.inline` and `hoodie.compact.schedule.inline` to false and have both scheduling and execution triggered by any async process. "
          + "But if `hoodie.compact.inline` is set to false, and `hoodie.compact.schedule.inline` is set to true, regular writers will schedule compaction inline, "
          + "but users are expected to trigger async job for execution. If `hoodie.compact.inline` is set to true, regular writers will do both scheduling and "
          + "execution inline for compaction");

  public static final ConfigProperty<String> INLINE_COMPACT_NUM_DELTA_COMMITS = ConfigProperty
      .key("hoodie.compact.inline.max.delta.commits")
      .defaultValue("5")
      .withDocumentation("Number of delta commits after the last compaction, before scheduling of a new compaction is attempted.");

  public static final ConfigProperty<String> INLINE_COMPACT_TIME_DELTA_SECONDS = ConfigProperty
      .key("hoodie.compact.inline.max.delta.seconds")
      .defaultValue(String.valueOf(60 * 60))
      .withDocumentation("Number of elapsed seconds after the last compaction, before scheduling a new one.");

  public static final ConfigProperty<String> INLINE_COMPACT_TRIGGER_STRATEGY = ConfigProperty
      .key("hoodie.compact.inline.trigger.strategy")
      .defaultValue(CompactionTriggerStrategy.NUM_COMMITS.name())
      .withDocumentation("Controls how compaction scheduling is triggered, by time or num delta commits or combination of both. "
          + "Valid options: " + Arrays.stream(CompactionTriggerStrategy.values()).map(Enum::name).collect(Collectors.joining(",")));

  public static final ConfigProperty<String> CLEAN_TRIGGER_STRATEGY = ConfigProperty
          .key("hoodie.clean.trigger.strategy")
          .defaultValue(CleaningTriggerStrategy.NUM_COMMITS.name())
          .withDocumentation("Controls how cleaning is scheduled. Valid options: "
                  + Arrays.stream(CleaningTriggerStrategy.values()).map(Enum::name).collect(Collectors.joining(",")));

  public static final ConfigProperty<String> CLEAN_MAX_COMMITS = ConfigProperty
          .key("hoodie.clean.max.commits")
          .defaultValue("1")
          .withDocumentation("Number of commits after the last clean operation, before scheduling of a new clean is attempted.");

  public static final ConfigProperty<String> CLEANER_FILE_VERSIONS_RETAINED = ConfigProperty
      .key("hoodie.cleaner.fileversions.retained")
      .defaultValue("3")
      .withDocumentation("When " + HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name() + " cleaning policy is used, "
          + " the minimum number of file slices to retain in each file group, during cleaning.");

  public static final ConfigProperty<String> CLEANER_INCREMENTAL_MODE_ENABLE = ConfigProperty
      .key("hoodie.cleaner.incremental.mode")
      .defaultValue("true")
      .withDocumentation("When enabled, the plans for each cleaner service run is computed incrementally off the events "
          + " in the timeline, since the last cleaner run. This is much more efficient than obtaining listings for the full"
          + " table for each planning (even with a metadata table).");

  public static final ConfigProperty<String> MAX_COMMITS_TO_KEEP = ConfigProperty
      .key("hoodie.keep.max.commits")
      .defaultValue("30")
      .withDocumentation("Archiving service moves older entries from timeline into an archived log after each write, to "
          + " keep the metadata overhead constant, even as the table size grows."
          + "This config controls the maximum number of instants to retain in the active timeline. ");

  public static final ConfigProperty<Integer> DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.archive.delete.parallelism")
      .defaultValue(100)
      .withDocumentation("Parallelism for deleting archived hoodie commits.");

  public static final ConfigProperty<String> MIN_COMMITS_TO_KEEP = ConfigProperty
      .key("hoodie.keep.min.commits")
      .defaultValue("20")
      .withDocumentation("Similar to " + MAX_COMMITS_TO_KEEP.key() + ", but controls the minimum number of"
          + "instants to retain in the active timeline.");

  public static final ConfigProperty<String> COMMITS_ARCHIVAL_BATCH_SIZE = ConfigProperty
      .key("hoodie.commits.archival.batch")
      .defaultValue(String.valueOf(10))
      .withDocumentation("Archiving of instants is batched in best-effort manner, to pack more instants into a single"
          + " archive log. This config controls such archival batch size.");

  public static final ConfigProperty<String> CLEANER_BOOTSTRAP_BASE_FILE_ENABLE = ConfigProperty
      .key("hoodie.cleaner.delete.bootstrap.base.file")
      .defaultValue("false")
      .withDocumentation("When set to true, cleaner also deletes the bootstrap base file when it's skeleton base file is "
          + " cleaned. Turn this to true, if you want to ensure the bootstrap dataset storage is reclaimed over time, as the"
          + " table receives updates/deletes. Another reason to turn this on, would be to ensure data residing in bootstrap "
          + " base files are also physically deleted, to comply with data privacy enforcement processes.");

  public static final ConfigProperty<String> PARQUET_SMALL_FILE_LIMIT = ConfigProperty
      .key("hoodie.parquet.small.file.limit")
      .defaultValue(String.valueOf(104857600))
      .withDocumentation("During upsert operation, we opportunistically expand existing small files on storage, instead of writing"
          + " new files, to keep number of files to an optimum. This config sets the file size limit below which a file on storage "
          + " becomes a candidate to be selected as such a `small file`. By default, treat any file <= 100MB as a small file."
          + " Also note that if this set <= 0, will not try to get small files and directly write new files");

  public static final ConfigProperty<String> RECORD_SIZE_ESTIMATION_THRESHOLD = ConfigProperty
      .key("hoodie.record.size.estimation.threshold")
      .defaultValue("1.0")
      .withDocumentation("We use the previous commits' metadata to calculate the estimated record size and use it "
          + " to bin pack records into partitions. If the previous commit is too small to make an accurate estimation, "
          + " Hudi will search commits in the reverse order, until we find a commit that has totalBytesWritten "
          + " larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * this_threshold)");

  public static final ConfigProperty<String> CLEANER_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.cleaner.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism for the cleaning operation. Increase this if cleaning becomes slow.");

  // 500GB of target IO per compaction (both read and write
  public static final ConfigProperty<String> TARGET_IO_PER_COMPACTION_IN_MB = ConfigProperty
      .key("hoodie.compaction.target.io")
      .defaultValue(String.valueOf(500 * 1024))
      .withDocumentation("Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. "
          + "This value helps bound ingestion latency while compaction is run inline mode.");

  public static final ConfigProperty<Long> COMPACTION_LOG_FILE_SIZE_THRESHOLD = ConfigProperty
      .key("hoodie.compaction.logfile.size.threshold")
      .defaultValue(0L)
      .withDocumentation("Only if the log file size is greater than the threshold in bytes,"
          + " the file group will be compacted.");

  public static final ConfigProperty<String> COMPACTION_STRATEGY = ConfigProperty
      .key("hoodie.compaction.strategy")
      .defaultValue(LogFileSizeBasedCompactionStrategy.class.getName())
      .withDocumentation("Compaction strategy decides which file groups are picked up for "
          + "compaction during each compaction run. By default. Hudi picks the log file "
          + "with most accumulated unmerged data");

  public static final ConfigProperty<String> PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("This needs to be same as class used during insert/upserts. Just like writing, compaction also uses "
          + "the record payload class to merge records in the log against each other, merge again with the base file and "
          + "produce the final record to be written after compaction.");

  public static final ConfigProperty<String> MERGE_CLASS_NAME = ConfigProperty
      .key("hoodie.compaction.merge.class")
      .defaultValue(HoodieAvroRecordMerge.class.getName())
      .withDocumentation("Merge class provide stateless component interface for merging records, and support various HoodieRecord "
          + "types, such as Spark records or Flink records.");

  public static final ConfigProperty<String> COMPACTION_LAZY_BLOCK_READ_ENABLE = ConfigProperty
      .key("hoodie.compaction.lazy.block.read")
      .defaultValue("true")
      .withDocumentation("When merging the delta log files, this config helps to choose whether the log blocks "
          + "should be read lazily or not. Choose true to use lazy block reading (low memory usage, but incurs seeks to each block"
          + " header) or false for immediate block read (higher memory usage)");

  public static final ConfigProperty<String> COMPACTION_REVERSE_LOG_READ_ENABLE = ConfigProperty
      .key("hoodie.compaction.reverse.log.read")
      .defaultValue("false")
      .withDocumentation("HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. "
          + "If this config is set to true, the reader reads the logfile in reverse direction, from pos=file_length to pos=0");

  public static final ConfigProperty<String> FAILED_WRITES_CLEANER_POLICY = ConfigProperty
      .key("hoodie.cleaner.policy.failed.writes")
      .defaultValue(HoodieFailedWritesCleaningPolicy.EAGER.name())
      .withDocumentation("Cleaning policy for failed writes to be used. Hudi will delete any files written by "
          + "failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before "
          + "every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)");

  public static final ConfigProperty<String> TARGET_PARTITIONS_PER_DAYBASED_COMPACTION = ConfigProperty
      .key("hoodie.compaction.daybased.target.partitions")
      .defaultValue("10")
      .withDocumentation("Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of "
          + "latest partitions to compact during a compaction run.");

  public static final ConfigProperty<Boolean> PRESERVE_COMMIT_METADATA = ConfigProperty
      .key("hoodie.compaction.preserve.commit.metadata")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("When rewriting data, preserves existing hoodie_commit_time");

  /**
   * Configs related to specific table types.
   */
  public static final ConfigProperty<String> COPY_ON_WRITE_INSERT_SPLIT_SIZE = ConfigProperty
      .key("hoodie.copyonwrite.insert.split.size")
      .defaultValue(String.valueOf(500000))
      .withDocumentation("Number of inserts assigned for each partition/bucket for writing. "
          + "We based the default on writing out 100MB files, with at least 1kb records (100K records per file), and "
          + "  over provision to 500K. As long as auto-tuning of splits is turned on, this only affects the first "
          + "  write, where there is no history to learn record sizes from.");

  public static final ConfigProperty<String> COPY_ON_WRITE_AUTO_SPLIT_INSERTS = ConfigProperty
      .key("hoodie.copyonwrite.insert.auto.split")
      .defaultValue("true")
      .withDocumentation("Config to control whether we control insert split sizes automatically based on average"
          + " record sizes. It's recommended to keep this turned on, since hand tuning is otherwise extremely"
          + " cumbersome.");

  public static final ConfigProperty<String> COPY_ON_WRITE_RECORD_SIZE_ESTIMATE = ConfigProperty
      .key("hoodie.copyonwrite.record.size.estimate")
      .defaultValue(String.valueOf(1024))
      .withDocumentation("The average record size. If not explicitly specified, hudi will compute the "
          + "record size estimate compute dynamically based on commit metadata. "
          + " This is critical in computing the insert parallelism and bin-packing inserts into small files.");
  
  public static final ConfigProperty<Boolean> ALLOW_MULTIPLE_CLEANS = ConfigProperty
      .key("hoodie.clean.allow.multiple")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("Allows scheduling/executing multiple cleans by enabling this config. If users prefer to strictly ensure clean requests should be mutually exclusive, "
          + ".i.e. a 2nd clean will not be scheduled if another clean is not yet completed to avoid repeat cleaning of same files, they might want to disable this config.");

  public static final ConfigProperty<Integer> ARCHIVE_MERGE_FILES_BATCH_SIZE = ConfigProperty
      .key("hoodie.archive.merge.files.batch.size")
      .defaultValue(10)
      .withDocumentation("The number of small archive files to be merged at once.");

  public static final ConfigProperty<Long> ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES = ConfigProperty
      .key("hoodie.archive.merge.small.file.limit.bytes")
      .defaultValue(20L * 1024 * 1024)
      .withDocumentation("This config sets the archive file size limit below which an archive file becomes a candidate to be selected as such a small file.");

  public static final ConfigProperty<Boolean> ARCHIVE_MERGE_ENABLE = ConfigProperty
      .key("hoodie.archive.merge.enable")
      .defaultValue(false)
      .withDocumentation("When enable, hoodie will auto merge several small archive files into larger one. It's"
          + " useful when storage scheme doesn't support append operation.");

  /** @deprecated Use {@link #CLEANER_POLICY} and its methods instead */
  @Deprecated
  public static final String CLEANER_POLICY_PROP = CLEANER_POLICY.key();
  /** @deprecated Use {@link #AUTO_CLEAN} and its methods instead */
  @Deprecated
  public static final String AUTO_CLEAN_PROP = AUTO_CLEAN.key();
  /** @deprecated Use {@link #ASYNC_CLEAN} and its methods instead */
  @Deprecated
  public static final String ASYNC_CLEAN_PROP = ASYNC_CLEAN.key();
  /** @deprecated Use {@link #INLINE_COMPACT} and its methods instead */
  @Deprecated
  public static final String INLINE_COMPACT_PROP = INLINE_COMPACT.key();
  /** @deprecated Use {@link #INLINE_COMPACT_NUM_DELTA_COMMITS} and its methods instead */
  @Deprecated
  public static final String INLINE_COMPACT_NUM_DELTA_COMMITS_PROP = INLINE_COMPACT_NUM_DELTA_COMMITS.key();
  /** @deprecated Use {@link #INLINE_COMPACT_TIME_DELTA_SECONDS} and its methods instead */
  @Deprecated
  public static final String INLINE_COMPACT_TIME_DELTA_SECONDS_PROP = INLINE_COMPACT_TIME_DELTA_SECONDS.key();
  /** @deprecated Use {@link #INLINE_COMPACT_TRIGGER_STRATEGY} and its methods instead */
  @Deprecated
  public static final String INLINE_COMPACT_TRIGGER_STRATEGY_PROP = INLINE_COMPACT_TRIGGER_STRATEGY.key();
  /** @deprecated Use {@link #CLEANER_FILE_VERSIONS_RETAINED} and its methods instead */
  @Deprecated
  public static final String CLEANER_FILE_VERSIONS_RETAINED_PROP = CLEANER_FILE_VERSIONS_RETAINED.key();
  /**
   * @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_COMMITS_RETAINED_PROP = CLEANER_COMMITS_RETAINED.key();
  /**
   * @deprecated Use {@link #CLEANER_INCREMENTAL_MODE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_INCREMENTAL_MODE = CLEANER_INCREMENTAL_MODE_ENABLE.key();
  /**
   * @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  public static final String MAX_COMMITS_TO_KEEP_PROP = MAX_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  public static final String MIN_COMMITS_TO_KEEP_PROP = MIN_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #COMMITS_ARCHIVAL_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  public static final String COMMITS_ARCHIVAL_BATCH_SIZE_PROP = COMMITS_ARCHIVAL_BATCH_SIZE.key();
  /**
   * @deprecated Use {@link #CLEANER_BOOTSTRAP_BASE_FILE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = CLEANER_BOOTSTRAP_BASE_FILE_ENABLE.key();
  /**
   * @deprecated Use {@link #PARQUET_SMALL_FILE_LIMIT} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_SMALL_FILE_LIMIT_BYTES = PARQUET_SMALL_FILE_LIMIT.key();
  /**
   * @deprecated Use {@link #PARQUET_SMALL_FILE_LIMIT} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_SMALL_FILE_LIMIT_BYTES = PARQUET_SMALL_FILE_LIMIT.defaultValue();
  /**
   * @deprecated Use {@link #RECORD_SIZE_ESTIMATION_THRESHOLD} and its methods instead
   */
  @Deprecated
  public static final String RECORD_SIZE_ESTIMATION_THRESHOLD_PROP = RECORD_SIZE_ESTIMATION_THRESHOLD.key();
  /**
   * @deprecated Use {@link #RECORD_SIZE_ESTIMATION_THRESHOLD} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_RECORD_SIZE_ESTIMATION_THRESHOLD = RECORD_SIZE_ESTIMATION_THRESHOLD.defaultValue();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_INSERT_SPLIT_SIZE} and its methods instead
   */
  @Deprecated
  public static final String COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = COPY_ON_WRITE_INSERT_SPLIT_SIZE.key();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_INSERT_SPLIT_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = COPY_ON_WRITE_INSERT_SPLIT_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_AUTO_SPLIT_INSERTS} and its methods instead
   */
  @Deprecated
  public static final String COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = COPY_ON_WRITE_AUTO_SPLIT_INSERTS.key();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_AUTO_SPLIT_INSERTS} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = COPY_ON_WRITE_AUTO_SPLIT_INSERTS.defaultValue();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_RECORD_SIZE_ESTIMATE} and its methods instead
   */
  @Deprecated
  public static final String COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key();
  /**
   * @deprecated Use {@link #COPY_ON_WRITE_RECORD_SIZE_ESTIMATE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_PARALLELISM = CLEANER_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #CLEANER_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLEANER_PARALLELISM = CLEANER_PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #TARGET_IO_PER_COMPACTION_IN_MB} and its methods instead
   */
  @Deprecated
  public static final String TARGET_IO_PER_COMPACTION_IN_MB_PROP = TARGET_IO_PER_COMPACTION_IN_MB.key();
  /**
   * @deprecated Use {@link #TARGET_IO_PER_COMPACTION_IN_MB} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_TARGET_IO_PER_COMPACTION_IN_MB = TARGET_IO_PER_COMPACTION_IN_MB.defaultValue();
  /**
   * @deprecated Use {@link #COMPACTION_STRATEGY} and its methods instead
   */
  @Deprecated
  public static final String COMPACTION_STRATEGY_PROP = COMPACTION_STRATEGY.key();
  /** @deprecated Use {@link #COMPACTION_STRATEGY} and its methods instead */
  @Deprecated
  public static final String DEFAULT_COMPACTION_STRATEGY = COMPACTION_STRATEGY.defaultValue();
  /** @deprecated Use {@link #PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String DEFAULT_PAYLOAD_CLASS = PAYLOAD_CLASS_NAME.defaultValue();
  /** @deprecated Use {@link #PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String PAYLOAD_CLASS_PROP = PAYLOAD_CLASS_NAME.key();
  /** @deprecated Use {@link #COMPACTION_LAZY_BLOCK_READ_ENABLE} and its methods instead */
  @Deprecated
  public static final String COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = COMPACTION_LAZY_BLOCK_READ_ENABLE.key();
  /** @deprecated Use {@link #COMPACTION_LAZY_BLOCK_READ_ENABLE} and its methods instead */
  @Deprecated
  public static final String DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED = COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue();
  /** @deprecated Use {@link #COMPACTION_REVERSE_LOG_READ_ENABLE} and its methods instead */
  @Deprecated
  public static final String COMPACTION_REVERSE_LOG_READ_ENABLED_PROP = COMPACTION_REVERSE_LOG_READ_ENABLE.key();
  /** @deprecated Use {@link #COMPACTION_REVERSE_LOG_READ_ENABLE} and its methods instead */
  @Deprecated
  public static final String DEFAULT_COMPACTION_REVERSE_LOG_READ_ENABLED = COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue();
  /** @deprecated Use {@link #CLEANER_POLICY} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_POLICY = CLEANER_POLICY.defaultValue();
  /** @deprecated Use {@link #FAILED_WRITES_CLEANER_POLICY} and its methods instead */
  @Deprecated
  public static final String FAILED_WRITES_CLEANER_POLICY_PROP = FAILED_WRITES_CLEANER_POLICY.key();
  /** @deprecated Use {@link #FAILED_WRITES_CLEANER_POLICY} and its methods instead */
  @Deprecated
  private  static final String DEFAULT_FAILED_WRITES_CLEANER_POLICY = FAILED_WRITES_CLEANER_POLICY.defaultValue();
  /** @deprecated Use {@link #AUTO_CLEAN} and its methods instead */
  @Deprecated
  private static final String DEFAULT_AUTO_CLEAN = AUTO_CLEAN.defaultValue();
  /**
   * @deprecated Use {@link #ASYNC_CLEAN} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_ASYNC_CLEAN = ASYNC_CLEAN.defaultValue();
  /**
   * @deprecated Use {@link #INLINE_COMPACT} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_INLINE_COMPACT = INLINE_COMPACT.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_INCREMENTAL_MODE_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_INCREMENTAL_CLEANER = CLEANER_INCREMENTAL_MODE_ENABLE.defaultValue();
  /** @deprecated Use {@link #INLINE_COMPACT_NUM_DELTA_COMMITS} and its methods instead */
  @Deprecated
  private static final String DEFAULT_INLINE_COMPACT_NUM_DELTA_COMMITS = INLINE_COMPACT_NUM_DELTA_COMMITS.defaultValue();
  /** @deprecated Use {@link #INLINE_COMPACT_TIME_DELTA_SECONDS} and its methods instead */
  @Deprecated
  private static final String DEFAULT_INLINE_COMPACT_TIME_DELTA_SECONDS = INLINE_COMPACT_TIME_DELTA_SECONDS.defaultValue();
  /** @deprecated Use {@link #INLINE_COMPACT_TRIGGER_STRATEGY} and its methods instead */
  @Deprecated
  private static final String DEFAULT_INLINE_COMPACT_TRIGGER_STRATEGY = INLINE_COMPACT_TRIGGER_STRATEGY.defaultValue();
  /** @deprecated Use {@link #CLEANER_FILE_VERSIONS_RETAINED} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_FILE_VERSIONS_RETAINED = CLEANER_FILE_VERSIONS_RETAINED.defaultValue();
  /** @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_COMMITS_RETAINED = CLEANER_COMMITS_RETAINED.defaultValue();
  /** @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods instead */
  @Deprecated
  private static final String DEFAULT_MAX_COMMITS_TO_KEEP = MAX_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_MIN_COMMITS_TO_KEEP = MIN_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #COMMITS_ARCHIVAL_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_COMMITS_ARCHIVAL_BATCH_SIZE = COMMITS_ARCHIVAL_BATCH_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_BOOTSTRAP_BASE_FILE_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = CLEANER_BOOTSTRAP_BASE_FILE_ENABLE.defaultValue();
  /** @deprecated Use {@link #TARGET_PARTITIONS_PER_DAYBASED_COMPACTION} and its methods instead */
  @Deprecated
  public static final String TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP = TARGET_PARTITIONS_PER_DAYBASED_COMPACTION.key();
  /** @deprecated Use {@link #TARGET_PARTITIONS_PER_DAYBASED_COMPACTION} and its methods instead */
  @Deprecated
  public static final String DEFAULT_TARGET_PARTITIONS_PER_DAYBASED_COMPACTION = TARGET_PARTITIONS_PER_DAYBASED_COMPACTION.defaultValue();

  private HoodieCompactionConfig() {
    super();
  }

  public static HoodieCompactionConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieCompactionConfig compactionConfig = new HoodieCompactionConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.compactionConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.compactionConfig.getProps().putAll(props);
      return this;
    }

    public Builder withAutoArchive(Boolean autoArchive) {
      compactionConfig.setValue(AUTO_ARCHIVE, String.valueOf(autoArchive));
      return this;
    }

    public Builder withAsyncArchive(Boolean asyncArchive) {
      compactionConfig.setValue(ASYNC_ARCHIVE, String.valueOf(asyncArchive));
      return this;
    }

    public Builder withAutoClean(Boolean autoClean) {
      compactionConfig.setValue(AUTO_CLEAN, String.valueOf(autoClean));
      return this;
    }

    public Builder withAsyncClean(Boolean asyncClean) {
      compactionConfig.setValue(ASYNC_CLEAN, String.valueOf(asyncClean));
      return this;
    }

    public Builder withIncrementalCleaningMode(Boolean incrementalCleaningMode) {
      compactionConfig.setValue(CLEANER_INCREMENTAL_MODE_ENABLE, String.valueOf(incrementalCleaningMode));
      return this;
    }

    public Builder withInlineCompaction(Boolean inlineCompaction) {
      compactionConfig.setValue(INLINE_COMPACT, String.valueOf(inlineCompaction));
      return this;
    }

    public Builder withScheduleInlineCompaction(Boolean scheduleAsyncCompaction) {
      compactionConfig.setValue(SCHEDULE_INLINE_COMPACT, String.valueOf(scheduleAsyncCompaction));
      return this;
    }

    public Builder withInlineCompactionTriggerStrategy(CompactionTriggerStrategy compactionTriggerStrategy) {
      compactionConfig.setValue(INLINE_COMPACT_TRIGGER_STRATEGY, compactionTriggerStrategy.name());
      return this;
    }

    public Builder withCleaningTriggerStrategy(String cleaningTriggerStrategy) {
      compactionConfig.setValue(CLEAN_TRIGGER_STRATEGY, cleaningTriggerStrategy);
      return this;
    }

    public Builder withMaxCommitsBeforeCleaning(int maxCommitsBeforeCleaning) {
      compactionConfig.setValue(CLEAN_MAX_COMMITS, String.valueOf(maxCommitsBeforeCleaning));
      return this;
    }

    public Builder withCleanerPolicy(HoodieCleaningPolicy policy) {
      compactionConfig.setValue(CLEANER_POLICY, policy.name());
      return this;
    }

    public Builder retainFileVersions(int fileVersionsRetained) {
      compactionConfig.setValue(CLEANER_FILE_VERSIONS_RETAINED, String.valueOf(fileVersionsRetained));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      compactionConfig.setValue(CLEANER_COMMITS_RETAINED, String.valueOf(commitsRetained));
      return this;
    }

    public Builder cleanerNumHoursRetained(int cleanerHoursRetained) {
      compactionConfig.setValue(CLEANER_HOURS_RETAINED, String.valueOf(cleanerHoursRetained));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      compactionConfig.setValue(MIN_COMMITS_TO_KEEP, String.valueOf(minToKeep));
      compactionConfig.setValue(MAX_COMMITS_TO_KEEP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder withArchiveMergeFilesBatchSize(int number) {
      compactionConfig.setValue(ARCHIVE_MERGE_FILES_BATCH_SIZE, String.valueOf(number));
      return this;
    }

    public Builder withArchiveMergeSmallFileLimit(long size) {
      compactionConfig.setValue(ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES, String.valueOf(size));
      return this;
    }

    public Builder withArchiveMergeEnable(boolean enable) {
      compactionConfig.setValue(ARCHIVE_MERGE_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder compactionSmallFileSize(long smallFileLimitBytes) {
      compactionConfig.setValue(PARQUET_SMALL_FILE_LIMIT, String.valueOf(smallFileLimitBytes));
      return this;
    }

    public Builder compactionRecordSizeEstimateThreshold(double threshold) {
      compactionConfig.setValue(RECORD_SIZE_ESTIMATION_THRESHOLD, String.valueOf(threshold));
      return this;
    }

    public Builder insertSplitSize(int insertSplitSize) {
      compactionConfig.setValue(COPY_ON_WRITE_INSERT_SPLIT_SIZE, String.valueOf(insertSplitSize));
      return this;
    }

    public Builder autoTuneInsertSplits(boolean autoTuneInsertSplits) {
      compactionConfig.setValue(COPY_ON_WRITE_AUTO_SPLIT_INSERTS, String.valueOf(autoTuneInsertSplits));
      return this;
    }

    public Builder approxRecordSize(int recordSizeEstimate) {
      compactionConfig.setValue(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE, String.valueOf(recordSizeEstimate));
      return this;
    }

    public Builder allowMultipleCleans(boolean allowMultipleCleanSchedules) {
      compactionConfig.setValue(ALLOW_MULTIPLE_CLEANS, String.valueOf(allowMultipleCleanSchedules));
      return this;
    }

    public Builder withCleanerParallelism(int cleanerParallelism) {
      compactionConfig.setValue(CLEANER_PARALLELISM_VALUE, String.valueOf(cleanerParallelism));
      return this;
    }

    public Builder withCompactionStrategy(CompactionStrategy compactionStrategy) {
      compactionConfig.setValue(COMPACTION_STRATEGY, compactionStrategy.getClass().getName());
      return this;
    }

    public Builder withPayloadClass(String payloadClassName) {
      compactionConfig.setValue(PAYLOAD_CLASS_NAME, payloadClassName);
      return this;
    }

    public Builder withMergeClass(String mergeClass) {
      compactionConfig.setValue(MERGE_CLASS_NAME, mergeClass);
      return this;
    }

    public Builder withTargetIOPerCompactionInMB(long targetIOPerCompactionInMB) {
      compactionConfig.setValue(TARGET_IO_PER_COMPACTION_IN_MB, String.valueOf(targetIOPerCompactionInMB));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      compactionConfig.setValue(INLINE_COMPACT_NUM_DELTA_COMMITS, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withArchiveDeleteParallelism(int archiveDeleteParallelism) {
      compactionConfig.setValue(DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE, String.valueOf(archiveDeleteParallelism));
      return this;
    }

    public Builder withMaxDeltaSecondsBeforeCompaction(int maxDeltaSecondsBeforeCompaction) {
      compactionConfig.setValue(INLINE_COMPACT_TIME_DELTA_SECONDS, String.valueOf(maxDeltaSecondsBeforeCompaction));
      return this;
    }

    public Builder withCompactionLazyBlockReadEnabled(Boolean compactionLazyBlockReadEnabled) {
      compactionConfig.setValue(COMPACTION_LAZY_BLOCK_READ_ENABLE, String.valueOf(compactionLazyBlockReadEnabled));
      return this;
    }

    public Builder withCompactionReverseLogReadEnabled(Boolean compactionReverseLogReadEnabled) {
      compactionConfig.setValue(COMPACTION_REVERSE_LOG_READ_ENABLE, String.valueOf(compactionReverseLogReadEnabled));
      return this;
    }

    public Builder withTargetPartitionsPerDayBasedCompaction(int targetPartitionsPerCompaction) {
      compactionConfig.setValue(TARGET_PARTITIONS_PER_DAYBASED_COMPACTION, String.valueOf(targetPartitionsPerCompaction));
      return this;
    }

    public Builder withLogFileSizeThresholdBasedCompaction(long logFileSizeThreshold) {
      compactionConfig.setValue(COMPACTION_LOG_FILE_SIZE_THRESHOLD, String.valueOf(logFileSizeThreshold));
      return this;
    }

    public Builder withPreserveCommitMetadata(boolean preserveCommitMetadata) {
      compactionConfig.setValue(PRESERVE_COMMIT_METADATA, String.valueOf(preserveCommitMetadata));
      return this;
    }

    public Builder withCommitsArchivalBatchSize(int batchSize) {
      compactionConfig.setValue(COMMITS_ARCHIVAL_BATCH_SIZE, String.valueOf(batchSize));
      return this;
    }

    public Builder withCleanBootstrapBaseFileEnabled(Boolean cleanBootstrapSourceFileEnabled) {
      compactionConfig.setValue(CLEANER_BOOTSTRAP_BASE_FILE_ENABLE, String.valueOf(cleanBootstrapSourceFileEnabled));
      return this;
    }

    public Builder withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
      compactionConfig.setValue(FAILED_WRITES_CLEANER_POLICY, failedWritesPolicy.name());
      return this;
    }

    public HoodieCompactionConfig build() {
      compactionConfig.setDefaults(HoodieCompactionConfig.class.getName());
      // validation
      HoodieCleaningPolicy.valueOf(compactionConfig.getString(CLEANER_POLICY));

      // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
      // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
      int minInstantsToKeep = Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP));
      int maxInstantsToKeep = Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP));
      int cleanerCommitsRetained =
          Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED));
      ValidationUtils.checkArgument(maxInstantsToKeep > minInstantsToKeep,
          String.format(
              "Increase %s=%d to be greater than %s=%d.",
              HoodieCompactionConfig.MAX_COMMITS_TO_KEEP.key(), maxInstantsToKeep,
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep));
      ValidationUtils.checkArgument(minInstantsToKeep > cleanerCommitsRetained,
          String.format(
              "Increase %s=%d to be greater than %s=%d. Otherwise, there is risk of incremental pull "
                  + "missing data from few instants.",
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep,
              HoodieCompactionConfig.CLEANER_COMMITS_RETAINED.key(), cleanerCommitsRetained));

      boolean inlineCompact = compactionConfig.getBoolean(HoodieCompactionConfig.INLINE_COMPACT);
      boolean inlineCompactSchedule = compactionConfig.getBoolean(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT);
      ValidationUtils.checkArgument(!(inlineCompact && inlineCompactSchedule), String.format("Either of inline compaction (%s) or "
              + "schedule inline compaction (%s) can be enabled. Both can't be set to true at the same time. %s, %s", HoodieCompactionConfig.INLINE_COMPACT.key(),
          HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), inlineCompact, inlineCompactSchedule));
      return compactionConfig;
    }
  }
}
