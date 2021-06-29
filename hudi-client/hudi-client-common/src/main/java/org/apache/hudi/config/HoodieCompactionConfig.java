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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Compaction related config.
 */
@Immutable
public class HoodieCompactionConfig extends HoodieConfig {

  public static final ConfigProperty<String> CLEANER_POLICY_PROP = ConfigProperty
      .key("hoodie.cleaner.policy")
      .defaultValue(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
      .withDocumentation("Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space."
          + " Any Query/Computation referring to this version of the file will fail. "
          + "It is good to make sure that the data is retained for more than the maximum query execution time.");

  public static final ConfigProperty<String> AUTO_CLEAN_PROP = ConfigProperty
      .key("hoodie.clean.automatic")
      .defaultValue("true")
      .withDocumentation("Should cleanup if there is anything to cleanup immediately after the commit");

  public static final ConfigProperty<String> ASYNC_CLEAN_PROP = ConfigProperty
      .key("hoodie.clean.async")
      .defaultValue("false")
      .withDocumentation("Only applies when #withAutoClean is turned on. When turned on runs cleaner async with writing.");

  public static final ConfigProperty<String> INLINE_COMPACT_PROP = ConfigProperty
      .key("hoodie.compact.inline")
      .defaultValue("false")
      .withDocumentation("When set to true, compaction is triggered by the ingestion itself, "
          + "right after a commit/deltacommit action as part of insert/upsert/bulk_insert");

  public static final ConfigProperty<String> INLINE_COMPACT_NUM_DELTA_COMMITS_PROP = ConfigProperty
      .key("hoodie.compact.inline.max.delta.commits")
      .defaultValue("5")
      .withDocumentation("Number of max delta commits to keep before triggering an inline compaction");

  public static final ConfigProperty<String> INLINE_COMPACT_TIME_DELTA_SECONDS_PROP = ConfigProperty
      .key("hoodie.compact.inline.max.delta.seconds")
      .defaultValue(String.valueOf(60 * 60))
      .withDocumentation("Run a compaction when time elapsed > N seconds since last compaction");

  public static final ConfigProperty<String> INLINE_COMPACT_TRIGGER_STRATEGY_PROP = ConfigProperty
      .key("hoodie.compact.inline.trigger.strategy")
      .defaultValue(CompactionTriggerStrategy.NUM_COMMITS.name())
      .withDocumentation("");

  public static final ConfigProperty<String> CLEANER_FILE_VERSIONS_RETAINED_PROP = ConfigProperty
      .key("hoodie.cleaner.fileversions.retained")
      .defaultValue("3")
      .withDocumentation("");

  public static final ConfigProperty<String> CLEANER_COMMITS_RETAINED_PROP = ConfigProperty
      .key("hoodie.cleaner.commits.retained")
      .defaultValue("10")
      .withDocumentation("Number of commits to retain. So data will be retained for num_of_commits * time_between_commits "
          + "(scheduled). This also directly translates into how much you can incrementally pull on this table");

  public static final ConfigProperty<String> CLEANER_INCREMENTAL_MODE = ConfigProperty
      .key("hoodie.cleaner.incremental.mode")
      .defaultValue("true")
      .withDocumentation("");

  public static final ConfigProperty<String> MAX_COMMITS_TO_KEEP_PROP = ConfigProperty
      .key("hoodie.keep.max.commits")
      .defaultValue("30")
      .withDocumentation("Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of "
          + "small files, Hudi archives older commits into a sequential log. A commit is published atomically "
          + "by a rename of the commit file.");

  public static final ConfigProperty<String> MIN_COMMITS_TO_KEEP_PROP = ConfigProperty
      .key("hoodie.keep.min.commits")
      .defaultValue("20")
      .withDocumentation("Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of "
          + "small files, Hudi archives older commits into a sequential log. A commit is published atomically "
          + "by a rename of the commit file.");

  public static final ConfigProperty<String> COMMITS_ARCHIVAL_BATCH_SIZE_PROP = ConfigProperty
      .key("hoodie.commits.archival.batch")
      .defaultValue(String.valueOf(10))
      .withDocumentation("This controls the number of commit instants read in memory as a batch and archived together.");

  public static final ConfigProperty<String> CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = ConfigProperty
      .key("hoodie.cleaner.delete.bootstrap.base.file")
      .defaultValue("false")
      .withDocumentation("Set true to clean bootstrap source files when necessary");

  public static final ConfigProperty<String> PARQUET_SMALL_FILE_LIMIT_BYTES = ConfigProperty
      .key("hoodie.parquet.small.file.limit")
      .defaultValue(String.valueOf(104857600))
      .withDocumentation("Upsert uses this file size to compact new data onto existing files. "
          + "By default, treat any file <= 100MB as a small file.");

  public static final ConfigProperty<String> RECORD_SIZE_ESTIMATION_THRESHOLD_PROP = ConfigProperty
      .key("hoodie.record.size.estimation.threshold")
      .defaultValue("1.0")
      .withDocumentation("Hudi will use the previous commit to calculate the estimated record size by totalBytesWritten/totalRecordsWritten. "
          + "If the previous commit is too small to make an accurate estimation, Hudi will search commits in the reverse order, "
          + "until find a commit has totalBytesWritten larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * RECORD_SIZE_ESTIMATION_THRESHOLD)");

  public static final ConfigProperty<String> CLEANER_PARALLELISM = ConfigProperty
      .key("hoodie.cleaner.parallelism")
      .defaultValue("200")
      .withDocumentation("Increase this if cleaning becomes slow.");

  // 500GB of target IO per compaction (both read and write
  public static final ConfigProperty<String> TARGET_IO_PER_COMPACTION_IN_MB_PROP = ConfigProperty
      .key("hoodie.compaction.target.io")
      .defaultValue(String.valueOf(500 * 1024))
      .withDocumentation("Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. "
          + "This value helps bound ingestion latency while compaction is run inline mode.");

  public static final ConfigProperty<String> COMPACTION_STRATEGY_PROP = ConfigProperty
      .key("hoodie.compaction.strategy")
      .defaultValue(LogFileSizeBasedCompactionStrategy.class.getName())
      .withDocumentation("Compaction strategy decides which file groups are picked up for "
          + "compaction during each compaction run. By default. Hudi picks the log file "
          + "with most accumulated unmerged data");

  public static final ConfigProperty<String> PAYLOAD_CLASS_PROP = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("This needs to be same as class used during insert/upserts. Just like writing, compaction also uses "
          + "the record payload class to merge records in the log against each other, merge again with the base file and "
          + "produce the final record to be written after compaction.");

  public static final ConfigProperty<String> COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = ConfigProperty
      .key("hoodie.compaction.lazy.block.read")
      .defaultValue("false")
      .withDocumentation("When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks "
          + "should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false "
          + "for Memory intensive immediate block read (high memory usage)");

  public static final ConfigProperty<String> COMPACTION_REVERSE_LOG_READ_ENABLED_PROP = ConfigProperty
      .key("hoodie.compaction.reverse.log.read")
      .defaultValue("false")
      .withDocumentation("HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. "
          + "If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0");

  public static final ConfigProperty<String> FAILED_WRITES_CLEANER_POLICY_PROP = ConfigProperty
      .key("hoodie.cleaner.policy.failed.writes")
      .defaultValue(HoodieFailedWritesCleaningPolicy.EAGER.name())
      .withDocumentation("Cleaning policy for failed writes to be used. Hudi will delete any files written by "
          + "failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before "
          + "every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)");

  public static final ConfigProperty<String> TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP = ConfigProperty
      .key("hoodie.compaction.daybased.target.partitions")
      .defaultValue("10")
      .withDocumentation("Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of "
          + "latest partitions to compact during a compaction run.");

  /**
   * Configs related to specific table types.
   */
  public static final ConfigProperty<String> COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = ConfigProperty
      .key("hoodie.copyonwrite.insert.split.size")
      .defaultValue(String.valueOf(500000))
      .withDocumentation("Number of inserts, that will be put each partition/bucket for writing. "
          + "The rationale to pick the insert parallelism is the following. Writing out 100MB files, "
          + "with at least 1kb records, means 100K records per file. we just over provision to 500K.");

  public static final ConfigProperty<String> COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = ConfigProperty
      .key("hoodie.copyonwrite.insert.auto.split")
      .defaultValue("true")
      .withDocumentation("Config to control whether we control insert split sizes automatically based on average"
          + " record sizes.");

  public static final ConfigProperty<String> COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = ConfigProperty
      .key("hoodie.copyonwrite.record.size.estimate")
      .defaultValue(String.valueOf(1024))
      .withDocumentation("The average record size. If specified, hudi will use this and not compute dynamically "
          + "based on the last 24 commit’s metadata. No value set as default. This is critical in computing "
          + "the insert parallelism and bin-packing inserts into small files. See above.");

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

    public Builder withAutoClean(Boolean autoClean) {
      compactionConfig.setValue(AUTO_CLEAN_PROP, String.valueOf(autoClean));
      return this;
    }

    public Builder withAsyncClean(Boolean asyncClean) {
      compactionConfig.setValue(ASYNC_CLEAN_PROP, String.valueOf(asyncClean));
      return this;
    }

    public Builder withIncrementalCleaningMode(Boolean incrementalCleaningMode) {
      compactionConfig.setValue(CLEANER_INCREMENTAL_MODE, String.valueOf(incrementalCleaningMode));
      return this;
    }

    public Builder withInlineCompaction(Boolean inlineCompaction) {
      compactionConfig.setValue(INLINE_COMPACT_PROP, String.valueOf(inlineCompaction));
      return this;
    }

    public Builder withInlineCompactionTriggerStrategy(CompactionTriggerStrategy compactionTriggerStrategy) {
      compactionConfig.setValue(INLINE_COMPACT_TRIGGER_STRATEGY_PROP, compactionTriggerStrategy.name());
      return this;
    }

    public Builder withCleanerPolicy(HoodieCleaningPolicy policy) {
      compactionConfig.setValue(CLEANER_POLICY_PROP, policy.name());
      return this;
    }

    public Builder retainFileVersions(int fileVersionsRetained) {
      compactionConfig.setValue(CLEANER_FILE_VERSIONS_RETAINED_PROP, String.valueOf(fileVersionsRetained));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      compactionConfig.setValue(CLEANER_COMMITS_RETAINED_PROP, String.valueOf(commitsRetained));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      compactionConfig.setValue(MIN_COMMITS_TO_KEEP_PROP, String.valueOf(minToKeep));
      compactionConfig.setValue(MAX_COMMITS_TO_KEEP_PROP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder compactionSmallFileSize(long smallFileLimitBytes) {
      compactionConfig.setValue(PARQUET_SMALL_FILE_LIMIT_BYTES, String.valueOf(smallFileLimitBytes));
      return this;
    }

    public Builder compactionRecordSizeEstimateThreshold(double threshold) {
      compactionConfig.setValue(RECORD_SIZE_ESTIMATION_THRESHOLD_PROP, String.valueOf(threshold));
      return this;
    }

    public Builder insertSplitSize(int insertSplitSize) {
      compactionConfig.setValue(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE, String.valueOf(insertSplitSize));
      return this;
    }

    public Builder autoTuneInsertSplits(boolean autoTuneInsertSplits) {
      compactionConfig.setValue(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS, String.valueOf(autoTuneInsertSplits));
      return this;
    }

    public Builder approxRecordSize(int recordSizeEstimate) {
      compactionConfig.setValue(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE, String.valueOf(recordSizeEstimate));
      return this;
    }

    public Builder withCleanerParallelism(int cleanerParallelism) {
      compactionConfig.setValue(CLEANER_PARALLELISM, String.valueOf(cleanerParallelism));
      return this;
    }

    public Builder withCompactionStrategy(CompactionStrategy compactionStrategy) {
      compactionConfig.setValue(COMPACTION_STRATEGY_PROP, compactionStrategy.getClass().getName());
      return this;
    }

    public Builder withPayloadClass(String payloadClassName) {
      compactionConfig.setValue(PAYLOAD_CLASS_PROP, payloadClassName);
      return this;
    }

    public Builder withTargetIOPerCompactionInMB(long targetIOPerCompactionInMB) {
      compactionConfig.setValue(TARGET_IO_PER_COMPACTION_IN_MB_PROP, String.valueOf(targetIOPerCompactionInMB));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      compactionConfig.setValue(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withMaxDeltaSecondsBeforeCompaction(int maxDeltaSecondsBeforeCompaction) {
      compactionConfig.setValue(INLINE_COMPACT_TIME_DELTA_SECONDS_PROP, String.valueOf(maxDeltaSecondsBeforeCompaction));
      return this;
    }

    public Builder withCompactionLazyBlockReadEnabled(Boolean compactionLazyBlockReadEnabled) {
      compactionConfig.setValue(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, String.valueOf(compactionLazyBlockReadEnabled));
      return this;
    }

    public Builder withCompactionReverseLogReadEnabled(Boolean compactionReverseLogReadEnabled) {
      compactionConfig.setValue(COMPACTION_REVERSE_LOG_READ_ENABLED_PROP, String.valueOf(compactionReverseLogReadEnabled));
      return this;
    }

    public Builder withTargetPartitionsPerDayBasedCompaction(int targetPartitionsPerCompaction) {
      compactionConfig.setValue(TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP, String.valueOf(targetPartitionsPerCompaction));
      return this;
    }

    public Builder withCommitsArchivalBatchSize(int batchSize) {
      compactionConfig.setValue(COMMITS_ARCHIVAL_BATCH_SIZE_PROP, String.valueOf(batchSize));
      return this;
    }

    public Builder withCleanBootstrapBaseFileEnabled(Boolean cleanBootstrapSourceFileEnabled) {
      compactionConfig.setValue(CLEANER_BOOTSTRAP_BASE_FILE_ENABLED, String.valueOf(cleanBootstrapSourceFileEnabled));
      return this;
    }

    public Builder withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
      compactionConfig.setValue(FAILED_WRITES_CLEANER_POLICY_PROP, failedWritesPolicy.name());
      return this;
    }

    public HoodieCompactionConfig build() {
      compactionConfig.setDefaults(HoodieCompactionConfig.class.getName());
      // validation
      HoodieCleaningPolicy.valueOf(compactionConfig.getString(CLEANER_POLICY_PROP));

      // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
      // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
      int minInstantsToKeep = Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP));
      int maxInstantsToKeep = Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP));
      int cleanerCommitsRetained =
          Integer.parseInt(compactionConfig.getStringOrDefault(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP));
      ValidationUtils.checkArgument(maxInstantsToKeep > minInstantsToKeep,
          String.format(
              "Increase %s=%d to be greater than %s=%d.",
              HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP.key(), maxInstantsToKeep,
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP.key(), minInstantsToKeep));
      ValidationUtils.checkArgument(minInstantsToKeep > cleanerCommitsRetained,
          String.format(
              "Increase %s=%d to be greater than %s=%d. Otherwise, there is risk of incremental pull "
                  + "missing data from few instants.",
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP.key(), minInstantsToKeep,
              HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP.key(), cleanerCommitsRetained));
      return compactionConfig;
    }
  }
}
