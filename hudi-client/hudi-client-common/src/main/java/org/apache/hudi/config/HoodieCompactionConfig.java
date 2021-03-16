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
public class HoodieCompactionConfig extends DefaultHoodieConfig {

  public static final String CLEANER_POLICY_PROP = "hoodie.cleaner.policy";
  public static final String AUTO_CLEAN_PROP = "hoodie.clean.automatic";
  public static final String ASYNC_CLEAN_PROP = "hoodie.clean.async";
  // Turn on inline compaction - after fw delta commits a inline compaction will be run
  public static final String INLINE_COMPACT_PROP = "hoodie.compact.inline";
  // Run a compaction every N delta commits
  public static final String INLINE_COMPACT_NUM_DELTA_COMMITS_PROP = "hoodie.compact.inline.max.delta.commits";
  // Run a compaction when time elapsed > N seconds since last compaction
  public static final String INLINE_COMPACT_TIME_DELTA_SECONDS_PROP = "hoodie.compact.inline.max.delta.seconds";
  public static final String INLINE_COMPACT_TRIGGER_STRATEGY_PROP = "hoodie.compact.inline.trigger.strategy";
  public static final String CLEANER_FILE_VERSIONS_RETAINED_PROP = "hoodie.cleaner.fileversions.retained";
  public static final String CLEANER_COMMITS_RETAINED_PROP = "hoodie.cleaner.commits.retained";
  public static final String CLEANER_INCREMENTAL_MODE = "hoodie.cleaner.incremental.mode";
  public static final String MAX_COMMITS_TO_KEEP_PROP = "hoodie.keep.max.commits";
  public static final String MIN_COMMITS_TO_KEEP_PROP = "hoodie.keep.min.commits";
  public static final String COMMITS_ARCHIVAL_BATCH_SIZE_PROP = "hoodie.commits.archival.batch";
  // Set true to clean bootstrap source files when necessary
  public static final String CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = "hoodie.cleaner.delete.bootstrap.base.file";
  // Upsert uses this file size to compact new data onto existing files..
  public static final String PARQUET_SMALL_FILE_LIMIT_BYTES = "hoodie.parquet.small.file.limit";
  // By default, treat any file <= 100MB as a small file.
  public static final String DEFAULT_PARQUET_SMALL_FILE_LIMIT_BYTES = String.valueOf(104857600);
  // Hudi will use the previous commit to calculate the estimated record size by totalBytesWritten/totalRecordsWritten.
  // If the previous commit is too small to make an accurate estimation, Hudi will search commits in the reverse order,
  // until find a commit has totalBytesWritten larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * RECORD_SIZE_ESTIMATION_THRESHOLD)
  public static final String RECORD_SIZE_ESTIMATION_THRESHOLD_PROP = "hoodie.record.size.estimation.threshold";
  public static final String DEFAULT_RECORD_SIZE_ESTIMATION_THRESHOLD = "1.0";

  /**
   * Configs related to specific table types.
   */
  // Number of inserts, that will be put each partition/bucket for writing
  public static final String COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = "hoodie.copyonwrite.insert.split.size";
  // The rationale to pick the insert parallelism is the following. Writing out 100MB files,
  // with atleast 1kb records, means 100K records per file. we just overprovision to 500K
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = String.valueOf(500000);
  // Config to control whether we control insert split sizes automatically based on average
  // record sizes
  public static final String COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = "hoodie.copyonwrite.insert.auto.split";
  // its off by default
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = String.valueOf(true);
  // This value is used as a guesstimate for the record size, if we can't determine this from
  // previous commits
  public static final String COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = "hoodie.copyonwrite.record.size.estimate";
  // Used to determine how much more can be packed into a small file, before it exceeds the size
  // limit.
  public static final String DEFAULT_COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = String.valueOf(1024);
  public static final String CLEANER_PARALLELISM = "hoodie.cleaner.parallelism";
  public static final String DEFAULT_CLEANER_PARALLELISM = String.valueOf(200);
  public static final String TARGET_IO_PER_COMPACTION_IN_MB_PROP = "hoodie.compaction.target.io";
  // 500GB of target IO per compaction (both read and write)
  public static final String DEFAULT_TARGET_IO_PER_COMPACTION_IN_MB = String.valueOf(500 * 1024);
  public static final String COMPACTION_STRATEGY_PROP = "hoodie.compaction.strategy";
  // 200GB of target IO per compaction
  public static final String DEFAULT_COMPACTION_STRATEGY = LogFileSizeBasedCompactionStrategy.class.getName();
  // used to merge records written to log file
  public static final String DEFAULT_PAYLOAD_CLASS = OverwriteWithLatestAvroPayload.class.getName();
  public static final String PAYLOAD_CLASS_PROP = "hoodie.compaction.payload.class";

  // used to choose a trade off between IO vs Memory when performing compaction process
  // Depending on outputfile_size and memory provided, choose true to avoid OOM for large file
  // size + small memory
  public static final String COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = "hoodie.compaction.lazy.block.read";
  public static final String DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED = "false";
  // used to choose whether to enable reverse log reading (reverse log traversal)
  public static final String COMPACTION_REVERSE_LOG_READ_ENABLED_PROP = "hoodie.compaction.reverse.log.read";
  public static final String DEFAULT_COMPACTION_REVERSE_LOG_READ_ENABLED = "false";
  private static final String DEFAULT_CLEANER_POLICY = HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name();
  public static final String FAILED_WRITES_CLEANER_POLICY_PROP = "hoodie.failed.writes.cleaner.policy";
  private  static final String DEFAULT_FAILED_WRITES_CLEANER_POLICY =
      HoodieFailedWritesCleaningPolicy.EAGER.name();
  private static final String DEFAULT_AUTO_CLEAN = "true";
  private static final String DEFAULT_ASYNC_CLEAN = "false";
  private static final String DEFAULT_INLINE_COMPACT = "false";
  private static final String DEFAULT_INCREMENTAL_CLEANER = "true";
  private static final String DEFAULT_INLINE_COMPACT_NUM_DELTA_COMMITS = "5";
  private static final String DEFAULT_INLINE_COMPACT_TIME_DELTA_SECONDS = String.valueOf(60 * 60);
  private static final String DEFAULT_INLINE_COMPACT_TRIGGER_STRATEGY = CompactionTriggerStrategy.NUM_COMMITS.name();
  private static final String DEFAULT_CLEANER_FILE_VERSIONS_RETAINED = "3";
  private static final String DEFAULT_CLEANER_COMMITS_RETAINED = "10";
  private static final String DEFAULT_MAX_COMMITS_TO_KEEP = "30";
  private static final String DEFAULT_MIN_COMMITS_TO_KEEP = "20";
  private static final String DEFAULT_COMMITS_ARCHIVAL_BATCH_SIZE = String.valueOf(10);
  private static final String DEFAULT_CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = "false";
  public static final String TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP =
      "hoodie.compaction.daybased.target.partitions";
  // 500GB of target IO per compaction (both read and write)
  public static final String DEFAULT_TARGET_PARTITIONS_PER_DAYBASED_COMPACTION = String.valueOf(10);

  private HoodieCompactionConfig(Properties props) {
    super(props);
  }

  public static HoodieCompactionConfig.Builder newBuilder() {
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

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withAutoClean(Boolean autoClean) {
      props.setProperty(AUTO_CLEAN_PROP, String.valueOf(autoClean));
      return this;
    }

    public Builder withAsyncClean(Boolean asyncClean) {
      props.setProperty(ASYNC_CLEAN_PROP, String.valueOf(asyncClean));
      return this;
    }

    public Builder withIncrementalCleaningMode(Boolean incrementalCleaningMode) {
      props.setProperty(CLEANER_INCREMENTAL_MODE, String.valueOf(incrementalCleaningMode));
      return this;
    }

    public Builder withInlineCompaction(Boolean inlineCompaction) {
      props.setProperty(INLINE_COMPACT_PROP, String.valueOf(inlineCompaction));
      return this;
    }

    public Builder withInlineCompactionTriggerStrategy(CompactionTriggerStrategy compactionTriggerStrategy) {
      props.setProperty(INLINE_COMPACT_TRIGGER_STRATEGY_PROP, compactionTriggerStrategy.name());
      return this;
    }

    public Builder withCleanerPolicy(HoodieCleaningPolicy policy) {
      props.setProperty(CLEANER_POLICY_PROP, policy.name());
      return this;
    }

    public Builder retainFileVersions(int fileVersionsRetained) {
      props.setProperty(CLEANER_FILE_VERSIONS_RETAINED_PROP, String.valueOf(fileVersionsRetained));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      props.setProperty(CLEANER_COMMITS_RETAINED_PROP, String.valueOf(commitsRetained));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      props.setProperty(MIN_COMMITS_TO_KEEP_PROP, String.valueOf(minToKeep));
      props.setProperty(MAX_COMMITS_TO_KEEP_PROP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder compactionSmallFileSize(long smallFileLimitBytes) {
      props.setProperty(PARQUET_SMALL_FILE_LIMIT_BYTES, String.valueOf(smallFileLimitBytes));
      return this;
    }

    public Builder compactionRecordSizeEstimateThreshold(double threshold) {
      props.setProperty(RECORD_SIZE_ESTIMATION_THRESHOLD_PROP, String.valueOf(threshold));
      return this;
    }

    public Builder insertSplitSize(int insertSplitSize) {
      props.setProperty(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE, String.valueOf(insertSplitSize));
      return this;
    }

    public Builder autoTuneInsertSplits(boolean autoTuneInsertSplits) {
      props.setProperty(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS, String.valueOf(autoTuneInsertSplits));
      return this;
    }

    public Builder approxRecordSize(int recordSizeEstimate) {
      props.setProperty(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE, String.valueOf(recordSizeEstimate));
      return this;
    }

    public Builder withCleanerParallelism(int cleanerParallelism) {
      props.setProperty(CLEANER_PARALLELISM, String.valueOf(cleanerParallelism));
      return this;
    }

    public Builder withCompactionStrategy(CompactionStrategy compactionStrategy) {
      props.setProperty(COMPACTION_STRATEGY_PROP, compactionStrategy.getClass().getName());
      return this;
    }

    public Builder withPayloadClass(String payloadClassName) {
      props.setProperty(PAYLOAD_CLASS_PROP, payloadClassName);
      return this;
    }

    public Builder withTargetIOPerCompactionInMB(long targetIOPerCompactionInMB) {
      props.setProperty(TARGET_IO_PER_COMPACTION_IN_MB_PROP, String.valueOf(targetIOPerCompactionInMB));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      props.setProperty(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withMaxDeltaSecondsBeforeCompaction(int maxDeltaSecondsBeforeCompaction) {
      props.setProperty(INLINE_COMPACT_TIME_DELTA_SECONDS_PROP, String.valueOf(maxDeltaSecondsBeforeCompaction));
      return this;
    }

    public Builder withCompactionLazyBlockReadEnabled(Boolean compactionLazyBlockReadEnabled) {
      props.setProperty(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, String.valueOf(compactionLazyBlockReadEnabled));
      return this;
    }

    public Builder withCompactionReverseLogReadEnabled(Boolean compactionReverseLogReadEnabled) {
      props.setProperty(COMPACTION_REVERSE_LOG_READ_ENABLED_PROP, String.valueOf(compactionReverseLogReadEnabled));
      return this;
    }

    public Builder withTargetPartitionsPerDayBasedCompaction(int targetPartitionsPerCompaction) {
      props.setProperty(TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP, String.valueOf(targetPartitionsPerCompaction));
      return this;
    }

    public Builder withCommitsArchivalBatchSize(int batchSize) {
      props.setProperty(COMMITS_ARCHIVAL_BATCH_SIZE_PROP, String.valueOf(batchSize));
      return this;
    }

    public Builder withCleanBootstrapBaseFileEnabled(Boolean cleanBootstrapSourceFileEnabled) {
      props.setProperty(CLEANER_BOOTSTRAP_BASE_FILE_ENABLED, String.valueOf(cleanBootstrapSourceFileEnabled));
      return this;
    }

    public Builder withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
      props.setProperty(FAILED_WRITES_CLEANER_POLICY_PROP, failedWritesPolicy.name());
      return this;
    }

    public HoodieCompactionConfig build() {
      HoodieCompactionConfig config = new HoodieCompactionConfig(props);
      setDefaultOnCondition(props, !props.containsKey(AUTO_CLEAN_PROP), AUTO_CLEAN_PROP, DEFAULT_AUTO_CLEAN);
      setDefaultOnCondition(props, !props.containsKey(ASYNC_CLEAN_PROP), ASYNC_CLEAN_PROP,
              DEFAULT_ASYNC_CLEAN);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_INCREMENTAL_MODE), CLEANER_INCREMENTAL_MODE,
          DEFAULT_INCREMENTAL_CLEANER);
      setDefaultOnCondition(props, !props.containsKey(INLINE_COMPACT_PROP), INLINE_COMPACT_PROP,
          DEFAULT_INLINE_COMPACT);
      setDefaultOnCondition(props, !props.containsKey(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP),
          INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, DEFAULT_INLINE_COMPACT_NUM_DELTA_COMMITS);
      setDefaultOnCondition(props, !props.containsKey(INLINE_COMPACT_TIME_DELTA_SECONDS_PROP),
          INLINE_COMPACT_TIME_DELTA_SECONDS_PROP, DEFAULT_INLINE_COMPACT_TIME_DELTA_SECONDS);
      setDefaultOnCondition(props, !props.containsKey(INLINE_COMPACT_TRIGGER_STRATEGY_PROP),
          INLINE_COMPACT_TRIGGER_STRATEGY_PROP, DEFAULT_INLINE_COMPACT_TRIGGER_STRATEGY);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_POLICY_PROP), CLEANER_POLICY_PROP,
          DEFAULT_CLEANER_POLICY);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_FILE_VERSIONS_RETAINED_PROP),
          CLEANER_FILE_VERSIONS_RETAINED_PROP, DEFAULT_CLEANER_FILE_VERSIONS_RETAINED);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_COMMITS_RETAINED_PROP), CLEANER_COMMITS_RETAINED_PROP,
          DEFAULT_CLEANER_COMMITS_RETAINED);
      setDefaultOnCondition(props, !props.containsKey(MAX_COMMITS_TO_KEEP_PROP), MAX_COMMITS_TO_KEEP_PROP,
          DEFAULT_MAX_COMMITS_TO_KEEP);
      setDefaultOnCondition(props, !props.containsKey(MIN_COMMITS_TO_KEEP_PROP), MIN_COMMITS_TO_KEEP_PROP,
          DEFAULT_MIN_COMMITS_TO_KEEP);
      setDefaultOnCondition(props, !props.containsKey(PARQUET_SMALL_FILE_LIMIT_BYTES), PARQUET_SMALL_FILE_LIMIT_BYTES,
          DEFAULT_PARQUET_SMALL_FILE_LIMIT_BYTES);
      setDefaultOnCondition(props, !props.containsKey(RECORD_SIZE_ESTIMATION_THRESHOLD_PROP), RECORD_SIZE_ESTIMATION_THRESHOLD_PROP,
          DEFAULT_RECORD_SIZE_ESTIMATION_THRESHOLD);
      setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE),
          COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE, DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE);
      setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS),
          COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS, DEFAULT_COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS);
      setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE),
          COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE, DEFAULT_COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_PARALLELISM), CLEANER_PARALLELISM,
          DEFAULT_CLEANER_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(COMPACTION_STRATEGY_PROP), COMPACTION_STRATEGY_PROP,
          DEFAULT_COMPACTION_STRATEGY);
      setDefaultOnCondition(props, !props.containsKey(PAYLOAD_CLASS_PROP), PAYLOAD_CLASS_PROP, DEFAULT_PAYLOAD_CLASS);
      setDefaultOnCondition(props, !props.containsKey(TARGET_IO_PER_COMPACTION_IN_MB_PROP),
          TARGET_IO_PER_COMPACTION_IN_MB_PROP, DEFAULT_TARGET_IO_PER_COMPACTION_IN_MB);
      setDefaultOnCondition(props, !props.containsKey(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP),
          COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(COMPACTION_REVERSE_LOG_READ_ENABLED_PROP),
          COMPACTION_REVERSE_LOG_READ_ENABLED_PROP, DEFAULT_COMPACTION_REVERSE_LOG_READ_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP),
          TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP, DEFAULT_TARGET_PARTITIONS_PER_DAYBASED_COMPACTION);
      setDefaultOnCondition(props, !props.containsKey(COMMITS_ARCHIVAL_BATCH_SIZE_PROP),
          COMMITS_ARCHIVAL_BATCH_SIZE_PROP, DEFAULT_COMMITS_ARCHIVAL_BATCH_SIZE);
      setDefaultOnCondition(props, !props.containsKey(CLEANER_BOOTSTRAP_BASE_FILE_ENABLED),
          CLEANER_BOOTSTRAP_BASE_FILE_ENABLED, DEFAULT_CLEANER_BOOTSTRAP_BASE_FILE_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(FAILED_WRITES_CLEANER_POLICY_PROP),
          FAILED_WRITES_CLEANER_POLICY_PROP, DEFAULT_FAILED_WRITES_CLEANER_POLICY);

      HoodieCleaningPolicy.valueOf(props.getProperty(CLEANER_POLICY_PROP));

      // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
      // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
      int minInstantsToKeep = Integer.parseInt(props.getProperty(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP));
      int maxInstantsToKeep = Integer.parseInt(props.getProperty(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP));
      int cleanerCommitsRetained =
          Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP));
      ValidationUtils.checkArgument(maxInstantsToKeep > minInstantsToKeep,
          String.format(
              "Increase %s=%d to be greater than %s=%d.",
              HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP, maxInstantsToKeep,
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP, minInstantsToKeep));
      ValidationUtils.checkArgument(minInstantsToKeep > cleanerCommitsRetained,
          String.format(
              "Increase %s=%d to be greater than %s=%d. Otherwise, there is risk of incremental pull "
                  + "missing data from few instants.",
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP, minInstantsToKeep,
              HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP, cleanerCommitsRetained));
      return config;
    }
  }
}
