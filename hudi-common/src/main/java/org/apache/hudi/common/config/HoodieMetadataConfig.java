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

package org.apache.hudi.common.config;

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieNotSupportedException;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Configurations used by the HUDI Metadata Table.
 */
@Immutable
@ConfigClassProperty(name = "Metadata Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations used by the Hudi Metadata Table. "
        + "This table maintains the metadata about a given Hudi table (e.g file listings) "
        + " to avoid overhead of accessing cloud storage, during queries.")
public final class HoodieMetadataConfig extends HoodieConfig {

  public static final String METADATA_PREFIX = "hoodie.metadata";

  // Enable the internal Metadata Table which saves file listings
  public static final ConfigProperty<Boolean> ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".enable")
      .defaultValue(true)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable the internal metadata table which serves table metadata like level file listings");

  public static final boolean DEFAULT_METADATA_ENABLE_FOR_READERS = false;

  // Enable metrics for internal Metadata Table
  public static final ConfigProperty<Boolean> METRICS_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".metrics.enable")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable publishing of metrics around metadata table.");

  // Parallelism for inserts
  public static final ConfigProperty<Integer> INSERT_PARALLELISM_VALUE = ConfigProperty
      .key(METADATA_PREFIX + ".insert.parallelism")
      .defaultValue(1)
      .sinceVersion("0.7.0")
      .withDocumentation("Parallelism to use when inserting to the metadata table");

  // Async clean
  public static final ConfigProperty<Boolean> ASYNC_CLEAN_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".clean.async")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable asynchronous cleaning for metadata table");

  // Async index
  public static final ConfigProperty<Boolean> ASYNC_INDEX_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".index.async")
      .defaultValue(false)
      .sinceVersion("0.11.0")
      .withDocumentation("Enable asynchronous indexing of metadata table.");

  // Maximum delta commits before compaction occurs
  public static final ConfigProperty<Integer> COMPACT_NUM_DELTA_COMMITS = ConfigProperty
      .key(METADATA_PREFIX + ".compact.max.delta.commits")
      .defaultValue(10)
      .sinceVersion("0.7.0")
      .withDocumentation("Controls how often the metadata table is compacted.");

  // Archival settings
  public static final ConfigProperty<Integer> MIN_COMMITS_TO_KEEP = ConfigProperty
      .key(METADATA_PREFIX + ".keep.min.commits")
      .defaultValue(20)
      .sinceVersion("0.7.0")
      .withDocumentation("Archiving service moves older entries from metadata table’s timeline "
          + "into an archived log after each write, to keep the overhead constant, even as the "
          + "metadata table size grows.  This config controls the minimum number of instants "
          + "to retain in the active timeline.");

  public static final ConfigProperty<Integer> MAX_COMMITS_TO_KEEP = ConfigProperty
      .key(METADATA_PREFIX + ".keep.max.commits")
      .defaultValue(30)
      .sinceVersion("0.7.0")
      .withDocumentation("Similar to " + MIN_COMMITS_TO_KEEP.key() + ", this config controls "
          + "the maximum number of instants to retain in the active timeline.");

  // Cleaner commits retained
  public static final ConfigProperty<Integer> CLEANER_COMMITS_RETAINED = ConfigProperty
      .key(METADATA_PREFIX + ".cleaner.commits.retained")
      .defaultValue(3)
      .sinceVersion("0.7.0")
      .withDocumentation("Number of commits to retain, without cleaning, on metadata table.");

  // Regex to filter out matching directories during bootstrap
  public static final ConfigProperty<String> DIR_FILTER_REGEX = ConfigProperty
      .key(METADATA_PREFIX + ".dir.filter.regex")
      .defaultValue("")
      .sinceVersion("0.7.0")
      .withDocumentation("Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.");

  public static final ConfigProperty<String> ASSUME_DATE_PARTITIONING = ConfigProperty
      .key("hoodie.assume.date.partitioning")
      .defaultValue("false")
      .sinceVersion("0.3.0")
      .withDocumentation("Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. "
          + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually");

  public static final ConfigProperty<Integer> FILE_LISTING_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.file.listing.parallelism")
      .defaultValue(200)
      .sinceVersion("0.7.0")
      .withDocumentation("Parallelism to use, when listing the table on lake storage.");

  public static final ConfigProperty<Boolean> ENABLE_FULL_SCAN_LOG_FILES = ConfigProperty
      .key(METADATA_PREFIX + ".enable.full.scan.log.files")
      .defaultValue(true)
      .sinceVersion("0.10.0")
      .withDocumentation("Enable full scanning of log files while reading log records. If disabled, Hudi does look up of only interested entries.");

  public static final ConfigProperty<Boolean> ENABLE_METADATA_INDEX_BLOOM_FILTER = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.enable")
      .defaultValue(false)
      .sinceVersion("0.11.0")
      .withDocumentation("Enable indexing bloom filters of user data files under metadata table. When enabled, "
          + "metadata table will have a partition to store the bloom filter index and will be "
          + "used during the index lookups.");

  public static final ConfigProperty<Integer> METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.file.group.count")
      .defaultValue(4)
      .sinceVersion("0.11.0")
      .withDocumentation("Metadata bloom filter index partition file group count. This controls the size of the base and "
          + "log files and read parallelism in the bloom filter index partition. The recommendation is to size the "
          + "file group count such that the base files are under 1GB.");

  public static final ConfigProperty<Integer> BLOOM_FILTER_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.parallelism")
      .defaultValue(200)
      .sinceVersion("0.11.0")
      .withDocumentation("Parallelism to use for generating bloom filter index in metadata table.");

  public static final ConfigProperty<Boolean> ENABLE_METADATA_INDEX_COLUMN_STATS = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.enable")
      .defaultValue(false)
      .sinceVersion("0.11.0")
      .withDocumentation("Enable indexing column ranges of user data files under metadata table key lookups. When "
          + "enabled, metadata table will have a partition to store the column ranges and will be "
          + "used for pruning files during the index lookups.");

  public static final ConfigProperty<Integer> METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.file.group.count")
      .defaultValue(2)
      .sinceVersion("0.11.0")
      .withDocumentation("Metadata column stats partition file group count. This controls the size of the base and "
          + "log files and read parallelism in the column stats index partition. The recommendation is to size the "
          + "file group count such that the base files are under 1GB.");

  public static final ConfigProperty<Integer> COLUMN_STATS_INDEX_PARALLELISM = ConfigProperty
          .key(METADATA_PREFIX + ".index.column.stats.parallelism")
          .defaultValue(10)
          .sinceVersion("0.11.0")
          .withDocumentation("Parallelism to use, when generating column stats index.");

  public static final ConfigProperty<String> COLUMN_STATS_INDEX_FOR_COLUMNS = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.column.list")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of columns for which column stats index will be built. If not set, all columns will be indexed");

  public static final String COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY = "in-memory";
  public static final String COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE = "engine";

  public static final ConfigProperty<String> COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.processing.mode.override")
      .noDefaultValue()
      .withValidValues(COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY, COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE)
      .sinceVersion("0.12.0")
      .withDocumentation("By default Column Stats Index is automatically determining whether it should be read and processed either"
          + "'in-memory' (w/in executing process) or using Spark (on a cluster), based on some factors like the size of the Index "
          + "and how many columns are read. This config allows to override this behavior.");

  public static final ConfigProperty<String> BLOOM_FILTER_INDEX_FOR_COLUMNS = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.column.list")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of columns for which bloom filter index will be built. If not set, only record key will be indexed.");

  public static final ConfigProperty<Integer> METADATA_INDEX_CHECK_TIMEOUT_SECONDS = ConfigProperty
      .key(METADATA_PREFIX + ".index.check.timeout.seconds")
      .defaultValue(900)
      .sinceVersion("0.11.0")
      .withDocumentation("After the async indexer has finished indexing upto the base instant, it will ensure that all inflight writers "
          + "reliably write index updates as well. If this timeout expires, then the indexer will abort itself safely.");

  public static final ConfigProperty<Boolean> POPULATE_META_FIELDS = ConfigProperty
      .key(METADATA_PREFIX + ".populate.meta.fields")
      .defaultValue(false)
      .sinceVersion("0.10.0")
      .withDocumentation("When enabled, populates all meta fields. When disabled, no meta fields are populated.");

  public static final ConfigProperty<Boolean> IGNORE_SPURIOUS_DELETES = ConfigProperty
      .key("_" + METADATA_PREFIX + ".ignore.spurious.deletes")
      .defaultValue(true)
      .sinceVersion("0.10.0")
      .withDocumentation("There are cases when extra files are requested to be deleted from "
          + "metadata table which are never added before. This config determines how to handle "
          + "such spurious deletes");

  private HoodieMetadataConfig() {
    super();
  }

  public static HoodieMetadataConfig.Builder newBuilder() {
    return new Builder();
  }

  public int getFileListingParallelism() {
    return Math.max(getInt(HoodieMetadataConfig.FILE_LISTING_PARALLELISM_VALUE), 1);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return getBoolean(HoodieMetadataConfig.ASSUME_DATE_PARTITIONING);
  }

  public boolean enabled() {
    return getBoolean(ENABLE);
  }

  public boolean isBloomFilterIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_BLOOM_FILTER);
  }

  public boolean isColumnStatsIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_COLUMN_STATS);
  }

  public List<String> getColumnsEnabledForColumnStatsIndex() {
    return StringUtils.split(getString(COLUMN_STATS_INDEX_FOR_COLUMNS), CONFIG_VALUES_DELIMITER);
  }

  public String getColumnStatsIndexProcessingModeOverride() {
    return getString(COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE);
  }

  public List<String> getColumnsEnabledForBloomFilterIndex() {
    return StringUtils.split(getString(BLOOM_FILTER_INDEX_FOR_COLUMNS), CONFIG_VALUES_DELIMITER);
  }

  public int getBloomFilterIndexFileGroupCount() {
    return getIntOrDefault(METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT);
  }

  public int getColumnStatsIndexFileGroupCount() {
    return getIntOrDefault(METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT);
  }

  public int getBloomFilterIndexParallelism() {
    return getIntOrDefault(BLOOM_FILTER_INDEX_PARALLELISM);
  }

  public int getColumnStatsIndexParallelism() {
    return getIntOrDefault(COLUMN_STATS_INDEX_PARALLELISM);
  }

  public int getIndexingCheckTimeoutSeconds() {
    return getIntOrDefault(METADATA_INDEX_CHECK_TIMEOUT_SECONDS);
  }

  public boolean enableMetrics() {
    return getBoolean(METRICS_ENABLE);
  }

  public String getDirectoryFilterRegex() {
    return getString(DIR_FILTER_REGEX);
  }

  public boolean allowFullScan() {
    return getBooleanOrDefault(ENABLE_FULL_SCAN_LOG_FILES);
  }

  public boolean populateMetaFields() {
    return getBooleanOrDefault(HoodieMetadataConfig.POPULATE_META_FIELDS);
  }

  public boolean ignoreSpuriousDeletes() {
    return getBoolean(IGNORE_SPURIOUS_DELETES);
  }

  public static class Builder {

    private EngineType engineType = EngineType.SPARK;
    private final HoodieMetadataConfig metadataConfig = new HoodieMetadataConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.metadataConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.metadataConfig.getProps().putAll(props);
      return this;
    }

    public Builder enable(boolean enable) {
      metadataConfig.setValue(ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withMetadataIndexBloomFilter(boolean enable) {
      metadataConfig.setValue(ENABLE_METADATA_INDEX_BLOOM_FILTER, String.valueOf(enable));
      return this;
    }

    public Builder withMetadataIndexBloomFilterFileGroups(int fileGroupCount) {
      metadataConfig.setValue(METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT, String.valueOf(fileGroupCount));
      return this;
    }

    public Builder withBloomFilterIndexParallelism(int parallelism) {
      metadataConfig.setValue(BLOOM_FILTER_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withMetadataIndexColumnStats(boolean enable) {
      metadataConfig.setValue(ENABLE_METADATA_INDEX_COLUMN_STATS, String.valueOf(enable));
      return this;
    }

    public Builder withMetadataIndexColumnStatsFileGroupCount(int fileGroupCount) {
      metadataConfig.setValue(METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT, String.valueOf(fileGroupCount));
      return this;
    }

    public Builder withColumnStatsIndexParallelism(int parallelism) {
      metadataConfig.setValue(COLUMN_STATS_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withColumnStatsIndexForColumns(String columns) {
      metadataConfig.setValue(COLUMN_STATS_INDEX_FOR_COLUMNS, columns);
      return this;
    }

    public Builder withBloomFilterIndexForColumns(String columns) {
      metadataConfig.setValue(BLOOM_FILTER_INDEX_FOR_COLUMNS, columns);
      return this;
    }

    public Builder withIndexingCheckTimeout(int timeoutInSeconds) {
      metadataConfig.setValue(METADATA_INDEX_CHECK_TIMEOUT_SECONDS, String.valueOf(timeoutInSeconds));
      return this;
    }

    public Builder enableMetrics(boolean enableMetrics) {
      metadataConfig.setValue(METRICS_ENABLE, String.valueOf(enableMetrics));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      metadataConfig.setValue(INSERT_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withAsyncClean(boolean asyncClean) {
      metadataConfig.setValue(ASYNC_CLEAN_ENABLE, String.valueOf(asyncClean));
      return this;
    }

    public Builder withAsyncIndex(boolean asyncIndex) {
      metadataConfig.setValue(ASYNC_INDEX_ENABLE, String.valueOf(asyncIndex));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      metadataConfig.setValue(COMPACT_NUM_DELTA_COMMITS, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withPopulateMetaFields(boolean populateMetaFields) {
      metadataConfig.setValue(POPULATE_META_FIELDS, Boolean.toString(populateMetaFields));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      metadataConfig.setValue(MIN_COMMITS_TO_KEEP, String.valueOf(minToKeep));
      metadataConfig.setValue(MAX_COMMITS_TO_KEEP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      metadataConfig.setValue(CLEANER_COMMITS_RETAINED, String.valueOf(commitsRetained));
      return this;
    }

    public Builder withFileListingParallelism(int parallelism) {
      metadataConfig.setValue(FILE_LISTING_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
      metadataConfig.setValue(ASSUME_DATE_PARTITIONING, String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withDirectoryFilterRegex(String regex) {
      metadataConfig.setValue(DIR_FILTER_REGEX, regex);
      return this;
    }

    public Builder enableFullScan(boolean enableFullScan) {
      metadataConfig.setValue(ENABLE_FULL_SCAN_LOG_FILES, String.valueOf(enableFullScan));
      return this;
    }

    public Builder ignoreSpuriousDeletes(boolean validateMetadataPayloadConsistency) {
      metadataConfig.setValue(IGNORE_SPURIOUS_DELETES, String.valueOf(validateMetadataPayloadConsistency));
      return this;
    }

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.metadataConfig.getProps().putAll(properties);
      return this;
    }

    public HoodieMetadataConfig build() {
      metadataConfig.setDefaultValue(ENABLE, getDefaultMetadataEnable(engineType));
      metadataConfig.setDefaults(HoodieMetadataConfig.class.getName());
      return metadataConfig;
    }

    private boolean getDefaultMetadataEnable(EngineType engineType) {
      switch (engineType) {
        case FLINK:
        case SPARK:
          return ENABLE.defaultValue();
        case JAVA:
          return false;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }

  /**
   * @deprecated Use {@link #ENABLE} and its methods.
   */
  @Deprecated
  public static final String METADATA_ENABLE_PROP = ENABLE.key();
  /**
   * @deprecated Use {@link #ENABLE} and its methods.
   */
  @Deprecated
  public static final boolean DEFAULT_METADATA_ENABLE = ENABLE.defaultValue();

  /**
   * @deprecated Use {@link #METRICS_ENABLE} and its methods.
   */
  @Deprecated
  public static final String METADATA_METRICS_ENABLE_PROP = METRICS_ENABLE.key();
  /**
   * @deprecated Use {@link #METRICS_ENABLE} and its methods.
   */
  @Deprecated
  public static final boolean DEFAULT_METADATA_METRICS_ENABLE = METRICS_ENABLE.defaultValue();

  /**
   * @deprecated Use {@link #INSERT_PARALLELISM_VALUE} and its methods.
   */
  @Deprecated
  public static final String METADATA_INSERT_PARALLELISM_PROP = INSERT_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #INSERT_PARALLELISM_VALUE} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_METADATA_INSERT_PARALLELISM = INSERT_PARALLELISM_VALUE.defaultValue();

  /**
   * @deprecated Use {@link #ASYNC_CLEAN_ENABLE} and its methods.
   */
  @Deprecated
  public static final String METADATA_ASYNC_CLEAN_PROP = ASYNC_CLEAN_ENABLE.key();
  /**
   * @deprecated Use {@link #ASYNC_CLEAN_ENABLE} and its methods.
   */
  @Deprecated
  public static final boolean DEFAULT_METADATA_ASYNC_CLEAN = ASYNC_CLEAN_ENABLE.defaultValue();

  /**
   * @deprecated Use {@link #COMPACT_NUM_DELTA_COMMITS} and its methods.
   */
  @Deprecated
  public static final String METADATA_COMPACT_NUM_DELTA_COMMITS_PROP = COMPACT_NUM_DELTA_COMMITS.key();
  /**
   * @deprecated Use {@link #COMPACT_NUM_DELTA_COMMITS} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_METADATA_COMPACT_NUM_DELTA_COMMITS = COMPACT_NUM_DELTA_COMMITS.defaultValue();

  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods.
   */
  @Deprecated
  public static final String MIN_COMMITS_TO_KEEP_PROP = MIN_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_MIN_COMMITS_TO_KEEP = MIN_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods.
   */
  @Deprecated
  public static final String MAX_COMMITS_TO_KEEP_PROP = MAX_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_MAX_COMMITS_TO_KEEP = MAX_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods.
   */
  @Deprecated
  public static final String CLEANER_COMMITS_RETAINED_PROP = CLEANER_COMMITS_RETAINED.key();
  /**
   * @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_CLEANER_COMMITS_RETAINED = CLEANER_COMMITS_RETAINED.defaultValue();
  /**
   * @deprecated No longer takes any effect.
   */
  @Deprecated
  public static final String ENABLE_FALLBACK_PROP = METADATA_PREFIX + ".fallback.enable";
  /**
   * @deprecated No longer takes any effect.
   */
  @Deprecated
  public static final String DEFAULT_ENABLE_FALLBACK = "true";
  /**
   * @deprecated Use {@link #DIR_FILTER_REGEX} and its methods.
   */
  @Deprecated
  public static final String DIRECTORY_FILTER_REGEX = DIR_FILTER_REGEX.key();
  /**
   * @deprecated Use {@link #DIR_FILTER_REGEX} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_DIRECTORY_FILTER_REGEX = DIR_FILTER_REGEX.defaultValue();
  /**
   * @deprecated Use {@link #ASSUME_DATE_PARTITIONING} and its methods.
   */
  @Deprecated
  public static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP = ASSUME_DATE_PARTITIONING.key();
  /**
   * @deprecated Use {@link #ASSUME_DATE_PARTITIONING} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_ASSUME_DATE_PARTITIONING = ASSUME_DATE_PARTITIONING.defaultValue();
  /**
   * @deprecated Use {@link #FILE_LISTING_PARALLELISM_VALUE} and its methods.
   */
  @Deprecated
  public static final String FILE_LISTING_PARALLELISM_PROP = FILE_LISTING_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #FILE_LISTING_PARALLELISM_VALUE} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_FILE_LISTING_PARALLELISM = FILE_LISTING_PARALLELISM_VALUE.defaultValue();
}
