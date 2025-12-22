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
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
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

  // Asynchronous cleaning for metadata table is disabled by default
  public static final boolean DEFAULT_METADATA_ASYNC_CLEAN = false;
  // Full scanning of log files while reading log records is enabled by default for metadata table
  public static final boolean DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES = true;
  // Meta fields are not populated by default for metadata table
  public static final boolean DEFAULT_METADATA_POPULATE_META_FIELDS = false;
  // Default number of commits to retain, without cleaning, on metadata table
  public static final int DEFAULT_METADATA_CLEANER_COMMITS_RETAINED = 20;

  public static final String METADATA_PREFIX = "hoodie.metadata";
  public static final String OPTIMIZED_LOG_BLOCKS_SCAN = ".optimized.log.blocks.scan.enable";

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
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Enable publishing of metrics around metadata table.");

  // Async index
  public static final ConfigProperty<Boolean> ASYNC_INDEX_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".index.async")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Enable asynchronous indexing of metadata table.");

  // Maximum delta commits before compaction occurs
  public static final ConfigProperty<Integer> COMPACT_NUM_DELTA_COMMITS = ConfigProperty
      .key(METADATA_PREFIX + ".compact.max.delta.commits")
      .defaultValue(10)
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Controls how often the metadata table is compacted.");

  public static final ConfigProperty<String> ENABLE_LOG_COMPACTION_ON_METADATA_TABLE = ConfigProperty
      .key(METADATA_PREFIX + ".log.compaction.enable")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("This configs enables logcompaction for the metadata table.");

  // Log blocks threshold, after a file slice crosses this threshold log compact operation is scheduled.
  public static final ConfigProperty<Integer> LOG_COMPACT_BLOCKS_THRESHOLD = ConfigProperty
      .key(METADATA_PREFIX + ".log.compaction.blocks.threshold")
      .defaultValue(5)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Controls the criteria to log compacted files groups in metadata table.");

  // Regex to filter out matching directories during bootstrap
  public static final ConfigProperty<String> DIR_FILTER_REGEX = ConfigProperty
      .key(METADATA_PREFIX + ".dir.filter.regex")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.");

  public static final ConfigProperty<String> ASSUME_DATE_PARTITIONING = ConfigProperty
      .key("hoodie.assume.date.partitioning")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.3.0")
      .withDocumentation("Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. "
          + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually");

  public static final ConfigProperty<Integer> FILE_LISTING_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.file.listing.parallelism")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Parallelism to use, when listing the table on lake storage.");

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
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Metadata bloom filter index partition file group count. This controls the size of the base and "
          + "log files and read parallelism in the bloom filter index partition. The recommendation is to size the "
          + "file group count such that the base files are under 1GB.");

  public static final ConfigProperty<Integer> BLOOM_FILTER_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.parallelism")
      .defaultValue(200)
      .markAdvanced()
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
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Metadata column stats partition file group count. This controls the size of the base and "
          + "log files and read parallelism in the column stats index partition. The recommendation is to size the "
          + "file group count such that the base files are under 1GB.");

  public static final ConfigProperty<Integer> COLUMN_STATS_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.parallelism")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Parallelism to use, when generating column stats index.");

  public static final ConfigProperty<String> COLUMN_STATS_INDEX_FOR_COLUMNS = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.column.list")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of columns for which column stats index will be built. If not set, all columns will be indexed");

  public static final String COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY = "in-memory";
  public static final String COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE = "engine";

  public static final ConfigProperty<String> COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.processing.mode.override")
      .noDefaultValue()
      .withValidValues(COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY, COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE)
      .markAdvanced()
      .sinceVersion("0.12.0")
      .withDocumentation("By default Column Stats Index is automatically determining whether it should be read and processed either"
          + "'in-memory' (w/in executing process) or using Spark (on a cluster), based on some factors like the size of the Index "
          + "and how many columns are read. This config allows to override this behavior.");

  public static final ConfigProperty<Integer> COLUMN_STATS_INDEX_IN_MEMORY_PROJECTION_THRESHOLD = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.inMemory.projection.threshold")
      .defaultValue(100000)
      .markAdvanced()
      .sinceVersion("0.12.0")
      .withDocumentation("When reading Column Stats Index, if the size of the expected resulting projection is below the in-memory"
          + " threshold (counted by the # of rows), it will be attempted to be loaded \"in-memory\" (ie not using the execution engine"
          + " like Spark, Flink, etc). If the value is above the threshold execution engine will be used to compose the projection.");

  public static final ConfigProperty<String> BLOOM_FILTER_INDEX_FOR_COLUMNS = ConfigProperty
      .key(METADATA_PREFIX + ".index.bloom.filter.column.list")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of columns for which bloom filter index will be built. If not set, only record key will be indexed.");

  public static final ConfigProperty<Integer> METADATA_INDEX_CHECK_TIMEOUT_SECONDS = ConfigProperty
      .key(METADATA_PREFIX + ".index.check.timeout.seconds")
      .defaultValue(900)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("After the async indexer has finished indexing upto the base instant, it will ensure that all inflight writers "
          + "reliably write index updates as well. If this timeout expires, then the indexer will abort itself safely.");

  public static final ConfigProperty<Boolean> IGNORE_SPURIOUS_DELETES = ConfigProperty
      .key("_" + METADATA_PREFIX + ".ignore.spurious.deletes")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("There are cases when extra files are requested to be deleted from "
          + "metadata table which are never added before. This config determines how to handle "
          + "such spurious deletes");

  public static final ConfigProperty<Boolean> ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN = ConfigProperty
      .key(METADATA_PREFIX + OPTIMIZED_LOG_BLOCKS_SCAN)
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Optimized log blocks scanner that addresses all the multi-writer use-cases while appending to log files. "
          + "It also differentiates original blocks written by ingestion writers and compacted blocks written by log compaction.");

  public static final ConfigProperty<Integer> METADATA_MAX_NUM_DELTACOMMITS_WHEN_PENDING = ConfigProperty
      .key(METADATA_PREFIX + ".max.deltacommits.when_pending")
      .defaultValue(1000)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("When there is a pending instant in data table, this config limits the allowed number of deltacommits in metadata table to "
          + "prevent the metadata table's timeline from growing unboundedly as compaction won't be triggered due to the pending data table instant.");

  public static final ConfigProperty<Boolean> RECORD_INDEX_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Create the HUDI Record Index within the Metadata Table");

  public static final ConfigProperty<Integer> RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.min.filegroup.count")
      .defaultValue(10)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Minimum number of file groups to use for Record Index.");

  public static final ConfigProperty<Integer> RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.max.filegroup.count")
      .defaultValue(10000)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum number of file groups to use for Record Index.");

  public static final ConfigProperty<Integer> RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.max.filegroup.size")
      .defaultValue(1024 * 1024 * 1024)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum size in bytes of a single file group. Large file group takes longer to compact.");

  public static final ConfigProperty<Float> RECORD_INDEX_GROWTH_FACTOR_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.growth.factor")
      .defaultValue(2.0f)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("The current number of records are multiplied by this number when estimating the number of "
          + "file groups to create automatically. This helps account for growth in the number of records in the dataset.");

  public static final ConfigProperty<Integer> RECORD_INDEX_MAX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".max.init.parallelism")
      .defaultValue(100000)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum parallelism to use when initializing Record Index.");

  public static final ConfigProperty<Long> MAX_READER_MEMORY_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".max.reader.memory")
      .defaultValue(1024 * 1024 * 1024L)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Max memory to use for the reader to read from metadata");

  public static final ConfigProperty<Integer> MAX_READER_BUFFER_SIZE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".max.reader.buffer.size")
      .defaultValue(10 * 1024 * 1024)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Max memory to use for the reader buffer while merging log blocks");

  public static final ConfigProperty<String> SPILLABLE_MAP_DIR_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".spillable.map.path")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.of(cfg.getStringOrDefault(FileSystemViewStorageConfig.SPILLABLE_DIR)))
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Path on local storage to use, when keys read from metadata are held in a spillable map.");

  public static final ConfigProperty<Long> MAX_LOG_FILE_SIZE_BYTES_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".max.logfile.size")
      .defaultValue(2 * 1024 * 1024 * 1024L)  // 2GB
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum size in bytes of a single log file. Larger log files can contain larger log blocks "
          + "thereby reducing the number of blocks to search for keys");

  public static final ConfigProperty<Boolean> AUTO_INITIALIZE = ConfigProperty
      .key(METADATA_PREFIX + ".auto.initialize")
      .defaultValue(true)
      .sinceVersion("0.14.0")
      .markAdvanced()
      .withDocumentation("Initializes the metadata table by reading from the file system when the table is first created. Enabled by default. "
          + "Warning: This should only be disabled when manually constructing the metadata table outside of typical Hudi writer flows.");

  public long getMaxLogFileSize() {
    return getLong(MAX_LOG_FILE_SIZE_BYTES_PROP);
  }

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

  public boolean isEnabled() {
    return getBoolean(ENABLE);
  }

  public boolean isBloomFilterIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_BLOOM_FILTER);
  }

  public boolean isColumnStatsIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_COLUMN_STATS);
  }

  public boolean isRecordIndexEnabled() {
    return isEnabled() && getBooleanOrDefault(RECORD_INDEX_ENABLE_PROP);
  }

  public List<String> getColumnsEnabledForColumnStatsIndex() {
    return StringUtils.split(getString(COLUMN_STATS_INDEX_FOR_COLUMNS), CONFIG_VALUES_DELIMITER);
  }

  public String getColumnStatsIndexProcessingModeOverride() {
    return getString(COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE);
  }

  public Integer getColumnStatsIndexInMemoryProjectionThreshold() {
    return getIntOrDefault(COLUMN_STATS_INDEX_IN_MEMORY_PROJECTION_THRESHOLD);
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

  public boolean isMetricsEnabled() {
    return getBoolean(METRICS_ENABLE);
  }

  public String getDirectoryFilterRegex() {
    return getString(DIR_FILTER_REGEX);
  }

  public boolean shouldIgnoreSpuriousDeletes() {
    return getBoolean(IGNORE_SPURIOUS_DELETES);
  }

  public boolean isOptimizedLogBlocksScanEnabled() {
    return getBoolean(ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN);
  }

  public int getMaxNumDeltacommitsWhenPending() {
    return getIntOrDefault(METADATA_MAX_NUM_DELTACOMMITS_WHEN_PENDING);
  }

  public int getRecordIndexMinFileGroupCount() {
    return getInt(RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP);
  }

  public int getRecordIndexMaxFileGroupCount() {
    return getInt(RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP);
  }

  public float getRecordIndexGrowthFactor() {
    return getFloat(RECORD_INDEX_GROWTH_FACTOR_PROP);
  }

  public int getRecordIndexMaxFileGroupSizeBytes() {
    return getInt(RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP);
  }

  public String getSplliableMapDir() {
    return getString(SPILLABLE_MAP_DIR_PROP);
  }

  public long getMaxReaderMemory() {
    return getLong(MAX_READER_MEMORY_PROP);
  }

  public int getMaxReaderBufferSize() {
    return getInt(MAX_READER_BUFFER_SIZE_PROP);
  }

  public int getRecordIndexMaxParallelism() {
    return getInt(RECORD_INDEX_MAX_PARALLELISM);
  }

  public boolean shouldAutoInitialize() {
    return getBoolean(AUTO_INITIALIZE);
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

    public Builder withAsyncIndex(boolean asyncIndex) {
      metadataConfig.setValue(ASYNC_INDEX_ENABLE, String.valueOf(asyncIndex));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      metadataConfig.setValue(COMPACT_NUM_DELTA_COMMITS, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withLogCompactionEnabled(boolean enableLogCompaction) {
      metadataConfig.setValue(ENABLE_LOG_COMPACTION_ON_METADATA_TABLE, Boolean.toString(enableLogCompaction));
      return this;
    }

    public Builder withLogCompactBlocksThreshold(int logCompactBlocksThreshold) {
      metadataConfig.setValue(LOG_COMPACT_BLOCKS_THRESHOLD, Integer.toString(logCompactBlocksThreshold));
      return this;
    }

    public Builder withFileListingParallelism(int parallelism) {
      metadataConfig.setValue(FILE_LISTING_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withRecordIndexMaxParallelism(int parallelism) {
      metadataConfig.setValue(RECORD_INDEX_MAX_PARALLELISM, String.valueOf(parallelism));
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

    public Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      metadataConfig.setValue(ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN, String.valueOf(enableOptimizedLogBlocksScan));
      return this;
    }

    public Builder withMaxNumDeltacommitsWhenPending(int maxNumDeltaCommitsWhenPending) {
      metadataConfig.setValue(METADATA_MAX_NUM_DELTACOMMITS_WHEN_PENDING, String.valueOf(maxNumDeltaCommitsWhenPending));
      return this;
    }

    public Builder withEnableRecordIndex(boolean enabled) {
      metadataConfig.setValue(RECORD_INDEX_ENABLE_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withRecordIndexFileGroupCount(int minCount, int maxCount) {
      metadataConfig.setValue(RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP, String.valueOf(minCount));
      metadataConfig.setValue(RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP, String.valueOf(maxCount));
      return this;
    }

    public Builder withRecordIndexGrowthFactor(float factor) {
      metadataConfig.setValue(RECORD_INDEX_GROWTH_FACTOR_PROP, String.valueOf(factor));
      return this;
    }

    public Builder withRecordIndexMaxFileGroupSizeBytes(long sizeInBytes) {
      metadataConfig.setValue(RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP, String.valueOf(sizeInBytes));
      return this;
    }

    public Builder withSpillableMapDir(String dir) {
      metadataConfig.setValue(SPILLABLE_MAP_DIR_PROP, dir);
      return this;
    }

    public Builder withMaxReaderMemory(long mem) {
      metadataConfig.setValue(MAX_READER_MEMORY_PROP, String.valueOf(mem));
      return this;
    }

    public Builder withMaxReaderBufferSize(long mem) {
      metadataConfig.setValue(MAX_READER_BUFFER_SIZE_PROP, String.valueOf(mem));
      return this;
    }

    public Builder withMaxLogFileSizeBytes(long sizeInBytes) {
      metadataConfig.setValue(MAX_LOG_FILE_SIZE_BYTES_PROP, String.valueOf(sizeInBytes));
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
