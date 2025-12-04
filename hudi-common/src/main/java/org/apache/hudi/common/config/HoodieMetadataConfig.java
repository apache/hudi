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

import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.metadata.MetadataPartitionType;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;

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

  public static final ConfigProperty<Boolean> STREAMING_WRITE_ENABLED = ConfigProperty
      .key(METADATA_PREFIX + ".streaming.write.enabled")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Whether to enable streaming writes to metadata table or not. With streaming writes, we execute writes to both data table and metadata table "
          + "in streaming manner rather than two disjoint writes. By default "
          + "streaming writes to metadata table is enabled for SPARK engine for incremental operations and disabled for all other cases.");

  public static final ConfigProperty<Integer> STREAMING_WRITE_DATATABLE_WRITE_STATUSES_COALESCE_DIVISOR = ConfigProperty
      .key(METADATA_PREFIX + ".streaming.write.datatable.write.statuses.coalesce.divisor")
      .defaultValue(5000)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("When streaming writes to metadata table is enabled via hoodie.metadata.streaming.write.enabled, the data table write statuses are unioned "
          + "with metadata table write statuses before triggering the entire write dag. The data table write statuses will be coalesce down to the number of write statuses "
          + "divided by the specified divisor to avoid triggering thousands of no-op tasks for the data table writes which have their status cached.");

  public static final boolean DEFAULT_METADATA_ENABLE_FOR_READERS = true;

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

  public static final ConfigProperty<Integer> COLUMN_STATS_INDEX_MAX_COLUMNS = ConfigProperty
      .key(METADATA_PREFIX + ".index.column.stats.max.columns.to.index")
      .defaultValue(32)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Maximum number of columns to generate column stats for. If the config `"
          + COLUMN_STATS_INDEX_FOR_COLUMNS.key() + "` is set, this config will be ignored. "
          + "If the config `" + COLUMN_STATS_INDEX_FOR_COLUMNS.key() + "` is not set, "
          + "the column stats of the first `n` columns (`n` defined by this config) in the "
          + "table schema are generated.");

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

  public static final ConfigProperty<Boolean> GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".global.record.level.index.enable")
      .defaultValue(false)
      .withAlternatives(METADATA_PREFIX + ".record.index.enable")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Create the HUDI Record Index within the Metadata Table");

  public static final ConfigProperty<Boolean> RECORD_LEVEL_INDEX_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.level.index.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Create the HUDI Record Index within the Metadata Table for a partitioned dataset where a "
          + "pair of partition path and record key is unique across the entire table");

  public static final ConfigProperty<Integer> GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".global.record.level.index.min.filegroup.count")
      .defaultValue(10)
      .withAlternatives(METADATA_PREFIX + ".record.index.min.filegroup.count")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Minimum number of file groups to use for Record Index.");

  public static final ConfigProperty<Integer> GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".global.record.level.index.max.filegroup.count")
      .defaultValue(10000)
      .withAlternatives(METADATA_PREFIX + ".record.index.max.filegroup.count")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum number of file groups to use for Record Index.");

  public static final ConfigProperty<Integer> RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.level.index.min.filegroup.count")
      .defaultValue(1)
      .withAlternatives(METADATA_PREFIX + ".partitioned.record.index.min.filegroup.count")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Minimum number of file groups to use for Partitioned Record Index.");

  public static final ConfigProperty<Integer> RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".record.level.index.max.filegroup.count")
      .defaultValue(10)
      .withAlternatives(METADATA_PREFIX + ".partitioned.record.index.max.filegroup.count")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Maximum number of file groups to use for Partitioned Record Index.");

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

  public static final ConfigProperty<Boolean> EXPRESSION_INDEX_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".index.expression.enable")
      .defaultValue(false)
      .sinceVersion("1.0.0")
      .withDocumentation("Enable expression index within the metadata table. "
          + " When this configuration property is enabled (`true`), the Hudi writer automatically "
          + " keeps all expression indexes consistent with the data table. "
          + " When disabled (`false`), all expression indexes are deleted. "
          + " Note that individual expression index can only be created through a `CREATE INDEX` "
          + " and deleted through a `DROP INDEX` statement in Spark SQL.");

  public static final ConfigProperty<Integer> EXPRESSION_INDEX_FILE_GROUP_COUNT = ConfigProperty
      .key(METADATA_PREFIX + ".index.expression.file.group.count")
      .defaultValue(2)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Metadata expression index partition file group count.");

  public static final ConfigProperty<Integer> EXPRESSION_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.expression.parallelism")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Parallelism to use, when generating expression index.");

  public static final ConfigProperty<String> EXPRESSION_INDEX_COLUMN = ConfigProperty
      .key(METADATA_PREFIX + ".index.expression.column")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("1.0.1")
      .withDocumentation("Column for which expression index will be built.");

  public static final ConfigProperty<String> EXPRESSION_INDEX_NAME = HoodieIndexingConfig.INDEX_NAME;

  public static final ConfigProperty<String> EXPRESSION_INDEX_TYPE = HoodieIndexingConfig.INDEX_TYPE;

  public static final ConfigProperty<String> EXPRESSION_INDEX_OPTIONS = ConfigProperty
      .key(METADATA_PREFIX + ".index.expression.options")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("1.0.1")
      .withDocumentation("Options for the expression index, e.g. \"expr='from_unixtime', format='yyyy-MM-dd'\"");

  public static final ConfigProperty<Integer> METADATA_INDEX_PARTITION_STATS_FILE_GROUP_COUNT = ConfigProperty
      .key(METADATA_PREFIX + ".index.partition.stats.file.group.count")
      .defaultValue(1)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Metadata partition stats file group count. This controls the size of the base and "
          + "log files and read parallelism in the partition stats index.");

  public static final ConfigProperty<Integer> PARTITION_STATS_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.partition.stats.parallelism")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Parallelism to use, when generating partition stats index.");

  public static final ConfigProperty<Boolean> SECONDARY_INDEX_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".index.secondary.enable")
      .defaultValue(true)
      .sinceVersion("1.0.0")
      .withDocumentation("Enable secondary index within the metadata table. "
          + " When this configuration property is enabled (`true`), the Hudi writer automatically "
          + " keeps all secondary indexes consistent with the data table. "
          + " When disabled (`false`), all secondary indexes are deleted. "
          + " Note that individual secondary index can only be created through a `CREATE INDEX` "
          + " and deleted through a `DROP INDEX` statement in Spark SQL. ");

  public static final ConfigProperty<Integer> SECONDARY_INDEX_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".index.secondary.parallelism")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Parallelism to use, when generating secondary index.");

  public static final ConfigProperty<String> SECONDARY_INDEX_NAME = HoodieIndexingConfig.INDEX_NAME;

  public static final ConfigProperty<String> SECONDARY_INDEX_COLUMN = ConfigProperty
      .key(METADATA_PREFIX + ".index.secondary.column")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("1.0.1")
      .withDocumentation("Column for which secondary index will be built.");

  // Config to specify metadata index to delete
  public static final ConfigProperty<String> DROP_METADATA_INDEX = ConfigProperty
      .key(METADATA_PREFIX + ".index.drop")
      .noDefaultValue()
      .sinceVersion("1.0.1")
      .withDocumentation("Drop the specified index. "
          + "The value should be the name of the index to delete. You can check index names using `SHOW INDEXES` command. "
          + "The index name either starts with or matches exactly can be one of the following: "
          + StringUtils.join(Arrays.stream(MetadataPartitionType.values()).map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList()), ", "));

  // Range-based repartitioning configuration for metadata table lookups
  public static final ConfigProperty<Double> RANGE_REPARTITION_SAMPLING_FRACTION = ConfigProperty
      .key(METADATA_PREFIX + ".range.repartition.sampling.fraction")
      .defaultValue(0.01)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Sampling fraction used for range-based repartitioning during metadata table lookups. "
          + "This controls the accuracy vs performance trade-off for key distribution sampling.");

  public static final ConfigProperty<Integer> RANGE_REPARTITION_TARGET_RECORDS_PER_PARTITION = ConfigProperty
      .key(METADATA_PREFIX + ".range.repartition.target.records.per.partition")
      .defaultValue(10000)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Target number of records per partition during range-based repartitioning. "
          + "This helps control the size of each partition for optimal processing.");

  public static final ConfigProperty<Integer> RANGE_REPARTITION_RANDOM_SEED = ConfigProperty
      .key(METADATA_PREFIX + ".range.repartition.random.seed")
      .defaultValue(42)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Random seed used for sampling during range-based repartitioning. "
          + "This ensures reproducible results across runs.");

  public static final ConfigProperty<Integer> REPARTITION_MIN_PARTITIONS_THRESHOLD = ConfigProperty
      .key(METADATA_PREFIX + ".repartition.min.partitions.threshold")
      .defaultValue(100)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Minimum number of partitions threshold below which repartitioning is triggered. "
          + "When the number of partitions is below this threshold, data will be repartitioned for better parallelism.");

  public static final ConfigProperty<Integer> REPARTITION_DEFAULT_PARTITIONS = ConfigProperty
      .key(METADATA_PREFIX + ".repartition.default.partitions")
      .defaultValue(200)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Default number of partitions to use when repartitioning is needed. "
          + "This provides a reasonable level of parallelism for metadata table operations.");

  public static final ConfigProperty<Integer> METADATA_FILE_CACHE_MAX_SIZE_MB = ConfigProperty
      .key(METADATA_PREFIX + ".file.cache.max.size.mb")
      .defaultValue(50)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Max size in MB below which metadata file (HFile) will be downloaded "
          + "and cached entirely for the HFileReader.");

  public static final ConfigProperty<Boolean> BLOOM_FILTER_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".bloom.filter.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Whether to use bloom filter in the files for lookup in the metadata table.");

  // Configs that control the bloom filter that is written to the file footer
  public static final ConfigProperty<String> BLOOM_FILTER_TYPE = ConfigProperty
      .key(METADATA_PREFIX + ".bloom.filter.type")
      .defaultValue(BloomFilterTypeCode.DYNAMIC_V0.name())
      .withValidValues(BloomFilterTypeCode.SIMPLE.name(), BloomFilterTypeCode.DYNAMIC_V0.name())
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation(BloomFilterTypeCode.class, "Bloom filter type for the files in the metadata table");

  public static final ConfigProperty<String> BLOOM_FILTER_NUM_ENTRIES = ConfigProperty
      .key(METADATA_PREFIX + ".bloom.filter.num.entries")
      .defaultValue("10000")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("This is the number of entries stored in a bloom filter for the files in the metadata table. "
          + "The rationale for the default: 10000 is chosen to be a good tradeoff between false positive rate and "
          + "storage size. Warning: Setting this very low generates a lot of false positives and the metadata "
          + "table reading has to scan a lot more files than it has to and setting this to a very high number "
          + "increases the size every base file linearly (roughly 4KB for every 50000 entries). "
          + "This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.");

  public static final ConfigProperty<String> BLOOM_FILTER_FPP = ConfigProperty
      .key(METADATA_PREFIX + ".bloom.filter.fpp")
      .defaultValue("0.000000001")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Expected probability a false positive in a bloom filter for the files in the "
          + "metadata table. This is used to calculate how many bits should be assigned for the bloom filter "
          + "and the number of hash functions. This is usually set very low (default: 0.000000001), "
          + "we like to tradeoff disk space for lower false positives. If the number of entries "
          + "added to bloom filter exceeds the configured value (hoodie.metadata.bloom.num_entries), "
          + "then this fpp may not be honored.");

  public static final ConfigProperty<String> BLOOM_FILTER_DYNAMIC_MAX_ENTRIES = ConfigProperty
      .key(METADATA_PREFIX + ".bloom.filter.dynamic.max.entries")
      .defaultValue("100000")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("The threshold for the maximum number of keys to record in a dynamic "
          + "bloom filter row for the files in the metadata table. Only applies if the filter "
          + "type (" + BLOOM_FILTER_TYPE.key() + " ) is BloomFilterTypeCode.DYNAMIC_V0.");

  public static final ConfigProperty<Integer> RECORD_PREPARATION_PARALLELISM = ConfigProperty
      .key(METADATA_PREFIX + ".record.preparation.parallelism")
      .defaultValue(0)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("when set to positive number, metadata table record preparation stages "
          + "honor the set value for number of tasks. If not, number of write status's from data "
          + "table writes will be used for metadata table record preparation");

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

  public boolean isEnabled() {
    return getBoolean(ENABLE);
  }

  public boolean isStreamingWriteEnabled() {
    return getBoolean(STREAMING_WRITE_ENABLED);
  }

  public int getStreamingWritesCoalesceDivisorForDataTableWrites() {
    return getInt(HoodieMetadataConfig.STREAMING_WRITE_DATATABLE_WRITE_STATUSES_COALESCE_DIVISOR);
  }

  public boolean isBloomFilterIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_BLOOM_FILTER);
  }

  public boolean isColumnStatsIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_COLUMN_STATS);
  }

  public boolean isGlobalRecordLevelIndexEnabled() {
    return isEnabled() && getBooleanOrDefault(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP);
  }

  public boolean isRecordLevelIndexEnabled() {
    return isEnabled() && getBooleanOrDefault(RECORD_LEVEL_INDEX_ENABLE_PROP);
  }

  public List<String> getColumnsEnabledForColumnStatsIndex() {
    return StringUtils.split(getString(COLUMN_STATS_INDEX_FOR_COLUMNS), CONFIG_VALUES_DELIMITER);
  }

  public Integer maxColumnsToIndexForColStats() {
    return getIntOrDefault(COLUMN_STATS_INDEX_MAX_COLUMNS);
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

  public int getGlobalRecordLevelIndexMinFileGroupCount() {
    return getInt(GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP);
  }

  public int getRecordLevelIndexMinFileGroupCount() {
    return getInt(RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP);
  }

  public int getGlobalRecordLevelIndexMaxFileGroupCount() {
    return getInt(GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP);
  }

  public int getRecordLevelIndexMaxFileGroupCount() {
    return getInt(RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP);
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

  public boolean isExpressionIndexEnabled() {
    return getBooleanOrDefault(EXPRESSION_INDEX_ENABLE_PROP) && !isDropMetadataIndex(MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath());
  }

  public int getExpressionIndexFileGroupCount() {
    return getInt(EXPRESSION_INDEX_FILE_GROUP_COUNT);
  }

  public int getExpressionIndexParallelism() {
    return getInt(EXPRESSION_INDEX_PARALLELISM);
  }

  public String getExpressionIndexColumn() {
    return getString(EXPRESSION_INDEX_COLUMN);
  }

  public String getExpressionIndexName() {
    return getString(EXPRESSION_INDEX_NAME);
  }

  public String getExpressionIndexType() {
    return getString(EXPRESSION_INDEX_TYPE);
  }

  public Map<String, String> getExpressionIndexOptions() {
    return getExpressionIndexOptions(getString(EXPRESSION_INDEX_OPTIONS));
  }

  public boolean enableBloomFilter() {
    return getBooleanOrDefault(BLOOM_FILTER_ENABLE);
  }

  public String getBloomFilterType() {
    return getStringOrDefault(BLOOM_FILTER_TYPE);
  }

  public int getBloomFilterNumEntries() {
    return getIntOrDefault(BLOOM_FILTER_NUM_ENTRIES);
  }

  public double getBloomFilterFpp() {
    return getDoubleOrDefault(BLOOM_FILTER_FPP);
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return getIntOrDefault(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES);
  }

  private Map<String, String> getExpressionIndexOptions(String configValue) {
    Map<String, String> optionsMap = new HashMap<>();
    if (StringUtils.isNullOrEmpty(configValue)) {
      return optionsMap;
    }

    // Split the string into key-value pairs by comma
    String[] keyValuePairs = configValue.split(",");
    for (String pair : keyValuePairs) {
      String[] keyValue = pair.split("=", 2); // Split into key and value, allowing '=' in the value
      if (keyValue.length == 2) {
        optionsMap.put(keyValue[0].trim(), keyValue[1].trim());
      } else {
        throw new IllegalArgumentException("Invalid key-value pair: " + pair);
      }
    }

    return optionsMap;
  }

  public boolean isPartitionStatsIndexEnabled() {
    return getBooleanOrDefault(ENABLE_METADATA_INDEX_COLUMN_STATS);
  }

  public int getPartitionStatsIndexFileGroupCount() {
    return getInt(METADATA_INDEX_PARTITION_STATS_FILE_GROUP_COUNT);
  }

  public int getPartitionStatsIndexParallelism() {
    return getInt(PARTITION_STATS_INDEX_PARALLELISM);
  }

  public boolean isSecondaryIndexEnabled() {
    // Secondary index is enabled only iff record index (primary key index) is also enabled and a secondary index column is specified.
    return isGlobalRecordLevelIndexEnabled() && getBoolean(SECONDARY_INDEX_ENABLE_PROP) && StringUtils.nonEmpty(getSecondaryIndexColumn())
        && !isDropMetadataIndex(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath());
  }

  public int getSecondaryIndexParallelism() {
    return getInt(SECONDARY_INDEX_PARALLELISM);
  }

  public String getSecondaryIndexColumn() {
    return getString(SECONDARY_INDEX_COLUMN);
  }

  public String getSecondaryIndexName() {
    return getString(SECONDARY_INDEX_NAME);
  }

  public String getMetadataIndexToDrop() {
    return getString(DROP_METADATA_INDEX);
  }

  public double getRangeRepartitionSamplingFraction() {
    return getDouble(RANGE_REPARTITION_SAMPLING_FRACTION);
  }

  public int getRangeRepartitionTargetRecordsPerPartition() {
    return getInt(RANGE_REPARTITION_TARGET_RECORDS_PER_PARTITION);
  }

  public int getRangeRepartitionRandomSeed() {
    return getInt(RANGE_REPARTITION_RANDOM_SEED);
  }

  public int getRepartitionMinPartitionsThreshold() {
    return getInt(REPARTITION_MIN_PARTITIONS_THRESHOLD);
  }

  public int getRepartitionDefaultPartitions() {
    return getInt(REPARTITION_DEFAULT_PARTITIONS);
  }

  public int getFileCacheMaxSizeMB() {
    return getInt(METADATA_FILE_CACHE_MAX_SIZE_MB);
  }

  public int getRecordPreparationParallelism() {
    return getIntOrDefault(RECORD_PREPARATION_PARALLELISM);
  }

  /**
   * Checks if a specific metadata index is marked for dropping based on the metadata configuration.
   * NOTE: Only applicable for secondary indexes (SI) or expression indexes (EI).
   *
   * <p>An index is considered marked for dropping if:
   * <ul>
   *   <li>The metadata configuration specifies a non-empty index to drop, and</li>
   *   <li>The specified index matches the given index name.</li>
   * </ul>
   *
   * @param indexName the name of the metadata index to check
   * @return {@code true} if the specified metadata index is marked for dropping, {@code false} otherwise.
   */
  public boolean isDropMetadataIndex(String indexName) {
    String subIndexNameToDrop = getMetadataIndexToDrop();
    if (StringUtils.isNullOrEmpty(subIndexNameToDrop)) {
      return false;
    }
    if (StringUtils.isNullOrEmpty(indexName)) {
      return false;
    }
    // Only applicable for SI or EI
    checkArgument(indexName.startsWith(PARTITION_NAME_EXPRESSION_INDEX_PREFIX)
        || indexName.startsWith(PARTITION_NAME_SECONDARY_INDEX_PREFIX), "Unexpected index name to drop: " + indexName);
    return subIndexNameToDrop.contains(indexName);
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

    public Builder withStreamingWriteEnabled(boolean enabled) {
      metadataConfig.setValue(STREAMING_WRITE_ENABLED, String.valueOf(enabled));
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

    public Builder withMaxColumnsToIndexForColStats(int maxCols) {
      metadataConfig.setValue(COLUMN_STATS_INDEX_MAX_COLUMNS, String.valueOf(maxCols));
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

    public Builder withEnableGlobalRecordLevelIndex(boolean enabled) {
      metadataConfig.setValue(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withRecordIndexFileGroupCount(int minCount, int maxCount) {
      metadataConfig.setValue(GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP, String.valueOf(minCount));
      metadataConfig.setValue(GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP, String.valueOf(maxCount));
      return this;
    }

    public Builder withEnableRecordLevelIndex(boolean enabled) {
      metadataConfig.setValue(RECORD_LEVEL_INDEX_ENABLE_PROP, String.valueOf(enabled));
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

    public Builder withExpressionIndexFileGroupCount(int fileGroupCount) {
      metadataConfig.setValue(EXPRESSION_INDEX_FILE_GROUP_COUNT, String.valueOf(fileGroupCount));
      return this;
    }

    public Builder withExpressionIndexParallelism(int parallelism) {
      metadataConfig.setValue(EXPRESSION_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withExpressionIndexColumn(String column) {
      metadataConfig.setValue(EXPRESSION_INDEX_COLUMN, column);
      return this;
    }

    public Builder withExpressionIndexName(String name) {
      metadataConfig.setValue(EXPRESSION_INDEX_NAME, name);
      return this;
    }

    public Builder withExpressionIndexType(String type) {
      metadataConfig.setValue(EXPRESSION_INDEX_TYPE, type);
      return this;
    }

    public Builder withExpressionIndexOptions(Map<String, String> options) {
      metadataConfig.setValue(EXPRESSION_INDEX_OPTIONS, options.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(",")));
      return this;
    }

    public Builder withMetadataIndexPartitionStatsFileGroupCount(int fileGroupCount) {
      metadataConfig.setValue(METADATA_INDEX_PARTITION_STATS_FILE_GROUP_COUNT, String.valueOf(fileGroupCount));
      return this;
    }

    public Builder withPartitionStatsIndexParallelism(int parallelism) {
      metadataConfig.setValue(PARTITION_STATS_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withSecondaryIndexEnabled(boolean enabled) {
      metadataConfig.setValue(SECONDARY_INDEX_ENABLE_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withExpressionIndexEnabled(boolean enabled) {
      metadataConfig.setValue(EXPRESSION_INDEX_ENABLE_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withSecondaryIndexForColumn(String column) {
      metadataConfig.setValue(SECONDARY_INDEX_COLUMN, column);
      return this;
    }

    public Builder withSecondaryIndexName(String name) {
      metadataConfig.setValue(SECONDARY_INDEX_NAME, name);
      return this;
    }

    public Builder withSecondaryIndexParallelism(int parallelism) {
      metadataConfig.setValue(SECONDARY_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withDropMetadataIndex(String indexName) {
      metadataConfig.setValue(DROP_METADATA_INDEX, indexName);
      return this;
    }

    public Builder withRangeRepartitionSamplingFraction(double samplingFraction) {
      metadataConfig.setValue(RANGE_REPARTITION_SAMPLING_FRACTION, String.valueOf(samplingFraction));
      return this;
    }

    public Builder withRangeRepartitionTargetRecordsPerPartition(int targetRecordsPerPartition) {
      metadataConfig.setValue(RANGE_REPARTITION_TARGET_RECORDS_PER_PARTITION, String.valueOf(targetRecordsPerPartition));
      return this;
    }

    public Builder withRangeRepartitionRandomSeed(int randomSeed) {
      metadataConfig.setValue(RANGE_REPARTITION_RANDOM_SEED, String.valueOf(randomSeed));
      return this;
    }

    public Builder withRepartitionMinPartitionsThreshold(int minPartitionsThreshold) {
      metadataConfig.setValue(REPARTITION_MIN_PARTITIONS_THRESHOLD, String.valueOf(minPartitionsThreshold));
      return this;
    }

    public Builder withRepartitionDefaultPartitions(int defaultPartitions) {
      metadataConfig.setValue(REPARTITION_DEFAULT_PARTITIONS, String.valueOf(defaultPartitions));
      return this;
    }

    public HoodieMetadataConfig build() {
      metadataConfig.setDefaultValue(ENABLE, getDefaultMetadataEnable(engineType));
      metadataConfig.setDefaultValue(ENABLE_METADATA_INDEX_COLUMN_STATS, getDefaultColStatsEnable(engineType));
      metadataConfig.setDefaultValue(SECONDARY_INDEX_ENABLE_PROP, getDefaultSecondaryIndexEnable(engineType));
      metadataConfig.setDefaultValue(STREAMING_WRITE_ENABLED, getDefaultForStreamingWriteEnabled(engineType));
      // fix me: disable when schema on read is enabled.
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

    private boolean getDefaultForStreamingWriteEnabled(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return true;
        case FLINK:
        case JAVA:
          return false;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    private boolean getDefaultColStatsEnable(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return true;
        case FLINK:
        case JAVA:
          return false; // HUDI-8814
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    private boolean getDefaultSecondaryIndexEnable(EngineType engineType) {
      switch (engineType) {
        case SPARK:
        case JAVA:
          return true;
        case FLINK:
          return false;
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }

  /**
   * The config is now deprecated. Partition stats are configured using the column stats config itself.
   */
  @Deprecated
  public static final String ENABLE_METADATA_INDEX_PARTITION_STATS =
      METADATA_PREFIX + ".index.partition.stats.enable";

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
