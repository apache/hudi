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

import org.apache.hudi.client.clustering.plan.strategy.FlinkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.common.config.AdvancedConfig;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSyncTableStrategy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.overwrite.PartitionOverwriteMode;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.util.ClientIds;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;

/**
 * Hoodie Flink config options.
 *
 * <p>It has the options for Hoodie table read and write. It also defines some utilities.
 */
@ConfigClassProperty(name = "Flink Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + " The actual datasource level configs are listed below.")
public class FlinkOptions extends HoodieConfig {
  private FlinkOptions() {
  }

  // ------------------------------------------------------------------------
  //  Base Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<String> PATH = ConfigOptions
      .key("path")
      .stringType()
      .noDefaultValue()
      .withDescription("Base path for the target hoodie table.\n"
          + "The path would be created if it does not exist,\n"
          + "otherwise a Hoodie table expects to be initialized successfully");

  // ------------------------------------------------------------------------
  //  Common Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<String> DATABASE_NAME = ConfigOptions
      .key(HoodieTableConfig.DATABASE_NAME.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Database name to register to Hive metastore");

  public static final ConfigOption<String> TABLE_NAME = ConfigOptions
      .key(HoodieWriteConfig.TBL_NAME.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Table name to register to Hive metastore");

  public static final String TABLE_TYPE_COPY_ON_WRITE = HoodieTableType.COPY_ON_WRITE.name();
  public static final String TABLE_TYPE_MERGE_ON_READ = HoodieTableType.MERGE_ON_READ.name();
  public static final ConfigOption<String> TABLE_TYPE = ConfigOptions
      .key("table.type")
      .stringType()
      .defaultValue(TABLE_TYPE_COPY_ON_WRITE)
      .withFallbackKeys(HoodieTableConfig.TYPE.key())
      .withDescription("Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ");

  public static final String NO_PRE_COMBINE = "no_precombine";
  public static final ConfigOption<String> ORDERING_FIELDS = ConfigOptions
      .key("ordering.fields")
      .stringType()
      .defaultValue("ts")
      .withFallbackKeys("precombine.field", "write.precombine.field", HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key())
      .withDescription("Comma separated list of fields used in records merging. When two records have the same\n"
          + "key value, we will pick the one with the largest value for the ordering field,\n"
          + "determined by Object.compareTo(..). For multiple fields if first key comparison is same, second key comparison is made and so on.\n"
          + "Config precombine.field is now deprecated, please use ordering.fields instead.");

  @AdvancedConfig
  public static final ConfigOption<String> PAYLOAD_CLASS_NAME = ConfigOptions
      .key("payload.class")
      .stringType()
      .defaultValue(EventTimeAvroPayload.class.getName())
      .withFallbackKeys("write.payload.class", HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key())
      .withDescription("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.\n"
          + "This will render any value set for the option in-effective");

  public static final ConfigOption<String> INSERT_PARTITIONER_CLASS_NAME = ConfigOptions
      .key("write.insert.partitioner.class.name")
      .stringType()
      .defaultValue("")
      .withDescription("Insert partitioner to use aiming to re-balance records and reducing small file number "
          + "in the scenario of multi-level partitioning. For example dt/hour/eventID"
          + "Currently support org.apache.hudi.sink.partitioner.GroupedInsertPartitioner");

  public static final ConfigOption<Integer> DEFAULT_PARALLELISM_PER_PARTITION = ConfigOptions
      .key("write.insert.partitioner.default_parallelism_per_partition")
      .intType()
      .defaultValue(30)
      .withDescription("The parallelism to use in each partition when using GroupedInsertPartitioner.");

  @AdvancedConfig
  public static final ConfigOption<String> RECORD_MERGER_IMPLS = ConfigOptions
      .key("record.merger.impls")
      .stringType()
      .defaultValue(EventTimeFlinkRecordMerger.class.getName())
      .withFallbackKeys(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key())
      .withDescription("List of HoodieMerger implementations constituting Hudi's merging strategy -- based on the engine used. "
          + "These merger impls will filter by record.merger.strategy. "
          + "Hudi will pick most efficient implementation to perform merging/combining of the records (during update, reading MOR table, etc)");

  @AdvancedConfig
  public static final ConfigOption<String> RECORD_MERGE_MODE = ConfigOptions
      .key(HoodieWriteConfig.RECORD_MERGE_MODE.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Using EVENT_TIME_ORDERING to merge records by ordering value,"
          + "using COMMIT_TIME_ORDERING to merge records by commit time,"
          + "and using CUSTOM to merge records by the user specified logic.");

  @AdvancedConfig
  public static final ConfigOption<String> RECORD_MERGER_STRATEGY_ID = ConfigOptions
      .key("record.merger.strategy")
      .stringType()
      .defaultValue(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID)
      .withFallbackKeys(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key())
      .withDescription("Id of merger strategy. Hudi will pick HoodieRecordMerger implementations in record.merger.impls which has the same merger strategy id");

  @AdvancedConfig
  public static final ConfigOption<String> PARTITION_DEFAULT_NAME = ConfigOptions
      .key("partition.default_name")
      .stringType()
      .defaultValue(DEFAULT_PARTITION_PATH) // keep sync with hoodie style
      .withDescription("The default partition name in case the dynamic partition"
          + " column value is null/empty string");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_EXTRA_METADATA_ENABLED = ConfigOptions
      .key("write.extra.metadata.enabled")
      .booleanType()
      .defaultValue(false) // keep sync with hoodie style
      .withDescription("If enabled, the checkpoint Id will also be written to hudi metadata.");

  // ------------------------------------------------------------------------
  //  Changelog Capture Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<Boolean> CHANGELOG_ENABLED = ConfigOptions
      .key("changelog.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to keep all the intermediate changes, "
          + "we try to keep all the changes of a record when enabled:\n"
          + "1). The sink accept the UPDATE_BEFORE message;\n"
          + "2). The source try to emit every changes of a record.\n"
          + "The semantics is best effort because the compaction job would finally merge all changes of a record into one.\n"
          + " default false to have UPSERT semantics");

  public static final ConfigOption<Boolean> CDC_ENABLED = ConfigOptions
      .key("cdc.enabled")
      .booleanType()
      .defaultValue(false)
      .withFallbackKeys(HoodieTableConfig.CDC_ENABLED.key())
      .withDescription("When enable, persist the change data if necessary, and can be queried as a CDC query mode");

  public static final ConfigOption<String> SUPPLEMENTAL_LOGGING_MODE = ConfigOptions
      .key("cdc.supplemental.logging.mode")
      .stringType()
      .defaultValue(DATA_BEFORE_AFTER.name())
      .withFallbackKeys(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key())
      .withDescription("Setting 'op_key_only' persists the 'op' and the record key only, "
          + "setting 'data_before' persists the additional 'before' image, "
          + "and setting 'data_before_after' persists the additional 'before' and 'after' images.");

  // ------------------------------------------------------------------------
  //  Metadata table Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<Boolean> METADATA_ENABLED = ConfigOptions
      .key("metadata.enabled")
      .booleanType()
      .defaultValue(true)
      .withFallbackKeys(HoodieMetadataConfig.ENABLE.key())
      .withDescription("Enable the internal metadata table which serves table metadata like level file listings, default enabled");

  public static final ConfigOption<Integer> METADATA_COMPACTION_DELTA_COMMITS = ConfigOptions
      .key("metadata.compaction.delta_commits")
      .intType()
      .defaultValue(10)
      .withDescription("Max delta commits for metadata table to trigger compaction, default 10");

  // ------------------------------------------------------------------------
  //  Index Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<String> INDEX_TYPE = ConfigOptions
      .key("index.type")
      .stringType()
      .defaultValue(HoodieIndex.IndexType.FLINK_STATE.name())
      .withFallbackKeys(HoodieIndexConfig.INDEX_TYPE.key())
      .withDescription("Index type of Flink write job, default is using state backed index.");

  @AdvancedConfig
  public static final ConfigOption<Boolean> INDEX_BOOTSTRAP_ENABLED = ConfigOptions
      .key("index.bootstrap.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to bootstrap the index state from existing hoodie table, default false");

  @AdvancedConfig
  public static final ConfigOption<Double> INDEX_STATE_TTL = ConfigOptions
      .key("index.state.ttl")
      .doubleType()
      .defaultValue(0D)
      .withDescription("Index state ttl in days, default stores the index permanently");

  @AdvancedConfig
  public static final ConfigOption<Boolean> INDEX_GLOBAL_ENABLED = ConfigOptions
      .key("index.global.enabled")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to update index for the old partition path\n"
          + "if same key record with different partition path came in, default true");

  @AdvancedConfig
  public static final ConfigOption<String> INDEX_PARTITION_REGEX = ConfigOptions
      .key("index.partition.regex")
      .stringType()
      .defaultValue(".*")
      .withDescription("Whether to load partitions in state if partition path matching， default `*`");

  // ------------------------------------------------------------------------
  //  Read Options
  // ------------------------------------------------------------------------

  @AdvancedConfig
  public static final ConfigOption<Integer> READ_TASKS = ConfigOptions
      .key("read.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do actual read, default is the parallelism of the execution environment");

  @AdvancedConfig
  public static final ConfigOption<String> SOURCE_AVRO_SCHEMA_PATH = ConfigOptions
      .key("source.avro-schema.path")
      .stringType()
      .noDefaultValue()
      .withDescription("Source avro schema file path, the parsed schema is used for deserialization");

  @AdvancedConfig
  public static final ConfigOption<String> SOURCE_AVRO_SCHEMA = ConfigOptions
      .key("source.avro-schema")
      .stringType()
      .noDefaultValue()
      .withDescription("Source avro schema string, the parsed schema is used for deserialization");

  public static final String QUERY_TYPE_SNAPSHOT = "snapshot";
  public static final String QUERY_TYPE_READ_OPTIMIZED = "read_optimized";
  public static final String QUERY_TYPE_INCREMENTAL = "incremental";
  public static final ConfigOption<String> QUERY_TYPE = ConfigOptions
      .key("hoodie.datasource.query.type")
      .stringType()
      .defaultValue(QUERY_TYPE_SNAPSHOT)
      .withDescription("Decides how data files need to be read, in\n"
          + "1) Snapshot mode (obtain latest view, based on row & columnar data);\n"
          + "2) incremental mode (new data since an instantTime);\n"
          + "3) Read Optimized mode (obtain latest view, based on columnar data)\n."
          + "Default: snapshot");

  public static final String REALTIME_SKIP_MERGE = HoodieReaderConfig.REALTIME_SKIP_MERGE;
  public static final String REALTIME_PAYLOAD_COMBINE = HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE;
  @AdvancedConfig
  public static final ConfigOption<String> MERGE_TYPE = ConfigOptions
      .key(HoodieReaderConfig.MERGE_TYPE.key())
      .stringType()
      .defaultValue(HoodieReaderConfig.MERGE_TYPE.defaultValue())
      .withDescription(HoodieReaderConfig.MERGE_TYPE.doc());

  @AdvancedConfig
  public static final ConfigOption<Boolean> READ_UTC_TIMEZONE = ConfigOptions
      .key("read.utc-timezone")
      .booleanType()
      .defaultValue(true)
      .withDescription("Use UTC timezone or local timezone to the conversion between epoch"
          + " time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x"
          + " use UTC timezone, by default true");

  public static final ConfigOption<Boolean> READ_AS_STREAMING = ConfigOptions
      .key("read.streaming.enabled")
      .booleanType()
      .defaultValue(false)// default read as batch
      .withDescription("Whether to read as streaming source, default false");

  @AdvancedConfig
  public static final ConfigOption<Integer> READ_STREAMING_CHECK_INTERVAL = ConfigOptions
      .key("read.streaming.check-interval")
      .intType()
      .defaultValue(60)// default 1 minute
      .withDescription("Check interval for streaming read of SECOND, default 1 minute");

  @AdvancedConfig
  // this option is experimental
  public static final ConfigOption<Boolean> READ_STREAMING_SKIP_COMPACT = ConfigOptions
      .key("read.streaming.skip_compaction")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to skip compaction instants and avoid reading compacted base files for streaming read to improve read performance.\n"
          + "This option can be used to avoid reading duplicates when changelog mode is enabled, it is a solution to keep data integrity\n");

  @AdvancedConfig
  // this option is experimental
  public static final ConfigOption<Boolean> READ_STREAMING_SKIP_CLUSTERING = ConfigOptions
      .key("read.streaming.skip_clustering")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to skip clustering instants to avoid reading base files of clustering operations for streaming read "
          + "to improve read performance.");

  // this option is experimental
  public static final ConfigOption<Boolean> READ_STREAMING_SKIP_INSERT_OVERWRITE = ConfigOptions
      .key("read.streaming.skip_insertoverwrite")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to skip insert overwrite instants to avoid reading base files of insert overwrite operations for streaming read. "
          + "In streaming scenarios, insert overwrite is usually used to repair data, here you can control the visibility of downstream streaming read.");

  public static final String START_COMMIT_EARLIEST = "earliest";
  public static final ConfigOption<String> READ_START_COMMIT = ConfigOptions
      .key("read.start-commit")
      .stringType()
      .noDefaultValue()
      .withDescription("Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss', "
          + "by default reading from the latest instant for streaming read");

  public static final ConfigOption<String> READ_END_COMMIT = ConfigOptions
      .key("read.end-commit")
      .stringType()
      .noDefaultValue()
      .withDescription("End commit instant for reading, the commit time format should be 'yyyyMMddHHmmss'");

  public static final ConfigOption<Integer> READ_COMMITS_LIMIT = ConfigOptions
      .key("read.commits.limit")
      .intType()
      .noDefaultValue()
      .withDescription("The maximum number of commits allowed to read in each instant check, if it is streaming read, "
          + "the avg read instants number per-second would be 'read.commits.limit'/'read.streaming.check-interval', by "
          + "default no limit");

  public static final ConfigOption<Integer> READ_SPLITS_LIMIT = ConfigOptions
      .key("read.splits.limit")
      .intType()
      .defaultValue(Integer.MAX_VALUE)
      .withDescription("The maximum number of splits allowed to read in each instant check, if it is streaming read, "
          + "the avg read splits number per-second would be 'read.splits.limit'/'read.streaming.check-interval', by "
          + "default no limit");

  @AdvancedConfig
  public static final ConfigOption<Boolean> READ_CDC_FROM_CHANGELOG = ConfigOptions
      .key("read.cdc.from.changelog")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to consume the delta changes only from the cdc changelog files.\n"
          + "When CDC is enabled, i). for COW table, the changelog is generated on each file update;\n"
          + "ii). for MOR table, the changelog is generated on compaction.\n"
          + "By default, always read from the changelog file,\n"
          + "once it is disabled, the reader would infer the changes based on the file slice dependencies.");

  @AdvancedConfig
  public static final ConfigOption<Boolean> READ_DATA_SKIPPING_ENABLED = ConfigOptions
      .key("read.data.skipping.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Enables data-skipping allowing queries to leverage indexes to reduce the search space by "
          + "skipping over files");

  @AdvancedConfig
  public static final ConfigOption<Integer> RECORD_INDEX_KEYS_MAX_COUNT = ConfigOptions
      .key("read.record.index.keys.max.count")
      .intType()
      .defaultValue(8)
      .withDescription("Record Level index statistics will be read from metadata table (MDT) for data skipping optimization,\n"
          + "and currently the index statistics are collected by a single process, i.e., flink client for batch query or\n"
          + "split monitor operator for streaming query. This config is used to constrain the maximum number of hoodie\n"
          + "keys that can be read from MDT without sacrificing any performance. If the number of hoodie keys from query\n"
          + "predicate is bigger than the maximum value, the query will fallback to not using record level index.\n"
          + "E.g., given query: SELECT * FROM T WHERE `uuid` IN (1,2,3,4,5,6,7,8,9), the number of hoodie keys is 9, and\n"
          + "the maximum value is 8, so the source will not perform data skipping based on record level index.");

  // ------------------------------------------------------------------------
  //  Write Options
  // ------------------------------------------------------------------------

  @AdvancedConfig
  public static final ConfigOption<Boolean> INSERT_CLUSTER = ConfigOptions
      .key("write.insert.cluster")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to merge small files for insert mode, "
          + "if true, the write throughput will decrease because the read/write of existing small file, "
          + "only valid for COW table, default false");

  public static final ConfigOption<String> OPERATION = ConfigOptions
      .key("write.operation")
      .stringType()
      .defaultValue(WriteOperationType.UPSERT.value())
      .withDescription("The write operation, that this write should do");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_TABLE_VERSION = ConfigOptions
      .key(HoodieWriteConfig.WRITE_TABLE_VERSION.key())
      .intType()
      .defaultValue(HoodieWriteConfig.WRITE_TABLE_VERSION.defaultValue())
      .withDescription("Table version produced by this writer.");

  @AdvancedConfig
  public static final ConfigOption<String> WRITE_TABLE_FORMAT = ConfigOptions
          .key(HoodieTableConfig.TABLE_FORMAT.key())
          .stringType()
          .defaultValue(HoodieTableConfig.TABLE_FORMAT.defaultValue())
          .withDescription("Table format produced by this writer.");

  /**
   * Flag to indicate whether to drop duplicates before insert/upsert.
   * By default false to gain extra performance.
   */
  @AdvancedConfig
  public static final ConfigOption<Boolean> PRE_COMBINE = ConfigOptions
      .key("write.precombine")
      .booleanType()
      .defaultValue(false)
      .withDescription("Flag to indicate whether to drop duplicates before insert/upsert.\n"
          + "By default these cases will accept duplicates, to gain extra performance:\n"
          + "1) insert operation;\n"
          + "2) upsert for MOR table, the MOR table deduplicate on reading");

  @AdvancedConfig
  public static final ConfigOption<Integer> RETRY_TIMES = ConfigOptions
      .key("write.retry.times")
      .intType()
      .defaultValue(3)
      .withDescription("Flag to indicate how many times streaming job should retry for a failed checkpoint batch.\n"
          + "By default 3");

  @AdvancedConfig
  public static final ConfigOption<Long> RETRY_INTERVAL_MS = ConfigOptions
      .key("write.retry.interval.ms")
      .longType()
      .defaultValue(2000L)
      .withDescription("Flag to indicate how long (by millisecond) before a retry should issued for failed checkpoint batch.\n"
          + "By default 2000 and it will be doubled by every retry");

  @AdvancedConfig
  public static final ConfigOption<Boolean> IGNORE_FAILED = ConfigOptions
      .key("write.ignore.failed")
      .booleanType()
      .defaultValue(false)
      .withDescription("Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. \n"
          + "By default false. Turning this on, could hide the write status errors while the flink checkpoint moves ahead. \n"
          + "So, would recommend users to use this with caution.");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_FAIL_FAST = ConfigOptions
          .key("write.fail.fast")
          .booleanType()
          .defaultValue(false)
          .withDescription("Flag to indicate whether to fail job immediately when an error record is detected. \n"
                  + "Currently, this option is only applied to Flink append write functions.");

  public static final ConfigOption<String> RECORD_KEY_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())
      .stringType()
      .defaultValue("uuid")
      .withDescription("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
          + "the dot notation eg: `a.b.c`");

  @AdvancedConfig
  public static final ConfigOption<String> INDEX_KEY_FIELD = ConfigOptions
      .key(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key())
      .stringType()
      .defaultValue("")
      .withDescription("Index key field. Value to be used as hashing to find the bucket ID. Should be a subset of or equal to the recordKey fields.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
          + "the dot notation eg: `a.b.c`");

  @AdvancedConfig
  public static final ConfigOption<String> BUCKET_INDEX_ENGINE_TYPE = ConfigOptions
      .key(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key())
      .stringType()
      .defaultValue("SIMPLE")
      .withDescription("Type of bucket index engine. Available options: [SIMPLE | CONSISTENT_HASHING]");

  @AdvancedConfig
  public static final ConfigOption<Integer> BUCKET_INDEX_NUM_BUCKETS = ConfigOptions
      .key(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())
      .intType()
      .defaultValue(4) // default 4 buckets per partition
      .withDescription("Hudi bucket number per partition. Only affected if using Hudi bucket index.");

  @AdvancedConfig
  public static final ConfigOption<String> BUCKET_INDEX_PARTITION_RULE = ConfigOptions
      .key(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.key())
      .stringType()
      .defaultValue(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.defaultValue())
      .withDescription("Rule parser for expressions when using partition level bucket index, default regex.");

  @AdvancedConfig
  public static final ConfigOption<String> BUCKET_INDEX_PARTITION_EXPRESSIONS = ConfigOptions
      .key(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Users can use this parameter to specify expression and the corresponding bucket "
          + "numbers (separated by commas).Multiple rules are separated by semicolons like "
          + "hoodie.bucket.index.partition.expressions=expression1,bucket-number1;expression2,bucket-number2");
  public static final ConfigOption<String> PARTITION_PATH_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
      .stringType()
      .defaultValue("")
      .withDescription("Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.\n"
          + "Actual value obtained by invoking .toString(), default ''");

  @AdvancedConfig
  public static final ConfigOption<Boolean> URL_ENCODE_PARTITIONING = ConfigOptions
      .key(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key())
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to encode the partition path url, default false");

  public static final ConfigOption<Boolean> HIVE_STYLE_PARTITIONING = ConfigOptions
      .key(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key())
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to use Hive style partitioning.\n"
          + "If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.\n"
          + "By default false (the names of partition folders are only partition values)");

  @AdvancedConfig
  public static final ConfigOption<String> KEYGEN_CLASS_NAME = ConfigOptions
      .key(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Key generator class, that implements will extract the key out of incoming record");

  @AdvancedConfig
  public static final ConfigOption<String> KEYGEN_TYPE = ConfigOptions
      .key(HoodieWriteConfig.KEYGENERATOR_TYPE.key())
      .stringType()
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .withDescription("Key generator type, that implements will extract the key out of incoming record. "
          + "**Note** This is being actively worked on. Please use "
          + "`hoodie.datasource.write.keygenerator.class` instead.");

  public static final String PARTITION_FORMAT_HOUR = "yyyyMMddHH";
  public static final String PARTITION_FORMAT_DAY = "yyyyMMdd";
  public static final String PARTITION_FORMAT_DASHED_DAY = "yyyy-MM-dd";

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_UTC_TIMEZONE = ConfigOptions
        .key("write.utc-timezone")
        .booleanType()
        .defaultValue(true)
        .withDescription("Use UTC timezone or local timezone to the conversion between epoch"
            + " time and LocalDateTime. Default value is utc timezone for forward compatibility.");

  @AdvancedConfig
  public static final ConfigOption<String> PARTITION_FORMAT = ConfigOptions
      .key("write.partition.format")
      .stringType()
      .noDefaultValue()
      .withDescription("Partition path format, only valid when 'write.datetime.partitioning' is true, default is:\n"
          + "1) 'yyyyMMddHH' for timestamp(3) WITHOUT TIME ZONE, LONG, FLOAT, DOUBLE, DECIMAL;\n"
          + "2) 'yyyyMMdd' for DATE and INT.");

  @AdvancedConfig
  public static final ConfigOption<Integer> INDEX_BOOTSTRAP_TASKS = ConfigOptions
      .key("write.index_bootstrap.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do index bootstrap, default same as the write task parallelism");

  @AdvancedConfig
  public static final ConfigOption<Integer> BUCKET_ASSIGN_TASKS = ConfigOptions
      .key("write.bucket_assign.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do bucket assign, default same as the write task parallelism");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_TASKS = ConfigOptions
      .key("write.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do actual write, default is the parallelism of the execution environment");

  @AdvancedConfig
  public static final ConfigOption<Double> WRITE_TASK_MAX_SIZE = ConfigOptions
      .key("write.task.max.size")
      .doubleType()
      .defaultValue(1024D) // 1GB
      .withDescription("Maximum memory in MB for a write task, when the threshold hits,\n"
          + "it flushes the max size data bucket to avoid OOM, default 1GB");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_BUFFER_SORT_ENABLED = ConfigOptions
      .key("write.buffer.sort.enabled")
      .booleanType()
      .defaultValue(false) // default no sort
      .withDescription("Whether to enable buffer sort within append write function. Data is sorted within the buffer configured by number of records or buffer size."
          + " The order of entire parquet file is not guaranteed.");

  @AdvancedConfig
  public static final ConfigOption<String> WRITE_BUFFER_SORT_KEYS = ConfigOptions
      .key("write.buffer.sort.keys")
      .stringType()
      .noDefaultValue() // default no sort key
      .withDescription("Sort keys concatenated by comma for buffer sort in append write function. Data is sorted within the buffer configured by number of records or buffer size."
          + " The order of entire parquet file is not guaranteed.");

  @AdvancedConfig
  public static final ConfigOption<Long> WRITE_BUFFER_SIZE = ConfigOptions
      .key("write.buffer.size")
      .longType()
      .defaultValue(1000L) // 1000 records
      .withDescription("Buffer size of each partition key for buffer sort in append write function. Data is sorted within the buffer configured by number of records."
          +  " The order of entire parquet file is not guaranteed.");

  @AdvancedConfig
  public static final ConfigOption<Long> WRITE_RATE_LIMIT = ConfigOptions
      .key("write.rate.limit")
      .longType()
      .defaultValue(0L) // default no limit
      .withDescription("Write record rate limit per second to prevent traffic jitter and improve stability, default 0 (no limit)");

  @AdvancedConfig
  public static final ConfigOption<Double> WRITE_BATCH_SIZE = ConfigOptions
      .key("write.batch.size")
      .doubleType()
      .defaultValue(256D) // 256MB
      .withDescription("Batch buffer size in MB to flush data into the underneath filesystem, default 256MB");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_LOG_BLOCK_SIZE = ConfigOptions
      .key("write.log_block.size")
      .intType()
      .defaultValue(128)
      .withDescription("Max log block size in MB for log file, default 128MB");

  @AdvancedConfig
  public static final ConfigOption<Long> WRITE_LOG_MAX_SIZE = ConfigOptions
      .key("write.log.max.size")
      .longType()
      .defaultValue(1024L)
      .withDescription("Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_PARQUET_BLOCK_SIZE = ConfigOptions
      .key("write.parquet.block.size")
      .intType()
      .defaultValue(120)
      .withDescription("Parquet RowGroup size. It's recommended to make this large enough that scan costs can be"
          + " amortized by packing enough column values into a single row group.");

  public static final ConfigOption<Integer> WRITE_PARQUET_MAX_FILE_SIZE = ConfigOptions
      .key("write.parquet.max.file.size")
      .intType()
      .defaultValue(120)
      .withDescription("Target size for parquet files produced by Hudi write phases. "
          + "For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_PARQUET_PAGE_SIZE = ConfigOptions
      .key("write.parquet.page.size")
      .intType()
      .defaultValue(1)
      .withDescription("Parquet page size. Page is the unit of read within a parquet file. "
          + "Within a block, pages are compressed separately.");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_MERGE_MAX_MEMORY = ConfigOptions
      .key("write.merge.max_memory")
      .intType()
      .defaultValue(100) // default 100 MB
      .withDescription("Max memory in MB for merge, default 100MB");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_MEMORY_SEGMENT_PAGE_SIZE = ConfigOptions
      .key("write.memory.segment.page.size")
      .intType()
      .defaultValue(32 * 1024) // default 32 KB
      .withDescription("Page size for memory segment used for write buffer.");

  // this is only for internal use
  @AdvancedConfig
  public static final ConfigOption<Long> WRITE_COMMIT_ACK_TIMEOUT = ConfigOptions
      .key("write.commit.ack.timeout")
      .longType()
      .defaultValue(300_000L)
      .withDescription("Timeout limit for a writer task after it finishes a checkpoint and\n"
          + "waits for the instant commit success, only for internal use.");

  // this is only for internal use
  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_INCREMENTAL_JOB_GRAPH_GENERATION = ConfigOptions
      .key("write.incremental.job.graph.generation")
      .booleanType()
      .defaultValue(false)
      .withDescription("Flag saying whether the incremental job graph generation is enabled.");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_BULK_INSERT_SHUFFLE_INPUT = ConfigOptions
      .key("write.bulk_insert.shuffle_input")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to shuffle the inputs by specific fields for bulk insert tasks, default true");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_BULK_INSERT_SORT_INPUT = ConfigOptions
      .key("write.bulk_insert.sort_input")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to sort the inputs by specific fields for bulk insert tasks, default true");

  @AdvancedConfig
  public static final ConfigOption<Boolean> WRITE_BULK_INSERT_SORT_INPUT_BY_RECORD_KEY = ConfigOptions
      .key("write.bulk_insert.sort_input.by_record_key")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to sort the inputs by record keys for bulk insert tasks, default false");

  @AdvancedConfig
  public static final ConfigOption<Integer> WRITE_SORT_MEMORY = ConfigOptions
      .key("write.sort.memory")
      .intType()
      .defaultValue(128)
      .withDescription("Sort memory in MB, default 128MB");

  @AdvancedConfig
  public static final ConfigOption<String> WRITE_PARTITION_OVERWRITE_MODE = ConfigOptions
      .key("write.partition.overwrite.mode")
      .stringType()
      .defaultValue(PartitionOverwriteMode.STATIC.name())
      .withDescription("When INSERT OVERWRITE a partitioned data source table, we currently support 2 modes: static and dynamic. "
          + "Static mode deletes all the partitions that match the partition specification(e.g. PARTITION(a=1,b)) in the INSERT statement, before overwriting. "
          + "Dynamic mode doesn't delete partitions ahead, and only overwrite those partitions that have data written into it at runtime. "
          + "By default we use static mode to keep the same behavior of previous version.");

  // this is only for internal use
  @AdvancedConfig
  public static final ConfigOption<String> WRITE_CLIENT_ID = ConfigOptions
      .key("write.client.id")
      .stringType()
      .defaultValue(ClientIds.INIT_CLIENT_ID)
      .withDescription("Unique identifier used to distinguish different writer pipelines for concurrent mode");

  // ------------------------------------------------------------------------
  //  Compaction Options
  // ------------------------------------------------------------------------

  @AdvancedConfig
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

  @AdvancedConfig
  public static final ConfigOption<Integer> COMPACTION_TASKS = ConfigOptions
      .key("compaction.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do actual compaction, default same as the write task parallelism");

  public static final String NUM_COMMITS = "num_commits";
  public static final String TIME_ELAPSED = "time_elapsed";
  public static final String NUM_AND_TIME = "num_and_time";
  public static final String NUM_OR_TIME = "num_or_time";
  @AdvancedConfig
  public static final ConfigOption<String> COMPACTION_TRIGGER_STRATEGY = ConfigOptions
      .key("compaction.trigger.strategy")
      .stringType()
      .defaultValue(NUM_COMMITS) // default true for MOR write
      .withDescription("Strategy to trigger compaction, options are "
          + "'num_commits': trigger compaction when there are at least N delta commits after last completed compaction;\n"
          + "'num_commits_after_last_request': trigger compaction when there are at least N delta commits after last completed/requested compaction;\n"
          + "'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction;\n"
          + "'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;\n"
          + "'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.\n"
          + "Default is 'num_commits'");

  public static final ConfigOption<Integer> COMPACTION_DELTA_COMMITS = ConfigOptions
      .key("compaction.delta_commits")
      .intType()
      .defaultValue(5)
      .withDescription("Max delta commits needed to trigger compaction, default 5 commits");

  @AdvancedConfig
  public static final ConfigOption<Integer> COMPACTION_DELTA_SECONDS = ConfigOptions
      .key("compaction.delta_seconds")
      .intType()
      .defaultValue(3600) // default 1 hour
      .withDescription("Max delta seconds time needed to trigger compaction, default 1 hour");

  @AdvancedConfig
  public static final ConfigOption<Integer> COMPACTION_TIMEOUT_SECONDS = ConfigOptions
      .key("compaction.timeout.seconds")
      .intType()
      .defaultValue(1200) // default 20 minutes
      .withDescription("Max timeout time in seconds for online compaction to rollback, default 20 minutes");

  @AdvancedConfig
  public static final ConfigOption<Integer> COMPACTION_MAX_MEMORY = ConfigOptions
      .key("compaction.max_memory")
      .intType()
      .defaultValue(100) // default 100 MB
      .withDescription("Max memory in MB for compaction spillable map, default 100MB");

  @AdvancedConfig
  public static final ConfigOption<Long> COMPACTION_TARGET_IO = ConfigOptions
      .key("compaction.target_io")
      .longType()
      .defaultValue(500 * 1024L) // default 500 GB
      .withDescription("Target IO in MB for per compaction (both read and write), default 500 GB");

  public static final ConfigOption<Boolean> CLEAN_ASYNC_ENABLED = ConfigOptions
      .key("clean.async.enabled")
      .booleanType()
      .defaultValue(true)
      .withFallbackKeys("hoodie.clean.async.enabled")
      .withDescription("Whether to cleanup the old commits immediately on new commits, enabled by default");

  @AdvancedConfig
  public static final ConfigOption<String> CLEAN_POLICY = ConfigOptions
      .key("clean.policy")
      .stringType()
      .defaultValue(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
      .withFallbackKeys("hoodie.clean.policy")
      .withDescription("Clean policy to manage the Hudi table. Available option: KEEP_LATEST_COMMITS, KEEP_LATEST_FILE_VERSIONS, KEEP_LATEST_BY_HOURS."
          + "Default is KEEP_LATEST_COMMITS.");

  public static final ConfigOption<Integer> CLEAN_RETAIN_COMMITS = ConfigOptions
      .key("clean.retain_commits")
      .intType()
      .defaultValue(30)// default 30 commits
      .withFallbackKeys("hoodie.clean.commits.retained")
      .withDescription("Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).\n"
          + "This also directly translates into how much you can incrementally pull on this table, default 30");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLEAN_RETAIN_HOURS = ConfigOptions
      .key("clean.retain_hours")
      .intType()
      .defaultValue(24)// default 24 hours
      .withFallbackKeys("hoodie.clean.hours.retained")
      .withDescription("Number of hours for which commits need to be retained. This config provides a more flexible option as"
          + "compared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group,"
          + " corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLEAN_RETAIN_FILE_VERSIONS = ConfigOptions
      .key("clean.retain_file_versions")
      .intType()
      .defaultValue(5)// default 5 version
      .withFallbackKeys("hoodie.clean.fileversions.retained")
      .withDescription("Number of file versions to retain. default 5");

  public static final ConfigOption<Integer> ARCHIVE_MAX_COMMITS = ConfigOptions
      .key("archive.max_commits")
      .intType()
      .defaultValue(50)// default max 50 commits
      .withDescription("Max number of commits to keep before archiving older commits into a sequential log, default 50");

  public static final ConfigOption<Integer> ARCHIVE_MIN_COMMITS = ConfigOptions
      .key("archive.min_commits")
      .intType()
      .defaultValue(40)// default min 40 commits
      .withDescription("Min number of commits to keep before archiving older commits into a sequential log, default 40");

  // ------------------------------------------------------------------------
  //  Clustering Options
  // ------------------------------------------------------------------------

  @AdvancedConfig
  public static final ConfigOption<Boolean> CLUSTERING_SCHEDULE_ENABLED = ConfigOptions
      .key("clustering.schedule.enabled")
      .booleanType()
      .defaultValue(false) // default false for pipeline
      .withDescription("Schedule the cluster plan, default false");

  public static final ConfigOption<Boolean> CLUSTERING_ASYNC_ENABLED = ConfigOptions
      .key("clustering.async.enabled")
      .booleanType()
      .defaultValue(false) // default false for pipeline
      .withDescription("Async Clustering, default false");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLUSTERING_DELTA_COMMITS = ConfigOptions
      .key("clustering.delta_commits")
      .intType()
      .defaultValue(4)
      .withDescription("Max delta commits needed to trigger clustering, default 4 commits");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLUSTERING_TASKS = ConfigOptions
      .key("clustering.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do actual clustering, default same as the write task parallelism");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLUSTERING_TARGET_PARTITIONS = ConfigOptions
      .key("clustering.plan.strategy.daybased.lookback.partitions")
      .intType()
      .defaultValue(2)
      .withDescription("Number of partitions to list to create ClusteringPlan, default is 2");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST = ConfigOptions
      .key("clustering.plan.strategy.daybased.skipfromlatest.partitions")
      .intType()
      .defaultValue(0)
      .withDescription("Number of partitions to skip from latest when choosing partitions to create ClusteringPlan");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION = ConfigOptions
      .key("clustering.plan.strategy.cluster.begin.partition")
      .stringType()
      .defaultValue("")
      .withDescription("Begin partition used to filter partition (inclusive)");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION = ConfigOptions
      .key("clustering.plan.strategy.cluster.end.partition")
      .stringType()
      .defaultValue("")
      .withDescription("End partition used to filter partition (inclusive)");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN = ConfigOptions
      .key("clustering.plan.strategy.partition.regex.pattern")
      .stringType()
      .defaultValue("")
      .withDescription("Filter clustering partitions that matched regex pattern");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_PARTITION_SELECTED = ConfigOptions
      .key("clustering.plan.strategy.partition.selected")
      .stringType()
      .defaultValue("")
      .withDescription("Partitions to run clustering");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_CLASS = ConfigOptions
      .key("clustering.plan.strategy.class")
      .stringType()
      .defaultValue(FlinkSizeBasedClusteringPlanStrategy.class.getName())
      .withDescription("Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan "
          + "i.e select what file groups are being clustered. Default strategy, looks at the last N (determined by "
          + CLUSTERING_TARGET_PARTITIONS.key() + ") day based partitions picks the small file slices within those partitions.");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME = ConfigOptions
      .key("clustering.plan.partition.filter.mode")
      .stringType()
      .defaultValue(ClusteringPlanPartitionFilterMode.NONE.name())
      .withDescription("Partition filter mode used in the creation of clustering plan. Available values are - "
          + "NONE: do not filter table partition and thus the clustering plan will include all partitions that have clustering candidate."
          + "RECENT_DAYS: keep a continuous range of partitions, worked together with configs '" + CLUSTERING_TARGET_PARTITIONS.key() + "' and '"
          + CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST.key() + "."
          + "SELECTED_PARTITIONS: keep partitions that are in the specified range ['" + CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION.key() + "', '"
          + CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION.key() + "']."
          + "DAY_ROLLING: clustering partitions on a rolling basis by the hour to avoid clustering all partitions each time, "
          + "which strategy sorts the partitions asc and chooses the partition of which index is divided by 24 and the remainder is equal to the current hour.");

  public static final ConfigOption<Long> CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES = ConfigOptions
      .key("clustering.plan.strategy.target.file.max.bytes")
      .longType()
      .defaultValue(1024 * 1024 * 1024L) // default 1 GB
      .withDescription("Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB");

  public static final ConfigOption<Long> CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT = ConfigOptions
      .key("clustering.plan.strategy.small.file.limit")
      .longType()
      .defaultValue(600L) // default 600 MB
      .withDescription("Files smaller than the size specified here are candidates for clustering, default 600 MB");

  @AdvancedConfig
  public static final ConfigOption<String> CLUSTERING_SORT_COLUMNS = ConfigOptions
      .key("clustering.plan.strategy.sort.columns")
      .stringType()
      .defaultValue("")
      .withDescription("Columns to sort the data by when clustering");

  @AdvancedConfig
  public static final ConfigOption<Integer> CLUSTERING_MAX_NUM_GROUPS = ConfigOptions
      .key("clustering.plan.strategy.max.num.groups")
      .intType()
      .defaultValue(30)
      .withDescription("Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism, default is 30");

  // ------------------------------------------------------------------------
  //  Hive Sync Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<Boolean> HIVE_SYNC_ENABLED = ConfigOptions
      .key("hive_sync.enabled")
      .booleanType()
      .defaultValue(false)
      .withFallbackKeys("hive_sync.enable")
      .withDescription("Asynchronously sync Hive meta to HMS, default false");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_DB = ConfigOptions
      .key("hive_sync.db")
      .stringType()
      .defaultValue("default")
      .withDescription("Database name for hive sync, default 'default'");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_TABLE = ConfigOptions
      .key("hive_sync.table")
      .stringType()
      .defaultValue("unknown")
      .withDescription("Table name for hive sync, default 'unknown'");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_FILE_FORMAT = ConfigOptions
      .key("hive_sync.file_format")
      .stringType()
      .defaultValue("PARQUET")
      .withDescription("File format for hive sync, default 'PARQUET'");

  public static final ConfigOption<String> HIVE_SYNC_MODE = ConfigOptions
      .key("hive_sync.mode")
      .stringType()
      .defaultValue(HiveSyncMode.HMS.name())
      .withDescription("Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'hms'");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_USERNAME = ConfigOptions
      .key("hive_sync.username")
      .stringType()
      .defaultValue("hive")
      .withDescription("Username for hive sync, default 'hive'");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_PASSWORD = ConfigOptions
      .key("hive_sync.password")
      .stringType()
      .defaultValue("hive")
      .withDescription("Password for hive sync, default 'hive'");

  public static final ConfigOption<String> HIVE_SYNC_JDBC_URL = ConfigOptions
      .key("hive_sync.jdbc_url")
      .stringType()
      .defaultValue("jdbc:hive2://localhost:10000")
      .withDescription("Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'");

  public static final ConfigOption<String> HIVE_SYNC_METASTORE_URIS = ConfigOptions
      .key("hive_sync.metastore.uris")
      .stringType()
      .defaultValue("")
      .withDescription("Metastore uris for hive sync, default ''");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_PARTITION_FIELDS = ConfigOptions
      .key("hive_sync.partition_fields")
      .stringType()
      .defaultValue("")
      .withDescription("Partition fields for hive sync, default ''");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME = ConfigOptions
      .key("hive_sync.partition_extractor_class")
      .stringType()
      .defaultValue(MultiPartKeysValueExtractor.class.getName())
      .withDescription("Tool to extract the partition value from HDFS path, "
          + "default 'MultiPartKeysValueExtractor'");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_ASSUME_DATE_PARTITION = ConfigOptions
      .key("hive_sync.assume_date_partitioning")
      .booleanType()
      .defaultValue(false)
      .withDescription("Assume partitioning is yyyy/mm/dd, default false");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_USE_JDBC = ConfigOptions
      .key("hive_sync.use_jdbc")
      .booleanType()
      .defaultValue(true)
      .withDescription("Use JDBC when hive synchronization is enabled, default true");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_AUTO_CREATE_DB = ConfigOptions
      .key("hive_sync.auto_create_db")
      .booleanType()
      .defaultValue(true)
      .withDescription("Auto create hive database if it does not exists, default true");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_IGNORE_EXCEPTIONS = ConfigOptions
      .key("hive_sync.ignore_exceptions")
      .booleanType()
      .defaultValue(false)
      .withDescription("Ignore exceptions during hive synchronization, default false");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_SKIP_RO_SUFFIX = ConfigOptions
      .key("hive_sync.skip_ro_suffix")
      .booleanType()
      .defaultValue(false)
      .withDescription("Skip the _ro suffix for Read optimized table when registering, default false");

  @AdvancedConfig
  public static final ConfigOption<Boolean> HIVE_SYNC_SUPPORT_TIMESTAMP = ConfigOptions
      .key("hive_sync.support_timestamp")
      .booleanType()
      .defaultValue(true)
      .withDescription("INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type.\n"
          + "Disabled by default for backward compatibility.");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_TABLE_PROPERTIES = ConfigOptions
      .key("hive_sync.table_properties")
      .stringType()
      .noDefaultValue()
      .withDescription("Additional properties to store with table, the data format is k1=v1\nk2=v2");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_TABLE_SERDE_PROPERTIES = ConfigOptions
      .key("hive_sync.serde_properties")
      .stringType()
      .noDefaultValue()
      .withDescription("Serde properties to hive table, the data format is k1=v1\nk2=v2");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_CONF_DIR = ConfigOptions
      .key("hive_sync.conf.dir")
      .stringType()
      .noDefaultValue()
      .withDescription("The hive configuration directory, where the hive-site.xml lies in, the file should be put on the client machine");

  @AdvancedConfig
  public static final ConfigOption<String> HIVE_SYNC_TABLE_STRATEGY = ConfigOptions
      .key("hive_sync.table.strategy")
      .stringType()
      .defaultValue(HoodieSyncTableStrategy.ALL.name())
      .withDescription("Hive table synchronization strategy. Available option: RO, RT, ALL.");

  public static final ConfigOption<Duration> LOOKUP_JOIN_CACHE_TTL =
      key("lookup.join.cache.ttl")
          .durationType()
          .defaultValue(Duration.ofMinutes(60))
          .withDescription(
              "The cache TTL (e.g. 10min) for the build table in lookup join.");


  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  // Prefix for Hoodie specific properties.
  private static final String PROPERTIES_PREFIX = "properties.";

  /**
   * Collects the config options that start with specified prefix {@code prefix} into a 'key'='value' list.
   */
  public static Map<String, String> getPropertiesWithPrefix(Map<String, String> options, String prefix) {
    final Map<String, String> hoodieProperties = new HashMap<>();
    if (hasPropertyOptions(options, prefix)) {
      options.keySet().stream()
          .filter(key -> key.startsWith(prefix))
          .forEach(key -> {
            final String value = options.get(key);
            final String subKey = key.substring(prefix.length());
            hoodieProperties.put(subKey, value);
          });
    }
    return hoodieProperties;
  }

  /**
   * Collects all the config options, the 'properties.' prefix would be removed if the option key starts with it.
   */
  public static Configuration flatOptions(Configuration conf) {
    final Map<String, String> propsMap = new HashMap<>();

    conf.toMap().forEach((key, value) -> {
      final String subKey = key.startsWith(PROPERTIES_PREFIX)
          ? key.substring((PROPERTIES_PREFIX).length())
          : key;
      propsMap.put(subKey, value);
    });
    return fromMap(propsMap);
  }

  private static boolean hasPropertyOptions(Map<String, String> options, String prefix) {
    return options.keySet().stream().anyMatch(k -> k.startsWith(prefix));
  }

  /**
   * Creates a new configuration that is initialized with the options of the given map.
   */
  public static Configuration fromMap(Map<String, String> map) {
    final Configuration configuration = new Configuration();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      configuration.setString(entry.getKey().trim(), entry.getValue());
    }
    return configuration;
  }

  /**
   * Returns whether the given conf defines default value for the option {@code option}.
   */
  public static <T> boolean isDefaultValueDefined(Configuration conf, ConfigOption<T> option) {
    return !conf.getOptional(option).isPresent()
        || conf.get(option).equals(option.defaultValue());
  }

  /**
   * Returns all the optional config options.
   */
  public static Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>(allOptions());
    options.remove(PATH);
    return options;
  }

  /**
   * Returns all the config options.
   */
  public static List<ConfigOption<?>> allOptions() {
    return OptionsResolver.allOptions(FlinkOptions.class);
  }
}
