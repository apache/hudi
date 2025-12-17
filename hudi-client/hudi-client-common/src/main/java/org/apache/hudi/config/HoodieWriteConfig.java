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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieIndexingConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.queue.DisruptorWaitStrategyType;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.config.metrics.HoodieMetricsM3Config;
import org.apache.hudi.estimator.AverageRecordSizeEstimator;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle;
import org.apache.hudi.io.HoodieConcatHandle;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.RandomFileIdPrefixProvider;
import org.apache.hudi.table.action.clean.CleaningTriggerStrategy;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.CompositeCompactionStrategy;
import org.apache.hudi.table.storage.HoodieStorageLayout;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.orc.CompressionKind;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMemoryConfig.DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_POLICY;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD;
import static org.apache.hudi.table.marker.ConflictDetectionUtils.getDefaultEarlyConflictDetectionStrategy;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Getter
@Immutable
@Slf4j
@ConfigClassProperty(name = "Write Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control write behavior on Hudi tables. These can be directly passed down from even "
        + "higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g Hudi Streamer).")
public class HoodieWriteConfig extends HoodieConfig {
  private static final long serialVersionUID = 0L;

  // This is a constant as is should never be changed via config (will invalidate previous commits)
  // It is here so that both the client and Hudi Streamer use the same reference
  public static final String STREAMER_CHECKPOINT_KEY = STREAMER_CHECKPOINT_KEY_V1;
  @Deprecated
  public static final String DELTASTREAMER_CHECKPOINT_KEY = STREAMER_CHECKPOINT_KEY;

  public static final String CONCURRENCY_PREFIX = "hoodie.write.concurrency.";

  public static final ConfigProperty<String> TBL_NAME = ConfigProperty
      .key(HoodieTableConfig.HOODIE_TABLE_NAME_KEY)
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with metastores like HMS. Needs to be same across runs.");

  public static final ConfigProperty<Integer> WRITE_TABLE_VERSION = ConfigProperty
      .key("hoodie.write.table.version")
      .defaultValue(HoodieTableVersion.current().versionCode())
      .withValidValues(
          String.valueOf(HoodieTableVersion.SIX.versionCode()),
          String.valueOf(HoodieTableVersion.EIGHT.versionCode()),
          String.valueOf(HoodieTableVersion.NINE.versionCode())
      )
      .sinceVersion("1.0.0")
      .withDocumentation("The table version this writer is storing the table in. This should match the current table version.");

  public static final ConfigProperty<Boolean> AUTO_UPGRADE_VERSION = ConfigProperty
      .key("hoodie.write.auto.upgrade")
      .defaultValue(true)
      .sinceVersion("1.0.0")
      .withDocumentation("If enabled, writers automatically migrate the table to the specified write table version "
          + "if the current table version is lower.");

  public static final ConfigProperty<String> TAGGED_RECORD_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.write.tagged.record.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .markAdvanced()
      .withDocumentation("Determine what level of persistence is used to cache write RDDs. "
          + "Refer to org.apache.spark.storage.StorageLevel for different values");

  @Deprecated
  public static final ConfigProperty<String> PRECOMBINE_FIELD_NAME = ConfigProperty
      .key("hoodie.datasource.write.precombine.field")
      .noDefaultValue()
      .withDocumentation("Comma separated list of fields used in preCombining before actual write. When two records have the same key value, "
          + "we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..). "
          + "For multiple fields if first key comparison is same, second key comparison is made and so on. This config is used for combining records "
          + "within the same batch and also for merging using event time merge mode");

  public static final ConfigProperty<String> WRITE_PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.datasource.write.payload.class")
      .noDefaultValue()
      .markAdvanced()
      .deprecatedAfter("1.0.0")
      .withDocumentation("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. "
          + "This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective");

  // This ConfigProperty is also used in SQL options which expect String type
  public static final ConfigProperty<String> RECORD_MERGE_MODE = ConfigProperty
      .key("hoodie.write.record.merge.mode")
      .noDefaultValue("COMMIT_TIME_ORDERING if ordering field is not set; EVENT_TIME_ORDERING if ordering field is set")
      .sinceVersion("1.0.0")
      .withDocumentation(RecordMergeMode.class);

  public static final ConfigProperty<String> RECORD_MERGE_STRATEGY_ID = ConfigProperty
      .key("hoodie.write.record.merge.strategy.id")
      .noDefaultValue()
      .markAdvanced()
      .withAlternatives("hoodie.datasource.write.record.merger.strategy")
      .sinceVersion("0.13.0")
      .withDocumentation("ID of record merge strategy. Hudi will pick HoodieRecordMerger implementations in `"
          + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "` which has the same merge strategy id");

  public static final ConfigProperty<String> RECORD_MERGE_IMPL_CLASSES = ConfigProperty
      .key(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY)
      .noDefaultValue()
      .markAdvanced()
      .withAlternatives(RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY)
      .sinceVersion("0.13.0")
      .withDocumentation("List of HoodieMerger implementations constituting Hudi's merging strategy -- based on the engine used. "
          + "These record merge impls will filter by " + RECORD_MERGE_STRATEGY_ID.key()
          + "Hudi will pick most efficient implementation to perform merging/combining of the records (during update, reading MOR table, etc)");

  public static final ConfigProperty<String> KEYGENERATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.class")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` "
          + "extract a key out of incoming records.");

  public static final ConfigProperty<String> RECORD_SIZE_ESTIMATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.record.size.estimator.class")
      .defaultValue(AverageRecordSizeEstimator.class.getName())
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Class that estimates the size of records written by implementing "
          + "`org.apache.hudi.estimator.RecordSizeEstimator`. Default implementation is `org.apache.hudi.estimator.AverageRecordSizeEstimator`");

  public static final ConfigProperty<Integer> RECORD_SIZE_ESTIMATOR_MAX_COMMITS = ConfigProperty
      .key("_hoodie.record.size.estimator.max.commits")
      .defaultValue(5)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("The maximum number of commits that will be read to estimate the avg record size. "
          + "This makes sure we parse a limited number of commit metadata, as parsing the entire active timeline can be expensive and unnecessary.");

  public static final ConfigProperty<String> RECORD_SIZE_ESTIMATOR_AVERAGE_METADATA_SIZE = ConfigProperty
      .key("hoodie.record.size.estimator.average.metadata.size")
      .defaultValue("0")
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("The approximate metadata size in bytes to subtract from the file size when estimating the record size.");

  public static final ConfigProperty<String> WRITE_EXECUTOR_TYPE = ConfigProperty
      .key("hoodie.write.executor.type")
      .defaultValue(ExecutorType.SIMPLE.name())
      .withValidValues(Arrays.stream(ExecutorType.values()).map(Enum::name).toArray(String[]::new))
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation(ExecutorType.class);

  public static final ConfigProperty<String> KEYGENERATOR_TYPE = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.type")
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .markAdvanced()
      .withDocumentation(KeyGeneratorType.class,
          "**Note** This is being actively worked on. Please use "
              + "`hoodie.datasource.write.keygenerator.class` instead.");

  public static final ConfigProperty<Boolean> COMPLEX_KEYGEN_NEW_ENCODING = ConfigProperty
      .key("hoodie.write.complex.keygen.new.encoding")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .supportedVersions("0.14.2", "0.15.1", "1.0.3")
      .withDocumentation("This config only takes effect for writing table version 8 and below. "
          + "If set to false, the record key field name is encoded and prepended "
          + "in the case where a single record key field is used in the complex key generator, "
          + "i.e., record keys stored in _hoodie_record_key meta field is in the format of "
          + "`<field_name>:<field_value>`, which conforms to the behavior "
          + "in 0.14.0 release and older. If set to true, the record key field name is not "
          + "encoded under the same case in the complex key generator, i.e., record keys stored "
          + "in _hoodie_record_key meta field is in the format of `<field_value>`, "
          + "which conforms to the behavior in 0.14.1, 0.15.0, 1.0.0, 1.0.1, 1.0.2 releases.");

  public static final ConfigProperty<Boolean> ENABLE_COMPLEX_KEYGEN_VALIDATION = ConfigProperty
      .key("hoodie.write.complex.keygen.validation.enable")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .supportedVersions("0.14.2", "0.15.1", "1.0.3")
      .withDocumentation("This config only takes effect for writing table version 8 and below, "
          + "upgrade or downgrade. If set to true, the writer enables the validation on whether the "
          + "table uses the complex key generator with a single record key field, which can be affected "
          + "by a breaking change in 0.14.1, 0.15.0, 1.0.0, 1.0.1, 1.0.2 releases, causing key "
          + "encoding change and potential duplicates in the table. The validation fails the "
          + "pipeline if the table meets the condition for the user to take proper action. "
          + "The user can turn this validation off by setting the config to false, after "
          + "evaluating the table and situation and doing table repair if needed.");

  public static final ConfigProperty<String> ROLLBACK_USING_MARKERS_ENABLE = ConfigProperty
      .key("hoodie.rollback.using.markers")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Enables a more efficient mechanism for rollbacks based on the marker files generated "
          + "during the writes. Turned on by default.");

  public static final ConfigProperty<String> FAIL_JOB_ON_DUPLICATE_DATA_FILE_DETECTION = ConfigProperty
      .key("hoodie.fail.job.on.duplicate.data.file.detection")
      .defaultValue("false")
      .withDocumentation("If config is enabled, entire job is failed on invalid file detection");

  public static final ConfigProperty<String> TIMELINE_LAYOUT_VERSION_NUM = ConfigProperty
      .key("hoodie.timeline.layout.version")
      .defaultValue(Integer.toString(TimelineLayoutVersion.CURR_VERSION))
      .withValidValues(Integer.toString(TimelineLayoutVersion.VERSION_0),
          Integer.toString(TimelineLayoutVersion.VERSION_1),
          Integer.toString(TimelineLayoutVersion.VERSION_2))
      .markAdvanced()
      .sinceVersion("0.5.1")
      .withDocumentation("Controls the layout of the timeline. Version 0 relied on renames, Version 1 (default) models "
          + "the timeline as an immutable log relying only on atomic writes for object storage.");

  public static final ConfigProperty<HoodieFileFormat> BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withValidValues(HoodieFileFormat.PARQUET.name(), HoodieFileFormat.ORC.name(), HoodieFileFormat.HFILE.name())
      .withAlternatives("hoodie.table.ro.file.format")
      .markAdvanced()
      .withDocumentation(HoodieFileFormat.class, "File format to store all the base file data.");

  public static final ConfigProperty<String> BASE_PATH = HoodieCommonConfig.BASE_PATH;

  public static final ConfigProperty<String> AVRO_SCHEMA_STRING = ConfigProperty
      .key("hoodie.avro.schema")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Schema string representing the current write schema of the table. Hudi passes this to "
          + "implementations of HoodieRecordPayload to convert incoming records to avro. This is also used as the write schema "
          + "evolving records during an update.");

  public static final ConfigProperty<String> INTERNAL_SCHEMA_STRING = ConfigProperty
      .key("hoodie.internal.schema")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Schema string representing the latest schema of the table. Hudi passes this to "
          + "implementations of evolution of schema");

  public static final ConfigProperty<Boolean> ENABLE_SCHEMA_CONFLICT_RESOLUTION = ConfigProperty
      .key(CONCURRENCY_PREFIX + "schema.conflict.resolution.enable")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("If turned on, we detect and abort incompatible concurrent schema evolution.");

  public static final ConfigProperty<String> AVRO_SCHEMA_VALIDATE_ENABLE = ConfigProperty
      .key("hoodie.avro.schema.validate")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("Validate the schema used for the write against the latest schema, for backwards compatibility.");

  public static final ConfigProperty<String> SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP = ConfigProperty
      .key("hoodie.datasource.write.schema.allow.auto.evolution.column.drop")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Controls whether table's schema is allowed to automatically evolve when "
          + "incoming batch's schema can have any of the columns dropped. By default, Hudi will not "
          + "allow this kind of (auto) schema evolution. Set this config to true to allow table's "
          + "schema to be updated automatically when columns are dropped from the new incoming batch.");

  public static final ConfigProperty<String> INSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.insert.shuffle.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Parallelism for inserting records into the table. Inserts can shuffle "
          + "data before writing to tune file sizes and optimize the storage layout. Before "
          + "0.13.0 release, if users do not configure it, Hudi would use 200 as the default "
          + "shuffle parallelism. From 0.13.0 onwards Hudi by default automatically uses the "
          + "parallelism deduced by Spark based on the source data. If the shuffle parallelism "
          + "is explicitly configured by the user, the user-configured parallelism is "
          + "used in defining the actual parallelism. If you observe small files from the insert "
          + "operation, we suggest configuring this shuffle parallelism explicitly, so that the "
          + "parallelism is around total_input_data_size/120MB.");

  public static final ConfigProperty<String> BULKINSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.bulkinsert.shuffle.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("For large initial imports using bulk_insert operation, controls the "
          + "parallelism to use for sort modes or custom partitioning done before writing records "
          + "to the table. Before 0.13.0 release, if users do not configure it, Hudi would use "
          + "200 as the default shuffle parallelism. From 0.13.0 onwards Hudi by default "
          + "automatically uses the parallelism deduced by Spark based on the source data or "
          + "the parallelism based on the logical plan for row writer. If the shuffle parallelism "
          + "is explicitly configured by the user, the user-configured parallelism is "
          + "used in defining the actual parallelism. If you observe small files from the bulk insert "
          + "operation, we suggest configuring this shuffle parallelism explicitly, so that the "
          + "parallelism is around total_input_data_size/120MB.");

  public static final ConfigProperty<String> BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS = ConfigProperty
      .key("hoodie.bulkinsert.user.defined.partitioner.sort.columns")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Columns to sort the data by when use org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner as user defined partitioner during bulk_insert. "
          + "For example 'column1,column2'");

  public static final ConfigProperty<Boolean> BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS = ConfigProperty
      .key("hoodie.bulkinsert.sort.suffix.record_key")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation(
          "When using user defined sort columns there can be possibility of skew because spark's RangePartitioner used in sort can reduce the number of outputSparkPartitions"
              + "if the sampled dataset has a low cardinality on the provided sort columns. This can cause an increase in commit durations as we are not leveraging the original parallelism."
              + "Enabling this config suffixes the record key at the end to avoid skew."
              + "This config is used by RowCustomColumnsSortPartitioner, RDDCustomColumnsSortPartitioner and JavaCustomColumnsSortPartitioner");

  public static final ConfigProperty<String> BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME = ConfigProperty
      .key("hoodie.bulkinsert.user.defined.partitioner.class")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("If specified, this class will be used to re-partition records before they are bulk inserted. This can be used to sort, pack, cluster data"
          + " optimally for common query patterns. For now we support a build-in user defined bulkinsert partitioner org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner"
          + " which can does sorting based on specified column values set by " + BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS.key());

  public static final ConfigProperty<String> UPSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.upsert.shuffle.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Parallelism to use for upsert operation on the table. Upserts can "
          + "shuffle data to perform index lookups, file sizing, bin packing records optimally "
          + "into file groups. Before 0.13.0 release, "
          + "if users do not configure it, Hudi would use 200 as the default "
          + "shuffle parallelism. From 0.13.0 onwards Hudi by default automatically uses the "
          + "parallelism deduced by Spark based on the source data. If the shuffle parallelism "
          + "is explicitly configured by the user, the user-configured parallelism is "
          + "used in defining the actual parallelism. If you observe small files from the upsert "
          + "operation, we suggest configuring this shuffle parallelism explicitly, so that the "
          + "parallelism is around total_input_data_size/120MB.");

  public static final ConfigProperty<String> DELETE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.delete.shuffle.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Parallelism used for delete operation. Delete operations also performs "
          + "shuffles, similar to upsert operation. Before 0.13.0 release, "
          + "if users do not configure it, Hudi would use 200 as the default "
          + "shuffle parallelism. From 0.13.0 onwards Hudi by default automatically uses the "
          + "parallelism deduced by Spark based on the source data. If the shuffle parallelism "
          + "is explicitly configured by the user, the user-configured parallelism is "
          + "used in defining the actual parallelism.");

  public static final ConfigProperty<String> ROLLBACK_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.rollback.parallelism")
      .defaultValue("100")
      .markAdvanced()
      .withDocumentation("This config controls the parallelism for rollback of commits. "
          + "Rollbacks perform deletion of files or logging delete blocks to file groups on "
          + "storage in parallel. The configure value limits the parallelism so that the number "
          + "of Spark tasks do not exceed the value. If rollback is slow due to the limited "
          + "parallelism, you can increase this to tune the performance.");

  public static final ConfigProperty<String> WRITE_BUFFER_LIMIT_BYTES_VALUE = ConfigProperty
      .key("hoodie.write.buffer.limit.bytes")
      .defaultValue(String.valueOf(4 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("Size of in-memory buffer used for parallelizing network reads and lake storage writes.");

  public static final ConfigProperty<String> WRITE_BUFFER_RECORD_SAMPLING_RATE = ConfigProperty
      .key("hoodie.write.buffer.record.sampling.rate")
      .defaultValue(String.valueOf(64))
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Sampling rate of in-memory buffer used to estimate object size. Higher value lead to lower CPU usage.");

  public static final ConfigProperty<String> WRITE_BUFFER_RECORD_CACHE_LIMIT = ConfigProperty
      .key("hoodie.write.buffer.record.cache.limit")
      .defaultValue(String.valueOf(128 * 1024))
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Maximum queue size of in-memory buffer for parallelizing network reads and lake storage writes.");

  public static final ConfigProperty<String> WRITE_EXECUTOR_DISRUPTOR_BUFFER_LIMIT_BYTES = ConfigProperty
      .key("hoodie.write.executor.disruptor.buffer.limit.bytes")
      .defaultValue(String.valueOf(1024))
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The size of the Disruptor Executor ring buffer, must be power of 2");

  public static final ConfigProperty<String> WRITE_EXECUTOR_DISRUPTOR_WAIT_STRATEGY = ConfigProperty
      .key("hoodie.write.executor.disruptor.wait.strategy")
      .defaultValue(DisruptorWaitStrategyType.BLOCKING_WAIT.name())
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation(DisruptorWaitStrategyType.class);

  public static final ConfigProperty<String> COMBINE_BEFORE_INSERT = ConfigProperty
      .key("hoodie.combine.before.insert")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage.");

  public static final ConfigProperty<String> COMBINE_BEFORE_UPSERT = ConfigProperty
      .key("hoodie.combine.before.upsert")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming, "
          + " otherwise it can lead to duplicate keys and violate the uniqueness guarantees.");

  public static final ConfigProperty<String> COMBINE_BEFORE_DELETE = ConfigProperty
      .key("hoodie.combine.before.delete")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("During delete operations, controls whether we should combine deletes (and potentially also upserts) before "
          + " writing to storage.");

  public static final ConfigProperty<String> WRITE_STATUS_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.write.status.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .markAdvanced()
      .withDocumentation("Write status objects hold metadata about a write (stats, errors), that is not yet committed to storage. "
          + "This controls the how that information is cached for inspection by clients. We rarely expect this to be changed.");

  public static final ConfigProperty<String> WRITE_STATUS_CLASS_NAME = ConfigProperty
      .key("hoodie.writestatus.class")
      .defaultValue(WriteStatus.class.getName())
      .markAdvanced()
      .withDocumentation("Subclass of " + WriteStatus.class.getName() + " to be used to collect information about a write. Can be "
          + "overridden to collection additional metrics/statistics about the data if needed.");

  public static final ConfigProperty<String> FINALIZE_WRITE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.finalize.write.parallelism")
      .defaultValue("200")
      .markAdvanced()
      .withDocumentation("Parallelism for the write finalization internal operation, which involves removing any partially written "
          + "files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables "
          + "or low latency writes.");

  public static final ConfigProperty<String> MARKERS_TYPE = ConfigProperty
      .key("hoodie.write.markers.type")
      .defaultValue(MarkerType.TIMELINE_SERVER_BASED.name())
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation(MarkerType.class);

  public static final ConfigProperty<Integer> MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS = ConfigProperty
      .key("hoodie.markers.timeline_server_based.batch.num_threads")
      .defaultValue(20)
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("Number of threads to use for batch processing marker "
          + "creation requests at the timeline server");

  public static final ConfigProperty<Long> MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS = ConfigProperty
      .key("hoodie.markers.timeline_server_based.batch.interval_ms")
      .defaultValue(50L)
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("The batch interval in milliseconds for marker creation batch processing");

  public static final ConfigProperty<String> MARKERS_DELETE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.markers.delete.parallelism")
      .defaultValue("100")
      .markAdvanced()
      .withDocumentation("Determines the parallelism for deleting marker files, which are used to track all files (valid or invalid/partial) written during "
          + "a write operation. Increase this value if delays are observed, with large batch writes.");

  public static final ConfigProperty<String> BULK_INSERT_SORT_MODE = ConfigProperty
      .key("hoodie.bulkinsert.sort.mode")
      .defaultValue(BulkInsertSortMode.NONE.name())
      .markAdvanced()
      .withDocumentation(BulkInsertSortMode.class);

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),"
          + "running on each writer's driver process, accepting requests during the write from executors.");

  public static final ConfigProperty<Boolean> EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED = ConfigProperty
      .key("hoodie.embed.timeline.server.reuse.enabled")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Controls whether the timeline server instance should be cached and reused across the tables"
          + "to avoid startup costs and server overhead. This should only be used if you are running multiple writers in the same JVM.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_PORT_NUM = ConfigProperty
      .key("hoodie.embed.timeline.server.port")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Port at which the timeline server listens for requests. When running embedded in each writer, it picks "
          + "a free port and communicates to all the executors. This should rarely be changed.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_NUM_SERVER_THREADS = ConfigProperty
      .key("hoodie.embed.timeline.server.threads")
      .defaultValue("-1")
      .markAdvanced()
      .withDocumentation("Number of threads to serve requests in the timeline server. By default, auto configured based on the number of underlying cores.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server.gzip")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Controls whether gzip compression is used, for large responses from the timeline server, to improve latency.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server.async")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("Controls whether or not, the requests to the timeline server are processed in asynchronous fashion, "
          + "potentially improving throughput.");

  public static final ConfigProperty<String> FAIL_ON_TIMELINE_ARCHIVING_ENABLE = ConfigProperty
      .key("hoodie.fail.on.timeline.archiving")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Timeline archiving removes older instants from the timeline, after each write operation, to minimize metadata overhead. "
          + "Controls whether or not, the write should be failed as well, if such archiving fails.");

  public static final ConfigProperty<String> FAIL_ON_INLINE_TABLE_SERVICE_EXCEPTION = ConfigProperty
      .key("hoodie.fail.writes.on.inline.table.service.exception")
      .defaultValue("true")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Table services such as compaction and clustering can fail and prevent syncing to "
          + "the metaclient. Set this to true to fail writes when table services fail");

  public static final ConfigProperty<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(2000L)
      .markAdvanced()
      .withDocumentation("Initial time between successive attempts to ensure written data's metadata is consistent on storage. Grows with exponential"
          + " backoff after the initial value.");

  public static final ConfigProperty<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(300000L)
      .markAdvanced()
      .withDocumentation("Max time to wait between successive attempts at performing consistency checks");

  public static final ConfigProperty<Integer> MAX_CONSISTENCY_CHECKS = ConfigProperty
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(7)
      .markAdvanced()
      .withDocumentation("Maximum number of checks, for consistency of written data.");

  public static final ConfigProperty<String> MERGE_DATA_VALIDATION_CHECK_ENABLE = ConfigProperty
      .key("hoodie.merge.data.validation.enabled")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("When enabled, data validation checks are performed during merges to ensure expected "
          + "number of records after merge operation.");

  public static final ConfigProperty<String> MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE = ConfigProperty
      .key("hoodie.merge.allow.duplicate.on.inserts")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("When enabled, we allow duplicate keys even if inserts are routed to merge with an existing file (for ensuring file sizing)."
          + " This is only relevant for insert operation, since upsert, delete operations will ensure unique key constraints are maintained.");

  public static final ConfigProperty<Integer> MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT = ConfigProperty
      .key("hoodie.merge.small.file.group.candidates.limit")
      .defaultValue(1)
      .markAdvanced()
      .withDocumentation("Limits number of file groups, whose base file satisfies small-file limit, to consider for appending records during upsert operation. "
          + "Only applicable to MOR tables");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_INTERVAL_IN_MS = ConfigProperty
      .key("hoodie.client.heartbeat.interval_in_ms")
      .defaultValue(60 * 1000)
      .markAdvanced()
      .withDocumentation("Writers perform heartbeats to indicate liveness. Controls how often (in ms), such heartbeats are registered to lake storage.");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES = ConfigProperty
      .key("hoodie.client.heartbeat.tolerable.misses")
      .defaultValue(2)
      .markAdvanced()
      .withDocumentation("Number of heartbeat misses, before a writer is deemed not alive and all pending writes are aborted.");

  public static final ConfigProperty<String> WRITE_CONCURRENCY_MODE = ConfigProperty
      .key("hoodie.write.concurrency.mode")
      .defaultValue(WriteConcurrencyMode.SINGLE_WRITER.name())
      .withDocumentation(WriteConcurrencyMode.class);

  public static final ConfigProperty<Integer> NUM_RETRIES_ON_CONFLICT_FAILURES = ConfigProperty
      .key("hoodie.write.num.retries.on.conflict.failures")
      .defaultValue(0)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Maximum number of times to retry a batch on conflict failure.");

  public static final ConfigProperty<String> WRITE_SCHEMA_OVERRIDE = ConfigProperty
      .key("hoodie.write.schema")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Config allowing to override writer's schema. This might be necessary in "
          + "cases when writer's schema derived from the incoming dataset might actually be different from "
          + "the schema we actually want to use when writing. This, for ex, could be the case for"
          + "'partial-update' use-cases (like `MERGE INTO` Spark SQL statement for ex) where only "
          + "a projection of the incoming dataset might be used to update the records in the existing table, "
          + "prompting us to override the writer's schema");

  public static final ConfigProperty<Long> CDC_FILE_GROUP_ITERATOR_MEMORY_SPILL_BYTES = ConfigProperty
      .key("hoodie.cdc.file.group.iterator.memory.spill.bytes")
      .defaultValue(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
      .markAdvanced()
      .sinceVersion("1.0.1")
      .withDocumentation("Amount of memory in bytes to be used in bytes for CDCFileGroupIterator holding data in-memory, before spilling to disk.");

  /**
   * HUDI-858 : There are users who had been directly using RDD APIs and have relied on a behavior in 0.4.x to allow
   * multiple write operations (upsert/buk-insert/...) to be executed within a single commit.
   * <p>
   * Given Hudi commit protocol, these are generally unsafe operations and user need to handle failure scenarios. It
   * only works with COW table. Hudi 0.5.x had stopped this behavior.
   * <p>
   * Given the importance of supporting such cases for the user's migration to 0.5.x, we are proposing a safety flag
   * (disabled by default) which will allow this old behavior.
   */
  public static final ConfigProperty<String> ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE = ConfigProperty
      .key("_.hoodie.allow.multi.write.on.same.instant")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("");

  public static final ConfigProperty<String> AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE = ConfigProperty
      .key(AVRO_SCHEMA_STRING.key() + ".external.transformation")
      .defaultValue("false")
      .withAlternatives(AVRO_SCHEMA_STRING.key() + ".externalTransformation")
      .markAdvanced()
      .withDocumentation("When enabled, records in older schema are rewritten into newer schema during upsert,delete and background"
          + " compaction,clustering operations.");

  public static final ConfigProperty<Boolean> ALLOW_EMPTY_COMMIT = ConfigProperty
      .key("hoodie.allow.empty.commit")
      .defaultValue(true)
      .markAdvanced()
      .withDocumentation("Whether to allow generation of empty commits, even if no data was written in the commit. "
          + "It's useful in cases where extra metadata needs to be published regardless e.g tracking source offsets when ingesting data");

  public static final ConfigProperty<Boolean> ALLOW_OPERATION_METADATA_FIELD = ConfigProperty
      .key("hoodie.allow.operation.metadata.field")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("Whether to include '_hoodie_operation' in the metadata fields. "
          + "Once enabled, all the changes of a record are persisted to the delta log directly without merge");

  public static final ConfigProperty<String> FILEID_PREFIX_PROVIDER_CLASS = ConfigProperty
      .key("hoodie.fileid.prefix.provider.class")
      .defaultValue(RandomFileIdPrefixProvider.class.getName())
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("File Id Prefix provider class, that implements `org.apache.hudi.fileid.FileIdPrefixProvider`");

  public static final ConfigProperty<Boolean> TABLE_SERVICES_ENABLED = ConfigProperty
      .key("hoodie.table.services.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Master control to disable all table services including archive, clean, compact, cluster, etc.");

  public static final ConfigProperty<Boolean> RELEASE_RESOURCE_ENABLE = ConfigProperty
      .key("hoodie.release.resource.on.completion.enable")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Control to enable release all persist rdds when the spark job finish.");

  public static final ConfigProperty<Boolean> AUTO_ADJUST_LOCK_CONFIGS = ConfigProperty
      .key("hoodie.auto.adjust.lock.configs")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Auto adjust lock configurations when metadata table is enabled and for async table services.");

  public static final ConfigProperty<Boolean> SKIP_DEFAULT_PARTITION_VALIDATION = ConfigProperty
      .key("hoodie.skip.default.partition.validation")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.12.0")
      .withDocumentation("When table is upgraded from pre 0.12 to 0.12, we check for \"default\" partition and fail if found one. "
          + "Users are expected to rewrite the data in those partitions. Enabling this config will bypass this validation");

  public static final ConfigProperty<String> EARLY_CONFLICT_DETECTION_STRATEGY_CLASS_NAME = ConfigProperty
      .key(CONCURRENCY_PREFIX + "early.conflict.detection.strategy")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withInferFunction(cfg -> {
        MarkerType markerType = MarkerType.valueOf(cfg.getStringOrDefault(MARKERS_TYPE).toUpperCase());
        return Option.of(getDefaultEarlyConflictDetectionStrategy(markerType));
      })
      .withDocumentation("The class name of the early conflict detection strategy to use. "
          + "This should be a subclass of "
          + "`org.apache.hudi.common.conflict.detection.EarlyConflictDetectionStrategy`.");

  public static final ConfigProperty<Boolean> EARLY_CONFLICT_DETECTION_ENABLE = ConfigProperty
      .key(CONCURRENCY_PREFIX + "early.conflict.detection.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Whether to enable early conflict detection based on markers. "
          + "It eagerly detects writing conflict before create markers and fails fast if a "
          + "conflict is detected, to release cluster compute resources as soon as possible.");

  public static final ConfigProperty<Long> ASYNC_CONFLICT_DETECTOR_INITIAL_DELAY_MS = ConfigProperty
      .key(CONCURRENCY_PREFIX + "async.conflict.detector.initial_delay_ms")
      .defaultValue(0L)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Used for timeline-server-based markers with "
          + "`AsyncTimelineServerBasedDetectionStrategy`. "
          + "The time in milliseconds to delay the first execution of async marker-based conflict detection.");

  public static final ConfigProperty<Long> ASYNC_CONFLICT_DETECTOR_PERIOD_MS = ConfigProperty
      .key(CONCURRENCY_PREFIX + "async.conflict.detector.period_ms")
      .defaultValue(30000L)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Used for timeline-server-based markers with "
          + "`AsyncTimelineServerBasedDetectionStrategy`. "
          + "The period in milliseconds between successive executions of async marker-based conflict detection.");

  public static final ConfigProperty<Boolean> EARLY_CONFLICT_DETECTION_CHECK_COMMIT_CONFLICT = ConfigProperty
      .key(CONCURRENCY_PREFIX + "early.conflict.check.commit.conflict")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Whether to enable commit conflict checking or not during early "
          + "conflict detection.");

  public static final ConfigProperty<String> SENSITIVE_CONFIG_KEYS_FILTER = ConfigProperty
      .key("hoodie.sensitive.config.keys")
      .defaultValue("ssl,tls,sasl,auth,credentials")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Comma separated list of filters for sensitive config keys. Hudi Streamer "
          + "will not print any configuration which contains the configured filter. For example with "
          + "a configured filter `ssl`, value for config `ssl.trustore.location` would be masked.");

  public static final ConfigProperty<Boolean> ROLLBACK_INSTANT_BACKUP_ENABLED = ConfigProperty
      .key("hoodie.rollback.instant.backup.enabled")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Backup instants removed during rollback and restore (useful for debugging)");

  public static final ConfigProperty<String> ROLLBACK_INSTANT_BACKUP_DIRECTORY = ConfigProperty
      .key("hoodie.rollback.instant.backup.dir")
      .defaultValue(".rollback_backup")
      .markAdvanced()
      .withDocumentation("Path where instants being rolled back are copied. If not absolute path then a directory relative to .hoodie folder is created.");

  public static final ConfigProperty<String> CLIENT_INIT_CALLBACK_CLASS_NAMES = ConfigProperty
      .key("hoodie.client.init.callback.classes")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Fully-qualified class names of the Hudi client init callbacks to run "
          + "at the initialization of the Hudi client.  The class names are separated by `,`. "
          + "The class must be a subclass of `org.apache.hudi.callback.HoodieClientInitCallback`."
          + "By default, no Hudi client init callback is executed.");

  public static final ConfigProperty<Boolean> WRITE_RECORD_POSITIONS = ConfigProperty
      .key("hoodie.write.record.positions")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Whether to write record positions to the block header for data blocks containing updates and delete blocks. "
          + "The record positions can be used to improve the performance of merging records from base and log files.");

  public static final ConfigProperty<String> WRITE_PARTIAL_UPDATE_SCHEMA = ConfigProperty
      .key("hoodie.write.partial.update.schema")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Avro schema of the partial updates. This is automatically set by the "
          + "Hudi write client and user is not expected to manually change the value.");

  public static final ConfigProperty<Boolean> INCREMENTAL_TABLE_SERVICE_ENABLED = ConfigProperty
      .key("hoodie.table.services.incremental.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Whether to enable incremental table service. "
          + "So far Clustering and Compaction support incremental processing.");

  public static final ConfigProperty<Boolean> TRACK_EVENT_TIME_WATERMARK = ConfigProperty
      .key("hoodie.write.track.event.time.watermark")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Records event time watermark metadata in commit metadata when enabled");

  public static final ConfigProperty<String> MERGE_HANDLE_CLASS_NAME = ConfigProperty
      .key("hoodie.write.merge.handle.class")
      .defaultValue(FileGroupReaderBasedMergeHandle.class.getName())
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("The merge handle class that implements interface{@link HoodieMergeHandle} to merge the records "
          + "from a base file with an iterator of incoming records or a map of updates and deletes from log files at a file group level.");

  public static final ConfigProperty<String> CONCAT_HANDLE_CLASS_NAME = ConfigProperty
      .key("hoodie.write.concat.handle.class")
      .defaultValue(HoodieConcatHandle.class.getName())
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("The merge handle class to use to concat the records from a base file with an iterator of incoming records.");

  public static final ConfigProperty<String> COMPACT_MERGE_HANDLE_CLASS_NAME = ConfigProperty
      .key("hoodie.compact.merge.handle.class")
      .defaultValue(FileGroupReaderBasedMergeHandle.class.getName())
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Merge handle class for compaction");

  public static final ConfigProperty<Boolean> MERGE_HANDLE_PERFORM_FALLBACK = ConfigProperty
      .key("hoodie.write.merge.handle.fallback")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("When using a custom Hoodie Merge Handle Implementation controlled by the config " + MERGE_HANDLE_CLASS_NAME.key()
          + " or when using a custom Hoodie Concat Handle Implementation controlled by the config " + CONCAT_HANDLE_CLASS_NAME.key()
              + ", enabling this config results in fallback to the default implementations if instantiation of the custom implementation fails");

  /**
   * Config key with boolean value that indicates whether record being written during MERGE INTO Spark SQL
   * operation are already prepped.
   */
  public static final String SPARK_SQL_MERGE_INTO_PREPPED_KEY = "_hoodie.spark.sql.merge.into.prepped";

  /**
   * An internal config referring to fileID encoding. 0 refers to UUID based encoding and 1 refers to raw string format(random string).
   */
  public static final String WRITES_FILEID_ENCODING = "_hoodie.writes.fileid.encoding";

  @Setter
  private ConsistencyGuardConfig consistencyGuardConfig;
  private FileSystemRetryConfig fileSystemRetryConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  @Setter
  private FileSystemViewStorageConfig viewStorageConfig;
  private HoodiePayloadConfig hoodiePayloadConfig;
  private HoodieMetadataConfig metadataConfig;
  private HoodieMetricsConfig metricsConfig;
  private HoodieMetaserverConfig metaserverConfig;
  private HoodieTableServiceManagerConfig tableServiceManagerConfig;
  private HoodieCommonConfig commonConfig;
  private HoodieStorageConfig storageConfig;
  private HoodieTimeGeneratorConfig timeGeneratorConfig;
  private HoodieIndexingConfig indexingConfig;
  private final EngineType engineType;

  /**
   * @deprecated Use {@link #TBL_NAME} and its methods instead
   */
  @Deprecated
  public static final String TABLE_NAME = TBL_NAME.key();
  /**
   * @deprecated Use {@link #PRECOMBINE_FIELD_NAME} and its methods instead
   */
  @Deprecated
  public static final String PRECOMBINE_FIELD_PROP = PRECOMBINE_FIELD_NAME.key();
  /**
   * @deprecated Use {@link #WRITE_PAYLOAD_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String WRITE_PAYLOAD_CLASS = WRITE_PAYLOAD_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #KEYGENERATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String KEYGENERATOR_CLASS_PROP = KEYGENERATOR_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #KEYGENERATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_KEYGENERATOR_CLASS = SimpleAvroKeyGenerator.class.getName();
  /**
   * @deprecated Use {@link #ROLLBACK_USING_MARKERS_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_ROLLBACK_USING_MARKERS = ROLLBACK_USING_MARKERS_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #ROLLBACK_USING_MARKERS_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String ROLLBACK_USING_MARKERS = ROLLBACK_USING_MARKERS_ENABLE.key();
  /**
   * @deprecated Use {@link #TIMELINE_LAYOUT_VERSION_NUM} and its methods instead
   */
  @Deprecated
  public static final String TIMELINE_LAYOUT_VERSION = TIMELINE_LAYOUT_VERSION_NUM.key();
  /**
   * @deprecated Use {@link #BASE_PATH} and its methods instead
   */
  @Deprecated
  public static final String BASE_PATH_PROP = BASE_PATH.key();
  /**
   * @deprecated Use {@link #AVRO_SCHEMA_STRING} and its methods instead
   */
  @Deprecated
  public static final String AVRO_SCHEMA = AVRO_SCHEMA_STRING.key();
  /**
   * @deprecated Use {@link #AVRO_SCHEMA_VALIDATE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String AVRO_SCHEMA_VALIDATE = AVRO_SCHEMA_VALIDATE_ENABLE.key();
  /**
   * @deprecated Use {@link #AVRO_SCHEMA_VALIDATE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_AVRO_SCHEMA_VALIDATE = AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #INSERT_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARALLELISM = INSERT_PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #INSERT_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String INSERT_PARALLELISM = INSERT_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #BULKINSERT_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String BULKINSERT_PARALLELISM = BULKINSERT_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String BULKINSERT_USER_DEFINED_PARTITIONER_CLASS = BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME.key();
  @Deprecated
  public static final String BULKINSERT_INPUT_DATA_SCHEMA_DDL = "hoodie.bulkinsert.schema.ddl";
  /**
   * @deprecated Use {@link #UPSERT_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String UPSERT_PARALLELISM = UPSERT_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #DELETE_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DELETE_PARALLELISM = DELETE_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #ROLLBACK_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_ROLLBACK_PARALLELISM = ROLLBACK_PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #ROLLBACK_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String ROLLBACK_PARALLELISM = ROLLBACK_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #WRITE_BUFFER_LIMIT_BYTES_VALUE} and its methods instead
   */
  @Deprecated
  public static final String WRITE_BUFFER_LIMIT_BYTES = WRITE_BUFFER_LIMIT_BYTES_VALUE.key();
  /**
   * @deprecated Use {@link #WRITE_BUFFER_LIMIT_BYTES_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_WRITE_BUFFER_LIMIT_BYTES = WRITE_BUFFER_LIMIT_BYTES_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_INSERT} and its methods instead
   */
  @Deprecated
  public static final String COMBINE_BEFORE_INSERT_PROP = COMBINE_BEFORE_INSERT.key();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_INSERT} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COMBINE_BEFORE_INSERT = COMBINE_BEFORE_INSERT.defaultValue();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_UPSERT} and its methods instead
   */
  @Deprecated
  public static final String COMBINE_BEFORE_UPSERT_PROP = COMBINE_BEFORE_UPSERT.key();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_UPSERT} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COMBINE_BEFORE_UPSERT = COMBINE_BEFORE_UPSERT.defaultValue();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_DELETE} and its methods instead
   */
  @Deprecated
  public static final String COMBINE_BEFORE_DELETE_PROP = COMBINE_BEFORE_DELETE.key();
  /**
   * @deprecated Use {@link #COMBINE_BEFORE_DELETE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_COMBINE_BEFORE_DELETE = COMBINE_BEFORE_DELETE.defaultValue();
  /**
   * @deprecated Use {@link #WRITE_STATUS_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String WRITE_STATUS_STORAGE_LEVEL = WRITE_STATUS_STORAGE_LEVEL_VALUE.key();
  /**
   * @deprecated Use {@link #WRITE_STATUS_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = WRITE_STATUS_STORAGE_LEVEL_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #WRITE_STATUS_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_WRITE_STATUS_CLASS_PROP = WRITE_STATUS_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #WRITE_STATUS_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HOODIE_WRITE_STATUS_CLASS = WRITE_STATUS_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #FINALIZE_WRITE_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String FINALIZE_WRITE_PARALLELISM = FINALIZE_WRITE_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #FINALIZE_WRITE_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_FINALIZE_WRITE_PARALLELISM = FINALIZE_WRITE_PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #MARKERS_DELETE_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String MARKERS_DELETE_PARALLELISM = MARKERS_DELETE_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #MARKERS_DELETE_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_MARKERS_DELETE_PARALLELISM = MARKERS_DELETE_PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #BULK_INSERT_SORT_MODE} and its methods instead
   */
  @Deprecated
  public static final String BULKINSERT_SORT_MODE = BULK_INSERT_SORT_MODE.key();
  /**
   * @deprecated Use {@link #BULK_INSERT_SORT_MODE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BULKINSERT_SORT_MODE = BULK_INSERT_SORT_MODE.defaultValue();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String EMBEDDED_TIMELINE_SERVER_ENABLED = EMBEDDED_TIMELINE_SERVER_ENABLE.key();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_ENABLED = EMBEDDED_TIMELINE_SERVER_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String EMBEDDED_TIMELINE_SERVER_PORT = EMBEDDED_TIMELINE_SERVER_PORT_NUM.key();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_PORT = EMBEDDED_TIMELINE_SERVER_PORT_NUM.defaultValue();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_NUM_SERVER_THREADS} and its methods instead
   */
  @Deprecated
  public static final String EMBEDDED_TIMELINE_SERVER_THREADS = EMBEDDED_TIMELINE_NUM_SERVER_THREADS.key();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_NUM_SERVER_THREADS} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_THREADS = EMBEDDED_TIMELINE_NUM_SERVER_THREADS.defaultValue();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT = EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE.key();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EMBEDDED_TIMELINE_COMPRESS_OUTPUT = EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String EMBEDDED_TIMELINE_SERVER_USE_ASYNC = EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE.key();
  /**
   * @deprecated Use {@link #EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_ASYNC = EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #FAIL_ON_TIMELINE_ARCHIVING_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP = FAIL_ON_TIMELINE_ARCHIVING_ENABLE.key();
  /**
   * @deprecated Use {@link #FAIL_ON_TIMELINE_ARCHIVING_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_FAIL_ON_TIMELINE_ARCHIVING_ENABLED = FAIL_ON_TIMELINE_ARCHIVING_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #INITIAL_CONSISTENCY_CHECK_INTERVAL_MS} and its methods instead
   */
  @Deprecated
  public static final String INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = INITIAL_CONSISTENCY_CHECK_INTERVAL_MS.key();
  /**
   * @deprecated Use {@link #INITIAL_CONSISTENCY_CHECK_INTERVAL_MS} and its methods instead
   */
  @Deprecated
  public static long DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = INITIAL_CONSISTENCY_CHECK_INTERVAL_MS.defaultValue();
  /**
   * @deprecated Use {@link #MAX_CONSISTENCY_CHECK_INTERVAL_MS} and its methods instead
   */
  @Deprecated
  public static final String MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = MAX_CONSISTENCY_CHECK_INTERVAL_MS.key();
  /**
   * @deprecated Use {@link #MAX_CONSISTENCY_CHECK_INTERVAL_MS} and its methods instead
   */
  @Deprecated
  public static long DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS = MAX_CONSISTENCY_CHECK_INTERVAL_MS.defaultValue();
  /**
   * @deprecated Use {@link #MAX_CONSISTENCY_CHECKS} and its methods instead
   */
  @Deprecated
  public static final String MAX_CONSISTENCY_CHECKS_PROP = MAX_CONSISTENCY_CHECKS.key();
  /**
   * @deprecated Use {@link #MAX_CONSISTENCY_CHECKS} and its methods instead
   */
  @Deprecated
  public static int DEFAULT_MAX_CONSISTENCY_CHECKS = MAX_CONSISTENCY_CHECKS.defaultValue();
  /**
   * @deprecated Use {@link #MERGE_DATA_VALIDATION_CHECK_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String MERGE_DATA_VALIDATION_CHECK_ENABLED = MERGE_DATA_VALIDATION_CHECK_ENABLE.key();
  /**
   * @deprecated Use {@link #MERGE_DATA_VALIDATION_CHECK_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_MERGE_DATA_VALIDATION_CHECK_ENABLED = MERGE_DATA_VALIDATION_CHECK_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String MERGE_ALLOW_DUPLICATE_ON_INSERTS = MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key();
  /**
   * @deprecated Use {@link #MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_MERGE_ALLOW_DUPLICATE_ON_INSERTS = MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #CLIENT_HEARTBEAT_INTERVAL_IN_MS} and its methods instead
   */
  @Deprecated
  public static final String CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP = CLIENT_HEARTBEAT_INTERVAL_IN_MS.key();
  /**
   * @deprecated Use {@link #CLIENT_HEARTBEAT_INTERVAL_IN_MS} and its methods instead
   */
  @Deprecated
  public static final Integer DEFAULT_CLIENT_HEARTBEAT_INTERVAL_IN_MS = CLIENT_HEARTBEAT_INTERVAL_IN_MS.defaultValue();
  /**
   * @deprecated Use {@link #CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES} and its methods instead
   */
  @Deprecated
  public static final String CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP = CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.key();
  /**
   * @deprecated Use {@link #CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES} and its methods instead
   */
  @Deprecated
  public static final Integer DEFAULT_CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES = CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.defaultValue();
  /**
   * @deprecated Use {@link #WRITE_CONCURRENCY_MODE} and its methods instead
   */
  @Deprecated
  public static final String WRITE_CONCURRENCY_MODE_PROP = WRITE_CONCURRENCY_MODE.key();
  /**
   * @deprecated Use {@link #WRITE_CONCURRENCY_MODE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_WRITE_CONCURRENCY_MODE = WRITE_CONCURRENCY_MODE.defaultValue();
  /**
   * @deprecated Use {@link #ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String ALLOW_MULTI_WRITE_ON_SAME_INSTANT = ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE.key();
  /**
   * @deprecated Use {@link #ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_ALLOW_MULTI_WRITE_ON_SAME_INSTANT = ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE.key();
  /**
   * @deprecated Use {@link #AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE.defaultValue();
  /**
   * Use Spark engine by default.
   */
  protected HoodieWriteConfig() {
    super();
    this.engineType = EngineType.SPARK;
    this.clientSpecifiedViewStorageConfig = null;
  }

  protected HoodieWriteConfig(EngineType engineType, Properties props) {
    super(props);
    Properties newProps = new Properties();
    newProps.putAll(props);
    this.engineType = engineType;
    this.consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().fromProperties(newProps).build();
    this.fileSystemRetryConfig = FileSystemRetryConfig.newBuilder().fromProperties(newProps).build();
    this.clientSpecifiedViewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(newProps).build();
    this.viewStorageConfig = clientSpecifiedViewStorageConfig;
    this.hoodiePayloadConfig = HoodiePayloadConfig.newBuilder().fromProperties(newProps).build();
    this.metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(props).build();
    this.metricsConfig = HoodieMetricsConfig.newBuilder().fromProperties(props).build();
    this.metaserverConfig = HoodieMetaserverConfig.newBuilder().fromProperties(props).build();
    this.tableServiceManagerConfig = HoodieTableServiceManagerConfig.newBuilder().fromProperties(props).build();
    this.commonConfig = HoodieCommonConfig.newBuilder().fromProperties(props).build();
    this.storageConfig = HoodieStorageConfig.newBuilder().fromProperties(props).build();
    this.timeGeneratorConfig = HoodieTimeGeneratorConfig.newBuilder().fromProperties(props)
        .withDefaultLockProvider(!isLockRequired()).build();
    this.indexingConfig = HoodieIndexingConfig.newBuilder().fromProperties(props).build();
  }

  public static HoodieWriteConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties.
   */
  public String getBasePath() {
    return getString(BASE_PATH);
  }

  public HoodieFileFormat getBaseFileFormat() {
    return HoodieFileFormat.getValue(getStringOrDefault(BASE_FILE_FORMAT));
  }

  public String getRecordMergeStrategyId() {
    return getString(RECORD_MERGE_STRATEGY_ID);
  }

  public RecordMergeMode getRecordMergeMode() {
    return RecordMergeMode.getValue(getString(RECORD_MERGE_MODE));
  }

  public HoodieRecordMerger getRecordMerger() {
    return HoodieRecordUtils.createRecordMerger(getString(BASE_PATH),
        engineType, getSplitStrings(RECORD_MERGE_IMPL_CLASSES), getString(RECORD_MERGE_STRATEGY_ID));
  }

  public String getSchema() {
    return getString(AVRO_SCHEMA_STRING);
  }

  public void setSchema(String schemaStr) {
    setValue(AVRO_SCHEMA_STRING, schemaStr);
  }

  public void setRecordMergerClass(String recordMergerClass) {
    setValue(RECORD_MERGE_IMPL_CLASSES, recordMergerClass);
  }

  /**
   * Returns schema used for writing records
   *
   * NOTE: This method respects {@link HoodieWriteConfig#WRITE_SCHEMA_OVERRIDE} being
   *       specified overriding original writing schema
   */
  public String getWriteSchema() {
    if (props.containsKey(WRITE_SCHEMA_OVERRIDE.key())) {
      return getString(WRITE_SCHEMA_OVERRIDE);
    }
    return getSchema();
  }

  public HoodieTableVersion getWriteVersion() {
    Integer versionCode = getInt(WRITE_TABLE_VERSION);
    if (versionCode != null) {
      WRITE_TABLE_VERSION.checkValues(versionCode.toString());
    }
    return HoodieTableVersion.fromVersionCode(getIntOrDefault(WRITE_TABLE_VERSION));
  }

  public void setWriteVersion(HoodieTableVersion version) {
    setValue(WRITE_TABLE_VERSION, String.valueOf(version.versionCode()));
  }

  public boolean autoUpgrade() {
    return getBoolean(AUTO_UPGRADE_VERSION);
  }

  public String getTaggedRecordStorageLevel() {
    return getString(TAGGED_RECORD_STORAGE_LEVEL_VALUE);
  }

  public Boolean isSourceRddPersisted() {
    return getBoolean(ERROR_TABLE_PERSIST_SOURCE_RDD);
  }

  public String getInternalSchema() {
    return getString(INTERNAL_SCHEMA_STRING);
  }

  public void setInternalSchemaString(String internalSchemaString) {
    setValue(INTERNAL_SCHEMA_STRING, internalSchemaString);
  }

  public boolean getSchemaEvolutionEnable() {
    return getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE);
  }

  public void setSchemaEvolutionEnable(boolean enable) {
    setValue(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE, String.valueOf(enable));
  }

  public boolean shouldValidateAvroSchema() {
    return getBoolean(AVRO_SCHEMA_VALIDATE_ENABLE);
  }

  public boolean shouldAllowAutoEvolutionColumnDrop() {
    return getBooleanOrDefault(SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP);
  }

  public String getTableName() {
    return getString(TBL_NAME);
  }

  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(getStringOrDefault(
        HoodieTableConfig.TYPE, HoodieTableConfig.TYPE.defaultValue().name()).toUpperCase());
  }

  @Deprecated
  public List<String> getPreCombineFields() {
    return Option.ofNullable(getString(PRECOMBINE_FIELD_NAME))
        .map(preCombine -> Arrays.asList(preCombine.split(",")))
        .orElse(Collections.emptyList())
        .stream()
        .filter(StringUtils::nonEmpty)
        .collect(Collectors.toList());
  }

  public String getKeyGeneratorClass() {
    return getString(KEYGENERATOR_CLASS_NAME);
  }

  public ExecutorType getExecutorType() {
    return ExecutorType.valueOf(getStringOrDefault(WRITE_EXECUTOR_TYPE).toUpperCase(Locale.ROOT));
  }

  public boolean isConsistentHashingEnabled() {
    return getIndexType() == HoodieIndex.IndexType.BUCKET && getBucketIndexEngineType() == HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING;
  }

  public boolean isSimpleBucketIndex() {
    return HoodieIndex.IndexType.BUCKET.equals(getIndexType())
        && HoodieIndex.BucketIndexEngineType.SIMPLE.equals(getBucketIndexEngineType());
  }

  /**
   * Returns whether the table writer would generate pure log files at the very first place.
   */
  public boolean isYieldingPureLogForMor() {
    switch (getIndexType()) {
      case BUCKET:
      case FLINK_STATE:
        return true;
      default:
        return false;
    }
  }

  public boolean isMergeHandleFallbackEnabled() {
    return getBooleanOrDefault(HoodieWriteConfig.MERGE_HANDLE_PERFORM_FALLBACK);
  }

  public boolean isConsistentLogicalTimestampEnabled() {
    return getBooleanOrDefault(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED);
  }

  public boolean shouldUseExternalSchemaTransformation() {
    return getBoolean(AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE);
  }

  public Integer getTimelineLayoutVersion() {
    return getInt(TIMELINE_LAYOUT_VERSION_NUM);
  }

  public int getBulkInsertShuffleParallelism() {
    return getInt(BULKINSERT_PARALLELISM_VALUE);
  }

  public String getUserDefinedBulkInsertPartitionerClass() {
    return getString(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME);
  }

  public String getUserDefinedBulkInsertPartitionerSortColumns() {
    return getString(BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS);
  }

  public int getInsertShuffleParallelism() {
    return getInt(INSERT_PARALLELISM_VALUE);
  }

  public int getUpsertShuffleParallelism() {
    return getInt(UPSERT_PARALLELISM_VALUE);
  }

  public int getDeleteShuffleParallelism() {
    return getInt(DELETE_PARALLELISM_VALUE);
  }

  public int getRollbackParallelism() {
    return getInt(ROLLBACK_PARALLELISM_VALUE);
  }

  public int getFileListingParallelism() {
    return metadataConfig.getFileListingParallelism();
  }

  public boolean shouldRollbackUsingMarkers() {
    return getBoolean(ROLLBACK_USING_MARKERS_ENABLE);
  }

  public boolean enableComplexKeygenValidation() {
    return getBoolean(ENABLE_COMPLEX_KEYGEN_VALIDATION);
  }

  public boolean shouldFailOnDuplicateDataFileDetection() {
    return getBoolean(FAIL_JOB_ON_DUPLICATE_DATA_FILE_DETECTION);
  }

  public int getWriteBufferLimitBytes() {
    return Integer.parseInt(getStringOrDefault(WRITE_BUFFER_LIMIT_BYTES_VALUE));
  }

  public int getWriteBufferRecordSamplingRate() {
    return Integer.parseInt(getStringOrDefault(WRITE_BUFFER_RECORD_SAMPLING_RATE));
  }

  public int getWriteBufferRecordCacheLimit() {
    return Integer.parseInt(getStringOrDefault(WRITE_BUFFER_RECORD_CACHE_LIMIT));
  }

  public String getWriteExecutorDisruptorWaitStrategy() {
    return getStringOrDefault(WRITE_EXECUTOR_DISRUPTOR_WAIT_STRATEGY);
  }

  public Integer getWriteExecutorDisruptorWriteBufferLimitBytes() {
    return Integer.parseInt(getStringOrDefault(WRITE_EXECUTOR_DISRUPTOR_BUFFER_LIMIT_BYTES));
  }

  public boolean shouldCombineBeforeInsert() {
    return getBoolean(COMBINE_BEFORE_INSERT);
  }

  public boolean shouldCombineBeforeUpsert() {
    return getBoolean(COMBINE_BEFORE_UPSERT);
  }

  public boolean shouldCombineBeforeDelete() {
    return getBoolean(COMBINE_BEFORE_DELETE);
  }

  public boolean shouldAllowMultiWriteOnSameInstant() {
    return getBoolean(ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE);
  }

  public boolean shouldDropPartitionColumns() {
    return getBoolean(HoodieTableConfig.DROP_PARTITION_COLUMNS);
  }

  public String getWriteStatusClassName() {
    return getString(WRITE_STATUS_CLASS_NAME);
  }

  public String getMergeHandleClassName() {
    return getStringOrDefault(MERGE_HANDLE_CLASS_NAME);
  }

  public String getConcatHandleClassName() {
    return getStringOrDefault(CONCAT_HANDLE_CLASS_NAME);
  }

  public String getCompactionMergeHandleClassName() {
    return getStringOrDefault(COMPACT_MERGE_HANDLE_CLASS_NAME);
  }

  public int getFinalizeWriteParallelism() {
    return getInt(FINALIZE_WRITE_PARALLELISM_VALUE);
  }

  public MarkerType getMarkersType() {
    String markerType = getString(MARKERS_TYPE);
    return MarkerType.valueOf(markerType.toUpperCase());
  }

  public boolean isHiveStylePartitioningEnabled() {
    return getBooleanOrDefault(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE);
  }

  public int getMarkersTimelineServerBasedBatchNumThreads() {
    return getInt(MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS);
  }

  public long getMarkersTimelineServerBasedBatchIntervalMs() {
    return getLong(MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS);
  }

  public int getMarkersDeleteParallelism() {
    return getInt(MARKERS_DELETE_PARALLELISM_VALUE);
  }

  public boolean isEmbeddedTimelineServerEnabled() {
    return getBoolean(EMBEDDED_TIMELINE_SERVER_ENABLE);
  }

  public boolean isEmbeddedTimelineServerReuseEnabled() {
    return getBoolean(EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED);
  }

  public boolean isRemoteViewStorageType() {
    FileSystemViewStorageType storageType = getViewStorageConfig().getStorageType();
    return storageType == FileSystemViewStorageType.REMOTE_ONLY
        || storageType == FileSystemViewStorageType.REMOTE_FIRST;
  }

  public int getEmbeddedTimelineServerPort() {
    return Integer.parseInt(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_PORT_NUM));
  }

  public int getEmbeddedTimelineServerThreads() {
    return Integer.parseInt(getStringOrDefault(EMBEDDED_TIMELINE_NUM_SERVER_THREADS));
  }

  public boolean getEmbeddedTimelineServerCompressOutput() {
    return Boolean.parseBoolean(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE));
  }

  public boolean getEmbeddedTimelineServerUseAsync() {
    return Boolean.parseBoolean(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE));
  }

  public boolean isFailOnTimelineArchivingEnabled() {
    return getBoolean(FAIL_ON_TIMELINE_ARCHIVING_ENABLE);
  }

  public boolean isFailOnInlineTableServiceExceptionEnabled() {
    return getBoolean(FAIL_ON_INLINE_TABLE_SERVICE_EXCEPTION);
  }

  public int getMaxConsistencyChecks() {
    return getInt(MAX_CONSISTENCY_CHECKS);
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return getInt(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS);
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return getInt(MAX_CONSISTENCY_CHECK_INTERVAL_MS);
  }

  public BulkInsertSortMode getBulkInsertSortMode() {
    String sortMode = getStringOrDefault(BULK_INSERT_SORT_MODE);
    return BulkInsertSortMode.valueOf(sortMode.toUpperCase());
  }

  public boolean isMergeDataValidationCheckEnabled() {
    return getBoolean(MERGE_DATA_VALIDATION_CHECK_ENABLE);
  }

  public boolean allowDuplicateInserts() {
    return getBoolean(MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE);
  }

  public int getSmallFileGroupCandidatesLimit() {
    return getInt(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT);
  }

  public boolean populateMetaFields() {
    return getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
  }

  /**
   * compaction properties.
   */

  public boolean isLogCompactionEnabled() {
    return getBoolean(HoodieCompactionConfig.ENABLE_LOG_COMPACTION);
  }

  public int getLogCompactionBlocksThreshold() {
    return getInt(HoodieCompactionConfig.LOG_COMPACTION_BLOCKS_THRESHOLD);
  }

  public boolean enableOptimizedLogBlocksScan() {
    return getBoolean(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN);
  }

  public HoodieCleaningPolicy getCleanerPolicy() {
    return HoodieCleaningPolicy.valueOf(getString(CLEANER_POLICY));
  }

  public int getCleanerFileVersionsRetained() {
    return getInt(HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED);
  }

  public int getCleanerCommitsRetained() {
    return getInt(HoodieCleanConfig.CLEANER_COMMITS_RETAINED);
  }

  public int getCleanerHoursRetained() {
    return getInt(HoodieCleanConfig.CLEANER_HOURS_RETAINED);
  }

  public int getMaxCommitsToKeep() {
    return getInt(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP);
  }

  public int getMinCommitsToKeep() {
    return getInt(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP);
  }

  public int getTimelineCompactionBatchSize() {
    return getInt(HoodieArchivalConfig.TIMELINE_COMPACTION_BATCH_SIZE);
  }

  public int getParquetSmallFileLimit() {
    return getInt(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT);
  }

  public double getRecordSizeEstimationThreshold() {
    return getDouble(HoodieCompactionConfig.RECORD_SIZE_ESTIMATION_THRESHOLD);
  }

  public int getCopyOnWriteInsertSplitSize() {
    return getInt(HoodieCompactionConfig.COPY_ON_WRITE_INSERT_SPLIT_SIZE);
  }

  public int getCopyOnWriteRecordSizeEstimate() {
    return getInt(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE);
  }

  public String getRecordSizeEstimator() {
    return getStringOrDefault(RECORD_SIZE_ESTIMATOR_CLASS_NAME);
  }

  public int getRecordSizeEstimatorMaxCommits() {
    return getIntOrDefault(RECORD_SIZE_ESTIMATOR_MAX_COMMITS);
  }

  public long getRecordSizeEstimatorAverageMetadataSize() {
    return Long.parseLong(getStringOrDefault(RECORD_SIZE_ESTIMATOR_AVERAGE_METADATA_SIZE));
  }

  public boolean allowMultipleCleans() {
    return getBoolean(HoodieCleanConfig.ALLOW_MULTIPLE_CLEANS);
  }

  public boolean shouldAutoTuneInsertSplits() {
    return getBoolean(HoodieCompactionConfig.COPY_ON_WRITE_AUTO_SPLIT_INSERTS);
  }

  public int getCleanerParallelism() {
    return getInt(HoodieCleanConfig.CLEANER_PARALLELISM_VALUE);
  }

  public int getCleaningMaxCommits() {
    return getInt(HoodieCleanConfig.CLEAN_MAX_COMMITS);
  }

  public CleaningTriggerStrategy getCleaningTriggerStrategy() {
    return CleaningTriggerStrategy.valueOf(getString(HoodieCleanConfig.CLEAN_TRIGGER_STRATEGY));
  }

  public boolean isAutoClean() {
    return getBoolean(HoodieCleanConfig.AUTO_CLEAN);
  }

  public boolean shouldArchiveBeyondSavepoint() {
    return getBooleanOrDefault(HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT);
  }

  public boolean isAutoArchive() {
    return getBoolean(HoodieArchivalConfig.AUTO_ARCHIVE);
  }

  public boolean isAsyncArchive() {
    return getBoolean(HoodieArchivalConfig.ASYNC_ARCHIVE);
  }

  public boolean isAsyncClean() {
    return getBoolean(HoodieCleanConfig.ASYNC_CLEAN);
  }

  public boolean incrementalCleanerModeEnabled() {
    return getBoolean(HoodieCleanConfig.CLEANER_INCREMENTAL_MODE_ENABLE);
  }

  public boolean inlineCompactionEnabled() {
    return getBoolean(HoodieCompactionConfig.INLINE_COMPACT);
  }

  public boolean scheduleInlineCompaction() {
    return getBoolean(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT);
  }

  public boolean inlineLogCompactionEnabled() {
    return getBoolean(HoodieCompactionConfig.INLINE_LOG_COMPACT);
  }

  public CompactionTriggerStrategy getInlineCompactTriggerStrategy() {
    return CompactionTriggerStrategy.valueOf(getString(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY));
  }

  public int getInlineCompactDeltaCommitMax() {
    return getInt(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS);
  }

  public int getInlineCompactDeltaSecondsMax() {
    return getInt(HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS);
  }

  public CompactionStrategy getCompactionStrategy() {
    String compactionStrategiesStr = getString(HoodieCompactionConfig.COMPACTION_STRATEGY);
    String[] compactionStrategyArr = compactionStrategiesStr.split(",");
    List<CompactionStrategy> compactionStrategies = Arrays.stream(compactionStrategyArr)
        .map(className -> (CompactionStrategy) ReflectionUtils.loadClass(className)).collect(Collectors.toList());
    return compactionStrategies.size() == 1 ? compactionStrategies.get(0) : new CompositeCompactionStrategy(compactionStrategies);
  }

  public Long getTargetIOPerCompactionInMB() {
    return getLong(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB);
  }

  public Long getCompactionLogFileSizeThreshold() {
    return getLong(HoodieCompactionConfig.COMPACTION_LOG_FILE_SIZE_THRESHOLD);
  }

  public Long getCompactionLogFileNumThreshold() {
    return getLong(HoodieCompactionConfig.COMPACTION_LOG_FILE_NUM_THRESHOLD);
  }

  public Boolean getCompactionLazyBlockReadEnabled() {
    return getBoolean(HoodieReaderConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE);
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return getBoolean(HoodieReaderConfig.COMPACTION_REVERSE_LOG_READ_ENABLE);
  }

  public int getArchiveDeleteParallelism() {
    return getInt(HoodieArchivalConfig.DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE);
  }

  public boolean inlineClusteringEnabled() {
    return getBoolean(HoodieClusteringConfig.INLINE_CLUSTERING);
  }

  public boolean scheduleInlineClustering() {
    return getBoolean(HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING);
  }

  public boolean isAsyncClusteringEnabled() {
    return getBoolean(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE);
  }

  public boolean isClusteringEnabled() {
    // TODO: future support async clustering
    return inlineClusteringEnabled() || isAsyncClusteringEnabled();
  }

  public boolean isRollbackPendingClustering() {
    return getBoolean(HoodieClusteringConfig.ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT);
  }

  public boolean isBinaryCopySchemaEvolutionEnabled() {
    return getBooleanOrDefault(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE);
  }

  public int getInlineClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS);
  }

  public int getAsyncClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS);
  }

  public String getPayloadClass() {
    return HoodieRecordPayload.getPayloadClassName(this);
  }

  public int getTargetPartitionsPerDayBasedCompaction() {
    return getInt(HoodieCompactionConfig.TARGET_PARTITIONS_PER_DAYBASED_COMPACTION);
  }

  public int getCommitArchivalBatchSize() {
    return getInt(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE);
  }

  public Boolean shouldCleanBootstrapBaseFile() {
    return getBoolean(HoodieCleanConfig.CLEANER_BOOTSTRAP_BASE_FILE_ENABLE);
  }

  public String getClusteringUpdatesStrategyClass() {
    return getString(HoodieClusteringConfig.UPDATES_STRATEGY);
  }

  public HoodieFailedWritesCleaningPolicy getFailedWritesCleanPolicy() {
    return HoodieFailedWritesCleaningPolicy
        .valueOf(getString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY));
  }

  public String getCompactionSpecifyPartitionPathRegex() {
    return getString(HoodieCompactionConfig.COMPACTION_SPECIFY_PARTITION_PATH_REGEX);
  }

  /**
   * Clustering properties.
   */
  public String getClusteringPlanStrategyClass() {
    return getString(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME);
  }

  public int getClusteringMaxParallelism() {
    return getInt(HoodieClusteringConfig.CLUSTERING_MAX_PARALLELISM);
  }

  public ClusteringPlanPartitionFilterMode getClusteringPlanPartitionFilterMode() {
    String mode = getString(HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME);
    return ClusteringPlanPartitionFilterMode.valueOf(mode);
  }

  public String getBeginPartitionForClustering() {
    return getString(HoodieClusteringConfig.PARTITION_FILTER_BEGIN_PARTITION);
  }

  public String getEndPartitionForClustering() {
    return getString(HoodieClusteringConfig.PARTITION_FILTER_END_PARTITION);
  }

  public String getClusteringExecutionStrategyClass() {
    return getString(HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME);
  }

  public long getClusteringMaxBytesInGroup() {
    return getLong(HoodieClusteringConfig.PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP);
  }

  public long getClusteringSmallFileLimit() {
    return getLong(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT);
  }

  public String getClusteringPartitionSelected() {
    return getString(HoodieClusteringConfig.PARTITION_SELECTED);
  }

  public String getClusteringPartitionFilterRegexPattern() {
    return getString(HoodieClusteringConfig.PARTITION_REGEX_PATTERN);
  }

  public int getClusteringMaxNumGroups() {
    return getInt(HoodieClusteringConfig.PLAN_STRATEGY_MAX_GROUPS);
  }

  public long getClusteringTargetFileMaxBytes() {
    return getLong(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES);
  }

  public int getTargetPartitionsForClustering() {
    return getInt(HoodieClusteringConfig.DAYBASED_LOOKBACK_PARTITIONS);
  }

  public int getSkipPartitionsFromLatestForClustering() {
    return getInt(HoodieClusteringConfig.PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST);
  }

  public boolean isSingleGroupClusteringEnabled() {
    return getBoolean(HoodieClusteringConfig.PLAN_STRATEGY_SINGLE_GROUP_CLUSTERING_ENABLED);
  }

  public boolean shouldClusteringSingleGroup() {
    return isClusteringSortEnabled() || isSingleGroupClusteringEnabled();
  }

  public String getClusteringSortColumns() {
    return getString(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS);
  }

  public boolean isClusteringSortEnabled() {
    return !StringUtils.isNullOrEmpty(getString(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS));
  }

  public HoodieClusteringConfig.LayoutOptimizationStrategy getLayoutOptimizationStrategy() {
    return HoodieClusteringConfig.resolveLayoutOptimizationStrategy(getStringOrDefault(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY));
  }

  public HoodieClusteringConfig.SpatialCurveCompositionStrategyType getLayoutOptimizationCurveBuildMethod() {
    return ConfigUtils.resolveEnum(HoodieClusteringConfig.SpatialCurveCompositionStrategyType.class,
        getString(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD));
  }

  public int getLayoutOptimizationSampleSize() {
    return getInt(HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE);
  }

  public int getClusteringGroupReadParallelism() {
    return getInt(HoodieClusteringConfig.CLUSTERING_GROUP_READ_PARALLELISM);
  }

  /**
   * index properties.
   */
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(getString(HoodieIndexConfig.INDEX_TYPE));
  }

  public String getBucketIndexPartitionExpression() {
    return getString(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS);
  }

  public String getBucketIndexPartitionRuleType() {
    return getString(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE);
  }

  public String getIndexClass() {
    return getString(HoodieIndexConfig.INDEX_CLASS_NAME);
  }

  public HoodieIndex.BucketIndexEngineType getBucketIndexEngineType() {
    return HoodieIndex.BucketIndexEngineType.valueOf(getString(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE));
  }

  public int getBloomFilterNumEntries() {
    return getInt(HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE);
  }

  public double getBloomFilterFPP() {
    return getDouble(HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE);
  }

  public String getBloomFilterType() {
    return getStorageConfig().getBloomFilterType();
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return getInt(HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES);
  }

  public int getBloomIndexParallelism() {
    return getInt(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM);
  }

  public boolean getBloomIndexPruneByRanges() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES);
  }

  public boolean getBloomIndexUseCaching() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_USE_CACHING);
  }

  public boolean getBloomIndexUseMetadata() {
    return getBooleanOrDefault(HoodieIndexConfig.BLOOM_INDEX_USE_METADATA);
  }

  public String getBloomIndexInputStorageLevel() {
    return getStringOrDefault(HoodieIndexConfig.BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE);
  }

  public boolean useBloomIndexTreebasedFilter() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_TREE_BASED_FILTER);
  }

  public boolean useBloomIndexBucketizedChecking() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING);
  }

  public boolean useBloomIndexBucketizedCheckingWithDynamicParallelism() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING_ENABLE_DYNAMIC_PARALLELISM);
  }

  public boolean isBloomIndexFileGroupIdKeySortingEnabled() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_FILE_GROUP_ID_KEY_SORTING);
  }

  /**
   * Determines if the metadata bloom filter index is enabled.
   *
   * <p>The bloom filter index is enabled if the metadata table is enabled and bloom filter index is enabled in the metadata configuration.
   *
   * <p>IMPORTANT: Make sure the logic is consistent with {@code MetadataPartitionType.isMetadataPartitionEnabled}
   * which is the only truth that defines whether the index is enabled(through table config {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS}).
   *
   * @return {@code true} if the metadata bloom filter index is enabled, {@code false} otherwise.
   */
  public boolean isMetadataBloomFilterIndexEnabled() {
    return isMetadataTableEnabled() && getMetadataConfig().isBloomFilterIndexEnabled();
  }

  /**
   * Determines if the metadata column stats index is enabled.
   *
   * <p>The column stats index is enabled if metadata table is enabled and column stats index is enabled in the metadata configuration.
   *
   * <p>IMPORTANT: Make sure the logic is consistent with {@code MetadataPartitionType.isMetadataPartitionEnabled}
   * which is the only truth that defines whether the index is enabled(through table config {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS}).
   *
   * @return {@code true} if the metadata column stats index is enabled, {@code false} otherwise.
   */
  public boolean isMetadataColumnStatsIndexEnabled() {
    return isMetadataTableEnabled() && getMetadataConfig().isColumnStatsIndexEnabled();
  }

  /**
   * Determines if the partition stats index is enabled.
   *
   * <p>The partition stats index is enabled if:
   * <ul>
   *   <li>The column stats is enabled. Partition stats cannot be created without column stats;</li>
   *   <li>The metadata table is enabled and partition stats index is enabled in the metadata configuration.</li>
   * </ul>
   *
   * <p>IMPORTANT: Make sure the logic is consistent with {@code MetadataPartitionType.isMetadataPartitionEnabled}
   * which is the only truth that defines whether the index is enabled(through table config {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS}).
   *
   * @return {@code true} if the partition stats index is enabled, {@code false} otherwise.
   */
  public boolean isPartitionStatsIndexEnabled() {
    return isMetadataColumnStatsIndexEnabled();
  }

  /**
   * Determines if the global record index is enabled.
   *
   * <p>The global record index is enabled if the record index is enabled in the metadata configuration.
   *
   * <p>IMPORTANT: Make sure the logic is consistent with {@code MetadataPartitionType.isMetadataPartitionEnabled}
   * which is the only truth that defines whether the index is enabled(through table config {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS}).
   *
   * @return {@code true} if the record index is enabled, {@code false} otherwise.
   */
  public boolean isGlobalRecordLevelIndexEnabled() {
    return metadataConfig.isGlobalRecordLevelIndexEnabled();
  }

  public boolean isRecordLevelIndexEnabled() {
    return metadataConfig.isRecordLevelIndexEnabled();
  }

  public int getPartitionStatsIndexParallelism() {
    return metadataConfig.getPartitionStatsIndexParallelism();
  }

  public List<String> getColumnsEnabledForBloomFilterIndex() {
    return getMetadataConfig().getColumnsEnabledForBloomFilterIndex();
  }

  public int getIndexingCheckTimeoutSeconds() {
    return getMetadataConfig().getIndexingCheckTimeoutSeconds();
  }

  public int getMetadataBloomFilterIndexParallelism() {
    return metadataConfig.getBloomFilterIndexParallelism();
  }

  public int getColumnStatsIndexParallelism() {
    return metadataConfig.getColumnStatsIndexParallelism();
  }

  public int getBloomIndexKeysPerBucket() {
    return getInt(HoodieIndexConfig.BLOOM_INDEX_KEYS_PER_BUCKET);
  }

  public boolean getGlobalBloomIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE);
  }

  public int getSimpleIndexParallelism() {
    return getInt(HoodieIndexConfig.SIMPLE_INDEX_PARALLELISM);
  }

  public String getSimpleIndexInputStorageLevel() {
    return getStringOrDefault(HoodieIndexConfig.SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE);
  }

  public boolean getSimpleIndexUseCaching() {
    return getBoolean(HoodieIndexConfig.SIMPLE_INDEX_USE_CACHING);
  }

  public int getGlobalSimpleIndexParallelism() {
    return getInt(HoodieIndexConfig.GLOBAL_SIMPLE_INDEX_PARALLELISM);
  }

  public boolean getGlobalSimpleIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE);
  }

  public int getGlobalIndexReconcileParallelism() {
    return getInt(HoodieIndexConfig.GLOBAL_INDEX_RECONCILE_PARALLELISM);
  }

  public int getBucketIndexNumBuckets() {
    return getIntOrDefault(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS);
  }

  public int getBucketIndexMaxNumBuckets() {
    return getInt(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS);
  }

  public int getBucketIndexMinNumBuckets() {
    return getInt(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS);
  }

  public double getBucketSplitThreshold() {
    return getDouble(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD);
  }

  public double getBucketMergeThreshold() {
    return getDouble(HoodieIndexConfig.BUCKET_MERGE_THRESHOLD);
  }

  public String getBucketIndexHashField() {
    return getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD);
  }

  public String getBucketIndexHashFieldWithDefault() {
    return getStringOrDefault(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD, getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME));
  }

  public boolean getRecordIndexUseCaching() {
    return getBoolean(HoodieIndexConfig.RECORD_INDEX_USE_CACHING);
  }

  public boolean getRecordIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE);
  }

  public String getRecordIndexInputStorageLevel() {
    return getStringOrDefault(HoodieIndexConfig.RECORD_INDEX_INPUT_STORAGE_LEVEL_VALUE);
  }

  public boolean isUsingRemotePartitioner() {
    return getBoolean(HoodieIndexConfig.BUCKET_PARTITIONER);
  }

  /**
   * storage properties.
   */
  public long getMaxFileSize(HoodieFileFormat format) {
    switch (format) {
      case PARQUET:
        return getParquetMaxFileSize();
      case HFILE:
        return getHFileMaxFileSize();
      case ORC:
        return getOrcMaxFileSize();
      default:
        throw new HoodieNotSupportedException("Unknown file format: " + format);
    }
  }

  public long getParquetMaxFileSize() {
    return getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE);
  }

  public int getParquetBlockSize() {
    return getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE);
  }

  public int getParquetPageSize() {
    return getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE);
  }

  public long getLogFileDataBlockMaxSize() {
    return getLong(HoodieStorageConfig.LOGFILE_DATA_BLOCK_MAX_SIZE);
  }

  public boolean shouldWriteRecordPositions() {
    return getBoolean(WRITE_RECORD_POSITIONS);
  }

  public boolean shouldWritePartialUpdates() {
    return !StringUtils.isNullOrEmpty(getString(WRITE_PARTIAL_UPDATE_SCHEMA));
  }

  public String getPartialUpdateSchema() {
    return getString(WRITE_PARTIAL_UPDATE_SCHEMA);
  }

  public boolean isIncrementalTableServiceEnabled() {
    return getBoolean(INCREMENTAL_TABLE_SERVICE_ENABLED);
  }

  public double getParquetCompressionRatio() {
    return getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION);
  }

  public String getParquetCompressionCodec() {
    return getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
  }

  public boolean parquetDictionaryEnabled() {
    return getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED);
  }

  public String parquetOutputTimestampType() {
    return getString(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE);
  }

  public String parquetFieldIdWriteEnabled() {
    return getString(HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED);
  }

  public boolean parquetBloomFilterEnabled() {
    return getBooleanOrDefault(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED);
  }

  public Option<HoodieLogBlock.HoodieLogBlockType> getLogDataBlockFormat() {
    return Option.ofNullable(getString(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT))
        .map(HoodieLogBlock.HoodieLogBlockType::fromId);
  }

  public long getLogFileMaxSize() {
    return getLong(HoodieStorageConfig.LOGFILE_MAX_SIZE);
  }

  public double getLogFileToParquetCompressionRatio() {
    return getDouble(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION);
  }

  public long getHFileMaxFileSize() {
    return getLong(HoodieStorageConfig.HFILE_MAX_FILE_SIZE);
  }

  public int getHFileBlockSize() {
    return getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE);
  }

  public String getHFileCompressionAlgorithm() {
    return getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME);
  }

  public long getOrcMaxFileSize() {
    return getLong(HoodieStorageConfig.ORC_FILE_MAX_SIZE);
  }

  public int getOrcStripeSize() {
    return getInt(HoodieStorageConfig.ORC_STRIPE_SIZE);
  }

  public int getOrcBlockSize() {
    return getInt(HoodieStorageConfig.ORC_BLOCK_SIZE);
  }

  public CompressionKind getOrcCompressionCodec() {
    return CompressionKind.valueOf(getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC_NAME));
  }

  /**
   * metrics properties.
   */
  public boolean isMetricsOn() {
    return metricsConfig.isMetricsOn();
  }

  /**
   * metrics properties.
   */
  public boolean isCompactionLogBlockMetricsOn() {
    return metricsConfig.isCompactionLogBlockMetricsOn();
  }

  public boolean isExecutorMetricsEnabled() {
    return metricsConfig.isExecutorMetricsEnabled();
  }

  public boolean isLockingMetricsEnabled() {
    return metricsConfig.isLockingMetricsEnabled();
  }

  public MetricsReporterType getMetricsReporterType() {
    return metricsConfig.getMetricsReporterType();
  }

  public String getGraphiteServerHost() {
    return metricsConfig.getGraphiteServerHost();
  }

  public int getGraphiteServerPort() {
    return metricsConfig.getGraphiteServerPort();
  }

  public String getGraphiteMetricPrefix() {
    return metricsConfig.getGraphiteMetricPrefix();
  }

  public int getGraphiteReportPeriodSeconds() {
    return metricsConfig.getGraphiteReportPeriodSeconds();
  }

  public String getM3ServerHost() {
    return metricsConfig.getM3ServerHost();
  }

  public int getM3ServerPort() {
    return metricsConfig.getM3ServerPort();
  }

  public String getM3Tags() {
    return metricsConfig.getM3Tags();
  }

  public String getM3Env() {
    return metricsConfig.getM3Env();
  }

  public String getM3Service() {
    return metricsConfig.getM3Service();
  }

  public String getJmxHost() {
    return metricsConfig.getJmxHost();
  }

  public String getJmxPort() {
    return metricsConfig.getJmxPort();
  }

  public int getDatadogReportPeriodSeconds() {
    return metricsConfig.getDatadogReportPeriodSeconds();
  }

  public ApiSite getDatadogApiSite() {
    return metricsConfig.getDatadogApiSite();
  }

  public String getDatadogApiKey() {
    return metricsConfig.getDatadogApiKey();
  }

  public boolean getDatadogApiKeySkipValidation() {
    return metricsConfig.getDatadogApiKeySkipValidation();
  }

  public int getDatadogApiTimeoutSeconds() {
    return metricsConfig.getDatadogApiTimeoutSeconds();
  }

  public String getDatadogMetricPrefix() {
    return metricsConfig.getDatadogMetricPrefix();
  }

  public String getDatadogMetricHost() {
    return metricsConfig.getDatadogMetricHost();
  }

  public List<String> getDatadogMetricTags() {
    return metricsConfig.getDatadogMetricTags();
  }

  public int getCloudWatchReportPeriodSeconds() {
    return metricsConfig.getCloudWatchReportPeriodSeconds();
  }

  public String getCloudWatchMetricPrefix() {
    return metricsConfig.getCloudWatchMetricPrefix();
  }

  public String getCloudWatchMetricNamespace() {
    return metricsConfig.getCloudWatchMetricNamespace();
  }

  public int getCloudWatchMaxDatumsPerRequest() {
    return metricsConfig.getCloudWatchMaxDatumsPerRequest();
  }

  public String getMetricReporterClassName() {
    return metricsConfig.getMetricReporterClassName();
  }

  public int getPrometheusPort() {
    return metricsConfig.getPrometheusPort();
  }

  public String getPushGatewayHost() {
    return metricsConfig.getPushGatewayHost();
  }

  public int getPushGatewayPort() {
    return metricsConfig.getPushGatewayPort();
  }

  public int getPushGatewayReportPeriodSeconds() {
    return metricsConfig.getPushGatewayReportPeriodSeconds();
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return metricsConfig.getPushGatewayDeleteOnShutdown();
  }

  public String getPushGatewayJobName() {
    return metricsConfig.getPushGatewayJobName();
  }

  public String getPushGatewayLabels() {
    return metricsConfig.getPushGatewayLabels();
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return metricsConfig.getPushGatewayRandomJobNameSuffix();
  }

  public String getMetricReporterMetricsNamePrefix() {
    return metricsConfig.getMetricReporterMetricsNamePrefix();
  }

  public String getMetricReporterFileBasedConfigs() {
    return metricsConfig.getMetricReporterFileBasedConfigs();
  }

  /**
   * memory configs.
   */
  public int getMaxDFSStreamBufferSize() {
    return getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE);
  }

  public String getSpillableMapBasePath() {
    return Option.ofNullable(getString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH))
        .orElseGet(FileIOUtils::getDefaultSpillableMapBasePath);
  }

  public double getWriteStatusFailureFraction() {
    return getDouble(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION);
  }

  public void resetViewStorageConfig() {
    this.setViewStorageConfig(getClientSpecifiedViewStorageConfig());
  }

  public HoodiePayloadConfig getPayloadConfig() {
    return hoodiePayloadConfig;
  }

  /**
   * Commit call back configs.
   */
  public boolean writeCommitCallbackOn() {
    return getBoolean(HoodieWriteCommitCallbackConfig.TURN_CALLBACK_ON);
  }

  public String getCallbackClass() {
    return getString(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME);
  }

  public String getBootstrapSourceBasePath() {
    return getString(HoodieBootstrapConfig.BASE_PATH);
  }

  public String getBootstrapModeSelectorClass() {
    return getString(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME);
  }

  public String getFullBootstrapInputProvider() {
    return getString(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME);
  }

  public String getBootstrapModeSelectorRegex() {
    return getString(HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_PATTERN);
  }

  public BootstrapMode getBootstrapModeForRegexMatch() {
    return BootstrapMode.valueOf(getString(HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_MODE));
  }

  public String getBootstrapPartitionPathTranslatorClass() {
    return getString(HoodieBootstrapConfig.PARTITION_PATH_TRANSLATOR_CLASS_NAME);
  }

  public int getBootstrapParallelism() {
    return getInt(HoodieBootstrapConfig.PARALLELISM_VALUE);
  }

  public Long getMaxMemoryPerPartitionMerge() {
    return getLong(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE);
  }

  public Long getHoodieClientHeartbeatIntervalInMs() {
    return getLong(CLIENT_HEARTBEAT_INTERVAL_IN_MS);
  }

  public Integer getHoodieClientHeartbeatTolerableMisses() {
    return getInt(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES);
  }

  /**
   * File listing metadata configs.
   */
  public boolean isMetadataTableEnabled() {
    return getBooleanOrDefault(HoodieMetadataConfig.ENABLE);
  }

  public int getMetadataCompactDeltaCommitMax() {
    return getInt(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS);
  }

  public String getMetadataCompactionTriggerStrategy() {
    return getString(HoodieMetadataConfig.COMPACT_TRIGGER_STRATEGY);
  }

  public int getMetadataMaxDeltaSecondsBeforeCompaction() {
    return getInt(HoodieMetadataConfig.COMPACT_TIME_DELTA_SECONDS);
  }

  public boolean isMetadataAsyncIndex() {
    return getBooleanOrDefault(HoodieMetadataConfig.ASYNC_INDEX_ENABLE);
  }

  public int getMetadataLogCompactBlocksThreshold() {
    return getInt(HoodieMetadataConfig.LOG_COMPACT_BLOCKS_THRESHOLD);
  }

  public boolean isLogCompactionEnabledOnMetadata() {
    return getBoolean(HoodieMetadataConfig.ENABLE_LOG_COMPACTION_ON_METADATA_TABLE);
  }

  public int getGlobalRecordLevelIndexMinFileGroupCount() {
    return metadataConfig.getGlobalRecordLevelIndexMinFileGroupCount();
  }

  public int getGlobalRecordLevelIndexMaxFileGroupCount() {
    return metadataConfig.getGlobalRecordLevelIndexMaxFileGroupCount();
  }

  public int getRecordLevelIndexMinFileGroupCount() {
    return metadataConfig.getRecordLevelIndexMinFileGroupCount();
  }

  public int getRecordLevelIndexMaxFileGroupCount() {
    return metadataConfig.getRecordLevelIndexMaxFileGroupCount();
  }

  public float getRecordIndexGrowthFactor() {
    return metadataConfig.getRecordIndexGrowthFactor();
  }

  public long getRecordIndexMaxFileGroupSizeBytes() {
    return metadataConfig.getRecordIndexMaxFileGroupSizeBytes();
  }

  /**
   * Hoodie Client Lock Configs.
   *
   * @return
   */
  public boolean isAutoAdjustLockConfigs() {
    return getBooleanOrDefault(AUTO_ADJUST_LOCK_CONFIGS);
  }

  public String getLockProviderClass() {
    return getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME);
  }

  public String getLockHiveDatabaseName() {
    return getString(HoodieLockConfig.HIVE_DATABASE_NAME);
  }

  public String getLockHiveTableName() {
    return getString(HoodieLockConfig.HIVE_TABLE_NAME);
  }

  public ConflictResolutionStrategy getWriteConflictResolutionStrategy() {
    return ReflectionUtils.loadClass(getString(HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME));
  }

  public Long getAsyncConflictDetectorInitialDelayMs() {
    return getLong(ASYNC_CONFLICT_DETECTOR_INITIAL_DELAY_MS);
  }

  public Long getAsyncConflictDetectorPeriodMs() {
    return getLong(ASYNC_CONFLICT_DETECTOR_PERIOD_MS);
  }

  public Long getLockAcquireWaitTimeoutInMs() {
    return getLong(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS);
  }

  public WriteConcurrencyMode getWriteConcurrencyMode() {
    return WriteConcurrencyMode.valueOf(getStringOrDefault(WRITE_CONCURRENCY_MODE).toUpperCase());
  }

  public boolean isEarlyConflictDetectionEnable() {
    return getBoolean(EARLY_CONFLICT_DETECTION_ENABLE);
  }

  public String getEarlyConflictDetectionStrategyClassName() {
    return getString(EARLY_CONFLICT_DETECTION_STRATEGY_CLASS_NAME);
  }

  public boolean earlyConflictDetectionCheckCommitConflict() {
    return getBoolean(EARLY_CONFLICT_DETECTION_CHECK_COMMIT_CONFLICT);
  }

  // misc configs
  public Boolean doSkipDefaultPartitionValidation() {
    return getBoolean(SKIP_DEFAULT_PARTITION_VALIDATION);
  }

  /**
   * Are any table services configured to run inline for both scheduling and execution?
   *
   * @return True if any table services are configured to run inline, false otherwise.
   */
  public Boolean areAnyTableServicesExecutedInline() {
    return areTableServicesEnabled()
        && (inlineClusteringEnabled() || inlineCompactionEnabled() || inlineLogCompactionEnabled()
        || (isAutoClean() && !isAsyncClean()) || (isAutoArchive() && !isAsyncArchive()));
  }

  /**
   * Are any table services configured to run async?
   *
   * @return True if any table services are configured to run async, false otherwise.
   */
  public Boolean areAnyTableServicesAsync() {
    return areTableServicesEnabled()
        && (isAsyncClusteringEnabled()
        || (getTableType() == HoodieTableType.MERGE_ON_READ && !inlineCompactionEnabled())
        || (isAutoClean() && isAsyncClean()) || (isAutoArchive() && isAsyncArchive()));
  }

  public Boolean areAnyTableServicesScheduledInline() {
    return scheduleInlineCompaction() || scheduleInlineClustering();
  }

  public String getPreCommitValidators() {
    return getString(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES);
  }

  public String getPreCommitValidatorEqualitySqlQueries() {
    return getString(HoodiePreCommitValidatorConfig.EQUALITY_SQL_QUERIES);
  }

  public String getPreCommitValidatorSingleResultSqlQueries() {
    return getString(HoodiePreCommitValidatorConfig.SINGLE_VALUE_SQL_QUERIES);
  }

  public String getPreCommitValidatorInequalitySqlQueries() {
    return getString(HoodiePreCommitValidatorConfig.INEQUALITY_SQL_QUERIES);
  }

  public boolean allowEmptyCommit() {
    return getBooleanOrDefault(ALLOW_EMPTY_COMMIT);
  }

  public boolean allowOperationMetadataField() {
    return getBooleanOrDefault(ALLOW_OPERATION_METADATA_FIELD);
  }

  public String getFileIdPrefixProviderClassName() {
    return getString(FILEID_PREFIX_PROVIDER_CLASS);
  }

  public boolean areTableServicesEnabled() {
    return getBooleanOrDefault(TABLE_SERVICES_ENABLED);
  }

  public boolean areReleaseResourceEnabled() {
    return getBooleanOrDefault(RELEASE_RESOURCE_ENABLE);
  }

  /**
   * Returns whether the explicit guard of lock is required.
   */
  public boolean isLockRequired() {
    return isLockProviderSet() || getWriteConcurrencyMode().supportsMultiWriter();
  }

  /**
   * Returns whether the lock provider is default.
   */
  private boolean isLockProviderSet() {
    return getLockProviderClass() != null;
  }

  /**
   * Layout configs.
   */
  public HoodieStorageLayout.LayoutType getLayoutType() {
    return HoodieStorageLayout.LayoutType.valueOf(getString(HoodieLayoutConfig.LAYOUT_TYPE));
  }

  /**
   * Metaserver configs.
   */
  public boolean isMetaserverEnabled() {
    return metaserverConfig.isMetaserverEnabled();
  }

  public boolean shouldBackupRollbacks() {
    return getBoolean(ROLLBACK_INSTANT_BACKUP_ENABLED);
  }

  public String getRollbackBackupDirectory() {
    return getString(ROLLBACK_INSTANT_BACKUP_DIRECTORY);
  }

  public String getClientInitCallbackClassNames() {
    return getString(CLIENT_INIT_CALLBACK_CLASS_NAMES);
  }

  public Integer getWritesFileIdEncoding() {
    return props.getInteger(WRITES_FILEID_ENCODING, HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_ENCODING_UUID);
  }

  public boolean needResolveWriteConflict(WriteOperationType operationType, boolean isMetadataTable, HoodieWriteConfig config,
                                          HoodieTableConfig tableConfig) {
    WriteConcurrencyMode mode = getWriteConcurrencyMode();
    switch (mode) {
      case SINGLE_WRITER:
        return false;
      case OPTIMISTIC_CONCURRENCY_CONTROL:
        return true;
      case NON_BLOCKING_CONCURRENCY_CONTROL: {
        if (isMetadataTable) {
          // datatable NB-CC is still evolving and might go through evolution compared to its current state.
          // But in case of metadata table, when streaming writes are enabled, no two writes can conflict and hence.
          return false;
        } else {
          // NB-CC don't need to resolve write conflict except bulk insert operation
          return WriteOperationType.BULK_INSERT == operationType;
        }
      }
      default:
        throw new IllegalArgumentException("Invalid WriteConcurrencyMode " + mode);
    }
  }

  public boolean isNonBlockingConcurrencyControl() {
    return getWriteConcurrencyMode().isNonBlockingConcurrencyControl();
  }

  /**
   * TTL configs.
   */
  public boolean isInlinePartitionTTLEnable() {
    return getBoolean(HoodieTTLConfig.INLINE_PARTITION_TTL);
  }

  public String getPartitionTTLStrategyClassName() {
    return getString(HoodieTTLConfig.PARTITION_TTL_STRATEGY_CLASS_NAME);
  }

  public Integer getPartitionTTLStrategyDaysRetain() {
    return getInt(HoodieTTLConfig.DAYS_RETAIN);
  }

  public String getPartitionTTLPartitionSelected() {
    return getString(HoodieTTLConfig.PARTITION_SELECTED);
  }

  public Integer getPartitionTTLMaxPartitionsToDelete() {
    return getInt(HoodieTTLConfig.MAX_PARTITION_TO_DELETE);
  }

  public boolean isSecondaryIndexEnabled() {
    return metadataConfig.isSecondaryIndexEnabled();
  }

  public boolean isExpressionIndexEnabled() {
    return metadataConfig.isExpressionIndexEnabled();
  }

  public int getSecondaryIndexParallelism() {
    return metadataConfig.getSecondaryIndexParallelism();
  }

  /**
   * Whether to enable streaming writes to metadata table or not.
   * We have support for streaming writes only in SPARK engine (due to spark task retries intricacies) and for table version >= 8 due to the
   * pre-requisite of NBCC.
   *
   * <p>To support streaming writes, we need NBCC support for metadata table, since there could have an ingestion and a table service from data table
   * concurrently trying to write to metadata table.
   *
   * <p>In Spark, when streaming writes are enabled, incremental operations from data table like insert, upsert, delete and table services
   * (compaction and clustering) will take the streaming writes flow, while all other operations (like delete_partition, insert_overwrite, etc.) go through
   * legacy metadata write paths (since these might involve reading entire partition and not purely rely on incremental data written).
   *
   * @param tableVersion {@link HoodieTableVersion} of interest.
   * @return true if streaming writes are enabled. false otherwise.
   */
  public boolean isMetadataStreamingWritesEnabled(HoodieTableVersion tableVersion) {
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      return metadataConfig.isStreamingWriteEnabled();
    } else {
      return false;
    }
  }

  public static class Builder {

    protected final HoodieWriteConfig writeConfig = new HoodieWriteConfig();
    protected EngineType engineType = EngineType.SPARK;
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isCleanConfigSet = false;
    private boolean isArchivalConfigSet = false;
    private boolean isClusteringConfigSet = false;
    private final boolean isOptimizeConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isBootstrapConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;
    private boolean isCallbackConfigSet = false;
    private boolean isPayloadConfigSet = false;
    private boolean isMetadataConfigSet = false;

    private boolean isTTLConfigSet = false;
    private boolean isLockConfigSet = false;
    private boolean isPreCommitValidationConfigSet = false;
    private boolean isMetricsJmxConfigSet = false;
    private boolean isMetricsGraphiteConfigSet = false;
    private boolean isMetricsM3ConfigSet = false;
    private boolean isLayoutConfigSet = false;

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.writeConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromInputStream(InputStream inputStream) throws IOException {
      try {
        this.writeConfig.getProps().load(inputStream);
        return this;
      } finally {
        inputStream.close();
      }
    }

    public Builder withProps(Map kvprops) {
      writeConfig.getProps().putAll(kvprops);
      return this;
    }

    public Builder withPath(String basePath) {
      writeConfig.setValue(BASE_PATH, basePath);
      return this;
    }

    public Builder withPath(StoragePath basePath) {
      writeConfig.setValue(BASE_PATH, basePath.toString());
      return this;
    }

    public Builder withBaseFileFormat(String baseFileFormat) {
      writeConfig.setValue(BASE_FILE_FORMAT, HoodieFileFormat.valueOf(baseFileFormat).name());
      return this;
    }

    public Builder withSchema(String schemaStr) {
      writeConfig.setValue(AVRO_SCHEMA_STRING, schemaStr);
      return this;
    }

    public Builder withSchemaEvolutionEnable(boolean enable) {
      writeConfig.setValue(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withWriteTableVersion(int writeVersion) {
      HoodieTableVersion tableVersion = HoodieTableVersion.fromVersionCode(writeVersion);
      writeConfig.setValue(WRITE_TABLE_VERSION, String.valueOf(tableVersion.versionCode()));
      return withTimelineLayoutVersion(tableVersion.getTimelineLayoutVersion().getVersion());
    }

    public Builder withAutoUpgradeVersion(boolean enable) {
      writeConfig.setValue(AUTO_UPGRADE_VERSION, String.valueOf(enable));
      return this;
    }

    public Builder withAvroSchemaValidate(boolean enable) {
      writeConfig.setValue(AVRO_SCHEMA_VALIDATE_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withAllowAutoEvolutionColumnDrop(boolean shouldAllowDroppedColumns) {
      writeConfig.setValue(SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP, String.valueOf(shouldAllowDroppedColumns));
      return this;
    }

    public Builder forTable(String tableName) {
      writeConfig.setValue(TBL_NAME, tableName);
      return this;
    }

    @Deprecated
    public Builder withPreCombineField(String preCombineField) {
      writeConfig.setValue(PRECOMBINE_FIELD_NAME, preCombineField);
      return this;
    }

    public Builder withWritePayLoad(String payload) {
      writeConfig.setValue(WRITE_PAYLOAD_CLASS_NAME, payload);
      return this;
    }

    public Builder withRecordMergeImplClasses(String recordMergeImplClasses) {
      if (!StringUtils.isNullOrEmpty(recordMergeImplClasses)) {
        writeConfig.setValue(RECORD_MERGE_IMPL_CLASSES, recordMergeImplClasses);
      }
      return this;
    }

    public Builder withRecordMergeStrategyId(String recordMergeStrategyId) {
      writeConfig.setValue(RECORD_MERGE_STRATEGY_ID, recordMergeStrategyId);
      return this;
    }

    public Builder withKeyGenerator(String keyGeneratorClass) {
      writeConfig.setValue(KEYGENERATOR_CLASS_NAME, keyGeneratorClass);
      return this;
    }

    public Builder withRecordSizeEstimator(String recordSizeEstimator) {
      writeConfig.setValue(RECORD_SIZE_ESTIMATOR_CLASS_NAME, recordSizeEstimator);
      return this;
    }

    public Builder withRecordSizeEstimatorMaxCommits(int maxCommits) {
      writeConfig.setValue(RECORD_SIZE_ESTIMATOR_MAX_COMMITS, String.valueOf(maxCommits));
      return this;
    }

    public Builder withRecordSizeEstimatorAverageMetadataSize(long avgMetadataSize) {
      writeConfig.setValue(RECORD_SIZE_ESTIMATOR_AVERAGE_METADATA_SIZE, String.valueOf(avgMetadataSize));
      return this;
    }

    public Builder withExecutorType(String executorClass) {
      writeConfig.setValue(WRITE_EXECUTOR_TYPE, executorClass);
      return this;
    }

    public Builder withTimelineLayoutVersion(int version) {
      writeConfig.setValue(TIMELINE_LAYOUT_VERSION_NUM, String.valueOf(version));
      return this;
    }

    public Builder withBulkInsertParallelism(int bulkInsertParallelism) {
      writeConfig.setValue(BULKINSERT_PARALLELISM_VALUE, String.valueOf(bulkInsertParallelism));
      return this;
    }

    public Builder withUserDefinedBulkInsertPartitionerClass(String className) {
      writeConfig.setValue(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME, className);
      return this;
    }

    public Builder withUserDefinedBulkInsertPartitionerSortColumns(String columns) {
      writeConfig.setValue(BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS, columns);
      return this;
    }

    public Builder withDeleteParallelism(int parallelism) {
      writeConfig.setValue(DELETE_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withFailureOnInlineTableServiceException(boolean fail) {
      writeConfig.setValue(FAIL_ON_INLINE_TABLE_SERVICE_EXCEPTION, String.valueOf(fail));
      return this;
    }

    public Builder withParallelism(int insertShuffleParallelism, int upsertShuffleParallelism) {
      writeConfig.setValue(INSERT_PARALLELISM_VALUE, String.valueOf(insertShuffleParallelism));
      writeConfig.setValue(UPSERT_PARALLELISM_VALUE, String.valueOf(upsertShuffleParallelism));
      return this;
    }

    public Builder withRollbackParallelism(int rollbackParallelism) {
      writeConfig.setValue(ROLLBACK_PARALLELISM_VALUE, String.valueOf(rollbackParallelism));
      return this;
    }

    public Builder withRollbackUsingMarkers(boolean rollbackUsingMarkers) {
      writeConfig.setValue(ROLLBACK_USING_MARKERS_ENABLE, String.valueOf(rollbackUsingMarkers));
      return this;
    }

    public Builder withComplexKeygenValidation(boolean enableComplexKeygenValidation) {
      writeConfig.setValue(ENABLE_COMPLEX_KEYGEN_VALIDATION, String.valueOf(enableComplexKeygenValidation));
      return this;
    }

    public Builder withWriteBufferLimitBytes(int writeBufferLimit) {
      writeConfig.setValue(WRITE_BUFFER_LIMIT_BYTES_VALUE, String.valueOf(writeBufferLimit));
      return this;
    }

    public Builder withWriteBufferRecordSamplingRate(int recordSamplingRate) {
      writeConfig.setValue(WRITE_BUFFER_RECORD_SAMPLING_RATE, String.valueOf(recordSamplingRate));
      return this;
    }

    public Builder withWriteBufferRecordCacheLimit(int recordCacheLimit) {
      writeConfig.setValue(WRITE_BUFFER_RECORD_CACHE_LIMIT, String.valueOf(recordCacheLimit));
      return this;
    }

    public Builder withWriteExecutorDisruptorWaitStrategy(String waitStrategy) {
      writeConfig.setValue(WRITE_EXECUTOR_DISRUPTOR_WAIT_STRATEGY, String.valueOf(waitStrategy));
      return this;
    }

    public Builder withWriteExecutorDisruptorWriteBufferLimitBytes(long size) {
      writeConfig.setValue(WRITE_EXECUTOR_DISRUPTOR_BUFFER_LIMIT_BYTES, String.valueOf(size));
      return this;
    }

    public Builder combineInput(boolean onInsert, boolean onUpsert) {
      writeConfig.setValue(COMBINE_BEFORE_INSERT, String.valueOf(onInsert));
      writeConfig.setValue(COMBINE_BEFORE_UPSERT, String.valueOf(onUpsert));
      return this;
    }

    public Builder combineDeleteInput(boolean onDelete) {
      writeConfig.setValue(COMBINE_BEFORE_DELETE, String.valueOf(onDelete));
      return this;
    }

    public Builder withWriteStatusStorageLevel(String level) {
      writeConfig.setValue(WRITE_STATUS_STORAGE_LEVEL_VALUE, level);
      return this;
    }

    public Builder withIndexConfig(HoodieIndexConfig indexConfig) {
      writeConfig.getProps().putAll(indexConfig.getProps());
      isIndexConfigSet = true;
      return this;
    }

    public Builder withStorageConfig(HoodieStorageConfig storageConfig) {
      writeConfig.getProps().putAll(storageConfig.getProps());
      isStorageConfigSet = true;
      return this;
    }

    public Builder withCompactionConfig(HoodieCompactionConfig compactionConfig) {
      writeConfig.getProps().putAll(compactionConfig.getProps());
      isCompactionConfigSet = true;
      return this;
    }

    public Builder withCleanConfig(HoodieCleanConfig cleanConfig) {
      writeConfig.getProps().putAll(cleanConfig.getProps());
      isCleanConfigSet = true;
      return this;
    }

    public Builder withArchivalConfig(HoodieArchivalConfig cleanConfig) {
      writeConfig.getProps().putAll(cleanConfig.getProps());
      isArchivalConfigSet = true;
      return this;
    }

    public Builder withClusteringConfig(HoodieClusteringConfig clusteringConfig) {
      writeConfig.getProps().putAll(clusteringConfig.getProps());
      isClusteringConfigSet = true;
      return this;
    }

    public Builder withLockConfig(HoodieLockConfig lockConfig) {
      writeConfig.getProps().putAll(lockConfig.getProps());
      isLockConfigSet = true;
      return this;
    }

    public Builder withMetricsJmxConfig(HoodieMetricsJmxConfig metricsJmxConfig) {
      writeConfig.getProps().putAll(metricsJmxConfig.getProps());
      isMetricsJmxConfigSet = true;
      return this;
    }

    public Builder withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig mericsGraphiteConfig) {
      writeConfig.getProps().putAll(mericsGraphiteConfig.getProps());
      isMetricsGraphiteConfigSet = true;
      return this;
    }

    public Builder withMetricsM3Config(HoodieMetricsM3Config metricsM3Config) {
      writeConfig.getProps().putAll(metricsM3Config.getProps());
      isMetricsM3ConfigSet = true;
      return this;
    }

    public Builder withPreCommitValidatorConfig(HoodiePreCommitValidatorConfig validatorConfig) {
      writeConfig.getProps().putAll(validatorConfig.getProps());
      isPreCommitValidationConfigSet = true;
      return this;
    }

    public Builder withMetricsConfig(HoodieMetricsConfig metricsConfig) {
      writeConfig.getProps().putAll(metricsConfig.getProps());
      isMetricsConfigSet = true;
      return this;
    }

    public Builder withMemoryConfig(HoodieMemoryConfig memoryConfig) {
      writeConfig.getProps().putAll(memoryConfig.getProps());
      isMemoryConfigSet = true;
      return this;
    }

    public Builder withBootstrapConfig(HoodieBootstrapConfig bootstrapConfig) {
      writeConfig.getProps().putAll(bootstrapConfig.getProps());
      isBootstrapConfigSet = true;
      return this;
    }

    public Builder withPayloadConfig(HoodiePayloadConfig payloadConfig) {
      writeConfig.getProps().putAll(payloadConfig.getProps());
      isPayloadConfigSet = true;
      return this;
    }

    public Builder withRecordMergeMode(RecordMergeMode recordMergeMode) {
      writeConfig.setValue(RECORD_MERGE_MODE, recordMergeMode.name());
      return this;
    }

    public Builder withMetadataConfig(HoodieMetadataConfig metadataConfig) {
      writeConfig.getProps().putAll(metadataConfig.getProps());
      isMetadataConfigSet = true;
      return this;
    }

    public Builder withTTLConfig(HoodieTTLConfig ttlConfig) {
      writeConfig.getProps().putAll(ttlConfig.getProps());
      isTTLConfigSet = true;
      return this;
    }

    public Builder withWriteStatusClass(Class<? extends WriteStatus> writeStatusClass) {
      writeConfig.setValue(WRITE_STATUS_CLASS_NAME, writeStatusClass.getName());
      return this;
    }

    public Builder withFileSystemViewConfig(FileSystemViewStorageConfig viewStorageConfig) {
      writeConfig.getProps().putAll(viewStorageConfig.getProps());
      isViewConfigSet = true;
      return this;
    }

    public Builder withConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      writeConfig.getProps().putAll(consistencyGuardConfig.getProps());
      isConsistencyGuardSet = true;
      return this;
    }

    public Builder withCallbackConfig(HoodieWriteCommitCallbackConfig callbackConfig) {
      writeConfig.getProps().putAll(callbackConfig.getProps());
      isCallbackConfigSet = true;
      return this;
    }

    public Builder withLayoutConfig(HoodieLayoutConfig layoutConfig) {
      writeConfig.getProps().putAll(layoutConfig.getProps());
      isLayoutConfigSet = true;
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      writeConfig.setValue(FINALIZE_WRITE_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withMarkersType(String markerType) {
      writeConfig.setValue(MARKERS_TYPE, markerType);
      return this;
    }

    public Builder withMarkersTimelineServerBasedBatchNumThreads(int numThreads) {
      writeConfig.setValue(MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS, String.valueOf(numThreads));
      return this;
    }

    public Builder withMarkersTimelineServerBasedBatchIntervalMs(long intervalMs) {
      writeConfig.setValue(MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS, String.valueOf(intervalMs));
      return this;
    }

    public Builder withMarkersDeleteParallelism(int parallelism) {
      writeConfig.setValue(MARKERS_DELETE_PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withEmbeddedTimelineServerEnabled(boolean enabled) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withEmbeddedTimelineServerReuseEnabled(boolean enabled) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withEmbeddedTimelineServerPort(int port) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_PORT_NUM, String.valueOf(port));
      return this;
    }

    public Builder withBulkInsertSortMode(String mode) {
      writeConfig.setValue(BULK_INSERT_SORT_MODE, mode);
      return this;
    }

    public Builder withAllowMultiWriteOnSameInstant(boolean allow) {
      writeConfig.setValue(ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE, String.valueOf(allow));
      return this;
    }

    public Builder withHiveStylePartitioningEnabled(boolean enabled) {
      writeConfig.setValue(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withExternalSchemaTrasformation(boolean enabled) {
      writeConfig.setValue(AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withMergeDataValidationCheckEnabled(boolean enabled) {
      writeConfig.setValue(MERGE_DATA_VALIDATION_CHECK_ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withMergeAllowDuplicateOnInserts(boolean routeInsertsToNewFiles) {
      writeConfig.setValue(MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE, String.valueOf(routeInsertsToNewFiles));
      return this;
    }

    public Builder withMergeSmallFileGroupCandidatesLimit(int limit) {
      writeConfig.setValue(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT, String.valueOf(limit));
      return this;
    }

    public Builder withHeartbeatIntervalInMs(Integer heartbeatIntervalInMs) {
      writeConfig.setValue(CLIENT_HEARTBEAT_INTERVAL_IN_MS, String.valueOf(heartbeatIntervalInMs));
      return this;
    }

    public Builder withHeartbeatTolerableMisses(Integer heartbeatTolerableMisses) {
      writeConfig.setValue(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES, String.valueOf(heartbeatTolerableMisses));
      return this;
    }

    public Builder withWriteConcurrencyMode(WriteConcurrencyMode concurrencyMode) {
      writeConfig.setValue(WRITE_CONCURRENCY_MODE, concurrencyMode.name());
      return this;
    }

    public Builder withPopulateMetaFields(boolean populateMetaFields) {
      writeConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, Boolean.toString(populateMetaFields));
      return this;
    }

    public Builder withAllowOperationMetadataField(boolean allowOperationMetadataField) {
      writeConfig.setValue(ALLOW_OPERATION_METADATA_FIELD, Boolean.toString(allowOperationMetadataField));
      return this;
    }

    public Builder withFileIdPrefixProviderClassName(String fileIdPrefixProviderClassName) {
      writeConfig.setValue(FILEID_PREFIX_PROVIDER_CLASS, fileIdPrefixProviderClassName);
      return this;
    }

    public Builder withTableServicesEnabled(boolean enabled) {
      writeConfig.setValue(TABLE_SERVICES_ENABLED, Boolean.toString(enabled));
      return this;
    }

    public Builder withReleaseResourceEnabled(boolean enabled) {
      writeConfig.setValue(RELEASE_RESOURCE_ENABLE, Boolean.toString(enabled));
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.writeConfig.getProps().putAll(properties);
      return this;
    }

    public Builder withAutoAdjustLockConfigs(boolean autoAdjustLockConfigs) {
      writeConfig.setValue(AUTO_ADJUST_LOCK_CONFIGS, String.valueOf(autoAdjustLockConfigs));
      return this;
    }

    public Builder doSkipDefaultPartitionValidation(boolean skipDefaultPartitionValidation) {
      writeConfig.setValue(SKIP_DEFAULT_PARTITION_VALIDATION, String.valueOf(skipDefaultPartitionValidation));
      return this;
    }

    public Builder withEarlyConflictDetectionEnable(boolean enable) {
      writeConfig.setValue(EARLY_CONFLICT_DETECTION_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withAsyncConflictDetectorInitialDelayMs(long intervalMs) {
      writeConfig.setValue(ASYNC_CONFLICT_DETECTOR_INITIAL_DELAY_MS, String.valueOf(intervalMs));
      return this;
    }

    public Builder withAsyncConflictDetectorPeriodMs(long periodMs) {
      writeConfig.setValue(ASYNC_CONFLICT_DETECTOR_PERIOD_MS, String.valueOf(periodMs));
      return this;
    }

    public Builder withEarlyConflictDetectionCheckCommitConflict(boolean enable) {
      writeConfig.setValue(EARLY_CONFLICT_DETECTION_CHECK_COMMIT_CONFLICT, String.valueOf(enable));
      return this;
    }

    public Builder withEarlyConflictDetectionStrategy(String className) {
      writeConfig.setValue(EARLY_CONFLICT_DETECTION_STRATEGY_CLASS_NAME, className);
      return this;
    }

    public Builder withRollbackBackupEnabled(boolean rollbackBackupEnabled) {
      writeConfig.setValue(ROLLBACK_INSTANT_BACKUP_ENABLED, String.valueOf(rollbackBackupEnabled));
      return this;
    }

    public Builder withRollbackBackupDirectory(String backupDir) {
      writeConfig.setValue(ROLLBACK_INSTANT_BACKUP_DIRECTORY, backupDir);
      return this;
    }

    public Builder withClientInitCallbackClassNames(String classNames) {
      writeConfig.setValue(CLIENT_INIT_CALLBACK_CLASS_NAMES, classNames);
      return this;
    }

    public Builder withWritesFileIdEncoding(Integer fileIdEncoding) {
      writeConfig.setValue(WRITES_FILEID_ENCODING, Integer.toString(fileIdEncoding));
      return this;
    }

    public Builder withWriteRecordPositionsEnabled(boolean shouldWriteRecordPositions) {
      writeConfig.setValue(WRITE_RECORD_POSITIONS, String.valueOf(shouldWriteRecordPositions));
      return this;
    }

    public Builder withIncrementalTableServiceEnabled(boolean incrementalTableServiceEnabled) {
      writeConfig.setValue(INCREMENTAL_TABLE_SERVICE_ENABLED, String.valueOf(incrementalTableServiceEnabled));
      return this;
    }

    public Builder withMergeHandleClassName(String className) {
      writeConfig.setValue(MERGE_HANDLE_CLASS_NAME, className);
      return this;
    }

    public Builder withConcatHandleClassName(String className) {
      writeConfig.setValue(CONCAT_HANDLE_CLASS_NAME, className);
      return this;
    }

    public Builder withFileGroupReaderMergeHandleClassName(String className) {
      writeConfig.setValue(COMPACT_MERGE_HANDLE_CLASS_NAME, className);
      return this;
    }

    protected void setDefaults() {
      writeConfig.setDefaultValue(MARKERS_TYPE, getDefaultMarkersType(engineType));
      // Check for mandatory properties
      writeConfig.setDefaults(HoodieWriteConfig.class.getName());
      // Make sure the props is propagated
      writeConfig.setDefaultOnCondition(
          !isIndexConfigSet, HoodieIndexConfig.newBuilder().withEngineType(engineType).fromProperties(
              writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isStorageConfigSet, HoodieStorageConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isCleanConfigSet,
          HoodieCleanConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isArchivalConfigSet,
          HoodieArchivalConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isClusteringConfigSet,
          HoodieClusteringConfig.newBuilder().withEngineType(engineType)
              .fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMetricsConfigSet, HoodieMetricsConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isBootstrapConfigSet,
          HoodieBootstrapConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMemoryConfigSet, HoodieMemoryConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isViewConfigSet,
          FileSystemViewStorageConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isConsistencyGuardSet,
          ConsistencyGuardConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isCallbackConfigSet,
          HoodieWriteCommitCallbackConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isPayloadConfigSet,
          HoodiePayloadConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMetadataConfigSet,
          HoodieMetadataConfig.newBuilder().withEngineType(engineType).fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isPreCommitValidationConfigSet,
          HoodiePreCommitValidatorConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isLayoutConfigSet,
          HoodieLayoutConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultValue(TIMELINE_LAYOUT_VERSION_NUM, String.valueOf(TimelineLayoutVersion.CURR_VERSION));

      // isLockProviderPropertySet must be fetched before setting defaults of HoodieLockConfig
      final TypedProperties writeConfigProperties = writeConfig.getProps();
      final boolean isLockProviderPropertySet = writeConfigProperties.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key());
      writeConfig.setDefaultOnCondition(!isLockConfigSet,
          HoodieLockConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isTTLConfigSet,
          HoodieTTLConfig.newBuilder().fromProperties(writeConfig.getProps()).build());

      autoAdjustConfigsForConcurrencyMode(isLockProviderPropertySet);
    }

    private boolean isLockRequiredForSingleWriter() {
      // When metadata table is enabled, lock provider must be used for
      // single writer with async table services.
      // Async table services can update the metadata table and a lock provider is
      // needed to guard against any concurrent table write operations. If user has
      // not configured any lock provider, let's use the InProcess lock provider.
      return writeConfig.isMetadataTableEnabled() && writeConfig.areAnyTableServicesAsync()
          && !writeConfig.getWriteConcurrencyMode().supportsMultiWriter();
    }

    private void autoAdjustConfigsForConcurrencyMode(boolean isLockProviderPropertySet) {
      // for a single writer scenario, with all table services inline, lets set InProcessLockProvider
      if (writeConfig.isAutoAdjustLockConfigs() && writeConfig.getWriteConcurrencyMode() == WriteConcurrencyMode.SINGLE_WRITER && !writeConfig.areAnyTableServicesAsync()) {
        if (writeConfig.getLockProviderClass() != null && !writeConfig.getLockProviderClass().equals(InProcessLockProvider.class.getCanonicalName())) {
          // add logs only when explicitly overridden by the user.
          log.warn("For a single writer mode, overriding lock provider class ({}) to {}. So, user configured lock provider {} may not take effect",
              HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName(), writeConfig.getLockProviderClass());
          writeConfig.setValue(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
              InProcessLockProvider.class.getName());
        }
      }

      if (!isLockProviderPropertySet && writeConfig.isAutoAdjustLockConfigs() && isLockRequiredForSingleWriter()) {
        // auto adjustment is required only for deltastreamer and spark streaming where async table services can be executed in the same JVM.
        // This is targeted at Single writer with async table services
        // If user does not set the lock provider, likely that the concurrency mode is not set either
        // Override the configs for metadata table
        writeConfig.setValue(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
            InProcessLockProvider.class.getName());
        log.info("Automatically set {}={} since user has not set the "
                + "lock provider for single writer with async table services",
            HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName());
      }

      // We check if "hoodie.clean.failed.writes.policy"
      // is properly set to LAZY for multi-writers
      WriteConcurrencyMode writeConcurrencyMode = writeConfig.getWriteConcurrencyMode();
      if (writeConcurrencyMode.supportsMultiWriter()) {
        // In this case, we assume that the user takes care of setting the lock provider used
        writeConfig.setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
            HoodieFailedWritesCleaningPolicy.LAZY.name());
        log.info("Automatically set {}={} since {} is used",
            HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
            HoodieFailedWritesCleaningPolicy.LAZY.name(),
            writeConcurrencyMode.name());
      }
    }

    private void validate() {
      String layoutVersion = writeConfig.getString(TIMELINE_LAYOUT_VERSION_NUM);
      // Ensure Layout Version is good
      new TimelineLayoutVersion(Integer.parseInt(layoutVersion));
      Objects.requireNonNull(writeConfig.getString(BASE_PATH));
      WriteConcurrencyMode writeConcurrencyMode = writeConfig.getWriteConcurrencyMode();
      if (writeConfig.isEarlyConflictDetectionEnable()) {
        checkArgument(writeConcurrencyMode.isOptimisticConcurrencyControl(),
            "To use early conflict detection, set hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL");
      }
      if (writeConcurrencyMode.supportsMultiWriter()) {
        checkArgument(!writeConfig.getString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY)
            .equals(HoodieFailedWritesCleaningPolicy.EAGER.name()),
            String.format(
                "To enable %s, set hoodie.clean.failed.writes.policy=LAZY",
                writeConcurrencyMode.name()));
      }
      if (writeConcurrencyMode == WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL) {
        boolean isMetadataTable = HoodieTableMetadata.isMetadataTable(writeConfig.getBasePath());
        checkArgument(
            writeConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ) && (isMetadataTable || writeConfig.isSimpleBucketIndex()),
            "Non-blocking concurrency control requires the MOR table with simple bucket index or it has to be Metadata table");
      }

      HoodieCleaningPolicy cleaningPolicy = HoodieCleaningPolicy.valueOf(writeConfig.getString(CLEANER_POLICY));
      if (cleaningPolicy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
        // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
        // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
        int minInstantsToKeep = Integer.parseInt(writeConfig.getStringOrDefault(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP));
        int maxInstantsToKeep = Integer.parseInt(writeConfig.getStringOrDefault(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP));
        int cleanerCommitsRetained =
            Integer.parseInt(writeConfig.getStringOrDefault(HoodieCleanConfig.CLEANER_COMMITS_RETAINED));
        checkArgument(maxInstantsToKeep > minInstantsToKeep,
            String.format(
                "Increase %s=%d to be greater than %s=%d.",
                HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), maxInstantsToKeep,
                HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep));
        if (minInstantsToKeep <= cleanerCommitsRetained) {
          log.warn("Increase {}={} to be greater than {}={} (there is risk of incremental pull "
                  + "missing data from few instants based on the current configuration). "
                  + "The Hudi archiver will automatically adjust the configuration regardless.",
              HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep,
              HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), cleanerCommitsRetained);
        }
      }

      boolean inlineCompact = writeConfig.getBoolean(HoodieCompactionConfig.INLINE_COMPACT);
      boolean inlineCompactSchedule = writeConfig.getBoolean(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT);
      checkArgument(!(inlineCompact && inlineCompactSchedule), String.format("Either of inline compaction (%s) or "
              + "schedule inline compaction (%s) can be enabled. Both can't be set to true at the same time. %s, %s", HoodieCompactionConfig.INLINE_COMPACT.key(),
          HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), inlineCompact, inlineCompactSchedule));
    }

    public HoodieWriteConfig build() {
      return build(true);
    }

    @VisibleForTesting
    public HoodieWriteConfig build(boolean shouldValidate) {
      setDefaults();
      if (shouldValidate) {
        validate();
      }
      // Build WriteConfig at the end
      return new HoodieWriteConfig(engineType, writeConfig.getProps());
    }

    private String getDefaultMarkersType(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          if (writeConfig.isEmbeddedTimelineServerEnabled()) {
            return MarkerType.TIMELINE_SERVER_BASED.toString();
          } else {
            if (!HoodieTableMetadata.isMetadataTable(writeConfig.getBasePath())) {
              log.warn("Embedded timeline server is disabled, fallback to use direct marker type for spark");
            }
            return MarkerType.DIRECT.toString();
          }
        case FLINK:
        case JAVA:
          // Timeline-server-based marker is not supported for Flink and Java engines
          return MarkerType.DIRECT.toString();
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }

  public boolean isFileGroupReaderBasedMergeHandle() {
    return isFileGroupReaderBasedMergeHandle(props);
  }

  public static boolean isFileGroupReaderBasedMergeHandle(TypedProperties props) {
    return ReflectionUtils.isSubClass(ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME, true), FileGroupReaderBasedMergeHandle.class);
  }
}
