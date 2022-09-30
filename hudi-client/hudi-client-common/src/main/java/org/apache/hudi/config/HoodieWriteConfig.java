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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieMetastoreConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.metrics.HoodieMetricsCloudWatchConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;
import org.apache.hudi.table.RandomFileIdPrefixProvider;
import org.apache.hudi.table.action.clean.CleaningTriggerStrategy;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.storage.HoodieStorageLayout;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_POLICY;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Immutable
@ConfigClassProperty(name = "Write Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control write behavior on Hudi tables. These can be directly passed down from even "
        + "higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g DeltaStreamer).")
public class HoodieWriteConfig extends HoodieConfig {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteConfig.class);
  private static final long serialVersionUID = 0L;

  // This is a constant as is should never be changed via config (will invalidate previous commits)
  // It is here so that both the client and deltastreamer use the same reference
  public static final String DELTASTREAMER_CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  public static final ConfigProperty<String> TBL_NAME = ConfigProperty
      .key(HoodieTableConfig.HOODIE_TABLE_NAME_KEY)
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with metastores like HMS. Needs to be same across runs.");

  public static final ConfigProperty<String> PRECOMBINE_FIELD_NAME = ConfigProperty
      .key("hoodie.datasource.write.precombine.field")
      .defaultValue("ts")
      .withDocumentation("Field used in preCombining before actual write. When two records have the same key value, "
          + "we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)");

  public static final ConfigProperty<String> WRITE_PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.datasource.write.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. "
          + "This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective");

  public static final ConfigProperty<String> KEYGENERATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.class")
      .noDefaultValue()
      .withDocumentation("Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` "
          + "extract a key out of incoming records.");

  public static final ConfigProperty<String> KEYGENERATOR_TYPE = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.type")
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .withDocumentation("Easily configure one the built-in key generators, instead of specifying the key generator class."
          + "Currently supports SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE");

  public static final ConfigProperty<String> ROLLBACK_USING_MARKERS_ENABLE = ConfigProperty
      .key("hoodie.rollback.using.markers")
      .defaultValue("true")
      .withDocumentation("Enables a more efficient mechanism for rollbacks based on the marker files generated "
          + "during the writes. Turned on by default.");

  public static final ConfigProperty<String> TIMELINE_LAYOUT_VERSION_NUM = ConfigProperty
      .key("hoodie.timeline.layout.version")
      .defaultValue(Integer.toString(TimelineLayoutVersion.VERSION_1))
      .sinceVersion("0.5.1")
      .withDocumentation("Controls the layout of the timeline. Version 0 relied on renames, Version 1 (default) models "
          + "the timeline as an immutable log relying only on atomic writes for object storage.");

  public static final ConfigProperty<HoodieFileFormat> BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.table.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withAlternatives("hoodie.table.ro.file.format")
      .withDocumentation("Base file format to store all the base file data.");

  public static final ConfigProperty<String> BASE_PATH = ConfigProperty
      .key("hoodie.base.path")
      .noDefaultValue()
      .withDocumentation("Base path on lake storage, under which all the table data is stored. "
          + "Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). "
          + "Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs "
          + "etc in .hoodie directory under this base path directory.");

  public static final ConfigProperty<String> AVRO_SCHEMA_STRING = ConfigProperty
      .key("hoodie.avro.schema")
      .noDefaultValue()
      .withDocumentation("Schema string representing the current write schema of the table. Hudi passes this to "
          + "implementations of HoodieRecordPayload to convert incoming records to avro. This is also used as the write schema "
          + "evolving records during an update.");

  public static final ConfigProperty<String> INTERNAL_SCHEMA_STRING = ConfigProperty
      .key("hoodie.internal.schema")
      .noDefaultValue()
      .withDocumentation("Schema string representing the latest schema of the table. Hudi passes this to "
          + "implementations of evolution of schema");

  public static final ConfigProperty<Boolean> ENABLE_INTERNAL_SCHEMA_CACHE = ConfigProperty
      .key("hoodie.schema.cache.enable")
      .defaultValue(false)
      .withDocumentation("cache query internalSchemas in driver/executor side");

  public static final ConfigProperty<String> AVRO_SCHEMA_VALIDATE_ENABLE = ConfigProperty
      .key("hoodie.avro.schema.validate")
      .defaultValue("false")
      .withDocumentation("Validate the schema used for the write against the latest schema, for backwards compatibility.");

  public static final ConfigProperty<String> INSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.insert.shuffle.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism for inserting records into the table. Inserts can shuffle data before writing to tune file sizes and optimize the storage layout.");

  public static final ConfigProperty<String> BULKINSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.bulkinsert.shuffle.parallelism")
      .defaultValue("200")
      .withDocumentation("For large initial imports using bulk_insert operation, controls the parallelism to use for sort modes or custom partitioning done"
          + "before writing records to the table.");

  public static final ConfigProperty<String> BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS = ConfigProperty
      .key("hoodie.bulkinsert.user.defined.partitioner.sort.columns")
      .noDefaultValue()
      .withDocumentation("Columns to sort the data by when use org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner as user defined partitioner during bulk_insert. "
          + "For example 'column1,column2'");

  public static final ConfigProperty<String> BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME = ConfigProperty
      .key("hoodie.bulkinsert.user.defined.partitioner.class")
      .noDefaultValue()
      .withDocumentation("If specified, this class will be used to re-partition records before they are bulk inserted. This can be used to sort, pack, cluster data"
          + " optimally for common query patterns. For now we support a build-in user defined bulkinsert partitioner org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner"
          + " which can does sorting based on specified column values set by " + BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS.key());

  public static final ConfigProperty<String> UPSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.upsert.shuffle.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism to use for upsert operation on the table. Upserts can shuffle data to perform index lookups, file sizing, bin packing records optimally"
          + "into file groups.");

  public static final ConfigProperty<String> DELETE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.delete.shuffle.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism used for “delete” operation. Delete operations also performs shuffles, similar to upsert operation.");

  public static final ConfigProperty<String> ROLLBACK_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.rollback.parallelism")
      .defaultValue("100")
      .withDocumentation("Parallelism for rollback of commits. Rollbacks perform delete of files or logging delete blocks to file groups on storage in parallel.");

  public static final ConfigProperty<String> WRITE_BUFFER_LIMIT_BYTES_VALUE = ConfigProperty
      .key("hoodie.write.buffer.limit.bytes")
      .defaultValue(String.valueOf(4 * 1024 * 1024))
      .withDocumentation("Size of in-memory buffer used for parallelizing network reads and lake storage writes.");

  public static final ConfigProperty<String> COMBINE_BEFORE_INSERT = ConfigProperty
      .key("hoodie.combine.before.insert")
      .defaultValue("false")
      .withDocumentation("When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage.");

  public static final ConfigProperty<String> COMBINE_BEFORE_UPSERT = ConfigProperty
      .key("hoodie.combine.before.upsert")
      .defaultValue("true")
      .withDocumentation("When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming, "
          + " otherwise it can lead to duplicate keys and violate the uniqueness guarantees.");

  public static final ConfigProperty<String> COMBINE_BEFORE_DELETE = ConfigProperty
      .key("hoodie.combine.before.delete")
      .defaultValue("true")
      .withDocumentation("During delete operations, controls whether we should combine deletes (and potentially also upserts) before "
          + " writing to storage.");

  public static final ConfigProperty<String> WRITE_STATUS_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.write.status.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .withDocumentation("Write status objects hold metadata about a write (stats, errors), that is not yet committed to storage. "
          + "This controls the how that information is cached for inspection by clients. We rarely expect this to be changed.");

  public static final ConfigProperty<String> AUTO_COMMIT_ENABLE = ConfigProperty
      .key("hoodie.auto.commit")
      .defaultValue("true")
      .withDocumentation("Controls whether a write operation should auto commit. This can be turned off to perform inspection"
          + " of the uncommitted write before deciding to commit.");

  public static final ConfigProperty<String> WRITE_STATUS_CLASS_NAME = ConfigProperty
      .key("hoodie.writestatus.class")
      .defaultValue(WriteStatus.class.getName())
      .withDocumentation("Subclass of " + WriteStatus.class.getName() + " to be used to collect information about a write. Can be "
          + "overridden to collection additional metrics/statistics about the data if needed.");

  public static final ConfigProperty<String> FINALIZE_WRITE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.finalize.write.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism for the write finalization internal operation, which involves removing any partially written "
          + "files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables "
          + "or low latency writes.");

  public static final ConfigProperty<String> MARKERS_TYPE = ConfigProperty
      .key("hoodie.write.markers.type")
      .defaultValue(MarkerType.TIMELINE_SERVER_BASED.toString())
      .sinceVersion("0.9.0")
      .withDocumentation("Marker type to use.  Two modes are supported: "
          + "- DIRECT: individual marker file corresponding to each data file is directly "
          + "created by the writer. "
          + "- TIMELINE_SERVER_BASED: marker operations are all handled at the timeline service "
          + "which serves as a proxy.  New marker entries are batch processed and stored "
          + "in a limited number of underlying files for efficiency.  If HDFS is used or "
          + "timeline server is disabled, DIRECT markers are used as fallback even if this "
          + "is configure.  For Spark structured streaming, this configuration does not "
          + "take effect, i.e., DIRECT markers are always used for Spark structured streaming.");

  public static final ConfigProperty<Integer> MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS = ConfigProperty
      .key("hoodie.markers.timeline_server_based.batch.num_threads")
      .defaultValue(20)
      .sinceVersion("0.9.0")
      .withDocumentation("Number of threads to use for batch processing marker "
          + "creation requests at the timeline server");

  public static final ConfigProperty<Long> MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS = ConfigProperty
      .key("hoodie.markers.timeline_server_based.batch.interval_ms")
      .defaultValue(50L)
      .sinceVersion("0.9.0")
      .withDocumentation("The batch interval in milliseconds for marker creation batch processing");

  public static final ConfigProperty<String> MARKERS_DELETE_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.markers.delete.parallelism")
      .defaultValue("100")
      .withDocumentation("Determines the parallelism for deleting marker files, which are used to track all files (valid or invalid/partial) written during "
          + "a write operation. Increase this value if delays are observed, with large batch writes.");

  public static final ConfigProperty<String> BULK_INSERT_SORT_MODE = ConfigProperty
      .key("hoodie.bulkinsert.sort.mode")
      .defaultValue(BulkInsertSortMode.NONE.toString())
      .withDocumentation("Sorting modes to use for sorting records for bulk insert. This is use when user "
          + BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME.key() + "is not configured. Available values are - "
          + "GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. "
          + "PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing "
          + "lowest and best effort file sizing. "
          + "NONE: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server")
      .defaultValue("true")
      .withDocumentation("When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),"
          + "running on each writer's driver process, accepting requests during the write from executors.");

  public static final ConfigProperty<Boolean> EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED = ConfigProperty
      .key("hoodie.embed.timeline.server.reuse.enabled")
      .defaultValue(false)
      .withDocumentation("Controls whether the timeline server instance should be cached and reused across the JVM (across task lifecycles)"
          + "to avoid startup costs. This should rarely be changed.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_PORT_NUM = ConfigProperty
      .key("hoodie.embed.timeline.server.port")
      .defaultValue("0")
      .withDocumentation("Port at which the timeline server listens for requests. When running embedded in each writer, it picks "
          + "a free port and communicates to all the executors. This should rarely be changed.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_NUM_SERVER_THREADS = ConfigProperty
      .key("hoodie.embed.timeline.server.threads")
      .defaultValue("-1")
      .withDocumentation("Number of threads to serve requests in the timeline server. By default, auto configured based on the number of underlying cores.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server.gzip")
      .defaultValue("true")
      .withDocumentation("Controls whether gzip compression is used, for large responses from the timeline server, to improve latency.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE = ConfigProperty
      .key("hoodie.embed.timeline.server.async")
      .defaultValue("false")
      .withDocumentation("Controls whether or not, the requests to the timeline server are processed in asynchronous fashion, "
          + "potentially improving throughput.");

  public static final ConfigProperty<String> FAIL_ON_TIMELINE_ARCHIVING_ENABLE = ConfigProperty
      .key("hoodie.fail.on.timeline.archiving")
      .defaultValue("true")
      .withDocumentation("Timeline archiving removes older instants from the timeline, after each write operation, to minimize metadata overhead. "
          + "Controls whether or not, the write should be failed as well, if such archiving fails.");

  public static final ConfigProperty<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(2000L)
      .withDocumentation("Initial time between successive attempts to ensure written data's metadata is consistent on storage. Grows with exponential"
          + " backoff after the initial value.");

  public static final ConfigProperty<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(300000L)
      .withDocumentation("Max time to wait between successive attempts at performing consistency checks");

  public static final ConfigProperty<Integer> MAX_CONSISTENCY_CHECKS = ConfigProperty
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(7)
      .withDocumentation("Maximum number of checks, for consistency of written data.");

  public static final ConfigProperty<String> MERGE_DATA_VALIDATION_CHECK_ENABLE = ConfigProperty
      .key("hoodie.merge.data.validation.enabled")
      .defaultValue("false")
      .withDocumentation("When enabled, data validation checks are performed during merges to ensure expected "
          + "number of records after merge operation.");

  public static final ConfigProperty<String> MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE = ConfigProperty
      .key("hoodie.merge.allow.duplicate.on.inserts")
      .defaultValue("false")
      .withDocumentation("When enabled, we allow duplicate keys even if inserts are routed to merge with an existing file (for ensuring file sizing)."
          + " This is only relevant for insert operation, since upsert, delete operations will ensure unique key constraints are maintained.");

  public static final ConfigProperty<Integer> MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT = ConfigProperty
      .key("hoodie.merge.small.file.group.candidates.limit")
      .defaultValue(1)
      .withDocumentation("Limits number of file groups, whose base file satisfies small-file limit, to consider for appending records during upsert operation. "
          + "Only applicable to MOR tables");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_INTERVAL_IN_MS = ConfigProperty
      .key("hoodie.client.heartbeat.interval_in_ms")
      .defaultValue(60 * 1000)
      .withDocumentation("Writers perform heartbeats to indicate liveness. Controls how often (in ms), such heartbeats are registered to lake storage.");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES = ConfigProperty
      .key("hoodie.client.heartbeat.tolerable.misses")
      .defaultValue(2)
      .withDocumentation("Number of heartbeat misses, before a writer is deemed not alive and all pending writes are aborted.");

  public static final ConfigProperty<String> WRITE_CONCURRENCY_MODE = ConfigProperty
      .key("hoodie.write.concurrency.mode")
      .defaultValue(WriteConcurrencyMode.SINGLE_WRITER.name())
      .withDocumentation("Enable different concurrency modes. Options are "
          + "SINGLE_WRITER: Only one active writer to the table. Maximizes throughput"
          + "OPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table and exactly one of them succeed "
          + "if a conflict (writes affect the same file group) is detected.");

  /**
   * Currently the  use this to specify the write schema.
   */
  public static final ConfigProperty<String> WRITE_SCHEMA = ConfigProperty
      .key("hoodie.write.schema")
      .noDefaultValue()
      .withDocumentation("The specified write schema. In most case, we do not need set this parameter,"
          + " but for the case the write schema is not equal to the specified table schema, we can"
          + " specify the write schema by this parameter. Used by MergeIntoHoodieTableCommand");

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
      .withDocumentation("");

  public static final ConfigProperty<String> AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE = ConfigProperty
      .key(AVRO_SCHEMA_STRING.key() + ".external.transformation")
      .defaultValue("false")
      .withAlternatives(AVRO_SCHEMA_STRING.key() + ".externalTransformation")
      .withDocumentation("When enabled, records in older schema are rewritten into newer schema during upsert,delete and background"
          + " compaction,clustering operations.");

  public static final ConfigProperty<Boolean> ALLOW_EMPTY_COMMIT = ConfigProperty
      .key("hoodie.allow.empty.commit")
      .defaultValue(true)
      .withDocumentation("Whether to allow generation of empty commits, even if no data was written in the commit. "
          + "It's useful in cases where extra metadata needs to be published regardless e.g tracking source offsets when ingesting data");

  public static final ConfigProperty<Boolean> ALLOW_OPERATION_METADATA_FIELD = ConfigProperty
      .key("hoodie.allow.operation.metadata.field")
      .defaultValue(false)
      .sinceVersion("0.9.0")
      .withDocumentation("Whether to include '_hoodie_operation' in the metadata fields. "
          + "Once enabled, all the changes of a record are persisted to the delta log directly without merge");

  public static final ConfigProperty<String> FILEID_PREFIX_PROVIDER_CLASS = ConfigProperty
      .key("hoodie.fileid.prefix.provider.class")
      .defaultValue(RandomFileIdPrefixProvider.class.getName())
      .sinceVersion("0.10.0")
      .withDocumentation("File Id Prefix provider class, that implements `org.apache.hudi.fileid.FileIdPrefixProvider`");

  public static final ConfigProperty<Boolean> TABLE_SERVICES_ENABLED = ConfigProperty
      .key("hoodie.table.services.enabled")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("Master control to disable all table services including archive, clean, compact, cluster, etc.");

  public static final ConfigProperty<Boolean> RELEASE_RESOURCE_ENABLE = ConfigProperty
      .key("hoodie.release.resource.on.completion.enable")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("Control to enable release all persist rdds when the spark job finish.");

  public static final ConfigProperty<Boolean> AUTO_ADJUST_LOCK_CONFIGS = ConfigProperty
      .key("hoodie.auto.adjust.lock.configs")
      .defaultValue(false)
      .sinceVersion("0.11.0")
      .withDocumentation("Auto adjust lock configurations when metadata table is enabled and for async table services.");

  public static final ConfigProperty<Boolean> SKIP_DEFAULT_PARTITION_VALIDATION = ConfigProperty
      .key("hoodie.skip.default.partition.validation")
      .defaultValue(false)
      .sinceVersion("0.12.0")
      .withDocumentation("When table is upgraded from pre 0.12 to 0.12, we check for \"default\" partition and fail if found one. "
          + "Users are expected to rewrite the data in those partitions. Enabling this config will bypass this validation");

  private ConsistencyGuardConfig consistencyGuardConfig;
  private FileSystemRetryConfig fileSystemRetryConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  private FileSystemViewStorageConfig viewStorageConfig;
  private HoodiePayloadConfig hoodiePayloadConfig;
  private HoodieMetadataConfig metadataConfig;
  private HoodieMetastoreConfig metastoreConfig;
  private HoodieCommonConfig commonConfig;
  private EngineType engineType;

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
   * @deprecated Use {@link #WRITE_PAYLOAD_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_WRITE_PAYLOAD_CLASS = WRITE_PAYLOAD_CLASS_NAME.defaultValue();
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
   * @deprecated Use {@link #AUTO_COMMIT_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_AUTO_COMMIT_PROP = AUTO_COMMIT_ENABLE.key();
  /**
   * @deprecated Use {@link #AUTO_COMMIT_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HOODIE_AUTO_COMMIT = AUTO_COMMIT_ENABLE.defaultValue();
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
    this.metastoreConfig = HoodieMetastoreConfig.newBuilder().fromProperties(props).build();
    this.commonConfig = HoodieCommonConfig.newBuilder().fromProperties(props).build();
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

  public String getSchema() {
    return getString(AVRO_SCHEMA_STRING);
  }

  public void setSchema(String schemaStr) {
    setValue(AVRO_SCHEMA_STRING, schemaStr);
  }

  public String getInternalSchema() {
    return getString(INTERNAL_SCHEMA_STRING);
  }

  public boolean getInternalSchemaCacheEnable() {
    return getBoolean(ENABLE_INTERNAL_SCHEMA_CACHE);
  }

  public void setInternalSchemaString(String internalSchemaString) {
    setValue(INTERNAL_SCHEMA_STRING, internalSchemaString);
  }

  public void setInternalSchemaCacheEnable(boolean enable) {
    setValue(ENABLE_INTERNAL_SCHEMA_CACHE, String.valueOf(enable));
  }

  public boolean getSchemaEvolutionEnable() {
    return getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE);
  }

  public void setSchemaEvolutionEnable(boolean enable) {
    setValue(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE, String.valueOf(enable));
  }

  /**
   * Get the write schema for written records.
   *
   * If the WRITE_SCHEMA has specified, we use the WRITE_SCHEMA.
   * Or else we use the AVRO_SCHEMA as the write schema.
   * @return
   */
  public String getWriteSchema() {
    if (props.containsKey(WRITE_SCHEMA.key())) {
      return getString(WRITE_SCHEMA);
    }
    return getSchema();
  }

  public boolean getAvroSchemaValidate() {
    return getBoolean(AVRO_SCHEMA_VALIDATE_ENABLE);
  }

  public String getTableName() {
    return getString(TBL_NAME);
  }

  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(getStringOrDefault(
        HoodieTableConfig.TYPE, HoodieTableConfig.TYPE.defaultValue().name()).toUpperCase());
  }

  public String getPreCombineField() {
    return getString(PRECOMBINE_FIELD_NAME);
  }

  public String getWritePayloadClass() {
    return getString(WRITE_PAYLOAD_CLASS_NAME);
  }

  public String getKeyGeneratorClass() {
    return getString(KEYGENERATOR_CLASS_NAME);
  }

  public boolean isCDCEnabled() {
    return getBooleanOrDefault(
        HoodieTableConfig.CDC_ENABLED, HoodieTableConfig.CDC_ENABLED.defaultValue());
  }

  public boolean isConsistentLogicalTimestampEnabled() {
    return getBooleanOrDefault(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED);
  }

  public Boolean shouldAutoCommit() {
    return getBoolean(AUTO_COMMIT_ENABLE);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return metadataConfig.shouldAssumeDatePartitioning();
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
    return Math.max(getInt(DELETE_PARALLELISM_VALUE), 1);
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

  public int getWriteBufferLimitBytes() {
    return Integer.parseInt(getStringOrDefault(WRITE_BUFFER_LIMIT_BYTES_VALUE));
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

  public String getWriteStatusClassName() {
    return getString(WRITE_STATUS_CLASS_NAME);
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

  public EngineType getEngineType() {
    return engineType;
  }

  public boolean populateMetaFields() {
    return getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
  }

  /**
   * compaction properties.
   */
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

  public int getArchiveMergeFilesBatchSize() {
    return getInt(HoodieArchivalConfig.ARCHIVE_MERGE_FILES_BATCH_SIZE);
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
    return getInt(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE);
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

  public boolean getArchiveMergeEnable() {
    return getBooleanOrDefault(HoodieArchivalConfig.ARCHIVE_MERGE_ENABLE);
  }

  public boolean shouldArchiveBeyondSavepoint() {
    return getBooleanOrDefault(HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT);
  }

  public long getArchiveMergeSmallFileLimitBytes() {
    return getLong(HoodieArchivalConfig.ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES);
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
    return ReflectionUtils.loadClass(getString(HoodieCompactionConfig.COMPACTION_STRATEGY));
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
    return getBoolean(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE);
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return getBoolean(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLE);
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

  public boolean isPreserveHoodieCommitMetadataForClustering() {
    return getBoolean(HoodieClusteringConfig.PRESERVE_COMMIT_METADATA);
  }

  public boolean isPreserveHoodieCommitMetadataForCompaction() {
    return getBoolean(HoodieCompactionConfig.PRESERVE_COMMIT_METADATA);
  }

  public boolean isClusteringEnabled() {
    // TODO: future support async clustering
    return inlineClusteringEnabled() || isAsyncClusteringEnabled();
  }

  public boolean isRollbackPendingClustering() {
    return getBoolean(HoodieClusteringConfig.ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT);
  }

  public int getInlineClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS);
  }

  public int getAsyncClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS);
  }

  public String getPayloadClass() {
    return getString(HoodiePayloadConfig.PAYLOAD_CLASS_NAME);
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

  /**
   * Clustering properties.
   */
  public String getClusteringPlanStrategyClass() {
    return getString(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME);
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

  public String getClusteringSortColumns() {
    return getString(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS);
  }

  public HoodieClusteringConfig.LayoutOptimizationStrategy getLayoutOptimizationStrategy() {
    return HoodieClusteringConfig.LayoutOptimizationStrategy.fromValue(
        getStringOrDefault(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY)
    );
  }

  public HoodieClusteringConfig.SpatialCurveCompositionStrategyType getLayoutOptimizationCurveBuildMethod() {
    return HoodieClusteringConfig.SpatialCurveCompositionStrategyType.fromValue(
        getString(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD));
  }

  public int getLayoutOptimizationSampleSize() {
    return getInt(HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE);
  }

  /**
   * index properties.
   */
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(getString(HoodieIndexConfig.INDEX_TYPE));
  }

  public String getIndexClass() {
    return getString(HoodieIndexConfig.INDEX_CLASS_NAME);
  }

  public HoodieIndex.BucketIndexEngineType getBucketIndexEngineType() {
    return HoodieIndex.BucketIndexEngineType.valueOf(getString(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE));
  }

  public int getBloomFilterNumEntries() {
    return getInt(HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE);
  }

  public double getBloomFilterFPP() {
    return getDouble(HoodieIndexConfig.BLOOM_FILTER_FPP_VALUE);
  }

  public String getHbaseZkQuorum() {
    return getString(HoodieHBaseIndexConfig.ZKQUORUM);
  }

  public int getHbaseZkPort() {
    return getInt(HoodieHBaseIndexConfig.ZKPORT);
  }

  public String getHBaseZkZnodeParent() {
    return getString(HoodieHBaseIndexConfig.ZK_NODE_PATH);
  }

  public String getHbaseTableName() {
    return getString(HoodieHBaseIndexConfig.TABLENAME);
  }

  public int getHbaseIndexGetBatchSize() {
    return getInt(HoodieHBaseIndexConfig.GET_BATCH_SIZE);
  }

  public Boolean getHBaseIndexRollbackSync() {
    return getBoolean(HoodieHBaseIndexConfig.ROLLBACK_SYNC_ENABLE);
  }

  public int getHbaseIndexPutBatchSize() {
    return getInt(HoodieHBaseIndexConfig.PUT_BATCH_SIZE);
  }

  public boolean getHbaseIndexPutBatchSizeAutoCompute() {
    return getBooleanOrDefault(HoodieHBaseIndexConfig.PUT_BATCH_SIZE_AUTO_COMPUTE);
  }

  public String getHBaseQPSResourceAllocatorClass() {
    return getString(HoodieHBaseIndexConfig.QPS_ALLOCATOR_CLASS_NAME);
  }

  public String getHBaseQPSZKnodePath() {
    return getString(HoodieHBaseIndexConfig.ZKPATH_QPS_ROOT);
  }

  public String getHBaseZkZnodeSessionTimeout() {
    return getString(HoodieHBaseIndexConfig.ZK_SESSION_TIMEOUT_MS);
  }

  public String getHBaseZkZnodeConnectionTimeout() {
    return getString(HoodieHBaseIndexConfig.ZK_CONNECTION_TIMEOUT_MS);
  }

  public boolean getHBaseIndexShouldComputeQPSDynamically() {
    return getBoolean(HoodieHBaseIndexConfig.COMPUTE_QPS_DYNAMICALLY);
  }

  public String getHBaseIndexSecurityAuthentication() {
    return getString(HoodieHBaseIndexConfig.SECURITY_AUTHENTICATION);
  }

  public String getHBaseIndexKerberosUserKeytab() {
    return getString(HoodieHBaseIndexConfig.KERBEROS_USER_KEYTAB);
  }

  public String getHBaseIndexKerberosUserPrincipal() {
    return getString(HoodieHBaseIndexConfig.KERBEROS_USER_PRINCIPAL);
  }

  public String getHBaseIndexRegionserverPrincipal() {
    return getString(HoodieHBaseIndexConfig.REGIONSERVER_PRINCIPAL);
  }

  public String getHBaseIndexMasterPrincipal() {
    return getString(HoodieHBaseIndexConfig.MASTER_PRINCIPAL);
  }

  public int getHBaseIndexDesiredPutsTime() {
    return getInt(HoodieHBaseIndexConfig.DESIRED_PUTS_TIME_IN_SECONDS);
  }

  public String getBloomFilterType() {
    return getString(HoodieIndexConfig.BLOOM_FILTER_TYPE);
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return getInt(HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES);
  }

  /**
   * Fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have
   * input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for
   * the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public float getHbaseIndexQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.QPS_FRACTION);
  }

  public float getHBaseIndexMinQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.MIN_QPS_FRACTION);
  }

  public float getHBaseIndexMaxQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.MAX_QPS_FRACTION);
  }

  /**
   * This should be same across various jobs. This is intended to limit the aggregate QPS generated across various
   * Hoodie jobs to an Hbase Region Server
   */
  public int getHbaseIndexMaxQPSPerRegionServer() {
    return getInt(HoodieHBaseIndexConfig.MAX_QPS_PER_REGION_SERVER);
  }

  public boolean getHbaseIndexUpdatePartitionPath() {
    return getBooleanOrDefault(HoodieHBaseIndexConfig.UPDATE_PARTITION_PATH_ENABLE);
  }

  public int getHBaseIndexRegionCount() {
    return getInt(HoodieHBaseIndexConfig.BUCKET_NUMBER);
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

  public boolean useBloomIndexTreebasedFilter() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_TREE_BASED_FILTER);
  }

  public boolean useBloomIndexBucketizedChecking() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING);
  }

  public boolean isMetadataBloomFilterIndexEnabled() {
    return isMetadataTableEnabled() && getMetadataConfig().isBloomFilterIndexEnabled();
  }

  public boolean isMetadataColumnStatsIndexEnabled() {
    return isMetadataTableEnabled() && getMetadataConfig().isColumnStatsIndexEnabled();
  }

  public List<String> getColumnsEnabledForColumnStatsIndex() {
    return getMetadataConfig().getColumnsEnabledForColumnStatsIndex();
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

  public boolean getBloomIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE);
  }

  public int getSimpleIndexParallelism() {
    return getInt(HoodieIndexConfig.SIMPLE_INDEX_PARALLELISM);
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

  public int getLogFileDataBlockMaxSize() {
    return getInt(HoodieStorageConfig.LOGFILE_DATA_BLOCK_MAX_SIZE);
  }

  public double getParquetCompressionRatio() {
    return getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION);
  }

  public CompressionCodecName getParquetCompressionCodec() {
    String codecName = getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    return CompressionCodecName.fromConf(StringUtils.isNullOrEmpty(codecName) ? null : codecName);
  }

  public boolean parquetDictionaryEnabled() {
    return getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED);
  }

  public String parquetWriteLegacyFormatEnabled() {
    return getString(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED);
  }

  public String parquetOutputTimestampType() {
    return getString(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE);
  }

  public String parquetFieldIdWriteEnabled() {
    return getString(HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED);
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

  public Compression.Algorithm getHFileCompressionAlgorithm() {
    return Compression.Algorithm.valueOf(getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME));
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
    return getBoolean(HoodieMetricsConfig.TURN_METRICS_ON);
  }

  public boolean isExecutorMetricsEnabled() {
    return Boolean.parseBoolean(
        getStringOrDefault(HoodieMetricsConfig.EXECUTOR_METRICS_ENABLE, "false"));
  }

  public boolean isLockingMetricsEnabled() {
    return getBoolean(HoodieMetricsConfig.LOCK_METRICS_ENABLE);
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType.valueOf(getString(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE));
  }

  public String getGraphiteServerHost() {
    return getString(HoodieMetricsGraphiteConfig.GRAPHITE_SERVER_HOST_NAME);
  }

  public int getGraphiteServerPort() {
    return getInt(HoodieMetricsGraphiteConfig.GRAPHITE_SERVER_PORT_NUM);
  }

  public String getGraphiteMetricPrefix() {
    return getString(HoodieMetricsGraphiteConfig.GRAPHITE_METRIC_PREFIX_VALUE);
  }

  public int getGraphiteReportPeriodSeconds() {
    return getInt(HoodieMetricsGraphiteConfig.GRAPHITE_REPORT_PERIOD_IN_SECONDS);
  }

  public String getJmxHost() {
    return getString(HoodieMetricsJmxConfig.JMX_HOST_NAME);
  }

  public String getJmxPort() {
    return getString(HoodieMetricsJmxConfig.JMX_PORT_NUM);
  }

  public int getDatadogReportPeriodSeconds() {
    return getInt(HoodieMetricsDatadogConfig.REPORT_PERIOD_IN_SECONDS);
  }

  public ApiSite getDatadogApiSite() {
    return ApiSite.valueOf(getString(HoodieMetricsDatadogConfig.API_SITE_VALUE));
  }

  public String getDatadogApiKey() {
    if (props.containsKey(HoodieMetricsDatadogConfig.API_KEY.key())) {
      return getString(HoodieMetricsDatadogConfig.API_KEY);
    } else {
      Supplier<String> apiKeySupplier = ReflectionUtils.loadClass(
          getString(HoodieMetricsDatadogConfig.API_KEY_SUPPLIER));
      return apiKeySupplier.get();
    }
  }

  public boolean getDatadogApiKeySkipValidation() {
    return getBoolean(HoodieMetricsDatadogConfig.API_KEY_SKIP_VALIDATION);
  }

  public int getDatadogApiTimeoutSeconds() {
    return getInt(HoodieMetricsDatadogConfig.API_TIMEOUT_IN_SECONDS);
  }

  public String getDatadogMetricPrefix() {
    return getString(HoodieMetricsDatadogConfig.METRIC_PREFIX_VALUE);
  }

  public String getDatadogMetricHost() {
    return getString(HoodieMetricsDatadogConfig.METRIC_HOST_NAME);
  }

  public List<String> getDatadogMetricTags() {
    return Arrays.stream(getStringOrDefault(
        HoodieMetricsDatadogConfig.METRIC_TAG_VALUES, ",").split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public int getCloudWatchReportPeriodSeconds() {
    return getInt(HoodieMetricsCloudWatchConfig.REPORT_PERIOD_SECONDS);
  }

  public String getCloudWatchMetricPrefix() {
    return getString(HoodieMetricsCloudWatchConfig.METRIC_PREFIX);
  }

  public String getCloudWatchMetricNamespace() {
    return getString(HoodieMetricsCloudWatchConfig.METRIC_NAMESPACE);
  }

  public int getCloudWatchMaxDatumsPerRequest() {
    return getInt(HoodieMetricsCloudWatchConfig.MAX_DATUMS_PER_REQUEST);
  }

  public String getMetricReporterClassName() {
    return getString(HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME);
  }

  public int getPrometheusPort() {
    return getInt(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT_NUM);
  }

  public String getPushGatewayHost() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_HOST_NAME);
  }

  public int getPushGatewayPort() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_PORT_NUM);
  }

  public int getPushGatewayReportPeriodSeconds() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS);
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE);
  }

  public String getPushGatewayJobName() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_JOBNAME);
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX);
  }

  public String getMetricReporterMetricsNamePrefix() {
    return getStringOrDefault(HoodieMetricsConfig.METRICS_REPORTER_PREFIX);
  }

  /**
   * memory configs.
   */
  public int getMaxDFSStreamBufferSize() {
    return getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE);
  }

  public String getSpillableMapBasePath() {
    return getString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH);
  }

  public double getWriteStatusFailureFraction() {
    return getDouble(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION);
  }

  public ConsistencyGuardConfig getConsistencyGuardConfig() {
    return consistencyGuardConfig;
  }

  public FileSystemRetryConfig getFileSystemRetryConfig() {
    return fileSystemRetryConfig;
  }

  public void setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
    this.consistencyGuardConfig = consistencyGuardConfig;
  }

  public FileSystemViewStorageConfig getViewStorageConfig() {
    return viewStorageConfig;
  }

  public void setViewStorageConfig(FileSystemViewStorageConfig viewStorageConfig) {
    this.viewStorageConfig = viewStorageConfig;
  }

  public void resetViewStorageConfig() {
    this.setViewStorageConfig(getClientSpecifiedViewStorageConfig());
  }

  public FileSystemViewStorageConfig getClientSpecifiedViewStorageConfig() {
    return clientSpecifiedViewStorageConfig;
  }

  public HoodiePayloadConfig getPayloadConfig() {
    return hoodiePayloadConfig;
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  public HoodieCommonConfig getCommonConfig() {
    return commonConfig;
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

  public String getBootstrapKeyGeneratorClass() {
    return getString(HoodieBootstrapConfig.KEYGEN_CLASS_NAME);
  }

  public String getBootstrapKeyGeneratorType() {
    return getString(HoodieBootstrapConfig.KEYGEN_TYPE);
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
    return metadataConfig.enabled();
  }

  public int getMetadataInsertParallelism() {
    return getInt(HoodieMetadataConfig.INSERT_PARALLELISM_VALUE);
  }

  public int getMetadataCompactDeltaCommitMax() {
    return getInt(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS);
  }

  public boolean isMetadataAsyncClean() {
    return getBoolean(HoodieMetadataConfig.ASYNC_CLEAN_ENABLE);
  }

  public boolean isMetadataAsyncIndex() {
    return getBooleanOrDefault(HoodieMetadataConfig.ASYNC_INDEX_ENABLE);
  }

  public int getMetadataMaxCommitsToKeep() {
    return getInt(HoodieMetadataConfig.MAX_COMMITS_TO_KEEP);
  }

  public int getMetadataMinCommitsToKeep() {
    return getInt(HoodieMetadataConfig.MIN_COMMITS_TO_KEEP);
  }

  public int getMetadataCleanerCommitsRetained() {
    return getInt(HoodieMetadataConfig.CLEANER_COMMITS_RETAINED);
  }

  /**
   * Hoodie Client Lock Configs.
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

  public Long getLockAcquireWaitTimeoutInMs() {
    return getLong(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS);
  }

  public WriteConcurrencyMode getWriteConcurrencyMode() {
    return WriteConcurrencyMode.fromValue(getString(WRITE_CONCURRENCY_MODE));
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
        && (inlineClusteringEnabled() || inlineCompactionEnabled()
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
   * Layout configs.
   */
  public HoodieStorageLayout.LayoutType getLayoutType() {
    return HoodieStorageLayout.LayoutType.valueOf(getString(HoodieLayoutConfig.LAYOUT_TYPE));
  }

  /**
   * Metastore configs.
   */
  public boolean isMetastoreEnabled() {
    return metastoreConfig.enableMetastore();
  }

  /**
   * CDC supplemental logging mode.
   */
  public HoodieCDCSupplementalLoggingMode getCDCSupplementalLoggingMode() {
    return HoodieCDCSupplementalLoggingMode.parse(
        getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE));
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
    private boolean isOptimizeConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isBootstrapConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;
    private boolean isCallbackConfigSet = false;
    private boolean isPayloadConfigSet = false;
    private boolean isMetadataConfigSet = false;
    private boolean isLockConfigSet = false;
    private boolean isPreCommitValidationConfigSet = false;
    private boolean isMetricsJmxConfigSet = false;
    private boolean isMetricsGraphiteConfigSet = false;
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

    public Builder withSchema(String schemaStr) {
      writeConfig.setValue(AVRO_SCHEMA_STRING, schemaStr);
      return this;
    }

    public Builder withSchemaEvolutionEnable(boolean enable) {
      writeConfig.setValue(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder withInternalSchemaCacheEnable(boolean enable) {
      writeConfig.setValue(ENABLE_INTERNAL_SCHEMA_CACHE, String.valueOf(enable));
      return this;
    }

    public Builder withAvroSchemaValidate(boolean enable) {
      writeConfig.setValue(AVRO_SCHEMA_VALIDATE_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder forTable(String tableName) {
      writeConfig.setValue(TBL_NAME, tableName);
      return this;
    }

    public Builder withPreCombineField(String preCombineField) {
      writeConfig.setValue(PRECOMBINE_FIELD_NAME, preCombineField);
      return this;
    }

    public Builder withWritePayLoad(String payload) {
      writeConfig.setValue(WRITE_PAYLOAD_CLASS_NAME, payload);
      return this;
    }

    public Builder withKeyGenerator(String keyGeneratorClass) {
      writeConfig.setValue(KEYGENERATOR_CLASS_NAME, keyGeneratorClass);
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

    public Builder withWriteBufferLimitBytes(int writeBufferLimit) {
      writeConfig.setValue(WRITE_BUFFER_LIMIT_BYTES_VALUE, String.valueOf(writeBufferLimit));
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

    public Builder withMetadataConfig(HoodieMetadataConfig metadataConfig) {
      writeConfig.getProps().putAll(metadataConfig.getProps());
      isMetadataConfigSet = true;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      writeConfig.setValue(AUTO_COMMIT_ENABLE, String.valueOf(autoCommit));
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
      writeConfig.setValue(WRITE_CONCURRENCY_MODE, concurrencyMode.value());
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

    protected void setDefaults() {
      writeConfig.setDefaultValue(MARKERS_TYPE, getDefaultMarkersType(engineType));
      // Check for mandatory properties
      writeConfig.setDefaults(HoodieWriteConfig.class.getName());
      // Set default values of HoodieHBaseIndexConfig
      writeConfig.setDefaults(HoodieHBaseIndexConfig.class.getName());
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
      final boolean isLockProviderPropertySet = writeConfigProperties.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME)
          || writeConfigProperties.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_PROP);
      writeConfig.setDefaultOnCondition(!isLockConfigSet,
          HoodieLockConfig.newBuilder().fromProperties(writeConfig.getProps()).build());

      autoAdjustConfigsForConcurrencyMode(isLockProviderPropertySet);
    }

    private void autoAdjustConfigsForConcurrencyMode(boolean isLockProviderPropertySet) {
      if (writeConfig.isAutoAdjustLockConfigs()) {
        // auto adjustment is required only for deltastreamer and spark streaming where async table services can be executed in the same JVM.
        boolean isMetadataTableEnabled = writeConfig.getBoolean(HoodieMetadataConfig.ENABLE);

        if (isMetadataTableEnabled) {
          // When metadata table is enabled, optimistic concurrency control must be used for
          // single writer with async table services.
          // Async table services can update the metadata table and a lock provider is
          // needed to guard against any concurrent table write operations. If user has
          // not configured any lock provider, let's use the InProcess lock provider.
          boolean areTableServicesEnabled = writeConfig.areTableServicesEnabled();
          boolean areAsyncTableServicesEnabled = writeConfig.areAnyTableServicesAsync();
          if (!isLockProviderPropertySet && areTableServicesEnabled && areAsyncTableServicesEnabled) {
            // This is targeted at Single writer with async table services
            // If user does not set the lock provider, likely that the concurrency mode is not set either
            // Override the configs for metadata table
            writeConfig.setValue(WRITE_CONCURRENCY_MODE.key(),
                WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value());
            writeConfig.setValue(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
                InProcessLockProvider.class.getName());
            LOG.info(String.format("Automatically set %s=%s and %s=%s since user has not set the "
                    + "lock provider for single writer with async table services",
                WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value(),
                HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName()));
          }
        }
      }

      // We check if "hoodie.cleaner.policy.failed.writes"
      // is properly set to LAZY for optimistic concurrency control
      String writeConcurrencyMode = writeConfig.getString(WRITE_CONCURRENCY_MODE);
      if (WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value()
          .equalsIgnoreCase(writeConcurrencyMode)) {
        // In this case, we assume that the user takes care of setting the lock provider used
        writeConfig.setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
            HoodieFailedWritesCleaningPolicy.LAZY.name());
        LOG.info(String.format("Automatically set %s=%s since optimistic concurrency control is used",
            HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
            HoodieFailedWritesCleaningPolicy.LAZY.name()));
      }
    }

    private void validate() {
      String layoutVersion = writeConfig.getString(TIMELINE_LAYOUT_VERSION_NUM);
      // Ensure Layout Version is good
      new TimelineLayoutVersion(Integer.parseInt(layoutVersion));
      Objects.requireNonNull(writeConfig.getString(BASE_PATH));
      if (writeConfig.getString(WRITE_CONCURRENCY_MODE)
          .equalsIgnoreCase(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value())) {
        ValidationUtils.checkArgument(!writeConfig.getString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY)
            .equals(HoodieFailedWritesCleaningPolicy.EAGER.name()), "To enable optimistic concurrency control, set hoodie.cleaner.policy.failed.writes=LAZY");
      }

      HoodieCleaningPolicy.valueOf(writeConfig.getString(CLEANER_POLICY));
      // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
      // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
      int minInstantsToKeep = Integer.parseInt(writeConfig.getStringOrDefault(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP));
      int maxInstantsToKeep = Integer.parseInt(writeConfig.getStringOrDefault(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP));
      int cleanerCommitsRetained =
          Integer.parseInt(writeConfig.getStringOrDefault(HoodieCleanConfig.CLEANER_COMMITS_RETAINED));
      ValidationUtils.checkArgument(maxInstantsToKeep > minInstantsToKeep,
          String.format(
              "Increase %s=%d to be greater than %s=%d.",
              HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), maxInstantsToKeep,
              HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep));
      ValidationUtils.checkArgument(minInstantsToKeep > cleanerCommitsRetained,
          String.format(
              "Increase %s=%d to be greater than %s=%d. Otherwise, there is risk of incremental pull "
                  + "missing data from few instants.",
              HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep,
              HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), cleanerCommitsRetained));

      boolean inlineCompact = writeConfig.getBoolean(HoodieCompactionConfig.INLINE_COMPACT);
      boolean inlineCompactSchedule = writeConfig.getBoolean(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT);
      ValidationUtils.checkArgument(!(inlineCompact && inlineCompactSchedule), String.format("Either of inline compaction (%s) or "
              + "schedule inline compaction (%s) can be enabled. Both can't be set to true at the same time. %s, %s", HoodieCompactionConfig.INLINE_COMPACT.key(),
          HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), inlineCompact, inlineCompactSchedule));
    }

    public HoodieWriteConfig build() {
      setDefaults();
      validate();
      // Build WriteConfig at the end
      return new HoodieWriteConfig(engineType, writeConfig.getProps());
    }

    private String getDefaultMarkersType(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return MarkerType.TIMELINE_SERVER_BASED.toString();
        case FLINK:
        case JAVA:
          // Timeline-server-based marker is not supported for Flink and Java engines
          return MarkerType.DIRECT.toString();
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }
  }
}
