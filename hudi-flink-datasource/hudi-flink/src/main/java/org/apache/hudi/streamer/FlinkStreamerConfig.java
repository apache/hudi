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

package org.apache.hudi.streamer;

import org.apache.hudi.client.utils.OperationConverter;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sync.common.model.partextractor.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.util.FlinkStateBackendConverter;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.configuration.FlinkOptions.PARTITION_FORMAT_DAY;

/**
 * Configurations for Hoodie Flink streamer.
 */
public class FlinkStreamerConfig extends Configuration {
  @Parameter(names = {"--kafka-topic"}, description = "Kafka topic name.", required = true)
  public String kafkaTopic;

  @Parameter(names = {"--kafka-group-id"}, description = "Kafka consumer group id.", required = true)
  public String kafkaGroupId;

  @Parameter(names = {"--kafka-bootstrap-servers"}, description = "Kafka bootstrap.servers.", required = true)
  public String kafkaBootstrapServers;

  @Parameter(names = {"--flink-checkpoint-path"}, description = "Flink checkpoint path.")
  public String flinkCheckPointPath;

  @Parameter(names = {"--flink-state-backend-type"}, description = "Flink state backend type, support only hashmap and rocksdb by now,"
          + " default hashmap.", converter = FlinkStateBackendConverter.class)
  public StateBackend stateBackend = new HashMapStateBackend();

  @Parameter(names = {"--instant-retry-times"}, description = "Times to retry when latest instant has not completed.")
  public String instantRetryTimes = "10";

  @Parameter(names = {"--instant-retry-interval"}, description = "Seconds between two tries when latest instant has not completed.")
  public String instantRetryInterval = "1";

  @Parameter(names = {"--target-base-path"},
      description = "Base path for the target hoodie table. "
          + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table).",
      required = true)
  public String targetBasePath;

  @Parameter(names = {"--target-table"}, description = "Name of the target table in Hive.", required = true)
  public String targetTableName;

  @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ.", required = true)
  public String tableType;

  @Parameter(names = {"--insert-cluster"}, description = "Whether to merge small files for insert mode, "
      + "if true, the write throughput will decrease because the read/write of existing small file, default false.")
  public Boolean insertCluster = false;

  @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
      + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
      + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
      + "to individual classes, for supported properties.")
  public String propsFilePath = "";

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--props\") can also be passed command line using this parameter.")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--record-key-field"}, description = "Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
      + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
      + "the dot notation eg: `a.b.c`. By default `uuid`.")
  public String recordKeyField = "uuid";

  @Parameter(names = {"--partition-path-field"}, description = "Partition path field. Value to be used at \n"
      + "the `partitionPath` component of `HoodieKey`. Actual value obtained by invoking .toString(). By default `partitionpath`.")
  public String partitionPathField = "partitionpath";

  @Parameter(names = {"--keygen-class"}, description = "Key generator class, that implements will extract the key out of incoming record.")
  public String keygenClass;

  @Parameter(names = {"--keygen-type"}, description = "Key generator type, that implements will extract the key out of incoming record \n"
      + "By default `SIMPLE`.")
  public String keygenType = KeyGeneratorType.SIMPLE.name();

  @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
      + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record.")
  public String sourceOrderingField = "ts";

  @Parameter(names = {"--payload-class"}, description = "Subclass of HoodieRecordPayload, that works off "
      + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value.")
  public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

  @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
      + "is purely new data/inserts to gain speed).", converter = OperationConverter.class)
  public WriteOperationType operation = WriteOperationType.UPSERT;

  @Parameter(names = {"--filter-dupes"},
      description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert.")
  public Boolean preCombine = false;

  @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written.")
  public Boolean commitOnErrors = false;

  @Parameter(names = {"--transformer-class"},
      description = "A subclass or a list of subclasses of org.apache.hudi.sink.transform.Transformer"
          + ". Allows transforming raw source DataStream to a target DataStream (conforming to target schema) before "
          + "writing. Default : Not set. Pass a comma-separated list of subclass names to chain the transformations.")
  public List<String> transformerClassNames = null;

  @Parameter(names = {"--metadata-enabled"}, description = "Enable the internal metadata table which serves table metadata like level file listings, default false.")
  public Boolean metadataEnabled = false;

  @Parameter(names = {"--metadata-compaction-delta_commits"}, description = "Max delta commits for metadata table to trigger compaction, default 10.")
  public Integer metadataCompactionDeltaCommits = 10;

  @Parameter(names = {"--write-partition-format"}, description = "Partition path format, default is 'yyyyMMdd'.")
  public String writePartitionFormat = PARTITION_FORMAT_DAY;

  @Parameter(names = {"--write-rate-limit"}, description = "Write record rate limit per second to prevent traffic jitter and improve stability, default 0 (no limit).")
  public Long writeRateLimit = 0L;

  @Parameter(names = {"--write-parquet-block-size"}, description = "Parquet RowGroup size. It's recommended to make this large enough that scan costs can be"
      + " amortized by packing enough column values into a single row group.")
  public Integer writeParquetBlockSize = 120;

  @Parameter(names = {"--write-parquet-max-file-size"}, description = "Target size for parquet files produced by Hudi write phases. "
      + "For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.")
  public Integer writeParquetMaxFileSize = 120;

  @Parameter(names = {"--parquet-page-size"}, description = "Parquet page size. Page is the unit of read within a parquet file. "
      + "Within a block, pages are compressed separately.")
  public Integer parquetPageSize = 1;

  /**
   * Flink checkpoint interval.
   */
  @Parameter(names = {"--checkpoint-interval"}, description = "Flink checkpoint interval.")
  public Long checkpointInterval = 1000 * 5L;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  @Parameter(names = {"--index-bootstrap-num"}, description = "Parallelism of tasks that do bucket assign, default is 4.")
  public Integer indexBootstrapNum = 4;

  @Parameter(names = {"--bucket-assign-num"}, description = "Parallelism of tasks that do bucket assign, default is 4.")
  public Integer bucketAssignNum = 4;

  @Parameter(names = {"--write-task-num"}, description = "Parallelism of tasks that do actual write, default is 4.")
  public Integer writeTaskNum = 4;

  @Parameter(names = {"--partition-default-name"},
      description = "The default partition name in case the dynamic partition column value is null/empty string")
  public String partitionDefaultName = "default";

  @Parameter(names = {"--index-bootstrap-enabled"},
      description = "Whether to bootstrap the index state from existing hoodie table, default false")
  public Boolean indexBootstrapEnabled = false;

  @Parameter(names = {"--index-state-ttl"}, description = "Index state ttl in days, default stores the index permanently")
  public Double indexStateTtl = 0D;

  @Parameter(names = {"--index-global-enabled"}, description = "Whether to update index for the old partition path "
      + "if same key record with different partition path came in, default true")
  public Boolean indexGlobalEnabled = true;

  @Parameter(names = {"--index-partition-regex"},
      description = "Whether to load partitions in state if partition path matching, default *")
  public String indexPartitionRegex = ".*";

  @Parameter(names = {"--source-avro-schema-path"}, description = "Source avro schema file path, the parsed schema is used for deserialization")
  public String sourceAvroSchemaPath = "";

  @Parameter(names = {"--source-avro-schema"}, description = "Source avro schema string, the parsed schema is used for deserialization")
  public String sourceAvroSchema = "";

  @Parameter(names = {"--utc-timezone"}, description = "Use UTC timezone or local timezone to the conversion between epoch"
      + " time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x"
      + " use UTC timezone, by default true")
  public Boolean utcTimezone = true;

  @Parameter(names = {"--write-partition-url-encode"}, description = "Whether to encode the partition path url, default false")
  public Boolean writePartitionUrlEncode = false;

  @Parameter(names = {"--hive-style-partitioning"}, description = "Whether to use Hive style partitioning.\n"
      + "If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.\n"
      + "By default false (the names of partition folders are only partition values)")
  public Boolean hiveStylePartitioning = false;

  @Parameter(names = {"--write-task-max-size"}, description = "Maximum memory in MB for a write task, when the threshold hits,\n"
      + "it flushes the max size data bucket to avoid OOM, default 1GB")
  public Double writeTaskMaxSize = 1024D;

  @Parameter(names = {"--write-batch-size"},
      description = "Batch buffer size in MB to flush data into the underneath filesystem, default 256MB")
  public Double writeBatchSize = 256D;

  @Parameter(names = {"--write-log-block-size"}, description = "Max log block size in MB for log file, default 128MB")
  public Integer writeLogBlockSize = 128;

  @Parameter(names = {"--write-log-max-size"},
      description = "Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB")
  public Integer writeLogMaxSize = 1024;

  @Parameter(names = {"--write-merge-max-memory"}, description = "Max memory in MB for merge, default 100MB")
  public Integer writeMergeMaxMemory = 100;

  @Parameter(names = {"--compaction-async-enabled"}, description = "Async Compaction, enabled by default for MOR")
  public Boolean compactionAsyncEnabled = true;

  @Parameter(names = {"--compaction-tasks"}, description = "Parallelism of tasks that do actual compaction, default is 10")
  public Integer compactionTasks = 10;

  @Parameter(names = {"--compaction-trigger-strategy"},
      description = "Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits;\n"
          + "'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction;\n"
          + "'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;\n"
          + "'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.\n"
          + "Default is 'num_commits'")
  public String compactionTriggerStrategy = FlinkOptions.NUM_COMMITS;

  @Parameter(names = {"--compaction-delta-commits"}, description = "Max delta commits needed to trigger compaction, default 5 commits")
  public Integer compactionDeltaCommits = 5;

  @Parameter(names = {"--compaction-delta-seconds"}, description = "Max delta seconds time needed to trigger compaction, default 1 hour")
  public Integer compactionDeltaSeconds = 3600;

  @Parameter(names = {"--compaction-max-memory"}, description = "Max memory in MB for compaction spillable map, default 100MB")
  public Integer compactionMaxMemory = 100;

  @Parameter(names = {"--compaction-target-io"}, description = "Target IO per compaction (both read and write), default 500 GB")
  public Long compactionTargetIo = 512000L;

  @Parameter(names = {"--clean-async-enabled"}, description = "Whether to cleanup the old commits immediately on new commits, enabled by default")
  public Boolean cleanAsyncEnabled = true;

  @Parameter(names = {"--clean-policy"},
      description = "Clean policy to manage the Hudi table. Available option: KEEP_LATEST_COMMITS, KEEP_LATEST_FILE_VERSIONS, KEEP_LATEST_BY_HOURS."
          +  "Default is KEEP_LATEST_COMMITS.")
  public String cleanPolicy = HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name();

  @Parameter(names = {"--clean-retain-commits"},
      description = "Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).\n"
          + "This also directly translates into how much you can incrementally pull on this table, default 10")
  public Integer cleanRetainCommits = 10;

  @Parameter(names = {"--clean-retain-file-versions"},
      description = "Number of file versions to retain. Each file group will be retained for this number of version. default 5")
  public Integer cleanRetainFileVersions = 5;

  @Parameter(names = {"--archive-max-commits"},
      description = "Max number of commits to keep before archiving older commits into a sequential log, default 30")
  public Integer archiveMaxCommits = 30;

  @Parameter(names = {"--archive-min-commits"},
      description = "Min number of commits to keep before archiving older commits into a sequential log, default 20")
  public Integer archiveMinCommits = 20;

  @Parameter(names = {"--hive-sync-enable"}, description = "Asynchronously sync Hive meta to HMS, default false")
  public Boolean hiveSyncEnabled = false;

  @Parameter(names = {"--hive-sync-db"}, description = "Database name for hive sync, default 'default'")
  public String hiveSyncDb = "default";

  @Parameter(names = {"--hive-sync-table"}, description = "Table name for hive sync, default 'unknown'")
  public String hiveSyncTable = "unknown";

  @Parameter(names = {"--hive-sync-file-format"}, description = "File format for hive sync, default 'PARQUET'")
  public String hiveSyncFileFormat = "PARQUET";

  @Parameter(names = {"--hive-sync-mode"}, description = "Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'jdbc'")
  public String hiveSyncMode = "jdbc";

  @Parameter(names = {"--hive-sync-username"}, description = "Username for hive sync, default 'hive'")
  public String hiveSyncUsername = "hive";

  @Parameter(names = {"--hive-sync-password"}, description = "Password for hive sync, default 'hive'")
  public String hiveSyncPassword = "hive";

  @Parameter(names = {"--hive-sync-jdbc-url"}, description = "Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'")
  public String hiveSyncJdbcUrl = "jdbc:hive2://localhost:10000";

  @Parameter(names = {"--hive-sync-metastore-uris"}, description = "Metastore uris for hive sync, default ''")
  public String hiveSyncMetastoreUri = "";

  @Parameter(names = {"--hive-sync-partition-fields"}, description = "Partition fields for hive sync, default ''")
  public String hiveSyncPartitionFields = "";

  @Parameter(names = {"--hive-sync-partition-extractor-class"}, description = "Tool to extract the partition value from HDFS path, "
      + "default 'SlashEncodedDayPartitionValueExtractor'")
  public String hiveSyncPartitionExtractorClass = SlashEncodedDayPartitionValueExtractor.class.getCanonicalName();

  @Parameter(names = {"--hive-sync-assume-date-partitioning"}, description = "Assume partitioning is yyyy/mm/dd, default false")
  public Boolean hiveSyncAssumeDatePartition = false;

  @Parameter(names = {"--hive-sync-use-jdbc"}, description = "Use JDBC when hive synchronization is enabled, default true")
  public Boolean hiveSyncUseJdbc = true;

  @Parameter(names = {"--hive-sync-auto-create-db"}, description = "Auto create hive database if it does not exists, default true")
  public Boolean hiveSyncAutoCreateDb = true;

  @Parameter(names = {"--hive-sync-ignore-exceptions"}, description = "Ignore exceptions during hive synchronization, default false")
  public Boolean hiveSyncIgnoreExceptions = false;

  @Parameter(names = {"--hive-sync-skip-ro-suffix"}, description = "Skip the _ro suffix for Read optimized table when registering, default false")
  public Boolean hiveSyncSkipRoSuffix = false;

  @Parameter(names = {"--hive-sync-support-timestamp"}, description = "INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type.\n"
      + "Disabled by default for backward compatibility.")
  public Boolean hiveSyncSupportTimestamp = false;


  /**
   * Transforms a {@code HoodieFlinkStreamer.Config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  @SuppressWarnings("unchecked, rawtypes")
  public static org.apache.flink.configuration.Configuration toFlinkConfig(FlinkStreamerConfig config) {
    Map<String, String> propsMap = new HashMap<String, String>((Map) StreamerUtil.getProps(config));
    org.apache.flink.configuration.Configuration conf = fromMap(propsMap);

    conf.setString(FlinkOptions.PATH, config.targetBasePath);
    conf.setString(FlinkOptions.TABLE_NAME, config.targetTableName);
    // copy_on_write works same as COPY_ON_WRITE
    conf.setString(FlinkOptions.TABLE_TYPE, config.tableType.toUpperCase());
    conf.setBoolean(FlinkOptions.INSERT_CLUSTER, config.insertCluster);
    conf.setString(FlinkOptions.OPERATION, config.operation.value());
    conf.setString(FlinkOptions.PRECOMBINE_FIELD, config.sourceOrderingField);
    conf.setString(FlinkOptions.PAYLOAD_CLASS_NAME, config.payloadClassName);
    conf.setBoolean(FlinkOptions.PRE_COMBINE, config.preCombine);
    conf.setInteger(FlinkOptions.RETRY_TIMES, Integer.parseInt(config.instantRetryTimes));
    conf.setLong(FlinkOptions.RETRY_INTERVAL_MS, Long.parseLong(config.instantRetryInterval));
    conf.setBoolean(FlinkOptions.IGNORE_FAILED, config.commitOnErrors);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, config.recordKeyField);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, config.partitionPathField);
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, config.metadataEnabled);
    conf.setInteger(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS, config.metadataCompactionDeltaCommits);
    conf.setString(FlinkOptions.PARTITION_FORMAT, config.writePartitionFormat);
    conf.setLong(FlinkOptions.WRITE_RATE_LIMIT, config.writeRateLimit);
    conf.setInteger(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE, config.writeParquetBlockSize);
    conf.setInteger(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, config.writeParquetMaxFileSize);
    conf.setInteger(FlinkOptions.WRITE_PARQUET_PAGE_SIZE, config.parquetPageSize);
    if (!StringUtils.isNullOrEmpty(config.keygenClass)) {
      conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, config.keygenClass);
    } else {
      conf.setString(FlinkOptions.KEYGEN_TYPE, config.keygenType);
    }
    conf.setInteger(FlinkOptions.INDEX_BOOTSTRAP_TASKS, config.indexBootstrapNum);
    conf.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS, config.bucketAssignNum);
    conf.setInteger(FlinkOptions.WRITE_TASKS, config.writeTaskNum);
    conf.setString(FlinkOptions.PARTITION_DEFAULT_NAME, config.partitionDefaultName);
    conf.setBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, config.indexBootstrapEnabled);
    conf.setDouble(FlinkOptions.INDEX_STATE_TTL, config.indexStateTtl);
    conf.setBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED, config.indexGlobalEnabled);
    conf.setString(FlinkOptions.INDEX_PARTITION_REGEX, config.indexPartitionRegex);
    if (!StringUtils.isNullOrEmpty(config.sourceAvroSchemaPath)) {
      conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, config.sourceAvroSchemaPath);
    }
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, config.sourceAvroSchema);
    conf.setBoolean(FlinkOptions.UTC_TIMEZONE, config.utcTimezone);
    conf.setBoolean(FlinkOptions.URL_ENCODE_PARTITIONING, config.writePartitionUrlEncode);
    conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, config.hiveStylePartitioning);
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, config.writeTaskMaxSize);
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, config.writeBatchSize);
    conf.setInteger(FlinkOptions.WRITE_LOG_BLOCK_SIZE, config.writeLogBlockSize);
    conf.setLong(FlinkOptions.WRITE_LOG_MAX_SIZE, config.writeLogMaxSize);
    conf.setInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY, config.writeMergeMaxMemory);
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, config.compactionAsyncEnabled);
    conf.setInteger(FlinkOptions.COMPACTION_TASKS, config.compactionTasks);
    conf.setString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, config.compactionTriggerStrategy);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, config.compactionDeltaCommits);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, config.compactionDeltaSeconds);
    conf.setInteger(FlinkOptions.COMPACTION_MAX_MEMORY, config.compactionMaxMemory);
    conf.setLong(FlinkOptions.COMPACTION_TARGET_IO, config.compactionTargetIo);
    conf.setBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED, config.cleanAsyncEnabled);
    conf.setString(FlinkOptions.CLEAN_POLICY, config.cleanPolicy);
    conf.setInteger(FlinkOptions.CLEAN_RETAIN_COMMITS, config.cleanRetainCommits);
    conf.setInteger(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS, config.cleanRetainFileVersions);
    conf.setInteger(FlinkOptions.ARCHIVE_MAX_COMMITS, config.archiveMaxCommits);
    conf.setInteger(FlinkOptions.ARCHIVE_MIN_COMMITS, config.archiveMinCommits);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_ENABLED, config.hiveSyncEnabled);
    conf.setString(FlinkOptions.HIVE_SYNC_DB, config.hiveSyncDb);
    conf.setString(FlinkOptions.HIVE_SYNC_TABLE, config.hiveSyncTable);
    conf.setString(FlinkOptions.HIVE_SYNC_FILE_FORMAT, config.hiveSyncFileFormat);
    conf.setString(FlinkOptions.HIVE_SYNC_MODE, config.hiveSyncMode);
    conf.setString(FlinkOptions.HIVE_SYNC_USERNAME, config.hiveSyncUsername);
    conf.setString(FlinkOptions.HIVE_SYNC_PASSWORD, config.hiveSyncPassword);
    conf.setString(FlinkOptions.HIVE_SYNC_JDBC_URL, config.hiveSyncJdbcUrl);
    conf.setString(FlinkOptions.HIVE_SYNC_METASTORE_URIS, config.hiveSyncMetastoreUri);
    conf.setString(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS, config.hiveSyncPartitionFields);
    conf.setString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME, config.hiveSyncPartitionExtractorClass);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION, config.hiveSyncAssumeDatePartition);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC, config.hiveSyncUseJdbc);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB, config.hiveSyncAutoCreateDb);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS, config.hiveSyncIgnoreExceptions);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX, config.hiveSyncSkipRoSuffix);
    conf.setBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP, config.hiveSyncSupportTimestamp);
    return conf;
  }
}
