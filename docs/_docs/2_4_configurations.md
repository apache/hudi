---
title: Configurations
keywords: garbage collection, hudi, jvm, configs, tuning
permalink: /docs/configurations.html
summary: This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.
toc: true
last_modified_at: 2021-07-18T04:26:23.024238
---

This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.
- [**Record Payload Config**](#RECORD_PAYLOAD): This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.

### Read Options {#Read-Options}

Options useful for reading tables via `read.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br>
> #### hoodie.file.index.enable
> <br>
> `Default Value: true`<br>
> `Config Param: ENABLE_HOODIE_FILE_INDEX`<br>

---

> #### hoodie.datasource.merge.type
> <br>
> `Default Value: payload_combine`<br>
> `Config Param: REALTIME_MERGE_OPT_KEY`<br>

---

> #### hoodie.datasource.read.incr.path.glob
> <br>
> `Default Value: `<br>
> `Config Param: INCR_PATH_GLOB_OPT_KEY`<br>

---

> #### hoodie.datasource.query.type
> Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on columnar data) (or) Snapshot mode (obtain latest view, based on row & columnar data)<br>
> `Default Value: snapshot`<br>
> `Config Param: QUERY_TYPE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> `Default Value: ts`<br>
> `Config Param: READ_PRE_COMBINE_FIELD`<br>

---

> #### hoodie.datasource.read.end.instanttime
> Instant time to limit incrementally fetched data to. New data written with an instant_time <= END_INSTANTTIME are fetched out.<br>
> `Default Value: none`<br>
> `Config Param: END_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.begin.instanttime
> Instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.<br>
> `Default Value: none`<br>
> `Config Param: BEGIN_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.schema.use.end.instanttime
> Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.<br>
> `Default Value: false`<br>
> `Config Param: INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.incr.filters
> <br>
> `Default Value: `<br>
> `Config Param: PUSH_DOWN_INCR_FILTERS_OPT_KEY`<br>

---

> #### hoodie.datasource.read.paths
> <br>
> `Default Value: none`<br>
> `Config Param: READ_PATHS_OPT_KEY`<br>

---

### Write Options {#Write-Options}

You can pass down any of the WriteClient level configs directly using `options()` or `option(k,v)` methods.

```java
inputDF.write()
.format("org.apache.hudi")
.options(clientOpts) // any of the Hudi client opts can be passed in as well
.option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
.option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
.option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
.option(HoodieWriteConfig.TABLE_NAME, tableName)
.mode(SaveMode.Append)
.save(basePath);
```

Options useful for writing tables via `write.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br>
> #### hoodie.datasource.hive_sync.database
> database to sync to<br>
> `Default Value: default`<br>
> `Config Param: HIVE_DATABASE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> `Default Value: ts`<br>
> `Config Param: PRECOMBINE_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.username
> hive user name to use<br>
> `Default Value: hive`<br>
> `Config Param: HIVE_USER_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.table_properties
> <br>
> `Default Value: none`<br>
> `Config Param: HIVE_TABLE_PROPERTIES`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements will extract the key out of incoming Row object<br>
> `Default Value: none`<br>
> `Config Param: KEYGENERATOR_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.base_file_format
> <br>
> `Default Value: PARQUET`<br>
> `Config Param: HIVE_BASE_FILE_FORMAT_OPT_KEY`<br>

---

> #### hoodie.datasource.write.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br>
> `Default Value: COPY_ON_WRITE`<br>
> `Config Param: TABLE_TYPE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.operation
> Whether to do upsert, insert or bulkinsert for the write operation. Use bulkinsert to load new data into a table, and there on use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br>
> `Default Value: upsert`<br>
> `Config Param: OPERATION_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.retry.interval.ms
> <br>
> `Default Value: 2000`<br>
> `Config Param: STREAMING_RETRY_INTERVAL_MS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br>
> `Default Value: false`<br>
> `Config Param: HIVE_STYLE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.serde_properties
> <br>
> `Default Value: none`<br>
> `Config Param: HIVE_TABLE_SERDE_PROPERTIES`<br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br>
> `Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload`<br>
> `Config Param: PAYLOAD_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.assume_date_partitioning
> Assume partitioning is yyyy/mm/dd<br>
> `Default Value: false`<br>
> `Config Param: HIVE_ASSUME_DATE_PARTITION_OPT_KEY`<br>

---

> #### hoodie.meta.sync.client.tool.class
> <br>
> `Default Value: org.apache.hudi.hive.HiveSyncTool`<br>
> `Config Param: META_SYNC_CLIENT_TOOL_CLASS`<br>

---

> #### hoodie.datasource.hive_sync.password
> hive password to use<br>
> `Default Value: hive`<br>
> `Config Param: HIVE_PASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.partition_fields
> field in the table to use for determining hive partition columns.<br>
> `Default Value: `<br>
> `Config Param: HIVE_PARTITION_FIELDS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br>
> `Default Value: uuid`<br>
> `Config Param: RECORDKEY_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.write.commitmeta.key.prefix
> Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. This is useful to store checkpointing information, in a consistent way with the hudi timeline<br>
> `Default Value: _`<br>
> `Config Param: COMMIT_METADATA_KEYPREFIX_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.table
> table to sync to<br>
> `Default Value: unknown`<br>
> `Config Param: HIVE_TABLE_OPT_KEY`<br>

---

> #### hoodie.deltastreamer.source.kafka.value.deserializer.class
> <br>
> `Default Value: none`<br>
> `Config Param: KAFKA_AVRO_VALUE_DESERIALIZER`<br>

---

> #### hoodie.datasource.hive_sync.auto_create_database
> Auto create hive database if does not exists<br>
> `Default Value: true`<br>
> `Config Param: HIVE_AUTO_CREATE_DATABASE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.table.name
> Hive table name, to register the table into.<br>
> `Default Value: none`<br>
> `Config Param: TABLE_NAME_OPT_KEY`<br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> <br>
> `Default Value: false`<br>
> `Config Param: URL_ENCODE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.support_timestamp
> ‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. Disabled by default for backward compatibility.<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SUPPORT_TIMESTAMP`<br>

---

> #### hoodie.datasource.hive_sync.jdbcurl
> Hive metastore url<br>
> `Default Value: jdbc:hive2://localhost:10000`<br>
> `Config Param: HIVE_URL_OPT_KEY`<br>

---

> #### hoodie.datasource.compaction.async.enable
> <br>
> `Default Value: true`<br>
> `Config Param: ASYNC_COMPACT_ENABLE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.retry.count
> <br>
> `Default Value: 3`<br>
> `Config Param: STREAMING_RETRY_CNT_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.ignore_exceptions
> <br>
> `Default Value: false`<br>
> `Config Param: HIVE_IGNORE_EXCEPTIONS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.insert.drop.duplicates
> If set to true, filters out all duplicate records from incoming dataframe, during insert operations.<br>
> `Default Value: false`<br>
> `Config Param: INSERT_DROP_DUPS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.partition_extractor_class
> <br>
> `Default Value: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor`<br>
> `Config Param: HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.use_pre_apache_input_format
> <br>
> `Default Value: false`<br>
> `Config Param: HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY`<br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br>
> `Default Value: partitionpath`<br>
> `Config Param: PARTITIONPATH_FIELD_OPT_KEY`<br>

---

> #### hoodie.deltastreamer.source.kafka.value.deserializer.schema
> <br>
> `Default Value: none`<br>
> `Config Param: KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA`<br>

---

> #### hoodie.datasource.hive_sync.sync_as_datasource
> <br>
> `Default Value: true`<br>
> `Config Param: HIVE_SYNC_AS_DATA_SOURCE_TABLE`<br>

---

> #### hoodie.datasource.hive_sync.skip_ro_suffix
> Skip the _ro suffix for Read optimized table, when registering<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SKIP_RO_SUFFIX`<br>

---

> #### hoodie.datasource.meta.sync.enable
> <br>
> `Default Value: false`<br>
> `Config Param: META_SYNC_ENABLED_OPT_KEY`<br>

---

> #### hoodie.datasource.write.row.writer.enable
> <br>
> `Default Value: false`<br>
> `Config Param: ENABLE_ROW_WRITER_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.ignore.failed.batch
> <br>
> `Default Value: true`<br>
> `Config Param: STREAMING_IGNORE_FAILED_BATCH_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.enable
> When set to true, register/sync the table to Apache Hive metastore<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_ENABLED_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.use_jdbc
> Use JDBC when hive synchronization is enabled<br>
> `Default Value: true`<br>
> `Config Param: HIVE_USE_JDBC_OPT_KEY`<br>

---

Flink jobs using the SQL can be configured through the options in WITH clause. The actual datasource level configs are listed below.

`Config Class`: org.apache.hudi.configuration.FlinkOptions<br>
> #### read.streaming.enabled
> org.apache.flink.configuration.description.Description@53bc8c7e<br>
> `Default Value: false`<br>
> `Config Param: READ_AS_STREAMING`<br>

---

> #### hoodie.datasource.write.keygenerator.type
> org.apache.flink.configuration.description.Description@8ff4795<br>
> `Default Value: SIMPLE`<br>
> `Config Param: KEYGEN_TYPE`<br>

---

> #### compaction.trigger.strategy
> org.apache.flink.configuration.description.Description@19bdfa3e<br>
> `Default Value: num_commits`<br>
> `Config Param: COMPACTION_TRIGGER_STRATEGY`<br>

---

> #### index.state.ttl
> org.apache.flink.configuration.description.Description@78e3bebd<br>
> `Default Value: 1.5`<br>
> `Config Param: INDEX_STATE_TTL`<br>

---

> #### compaction.max_memory
> org.apache.flink.configuration.description.Description@4f0b02a3<br>
> `Default Value: 100`<br>
> `Config Param: COMPACTION_MAX_MEMORY`<br>

---

> #### hive_sync.support_timestamp
> org.apache.flink.configuration.description.Description@3d3a3738<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_SUPPORT_TIMESTAMP`<br>

---

> #### hive_sync.skip_ro_suffix
> org.apache.flink.configuration.description.Description@4ba056ab<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_SKIP_RO_SUFFIX`<br>

---

> #### hive_sync.assume_date_partitioning
> org.apache.flink.configuration.description.Description@2e530f34<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_ASSUME_DATE_PARTITION`<br>

---

> #### hive_sync.table
> org.apache.flink.configuration.description.Description@39da0e47<br>
> `Default Value: unknown`<br>
> `Config Param: HIVE_SYNC_TABLE`<br>

---

> #### compaction.tasks
> org.apache.flink.configuration.description.Description@55b56db3<br>
> `Default Value: 10`<br>
> `Config Param: COMPACTION_TASKS`<br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> org.apache.flink.configuration.description.Description@1c697ca0<br>
> `Default Value: false`<br>
> `Config Param: HIVE_STYLE_PARTITIONING`<br>

---

> #### table.type
> org.apache.flink.configuration.description.Description@2af5eab6<br>
> `Default Value: COPY_ON_WRITE`<br>
> `Config Param: TABLE_TYPE`<br>

---

> #### hive_sync.partition_extractor_class
> org.apache.flink.configuration.description.Description@43347199<br>
> `Default Value: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor`<br>
> `Config Param: HIVE_SYNC_PARTITION_EXTRACTOR_CLASS`<br>

---

> #### hive_sync.auto_create_db
> org.apache.flink.configuration.description.Description@3d1254b9<br>
> `Default Value: true`<br>
> `Config Param: HIVE_SYNC_AUTO_CREATE_DB`<br>

---

> #### hive_sync.username
> org.apache.flink.configuration.description.Description@6a49b3c7<br>
> `Default Value: hive`<br>
> `Config Param: HIVE_SYNC_USERNAME`<br>

---

> #### hive_sync.enable
> org.apache.flink.configuration.description.Description@6f6c4462<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_ENABLED`<br>

---

> #### read.streaming.check-interval
> org.apache.flink.configuration.description.Description@f3458af<br>
> `Default Value: 60`<br>
> `Config Param: READ_STREAMING_CHECK_INTERVAL`<br>

---

> #### hoodie.datasource.merge.type
> org.apache.flink.configuration.description.Description@20ac726c<br>
> `Default Value: payload_combine`<br>
> `Config Param: MERGE_TYPE`<br>

---

> #### write.retry.times
> org.apache.flink.configuration.description.Description@75c2a35<br>
> `Default Value: 3`<br>
> `Config Param: RETRY_TIMES`<br>

---

> #### read.tasks
> org.apache.flink.configuration.description.Description@a9e31e8<br>
> `Default Value: 4`<br>
> `Config Param: READ_TASKS`<br>

---

> #### write.log.max.size
> org.apache.flink.configuration.description.Description@27c2f134<br>
> `Default Value: 1024`<br>
> `Config Param: WRITE_LOG_MAX_SIZE`<br>

---

> #### hive_sync.file_format
> org.apache.flink.configuration.description.Description@64bed8b2<br>
> `Default Value: PARQUET`<br>
> `Config Param: HIVE_SYNC_FILE_FORMAT`<br>

---

> #### write.retry.interval.ms
> org.apache.flink.configuration.description.Description@2555b92<br>
> `Default Value: 2000`<br>
> `Config Param: RETRY_INTERVAL_MS`<br>

---

> #### hive_sync.db
> org.apache.flink.configuration.description.Description@793f2b41<br>
> `Default Value: default`<br>
> `Config Param: HIVE_SYNC_DB`<br>

---

> #### hive_sync.password
> org.apache.flink.configuration.description.Description@49442e03<br>
> `Default Value: hive`<br>
> `Config Param: HIVE_SYNC_PASSWORD`<br>

---

> #### hive_sync.use_jdbc
> org.apache.flink.configuration.description.Description@5bad04d1<br>
> `Default Value: true`<br>
> `Config Param: HIVE_SYNC_USE_JDBC`<br>

---

> #### hive_sync.jdbc_url
> org.apache.flink.configuration.description.Description@730c4dfd<br>
> `Default Value: jdbc:hive2://localhost:10000`<br>
> `Config Param: HIVE_SYNC_JDBC_URL`<br>

---

> #### write.batch.size
> org.apache.flink.configuration.description.Description@736905fe<br>
> `Default Value: 64.0`<br>
> `Config Param: WRITE_BATCH_SIZE`<br>

---

> #### archive.min_commits
> org.apache.flink.configuration.description.Description@1bb509a6<br>
> `Default Value: 20`<br>
> `Config Param: ARCHIVE_MIN_COMMITS`<br>

---

> #### index.global.enabled
> org.apache.flink.configuration.description.Description@280d1d8d<br>
> `Default Value: false`<br>
> `Config Param: INDEX_GLOBAL_ENABLED`<br>

---

> #### write.insert.drop.duplicates
> org.apache.flink.configuration.description.Description@397fced4<br>
> `Default Value: false`<br>
> `Config Param: INSERT_DROP_DUPS`<br>

---

> #### index.partition.regex
> org.apache.flink.configuration.description.Description@2026af0<br>
> `Default Value: .*`<br>
> `Config Param: INDEX_PARTITION_REGEX`<br>

---

> #### read.streaming.start-commit
> org.apache.flink.configuration.description.Description@31c9bb2f<br>
> `Default Value: none`<br>
> `Config Param: READ_STREAMING_START_COMMIT`<br>

---

> #### hoodie.table.name
> org.apache.flink.configuration.description.Description@71e839ee<br>
> `Default Value: none`<br>
> `Config Param: TABLE_NAME`<br>

---

> #### path
> org.apache.flink.configuration.description.Description@36a65069<br>
> `Default Value: none`<br>
> `Config Param: PATH`<br>

---

> #### index.bootstrap.enabled
> org.apache.flink.configuration.description.Description@2762e9a7<br>
> `Default Value: false`<br>
> `Config Param: INDEX_BOOTSTRAP_ENABLED`<br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> org.apache.flink.configuration.description.Description@5b8e2ea7<br>
> `Default Value: false`<br>
> `Config Param: URL_ENCODE_PARTITIONING`<br>

---

> #### compaction.async.enabled
> org.apache.flink.configuration.description.Description@6b994b71<br>
> `Default Value: true`<br>
> `Config Param: COMPACTION_ASYNC_ENABLED`<br>

---

> #### hive_sync.ignore_exceptions
> org.apache.flink.configuration.description.Description@6fb87b73<br>
> `Default Value: false`<br>
> `Config Param: HIVE_SYNC_IGNORE_EXCEPTIONS`<br>

---

> #### write.ignore.failed
> org.apache.flink.configuration.description.Description@5f2788f2<br>
> `Default Value: true`<br>
> `Config Param: IGNORE_FAILED`<br>

---

> #### write.commit.ack.timeout
> org.apache.flink.configuration.description.Description@75f67ea7<br>
> `Default Value: -1`<br>
> `Config Param: WRITE_COMMIT_ACK_TIMEOUT`<br>

---

> #### write.payload.class
> org.apache.flink.configuration.description.Description@549561ab<br>
> `Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload`<br>
> `Config Param: PAYLOAD_CLASS`<br>

---

> #### write.operation
> org.apache.flink.configuration.description.Description@785aeba9<br>
> `Default Value: upsert`<br>
> `Config Param: OPERATION`<br>

---

> #### hoodie.datasource.write.partitionpath.field
> org.apache.flink.configuration.description.Description@1bd98c48<br>
> `Default Value: `<br>
> `Config Param: PARTITION_PATH_FIELD`<br>

---

> #### compaction.delta_commits
> org.apache.flink.configuration.description.Description@5d9ccad2<br>
> `Default Value: 5`<br>
> `Config Param: COMPACTION_DELTA_COMMITS`<br>

---

> #### partition.default_name
> org.apache.flink.configuration.description.Description@22fb60f3<br>
> `Default Value: __DEFAULT_PARTITION__`<br>
> `Config Param: PARTITION_DEFAULT_NAME`<br>

---

> #### compaction.target_io
> org.apache.flink.configuration.description.Description@dd3d0a6<br>
> `Default Value: 5120`<br>
> `Config Param: COMPACTION_TARGET_IO`<br>

---

> #### write.log_block.size
> org.apache.flink.configuration.description.Description@5abfb698<br>
> `Default Value: 128`<br>
> `Config Param: WRITE_LOG_BLOCK_SIZE`<br>

---

> #### write.tasks
> org.apache.flink.configuration.description.Description@61ce2d47<br>
> `Default Value: 4`<br>
> `Config Param: WRITE_TASKS`<br>

---

> #### clean.async.enabled
> org.apache.flink.configuration.description.Description@184b3575<br>
> `Default Value: true`<br>
> `Config Param: CLEAN_ASYNC_ENABLED`<br>

---

> #### clean.retain_commits
> org.apache.flink.configuration.description.Description@b0e903a<br>
> `Default Value: 10`<br>
> `Config Param: CLEAN_RETAIN_COMMITS`<br>

---

> #### read.utc-timezone
> org.apache.flink.configuration.description.Description@2ca2fcb5<br>
> `Default Value: true`<br>
> `Config Param: UTC_TIMEZONE`<br>

---

> #### archive.max_commits
> org.apache.flink.configuration.description.Description@3c964873<br>
> `Default Value: 30`<br>
> `Config Param: ARCHIVE_MAX_COMMITS`<br>

---

> #### hoodie.datasource.query.type
> org.apache.flink.configuration.description.Description@7db72209<br>
> `Default Value: snapshot`<br>
> `Config Param: QUERY_TYPE`<br>

---

> #### read.avro-schema.path
> org.apache.flink.configuration.description.Description@2c7e2c5<br>
> `Default Value: none`<br>
> `Config Param: READ_AVRO_SCHEMA_PATH`<br>

---

> #### write.precombine.field
> org.apache.flink.configuration.description.Description@39bbe17c<br>
> `Default Value: ts`<br>
> `Config Param: PRECOMBINE_FIELD`<br>

---

> #### read.avro-schema
> org.apache.flink.configuration.description.Description@3760f3e8<br>
> `Default Value: none`<br>
> `Config Param: READ_AVRO_SCHEMA`<br>

---

> #### write.task.max.size
> org.apache.flink.configuration.description.Description@73032867<br>
> `Default Value: 1024.0`<br>
> `Config Param: WRITE_TASK_MAX_SIZE`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> org.apache.flink.configuration.description.Description@4e6f3d08<br>
> `Default Value: `<br>
> `Config Param: KEYGEN_CLASS`<br>

---

> #### hoodie.datasource.write.recordkey.field
> org.apache.flink.configuration.description.Description@28b995b8<br>
> `Default Value: uuid`<br>
> `Config Param: RECORD_KEY_FIELD`<br>

---

> #### compaction.delta_seconds
> org.apache.flink.configuration.description.Description@18f6ccf4<br>
> `Default Value: 3600`<br>
> `Config Param: COMPACTION_DELTA_SECONDS`<br>

---

> #### hive_sync.metastore.uris
> org.apache.flink.configuration.description.Description@72dc9f9d<br>
> `Default Value: `<br>
> `Config Param: HIVE_SYNC_METASTORE_URIS`<br>

---

> #### hive_sync.partition_fields
> org.apache.flink.configuration.description.Description@45ec6bb3<br>
> `Default Value: `<br>
> `Config Param: HIVE_SYNC_PARTITION_FIELDS`<br>

---

> #### write.merge.max_memory
> org.apache.flink.configuration.description.Description@219c32e3<br>
> `Default Value: 100`<br>
> `Config Param: WRITE_MERGE_MAX_MEMORY`<br>

---

## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.

### Consistency Guard Configurations {#Consistency-Guard-Configurations}

The consistency guard relevant config options.

`Config Class`: org.apache.hudi.common.fs.ConsistencyGuardConfig<br>
> #### hoodie.optimistic.consistency.guard.sleep_time_ms
> <br>
> `Default Value: 500`<br>
> `Config Param: OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.consistency.check.max_interval_ms
> <br>
> `Default Value: 20000`<br>
> `Config Param: MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.consistency.check.enabled
> Enabled to handle S3 eventual consistency issue. This property is no longer required since S3 is now strongly consistent. Will be removed in the future releases.<br>
> `Default Value: false`<br>
> `Config Param: CONSISTENCY_CHECK_ENABLED_PROP`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.consistency.check.initial_interval_ms
> <br>
> `Default Value: 400`<br>
> `Config Param: INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.consistency.check.max_checks
> <br>
> `Default Value: 6`<br>
> `Config Param: MAX_CONSISTENCY_CHECKS_PROP`<br>
> `Since Version: 0.5.0`<br>

---

> #### _hoodie.optimistic.consistency.guard.enable
> <br>
> `Default Value: true`<br>
> `Config Param: ENABLE_OPTIMISTIC_CONSISTENCY_GUARD_PROP`<br>
> `Since Version: 0.6.0`<br>

---

### Write Configurations {#Write-Configurations}

The datasource can be configured by passing the below options. These options are useful when writing tables.

`Config Class`: org.apache.hudi.config.HoodieWriteConfig<br>
> #### hoodie.embed.timeline.server.reuse.enabled
> <br>
> `Default Value: false`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements will extract the key out of incoming Row object<br>
> `Default Value: none`<br>
> `Config Param: KEYGENERATOR_CLASS_PROP`<br>

---

> #### hoodie.bulkinsert.shuffle.parallelism
> Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your table. Tune this to achieve a desired optimal size during initial import.<br>
> `Default Value: 1500`<br>
> `Config Param: BULKINSERT_PARALLELISM`<br>

---

> #### hoodie.rollback.parallelism
> Determines the parallelism for rollback of commits.<br>
> `Default Value: 100`<br>
> `Config Param: ROLLBACK_PARALLELISM`<br>

---

> #### hoodie.write.schema
> <br>
> `Default Value: none`<br>
> `Config Param: WRITE_SCHEMA_PROP`<br>

---

> #### hoodie.timeline.layout.version
> <br>
> `Default Value: none`<br>
> `Config Param: TIMELINE_LAYOUT_VERSION`<br>

---

> #### hoodie.avro.schema.validate
> <br>
> `Default Value: false`<br>
> `Config Param: AVRO_SCHEMA_VALIDATE`<br>

---

> #### hoodie.bulkinsert.sort.mode
> Sorting modes to use for sorting records for bulk insert. This is leveraged when user defined partitioner is not configured. Default is GLOBAL_SORT. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches spark.write.parquet() in terms of number of files, overheads<br>
> `Default Value: GLOBAL_SORT`<br>
> `Config Param: BULKINSERT_SORT_MODE`<br>

---

> #### hoodie.upsert.shuffle.parallelism
> Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data<br>
> `Default Value: 1500`<br>
> `Config Param: UPSERT_PARALLELISM`<br>

---

> #### hoodie.embed.timeline.server.gzip
> <br>
> `Default Value: true`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT`<br>

---

> #### hoodie.fail.on.timeline.archiving
> <br>
> `Default Value: true`<br>
> `Config Param: FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP`<br>

---

> #### hoodie.write.status.storage.level
> HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD<br>
> `Default Value: MEMORY_AND_DISK_SER`<br>
> `Config Param: WRITE_STATUS_STORAGE_LEVEL`<br>

---

> #### _.hoodie.allow.multi.write.on.same.instant
> <br>
> `Default Value: false`<br>
> `Config Param: ALLOW_MULTI_WRITE_ON_SAME_INSTANT`<br>

---

> #### hoodie.embed.timeline.server.port
> <br>
> `Default Value: 0`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_PORT`<br>

---

> #### hoodie.consistency.check.max_interval_ms
> Max interval time for consistency check<br>
> `Default Value: 300000`<br>
> `Config Param: MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>

---

> #### hoodie.write.concurrency.mode
> Enable different concurrency support<br>
> `Default Value: SINGLE_WRITER`<br>
> `Config Param: WRITE_CONCURRENCY_MODE_PROP`<br>

---

> #### hoodie.bulkinsert.schema.ddl
> <br>
> `Default Value: none`<br>
> `Config Param: BULKINSERT_INPUT_DATA_SCHEMA_DDL`<br>

---

> #### hoodie.delete.shuffle.parallelism
> This parallelism is Used for “delete” operation while deduping or repartioning.<br>
> `Default Value: 1500`<br>
> `Config Param: DELETE_PARALLELISM`<br>

---

> #### hoodie.embed.timeline.server
> <br>
> `Default Value: true`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_ENABLED`<br>

---

> #### hoodie.base.path
> Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory.<br>
> `Default Value: none`<br>
> `Config Param: BASE_PATH_PROP`<br>

---

> #### hoodie.avro.schema
> This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update.<br>
> `Default Value: none`<br>
> `Config Param: AVRO_SCHEMA`<br>

---

> #### hoodie.table.name
> Table name that will be used for registering with Hive. Needs to be same across runs.<br>
> `Default Value: none`<br>
> `Config Param: TABLE_NAME`<br>

---

> #### hoodie.embed.timeline.server.async
> <br>
> `Default Value: false`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_USE_ASYNC`<br>

---

> #### hoodie.combine.before.upsert
> Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS<br>
> `Default Value: true`<br>
> `Config Param: COMBINE_BEFORE_UPSERT_PROP`<br>

---

> #### hoodie.embed.timeline.server.threads
> <br>
> `Default Value: -1`<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_THREADS`<br>

---

> #### hoodie.consistency.check.max_checks
> Maximum number of checks, for consistency of written data. Will wait upto 256 Secs<br>
> `Default Value: 7`<br>
> `Config Param: MAX_CONSISTENCY_CHECKS_PROP`<br>

---

> #### hoodie.combine.before.delete
> Flag which first combines the input RDD and merges multiple partial records into a single record before deleting in DFS<br>
> `Default Value: true`<br>
> `Config Param: COMBINE_BEFORE_DELETE_PROP`<br>

---

> #### hoodie.datasource.write.keygenerator.type
> Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br>
> `Default Value: SIMPLE`<br>
> `Config Param: KEYGENERATOR_TYPE_PROP`<br>

---

> #### hoodie.write.buffer.limit.bytes
> <br>
> `Default Value: 4194304`<br>
> `Config Param: WRITE_BUFFER_LIMIT_BYTES`<br>

---

> #### hoodie.bulkinsert.user.defined.partitioner.class
> If specified, this class will be used to re-partition input records before they are inserted.<br>
> `Default Value: none`<br>
> `Config Param: BULKINSERT_USER_DEFINED_PARTITIONER_CLASS`<br>

---

> #### hoodie.client.heartbeat.interval_in_ms
> <br>
> `Default Value: 60000`<br>
> `Config Param: CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP`<br>

---

> #### hoodie.avro.schema.externalTransformation
> <br>
> `Default Value: false`<br>
> `Config Param: EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION`<br>

---

> #### hoodie.client.heartbeat.tolerable.misses
> <br>
> `Default Value: 2`<br>
> `Config Param: CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP`<br>

---

> #### hoodie.auto.commit
> Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a “defined success condition”<br>
> `Default Value: true`<br>
> `Config Param: HOODIE_AUTO_COMMIT_PROP`<br>

---

> #### hoodie.combine.before.insert
> Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS<br>
> `Default Value: false`<br>
> `Config Param: COMBINE_BEFORE_INSERT_PROP`<br>

---

> #### hoodie.writestatus.class
> <br>
> `Default Value: org.apache.hudi.client.WriteStatus`<br>
> `Config Param: HOODIE_WRITE_STATUS_CLASS_PROP`<br>

---

> #### hoodie.markers.delete.parallelism
> Determines the parallelism for deleting marker files.<br>
> `Default Value: 100`<br>
> `Config Param: MARKERS_DELETE_PARALLELISM`<br>

---

> #### hoodie.consistency.check.initial_interval_ms
> Time between successive attempts to ensure written data's metadata is consistent on storage<br>
> `Default Value: 2000`<br>
> `Config Param: INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>

---

> #### hoodie.merge.data.validation.enabled
> Data validation check performed during merges before actual commits<br>
> `Default Value: false`<br>
> `Config Param: MERGE_DATA_VALIDATION_CHECK_ENABLED`<br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br>
> `Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload`<br>
> `Config Param: WRITE_PAYLOAD_CLASS`<br>

---

> #### hoodie.insert.shuffle.parallelism
> Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data<br>
> `Default Value: 1500`<br>
> `Config Param: INSERT_PARALLELISM`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> `Default Value: ts`<br>
> `Config Param: PRECOMBINE_FIELD_PROP`<br>

---

> #### hoodie.write.meta.key.prefixes
> Comma separated metadata key prefixes to override from latest commit during overlapping commits via multi writing<br>
> `Default Value: `<br>
> `Config Param: WRITE_META_KEY_PREFIXES_PROP`<br>

---

> #### hoodie.finalize.write.parallelism
> <br>
> `Default Value: 1500`<br>
> `Config Param: FINALIZE_WRITE_PARALLELISM`<br>

---

> #### hoodie.merge.allow.duplicate.on.inserts
> Allow duplicates with inserts while merging with existing records<br>
> `Default Value: false`<br>
> `Config Param: MERGE_ALLOW_DUPLICATE_ON_INSERTS`<br>

---

> #### hoodie.rollback.using.markers
> Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned off by default.<br>
> `Default Value: false`<br>
> `Config Param: ROLLBACK_USING_MARKERS`<br>

---

### Payload Configurations {#Payload-Configurations}

Payload related configs. This config can be leveraged by payload implementations to determine their business logic.

`Config Class`: org.apache.hudi.config.HoodiePayloadConfig<br>
> #### hoodie.payload.event.time.field
> Property for payload event time field<br>
> `Default Value: ts`<br>
> `Config Param: PAYLOAD_EVENT_TIME_FIELD_PROP`<br>

---

> #### hoodie.payload.ordering.field
> Property to hold the payload ordering field name<br>
> `Default Value: ts`<br>
> `Config Param: PAYLOAD_ORDERING_FIELD_PROP`<br>

---

### HBase Index Configs {#HBase-Index-Configs}

Configurations that control indexing behavior (when HBase based indexing is enabled), which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieHBaseIndexConfig<br>
> #### hoodie.index.hbase.qps.fraction
> Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively. Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.<br>
> `Default Value: 0.5`<br>
> `Config Param: HBASE_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.zknode.path
> Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase<br>
> `Default Value: none`<br>
> `Config Param: HBASE_ZK_ZNODEPARENT`<br>

---

> #### hoodie.index.hbase.zkpath.qps_root
> <br>
> `Default Value: /QPS_ROOT`<br>
> `Config Param: HBASE_ZK_PATH_QPS_ROOT`<br>

---

> #### hoodie.index.hbase.put.batch.size
> <br>
> `Default Value: 100`<br>
> `Config Param: HBASE_PUT_BATCH_SIZE_PROP`<br>

---

> #### hoodie.index.hbase.dynamic_qps
> Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on volume<br>
> `Default Value: false`<br>
> `Config Param: HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY`<br>

---

> #### hoodie.index.hbase.max.qps.per.region.server
> Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
 limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this
 value based on global indexing throughput needs and most importantly, how much the HBase installation in use is
 able to tolerate without Region Servers going down.<br>
> `Default Value: 1000`<br>
> `Config Param: HBASE_MAX_QPS_PER_REGION_SERVER_PROP`<br>

---

> #### hoodie.index.hbase.zk.session_timeout_ms
> <br>
> `Default Value: 60000`<br>
> `Config Param: HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS`<br>

---

> #### hoodie.index.hbase.sleep.ms.for.get.batch
> <br>
> `Default Value: none`<br>
> `Config Param: HBASE_SLEEP_MS_GET_BATCH_PROP`<br>

---

> #### hoodie.index.hbase.min.qps.fraction
> Min for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads<br>
> `Default Value: none`<br>
> `Config Param: HBASE_MIN_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.desired_puts_time_in_secs
> <br>
> `Default Value: 600`<br>
> `Config Param: HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS`<br>

---

> #### hoodie.hbase.index.update.partition.path
> Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old paritition and will insert it as new record in new partition.<br>
> `Default Value: false`<br>
> `Config Param: HBASE_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.index.hbase.qps.allocator.class
> Property to set which implementation of HBase QPS resource allocator to be used<br>
> `Default Value: org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator`<br>
> `Config Param: HBASE_INDEX_QPS_ALLOCATOR_CLASS`<br>

---

> #### hoodie.index.hbase.sleep.ms.for.put.batch
> <br>
> `Default Value: none`<br>
> `Config Param: HBASE_SLEEP_MS_PUT_BATCH_PROP`<br>

---

> #### hoodie.index.hbase.rollback.sync
> When set to true, the rollback method will delete the last failed task index. The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback<br>
> `Default Value: false`<br>
> `Config Param: HBASE_INDEX_ROLLBACK_SYNC`<br>

---

> #### hoodie.index.hbase.zkquorum
> Only applies if index type is HBASE. HBase ZK Quorum url to connect to<br>
> `Default Value: none`<br>
> `Config Param: HBASE_ZKQUORUM_PROP`<br>

---

> #### hoodie.index.hbase.get.batch.size
> <br>
> `Default Value: 100`<br>
> `Config Param: HBASE_GET_BATCH_SIZE_PROP`<br>

---

> #### hoodie.index.hbase.zk.connection_timeout_ms
> <br>
> `Default Value: 15000`<br>
> `Config Param: HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS`<br>

---

> #### hoodie.index.hbase.put.batch.size.autocompute
> Property to set to enable auto computation of put batch size<br>
> `Default Value: false`<br>
> `Config Param: HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP`<br>

---

> #### hoodie.index.hbase.zkport
> Only applies if index type is HBASE. HBase ZK Quorum port to connect to<br>
> `Default Value: none`<br>
> `Config Param: HBASE_ZKPORT_PROP`<br>

---

> #### hoodie.index.hbase.max.qps.fraction
> Max for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads<br>
> `Default Value: none`<br>
> `Config Param: HBASE_MAX_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.table
> Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table<br>
> `Default Value: none`<br>
> `Config Param: HBASE_TABLENAME_PROP`<br>

---

### Write commit HTTP callback configs {#Write-commit-HTTP-callback-configs}

Controls HTTP callback behavior on write commit. Exception will be thrown if user enabled the callback service and errors occurred during the process of callback.

`Config Class`: org.apache.hudi.config.HoodieWriteCommitCallbackConfig<br>
> #### hoodie.write.commit.callback.http.api.key
> Http callback API key. hudi_write_commit_http_callback by default<br>
> `Default Value: hudi_write_commit_http_callback`<br>
> `Config Param: CALLBACK_HTTP_API_KEY`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.on
> Turn callback on/off. off by default.<br>
> `Default Value: false`<br>
> `Config Param: CALLBACK_ON`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.http.url
> Callback host to be sent along with callback messages<br>
> `Default Value: none`<br>
> `Config Param: CALLBACK_HTTP_URL_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.http.timeout.seconds
> Callback timeout in seconds. 3 by default<br>
> `Default Value: 3`<br>
> `Config Param: CALLBACK_HTTP_TIMEOUT_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.class
> Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default<br>
> `Default Value: org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback`<br>
> `Config Param: CALLBACK_CLASS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

### Write commit Kafka callback configs {#Write-commit-Kafka-callback-configs}

Controls Kafka callback behavior on write commit. Exception will be thrown if user enabled the callback service and errors occurred during the process of callback.

`Config Class`: org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig<br>
> #### hoodie.write.commit.callback.kafka.acks
> kafka acks level, all by default<br>
> `Default Value: all`<br>
> `Config Param: CALLBACK_KAFKA_ACKS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.bootstrap.servers
> Bootstrap servers of kafka callback cluster<br>
> `Default Value: none`<br>
> `Config Param: CALLBACK_KAFKA_BOOTSTRAP_SERVERS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.topic
> Kafka topic to be sent along with callback messages<br>
> `Default Value: none`<br>
> `Config Param: CALLBACK_KAFKA_TOPIC`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.retries
> Times to retry. 3 by default<br>
> `Default Value: 3`<br>
> `Config Param: CALLBACK_KAFKA_RETRIES`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.partition
> partition of CALLBACK_KAFKA_TOPIC, 0 by default<br>
> `Default Value: none`<br>
> `Config Param: CALLBACK_KAFKA_PARTITION`<br>
> `Since Version: 0.7.0`<br>

---

### Locks Configurations {#Locks-Configurations}

Configs that control locking mechanisms if WRITE_CONCURRENCY_MODE_PROP is set to optimistic_concurrency_control

`Config Class`: org.apache.hudi.config.HoodieLockConfig<br>
> #### hoodie.write.lock.zookeeper.url
> Set the list of comma separated servers to connect to<br>
> `Default Value: none`<br>
> `Config Param: ZK_CONNECT_URL_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.max_wait_time_ms_between_retry
> Parameter used in the exponential backoff retry policy. Stands for the maximum amount of time to wait between retries by lock provider client<br>
> `Default Value: 5000`<br>
> `Config Param: LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.filesystem.path
> <br>
> `Default Value: none`<br>
> `Config Param: FILESYSTEM_LOCK_PATH_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.uris
> <br>
> `Default Value: none`<br>
> `Config Param: HIVE_METASTORE_URI_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.wait_time_ms_between_retry
> Parameter used in the exponential backoff retry policy. Stands for the Initial amount of time to wait between retries by lock provider client<br>
> `Default Value: 5000`<br>
> `Config Param: LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.wait_time_ms
> <br>
> `Default Value: 60000`<br>
> `Config Param: LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.conflict.resolution.strategy
> Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy<br>
> `Default Value: org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy`<br>
> `Config Param: WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.num_retries
> Maximum number of times to retry by lock provider client<br>
> `Default Value: 3`<br>
> `Config Param: LOCK_ACQUIRE_NUM_RETRIES_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.connection_timeout_ms
> How long to wait when connecting to ZooKeeper before considering the connection a failure<br>
> `Default Value: 15000`<br>
> `Config Param: ZK_CONNECTION_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.client.num_retries
> Maximum number of times to retry to acquire lock additionally from the hudi client<br>
> `Default Value: 0`<br>
> `Config Param: LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.database
> The Hive database to acquire lock against<br>
> `Default Value: none`<br>
> `Config Param: HIVE_DATABASE_NAME_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.port
> The connection port to be used for Zookeeper<br>
> `Default Value: none`<br>
> `Config Param: ZK_PORT_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.client.wait_time_ms_between_retry
> Amount of time to wait between retries from the hudi client<br>
> `Default Value: 10000`<br>
> `Config Param: LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.lock_key
> Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. We recommend setting this to the table name<br>
> `Default Value: none`<br>
> `Config Param: ZK_LOCK_KEY_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.base_path
> The base path on Zookeeper under which to create a ZNode to acquire the lock. This should be common for all jobs writing to the same table<br>
> `Default Value: none`<br>
> `Config Param: ZK_BASE_PATH_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.session_timeout_ms
> How long to wait after losing a connection to ZooKeeper before the session is expired<br>
> `Default Value: 60000`<br>
> `Config Param: ZK_SESSION_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.table
> The Hive table under the hive database to acquire lock against<br>
> `Default Value: none`<br>
> `Config Param: HIVE_TABLE_NAME_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.provider
> Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider<br>
> `Default Value: org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider`<br>
> `Config Param: LOCK_PROVIDER_CLASS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

### Compaction Configs {#Compaction-Configs}

Configurations that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).

`Config Class`: org.apache.hudi.config.HoodieCompactionConfig<br>
> #### hoodie.compact.inline.trigger.strategy
> <br>
> `Default Value: NUM_COMMITS`<br>
> `Config Param: INLINE_COMPACT_TRIGGER_STRATEGY_PROP`<br>

---

> #### hoodie.cleaner.fileversions.retained
> <br>
> `Default Value: 3`<br>
> `Config Param: CLEANER_FILE_VERSIONS_RETAINED_PROP`<br>

---

> #### hoodie.cleaner.policy.failed.writes
> Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)<br>
> `Default Value: EAGER`<br>
> `Config Param: FAILED_WRITES_CLEANER_POLICY_PROP`<br>

---

> #### hoodie.parquet.small.file.limit
> Upsert uses this file size to compact new data onto existing files. By default, treat any file <= 100MB as a small file.<br>
> `Default Value: 104857600`<br>
> `Config Param: PARQUET_SMALL_FILE_LIMIT_BYTES`<br>

---

> #### hoodie.keep.max.commits
> Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.<br>
> `Default Value: 30`<br>
> `Config Param: MAX_COMMITS_TO_KEEP_PROP`<br>

---

> #### hoodie.compaction.lazy.block.read
> When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)<br>
> `Default Value: false`<br>
> `Config Param: COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP`<br>

---

> #### hoodie.clean.automatic
> Should cleanup if there is anything to cleanup immediately after the commit<br>
> `Default Value: true`<br>
> `Config Param: AUTO_CLEAN_PROP`<br>

---

> #### hoodie.commits.archival.batch
> This controls the number of commit instants read in memory as a batch and archived together.<br>
> `Default Value: 10`<br>
> `Config Param: COMMITS_ARCHIVAL_BATCH_SIZE_PROP`<br>

---

> #### hoodie.cleaner.parallelism
> Increase this if cleaning becomes slow.<br>
> `Default Value: 200`<br>
> `Config Param: CLEANER_PARALLELISM`<br>

---

> #### hoodie.cleaner.incremental.mode
> <br>
> `Default Value: true`<br>
> `Config Param: CLEANER_INCREMENTAL_MODE`<br>

---

> #### hoodie.cleaner.delete.bootstrap.base.file
> Set true to clean bootstrap source files when necessary<br>
> `Default Value: false`<br>
> `Config Param: CLEANER_BOOTSTRAP_BASE_FILE_ENABLED`<br>

---

> #### hoodie.copyonwrite.insert.split.size
> Number of inserts, that will be put each partition/bucket for writing. The rationale to pick the insert parallelism is the following. Writing out 100MB files, with at least 1kb records, means 100K records per file. we just over provision to 500K.<br>
> `Default Value: 500000`<br>
> `Config Param: COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE`<br>

---

> #### hoodie.compaction.strategy
> Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data<br>
> `Default Value: org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy`<br>
> `Config Param: COMPACTION_STRATEGY_PROP`<br>

---

> #### hoodie.compaction.target.io
> Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.<br>
> `Default Value: 512000`<br>
> `Config Param: TARGET_IO_PER_COMPACTION_IN_MB_PROP`<br>

---

> #### hoodie.compaction.payload.class
> This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.<br>
> `Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload`<br>
> `Config Param: PAYLOAD_CLASS_PROP`<br>

---

> #### hoodie.cleaner.commits.retained
> Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table<br>
> `Default Value: 10`<br>
> `Config Param: CLEANER_COMMITS_RETAINED_PROP`<br>

---

> #### hoodie.record.size.estimation.threshold
> Hudi will use the previous commit to calculate the estimated record size by totalBytesWritten/totalRecordsWritten. If the previous commit is too small to make an accurate estimation, Hudi will search commits in the reverse order, until find a commit has totalBytesWritten larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * RECORD_SIZE_ESTIMATION_THRESHOLD)<br>
> `Default Value: 1.0`<br>
> `Config Param: RECORD_SIZE_ESTIMATION_THRESHOLD_PROP`<br>

---

> #### hoodie.compaction.daybased.target.partitions
> Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.<br>
> `Default Value: 10`<br>
> `Config Param: TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP`<br>

---

> #### hoodie.keep.min.commits
> Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.<br>
> `Default Value: 20`<br>
> `Config Param: MIN_COMMITS_TO_KEEP_PROP`<br>

---

> #### hoodie.compaction.reverse.log.read
> HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0<br>
> `Default Value: false`<br>
> `Config Param: COMPACTION_REVERSE_LOG_READ_ENABLED_PROP`<br>

---

> #### hoodie.compact.inline
> When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert<br>
> `Default Value: false`<br>
> `Config Param: INLINE_COMPACT_PROP`<br>

---

> #### hoodie.clean.async
> Only applies when #withAutoClean is turned on. When turned on runs cleaner async with writing.<br>
> `Default Value: false`<br>
> `Config Param: ASYNC_CLEAN_PROP`<br>

---

> #### hoodie.copyonwrite.insert.auto.split
> Config to control whether we control insert split sizes automatically based on average record sizes.<br>
> `Default Value: true`<br>
> `Config Param: COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS`<br>

---

> #### hoodie.copyonwrite.record.size.estimate
> The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit’s metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.<br>
> `Default Value: 1024`<br>
> `Config Param: COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE`<br>

---

> #### hoodie.compact.inline.max.delta.commits
> Number of max delta commits to keep before triggering an inline compaction<br>
> `Default Value: 5`<br>
> `Config Param: INLINE_COMPACT_NUM_DELTA_COMMITS_PROP`<br>

---

> #### hoodie.compact.inline.max.delta.seconds
> Run a compaction when time elapsed > N seconds since last compaction<br>
> `Default Value: 3600`<br>
> `Config Param: INLINE_COMPACT_TIME_DELTA_SECONDS_PROP`<br>

---

> #### hoodie.cleaner.policy
> Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.<br>
> `Default Value: KEEP_LATEST_COMMITS`<br>
> `Config Param: CLEANER_POLICY_PROP`<br>

---

### File System View Storage Configurations {#File-System-View-Storage-Configurations}

Configurations that control the Filesystem view

`Config Class`: org.apache.hudi.common.table.view.FileSystemViewStorageConfig<br>
> #### hoodie.filesystem.view.spillable.replaced.mem.fraction
> <br>
> `Default Value: 0.01`<br>
> `Config Param: FILESYSTEM_VIEW_REPLACED_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.incr.timeline.sync.enable
> <br>
> `Default Value: false`<br>
> `Config Param: FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE`<br>

---

> #### hoodie.filesystem.view.secondary.type
> <br>
> `Default Value: MEMORY`<br>
> `Config Param: FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE`<br>

---

> #### hoodie.filesystem.view.spillable.compaction.mem.fraction
> <br>
> `Default Value: 0.8`<br>
> `Config Param: FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.spillable.mem
> <br>
> `Default Value: 104857600`<br>
> `Config Param: FILESYSTEM_VIEW_SPILLABLE_MEM`<br>

---

> #### hoodie.filesystem.view.spillable.clustering.mem.fraction
> <br>
> `Default Value: 0.01`<br>
> `Config Param: FILESYSTEM_VIEW_PENDING_CLUSTERING_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.remote.timeout.secs
> <br>
> `Default Value: 300`<br>
> `Config Param: FILESTYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS`<br>

---

> #### hoodie.filesystem.view.spillable.dir
> <br>
> `Default Value: /tmp/view_map/`<br>
> `Config Param: FILESYSTEM_VIEW_SPILLABLE_DIR`<br>

---

> #### hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction
> <br>
> `Default Value: 0.05`<br>
> `Config Param: FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION`<br>

---

> #### hoodie.filesystem.view.remote.port
> <br>
> `Default Value: 26754`<br>
> `Config Param: FILESYSTEM_VIEW_REMOTE_PORT`<br>

---

> #### hoodie.filesystem.view.type
> <br>
> `Default Value: MEMORY`<br>
> `Config Param: FILESYSTEM_VIEW_STORAGE_TYPE`<br>

---

> #### hoodie.filesystem.view.remote.host
> <br>
> `Default Value: localhost`<br>
> `Config Param: FILESYSTEM_VIEW_REMOTE_HOST`<br>

---

> #### hoodie.filesystem.view.rocksdb.base.path
> <br>
> `Default Value: /tmp/hoodie_timeline_rocksdb`<br>
> `Config Param: ROCKSDB_BASE_PATH_PROP`<br>

---

> #### hoodie.filesystem.remote.backup.view.enable
> <br>
> `Default Value: true`<br>
> `Config Param: REMOTE_BACKUP_VIEW_HANDLER_ENABLE`<br>

---

### Table Configurations {#Table-Configurations}

Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.

`Config Class`: org.apache.hudi.common.table.HoodieTableConfig<br>
> #### hoodie.table.partition.columns
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TABLE_PARTITION_COLUMNS_PROP`<br>

---

> #### hoodie.table.recordkey.fields
> <br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TABLE_RECORDKEY_FIELDS`<br>

---

> #### hoodie.archivelog.folder
> <br>
> `Default Value: archived`<br>
> `Config Param: HOODIE_ARCHIVELOG_FOLDER_PROP`<br>

---

> #### hoodie.table.create.schema
> <br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TABLE_CREATE_SCHEMA`<br>

---

> #### hoodie.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br>
> `Default Value: COPY_ON_WRITE`<br>
> `Config Param: HOODIE_TABLE_TYPE_PROP`<br>

---

> #### hoodie.table.base.file.format
> <br>
> `Default Value: PARQUET`<br>
> `Config Param: HOODIE_BASE_FILE_FORMAT_PROP`<br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br>
> `Default Value: none`<br>
> `Config Param: HOODIE_BOOTSTRAP_BASE_PATH_PROP`<br>

---

> #### hoodie.table.name
> Table name that will be used for registering with Hive. Needs to be same across runs.<br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TABLE_NAME_PROP`<br>

---

> #### hoodie.table.version
> <br>
> `Default Value: ZERO`<br>
> `Config Param: HOODIE_TABLE_VERSION_PROP`<br>

---

> #### hoodie.table.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TABLE_PRECOMBINE_FIELD_PROP`<br>

---

> #### hoodie.bootstrap.index.class
> <br>
> `Default Value: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex`<br>
> `Config Param: HOODIE_BOOTSTRAP_INDEX_CLASS_PROP`<br>

---

> #### hoodie.table.log.file.format
> <br>
> `Default Value: HOODIE_LOG`<br>
> `Config Param: HOODIE_LOG_FILE_FORMAT_PROP`<br>

---

> #### hoodie.bootstrap.index.enable
> <br>
> `Default Value: none`<br>
> `Config Param: HOODIE_BOOTSTRAP_INDEX_ENABLE_PROP`<br>

---

> #### hoodie.timeline.layout.version
> <br>
> `Default Value: none`<br>
> `Config Param: HOODIE_TIMELINE_LAYOUT_VERSION_PROP`<br>

---

> #### hoodie.compaction.payload.class
> <br>
> `Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload`<br>
> `Config Param: HOODIE_PAYLOAD_CLASS_PROP`<br>

---

### Memory Configurations {#Memory-Configurations}

Controls memory usage for compaction and merges, performed internally by Hudi.

`Config Class`: org.apache.hudi.config.HoodieMemoryConfig<br>
> #### hoodie.memory.merge.fraction
> This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge<br>
> `Default Value: 0.6`<br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_MERGE_PROP`<br>

---

> #### hoodie.memory.compaction.max.size
> Property to set the max memory for compaction<br>
> `Default Value: none`<br>
> `Config Param: MAX_MEMORY_FOR_COMPACTION_PROP`<br>

---

> #### hoodie.memory.spillable.map.path
> Default file path prefix for spillable file<br>
> `Default Value: /tmp/`<br>
> `Config Param: SPILLABLE_MAP_BASE_PATH_PROP`<br>

---

> #### hoodie.memory.compaction.fraction
> HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map<br>
> `Default Value: 0.6`<br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP`<br>

---

> #### hoodie.memory.writestatus.failure.fraction
> Property to control how what fraction of the failed record, exceptions we report back to driver. Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and mask actual data errors.<br>
> `Default Value: 0.1`<br>
> `Config Param: WRITESTATUS_FAILURE_FRACTION_PROP`<br>

---

> #### hoodie.memory.merge.max.size
> Property to set the max memory for merge<br>
> `Default Value: 1073741824`<br>
> `Config Param: MAX_MEMORY_FOR_MERGE_PROP`<br>

---

> #### hoodie.memory.dfs.buffer.max.size
> Property to set the max memory for dfs inputstream buffer size<br>
> `Default Value: 16777216`<br>
> `Config Param: MAX_DFS_STREAM_BUFFER_SIZE_PROP`<br>

---

### Index Configs {#Index-Configs}

Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieIndexConfig<br>
> #### hoodie.index.type
> Type of index to use. Default is Bloom filter. Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files<br>
> `Default Value: none`<br>
> `Config Param: INDEX_TYPE_PROP`<br>

---

> #### hoodie.bloom.index.use.treebased.filter
> Only applies if index type is BLOOM. When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode<br>
> `Default Value: true`<br>
> `Config Param: BLOOM_INDEX_TREE_BASED_FILTER_PROP`<br>

---

> #### hoodie.index.bloom.num_entries
> Only applies if index type is BLOOM. This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. HUDI-56 tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNNAMIC bloom filter which determines the initial size for the bloom.<br>
> `Default Value: 60000`<br>
> `Config Param: BLOOM_FILTER_NUM_ENTRIES`<br>

---

> #### hoodie.bloom.index.bucketized.checking
> Only applies if index type is BLOOM. When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup<br>
> `Default Value: true`<br>
> `Config Param: BLOOM_INDEX_BUCKETIZED_CHECKING_PROP`<br>

---

> #### hoodie.bloom.index.update.partition.path
> Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition<br>
> `Default Value: false`<br>
> `Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.bloom.index.parallelism
> Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics. Disable explicit bloom index parallelism setting by default - hoodie auto computes<br>
> `Default Value: 0`<br>
> `Config Param: BLOOM_INDEX_PARALLELISM_PROP`<br>

---

> #### hoodie.bloom.index.input.storage.level
> Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br>
> `Default Value: MEMORY_AND_DISK_SER`<br>
> `Config Param: BLOOM_INDEX_INPUT_STORAGE_LEVEL`<br>

---

> #### hoodie.bloom.index.keys.per.bucket
> Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. This configuration controls the “bucket” size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory.<br>
> `Default Value: 10000000`<br>
> `Config Param: BLOOM_INDEX_KEYS_PER_BUCKET_PROP`<br>

---

> #### hoodie.simple.index.update.partition.path
> <br>
> `Default Value: false`<br>
> `Config Param: SIMPLE_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.simple.index.input.storage.level
> Only applies when #simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br>
> `Default Value: MEMORY_AND_DISK_SER`<br>
> `Config Param: SIMPLE_INDEX_INPUT_STORAGE_LEVEL`<br>

---

> #### hoodie.index.bloom.fpp
> Only applies if index type is BLOOM. Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the congfigured value (hoodie.index.bloom.num_entries), then this fpp may not be honored.<br>
> `Default Value: 0.000000001`<br>
> `Config Param: BLOOM_FILTER_FPP`<br>

---

> #### hoodie.bloom.index.filter.dynamic.max.entries
> The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.<br>
> `Default Value: 100000`<br>
> `Config Param: HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES`<br>

---

> #### hoodie.index.class
> Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified<br>
> `Default Value: `<br>
> `Config Param: INDEX_CLASS_PROP`<br>

---

> #### hoodie.bloom.index.filter.type
> Filter type used. Default is BloomFilterTypeCode.SIMPLE. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.<br>
> `Default Value: SIMPLE`<br>
> `Config Param: BLOOM_INDEX_FILTER_TYPE`<br>

---

> #### hoodie.global.simple.index.parallelism
> Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br>
> `Default Value: 100`<br>
> `Config Param: GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP`<br>

---

> #### hoodie.bloom.index.use.caching
> Only applies if index type is BLOOM.When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br>
> `Default Value: true`<br>
> `Config Param: BLOOM_INDEX_USE_CACHING_PROP`<br>

---

> #### hoodie.bloom.index.prune.by.ranges
> Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off.<br>
> `Default Value: true`<br>
> `Config Param: BLOOM_INDEX_PRUNE_BY_RANGES_PROP`<br>

---

> #### hoodie.simple.index.use.caching
> Only applies if index type is SIMPLE. When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br>
> `Default Value: true`<br>
> `Config Param: SIMPLE_INDEX_USE_CACHING_PROP`<br>

---

> #### hoodie.simple.index.parallelism
> Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br>
> `Default Value: 50`<br>
> `Config Param: SIMPLE_INDEX_PARALLELISM_PROP`<br>

---

### Storage Configs {#Storage-Configs}

Configurations that control aspects around sizing parquet and log files.

`Config Class`: org.apache.hudi.config.HoodieStorageConfig<br>
> #### hoodie.parquet.compression.ratio
> Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files<br>
> `Default Value: 0.1`<br>
> `Config Param: PARQUET_COMPRESSION_RATIO`<br>

---

> #### hoodie.hfile.max.file.size
> <br>
> `Default Value: 125829120`<br>
> `Config Param: HFILE_FILE_MAX_BYTES`<br>

---

> #### hoodie.logfile.data.block.max.size
> LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.<br>
> `Default Value: 268435456`<br>
> `Config Param: LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES`<br>

---

> #### hoodie.parquet.block.size
> Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk<br>
> `Default Value: 125829120`<br>
> `Config Param: PARQUET_BLOCK_SIZE_BYTES`<br>

---

> #### hoodie.orc.stripe.size
> Size of the memory buffer in bytes for writing<br>
> `Default Value: 67108864`<br>
> `Config Param: ORC_STRIPE_SIZE`<br>

---

> #### hoodie.orc.block.size
> File system block size<br>
> `Default Value: 125829120`<br>
> `Config Param: ORC_BLOCK_SIZE`<br>

---

> #### hoodie.orc.max.file.size
> <br>
> `Default Value: 125829120`<br>
> `Config Param: ORC_FILE_MAX_BYTES`<br>

---

> #### hoodie.hfile.compression.algorithm
> <br>
> `Default Value: GZ`<br>
> `Config Param: HFILE_COMPRESSION_ALGORITHM`<br>

---

> #### hoodie.parquet.max.file.size
> Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br>
> `Default Value: 125829120`<br>
> `Config Param: PARQUET_FILE_MAX_BYTES`<br>

---

> #### hoodie.orc.compression.codec
> <br>
> `Default Value: ZLIB`<br>
> `Config Param: ORC_COMPRESSION_CODEC`<br>

---

> #### hoodie.logfile.max.size
> LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version.<br>
> `Default Value: 1073741824`<br>
> `Config Param: LOGFILE_SIZE_MAX_BYTES`<br>

---

> #### hoodie.parquet.compression.codec
> Compression Codec for parquet files<br>
> `Default Value: gzip`<br>
> `Config Param: PARQUET_COMPRESSION_CODEC`<br>

---

> #### hoodie.logfile.to.parquet.compression.ratio
> Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.<br>
> `Default Value: 0.35`<br>
> `Config Param: LOGFILE_TO_PARQUET_COMPRESSION_RATIO`<br>

---

> #### hoodie.parquet.page.size
> Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately.<br>
> `Default Value: 1048576`<br>
> `Config Param: PARQUET_PAGE_SIZE_BYTES`<br>

---

> #### hoodie.hfile.block.size
> <br>
> `Default Value: 1048576`<br>
> `Config Param: HFILE_BLOCK_SIZE_BYTES`<br>

---

### Clustering Configs {#Clustering-Configs}

Configurations that control clustering operations in hudi. Each clustering has to be configured for its strategy, and config params. This config drives the same.

`Config Class`: org.apache.hudi.config.HoodieClusteringConfig<br>
> #### hoodie.clustering.inline
> Turn on inline clustering - clustering will be run after write operation is complete<br>
> `Default Value: false`<br>
> `Config Param: INLINE_CLUSTERING_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.sort.columns
> Columns to sort the data by when clustering<br>
> `Default Value: none`<br>
> `Config Param: CLUSTERING_SORT_COLUMNS_PROPERTY`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.updates.strategy
> When file groups is in clustering, need to handle the update to these file groups. Default strategy just reject the update<br>
> `Default Value: org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy`<br>
> `Config Param: CLUSTERING_UPDATES_STRATEGY_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.inline.max.commits
> Config to control frequency of clustering<br>
> `Default Value: 4`<br>
> `Config Param: INLINE_CLUSTERING_MAX_COMMIT_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.small.file.limit
> Files smaller than the size specified here are candidates for clustering<br>
> `Default Value: 629145600`<br>
> `Config Param: CLUSTERING_PLAN_SMALL_FILE_LIMIT`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.target.file.max.bytes
> Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br>
> `Default Value: 1073741824`<br>
> `Config Param: CLUSTERING_TARGET_FILE_MAX_BYTES`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.daybased.lookback.partitions
> Number of partitions to list to create ClusteringPlan<br>
> `Default Value: 2`<br>
> `Config Param: CLUSTERING_TARGET_PARTITIONS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.async.enabled
> Async clustering<br>
> `Default Value: false`<br>
> `Config Param: ASYNC_CLUSTERING_ENABLE_OPT_KEY`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.max.bytes.per.group
> Each clustering operation can create multiple groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). Max amount of data to be included in one group<br>
> `Default Value: 2147483648`<br>
> `Config Param: CLUSTERING_MAX_BYTES_PER_GROUP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.execution.strategy.class
> Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy<br>
> `Default Value: org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy`<br>
> `Config Param: CLUSTERING_EXECUTION_STRATEGY_CLASS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.max.num.groups
> Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism<br>
> `Default Value: 30`<br>
> `Config Param: CLUSTERING_MAX_NUM_GROUPS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.class
> Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy<br>
> `Default Value: org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy`<br>
> `Config Param: CLUSTERING_PLAN_STRATEGY_CLASS`<br>
> `Since Version: 0.7.0`<br>

---

### Metadata Configs {#Metadata-Configs}

Configurations used by the HUDI Metadata Table. This table maintains the meta information stored in hudi dataset so that listing can be avoided during queries.

`Config Class`: org.apache.hudi.common.config.HoodieMetadataConfig<br>
> #### hoodie.metadata.enable
> Enable the internal Metadata Table which stores table level file listings<br>
> `Default Value: false`<br>
> `Config Param: METADATA_ENABLE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.insert.parallelism
> Parallelism to use when writing to the metadata table<br>
> `Default Value: 1`<br>
> `Config Param: METADATA_INSERT_PARALLELISM_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.compact.max.delta.commits
> Controls how often the metadata table is compacted.<br>
> `Default Value: 24`<br>
> `Config Param: METADATA_COMPACT_NUM_DELTA_COMMITS_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.cleaner.commits.retained
> <br>
> `Default Value: 3`<br>
> `Config Param: CLEANER_COMMITS_RETAINED_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.keep.min.commits
> Controls the archival of the metadata table’s timeline<br>
> `Default Value: 20`<br>
> `Config Param: MIN_COMMITS_TO_KEEP_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.metrics.enable
> <br>
> `Default Value: false`<br>
> `Config Param: METADATA_METRICS_ENABLE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.assume.date.partitioning
> Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually<br>
> `Default Value: false`<br>
> `Config Param: HOODIE_ASSUME_DATE_PARTITIONING_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.keep.max.commits
> Controls the archival of the metadata table’s timeline<br>
> `Default Value: 30`<br>
> `Config Param: MAX_COMMITS_TO_KEEP_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.dir.filter.regex
> <br>
> `Default Value: `<br>
> `Config Param: DIRECTORY_FILTER_REGEX`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.validate
> Validate contents of Metadata Table on each access against the actual listings from DFS<br>
> `Default Value: false`<br>
> `Config Param: METADATA_VALIDATE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.clean.async
> Enable asynchronous cleaning for metadata table<br>
> `Default Value: false`<br>
> `Config Param: METADATA_ASYNC_CLEAN_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.file.listing.parallelism
> <br>
> `Default Value: 1500`<br>
> `Config Param: FILE_LISTING_PARALLELISM_PROP`<br>
> `Since Version: 0.7.0`<br>

---

### Bootstrap Configs {#Bootstrap-Configs}

Configurations that control bootstrap related configs. If you want to bootstrap your data for the first time into hudi, this bootstrap operation will come in handy as you don’t need to wait for entire data to be loaded into hudi to start leveraging hudi.

`Config Class`: org.apache.hudi.config.HoodieBootstrapConfig<br>
> #### hoodie.bootstrap.partitionpath.translator.class
> Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.<br>
> `Default Value: org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator`<br>
> `Config Param: BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.keygen.class
> Key generator implementation to be used for generating keys from the bootstrapped dataset<br>
> `Default Value: none`<br>
> `Config Param: BOOTSTRAP_KEYGEN_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.mode.selector
> Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped<br>
> `Default Value: org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector`<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.mode.selector.regex.mode
> Bootstrap mode to apply for partition paths, that match regex above. METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.<br>
> `Default Value: METADATA_ONLY`<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX_MODE`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.index.class
> <br>
> `Default Value: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex`<br>
> `Config Param: BOOTSTRAP_INDEX_CLASS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.full.input.provider
> Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD<br>
> `Default Value: org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider`<br>
> `Config Param: FULL_BOOTSTRAP_INPUT_PROVIDER`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.parallelism
> Parallelism value to be used to bootstrap data into hudi<br>
> `Default Value: 1500`<br>
> `Config Param: BOOTSTRAP_PARALLELISM`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.mode.selector.regex
> Matches each bootstrap dataset partition against this regex and applies the mode below to it.<br>
> `Default Value: .*`<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br>
> `Default Value: none`<br>
> `Config Param: BOOTSTRAP_BASE_PATH_PROP`<br>
> `Since Version: 0.6.0`<br>

---

## Record Payload Config {#RECORD_PAYLOAD}
This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.

### Metrics Configurations for Datadog reporter {#Metrics-Configurations-for-Datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsDatadogConfig<br>
> #### hoodie.metrics.datadog.metric.tags
> Datadog metric tags (comma-delimited) to be sent along with metrics data.<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_METRIC_TAGS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key.supplier
> Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set.<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_API_KEY_SUPPLIER`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.metric.prefix
> Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended.<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_METRIC_PREFIX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.timeout.seconds
> Datadog API timeout in seconds. Default to 3.<br>
> `Default Value: 3`<br>
> `Config Param: DATADOG_API_TIMEOUT_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.report.period.seconds
> Datadog report period in seconds. Default to 30.<br>
> `Default Value: 30`<br>
> `Config Param: DATADOG_REPORT_PERIOD_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.metric.host
> Datadog metric host to be sent along with metrics data.<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_METRIC_HOST`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key.skip.validation
> Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.<br>
> `Default Value: false`<br>
> `Config Param: DATADOG_API_KEY_SKIP_VALIDATION`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.site
> Datadog API site: EU or US<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_API_SITE`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key
> Datadog API key<br>
> `Default Value: none`<br>
> `Config Param: DATADOG_API_KEY`<br>
> `Since Version: 0.6.0`<br>

---

### Metrics Configurations for Prometheus {#Metrics-Configurations-for-Prometheus}

Enables reporting on Hudi metrics using Prometheus.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsPrometheusConfig<br>
> #### hoodie.metrics.pushgateway.host
> <br>
> `Default Value: localhost`<br>
> `Config Param: PUSHGATEWAY_HOST`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.delete.on.shutdown
> <br>
> `Default Value: true`<br>
> `Config Param: PUSHGATEWAY_DELETE_ON_SHUTDOWN`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.prometheus.port
> <br>
> `Default Value: 9090`<br>
> `Config Param: PROMETHEUS_PORT`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.random.job.name.suffix
> <br>
> `Default Value: true`<br>
> `Config Param: PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.report.period.seconds
> <br>
> `Default Value: 30`<br>
> `Config Param: PUSHGATEWAY_REPORT_PERIOD_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.job.name
> <br>
> `Default Value: `<br>
> `Config Param: PUSHGATEWAY_JOB_NAME`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.port
> <br>
> `Default Value: 9091`<br>
> `Config Param: PUSHGATEWAY_PORT`<br>
> `Since Version: 0.6.0`<br>

---

### Metrics Configurations {#Metrics-Configurations}

Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.

`Config Class`: org.apache.hudi.config.HoodieMetricsConfig<br>
> #### hoodie.metrics.jmx.host
> Jmx host to connect to<br>
> `Default Value: localhost`<br>
> `Config Param: JMX_HOST`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.executor.enable
> <br>
> `Default Value: none`<br>
> `Config Param: ENABLE_EXECUTOR_METRICS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metrics.jmx.port
> Jmx port to connect to<br>
> `Default Value: 9889`<br>
> `Config Param: JMX_PORT`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.graphite.host
> Graphite host to connect to<br>
> `Default Value: localhost`<br>
> `Config Param: GRAPHITE_SERVER_HOST`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.on
> Turn on/off metrics reporting. off by default.<br>
> `Default Value: false`<br>
> `Config Param: METRICS_ON`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.graphite.metric.prefix
> Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g<br>
> `Default Value: none`<br>
> `Config Param: GRAPHITE_METRIC_PREFIX`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.graphite.port
> Graphite port to connect to<br>
> `Default Value: 4756`<br>
> `Config Param: GRAPHITE_SERVER_PORT`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.reporter.type
> Type of metrics reporter.<br>
> `Default Value: GRAPHITE`<br>
> `Config Param: METRICS_REPORTER_TYPE`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.reporter.class
> <br>
> `Default Value: `<br>
> `Config Param: METRICS_REPORTER_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.

