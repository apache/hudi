---
title: All Configurations
keywords: [ configurations, default, flink options, spark, configs, parameters ] 
permalink: /docs/configurations.html
summary: This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.
toc: true
last_modified_at: 2022-04-30T18:29:54.348
---

This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.
- [**Record Payload Config**](#RECORD_PAYLOAD): This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.
- [**Kafka Connect Configs**](#KAFKA_CONNECT): These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables
- [**Amazon Web Services Configs**](#AWS): Please fill in the description for Config Group Name: Amazon Web Services Configs

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.

### Read Options {#Read-Options}

Options useful for reading tables via `read.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br></br>
> #### hoodie.file.index.enable
> Enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ENABLE_HOODIE_FILE_INDEX`<br></br>
> `Deprecated Version: 0.11.0`<br></br>

---

> #### hoodie.datasource.read.paths
> Comma separated list of file paths to read within a Hudi table.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: READ_PATHS`<br></br>

---

> #### hoodie.datasource.read.incr.filters
> For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies opaque map functions, filters appearing late in the sequence of transformations cannot be automatically pushed down. This option allows setting filters directly on Hoodie Source.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: PUSH_DOWN_INCR_FILTERS`<br></br>

---

> #### hoodie.enable.data.skipping
> Enables data-skipping allowing queries to leverage indexes to reduce the search space by skipping over files<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ENABLE_DATA_SKIPPING`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### as.of.instant
> The query instant for time travel. Without specified this option, we query the latest snapshot.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TIME_TRAVEL_AS_OF_INSTANT`<br></br>

---

> #### hoodie.datasource.read.schema.use.end.instanttime
> Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME`<br></br>

---

> #### hoodie.datasource.read.incr.path.glob
> For the use-cases like users only want to incremental pull from certain partitions instead of the full table. This option allows using glob pattern to directly filter on path.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: INCR_PATH_GLOB`<br></br>

---

> #### hoodie.datasource.read.end.instanttime
> Instant time to limit incrementally fetched data to. New data written with an instant_time <= END_INSTANTTIME are fetched out.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: END_INSTANTTIME`<br></br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: READ_PRE_COMBINE_FIELD`<br></br>

---

> #### hoodie.datasource.merge.type
> For Snapshot query on merge on read table, control whether we invoke the record payload implementation to merge (payload_combine) or skip merging altogetherskip_merge<br></br>
> **Default Value**: payload_combine (Optional)<br></br>
> `Config Param: REALTIME_MERGE`<br></br>

---

> #### hoodie.datasource.read.extract.partition.values.from.path
> When set to true, values for partition columns (partition values) will be extracted from physical partition path (default Spark behavior). When set to false partition values will be read from the data file (in Hudi partition columns are persisted by default). This config is a fallback allowing to preserve existing behavior, and should not be used otherwise.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.datasource.read.begin.instanttime
> Instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BEGIN_INSTANTTIME`<br></br>

---

> #### hoodie.datasource.read.incr.fallback.fulltablescan.enable
> When doing an incremental query whether we should fall back to full table scans if file does not exist.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES`<br></br>

---

> #### hoodie.datasource.query.type
> Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode (obtain latest view, by merging base and (if any) log files)<br></br>
> **Default Value**: snapshot (Optional)<br></br>
> `Config Param: QUERY_TYPE`<br></br>

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


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br></br>
> #### hoodie.clustering.async.enabled
> Enable running of clustering service, asynchronously as inserts happen on the table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_CLUSTERING_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.datasource.write.operation
> Whether to do upsert, insert or bulkinsert for the write operation. Use bulkinsert to load new data into a table, and there on use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br></br>
> **Default Value**: upsert (Optional)<br></br>
> `Config Param: OPERATION`<br></br>

---

> #### hoodie.datasource.write.reconcile.schema
> When a new batch of write has records with old schema, but latest table schema got evolved, this config will upgrade the records to leverage latest table schema(default values will be injected to missing fields). If not, the write batch would fail.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: RECONCILE_SCHEMA`<br></br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br></br>
> **Default Value**: uuid (Optional)<br></br>
> `Config Param: RECORDKEY_FIELD`<br></br>

---

> #### hoodie.datasource.hive_sync.skip_ro_suffix
> Skip the _ro suffix for Read optimized table, when registering<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE`<br></br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: URL_ENCODE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.hive_sync.partition_extractor_class
> Class which implements PartitionValueExtractor to extract the partition values, default 'SlashEncodedDayPartitionValueExtractor'.<br></br>
> **Default Value**: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor (Optional)<br></br>
> `Config Param: HIVE_PARTITION_EXTRACTOR_CLASS`<br></br>

---

> #### hoodie.datasource.hive_sync.serde_properties
> Serde properties to hive table.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_TABLE_SERDE_PROPERTIES`<br></br>

---

> #### hoodie.datasource.hive_sync.sync_comment
> Whether to sync the table column comments while syncing the table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_COMMENT`<br></br>

---

> #### hoodie.datasource.hive_sync.password
> hive password to use<br></br>
> **Default Value**: hive (Optional)<br></br>
> `Config Param: HIVE_PASS`<br></br>

---

> #### hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled
> When set to true, consistent value will be generated for a logical timestamp type column, like timestamp-millis and timestamp-micros, irrespective of whether row-writer is enabled. Disabled by default so as not to break the pipeline that deploy either fully row-writer path or non row-writer path. For example, if it is kept disabled then record key of timestamp type with value `2016-12-29 09:54:00` will be written as timestamp `2016-12-29 09:54:00.0` in row-writer path, while it will be written as long value `1483023240000000` in non row-writer path. If enabled, then the timestamp value will be written in both the cases.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED`<br></br>

---

> #### hoodie.datasource.hive_sync.support_timestamp
> ‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. Disabled by default for backward compatibility.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SUPPORT_TIMESTAMP_TYPE`<br></br>

---

> #### hoodie.datasource.hive_sync.create_managed_table
> Whether to sync the table as managed table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_CREATE_MANAGED_TABLE`<br></br>

---

> #### hoodie.clustering.inline
> Turn on inline clustering - clustering will be run after each write operation is complete<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INLINE_CLUSTERING_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.datasource.compaction.async.enable
> Controls whether async compaction should be turned on for MOR table writing.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ASYNC_COMPACT_ENABLE`<br></br>

---

> #### hoodie.datasource.meta.sync.enable
> Enable Syncing the Hudi Table with an external meta store or data catalog.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: META_SYNC_ENABLED`<br></br>

---

> #### hoodie.datasource.write.streaming.ignore.failed.batch
> Config to indicate whether to ignore any non exception error (e.g. writestatus error) within a streaming microbatch<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: STREAMING_IGNORE_FAILED_BATCH`<br></br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: PRECOMBINE_FIELD`<br></br>

---

> #### hoodie.datasource.hive_sync.username
> hive user name to use<br></br>
> **Default Value**: hive (Optional)<br></br>
> `Config Param: HIVE_USER`<br></br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITIONPATH_FIELD`<br></br>

---

> #### hoodie.datasource.write.streaming.retry.count
> Config to indicate how many times streaming job should retry for a failed micro batch.<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: STREAMING_RETRY_CNT`<br></br>

---

> #### hoodie.datasource.hive_sync.partition_fields
> Field in the table to use for determining hive partition columns.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: HIVE_PARTITION_FIELDS`<br></br>

---

> #### hoodie.datasource.hive_sync.sync_as_datasource
> <br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_SYNC_AS_DATA_SOURCE_TABLE`<br></br>

---

> #### hoodie.sql.insert.mode
> Insert mode when insert data to pk-table. The optional modes are: upsert, strict and non-strict.For upsert mode, insert statement do the upsert operation for the pk-table which will update the duplicate record.For strict mode, insert statement will keep the primary key uniqueness constraint which do not allow duplicate record.While for non-strict mode, hudi just do the insert operation for the pk-table.<br></br>
> **Default Value**: upsert (Optional)<br></br>
> `Config Param: SQL_INSERT_MODE`<br></br>

---

> #### hoodie.datasource.hive_sync.use_jdbc
> Use JDBC when hive synchronization is enabled<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_USE_JDBC`<br></br>
> `Deprecated Version: 0.9.0`<br></br>

---

> #### hoodie.meta.sync.client.tool.class
> Sync tool class name used to sync to metastore. Defaults to Hive.<br></br>
> **Default Value**: org.apache.hudi.hive.HiveSyncTool (Optional)<br></br>
> `Config Param: META_SYNC_CLIENT_TOOL_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator`<br></br>
> **Default Value**: org.apache.hudi.keygen.SimpleKeyGenerator (Optional)<br></br>
> `Config Param: KEYGENERATOR_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: PAYLOAD_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.hive_sync.table_properties
> Additional properties to store with table.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_TABLE_PROPERTIES`<br></br>

---

> #### hoodie.datasource.hive_sync.jdbcurl
> Hive metastore url<br></br>
> **Default Value**: jdbc:hive2://localhost:10000 (Optional)<br></br>
> `Config Param: HIVE_URL`<br></br>

---

> #### hoodie.datasource.hive_sync.batch_num
> The number of partitions one batch when synchronous partitions to hive.<br></br>
> **Default Value**: 1000 (Optional)<br></br>
> `Config Param: HIVE_BATCH_SYNC_PARTITION_NUM`<br></br>

---

> #### hoodie.datasource.hive_sync.assume_date_partitioning
> Assume partitioning is yyyy/mm/dd<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_ASSUME_DATE_PARTITION`<br></br>

---

> #### hoodie.datasource.hive_sync.bucket_sync
> Whether sync hive metastore bucket specification when using bucket index.The specification is 'CLUSTERED BY (trace_id) SORTED BY (trace_id ASC) INTO 65536 BUCKETS'<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_BUCKET_SYNC`<br></br>

---

> #### hoodie.datasource.hive_sync.auto_create_database
> Auto create hive database if does not exists<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_AUTO_CREATE_DATABASE`<br></br>

---

> #### hoodie.datasource.hive_sync.database
> The name of the destination database that we should sync the hudi table to.<br></br>
> **Default Value**: default (Optional)<br></br>
> `Config Param: HIVE_DATABASE`<br></br>

---

> #### hoodie.datasource.write.streaming.retry.interval.ms
>  Config to indicate how long (by millisecond) before a retry should issued for failed microbatch<br></br>
> **Default Value**: 2000 (Optional)<br></br>
> `Config Param: STREAMING_RETRY_INTERVAL_MS`<br></br>

---

> #### hoodie.sql.bulk.insert.enable
> When set to true, the sql insert statement will use bulk insert.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: SQL_ENABLE_BULK_INSERT`<br></br>

---

> #### hoodie.datasource.write.commitmeta.key.prefix
> Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. This is useful to store checkpointing information, in a consistent way with the hudi timeline<br></br>
> **Default Value**: _ (Optional)<br></br>
> `Config Param: COMMIT_METADATA_KEYPREFIX`<br></br>

---

> #### hoodie.datasource.write.drop.partition.columns
> When set to true, will not write the partition columns into hudi. By default, false.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: DROP_PARTITION_COLUMNS`<br></br>

---

> #### hoodie.datasource.hive_sync.enable
> When set to true, register/sync the table to Apache Hive metastore.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_ENABLED`<br></br>

---

> #### hoodie.datasource.hive_sync.table
> The name of the destination table that we should sync the hudi table to.<br></br>
> **Default Value**: unknown (Optional)<br></br>
> `Config Param: HIVE_TABLE`<br></br>

---

> #### hoodie.datasource.hive_sync.ignore_exceptions
> Ignore exceptions when syncing with Hive.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_IGNORE_EXCEPTIONS`<br></br>

---

> #### hoodie.datasource.hive_sync.use_pre_apache_input_format
> Flag to choose InputFormat under com.uber.hoodie package instead of org.apache.hudi package. Use this when you are in the process of migrating from com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to org.apache.hudi input format<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_USE_PRE_APACHE_INPUT_FORMAT`<br></br>

---

> #### hoodie.datasource.write.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br></br>
> **Default Value**: COPY_ON_WRITE (Optional)<br></br>
> `Config Param: TABLE_TYPE`<br></br>

---

> #### hoodie.datasource.write.row.writer.enable
> When set to true, will perform write operations directly using the spark native `Row` representation, avoiding any additional conversion costs.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ENABLE_ROW_WRITER`<br></br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_STYLE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.meta_sync.condition.sync
> If true, only sync on conditions like schema change or partition change.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_CONDITIONAL_SYNC`<br></br>

---

> #### hoodie.datasource.hive_sync.mode
> Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_SYNC_MODE`<br></br>

---

> #### hoodie.datasource.write.table.name
> Table name for the datasource write. Also used to register the table into meta stores.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_NAME`<br></br>

---

> #### hoodie.datasource.hive_sync.base_file_format
> Base file format for the sync.<br></br>
> **Default Value**: PARQUET (Optional)<br></br>
> `Config Param: HIVE_BASE_FILE_FORMAT`<br></br>

---

> #### hoodie.deltastreamer.source.kafka.value.deserializer.class
> This class is used by kafka client to deserialize the records<br></br>
> **Default Value**: io.confluent.kafka.serializers.KafkaAvroDeserializer (Optional)<br></br>
> `Config Param: KAFKA_AVRO_VALUE_DESERIALIZER_CLASS`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.datasource.hive_sync.metastore.uris
> Hive metastore url<br></br>
> **Default Value**: thrift://localhost:9083 (Optional)<br></br>
> `Config Param: METASTORE_URIS`<br></br>

---

> #### hoodie.datasource.write.insert.drop.duplicates
> If set to true, filters out all duplicate records from incoming dataframe, during insert operations.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INSERT_DROP_DUPS`<br></br>

---

> #### hoodie.datasource.write.partitions.to.delete
> Comma separated list of partitions to delete<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITIONS_TO_DELETE`<br></br>

---

### PreCommit Validator Configurations {#PreCommit-Validator-Configurations}

The following set of configurations help validate new data before commits.

`Config Class`: org.apache.hudi.config.HoodiePreCommitValidatorConfig<br></br>
> #### hoodie.precommit.validators.single.value.sql.queries
> Spark SQL queries to run on table before committing new data to validate state after commit.Multiple queries separated by ';' delimiter are supported.Expected result is included as part of query separated by '#'. Example query: 'query1#result1:query2#result2'Note \<TABLE_NAME\> variable is expected to be present in query.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: SINGLE_VALUE_SQL_QUERIES`<br></br>

---

> #### hoodie.precommit.validators.equality.sql.queries
> Spark SQL queries to run on table before committing new data to validate state before and after commit. Multiple queries separated by ';' delimiter are supported. Example: "select count(*) from \<TABLE_NAME\> Note \<TABLE_NAME\> is replaced by table state before and after commit.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: EQUALITY_SQL_QUERIES`<br></br>

---

> #### hoodie.precommit.validators
> Comma separated list of class names that can be invoked to validate commit<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: VALIDATOR_CLASS_NAMES`<br></br>

---

> #### hoodie.precommit.validators.inequality.sql.queries
> Spark SQL queries to run on table before committing new data to validate state before and after commit.Multiple queries separated by ';' delimiter are supported.Example query: 'select count(*) from \<TABLE_NAME\> where col=null'Note \<TABLE_NAME\> variable is expected to be present in query.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: INEQUALITY_SQL_QUERIES`<br></br>

---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.

### Flink Options {#Flink-Options}

Flink jobs using the SQL can be configured through the options in WITH clause. The actual datasource level configs are listed below.

`Config Class`: org.apache.hudi.configuration.FlinkOptions<br></br>
> #### read.streaming.enabled
> Whether to read as streaming source, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: READ_AS_STREAMING`<br></br>

---

> #### hoodie.datasource.write.keygenerator.type
> Key generator type, that implements will extract the key out of incoming record<br></br>
> **Default Value**: SIMPLE (Optional)<br></br>
> `Config Param: KEYGEN_TYPE`<br></br>

---

> #### compaction.trigger.strategy
> Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits;
'time_elapsed': trigger compaction when time elapsed &gt; N seconds since last compaction;
'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;
'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.
Default is 'num_commits'<br></br>
> **Default Value**: num_commits (Optional)<br></br>
> `Config Param: COMPACTION_TRIGGER_STRATEGY`<br></br>

---

> #### index.state.ttl
> Index state ttl in days, default stores the index permanently<br></br>
> **Default Value**: 0.0 (Optional)<br></br>
> `Config Param: INDEX_STATE_TTL`<br></br>

---

> #### compaction.max_memory
> Max memory in MB for compaction spillable map, default 100MB<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: COMPACTION_MAX_MEMORY`<br></br>

---

> #### hive_sync.support_timestamp
> INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type.
Disabled by default for backward compatibility.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_SYNC_SUPPORT_TIMESTAMP`<br></br>

---

> #### hive_sync.serde_properties
> Serde properties to hive table, the data format is k1=v1
k2=v2<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_SYNC_TABLE_SERDE_PROPERTIES`<br></br>

---

> #### hive_sync.skip_ro_suffix
> Skip the _ro suffix for Read optimized table when registering, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_SKIP_RO_SUFFIX`<br></br>

---

> #### metadata.compaction.delta_commits
> Max delta commits for metadata table to trigger compaction, default 10<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: METADATA_COMPACTION_DELTA_COMMITS`<br></br>

---

> #### hive_sync.assume_date_partitioning
> Assume partitioning is yyyy/mm/dd, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_ASSUME_DATE_PARTITION`<br></br>

---

> #### write.parquet.block.size
> Parquet RowGroup size. It's recommended to make this large enough that scan costs can be amortized by packing enough column values into a single row group.<br></br>
> **Default Value**: 120 (Optional)<br></br>
> `Config Param: WRITE_PARQUET_BLOCK_SIZE`<br></br>

---

> #### hive_sync.table
> Table name for hive sync, default 'unknown'<br></br>
> **Default Value**: unknown (Optional)<br></br>
> `Config Param: HIVE_SYNC_TABLE`<br></br>

---

> #### write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
This will render any value set for the option in-effective<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: PAYLOAD_CLASS_NAME`<br></br>

---

> #### compaction.tasks
> Parallelism of tasks that do actual compaction, default is 4<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: COMPACTION_TASKS`<br></br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Whether to use Hive style partitioning.
If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format.
By default false (the names of partition folders are only partition values)<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_STYLE_PARTITIONING`<br></br>

---

> #### table.type
> Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ<br></br>
> **Default Value**: COPY_ON_WRITE (Optional)<br></br>
> `Config Param: TABLE_TYPE`<br></br>

---

> #### hive_sync.auto_create_db
> Auto create hive database if it does not exists, default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_SYNC_AUTO_CREATE_DB`<br></br>

---

> #### compaction.timeout.seconds
> Max timeout time in seconds for online compaction to rollback, default 20 minutes<br></br>
> **Default Value**: 1200 (Optional)<br></br>
> `Config Param: COMPACTION_TIMEOUT_SECONDS`<br></br>

---

> #### hive_sync.username
> Username for hive sync, default 'hive'<br></br>
> **Default Value**: hive (Optional)<br></br>
> `Config Param: HIVE_SYNC_USERNAME`<br></br>

---

> #### write.sort.memory
> Sort memory in MB, default 128MB<br></br>
> **Default Value**: 128 (Optional)<br></br>
> `Config Param: WRITE_SORT_MEMORY`<br></br>

---

> #### hive_sync.enable
> Asynchronously sync Hive meta to HMS, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_ENABLED`<br></br>

---

> #### changelog.enabled
> Whether to keep all the intermediate changes, we try to keep all the changes of a record when enabled:
1). The sink accept the UPDATE_BEFORE message;
2). The source try to emit every changes of a record.
The semantics is best effort because the compaction job would finally merge all changes of a record into one.
 default false to have UPSERT semantics<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: CHANGELOG_ENABLED`<br></br>

---

> #### read.streaming.check-interval
> Check interval for streaming read of SECOND, default 1 minute<br></br>
> **Default Value**: 60 (Optional)<br></br>
> `Config Param: READ_STREAMING_CHECK_INTERVAL`<br></br>

---

> #### write.bulk_insert.shuffle_input
> Whether to shuffle the inputs by specific fields for bulk insert tasks, default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: WRITE_BULK_INSERT_SHUFFLE_INPUT`<br></br>

---

> #### hoodie.datasource.merge.type
> For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in
1) skip_merge: read the base file records plus the log file records;
2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the
   log file records(combines the two records with same key for base and log file records), then read the left log file records<br></br>
> **Default Value**: payload_combine (Optional)<br></br>
> `Config Param: MERGE_TYPE`<br></br>

---

> #### write.retry.times
> Flag to indicate how many times streaming job should retry for a failed checkpoint batch.
By default 3<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: RETRY_TIMES`<br></br>

---

> #### metadata.enabled
> Enable the internal metadata table which serves table metadata like level file listings, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: METADATA_ENABLED`<br></br>

---

> #### read.tasks
> Parallelism of tasks that do actual read, default is 4<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: READ_TASKS`<br></br>

---

> #### write.parquet.max.file.size
> Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br></br>
> **Default Value**: 120 (Optional)<br></br>
> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE`<br></br>

---

> #### hoodie.bucket.index.hash.field
> Index key field. Value to be used as hashing to find the bucket ID. Should be a subset of or equal to the recordKey fields.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: INDEX_KEY_FIELD`<br></br>

---

> #### hoodie.bucket.index.num.buckets
> Hudi bucket number per partition. Only affected if using Hudi bucket index.<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: BUCKET_INDEX_NUM_BUCKETS`<br></br>

---

> #### read.end-commit
> End commit instant for reading, the commit time format should be 'yyyyMMddHHmmss'<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: READ_END_COMMIT`<br></br>

---

> #### write.log.max.size
> Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB<br></br>
> **Default Value**: 1024 (Optional)<br></br>
> `Config Param: WRITE_LOG_MAX_SIZE`<br></br>

---

> #### hive_sync.file_format
> File format for hive sync, default 'PARQUET'<br></br>
> **Default Value**: PARQUET (Optional)<br></br>
> `Config Param: HIVE_SYNC_FILE_FORMAT`<br></br>

---

> #### hive_sync.mode
> Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'jdbc'<br></br>
> **Default Value**: jdbc (Optional)<br></br>
> `Config Param: HIVE_SYNC_MODE`<br></br>

---

> #### write.retry.interval.ms
> Flag to indicate how long (by millisecond) before a retry should issued for failed checkpoint batch.
By default 2000 and it will be doubled by every retry<br></br>
> **Default Value**: 2000 (Optional)<br></br>
> `Config Param: RETRY_INTERVAL_MS`<br></br>

---

> #### write.partition.format
> Partition path format, only valid when 'write.datetime.partitioning' is true, default is:
1) 'yyyyMMddHH' for timestamp(3) WITHOUT TIME ZONE, LONG, FLOAT, DOUBLE, DECIMAL;
2) 'yyyyMMdd' for DATE and INT.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_FORMAT`<br></br>

---

> #### hive_sync.db
> Database name for hive sync, default 'default'<br></br>
> **Default Value**: default (Optional)<br></br>
> `Config Param: HIVE_SYNC_DB`<br></br>

---

> #### index.type
> Index type of Flink write job, default is using state backed index.<br></br>
> **Default Value**: FLINK_STATE (Optional)<br></br>
> `Config Param: INDEX_TYPE`<br></br>

---

> #### hive_sync.password
> Password for hive sync, default 'hive'<br></br>
> **Default Value**: hive (Optional)<br></br>
> `Config Param: HIVE_SYNC_PASSWORD`<br></br>

---

> #### hive_sync.use_jdbc
> Use JDBC when hive synchronization is enabled, default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: HIVE_SYNC_USE_JDBC`<br></br>

---

> #### compaction.schedule.enabled
> Schedule the compaction plan, enabled by default for MOR<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMPACTION_SCHEDULE_ENABLED`<br></br>

---

> #### hive_sync.jdbc_url
> Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'<br></br>
> **Default Value**: jdbc:hive2://localhost:10000 (Optional)<br></br>
> `Config Param: HIVE_SYNC_JDBC_URL`<br></br>

---

> #### hive_sync.partition_extractor_class
> Tool to extract the partition value from HDFS path, default 'SlashEncodedDayPartitionValueExtractor'<br></br>
> **Default Value**: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor (Optional)<br></br>
> `Config Param: HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME`<br></br>

---

> #### read.start-commit
> Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant for streaming read<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: READ_START_COMMIT`<br></br>

---

> #### write.precombine
> Flag to indicate whether to drop duplicates before insert/upsert.
By default these cases will accept duplicates, to gain extra performance:
1) insert operation;
2) upsert for MOR table, the MOR table deduplicate on reading<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: PRE_COMBINE`<br></br>

---

> #### write.batch.size
> Batch buffer size in MB to flush data into the underneath filesystem, default 256MB<br></br>
> **Default Value**: 256.0 (Optional)<br></br>
> `Config Param: WRITE_BATCH_SIZE`<br></br>

---

> #### archive.min_commits
> Min number of commits to keep before archiving older commits into a sequential log, default 40<br></br>
> **Default Value**: 40 (Optional)<br></br>
> `Config Param: ARCHIVE_MIN_COMMITS`<br></br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements will extract the key out of incoming record<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: KEYGEN_CLASS_NAME`<br></br>

---

> #### index.global.enabled
> Whether to update index for the old partition path
if same key record with different partition path came in, default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: INDEX_GLOBAL_ENABLED`<br></br>

---

> #### index.partition.regex
> Whether to load partitions in state if partition path matching， default `*`<br></br>
> **Default Value**: .* (Optional)<br></br>
> `Config Param: INDEX_PARTITION_REGEX`<br></br>

---

> #### hoodie.table.name
> Table name to register to Hive metastore<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_NAME`<br></br>

---

> #### path
> Base path for the target hoodie table.
The path would be created if it does not exist,
otherwise a Hoodie table expects to be initialized successfully<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PATH`<br></br>

---

> #### index.bootstrap.enabled
> Whether to bootstrap the index state from existing hoodie table, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INDEX_BOOTSTRAP_ENABLED`<br></br>

---

> #### read.streaming.skip_compaction
> Whether to skip compaction instants for streaming read,
there are two cases that this option can be used to avoid reading duplicates:
1) you are definitely sure that the consumer reads faster than any compaction instants, usually with delta time compaction strategy that is long enough, for e.g, one week;
2) changelog mode is enabled, this option is a solution to keep data integrity<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: READ_STREAMING_SKIP_COMPACT`<br></br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Whether to encode the partition path url, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: URL_ENCODE_PARTITIONING`<br></br>

---

> #### compaction.async.enabled
> Async Compaction, enabled by default for MOR<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMPACTION_ASYNC_ENABLED`<br></br>

---

> #### hive_sync.ignore_exceptions
> Ignore exceptions during hive synchronization, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_IGNORE_EXCEPTIONS`<br></br>

---

> #### hive_sync.table_properties
> Additional properties to store with table, the data format is k1=v1
k2=v2<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_SYNC_TABLE_PROPERTIES`<br></br>

---

> #### write.ignore.failed
> Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch.
By default true (in favor of streaming progressing over data integrity)<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: IGNORE_FAILED`<br></br>

---

> #### write.commit.ack.timeout
> Timeout limit for a writer task after it finishes a checkpoint and
waits for the instant commit success, only for internal use<br></br>
> **Default Value**: -1 (Optional)<br></br>
> `Config Param: WRITE_COMMIT_ACK_TIMEOUT`<br></br>

---

> #### write.operation
> The write operation, that this write should do<br></br>
> **Default Value**: upsert (Optional)<br></br>
> `Config Param: OPERATION`<br></br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
Actual value obtained by invoking .toString(), default ''<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: PARTITION_PATH_FIELD`<br></br>

---

> #### write.bucket_assign.tasks
> Parallelism of tasks that do bucket assign, default is the parallelism of the execution environment<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BUCKET_ASSIGN_TASKS`<br></br>

---

> #### source.avro-schema.path
> Source avro schema file path, the parsed schema is used for deserialization<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: SOURCE_AVRO_SCHEMA_PATH`<br></br>

---

> #### compaction.delta_commits
> Max delta commits needed to trigger compaction, default 5 commits<br></br>
> **Default Value**: 5 (Optional)<br></br>
> `Config Param: COMPACTION_DELTA_COMMITS`<br></br>

---

> #### write.insert.cluster
> Whether to merge small files for insert mode, if true, the write throughput will decrease because the read/write of existing small file, only valid for COW table, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INSERT_CLUSTER`<br></br>

---

> #### partition.default_name
> The default partition name in case the dynamic partition column value is null/empty string<br></br>
> **Default Value**: default (Optional)<br></br>
> `Config Param: PARTITION_DEFAULT_NAME`<br></br>

---

> #### write.bulk_insert.sort_input
> Whether to sort the inputs by specific fields for bulk insert tasks, default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: WRITE_BULK_INSERT_SORT_INPUT`<br></br>

---

> #### source.avro-schema
> Source avro schema string, the parsed schema is used for deserialization<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: SOURCE_AVRO_SCHEMA`<br></br>

---

> #### compaction.target_io
> Target IO in MB for per compaction (both read and write), default 500 GB<br></br>
> **Default Value**: 512000 (Optional)<br></br>
> `Config Param: COMPACTION_TARGET_IO`<br></br>

---

> #### write.rate.limit
> Write record rate limit per second to prevent traffic jitter and improve stability, default 0 (no limit)<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: WRITE_RATE_LIMIT`<br></br>

---

> #### write.log_block.size
> Max log block size in MB for log file, default 128MB<br></br>
> **Default Value**: 128 (Optional)<br></br>
> `Config Param: WRITE_LOG_BLOCK_SIZE`<br></br>

---

> #### write.tasks
> Parallelism of tasks that do actual write, default is 4<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: WRITE_TASKS`<br></br>

---

> #### clean.async.enabled
> Whether to cleanup the old commits immediately on new commits, enabled by default<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: CLEAN_ASYNC_ENABLED`<br></br>

---

> #### clean.retain_commits
> Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).
This also directly translates into how much you can incrementally pull on this table, default 30<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: CLEAN_RETAIN_COMMITS`<br></br>

---

> #### read.utc-timezone
> Use UTC timezone or local timezone to the conversion between epoch time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone, by default true<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: UTC_TIMEZONE`<br></br>

---

> #### archive.max_commits
> Max number of commits to keep before archiving older commits into a sequential log, default 50<br></br>
> **Default Value**: 50 (Optional)<br></br>
> `Config Param: ARCHIVE_MAX_COMMITS`<br></br>

---

> #### hoodie.datasource.query.type
> Decides how data files need to be read, in
1) Snapshot mode (obtain latest view, based on row &amp; columnar data);
2) incremental mode (new data since an instantTime);
3) Read Optimized mode (obtain latest view, based on columnar data)
.Default: snapshot<br></br>
> **Default Value**: snapshot (Optional)<br></br>
> `Config Param: QUERY_TYPE`<br></br>

---

> #### write.precombine.field
> Field used in preCombining before actual write. When two records have the same
key value, we will pick the one with the largest value for the precombine field,
determined by Object.compareTo(..)<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: PRECOMBINE_FIELD`<br></br>

---

> #### write.index_bootstrap.tasks
> Parallelism of tasks that do index bootstrap, default is the parallelism of the execution environment<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: INDEX_BOOTSTRAP_TASKS`<br></br>

---

> #### write.task.max.size
> Maximum memory in MB for a write task, when the threshold hits,
it flushes the max size data bucket to avoid OOM, default 1GB<br></br>
> **Default Value**: 1024.0 (Optional)<br></br>
> `Config Param: WRITE_TASK_MAX_SIZE`<br></br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br></br>
> **Default Value**: uuid (Optional)<br></br>
> `Config Param: RECORD_KEY_FIELD`<br></br>

---

> #### write.parquet.page.size
> Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed separately.<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: WRITE_PARQUET_PAGE_SIZE`<br></br>

---

> #### compaction.delta_seconds
> Max delta seconds time needed to trigger compaction, default 1 hour<br></br>
> **Default Value**: 3600 (Optional)<br></br>
> `Config Param: COMPACTION_DELTA_SECONDS`<br></br>

---

> #### hive_sync.metastore.uris
> Metastore uris for hive sync, default ''<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: HIVE_SYNC_METASTORE_URIS`<br></br>

---

> #### hive_sync.partition_fields
> Partition fields for hive sync, default ''<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: HIVE_SYNC_PARTITION_FIELDS`<br></br>

---

> #### write.merge.max_memory
> Max memory in MB for merge, default 100MB<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: WRITE_MERGE_MAX_MEMORY`<br></br>

---

## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.

### Layout Configs {#Layout-Configs}

Configurations that control storage layout and data distribution, which defines how the files are organized within a table.

`Config Class`: org.apache.hudi.config.HoodieLayoutConfig<br></br>
> #### hoodie.storage.layout.type
> Type of storage layout. Possible options are [DEFAULT | BUCKET]<br></br>
> **Default Value**: DEFAULT (Optional)<br></br>
> `Config Param: LAYOUT_TYPE`<br></br>

---

> #### hoodie.storage.layout.partitioner.class
> Partitioner class, it is used to distribute data in a specific way.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: LAYOUT_PARTITIONER_CLASS_NAME`<br></br>

---

### Write commit callback configs {#Write-commit-callback-configs}

Controls callback behavior into HTTP endpoints, to push  notifications on commits on hudi tables.

`Config Class`: org.apache.hudi.config.HoodieWriteCommitCallbackConfig<br></br>
> #### hoodie.write.commit.callback.on
> Turn commit callback on/off. off by default.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: TURN_CALLBACK_ON`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.write.commit.callback.http.url
> Callback host to be sent along with callback messages<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: CALLBACK_HTTP_URL`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.write.commit.callback.http.timeout.seconds
> Callback timeout in seconds. 3 by default<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: CALLBACK_HTTP_TIMEOUT_IN_SECONDS`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.write.commit.callback.class
> Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default<br></br>
> **Default Value**: org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback (Optional)<br></br>
> `Config Param: CALLBACK_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.write.commit.callback.http.api.key
> Http callback API key. hudi_write_commit_http_callback by default<br></br>
> **Default Value**: hudi_write_commit_http_callback (Optional)<br></br>
> `Config Param: CALLBACK_HTTP_API_KEY_VALUE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

### Table Configurations {#Table-Configurations}

Configurations that persist across writes and read on a Hudi table  like  base, log file formats, table name, creation schema, table version layouts.  Configurations are loaded from hoodie.properties, these properties are usually set during initializing a path as hoodie base path and rarely changes during the lifetime of the table. Writers/Queries' configurations are validated against these  each time for compatibility.

`Config Class`: org.apache.hudi.common.table.HoodieTableConfig<br></br>
> #### hoodie.table.precombine.field
> Field used in preCombining before actual write. By default, when two records have the same key value, the largest value for the precombine field determined by Object.compareTo(..), is picked.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PRECOMBINE_FIELD`<br></br>

---

> #### hoodie.archivelog.folder
> path under the meta folder, to store archived timeline instants at.<br></br>
> **Default Value**: archived (Optional)<br></br>
> `Config Param: ARCHIVELOG_FOLDER`<br></br>

---

> #### hoodie.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br></br>
> **Default Value**: COPY_ON_WRITE (Optional)<br></br>
> `Config Param: TYPE`<br></br>

---

> #### hoodie.table.timeline.timezone
> User can set hoodie commit timeline timezone, such as utc, local and so on. local is default<br></br>
> **Default Value**: LOCAL (Optional)<br></br>
> `Config Param: TIMELINE_TIMEZONE`<br></br>

---

> #### hoodie.partition.metafile.use.base.format
> If true, partition metafiles are saved in the same format as base-files for this dataset (e.g. Parquet / ORC). If false (default) partition metafiles are saved as properties files.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: PARTITION_METAFILE_USE_BASE_FORMAT`<br></br>

---

> #### hoodie.table.checksum
> Table checksum is used to guard against partial writes in HDFS. It is added as the last entry in hoodie.properties and then used to validate while reading table config.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_CHECKSUM`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.table.create.schema
> Schema used when creating the table, for the first time.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: CREATE_SCHEMA`<br></br>

---

> #### hoodie.table.recordkey.fields
> Columns used to uniquely identify the table. Concatenated values of these fields are used as  the record key component of HoodieKey.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: RECORDKEY_FIELDS`<br></br>

---

> #### hoodie.table.log.file.format
> Log format used for the delta logs.<br></br>
> **Default Value**: HOODIE_LOG (Optional)<br></br>
> `Config Param: LOG_FILE_FORMAT`<br></br>

---

> #### hoodie.bootstrap.index.enable
> Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined, default true.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BOOTSTRAP_INDEX_ENABLE`<br></br>

---

> #### hoodie.table.metadata.partitions
> Comma-separated list of metadata partitions that have been completely built and in-sync with data table. These partitions are ready for use by the readers<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_METADATA_PARTITIONS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.table.metadata.partitions.inflight
> Comma-separated list of metadata partitions whose building is in progress. These partitions are not yet ready for use by the readers.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_METADATA_PARTITIONS_INFLIGHT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.table.partition.fields
> Fields used to partition the table. Concatenated values of these fields are used as the partition path, by invoking toString()<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_FIELDS`<br></br>

---

> #### hoodie.populate.meta.fields
> When enabled, populates all meta fields. When disabled, no meta fields are populated and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: POPULATE_META_FIELDS`<br></br>

---

> #### hoodie.compaction.payload.class
> Payload class to use for performing compactions, i.e merge delta logs with current base file and then  produce a new base file.<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: PAYLOAD_CLASS_NAME`<br></br>

---

> #### hoodie.bootstrap.index.class
> Implementation to use, for mapping base files to bootstrap base file, that contain actual data.<br></br>
> **Default Value**: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex (Optional)<br></br>
> `Config Param: BOOTSTRAP_INDEX_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: URL_ENCODE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_STYLE_PARTITIONING_ENABLE`<br></br>

---

> #### hoodie.table.keygenerator.class
> Key Generator class property for the hoodie table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: KEY_GENERATOR_CLASS_NAME`<br></br>

---

> #### hoodie.table.version
> Version of table, used for running upgrade/downgrade steps between releases with potentially breaking/backwards compatible changes.<br></br>
> **Default Value**: ZERO (Optional)<br></br>
> `Config Param: VERSION`<br></br>

---

> #### hoodie.table.base.file.format
> Base file format to store all the base file data.<br></br>
> **Default Value**: PARQUET (Optional)<br></br>
> `Config Param: BASE_FILE_FORMAT`<br></br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BOOTSTRAP_BASE_PATH`<br></br>

---

> #### hoodie.datasource.write.drop.partition.columns
> When set to true, will not write the partition columns into hudi. By default, false.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: DROP_PARTITION_COLUMNS`<br></br>

---

> #### hoodie.database.name
> Database name that will be used for incremental query.If different databases have the same table name during incremental query, we can set it to limit the table name under a specific database<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: DATABASE_NAME`<br></br>

---

> #### hoodie.timeline.layout.version
> Version of timeline used, by the table.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TIMELINE_LAYOUT_VERSION`<br></br>

---

> #### hoodie.table.name
> Table name that will be used for registering with Hive. Needs to be same across runs.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: NAME`<br></br>

---

### Memory Configurations {#Memory-Configurations}

Controls memory usage for compaction and merges, performed internally by Hudi.

`Config Class`: org.apache.hudi.config.HoodieMemoryConfig<br></br>
> #### hoodie.memory.merge.fraction
> This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge<br></br>
> **Default Value**: 0.6 (Optional)<br></br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_MERGE`<br></br>

---

> #### hoodie.memory.dfs.buffer.max.size
> Property to control the max memory in bytes for dfs input stream buffer size<br></br>
> **Default Value**: 16777216 (Optional)<br></br>
> `Config Param: MAX_DFS_STREAM_BUFFER_SIZE`<br></br>

---

> #### hoodie.memory.writestatus.failure.fraction
> Property to control how what fraction of the failed record, exceptions we report back to driver. Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and mask actual data errors.<br></br>
> **Default Value**: 0.1 (Optional)<br></br>
> `Config Param: WRITESTATUS_FAILURE_FRACTION`<br></br>

---

> #### hoodie.memory.compaction.fraction
> HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map<br></br>
> **Default Value**: 0.6 (Optional)<br></br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_COMPACTION`<br></br>

---

> #### hoodie.memory.merge.max.size
> Maximum amount of memory used  in bytes for merge operations, before spilling to local storage.<br></br>
> **Default Value**: 1073741824 (Optional)<br></br>
> `Config Param: MAX_MEMORY_FOR_MERGE`<br></br>

---

> #### hoodie.memory.spillable.map.path
> Default file path prefix for spillable map<br></br>
> **Default Value**: /tmp/ (Optional)<br></br>
> `Config Param: SPILLABLE_MAP_BASE_PATH`<br></br>

---

> #### hoodie.memory.compaction.max.size
> Maximum amount of memory used  in bytes for compaction operations in bytes , before spilling to local storage.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: MAX_MEMORY_FOR_COMPACTION`<br></br>

---

### Storage Configs {#Storage-Configs}

Configurations that control aspects around writing, sizing, reading base and log files.

`Config Class`: org.apache.hudi.config.HoodieStorageConfig<br></br>
> #### hoodie.logfile.data.block.max.size
> LogFile Data block max size in bytes. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.<br></br>
> **Default Value**: 268435456 (Optional)<br></br>
> `Config Param: LOGFILE_DATA_BLOCK_MAX_SIZE`<br></br>

---

> #### hoodie.parquet.outputtimestamptype
> Sets spark.sql.parquet.outputTimestampType. Parquet timestamp type to use when Spark writes data to Parquet files.<br></br>
> **Default Value**: TIMESTAMP_MICROS (Optional)<br></br>
> `Config Param: PARQUET_OUTPUT_TIMESTAMP_TYPE`<br></br>

---

> #### hoodie.orc.stripe.size
> Size of the memory buffer in bytes for writing<br></br>
> **Default Value**: 67108864 (Optional)<br></br>
> `Config Param: ORC_STRIPE_SIZE`<br></br>

---

> #### hoodie.orc.block.size
> ORC block size, recommended to be aligned with the target file size.<br></br>
> **Default Value**: 125829120 (Optional)<br></br>
> `Config Param: ORC_BLOCK_SIZE`<br></br>

---

> #### hoodie.orc.compression.codec
> Compression codec to use for ORC base files.<br></br>
> **Default Value**: ZLIB (Optional)<br></br>
> `Config Param: ORC_COMPRESSION_CODEC_NAME`<br></br>

---

> #### hoodie.parquet.max.file.size
> Target size in bytes for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br></br>
> **Default Value**: 125829120 (Optional)<br></br>
> `Config Param: PARQUET_MAX_FILE_SIZE`<br></br>

---

> #### hoodie.hfile.max.file.size
> Target file size in bytes for HFile base files.<br></br>
> **Default Value**: 125829120 (Optional)<br></br>
> `Config Param: HFILE_MAX_FILE_SIZE`<br></br>

---

> #### hoodie.parquet.writelegacyformat.enabled
> Sets spark.sql.parquet.writeLegacyFormat. If true, data will be written in a way of Spark 1.4 and earlier. For example, decimal values will be written in Parquet's fixed-length byte array format which other systems such as Apache Hive and Apache Impala use. If false, the newer format in Parquet will be used. For example, decimals will be written in int-based format.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: PARQUET_WRITE_LEGACY_FORMAT_ENABLED`<br></br>

---

> #### hoodie.parquet.block.size
> Parquet RowGroup size in bytes. It's recommended to make this large enough that scan costs can be amortized by packing enough column values into a single row group.<br></br>
> **Default Value**: 125829120 (Optional)<br></br>
> `Config Param: PARQUET_BLOCK_SIZE`<br></br>

---

> #### hoodie.logfile.max.size
> LogFile max size in bytes. This is the maximum size allowed for a log file before it is rolled over to the next version.<br></br>
> **Default Value**: 1073741824 (Optional)<br></br>
> `Config Param: LOGFILE_MAX_SIZE`<br></br>

---

> #### hoodie.parquet.dictionary.enabled
> Whether to use dictionary encoding<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PARQUET_DICTIONARY_ENABLED`<br></br>

---

> #### hoodie.hfile.block.size
> Lower values increase the size in bytes of metadata tracked within HFile, but can offer potentially faster lookup times.<br></br>
> **Default Value**: 1048576 (Optional)<br></br>
> `Config Param: HFILE_BLOCK_SIZE`<br></br>

---

> #### hoodie.parquet.page.size
> Parquet page size in bytes. Page is the unit of read within a parquet file. Within a block, pages are compressed separately.<br></br>
> **Default Value**: 1048576 (Optional)<br></br>
> `Config Param: PARQUET_PAGE_SIZE`<br></br>

---

> #### hoodie.hfile.compression.algorithm
> Compression codec to use for hfile base files.<br></br>
> **Default Value**: GZ (Optional)<br></br>
> `Config Param: HFILE_COMPRESSION_ALGORITHM_NAME`<br></br>

---

> #### hoodie.orc.max.file.size
> Target file size in bytes for ORC base files.<br></br>
> **Default Value**: 125829120 (Optional)<br></br>
> `Config Param: ORC_FILE_MAX_SIZE`<br></br>

---

> #### hoodie.logfile.data.block.format
> Format of the data block within delta logs. Following formats are currently supported "avro", "hfile", "parquet"<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: LOGFILE_DATA_BLOCK_FORMAT`<br></br>

---

> #### hoodie.logfile.to.parquet.compression.ratio
> Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.<br></br>
> **Default Value**: 0.35 (Optional)<br></br>
> `Config Param: LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION`<br></br>

---

> #### hoodie.parquet.compression.ratio
> Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files<br></br>
> **Default Value**: 0.1 (Optional)<br></br>
> `Config Param: PARQUET_COMPRESSION_RATIO_FRACTION`<br></br>

---

> #### hoodie.parquet.compression.codec
> Compression Codec for parquet files<br></br>
> **Default Value**: gzip (Optional)<br></br>
> `Config Param: PARQUET_COMPRESSION_CODEC_NAME`<br></br>

---

### DynamoDB based Locks Configurations {#DynamoDB-based-Locks-Configurations}

Configs that control DynamoDB based locking mechanisms required for concurrency control  between writers to a Hudi table. Concurrency between Hudi's own table services  are auto managed internally.

`Config Class`: org.apache.hudi.config.DynamoDbBasedLockConfig<br></br>
> #### hoodie.write.lock.dynamodb.billing_mode
> For DynamoDB based lock provider, by default it is PAY_PER_REQUEST mode<br></br>
> **Default Value**: PAY_PER_REQUEST (Optional)<br></br>
> `Config Param: DYNAMODB_LOCK_BILLING_MODE`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.table
> For DynamoDB based lock provider, the name of the DynamoDB table acting as lock table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: DYNAMODB_LOCK_TABLE_NAME`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.region
> For DynamoDB based lock provider, the region used in endpoint for Amazon DynamoDB service. Would try to first get it from AWS_REGION environment variable. If not find, by default use us-east-1<br></br>
> **Default Value**: us-east-1 (Optional)<br></br>
> `Config Param: DYNAMODB_LOCK_REGION`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.partition_key
> For DynamoDB based lock provider, the partition key for the DynamoDB lock table. Each Hudi dataset should has it's unique key so concurrent writers could refer to the same partition key. By default we use the Hudi table name specified to be the partition key<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: DYNAMODB_LOCK_PARTITION_KEY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.write_capacity
> For DynamoDB based lock provider, write capacity units when using PROVISIONED billing mode<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: DYNAMODB_LOCK_WRITE_CAPACITY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.table_creation_timeout
> For DynamoDB based lock provider, the maximum number of milliseconds to wait for creating DynamoDB table<br></br>
> **Default Value**: 600000 (Optional)<br></br>
> `Config Param: DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.read_capacity
> For DynamoDB based lock provider, read capacity units when using PROVISIONED billing mode<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: DYNAMODB_LOCK_READ_CAPACITY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.write.lock.dynamodb.endpoint_url
> For DynamoDB based lock provider, the url endpoint used for Amazon DynamoDB service. Useful for development with a local dynamodb instance.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: DYNAMODB_ENDPOINT_URL`<br></br>
> `Since Version: 0.10.1`<br></br>

---

### Metadata Configs {#Metadata-Configs}

Configurations used by the Hudi Metadata Table. This table maintains the metadata about a given Hudi table (e.g file listings)  to avoid overhead of accessing cloud storage, during queries.

`Config Class`: org.apache.hudi.common.config.HoodieMetadataConfig<br></br>
> #### hoodie.metadata.index.column.stats.parallelism
> Parallelism to use, when generating column stats index.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: COLUMN_STATS_INDEX_PARALLELISM`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.compact.max.delta.commits
> Controls how often the metadata table is compacted.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: COMPACT_NUM_DELTA_COMMITS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.assume.date.partitioning
> Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASSUME_DATE_PARTITIONING`<br></br>
> `Since Version: 0.3.0`<br></br>

---

> #### hoodie.metadata.index.column.stats.enable
> Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ENABLE_METADATA_INDEX_COLUMN_STATS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.index.bloom.filter.column.list
> Comma-separated list of columns for which bloom filter index will be built. If not set, only record key will be indexed.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BLOOM_FILTER_INDEX_FOR_COLUMNS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.metrics.enable
> Enable publishing of metrics around metadata table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: METRICS_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.index.bloom.filter.file.group.count
> Metadata bloom filter index partition file group count. This controls the size of the base and log files and read parallelism in the bloom filter index partition. The recommendation is to size the file group count such that the base files are under 1GB.<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.cleaner.commits.retained
> Number of commits to retain, without cleaning, on metadata table.<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: CLEANER_COMMITS_RETAINED`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.index.check.timeout.seconds
> After the async indexer has finished indexing upto the base instant, it will ensure that all inflight writers reliably write index updates as well. If this timeout expires, then the indexer will abort itself safely.<br></br>
> **Default Value**: 900 (Optional)<br></br>
> `Config Param: METADATA_INDEX_CHECK_TIMEOUT_SECONDS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### _hoodie.metadata.ignore.spurious.deletes
> There are cases when extra files are requested to be deleted from metadata table which are never added before. This config determines how to handle such spurious deletes<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: IGNORE_SPURIOUS_DELETES`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.file.listing.parallelism
> Parallelism to use, when listing the table on lake storage.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: FILE_LISTING_PARALLELISM_VALUE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.populate.meta.fields
> When enabled, populates all meta fields. When disabled, no meta fields are populated.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: POPULATE_META_FIELDS`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metadata.index.async
> Enable asynchronous indexing of metadata table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_INDEX_ENABLE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.index.column.stats.column.list
> Comma-separated list of columns for which column stats index will be built. If not set, all columns will be indexed<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: COLUMN_STATS_INDEX_FOR_COLUMNS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.enable.full.scan.log.files
> Enable full scanning of log files while reading log records. If disabled, Hudi does look up of only interested entries.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ENABLE_FULL_SCAN_LOG_FILES`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metadata.index.column.stats.file.group.count
> Metadata column stats partition file group count. This controls the size of the base and log files and read parallelism in the column stats index partition. The recommendation is to size the file group count such that the base files are under 1GB.<br></br>
> **Default Value**: 2 (Optional)<br></br>
> `Config Param: METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.enable
> Enable the internal metadata table which serves table metadata like level file listings<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.index.bloom.filter.enable
> Enable indexing bloom filters of user data files under metadata table. When enabled, metadata table will have a partition to store the bloom filter index and will be used during the index lookups.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ENABLE_METADATA_INDEX_BLOOM_FILTER`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.index.bloom.filter.parallelism
> Parallelism to use for generating bloom filter index in metadata table.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_INDEX_PARALLELISM`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metadata.clean.async
> Enable asynchronous cleaning for metadata table<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_CLEAN_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.keep.max.commits
> Similar to hoodie.metadata.keep.min.commits, this config controls the maximum number of instants to retain in the active timeline.<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: MAX_COMMITS_TO_KEEP`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.insert.parallelism
> Parallelism to use when inserting to the metadata table<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: INSERT_PARALLELISM_VALUE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.dir.filter.regex
> Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: DIR_FILTER_REGEX`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metadata.keep.min.commits
> Archiving service moves older entries from metadata table’s timeline into an archived log after each write, to keep the overhead constant, even as the metadata table size grows.  This config controls the minimum number of instants to retain in the active timeline.<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: MIN_COMMITS_TO_KEEP`<br></br>
> `Since Version: 0.7.0`<br></br>

---

### Consistency Guard Configurations {#Consistency-Guard-Configurations}

The consistency guard related config options, to help talk to eventually consistent object storage.(Tip: S3 is NOT eventually consistent anymore!)

`Config Class`: org.apache.hudi.common.fs.ConsistencyGuardConfig<br></br>
> #### hoodie.optimistic.consistency.guard.sleep_time_ms
> Amount of time (in ms), to wait after which we assume storage is consistent.<br></br>
> **Default Value**: 500 (Optional)<br></br>
> `Config Param: OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.consistency.check.max_interval_ms
> Maximum amount of time (in ms), to wait for consistency checking.<br></br>
> **Default Value**: 20000 (Optional)<br></br>
> `Config Param: MAX_CHECK_INTERVAL_MS`<br></br>
> `Since Version: 0.5.0`<br></br>
> `Deprecated Version: 0.7.0`<br></br>

---

> #### _hoodie.optimistic.consistency.guard.enable
> Enable consistency guard, which optimistically assumes consistency is achieved after a certain time period.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: OPTIMISTIC_CONSISTENCY_GUARD_ENABLE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.consistency.check.enabled
> Enabled to handle S3 eventual consistency issue. This property is no longer required since S3 is now strongly consistent. Will be removed in the future releases.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ENABLE`<br></br>
> `Since Version: 0.5.0`<br></br>
> `Deprecated Version: 0.7.0`<br></br>

---

> #### hoodie.consistency.check.max_checks
> Maximum number of consistency checks to perform, with exponential backoff.<br></br>
> **Default Value**: 6 (Optional)<br></br>
> `Config Param: MAX_CHECKS`<br></br>
> `Since Version: 0.5.0`<br></br>
> `Deprecated Version: 0.7.0`<br></br>

---

> #### hoodie.consistency.check.initial_interval_ms
> Amount of time (in ms) to wait, before checking for consistency after an operation on storage.<br></br>
> **Default Value**: 400 (Optional)<br></br>
> `Config Param: INITIAL_CHECK_INTERVAL_MS`<br></br>
> `Since Version: 0.5.0`<br></br>
> `Deprecated Version: 0.7.0`<br></br>

---

### FileSystem Guard Configurations {#FileSystem-Guard-Configurations}

The filesystem retry related config options, to help deal with runtime exception like list/get/put/delete performance issues.

`Config Class`: org.apache.hudi.common.fs.FileSystemRetryConfig<br></br>
> #### hoodie.filesystem.operation.retry.max_interval_ms
> Maximum amount of time (in ms), to wait for next retry.<br></br>
> **Default Value**: 2000 (Optional)<br></br>
> `Config Param: MAX_RETRY_INTERVAL_MS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.filesystem.operation.retry.enable
> Enabled to handle list/get/delete etc file system performance issue.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: FILESYSTEM_RETRY_ENABLE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.filesystem.operation.retry.max_numbers
> Maximum number of retry actions to perform, with exponential backoff.<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: MAX_RETRY_NUMBERS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.filesystem.operation.retry.exceptions
> The class name of the Exception that needs to be re-tryed, separated by commas. Default is empty which means retry all the IOException and RuntimeException from FileSystem<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: RETRY_EXCEPTIONS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.filesystem.operation.retry.initial_interval_ms
> Amount of time (in ms) to wait, before retry to do operations on storage.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: INITIAL_RETRY_INTERVAL_MS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

### Write Configurations {#Write-Configurations}

Configurations that control write behavior on Hudi tables. These can be directly passed down from even higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g DeltaStreamer).

`Config Class`: org.apache.hudi.config.HoodieWriteConfig<br></br>
> #### hoodie.combine.before.upsert
> When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming,  otherwise it can lead to duplicate keys and violate the uniqueness guarantees.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_UPSERT`<br></br>

---

> #### hoodie.write.markers.type
> Marker type to use.  Two modes are supported: - DIRECT: individual marker file corresponding to each data file is directly created by the writer. - TIMELINE_SERVER_BASED: marker operations are all handled at the timeline service which serves as a proxy.  New marker entries are batch processed and stored in a limited number of underlying files for efficiency.  If HDFS is used or timeline server is disabled, DIRECT markers are used as fallback even if this is configure.  For Spark structured streaming, this configuration does not take effect, i.e., DIRECT markers are always used for Spark structured streaming.<br></br>
> **Default Value**: TIMELINE_SERVER_BASED (Optional)<br></br>
> `Config Param: MARKERS_TYPE`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.consistency.check.max_interval_ms
> Max time to wait between successive attempts at performing consistency checks<br></br>
> **Default Value**: 300000 (Optional)<br></br>
> `Config Param: MAX_CONSISTENCY_CHECK_INTERVAL_MS`<br></br>

---

> #### hoodie.embed.timeline.server.port
> Port at which the timeline server listens for requests. When running embedded in each writer, it picks a free port and communicates to all the executors. This should rarely be changed.<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_PORT_NUM`<br></br>

---

> #### hoodie.auto.adjust.lock.configs
> Auto adjust lock configurations when metadata table is enabled and for async table services.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: AUTO_ADJUST_LOCK_CONFIGS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.schema.on.read.enable
> enable full schema evolution for hoodie<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: SCHEMA_EVOLUTION_ENABLE`<br></br>

---

> #### hoodie.table.services.enabled
> Master control to disable all table services including archive, clean, compact, cluster, etc.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: TABLE_SERVICES_ENABLED`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.table.base.file.format
> Base file format to store all the base file data.<br></br>
> **Default Value**: PARQUET (Optional)<br></br>
> `Config Param: BASE_FILE_FORMAT`<br></br>

---

> #### hoodie.avro.schema.validate
> Validate the schema used for the write against the latest schema, for backwards compatibility.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: AVRO_SCHEMA_VALIDATE_ENABLE`<br></br>

---

> #### hoodie.write.buffer.limit.bytes
> Size of in-memory buffer used for parallelizing network reads and lake storage writes.<br></br>
> **Default Value**: 4194304 (Optional)<br></br>
> `Config Param: WRITE_BUFFER_LIMIT_BYTES_VALUE`<br></br>

---

> #### hoodie.insert.shuffle.parallelism
> Parallelism for inserting records into the table. Inserts can shuffle data before writing to tune file sizes and optimize the storage layout.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: INSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.embed.timeline.server.async
> Controls whether or not, the requests to the timeline server are processed in asynchronous fashion, potentially improving throughput.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE`<br></br>

---

> #### hoodie.rollback.parallelism
> Parallelism for rollback of commits. Rollbacks perform delete of files or logging delete blocks to file groups on storage in parallel.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: ROLLBACK_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.write.status.storage.level
> Write status objects hold metadata about a write (stats, errors), that is not yet committed to storage. This controls the how that information is cached for inspection by clients. We rarely expect this to be changed.<br></br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br></br>
> `Config Param: WRITE_STATUS_STORAGE_LEVEL_VALUE`<br></br>

---

> #### hoodie.writestatus.class
> Subclass of org.apache.hudi.client.WriteStatus to be used to collect information about a write. Can be overridden to collection additional metrics/statistics about the data if needed.<br></br>
> **Default Value**: org.apache.hudi.client.WriteStatus (Optional)<br></br>
> `Config Param: WRITE_STATUS_CLASS_NAME`<br></br>

---

> #### hoodie.base.path
> Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BASE_PATH`<br></br>

---

> #### hoodie.allow.empty.commit
> Whether to allow generation of empty commits, even if no data was written in the commit. It's useful in cases where extra metadata needs to be published regardless e.g tracking source offsets when ingesting data<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ALLOW_EMPTY_COMMIT`<br></br>

---

> #### hoodie.bulkinsert.user.defined.partitioner.class
> If specified, this class will be used to re-partition records before they are bulk inserted. This can be used to sort, pack, cluster data optimally for common query patterns. For now we support a build-in user defined bulkinsert partitioner org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner which can does sorting based on specified column values set by hoodie.bulkinsert.user.defined.partitioner.sort.columns<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME`<br></br>

---

> #### hoodie.table.name
> Table name that will be used for registering with metastores like HMS. Needs to be same across runs.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TBL_NAME`<br></br>

---

> #### hoodie.combine.before.delete
> During delete operations, controls whether we should combine deletes (and potentially also upserts) before  writing to storage.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_DELETE`<br></br>

---

> #### hoodie.embed.timeline.server.threads
> Number of threads to serve requests in the timeline server. By default, auto configured based on the number of underlying cores.<br></br>
> **Default Value**: -1 (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_NUM_SERVER_THREADS`<br></br>

---

> #### hoodie.fileid.prefix.provider.class
> File Id Prefix provider class, that implements `org.apache.hudi.fileid.FileIdPrefixProvider`<br></br>
> **Default Value**: org.apache.hudi.table.RandomFileIdPrefixProvider (Optional)<br></br>
> `Config Param: FILEID_PREFIX_PROVIDER_CLASS`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.fail.on.timeline.archiving
> Timeline archiving removes older instants from the timeline, after each write operation, to minimize metadata overhead. Controls whether or not, the write should be failed as well, if such archiving fails.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: FAIL_ON_TIMELINE_ARCHIVING_ENABLE`<br></br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` extract a key out of incoming records.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: KEYGENERATOR_CLASS_NAME`<br></br>

---

> #### hoodie.combine.before.insert
> When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_INSERT`<br></br>

---

> #### hoodie.embed.timeline.server.gzip
> Controls whether gzip compression is used, for large responses from the timeline server, to improve latency.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE`<br></br>

---

> #### hoodie.markers.timeline_server_based.batch.interval_ms
> The batch interval in milliseconds for marker creation batch processing<br></br>
> **Default Value**: 50 (Optional)<br></br>
> `Config Param: MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.markers.timeline_server_based.batch.num_threads
> Number of threads to use for batch processing marker creation requests at the timeline server<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### _.hoodie.allow.multi.write.on.same.instant
> <br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE`<br></br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: WRITE_PAYLOAD_CLASS_NAME`<br></br>

---

> #### hoodie.bulkinsert.shuffle.parallelism
> For large initial imports using bulk_insert operation, controls the parallelism to use for sort modes or custom partitioning donebefore writing records to the table.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: BULKINSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.delete.shuffle.parallelism
> Parallelism used for “delete” operation. Delete operations also performs shuffles, similar to upsert operation.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: DELETE_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.consistency.check.max_checks
> Maximum number of checks, for consistency of written data.<br></br>
> **Default Value**: 7 (Optional)<br></br>
> `Config Param: MAX_CONSISTENCY_CHECKS`<br></br>

---

> #### hoodie.datasource.write.keygenerator.type
> Easily configure one the built-in key generators, instead of specifying the key generator class.Currently supports SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br></br>
> **Default Value**: SIMPLE (Optional)<br></br>
> `Config Param: KEYGENERATOR_TYPE`<br></br>

---

> #### hoodie.merge.allow.duplicate.on.inserts
> When enabled, we allow duplicate keys even if inserts are routed to merge with an existing file (for ensuring file sizing). This is only relevant for insert operation, since upsert, delete operations will ensure unique key constraints are maintained.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE`<br></br>

---

> #### hoodie.embed.timeline.server.reuse.enabled
> Controls whether the timeline server instance should be cached and reused across the JVM (across task lifecycles)to avoid startup costs. This should rarely be changed.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED`<br></br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: PRECOMBINE_FIELD_NAME`<br></br>

---

> #### hoodie.bulkinsert.sort.mode
> Sorting modes to use for sorting records for bulk insert. This is use when user hoodie.bulkinsert.user.defined.partitioner.classis not configured. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads<br></br>
> **Default Value**: GLOBAL_SORT (Optional)<br></br>
> `Config Param: BULK_INSERT_SORT_MODE`<br></br>

---

> #### hoodie.avro.schema
> Schema string representing the current write schema of the table. Hudi passes this to implementations of HoodieRecordPayload to convert incoming records to avro. This is also used as the write schema evolving records during an update.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: AVRO_SCHEMA_STRING`<br></br>

---

> #### hoodie.auto.commit
> Controls whether a write operation should auto commit. This can be turned off to perform inspection of the uncommitted write before deciding to commit.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: AUTO_COMMIT_ENABLE`<br></br>

---

> #### hoodie.embed.timeline.server
> When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),running on each writer's driver process, accepting requests during the write from executors.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_ENABLE`<br></br>

---

> #### hoodie.timeline.layout.version
> Controls the layout of the timeline. Version 0 relied on renames, Version 1 (default) models the timeline as an immutable log relying only on atomic writes for object storage.<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: TIMELINE_LAYOUT_VERSION_NUM`<br></br>
> `Since Version: 0.5.1`<br></br>

---

> #### hoodie.schema.cache.enable
> cache query internalSchemas in driver/executor side<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ENABLE_INTERNAL_SCHEMA_CACHE`<br></br>

---

> #### hoodie.refresh.timeline.server.based.on.latest.commit
> Refresh timeline in timeline server based on latest commit apart from timeline hash difference. By default (false), <br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: REFRESH_TIMELINE_SERVER_BASED_ON_LATEST_COMMIT`<br></br>

---

> #### hoodie.upsert.shuffle.parallelism
> Parallelism to use for upsert operation on the table. Upserts can shuffle data to perform index lookups, file sizing, bin packing records optimallyinto file groups.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: UPSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.write.schema
> The specified write schema. In most case, we do not need set this parameter, but for the case the write schema is not equal to the specified table schema, we can specify the write schema by this parameter. Used by MergeIntoHoodieTableCommand<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: WRITE_SCHEMA`<br></br>

---

> #### hoodie.rollback.using.markers
> Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned on by default.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ROLLBACK_USING_MARKERS_ENABLE`<br></br>

---

> #### hoodie.merge.data.validation.enabled
> When enabled, data validation checks are performed during merges to ensure expected number of records after merge operation.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: MERGE_DATA_VALIDATION_CHECK_ENABLE`<br></br>

---

> #### hoodie.internal.schema
> Schema string representing the latest schema of the table. Hudi passes this to implementations of evolution of schema<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: INTERNAL_SCHEMA_STRING`<br></br>

---

> #### hoodie.client.heartbeat.tolerable.misses
> Number of heartbeat misses, before a writer is deemed not alive and all pending writes are aborted.<br></br>
> **Default Value**: 2 (Optional)<br></br>
> `Config Param: CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES`<br></br>

---

> #### hoodie.write.concurrency.mode
> Enable different concurrency modes. Options are SINGLE_WRITER: Only one active writer to the table. Maximizes throughputOPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table and exactly one of them succeed if a conflict (writes affect the same file group) is detected.<br></br>
> **Default Value**: SINGLE_WRITER (Optional)<br></br>
> `Config Param: WRITE_CONCURRENCY_MODE`<br></br>

---

> #### hoodie.markers.delete.parallelism
> Determines the parallelism for deleting marker files, which are used to track all files (valid or invalid/partial) written during a write operation. Increase this value if delays are observed, with large batch writes.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: MARKERS_DELETE_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.release.resource.on.completion.enable
> Control to enable release all persist rdds when the spark job finish.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: RELEASE_RESOURCE_ENABLE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.bulkinsert.user.defined.partitioner.sort.columns
> Columns to sort the data by when use org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner as user defined partitioner during bulk_insert. For example 'column1,column2'<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS`<br></br>

---

> #### hoodie.finalize.write.parallelism
> Parallelism for the write finalization internal operation, which involves removing any partially written files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables or low latency writes.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: FINALIZE_WRITE_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.merge.small.file.group.candidates.limit
> Limits number of file groups, whose base file satisfies small-file limit, to consider for appending records during upsert operation. Only applicable to MOR tables<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT`<br></br>

---

> #### hoodie.client.heartbeat.interval_in_ms
> Writers perform heartbeats to indicate liveness. Controls how often (in ms), such heartbeats are registered to lake storage.<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: CLIENT_HEARTBEAT_INTERVAL_IN_MS`<br></br>

---

> #### hoodie.allow.operation.metadata.field
> Whether to include '_hoodie_operation' in the metadata fields. Once enabled, all the changes of a record are persisted to the delta log directly without merge<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ALLOW_OPERATION_METADATA_FIELD`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.consistency.check.initial_interval_ms
> Initial time between successive attempts to ensure written data's metadata is consistent on storage. Grows with exponential backoff after the initial value.<br></br>
> **Default Value**: 2000 (Optional)<br></br>
> `Config Param: INITIAL_CONSISTENCY_CHECK_INTERVAL_MS`<br></br>

---

> #### hoodie.avro.schema.external.transformation
> When enabled, records in older schema are rewritten into newer schema during upsert,delete and background compaction,clustering operations.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: AVRO_EXTERNAL_SCHEMA_TRANSFORMATION_ENABLE`<br></br>

---

### Key Generator Options {#Key-Generator-Options}

Hudi maintains keys (record key + partition path) for uniquely identifying a particular record. This config allows developers to setup the Key generator class that will extract these out of incoming records.

`Config Class`: org.apache.hudi.keygen.constant.KeyGeneratorOptions<br></br>
> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: URL_ENCODE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_STYLE_PARTITIONING_ENABLE`<br></br>

---

> #### hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled
> When set to true, consistent value will be generated for a logical timestamp type column, like timestamp-millis and timestamp-micros, irrespective of whether row-writer is enabled. Disabled by default so as not to break the pipeline that deploy either fully row-writer path or non row-writer path. For example, if it is kept disabled then record key of timestamp type with value `2016-12-29 09:54:00` will be written as timestamp `2016-12-29 09:54:00.0` in row-writer path, while it will be written as long value `1483023240000000` in non row-writer path. If enabled, then the timestamp value will be written in both the cases.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED`<br></br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITIONPATH_FIELD_NAME`<br></br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br></br>
> **Default Value**: uuid (Optional)<br></br>
> `Config Param: RECORDKEY_FIELD_NAME`<br></br>

---

### HBase Index Configs {#HBase-Index-Configs}

Configurations that control indexing behavior (when HBase based indexing is enabled), which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieHBaseIndexConfig<br></br>
> #### hoodie.index.hbase.zkport
> Only applies if index type is HBASE. HBase ZK Quorum port to connect to<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZKPORT`<br></br>

---

> #### hoodie.hbase.index.update.partition.path
> Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old partition and will insert it as new record in new partition.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: UPDATE_PARTITION_PATH_ENABLE`<br></br>

---

> #### hoodie.index.hbase.qps.allocator.class
> Property to set which implementation of HBase QPS resource allocator to be used, whichcontrols the batching rate dynamically.<br></br>
> **Default Value**: org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator (Optional)<br></br>
> `Config Param: QPS_ALLOCATOR_CLASS_NAME`<br></br>

---

> #### hoodie.index.hbase.put.batch.size.autocompute
> Property to set to enable auto computation of put batch size<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: PUT_BATCH_SIZE_AUTO_COMPUTE`<br></br>

---

> #### hoodie.index.hbase.rollback.sync
> When set to true, the rollback method will delete the last failed task index. The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ROLLBACK_SYNC_ENABLE`<br></br>

---

> #### hoodie.index.hbase.get.batch.size
> Controls the batch size for performing gets against HBase. Batching improves throughput, by saving round trips.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: GET_BATCH_SIZE`<br></br>

---

> #### hoodie.index.hbase.zkpath.qps_root
> chroot in zookeeper, to use for all qps allocation co-ordination.<br></br>
> **Default Value**: /QPS_ROOT (Optional)<br></br>
> `Config Param: ZKPATH_QPS_ROOT`<br></br>

---

> #### hoodie.index.hbase.max.qps.per.region.server
> Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
 limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this
 value based on global indexing throughput needs and most importantly, how much the HBase installation in use is
 able to tolerate without Region Servers going down.<br></br>
> **Default Value**: 1000 (Optional)<br></br>
> `Config Param: MAX_QPS_PER_REGION_SERVER`<br></br>

---

> #### hoodie.index.hbase.max.qps.fraction
> Maximum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: MAX_QPS_FRACTION`<br></br>

---

> #### hoodie.index.hbase.min.qps.fraction
> Minimum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: MIN_QPS_FRACTION`<br></br>

---

> #### hoodie.index.hbase.zk.connection_timeout_ms
> Timeout to use for establishing connection with zookeeper, from HBase client.<br></br>
> **Default Value**: 15000 (Optional)<br></br>
> `Config Param: ZK_CONNECTION_TIMEOUT_MS`<br></br>

---

> #### hoodie.index.hbase.table
> Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLENAME`<br></br>

---

> #### hoodie.index.hbase.dynamic_qps
> Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on write volume.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: COMPUTE_QPS_DYNAMICALLY`<br></br>

---

> #### hoodie.index.hbase.zknode.path
> Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZK_NODE_PATH`<br></br>

---

> #### hoodie.index.hbase.zkquorum
> Only applies if index type is HBASE. HBase ZK Quorum url to connect to<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZKQUORUM`<br></br>

---

> #### hoodie.index.hbase.qps.fraction
> Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively. Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.<br></br>
> **Default Value**: 0.5 (Optional)<br></br>
> `Config Param: QPS_FRACTION`<br></br>

---

> #### hoodie.index.hbase.zk.session_timeout_ms
> Session timeout value to use for Zookeeper failure detection, for the HBase client.Lower this value, if you want to fail faster.<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: ZK_SESSION_TIMEOUT_MS`<br></br>

---

> #### hoodie.index.hbase.put.batch.size
> Controls the batch size for performing puts against HBase. Batching improves throughput, by saving round trips.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: PUT_BATCH_SIZE`<br></br>

---

> #### hoodie.index.hbase.desired_puts_time_in_secs
> <br></br>
> **Default Value**: 600 (Optional)<br></br>
> `Config Param: DESIRED_PUTS_TIME_IN_SECONDS`<br></br>

---

> #### hoodie.index.hbase.sleep.ms.for.put.batch
> <br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: SLEEP_MS_FOR_PUT_BATCH`<br></br>

---

> #### hoodie.index.hbase.sleep.ms.for.get.batch
> <br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: SLEEP_MS_FOR_GET_BATCH`<br></br>

---

### Write commit pulsar callback configs {#Write-commit-pulsar-callback-configs}

Controls notifications sent to pulsar, on events happening to a hudi table.

`Config Class`: org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig<br></br>
> #### hoodie.write.commit.callback.pulsar.operation-timeout
> Duration of waiting for completing an operation.<br></br>
> **Default Value**: 30s (Optional)<br></br>
> `Config Param: OPERATION_TIMEOUT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.topic
> pulsar topic name to publish timeline activity into.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TOPIC`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.producer.block-if-queue-full
> When the queue is full, the method is blocked instead of an exception is thrown.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PRODUCER_BLOCK_QUEUE_FULL`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.producer.send-timeout
> The timeout in each sending to pulsar.<br></br>
> **Default Value**: 30s (Optional)<br></br>
> `Config Param: PRODUCER_SEND_TIMEOUT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.broker.service.url
> Server's url of pulsar cluster, to be used for publishing commit metadata.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BROKER_SERVICE_URL`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.keepalive-interval
> Duration of keeping alive interval for each client broker connection.<br></br>
> **Default Value**: 30s (Optional)<br></br>
> `Config Param: KEEPALIVE_INTERVAL`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.producer.pending-total-size
> The maximum number of pending messages across partitions.<br></br>
> **Default Value**: 50000 (Optional)<br></br>
> `Config Param: PRODUCER_PENDING_SIZE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.request-timeout
> Duration of waiting for completing a request.<br></br>
> **Default Value**: 60s (Optional)<br></br>
> `Config Param: REQUEST_TIMEOUT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.producer.pending-queue-size
> The maximum size of a queue holding pending messages.<br></br>
> **Default Value**: 1000 (Optional)<br></br>
> `Config Param: PRODUCER_PENDING_QUEUE_SIZE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.producer.route-mode
> Message routing logic for producers on partitioned topics.<br></br>
> **Default Value**: RoundRobinPartition (Optional)<br></br>
> `Config Param: PRODUCER_ROUTE_MODE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.write.commit.callback.pulsar.connection-timeout
> Duration of waiting for a connection to a broker to be established.<br></br>
> **Default Value**: 10s (Optional)<br></br>
> `Config Param: CONNECTION_TIMEOUT`<br></br>
> `Since Version: 0.11.0`<br></br>

---

### Write commit Kafka callback configs {#Write-commit-Kafka-callback-configs}

Controls notifications sent to Kafka, on events happening to a hudi table.

`Config Class`: org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig<br></br>
> #### hoodie.write.commit.callback.kafka.topic
> Kafka topic name to publish timeline activity into.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TOPIC`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.write.commit.callback.kafka.partition
> It may be desirable to serialize all changes into a single Kafka partition  for providing strict ordering. By default, Kafka messages are keyed by table name, which  guarantees ordering at the table level, but not globally (or when new partitions are added)<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.write.commit.callback.kafka.retries
> Times to retry the produce. 3 by default<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: RETRIES`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.write.commit.callback.kafka.acks
> kafka acks level, all by default to ensure strong durability.<br></br>
> **Default Value**: all (Optional)<br></br>
> `Config Param: ACKS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.write.commit.callback.kafka.bootstrap.servers
> Bootstrap servers of kafka cluster, to be used for publishing commit metadata.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BOOTSTRAP_SERVERS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

### Locks Configurations {#Locks-Configurations}

Configs that control locking mechanisms required for concurrency control  between writers to a Hudi table. Concurrency between Hudi's own table services  are auto managed internally.

`Config Class`: org.apache.hudi.config.HoodieLockConfig<br></br>
> #### hoodie.write.lock.zookeeper.base_path
> The base path on Zookeeper under which to create lock related ZNodes. This should be same for all concurrent writers to the same table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZK_BASE_PATH`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.zookeeper.lock_key
> Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. If this parameter is not set, we would set it as the table name<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZK_LOCK_KEY`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.client.num_retries
> Maximum number of times to retry to acquire lock additionally from the lock manager.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_CLIENT_NUM_RETRIES`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.wait_time_ms_between_retry
> Initial amount of time to wait between retries to acquire locks,  subsequent retries will exponentially backoff.<br></br>
> **Default Value**: 1000 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.num_retries
> Maximum number of times to retry lock acquire, at each lock provider<br></br>
> **Default Value**: 15 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_NUM_RETRIES`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.wait_time_ms
> Timeout in ms, to wait on an individual lock acquire() call, at the lock provider.<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_WAIT_TIMEOUT_MS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.zookeeper.connection_timeout_ms
> Timeout in ms, to wait for establishing connection with Zookeeper.<br></br>
> **Default Value**: 15000 (Optional)<br></br>
> `Config Param: ZK_CONNECTION_TIMEOUT_MS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.zookeeper.port
> Zookeeper port to connect to.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZK_PORT`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.hivemetastore.table
> For Hive based lock provider, the Hive table to acquire lock against<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_TABLE_NAME`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.zookeeper.url
> Zookeeper URL to connect to.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: ZK_CONNECT_URL`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.filesystem.path
> For DFS based lock providers, path to store the locks under.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: FILESYSTEM_LOCK_PATH`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.provider
> Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider<br></br>
> **Default Value**: org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider (Optional)<br></br>
> `Config Param: LOCK_PROVIDER_CLASS_NAME`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.zookeeper.session_timeout_ms
> Timeout in ms, to wait after losing connection to ZooKeeper, before the session is expired<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: ZK_SESSION_TIMEOUT_MS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.conflict.resolution.strategy
> Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy<br></br>
> **Default Value**: org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy (Optional)<br></br>
> `Config Param: WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.hivemetastore.database
> For Hive based lock provider, the Hive database to acquire lock against<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_DATABASE_NAME`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.hivemetastore.uris
> For Hive based lock provider, the Hive metastore URI to acquire locks against.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_METASTORE_URI`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.max_wait_time_ms_between_retry
> Maximum amount of time to wait between retries by lock provider client. This bounds the maximum delay from the exponential backoff. Currently used by ZK based lock provider only.<br></br>
> **Default Value**: 5000 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

> #### hoodie.write.lock.client.wait_time_ms_between_retry
> Amount of time to wait between retries on the lock provider by the lock manager<br></br>
> **Default Value**: 10000 (Optional)<br></br>
> `Config Param: LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS`<br></br>
> `Since Version: 0.8.0`<br></br>

---

### Compaction Configs {#Compaction-Configs}

Configurations that control compaction (merging of log files onto a new base files) as well as  cleaning (reclamation of older/unused file groups/slices).

`Config Class`: org.apache.hudi.config.HoodieCompactionConfig<br></br>
> #### hoodie.compaction.payload.class
> This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: PAYLOAD_CLASS_NAME`<br></br>

---

> #### hoodie.copyonwrite.record.size.estimate
> The average record size. If not explicitly specified, hudi will compute the record size estimate compute dynamically based on commit metadata.  This is critical in computing the insert parallelism and bin-packing inserts into small files.<br></br>
> **Default Value**: 1024 (Optional)<br></br>
> `Config Param: COPY_ON_WRITE_RECORD_SIZE_ESTIMATE`<br></br>

---

> #### hoodie.cleaner.policy
> Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space. By default, cleaner spares the file slices written by the last N commits, determined by  hoodie.cleaner.commits.retained Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had   a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time<br></br>
> **Default Value**: KEEP_LATEST_COMMITS (Optional)<br></br>
> `Config Param: CLEANER_POLICY`<br></br>

---

> #### hoodie.compact.inline.max.delta.seconds
> Number of elapsed seconds after the last compaction, before scheduling a new one.<br></br>
> **Default Value**: 3600 (Optional)<br></br>
> `Config Param: INLINE_COMPACT_TIME_DELTA_SECONDS`<br></br>

---

> #### hoodie.cleaner.delete.bootstrap.base.file
> When set to true, cleaner also deletes the bootstrap base file when it's skeleton base file is  cleaned. Turn this to true, if you want to ensure the bootstrap dataset storage is reclaimed over time, as the table receives updates/deletes. Another reason to turn this on, would be to ensure data residing in bootstrap  base files are also physically deleted, to comply with data privacy enforcement processes.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: CLEANER_BOOTSTRAP_BASE_FILE_ENABLE`<br></br>

---

> #### hoodie.archive.merge.enable
> When enable, hoodie will auto merge several small archive files into larger one. It's useful when storage scheme doesn't support append operation.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_ENABLE`<br></br>

---

> #### hoodie.cleaner.commits.retained
> Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: CLEANER_COMMITS_RETAINED`<br></br>

---

> #### hoodie.cleaner.policy.failed.writes
> Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)<br></br>
> **Default Value**: EAGER (Optional)<br></br>
> `Config Param: FAILED_WRITES_CLEANER_POLICY`<br></br>

---

> #### hoodie.compaction.logfile.size.threshold
> Only if the log file size is greater than the threshold in bytes, the file group will be compacted.<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: COMPACTION_LOG_FILE_SIZE_THRESHOLD`<br></br>

---

> #### hoodie.clean.async
> Only applies when hoodie.clean.automatic is turned on. When turned on runs cleaner async with writing, which can speed up overall write performance.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_CLEAN`<br></br>

---

> #### hoodie.clean.automatic
> When enabled, the cleaner table service is invoked immediately after each commit, to delete older file slices. It's recommended to enable this, to ensure metadata and data storage growth is bounded.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: AUTO_CLEAN`<br></br>

---

> #### hoodie.commits.archival.batch
> Archiving of instants is batched in best-effort manner, to pack more instants into a single archive log. This config controls such archival batch size.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: COMMITS_ARCHIVAL_BATCH_SIZE`<br></br>

---

> #### hoodie.compaction.reverse.log.read
> HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the reader reads the logfile in reverse direction, from pos=file_length to pos=0<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: COMPACTION_REVERSE_LOG_READ_ENABLE`<br></br>

---

> #### hoodie.clean.allow.multiple
> Allows scheduling/executing multiple cleans by enabling this config. If users prefer to strictly ensure clean requests should be mutually exclusive, .i.e. a 2nd clean will not be scheduled if another clean is not yet completed to avoid repeat cleaning of same files, they might want to disable this config.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ALLOW_MULTIPLE_CLEANS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.archive.merge.small.file.limit.bytes
> This config sets the archive file size limit below which an archive file becomes a candidate to be selected as such a small file.<br></br>
> **Default Value**: 20971520 (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES`<br></br>

---

> #### hoodie.cleaner.fileversions.retained
> When KEEP_LATEST_FILE_VERSIONS cleaning policy is used,  the minimum number of file slices to retain in each file group, during cleaning.<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: CLEANER_FILE_VERSIONS_RETAINED`<br></br>

---

> #### hoodie.compact.inline
> When set to true, compaction service is triggered after each write. While being  simpler operationally, this adds extra latency on the write path.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INLINE_COMPACT`<br></br>

---

> #### hoodie.clean.max.commits
> Number of commits after the last clean operation, before scheduling of a new clean is attempted.<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: CLEAN_MAX_COMMITS`<br></br>

---

> #### hoodie.compaction.lazy.block.read
> When merging the delta log files, this config helps to choose whether the log blocks should be read lazily or not. Choose true to use lazy block reading (low memory usage, but incurs seeks to each block header) or false for immediate block read (higher memory usage)<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMPACTION_LAZY_BLOCK_READ_ENABLE`<br></br>

---

> #### hoodie.archive.merge.files.batch.size
> The number of small archive files to be merged at once.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_FILES_BATCH_SIZE`<br></br>

---

> #### hoodie.archive.async
> Only applies when hoodie.archive.automatic is turned on. When turned on runs archiver async with writing, which can speed up overall write performance.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_ARCHIVE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.parquet.small.file.limit
> During upsert operation, we opportunistically expand existing small files on storage, instead of writing new files, to keep number of files to an optimum. This config sets the file size limit below which a file on storage  becomes a candidate to be selected as such a `small file`. By default, treat any file <= 100MB as a small file. Also note that if this set <= 0, will not try to get small files and directly write new files<br></br>
> **Default Value**: 104857600 (Optional)<br></br>
> `Config Param: PARQUET_SMALL_FILE_LIMIT`<br></br>

---

> #### hoodie.compaction.strategy
> Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data<br></br>
> **Default Value**: org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy (Optional)<br></br>
> `Config Param: COMPACTION_STRATEGY`<br></br>

---

> #### hoodie.cleaner.hours.retained
> Number of hours for which commits need to be retained. This config provides a more flexible option ascompared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group, corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.<br></br>
> **Default Value**: 24 (Optional)<br></br>
> `Config Param: CLEANER_HOURS_RETAINED`<br></br>

---

> #### hoodie.compaction.target.io
> Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.<br></br>
> **Default Value**: 512000 (Optional)<br></br>
> `Config Param: TARGET_IO_PER_COMPACTION_IN_MB`<br></br>

---

> #### hoodie.archive.automatic
> When enabled, the archival table service is invoked immediately after each commit, to archive commits if we cross a maximum value of commits. It's recommended to enable this, to ensure number of active commits is bounded.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: AUTO_ARCHIVE`<br></br>

---

> #### hoodie.clean.trigger.strategy
> Controls how cleaning is scheduled. Valid options: NUM_COMMITS<br></br>
> **Default Value**: NUM_COMMITS (Optional)<br></br>
> `Config Param: CLEAN_TRIGGER_STRATEGY`<br></br>

---

> #### hoodie.compaction.preserve.commit.metadata
> When rewriting data, preserves existing hoodie_commit_time<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PRESERVE_COMMIT_METADATA`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.copyonwrite.insert.auto.split
> Config to control whether we control insert split sizes automatically based on average record sizes. It's recommended to keep this turned on, since hand tuning is otherwise extremely cumbersome.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COPY_ON_WRITE_AUTO_SPLIT_INSERTS`<br></br>

---

> #### hoodie.compact.inline.max.delta.commits
> Number of delta commits after the last compaction, before scheduling of a new compaction is attempted.<br></br>
> **Default Value**: 5 (Optional)<br></br>
> `Config Param: INLINE_COMPACT_NUM_DELTA_COMMITS`<br></br>

---

> #### hoodie.keep.min.commits
> Similar to hoodie.keep.max.commits, but controls the minimum number ofinstants to retain in the active timeline.<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: MIN_COMMITS_TO_KEEP`<br></br>

---

> #### hoodie.cleaner.parallelism
> Parallelism for the cleaning operation. Increase this if cleaning becomes slow.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: CLEANER_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.cleaner.incremental.mode
> When enabled, the plans for each cleaner service run is computed incrementally off the events  in the timeline, since the last cleaner run. This is much more efficient than obtaining listings for the full table for each planning (even with a metadata table).<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: CLEANER_INCREMENTAL_MODE_ENABLE`<br></br>

---

> #### hoodie.record.size.estimation.threshold
> We use the previous commits' metadata to calculate the estimated record size and use it  to bin pack records into partitions. If the previous commit is too small to make an accurate estimation,  Hudi will search commits in the reverse order, until we find a commit that has totalBytesWritten  larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * this_threshold)<br></br>
> **Default Value**: 1.0 (Optional)<br></br>
> `Config Param: RECORD_SIZE_ESTIMATION_THRESHOLD`<br></br>

---

> #### hoodie.compact.inline.trigger.strategy
> Controls how compaction scheduling is triggered, by time or num delta commits or combination of both. Valid options: NUM_COMMITS,TIME_ELAPSED,NUM_AND_TIME,NUM_OR_TIME<br></br>
> **Default Value**: NUM_COMMITS (Optional)<br></br>
> `Config Param: INLINE_COMPACT_TRIGGER_STRATEGY`<br></br>

---

> #### hoodie.keep.max.commits
> Archiving service moves older entries from timeline into an archived log after each write, to  keep the metadata overhead constant, even as the table size grows.This config controls the maximum number of instants to retain in the active timeline. <br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: MAX_COMMITS_TO_KEEP`<br></br>

---

> #### hoodie.archive.delete.parallelism
> Parallelism for deleting archived hoodie commits.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.copyonwrite.insert.split.size
> Number of inserts assigned for each partition/bucket for writing. We based the default on writing out 100MB files, with at least 1kb records (100K records per file), and   over provision to 500K. As long as auto-tuning of splits is turned on, this only affects the first   write, where there is no history to learn record sizes from.<br></br>
> **Default Value**: 500000 (Optional)<br></br>
> `Config Param: COPY_ON_WRITE_INSERT_SPLIT_SIZE`<br></br>

---

> #### hoodie.compact.schedule.inline
> When set to true, compaction service will be attempted for inline scheduling after each write. Users have to ensure they have a separate job to run async compaction(execution) for the one scheduled by this writer. Users can choose to set both `hoodie.compact.inline` and `hoodie.compact.schedule.inline` to false and have both scheduling and execution triggered by any async process. But if `hoodie.compact.inline` is set to false, and `hoodie.compact.schedule.inline` is set to true, regular writers will schedule compaction inline, but users are expected to trigger async job for execution. If `hoodie.compact.inline` is set to true, regular writers will do both scheduling and execution inline for compaction<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: SCHEDULE_INLINE_COMPACT`<br></br>

---

> #### hoodie.compaction.daybased.target.partitions
> Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: TARGET_PARTITIONS_PER_DAYBASED_COMPACTION`<br></br>

---

### File System View Storage Configurations {#File-System-View-Storage-Configurations}

Configurations that control how file metadata is stored by Hudi, for transaction processing and queries.

`Config Class`: org.apache.hudi.common.table.view.FileSystemViewStorageConfig<br></br>
> #### hoodie.filesystem.view.spillable.replaced.mem.fraction
> Fraction of the file system view memory, to be used for holding replace commit related metadata.<br></br>
> **Default Value**: 0.01 (Optional)<br></br>
> `Config Param: SPILLABLE_REPLACED_MEM_FRACTION`<br></br>

---

> #### hoodie.filesystem.view.spillable.dir
> Path on local storage to use, when file system view is held in a spillable map.<br></br>
> **Default Value**: /tmp/ (Optional)<br></br>
> `Config Param: SPILLABLE_DIR`<br></br>

---

> #### hoodie.filesystem.remote.backup.view.enable
> Config to control whether backup needs to be configured if clients were not able to reach timeline service.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: REMOTE_BACKUP_VIEW_ENABLE`<br></br>

---

> #### hoodie.filesystem.view.spillable.compaction.mem.fraction
> Fraction of the file system view memory, to be used for holding compaction related metadata.<br></br>
> **Default Value**: 0.8 (Optional)<br></br>
> `Config Param: SPILLABLE_COMPACTION_MEM_FRACTION`<br></br>

---

> #### hoodie.filesystem.view.spillable.mem
> Amount of memory to be used in bytes for holding file system view, before spilling to disk.<br></br>
> **Default Value**: 104857600 (Optional)<br></br>
> `Config Param: SPILLABLE_MEMORY`<br></br>

---

> #### hoodie.filesystem.view.secondary.type
> Specifies the secondary form of storage for file system view, if the primary (e.g timeline server)  is unavailable.<br></br>
> **Default Value**: MEMORY (Optional)<br></br>
> `Config Param: SECONDARY_VIEW_TYPE`<br></br>

---

> #### hoodie.filesystem.view.remote.host
> We expect this to be rarely hand configured.<br></br>
> **Default Value**: localhost (Optional)<br></br>
> `Config Param: REMOTE_HOST_NAME`<br></br>

---

> #### hoodie.filesystem.view.type
> File system view provides APIs for viewing the files on the underlying lake storage,  as file groups and file slices. This config controls how such a view is held. Options include MEMORY,SPILLABLE_DISK,EMBEDDED_KV_STORE,REMOTE_ONLY,REMOTE_FIRST which provide different trade offs for memory usage and API request performance.<br></br>
> **Default Value**: MEMORY (Optional)<br></br>
> `Config Param: VIEW_TYPE`<br></br>

---

> #### hoodie.filesystem.view.remote.timeout.secs
> Timeout in seconds, to wait for API requests against a remote file system view. e.g timeline server.<br></br>
> **Default Value**: 300 (Optional)<br></br>
> `Config Param: REMOTE_TIMEOUT_SECS`<br></br>

---

> #### hoodie.filesystem.view.remote.port
> Port to serve file system view queries, when remote. We expect this to be rarely hand configured.<br></br>
> **Default Value**: 26754 (Optional)<br></br>
> `Config Param: REMOTE_PORT_NUM`<br></br>

---

> #### hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction
> Fraction of the file system view memory, to be used for holding mapping to bootstrap base files.<br></br>
> **Default Value**: 0.05 (Optional)<br></br>
> `Config Param: BOOTSTRAP_BASE_FILE_MEM_FRACTION`<br></br>

---

> #### hoodie.filesystem.view.spillable.clustering.mem.fraction
> Fraction of the file system view memory, to be used for holding clustering related metadata.<br></br>
> **Default Value**: 0.01 (Optional)<br></br>
> `Config Param: SPILLABLE_CLUSTERING_MEM_FRACTION`<br></br>

---

> #### hoodie.filesystem.view.rocksdb.base.path
> Path on local storage to use, when storing file system view in embedded kv store/rocksdb.<br></br>
> **Default Value**: /tmp/hoodie_timeline_rocksdb (Optional)<br></br>
> `Config Param: ROCKSDB_BASE_PATH`<br></br>

---

> #### hoodie.filesystem.view.incr.timeline.sync.enable
> Controls whether or not, the file system view is incrementally updated as new actions are performed on the timeline.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INCREMENTAL_TIMELINE_SYNC_ENABLE`<br></br>

---

### Index Configs {#Index-Configs}

Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieIndexConfig<br></br>
> #### hoodie.index.bloom.num_entries
> Only applies if index type is BLOOM. This is the number of entries to be stored in the bloom filter. The rationale for the default: Assume the maxParquetFileSize is 128MB and averageRecordSize is 1kb and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and setting this to a very high number will increase the size every base file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_NUM_ENTRIES_VALUE`<br></br>

---

> #### hoodie.bloom.index.keys.per.bucket
> Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. This configuration controls the “bucket” size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory.<br></br>
> **Default Value**: 10000000 (Optional)<br></br>
> `Config Param: BLOOM_INDEX_KEYS_PER_BUCKET`<br></br>

---

> #### hoodie.simple.index.input.storage.level
> Only applies when #simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br></br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE`<br></br>

---

> #### hoodie.simple.index.parallelism
> Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.global.simple.index.parallelism
> Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: GLOBAL_SIMPLE_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.simple.index.update.partition.path
> Similar to Key: 'hoodie.bloom.index.update.partition.path' , default: true description: Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition since version: version is not defined deprecated after: version is not defined), but for simple index.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE`<br></br>

---

> #### hoodie.bucket.index.num.buckets
> Only applies if index type is BUCKET_INDEX. Determine the number of buckets in the hudi table, and each partition is divided to N buckets.<br></br>
> **Default Value**: 256 (Optional)<br></br>
> `Config Param: BUCKET_INDEX_NUM_BUCKETS`<br></br>

---

> #### hoodie.bucket.index.hash.field
> Index key. It is used to index the record and find its file group. If not set, use record key field as default<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BUCKET_INDEX_HASH_FIELD`<br></br>

---

> #### hoodie.bloom.index.use.metadata
> Only applies if index type is BLOOM.When true, the index lookup uses bloom filters and column stats from metadata table when available to speed up the process.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: BLOOM_INDEX_USE_METADATA`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.bloom.index.bucketized.checking
> Only applies if index type is BLOOM. When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_BUCKETIZED_CHECKING`<br></br>

---

> #### hoodie.index.type
> Type of index to use. Default is Bloom filter. Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE | BUCKET]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: INDEX_TYPE`<br></br>

---

> #### hoodie.index.bloom.fpp
> Only applies if index type is BLOOM. Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the configured value (hoodie.index.bloom.num_entries), then this fpp may not be honored.<br></br>
> **Default Value**: 0.000000001 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_FPP_VALUE`<br></br>

---

> #### hoodie.bloom.index.update.partition.path
> Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE`<br></br>

---

> #### hoodie.bloom.index.use.caching
> Only applies if index type is BLOOM.When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_USE_CACHING`<br></br>

---

> #### hoodie.bloom.index.input.storage.level
> Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br></br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br></br>
> `Config Param: BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE`<br></br>

---

> #### hoodie.bloom.index.use.treebased.filter
> Only applies if index type is BLOOM. When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_TREE_BASED_FILTER`<br></br>

---

> #### hoodie.bloom.index.parallelism
> Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a shuffle. By default, this is auto computed based on input workload characteristics.<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: BLOOM_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.bloom.index.filter.dynamic.max.entries
> The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.<br></br>
> **Default Value**: 100000 (Optional)<br></br>
> `Config Param: BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES`<br></br>

---

> #### hoodie.simple.index.use.caching
> Only applies if index type is SIMPLE. When true, the incoming writes will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_USE_CACHING`<br></br>

---

> #### hoodie.bloom.index.prune.by.ranges
> Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off, since range pruning will only  add extra overhead to the index lookup.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_PRUNE_BY_RANGES`<br></br>

---

> #### hoodie.bloom.index.filter.type
> Filter type used. Default is BloomFilterTypeCode.DYNAMIC_V0. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.<br></br>
> **Default Value**: DYNAMIC_V0 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_TYPE`<br></br>

---

> #### hoodie.index.class
> Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: INDEX_CLASS_NAME`<br></br>

---

### Clustering Configs {#Clustering-Configs}

Configurations that control the clustering table service in hudi, which optimizes the storage layout for better query performance by sorting and sizing data files.

`Config Class`: org.apache.hudi.config.HoodieClusteringConfig<br></br>
> #### hoodie.clustering.plan.strategy.cluster.end.partition
> End partition used to filter partition (inclusive), only effective when the filter mode 'hoodie.clustering.plan.partition.filter.mode' is SELECTED_PARTITIONS<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_FILTER_END_PARTITION`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.rollback.pending.replacecommit.on.conflict
> If updates are allowed to file groups pending clustering, then set this config to rollback failed or pending clustering instants. Pending clustering will be rolled back ONLY IF there is conflict between incoming upsert and filegroup to be clustered. Please exercise caution while setting this config, especially when clustering is done very frequently. This could lead to race condition in rare scenarios, for example, when the clustering completes after instants are fetched but before rollback completed.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ROLLBACK_PENDING_CLUSTERING_ON_CONFLICT`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.clustering.async.max.commits
> Config to control frequency of async clustering<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: ASYNC_CLUSTERING_MAX_COMMITS`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.clustering.inline.max.commits
> Config to control frequency of clustering planning<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: INLINE_CLUSTERING_MAX_COMMITS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.layout.optimize.enable
> This setting has no effect. Please refer to clustering configuration, as well as LAYOUT_OPTIMIZE_STRATEGY config to enable advanced record layout optimization strategies<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: LAYOUT_OPTIMIZE_ENABLE`<br></br>
> `Since Version: 0.10.0`<br></br>
> `Deprecated Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.target.file.max.bytes
> Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br></br>
> **Default Value**: 1073741824 (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions
> Number of partitions to skip from latest when choosing partitions to create ClusteringPlan<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.clustering.execution.strategy.class
> Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the  clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while  meeting the configured target file sizes.<br></br>
> **Default Value**: org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy (Optional)<br></br>
> `Config Param: EXECUTION_STRATEGY_CLASS_NAME`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.async.enabled
> Enable running of clustering service, asynchronously as inserts happen on the table.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_CLUSTERING_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.class
> Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan i.e select what file groups are being clustered. Default strategy, looks at the clustering small file size limit (determined by hoodie.clustering.plan.strategy.small.file.limit) to pick the small file slices within partitions for clustering.<br></br>
> **Default Value**: org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_CLASS_NAME`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.layout.optimize.build.curve.sample.size
> Determines target sample size used by the Boundary-based Interleaved Index method of building space-filling curve. Larger sample size entails better layout optimization outcomes, at the expense of higher memory footprint.<br></br>
> **Default Value**: 200000 (Optional)<br></br>
> `Config Param: LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.partition.selected
> Partitions to run clustering<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_SELECTED`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.updates.strategy
> Determines how to handle updates, deletes to file groups that are under clustering. Default strategy just rejects the update<br></br>
> **Default Value**: org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy (Optional)<br></br>
> `Config Param: UPDATES_STRATEGY`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.layout.optimize.strategy
> Determines ordering strategy used in records layout optimization. Currently supported strategies are "linear", "z-order" and "hilbert" values are supported.<br></br>
> **Default Value**: linear (Optional)<br></br>
> `Config Param: LAYOUT_OPTIMIZE_STRATEGY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.clustering.inline
> Turn on inline clustering - clustering will be run after each write operation is complete<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INLINE_CLUSTERING`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.cluster.begin.partition
> Begin partition used to filter partition (inclusive), only effective when the filter mode 'hoodie.clustering.plan.partition.filter.mode' is SELECTED_PARTITIONS<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_FILTER_BEGIN_PARTITION`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.sort.columns
> Columns to sort the data by when clustering<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PLAN_STRATEGY_SORT_COLUMNS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.preserve.commit.metadata
> When rewriting data, preserves existing hoodie_commit_time<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PRESERVE_COMMIT_METADATA`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.max.num.groups
> Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_MAX_GROUPS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.plan.partition.filter.mode
> Partition filter mode used in the creation of clustering plan. Available values are - NONE: do not filter table partition and thus the clustering plan will include all partitions that have clustering candidate.RECENT_DAYS: keep a continuous range of partitions, worked together with configs 'hoodie.clustering.plan.strategy.daybased.lookback.partitions' and 'hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions.SELECTED_PARTITIONS: keep partitions that are in the specified range ['hoodie.clustering.plan.strategy.cluster.begin.partition', 'hoodie.clustering.plan.strategy.cluster.end.partition'].<br></br>
> **Default Value**: NONE (Optional)<br></br>
> `Config Param: PLAN_PARTITION_FILTER_MODE_NAME`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.schedule.inline
> When set to true, clustering service will be attempted for inline scheduling after each write. Users have to ensure they have a separate job to run async clustering(execution) for the one scheduled by this writer. Users can choose to set both `hoodie.clustering.inline` and `hoodie.clustering.schedule.inline` to false and have both scheduling and execution triggered by any async process, on which case `hoodie.clustering.async.enabled` is expected to be set to true. But if `hoodie.clustering.inline` is set to false, and `hoodie.clustering.schedule.inline` is set to true, regular writers will schedule clustering inline, but users are expected to trigger async job for execution. If `hoodie.clustering.inline` is set to true, regular writers will do both scheduling and execution inline for clustering<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: SCHEDULE_INLINE_CLUSTERING`<br></br>

---

> #### hoodie.layout.optimize.data.skipping.enable
> Enable data skipping by collecting statistics once layout optimization is complete.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: LAYOUT_OPTIMIZE_DATA_SKIPPING_ENABLE`<br></br>
> `Since Version: 0.10.0`<br></br>
> `Deprecated Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.max.bytes.per.group
> Each clustering operation can create multiple output file groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). Max amount of data to be included in one group<br></br>
> **Default Value**: 2147483648 (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.small.file.limit
> Files smaller than the size in bytes specified here are candidates for clustering<br></br>
> **Default Value**: 314572800 (Optional)<br></br>
> `Config Param: PLAN_STRATEGY_SMALL_FILE_LIMIT`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.layout.optimize.curve.build.method
> Controls how data is sampled to build the space-filling curves. Two methods: "direct", "sample". The direct method is faster than the sampling, however sample method would produce a better data layout.<br></br>
> **Default Value**: direct (Optional)<br></br>
> `Config Param: LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.partition.regex.pattern
> Filter clustering partitions that matched regex pattern<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITION_REGEX_PATTERN`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clustering.plan.strategy.daybased.lookback.partitions
> Number of partitions to list to create ClusteringPlan<br></br>
> **Default Value**: 2 (Optional)<br></br>
> `Config Param: DAYBASED_LOOKBACK_PARTITIONS`<br></br>
> `Since Version: 0.7.0`<br></br>

---

### Common Configurations {#Common-Configurations}

The following set of configurations are common across Hudi.

`Config Class`: org.apache.hudi.common.config.HoodieCommonConfig<br></br>
> #### hoodie.common.diskmap.compression.enabled
> Turn on compression for BITCASK disk map used by the External Spillable Map<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: DISK_MAP_BITCASK_COMPRESSION_ENABLED`<br></br>

---

> #### hoodie.common.spillable.diskmap.type
> When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed.  By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.<br></br>
> **Default Value**: BITCASK (Optional)<br></br>
> `Config Param: SPILLABLE_DISK_MAP_TYPE`<br></br>

---

### Bootstrap Configs {#Bootstrap-Configs}

Configurations that control how you want to bootstrap your existing tables for the first time into hudi. The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing  writers and new hudi writers in parallel, to validate the migration.

`Config Class`: org.apache.hudi.config.HoodieBootstrapConfig<br></br>
> #### hoodie.bootstrap.partitionpath.translator.class
> Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.<br></br>
> **Default Value**: org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator (Optional)<br></br>
> `Config Param: PARTITION_PATH_TRANSLATOR_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.full.input.provider
> Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD<br></br>
> **Default Value**: org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider (Optional)<br></br>
> `Config Param: FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.keygen.type
> Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br></br>
> **Default Value**: SIMPLE (Optional)<br></br>
> `Config Param: KEYGEN_TYPE`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.bootstrap.keygen.class
> Key generator implementation to be used for generating keys from the bootstrapped dataset<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: KEYGEN_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.parallelism
> Parallelism value to be used to bootstrap data into hudi<br></br>
> **Default Value**: 1500 (Optional)<br></br>
> `Config Param: PARALLELISM_VALUE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BASE_PATH`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.mode.selector.regex
> Matches each bootstrap dataset partition against this regex and applies the mode below to it.<br></br>
> **Default Value**: .* (Optional)<br></br>
> `Config Param: PARTITION_SELECTOR_REGEX_PATTERN`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.index.class
> Implementation to use, for mapping a skeleton base file to a boostrap base file.<br></br>
> **Default Value**: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex (Optional)<br></br>
> `Config Param: INDEX_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.mode.selector.regex.mode
> Bootstrap mode to apply for partition paths, that match regex above. METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.<br></br>
> **Default Value**: METADATA_ONLY (Optional)<br></br>
> `Config Param: PARTITION_SELECTOR_REGEX_MODE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.bootstrap.mode.selector
> Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped<br></br>
> **Default Value**: org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector (Optional)<br></br>
> `Config Param: MODE_SELECTOR_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.

### Metrics Configurations for Datadog reporter {#Metrics-Configurations-for-Datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig<br></br>
> #### hoodie.metrics.datadog.api.timeout.seconds
> Datadog API timeout in seconds. Default to 3.<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: API_TIMEOUT_IN_SECONDS`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.metric.prefix
> Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: METRIC_PREFIX_VALUE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.api.site
> Datadog API site: EU or US<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: API_SITE_VALUE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.api.key.skip.validation
> Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: API_KEY_SKIP_VALIDATION`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.metric.host
> Datadog metric host to be sent along with metrics data.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: METRIC_HOST_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.report.period.seconds
> Datadog reporting period in seconds. Default to 30.<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: REPORT_PERIOD_IN_SECONDS`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.api.key
> Datadog API key<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: API_KEY`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.api.key.supplier
> Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: API_KEY_SUPPLIER`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.datadog.metric.tags
> Datadog metric tags (comma-delimited) to be sent along with metrics data.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: METRIC_TAG_VALUES`<br></br>
> `Since Version: 0.6.0`<br></br>

---

### Metrics Configurations {#Metrics-Configurations}

Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsConfig<br></br>
> #### hoodie.metrics.executor.enable
> <br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: EXECUTOR_METRICS_ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

> #### hoodie.metrics.reporter.metricsname.prefix
> The prefix given to the metrics names.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: METRICS_REPORTER_PREFIX`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.metrics.reporter.type
> Type of metrics reporter.<br></br>
> **Default Value**: GRAPHITE (Optional)<br></br>
> `Config Param: METRICS_REPORTER_TYPE_VALUE`<br></br>
> `Since Version: 0.5.0`<br></br>

---

> #### hoodie.metrics.reporter.class
> <br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: METRICS_REPORTER_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.on
> Turn on/off metrics reporting. off by default.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: TURN_METRICS_ON`<br></br>
> `Since Version: 0.5.0`<br></br>

---

### Metrics Configurations for Jmx {#Metrics-Configurations-for-Jmx}

Enables reporting on Hudi metrics using Jmx.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsJmxConfig<br></br>
> #### hoodie.metrics.jmx.host
> Jmx host to connect to<br></br>
> **Default Value**: localhost (Optional)<br></br>
> `Config Param: JMX_HOST_NAME`<br></br>
> `Since Version: 0.5.1`<br></br>

---

> #### hoodie.metrics.jmx.port
> Jmx port to connect to<br></br>
> **Default Value**: 9889 (Optional)<br></br>
> `Config Param: JMX_PORT_NUM`<br></br>
> `Since Version: 0.5.1`<br></br>

---

### Metrics Configurations for Prometheus {#Metrics-Configurations-for-Prometheus}

Enables reporting on Hudi metrics using Prometheus.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig<br></br>
> #### hoodie.metrics.pushgateway.random.job.name.suffix
> Whether the pushgateway name need a random suffix , default true.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.pushgateway.port
> Port for the push gateway.<br></br>
> **Default Value**: 9091 (Optional)<br></br>
> `Config Param: PUSHGATEWAY_PORT_NUM`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.pushgateway.delete.on.shutdown
> Delete the pushgateway info or not when job shutdown, true by default.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.prometheus.port
> Port for prometheus server.<br></br>
> **Default Value**: 9090 (Optional)<br></br>
> `Config Param: PROMETHEUS_PORT_NUM`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.pushgateway.job.name
> Name of the push gateway job.<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: PUSHGATEWAY_JOBNAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.pushgateway.report.period.seconds
> Reporting interval in seconds.<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS`<br></br>
> `Since Version: 0.6.0`<br></br>

---

> #### hoodie.metrics.pushgateway.host
> Hostname of the prometheus push gateway.<br></br>
> **Default Value**: localhost (Optional)<br></br>
> `Config Param: PUSHGATEWAY_HOST_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

### Metrics Configurations for Amazon CloudWatch {#Metrics-Configurations-for-Amazon-CloudWatch}

Enables reporting on Hudi metrics using Amazon CloudWatch.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsCloudWatchConfig<br></br>
> #### hoodie.metrics.cloudwatch.report.period.seconds
> Reporting interval in seconds<br></br>
> **Default Value**: 60 (Optional)<br></br>
> `Config Param: REPORT_PERIOD_SECONDS`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metrics.cloudwatch.namespace
> Namespace of reporter<br></br>
> **Default Value**: Hudi (Optional)<br></br>
> `Config Param: METRIC_NAMESPACE`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metrics.cloudwatch.metric.prefix
> Metric prefix of reporter<br></br>
> **Default Value**:  (Optional)<br></br>
> `Config Param: METRIC_PREFIX`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metrics.cloudwatch.maxDatumsPerRequest
> Max number of Datums per request<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: MAX_DATUMS_PER_REQUEST`<br></br>
> `Since Version: 0.10.0`<br></br>

---

### Metrics Configurations for Graphite {#Metrics-Configurations-for-Graphite}

Enables reporting on Hudi metrics using Graphite.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig<br></br>
> #### hoodie.metrics.graphite.port
> Graphite port to connect to.<br></br>
> **Default Value**: 4756 (Optional)<br></br>
> `Config Param: GRAPHITE_SERVER_PORT_NUM`<br></br>
> `Since Version: 0.5.0`<br></br>

---

> #### hoodie.metrics.graphite.report.period.seconds
> Graphite reporting period in seconds. Default to 30.<br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: GRAPHITE_REPORT_PERIOD_IN_SECONDS`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.metrics.graphite.host
> Graphite host to connect to.<br></br>
> **Default Value**: localhost (Optional)<br></br>
> `Config Param: GRAPHITE_SERVER_HOST_NAME`<br></br>
> `Since Version: 0.5.0`<br></br>

---

> #### hoodie.metrics.graphite.metric.prefix
> Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: GRAPHITE_METRIC_PREFIX_VALUE`<br></br>
> `Since Version: 0.5.1`<br></br>

---

## Record Payload Config {#RECORD_PAYLOAD}
This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

### Payload Configurations {#Payload-Configurations}

Payload related configs, that can be leveraged to control merges based on specific business fields in the data.

`Config Class`: org.apache.hudi.config.HoodiePayloadConfig<br></br>
> #### hoodie.payload.event.time.field
> Table column/field name to derive timestamp associated with the records. This canbe useful for e.g, determining the freshness of the table.<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: EVENT_TIME_FIELD`<br></br>

---

> #### hoodie.payload.ordering.field
> Table column/field name to order records that have the same key, before merging and writing to storage.<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: ORDERING_FIELD`<br></br>

---

## Kafka Connect Configs {#KAFKA_CONNECT}
These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables

### Kafka Sink Connect Configurations {#Kafka-Sink-Connect-Configurations}

Configurations for Kafka Connect Sink Connector for Hudi.

`Config Class`: org.apache.hudi.connect.writers.KafkaConnectConfigs<br></br>
> #### hoodie.kafka.coordinator.write.timeout.secs
> The timeout after sending an END_COMMIT until when the coordinator will wait for the write statuses from all the partitionsto ignore the current commit and start a new commit.<br></br>
> **Default Value**: 300 (Optional)<br></br>
> `Config Param: COORDINATOR_WRITE_TIMEOUT_SECS`<br></br>

---

> #### hoodie.meta.sync.classes
> Meta sync client tool, using comma to separate multi tools<br></br>
> **Default Value**: org.apache.hudi.hive.HiveSyncTool (Optional)<br></br>
> `Config Param: META_SYNC_CLASSES`<br></br>

---

> #### hoodie.kafka.allow.commit.on.errors
> Commit even when some records failed to be written<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ALLOW_COMMIT_ON_ERRORS`<br></br>

---

> #### hadoop.home
> The Hadoop home directory.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HADOOP_HOME`<br></br>

---

> #### hoodie.meta.sync.enable
> Enable Meta Sync such as Hive<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: META_SYNC_ENABLE`<br></br>

---

> #### hoodie.kafka.commit.interval.secs
> The interval at which Hudi will commit the records written to the files, making them consumable on the read-side.<br></br>
> **Default Value**: 60 (Optional)<br></br>
> `Config Param: COMMIT_INTERVAL_SECS`<br></br>

---

> #### hoodie.kafka.control.topic
> Kafka topic name used by the Hudi Sink Connector for sending and receiving control messages. Not used for data records.<br></br>
> **Default Value**: hudi-control-topic (Optional)<br></br>
> `Config Param: CONTROL_TOPIC_NAME`<br></br>

---

> #### bootstrap.servers
> The bootstrap servers for the Kafka Cluster.<br></br>
> **Default Value**: localhost:9092 (Optional)<br></br>
> `Config Param: KAFKA_BOOTSTRAP_SERVERS`<br></br>

---

> #### hoodie.schemaprovider.class
> subclass of org.apache.hudi.schema.SchemaProvider to attach schemas to input & target table data, built in options: org.apache.hudi.schema.FilebasedSchemaProvider.<br></br>
> **Default Value**: org.apache.hudi.schema.FilebasedSchemaProvider (Optional)<br></br>
> `Config Param: SCHEMA_PROVIDER_CLASS`<br></br>

---

> #### hadoop.conf.dir
> The Hadoop configuration directory.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HADOOP_CONF_DIR`<br></br>

---

> #### hoodie.kafka.compaction.async.enable
> Controls whether async compaction should be turned on for MOR table writing.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ASYNC_COMPACT_ENABLE`<br></br>

---

## Amazon Web Services Configs {#AWS}
Please fill in the description for Config Group Name: Amazon Web Services Configs

### Amazon Web Services Configs {#Amazon-Web-Services-Configs}

Amazon Web Services configurations to access resources like Amazon DynamoDB (for locks), Amazon CloudWatch (metrics).

`Config Class`: org.apache.hudi.config.HoodieAWSConfig<br></br>
> #### hoodie.aws.session.token
> AWS session token<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: AWS_SESSION_TOKEN`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.aws.access.key
> AWS access key id<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: AWS_ACCESS_KEY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

> #### hoodie.aws.secret.key
> AWS secret key<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: AWS_SECRET_KEY`<br></br>
> `Since Version: 0.10.0`<br></br>

---

