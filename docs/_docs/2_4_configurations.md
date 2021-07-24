---
title: Configurations
keywords: garbage collection, hudi, jvm, configs, tuning
permalink: /docs/configurations.html
summary: This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.
toc: true
last_modified_at: 2021-07-24T00:48:18.710466
---

This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.
- [**Record Payload Config**](#RECORD_PAYLOAD): This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.

### Metrics Configurations for Datadog reporter {#Metrics-Configurations-for-Datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsDatadogConfig<br>
> #### hoodie.metrics.datadog.metric.tags
> Datadog metric tags (comma-delimited) to be sent along with metrics data.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_METRIC_TAGS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key.supplier
> Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_API_KEY_SUPPLIER`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.metric.prefix
> Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_METRIC_PREFIX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.timeout.seconds
> Datadog API timeout in seconds. Default to 3.<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: DATADOG_API_TIMEOUT_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.report.period.seconds
> Datadog reporting period in seconds. Default to 30.<br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: DATADOG_REPORT_PERIOD_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.metric.host
> Datadog metric host to be sent along with metrics data.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_METRIC_HOST`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key.skip.validation
> Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: DATADOG_API_KEY_SKIP_VALIDATION`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.site
> Datadog API site: EU or US<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_API_SITE`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.datadog.api.key
> Datadog API key<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: DATADOG_API_KEY`<br>
> `Since Version: 0.6.0`<br>

---

### Metrics Configurations for Prometheus {#Metrics-Configurations-for-Prometheus}

Enables reporting on Hudi metrics using Prometheus.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsPrometheusConfig<br>
> #### hoodie.metrics.pushgateway.host
> Hostname of the prometheus push gateway<br>
> **Default Value**: localhost (Optional)<br>
> `Config Param: PUSHGATEWAY_HOST`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.delete.on.shutdown
> <br>
> **Default Value**: true (Optional)<br>
> `Config Param: PUSHGATEWAY_DELETE_ON_SHUTDOWN`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.prometheus.port
> Port for prometheus server.<br>
> **Default Value**: 9090 (Optional)<br>
> `Config Param: PROMETHEUS_PORT`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.random.job.name.suffix
> <br>
> **Default Value**: true (Optional)<br>
> `Config Param: PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.report.period.seconds
> Reporting interval in seconds.<br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: PUSHGATEWAY_REPORT_PERIOD_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.job.name
> Name of the push gateway job.<br>
> **Default Value**:  (Optional)<br>
> `Config Param: PUSHGATEWAY_JOB_NAME`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.metrics.pushgateway.port
> Port for the push gateway.<br>
> **Default Value**: 9091 (Optional)<br>
> `Config Param: PUSHGATEWAY_PORT`<br>
> `Since Version: 0.6.0`<br>

---

### Metrics Configurations {#Metrics-Configurations}

Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.

`Config Class`: org.apache.hudi.config.HoodieMetricsConfig<br>
> #### hoodie.metrics.jmx.host
> Jmx host to connect to<br>
> **Default Value**: localhost (Optional)<br>
> `Config Param: JMX_HOST`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.executor.enable
> <br>
> **Default Value**: N/A (Required)<br>
> `Config Param: ENABLE_EXECUTOR_METRICS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metrics.jmx.port
> Jmx port to connect to<br>
> **Default Value**: 9889 (Optional)<br>
> `Config Param: JMX_PORT`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.graphite.host
> Graphite host to connect to<br>
> **Default Value**: localhost (Optional)<br>
> `Config Param: GRAPHITE_SERVER_HOST`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.on
> Turn on/off metrics reporting. off by default.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: METRICS_ON`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.graphite.metric.prefix
> Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: GRAPHITE_METRIC_PREFIX`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.metrics.graphite.port
> Graphite port to connect to<br>
> **Default Value**: 4756 (Optional)<br>
> `Config Param: GRAPHITE_SERVER_PORT`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.reporter.type
> Type of metrics reporter.<br>
> **Default Value**: GRAPHITE (Optional)<br>
> `Config Param: METRICS_REPORTER_TYPE`<br>
> `Since Version: 0.5.0`<br>

---

> #### hoodie.metrics.reporter.class
> <br>
> **Default Value**:  (Optional)<br>
> `Config Param: METRICS_REPORTER_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

## Record Payload Config {#RECORD_PAYLOAD}
This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

### Payload Configurations {#Payload-Configurations}

Payload related configs, that can be leveraged to control merges based on specific business fields in the data.

`Config Class`: org.apache.hudi.config.HoodiePayloadConfig<br>
> #### hoodie.payload.event.time.field
> Table column/field name to derive timestamp associated with the records. This canbe useful for e.g, determining the freshness of the table.<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: PAYLOAD_EVENT_TIME_FIELD_PROP`<br>

---

> #### hoodie.payload.ordering.field
> Table column/field name to order records that have the same key, before merging and writing to storage.<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: PAYLOAD_ORDERING_FIELD_PROP`<br>

---

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.

### Read Options {#Read-Options}

Options useful for reading tables via `read.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br>
> #### hoodie.file.index.enable
> Enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: ENABLE_HOODIE_FILE_INDEX`<br>

---

> #### hoodie.datasource.merge.type
> For Snapshot query on merge on read table, control whether we invoke the record payload implementation to merge (payload_combine) or skip merging altogetherskip_merge<br>
> **Default Value**: payload_combine (Optional)<br>
> `Config Param: REALTIME_MERGE_OPT_KEY`<br>

---

> #### hoodie.datasource.read.incr.path.glob
> For the use-cases like users only want to incremental pull from certain partitions instead of the full table. This option allows using glob pattern to directly filter on path.<br>
> **Default Value**:  (Optional)<br>
> `Config Param: INCR_PATH_GLOB_OPT_KEY`<br>

---

> #### hoodie.datasource.query.type
> Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode (obtain latest view, by merging base and (if any) log files)<br>
> **Default Value**: snapshot (Optional)<br>
> `Config Param: QUERY_TYPE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: READ_PRE_COMBINE_FIELD`<br>

---

> #### hoodie.datasource.read.end.instanttime
> Instant time to limit incrementally fetched data to. New data written with an instant_time <= END_INSTANTTIME are fetched out.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: END_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.begin.instanttime
> Instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BEGIN_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.schema.use.end.instanttime
> Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY`<br>

---

> #### hoodie.datasource.read.incr.filters
> For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies opaque map functions, filters appearing late in the sequence of transformations cannot be automatically pushed down. This option allows setting filters directly on Hoodie Source.<br>
> **Default Value**:  (Optional)<br>
> `Config Param: PUSH_DOWN_INCR_FILTERS_OPT_KEY`<br>

---

> #### hoodie.datasource.read.paths
> Comma separated list of file paths to read within a Hudi table.<br>
> **Default Value**: N/A (Required)<br>
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
> **Default Value**: default (Optional)<br>
> `Config Param: HIVE_DATABASE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: PRECOMBINE_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.username
> hive user name to use<br>
> **Default Value**: hive (Optional)<br>
> `Config Param: HIVE_USER_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.table_properties
> Additional properties to store with table.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HIVE_TABLE_PROPERTIES`<br>

---

> #### hoodie.datasource.hive_sync.batch_num
> The number of partitions one batch when synchronous partitions to hive.<br>
> **Default Value**: 1000 (Optional)<br>
> `Config Param: HIVE_BATCH_SYNC_PARTITION_NUM`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` extract a key out of incoming records.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: KEYGENERATOR_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.base_file_format
> Base file format for the sync.<br>
> **Default Value**: PARQUET (Optional)<br>
> `Config Param: HIVE_BASE_FILE_FORMAT_OPT_KEY`<br>

---

> #### hoodie.datasource.write.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br>
> **Default Value**: COPY_ON_WRITE (Optional)<br>
> `Config Param: TABLE_TYPE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.operation
> Whether to do upsert, insert or bulkinsert for the write operation. Use bulkinsert to load new data into a table, and there on use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br>
> **Default Value**: upsert (Optional)<br>
> `Config Param: OPERATION_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.retry.interval.ms
>  Config to indicate how long (by millisecond) before a retry should issued for failed microbatch<br>
> **Default Value**: 2000 (Optional)<br>
> `Config Param: STREAMING_RETRY_INTERVAL_MS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_STYLE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.serde_properties
> <br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HIVE_TABLE_SERDE_PROPERTIES`<br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br>
> `Config Param: PAYLOAD_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.assume_date_partitioning
> Assume partitioning is yyyy/mm/dd<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_ASSUME_DATE_PARTITION_OPT_KEY`<br>

---

> #### hoodie.meta.sync.client.tool.class
> Sync tool class name used to sync to metastore. Defaults to Hive.<br>
> **Default Value**: org.apache.hudi.hive.HiveSyncTool (Optional)<br>
> `Config Param: META_SYNC_CLIENT_TOOL_CLASS`<br>

---

> #### hoodie.datasource.hive_sync.password
> hive password to use<br>
> **Default Value**: hive (Optional)<br>
> `Config Param: HIVE_PASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.create_managed_table
> Whether to sync the table as managed table.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_CREATE_MANAGED_TABLE`<br>

---

> #### hoodie.datasource.hive_sync.partition_fields
> field in the table to use for determining hive partition columns.<br>
> **Default Value**:  (Optional)<br>
> `Config Param: HIVE_PARTITION_FIELDS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br>
> **Default Value**: uuid (Optional)<br>
> `Config Param: RECORDKEY_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.write.commitmeta.key.prefix
> Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. This is useful to store checkpointing information, in a consistent way with the hudi timeline<br>
> **Default Value**: _ (Optional)<br>
> `Config Param: COMMIT_METADATA_KEYPREFIX_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.table
> table to sync to<br>
> **Default Value**: unknown (Optional)<br>
> `Config Param: HIVE_TABLE_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.auto_create_database
> Auto create hive database if does not exists<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HIVE_AUTO_CREATE_DATABASE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.table.name
> Table name for the datasource write. Also used to register the table into meta stores.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: TABLE_NAME_OPT_KEY`<br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: URL_ENCODE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.support_timestamp
> ‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. Disabled by default for backward compatibility.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SUPPORT_TIMESTAMP`<br>

---

> #### hoodie.datasource.hive_sync.jdbcurl
> Hive metastore url<br>
> **Default Value**: jdbc:hive2://localhost:10000 (Optional)<br>
> `Config Param: HIVE_URL_OPT_KEY`<br>

---

> #### hoodie.datasource.compaction.async.enable
> Controls whether async compaction should be turned on for MOR table writing.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: ASYNC_COMPACT_ENABLE_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.retry.count
> Config to indicate how many times streaming job should retry for a failed micro batch.<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: STREAMING_RETRY_CNT_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.ignore_exceptions
> <br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_IGNORE_EXCEPTIONS_OPT_KEY`<br>

---

> #### hoodie.datasource.write.insert.drop.duplicates
> If set to true, filters out all duplicate records from incoming dataframe, during insert operations.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INSERT_DROP_DUPS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.partition_extractor_class
> <br>
> **Default Value**: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor (Optional)<br>
> `Config Param: HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.use_pre_apache_input_format
> <br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY`<br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br>
> **Default Value**: partitionpath (Optional)<br>
> `Config Param: PARTITIONPATH_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.sync_as_datasource
> <br>
> **Default Value**: true (Optional)<br>
> `Config Param: HIVE_SYNC_AS_DATA_SOURCE_TABLE`<br>

---

> #### hoodie.deltastreamer.source.kafka.value.deserializer.class
> This class is used by kafka client to deserialize the records<br>
> **Default Value**: io.confluent.kafka.serializers.KafkaAvroDeserializer (Optional)<br>
> `Config Param: KAFKA_AVRO_VALUE_DESERIALIZER_CLASS`<br>
> `Since Version: 0.9.0`<br>

---

> #### hoodie.datasource.hive_sync.skip_ro_suffix
> Skip the _ro suffix for Read optimized table, when registering<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SKIP_RO_SUFFIX`<br>

---

> #### hoodie.datasource.meta.sync.enable
> <br>
> **Default Value**: false (Optional)<br>
> `Config Param: META_SYNC_ENABLED_OPT_KEY`<br>

---

> #### hoodie.datasource.write.row.writer.enable
> When set to true, will perform write operations directly using the spark native `Row` representation, avoiding any additional conversion costs.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: ENABLE_ROW_WRITER_OPT_KEY`<br>

---

> #### hoodie.datasource.write.streaming.ignore.failed.batch
> Config to indicate whether to ignore any non exception error (e.g. writestatus error) within a streaming microbatch<br>
> **Default Value**: true (Optional)<br>
> `Config Param: STREAMING_IGNORE_FAILED_BATCH_OPT_KEY`<br>

---

> #### hoodie.datasource.clustering.async.enable
> Enable asynchronous clustering. Disabled by default.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: ASYNC_CLUSTERING_ENABLE_OPT_KEY`<br>
> `Since Version: 0.9.0`<br>

---

> #### hoodie.datasource.clustering.inline.enable
> Enable inline clustering. Disabled by default.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INLINE_CLUSTERING_ENABLE_OPT_KEY`<br>
> `Since Version: 0.9.0`<br>

---

> #### hoodie.datasource.hive_sync.enable
> When set to true, register/sync the table to Apache Hive metastore<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_ENABLED_OPT_KEY`<br>

---

> #### hoodie.datasource.hive_sync.use_jdbc
> Use JDBC when hive synchronization is enabled<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HIVE_USE_JDBC_OPT_KEY`<br>

---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.

### Flink Options {#Flink-Options}

Flink jobs using the SQL can be configured through the options in WITH clause. The actual datasource level configs are listed below.

`Config Class`: org.apache.hudi.configuration.FlinkOptions<br>
> #### read.streaming.enabled
> Whether to read as streaming source, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: READ_AS_STREAMING`<br>

---

> #### hoodie.datasource.write.keygenerator.type
> Key generator type, that implements will extract the key out of incoming record<br>
> **Default Value**: SIMPLE (Optional)<br>
> `Config Param: KEYGEN_TYPE`<br>

---

> #### compaction.trigger.strategy
> Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits;
'time_elapsed': trigger compaction when time elapsed &gt; N seconds since last compaction;
'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied;
'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.
Default is 'num_commits'<br>
> **Default Value**: num_commits (Optional)<br>
> `Config Param: COMPACTION_TRIGGER_STRATEGY`<br>

---

> #### index.state.ttl
> Index state ttl in days, default 1.5 day<br>
> **Default Value**: 1.5 (Optional)<br>
> `Config Param: INDEX_STATE_TTL`<br>

---

> #### compaction.max_memory
> Max memory in MB for compaction spillable map, default 100MB<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: COMPACTION_MAX_MEMORY`<br>

---

> #### hive_sync.support_timestamp
> INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type.
Disabled by default for backward compatibility.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_SUPPORT_TIMESTAMP`<br>

---

> #### hive_sync.skip_ro_suffix
> Skip the _ro suffix for Read optimized table when registering, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_SKIP_RO_SUFFIX`<br>

---

> #### hive_sync.assume_date_partitioning
> Assume partitioning is yyyy/mm/dd, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_ASSUME_DATE_PARTITION`<br>

---

> #### hive_sync.table
> Table name for hive sync, default 'unknown'<br>
> **Default Value**: unknown (Optional)<br>
> `Config Param: HIVE_SYNC_TABLE`<br>

---

> #### compaction.tasks
> Parallelism of tasks that do actual compaction, default is 10<br>
> **Default Value**: 10 (Optional)<br>
> `Config Param: COMPACTION_TASKS`<br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Whether to use Hive style partitioning.
If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format.
By default false (the names of partition folders are only partition values)<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_STYLE_PARTITIONING`<br>

---

> #### table.type
> Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ<br>
> **Default Value**: COPY_ON_WRITE (Optional)<br>
> `Config Param: TABLE_TYPE`<br>

---

> #### hive_sync.partition_extractor_class
> Tool to extract the partition value from HDFS path, default 'SlashEncodedDayPartitionValueExtractor'<br>
> **Default Value**: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor (Optional)<br>
> `Config Param: HIVE_SYNC_PARTITION_EXTRACTOR_CLASS`<br>

---

> #### hive_sync.auto_create_db
> Auto create hive database if it does not exists, default true<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HIVE_SYNC_AUTO_CREATE_DB`<br>

---

> #### hive_sync.username
> Username for hive sync, default 'hive'<br>
> **Default Value**: hive (Optional)<br>
> `Config Param: HIVE_SYNC_USERNAME`<br>

---

> #### hive_sync.enable
> Asynchronously sync Hive meta to HMS, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_ENABLED`<br>

---

> #### read.streaming.check-interval
> Check interval for streaming read of SECOND, default 1 minute<br>
> **Default Value**: 60 (Optional)<br>
> `Config Param: READ_STREAMING_CHECK_INTERVAL`<br>

---

> #### hoodie.datasource.merge.type
> For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in
1) skip_merge: read the base file records plus the log file records;
2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the
   log file records(combines the two records with same key for base and log file records), then read the left log file records<br>
> **Default Value**: payload_combine (Optional)<br>
> `Config Param: MERGE_TYPE`<br>

---

> #### write.retry.times
> Flag to indicate how many times streaming job should retry for a failed checkpoint batch.
By default 3<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: RETRY_TIMES`<br>

---

> #### read.tasks
> Parallelism of tasks that do actual read, default is 4<br>
> **Default Value**: 4 (Optional)<br>
> `Config Param: READ_TASKS`<br>

---

> #### write.log.max.size
> Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB<br>
> **Default Value**: 1024 (Optional)<br>
> `Config Param: WRITE_LOG_MAX_SIZE`<br>

---

> #### hive_sync.file_format
> File format for hive sync, default 'PARQUET'<br>
> **Default Value**: PARQUET (Optional)<br>
> `Config Param: HIVE_SYNC_FILE_FORMAT`<br>

---

> #### write.retry.interval.ms
> Flag to indicate how long (by millisecond) before a retry should issued for failed checkpoint batch.
By default 2000 and it will be doubled by every retry<br>
> **Default Value**: 2000 (Optional)<br>
> `Config Param: RETRY_INTERVAL_MS`<br>

---

> #### hive_sync.db
> Database name for hive sync, default 'default'<br>
> **Default Value**: default (Optional)<br>
> `Config Param: HIVE_SYNC_DB`<br>

---

> #### hive_sync.password
> Password for hive sync, default 'hive'<br>
> **Default Value**: hive (Optional)<br>
> `Config Param: HIVE_SYNC_PASSWORD`<br>

---

> #### hive_sync.use_jdbc
> Use JDBC when hive synchronization is enabled, default true<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HIVE_SYNC_USE_JDBC`<br>

---

> #### compaction.schedule.enabled
> Schedule the compaction plan, enabled by default for MOR<br>
> **Default Value**: true (Optional)<br>
> `Config Param: COMPACTION_SCHEDULE_ENABLED`<br>

---

> #### hive_sync.jdbc_url
> Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'<br>
> **Default Value**: jdbc:hive2://localhost:10000 (Optional)<br>
> `Config Param: HIVE_SYNC_JDBC_URL`<br>

---

> #### write.batch.size
> Batch buffer size in MB to flush data into the underneath filesystem, default 64MB<br>
> **Default Value**: 64.0 (Optional)<br>
> `Config Param: WRITE_BATCH_SIZE`<br>

---

> #### archive.min_commits
> Min number of commits to keep before archiving older commits into a sequential log, default 20<br>
> **Default Value**: 20 (Optional)<br>
> `Config Param: ARCHIVE_MIN_COMMITS`<br>

---

> #### index.global.enabled
> Whether to update index for the old partition path
if same key record with different partition path came in, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INDEX_GLOBAL_ENABLED`<br>

---

> #### write.insert.drop.duplicates
> Flag to indicate whether to drop duplicates upon insert.
By default insert will accept duplicates, to gain extra performance<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INSERT_DROP_DUPS`<br>

---

> #### index.partition.regex
> Whether to load partitions in state if partition path matching， default *<br>
> **Default Value**: .* (Optional)<br>
> `Config Param: INDEX_PARTITION_REGEX`<br>

---

> #### read.streaming.start-commit
> Start commit instant for streaming read, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: READ_STREAMING_START_COMMIT`<br>

---

> #### hoodie.table.name
> Table name to register to Hive metastore<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: TABLE_NAME`<br>

---

> #### path
> Base path for the target hoodie table.
The path would be created if it does not exist,
otherwise a Hoodie table expects to be initialized successfully<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: PATH`<br>

---

> #### index.bootstrap.enabled
> Whether to bootstrap the index state from existing hoodie table, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INDEX_BOOTSTRAP_ENABLED`<br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Whether to encode the partition path url, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: URL_ENCODE_PARTITIONING`<br>

---

> #### compaction.async.enabled
> Async Compaction, enabled by default for MOR<br>
> **Default Value**: true (Optional)<br>
> `Config Param: COMPACTION_ASYNC_ENABLED`<br>

---

> #### hive_sync.ignore_exceptions
> Ignore exceptions during hive synchronization, default false<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_SYNC_IGNORE_EXCEPTIONS`<br>

---

> #### write.ignore.failed
> Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch.
By default true (in favor of streaming progressing over data integrity)<br>
> **Default Value**: true (Optional)<br>
> `Config Param: IGNORE_FAILED`<br>

---

> #### write.commit.ack.timeout
> Timeout limit for a writer task after it finishes a checkpoint and
waits for the instant commit success, only for internal use<br>
> **Default Value**: -1 (Optional)<br>
> `Config Param: WRITE_COMMIT_ACK_TIMEOUT`<br>

---

> #### write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
This will render any value set for the option in-effective<br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br>
> `Config Param: PAYLOAD_CLASS`<br>

---

> #### write.operation
> The write operation, that this write should do<br>
> **Default Value**: upsert (Optional)<br>
> `Config Param: OPERATION`<br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
Actual value obtained by invoking .toString(), default ''<br>
> **Default Value**:  (Optional)<br>
> `Config Param: PARTITION_PATH_FIELD`<br>

---

> #### write.bucket_assign.tasks
> Parallelism of tasks that do bucket assign, default is 4<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BUCKET_ASSIGN_TASKS`<br>

---

> #### source.avro-schema.path
> Source avro schema file path, the parsed schema is used for deserialization<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: SOURCE_AVRO_SCHEMA_PATH`<br>

---

> #### compaction.delta_commits
> Max delta commits needed to trigger compaction, default 5 commits<br>
> **Default Value**: 5 (Optional)<br>
> `Config Param: COMPACTION_DELTA_COMMITS`<br>

---

> #### partition.default_name
> The default partition name in case the dynamic partition column value is null/empty string<br>
> **Default Value**: __DEFAULT_PARTITION__ (Optional)<br>
> `Config Param: PARTITION_DEFAULT_NAME`<br>

---

> #### source.avro-schema
> Source avro schema string, the parsed schema is used for deserialization<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: SOURCE_AVRO_SCHEMA`<br>

---

> #### compaction.target_io
> Target IO per compaction (both read and write), default 5 GB<br>
> **Default Value**: 5120 (Optional)<br>
> `Config Param: COMPACTION_TARGET_IO`<br>

---

> #### write.log_block.size
> Max log block size in MB for log file, default 128MB<br>
> **Default Value**: 128 (Optional)<br>
> `Config Param: WRITE_LOG_BLOCK_SIZE`<br>

---

> #### write.tasks
> Parallelism of tasks that do actual write, default is 4<br>
> **Default Value**: 4 (Optional)<br>
> `Config Param: WRITE_TASKS`<br>

---

> #### clean.async.enabled
> Whether to cleanup the old commits immediately on new commits, enabled by default<br>
> **Default Value**: true (Optional)<br>
> `Config Param: CLEAN_ASYNC_ENABLED`<br>

---

> #### clean.retain_commits
> Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled).
This also directly translates into how much you can incrementally pull on this table, default 10<br>
> **Default Value**: 10 (Optional)<br>
> `Config Param: CLEAN_RETAIN_COMMITS`<br>

---

> #### read.utc-timezone
> Use UTC timezone or local timezone to the conversion between epoch time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone, by default true<br>
> **Default Value**: true (Optional)<br>
> `Config Param: UTC_TIMEZONE`<br>

---

> #### archive.max_commits
> Max number of commits to keep before archiving older commits into a sequential log, default 30<br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: ARCHIVE_MAX_COMMITS`<br>

---

> #### hoodie.datasource.query.type
> Decides how data files need to be read, in
1) Snapshot mode (obtain latest view, based on row &amp; columnar data);
2) incremental mode (new data since an instantTime);
3) Read Optimized mode (obtain latest view, based on columnar data)
.Default: snapshot<br>
> **Default Value**: snapshot (Optional)<br>
> `Config Param: QUERY_TYPE`<br>

---

> #### write.precombine.field
> Field used in preCombining before actual write. When two records have the same
key value, we will pick the one with the largest value for the precombine field,
determined by Object.compareTo(..)<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: PRECOMBINE_FIELD`<br>

---

> #### write.index_bootstrap.tasks
> Parallelism of tasks that do index bootstrap, default is 4<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: INDEX_BOOTSTRAP_TASKS`<br>

---

> #### write.task.max.size
> Maximum memory in MB for a write task, when the threshold hits,
it flushes the max size data bucket to avoid OOM, default 1GB<br>
> **Default Value**: 1024.0 (Optional)<br>
> `Config Param: WRITE_TASK_MAX_SIZE`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements will extract the key out of incoming record<br>
> **Default Value**:  (Optional)<br>
> `Config Param: KEYGEN_CLASS`<br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br>
> **Default Value**: uuid (Optional)<br>
> `Config Param: RECORD_KEY_FIELD`<br>

---

> #### compaction.delta_seconds
> Max delta seconds time needed to trigger compaction, default 1 hour<br>
> **Default Value**: 3600 (Optional)<br>
> `Config Param: COMPACTION_DELTA_SECONDS`<br>

---

> #### hive_sync.metastore.uris
> Metastore uris for hive sync, default ''<br>
> **Default Value**:  (Optional)<br>
> `Config Param: HIVE_SYNC_METASTORE_URIS`<br>

---

> #### hive_sync.partition_fields
> Partition fields for hive sync, default ''<br>
> **Default Value**:  (Optional)<br>
> `Config Param: HIVE_SYNC_PARTITION_FIELDS`<br>

---

> #### write.merge.max_memory
> Max memory in MB for merge, default 100MB<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: WRITE_MERGE_MAX_MEMORY`<br>

---

## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.

### Consistency Guard Configurations {#Consistency-Guard-Configurations}

The consistency guard related config options, to help talk to eventually consistent object storage.(Tip: S3 is NOT eventually consistent anymore!)

`Config Class`: org.apache.hudi.common.fs.ConsistencyGuardConfig<br>
> #### hoodie.optimistic.consistency.guard.sleep_time_ms
> Amount of time (in ms), to wait after which we assume storage is consistent.<br>
> **Default Value**: 500 (Optional)<br>
> `Config Param: OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.consistency.check.max_interval_ms
> Maximum amount of time (in ms), to wait for consistency checking.<br>
> **Default Value**: 20000 (Optional)<br>
> `Config Param: MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>
> `Since Version: 0.5.0`<br>
> `Deprecated Version: 0.7.0`<br>

---

> #### hoodie.consistency.check.enabled
> Enabled to handle S3 eventual consistency issue. This property is no longer required since S3 is now strongly consistent. Will be removed in the future releases.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: CONSISTENCY_CHECK_ENABLED_PROP`<br>
> `Since Version: 0.5.0`<br>
> `Deprecated Version: 0.7.0`<br>

---

> #### hoodie.consistency.check.initial_interval_ms
> Amount of time (in ms) to wait, before checking for consistency after an operation on storage.<br>
> **Default Value**: 400 (Optional)<br>
> `Config Param: INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>
> `Since Version: 0.5.0`<br>
> `Deprecated Version: 0.7.0`<br>

---

> #### hoodie.consistency.check.max_checks
> Maximum number of consistency checks to perform, with exponential backoff.<br>
> **Default Value**: 6 (Optional)<br>
> `Config Param: MAX_CONSISTENCY_CHECKS_PROP`<br>
> `Since Version: 0.5.0`<br>
> `Deprecated Version: 0.7.0`<br>

---

> #### _hoodie.optimistic.consistency.guard.enable
> Enable consistency guard, which optimistically assumes consistency is achieved after a certain time period.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: ENABLE_OPTIMISTIC_CONSISTENCY_GUARD_PROP`<br>
> `Since Version: 0.6.0`<br>

---

### Write Configurations {#Write-Configurations}

Configurations that control write behavior on Hudi tables. These can be directly passed down from even higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g DeltaStreamer).

`Config Class`: org.apache.hudi.config.HoodieWriteConfig<br>
> #### hoodie.embed.timeline.server.reuse.enabled
> Controls whether the timeline server instance should be cached and reused across the JVM (across task lifecycles)to avoid startup costs. This should rarely be changed.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED`<br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` extract a key out of incoming records.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: KEYGENERATOR_CLASS_PROP`<br>

---

> #### hoodie.bulkinsert.shuffle.parallelism
> For large initial imports using bulk_insert operation, controls the parallelism to use for sort modes or custom partitioning donebefore writing records to the table.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: BULKINSERT_PARALLELISM`<br>

---

> #### hoodie.rollback.parallelism
> Parallelism for rollback of commits. Rollbacks perform delete of files or logging delete blocks to file groups on storage in parallel.<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: ROLLBACK_PARALLELISM`<br>

---

> #### hoodie.write.schema
> The specified write schema. In most case, we do not need set this parameter, but for the case the write schema is not equal to the specified table schema, we can specify the write schema by this parameter. Used by MergeIntoHoodieTableCommand<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: WRITE_SCHEMA_PROP`<br>

---

> #### hoodie.timeline.layout.version
> Controls the layout of the timeline. Version 0 relied on renames, Version 1 (default) models the timeline as an immutable log relying only on atomic writes for object storage.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: TIMELINE_LAYOUT_VERSION`<br>
> `Since Version: 0.5.1`<br>

---

> #### hoodie.avro.schema.validate
> Validate the schema used for the write against the latest schema, for backwards compatibility.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: AVRO_SCHEMA_VALIDATE`<br>

---

> #### hoodie.bulkinsert.sort.mode
> Sorting modes to use for sorting records for bulk insert. This is user when user hoodie.bulkinsert.user.defined.partitioner.classis not configured. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads<br>
> **Default Value**: GLOBAL_SORT (Optional)<br>
> `Config Param: BULKINSERT_SORT_MODE`<br>

---

> #### hoodie.spillable.diskmap.type
> When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed.  By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.<br>
> **Default Value**: BITCASK (Optional)<br>
> `Config Param: SPILLABLE_DISK_MAP_TYPE`<br>

---

> #### hoodie.upsert.shuffle.parallelism
> Parallelism to use for upsert operation on the table. Upserts can shuffle data to perform index lookups, file sizing, bin packing records optimallyinto file groups.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: UPSERT_PARALLELISM`<br>

---

> #### hoodie.embed.timeline.server.gzip
> Controls whether gzip compression is used, for large responses from the timeline server, to improve latency.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT`<br>

---

> #### hoodie.fail.on.timeline.archiving
> Timeline archiving removes older instants from the timeline, after each write operation, to minimize metadata overhead. Controls whether or not, the write should be failed as well, if such archiving fails.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP`<br>

---

> #### hoodie.write.status.storage.level
> Write status objects hold metadata about a write (stats, errors), that is not yet committed to storage. This controls the how that information is cached for inspection by clients. We rarely expect this to be changed.<br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br>
> `Config Param: WRITE_STATUS_STORAGE_LEVEL`<br>

---

> #### hoodie.table.base.file.format
> <br>
> **Default Value**: PARQUET (Optional)<br>
> `Config Param: BASE_FILE_FORMAT`<br>

---

> #### _.hoodie.allow.multi.write.on.same.instant
> <br>
> **Default Value**: false (Optional)<br>
> `Config Param: ALLOW_MULTI_WRITE_ON_SAME_INSTANT`<br>

---

> #### hoodie.embed.timeline.server.port
> Port at which the timeline server listens for requests. When running embedded in each writer, it picks a free port and communicates to all the executors. This should rarely be changed.<br>
> **Default Value**: 0 (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_PORT`<br>

---

> #### hoodie.consistency.check.max_interval_ms
> Max time to wait between successive attempts at performing consistency checks<br>
> **Default Value**: 300000 (Optional)<br>
> `Config Param: MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>

---

> #### hoodie.write.concurrency.mode
> Enable different concurrency modes. Options are SINGLE_WRITER: Only one active writer to the table. Maximizes throughputOPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table and exactly one of them succeed if a conflict (writes affect the same file group) is detected.<br>
> **Default Value**: SINGLE_WRITER (Optional)<br>
> `Config Param: WRITE_CONCURRENCY_MODE_PROP`<br>

---

> #### hoodie.delete.shuffle.parallelism
> Parallelism used for “delete” operation. Delete operations also performs shuffles, similar to upsert operation.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: DELETE_PARALLELISM`<br>

---

> #### hoodie.embed.timeline.server
> When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),running on each writer's driver process, accepting requests during the write from executors.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_ENABLED`<br>

---

> #### hoodie.base.path
> Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BASE_PATH_PROP`<br>

---

> #### hoodie.avro.schema
> Schema string representing the current write schema of the table. Hudi passes this to implementations of HoodieRecordPayload to convert incoming records to avro. This is also used as the write schema evolving records during an update.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: AVRO_SCHEMA`<br>

---

> #### hoodie.table.name
> Table name that will be used for registering with metastores like HMS. Needs to be same across runs.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: TABLE_NAME`<br>

---

> #### hoodie.embed.timeline.server.async
> Controls whether or not, the requests to the timeline server are processed in asynchronous fashion, potentially improving throughput.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_USE_ASYNC`<br>

---

> #### hoodie.combine.before.upsert
> When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming,  otherwise it can lead to duplicate keys and violate the uniqueness guarantees.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: COMBINE_BEFORE_UPSERT_PROP`<br>

---

> #### hoodie.embed.timeline.server.threads
> Number of threads to serve requests in the timeline server. By default, auto configured based on the number of underlying cores.<br>
> **Default Value**: -1 (Optional)<br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_THREADS`<br>

---

> #### hoodie.diskmap.bitcask.compression.enabled
> Turn on compression for BITCASK disk map used by the External Spillable Map<br>
> **Default Value**: true (Optional)<br>
> `Config Param: DISK_MAP_BITCASK_COMPRESSION_ENABLED`<br>

---

> #### hoodie.consistency.check.max_checks
> Maximum number of checks, for consistency of written data.<br>
> **Default Value**: 7 (Optional)<br>
> `Config Param: MAX_CONSISTENCY_CHECKS_PROP`<br>

---

> #### hoodie.combine.before.delete
> During delete operations, controls whether we should combine deletes (and potentially also upserts) before  writing to storage.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: COMBINE_BEFORE_DELETE_PROP`<br>

---

> #### hoodie.datasource.write.keygenerator.type
> Easily configure one the built-in key generators, instead of specifying the key generator class.Currently supports SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br>
> **Default Value**: SIMPLE (Optional)<br>
> `Config Param: KEYGENERATOR_TYPE_PROP`<br>

---

> #### hoodie.write.buffer.limit.bytes
> Size of in-memory buffer used for parallelizing network reads and lake storage writes.<br>
> **Default Value**: 4194304 (Optional)<br>
> `Config Param: WRITE_BUFFER_LIMIT_BYTES`<br>

---

> #### hoodie.bulkinsert.user.defined.partitioner.class
> If specified, this class will be used to re-partition records before they are bulk inserted. This can be used to sort, pack, cluster data optimally for common query patterns.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BULKINSERT_USER_DEFINED_PARTITIONER_CLASS`<br>

---

> #### hoodie.client.heartbeat.interval_in_ms
> Writers perform heartbeats to indicate liveness. Controls how often (in ms), such heartbeats are registered to lake storage.<br>
> **Default Value**: 60000 (Optional)<br>
> `Config Param: CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP`<br>

---

> #### hoodie.avro.schema.external.transformation
> When enabled, records in older schema are rewritten into newer schema during upsert,delete and background compaction,clustering operations.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION`<br>

---

> #### hoodie.client.heartbeat.tolerable.misses
> Number of heartbeat misses, before a writer is deemed not alive and all pending writes are aborted.<br>
> **Default Value**: 2 (Optional)<br>
> `Config Param: CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP`<br>

---

> #### hoodie.auto.commit
> Controls whether a write operation should auto commit. This can be turned off to perform inspection of the uncommitted write before deciding to commit.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HOODIE_AUTO_COMMIT_PROP`<br>

---

> #### hoodie.combine.before.insert
> When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: COMBINE_BEFORE_INSERT_PROP`<br>

---

> #### hoodie.writestatus.class
> Subclass of org.apache.hudi.client.WriteStatus to be used to collect information about a write. Can be overridden to collection additional metrics/statistics about the data if needed.<br>
> **Default Value**: org.apache.hudi.client.WriteStatus (Optional)<br>
> `Config Param: HOODIE_WRITE_STATUS_CLASS_PROP`<br>

---

> #### hoodie.markers.delete.parallelism
> Determines the parallelism for deleting marker files, which are used to track all files (valid or invalid/partial) written during a write operation. Increase this value if delays are observed, with large batch writes.<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: MARKERS_DELETE_PARALLELISM`<br>

---

> #### hoodie.consistency.check.initial_interval_ms
> Initial time between successive attempts to ensure written data's metadata is consistent on storage. Grows with exponential backoff after the initial value.<br>
> **Default Value**: 2000 (Optional)<br>
> `Config Param: INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP`<br>

---

> #### hoodie.merge.data.validation.enabled
> When enabled, data validation checks are performed during merges to ensure expected number of records after merge operation.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: MERGE_DATA_VALIDATION_CHECK_ENABLED`<br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br>
> `Config Param: WRITE_PAYLOAD_CLASS`<br>

---

> #### hoodie.insert.shuffle.parallelism
> Parallelism for inserting records into the table. Inserts can shuffle data before writing to tune file sizes and optimize the storage layout.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: INSERT_PARALLELISM`<br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br>
> **Default Value**: ts (Optional)<br>
> `Config Param: PRECOMBINE_FIELD_PROP`<br>

---

> #### hoodie.write.meta.key.prefixes
> Comma separated metadata key prefixes to override from latest commit during overlapping commits via multi writing<br>
> **Default Value**:  (Optional)<br>
> `Config Param: WRITE_META_KEY_PREFIXES_PROP`<br>

---

> #### hoodie.finalize.write.parallelism
> Parallelism for the write finalization internal operation, which involves removing any partially written files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables or low latency writes.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: FINALIZE_WRITE_PARALLELISM`<br>

---

> #### hoodie.merge.allow.duplicate.on.inserts
> When enabled, we allow duplicate keys even if inserts are routed to merge with an existing file (for ensuring file sizing). This is only relevant for insert operation, since upsert, delete operations will ensure unique key constraints are maintained.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: MERGE_ALLOW_DUPLICATE_ON_INSERTS`<br>

---

> #### hoodie.rollback.using.markers
> Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned off by default.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: ROLLBACK_USING_MARKERS`<br>

---

### Key Generator Options {#Key-Generator-Options}

Hudi maintains keys (record key + partition path) for uniquely identifying a particular record. This config allows developers to setup the Key generator class that will extract these out of incoming records.

`Config Class`: org.apache.hudi.keygen.constant.KeyGeneratorOptions<br>
> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: URL_ENCODE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br>
> **Default Value**: uuid (Optional)<br>
> `Config Param: RECORDKEY_FIELD_OPT_KEY`<br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HIVE_STYLE_PARTITIONING_OPT_KEY`<br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br>
> **Default Value**: partitionpath (Optional)<br>
> `Config Param: PARTITIONPATH_FIELD_OPT_KEY`<br>

---

### HBase Index Configs {#HBase-Index-Configs}

Configurations that control indexing behavior (when HBase based indexing is enabled), which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieHBaseIndexConfig<br>
> #### hoodie.index.hbase.qps.fraction
> Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively. Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.<br>
> **Default Value**: 0.5 (Optional)<br>
> `Config Param: HBASE_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.zknode.path
> Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_ZK_ZNODEPARENT`<br>

---

> #### hoodie.index.hbase.zkpath.qps_root
> chroot in zookeeper, to use for all qps allocation co-ordination.<br>
> **Default Value**: /QPS_ROOT (Optional)<br>
> `Config Param: HBASE_ZK_PATH_QPS_ROOT`<br>

---

> #### hoodie.index.hbase.put.batch.size
> Controls the batch size for performing puts against HBase. Batching improves throughput, by saving round trips.<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: HBASE_PUT_BATCH_SIZE_PROP`<br>

---

> #### hoodie.index.hbase.dynamic_qps
> Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on write volume.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY`<br>

---

> #### hoodie.index.hbase.max.qps.per.region.server
> Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
 limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this
 value based on global indexing throughput needs and most importantly, how much the HBase installation in use is
 able to tolerate without Region Servers going down.<br>
> **Default Value**: 1000 (Optional)<br>
> `Config Param: HBASE_MAX_QPS_PER_REGION_SERVER_PROP`<br>

---

> #### hoodie.index.hbase.zk.session_timeout_ms
> Session timeout value to use for Zookeeper failure detection, for the HBase client.Lower this value, if you want to fail faster.<br>
> **Default Value**: 60000 (Optional)<br>
> `Config Param: HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS`<br>

---

> #### hoodie.index.hbase.sleep.ms.for.get.batch
> <br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_SLEEP_MS_GET_BATCH_PROP`<br>

---

> #### hoodie.index.hbase.min.qps.fraction
> Minimum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_MIN_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.desired_puts_time_in_secs
> <br>
> **Default Value**: 600 (Optional)<br>
> `Config Param: HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS`<br>

---

> #### hoodie.hbase.index.update.partition.path
> Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old paritition and will insert it as new record in new partition.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HBASE_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.index.hbase.qps.allocator.class
> Property to set which implementation of HBase QPS resource allocator to be used, whichcontrols the batching rate dynamically.<br>
> **Default Value**: org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator (Optional)<br>
> `Config Param: HBASE_INDEX_QPS_ALLOCATOR_CLASS`<br>

---

> #### hoodie.index.hbase.sleep.ms.for.put.batch
> <br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_SLEEP_MS_PUT_BATCH_PROP`<br>

---

> #### hoodie.index.hbase.rollback.sync
> When set to true, the rollback method will delete the last failed task index. The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HBASE_INDEX_ROLLBACK_SYNC`<br>

---

> #### hoodie.index.hbase.zkquorum
> Only applies if index type is HBASE. HBase ZK Quorum url to connect to<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_ZKQUORUM_PROP`<br>

---

> #### hoodie.index.hbase.get.batch.size
> Controls the batch size for performing gets against HBase. Batching improves throughput, by saving round trips.<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: HBASE_GET_BATCH_SIZE_PROP`<br>

---

> #### hoodie.index.hbase.zk.connection_timeout_ms
> Timeout to use for establishing connection with zookeeper, from HBase client.<br>
> **Default Value**: 15000 (Optional)<br>
> `Config Param: HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS`<br>

---

> #### hoodie.index.hbase.put.batch.size.autocompute
> Property to set to enable auto computation of put batch size<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP`<br>

---

> #### hoodie.index.hbase.zkport
> Only applies if index type is HBASE. HBase ZK Quorum port to connect to<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_ZKPORT_PROP`<br>

---

> #### hoodie.index.hbase.max.qps.fraction
> Maximum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_MAX_QPS_FRACTION_PROP`<br>

---

> #### hoodie.index.hbase.table
> Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HBASE_TABLENAME_PROP`<br>

---

### Write commit callback configs {#Write-commit-callback-configs}

Controls callback behavior into HTTP endpoints, to push  notifications on commits on hudi tables.

`Config Class`: org.apache.hudi.config.HoodieWriteCommitCallbackConfig<br>
> #### hoodie.write.commit.callback.http.api.key
> Http callback API key. hudi_write_commit_http_callback by default<br>
> **Default Value**: hudi_write_commit_http_callback (Optional)<br>
> `Config Param: CALLBACK_HTTP_API_KEY`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.on
> Turn commit callback on/off. off by default.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: CALLBACK_ON`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.http.url
> Callback host to be sent along with callback messages<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: CALLBACK_HTTP_URL_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.http.timeout.seconds
> Callback timeout in seconds. 3 by default<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: CALLBACK_HTTP_TIMEOUT_SECONDS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.write.commit.callback.class
> Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default<br>
> **Default Value**: org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback (Optional)<br>
> `Config Param: CALLBACK_CLASS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

### Write commit Kafka callback configs {#Write-commit-Kafka-callback-configs}

Controls notifications sent to Kafka, on events happening to a hudi table.

`Config Class`: org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig<br>
> #### hoodie.write.commit.callback.kafka.acks
> kafka acks level, all by default to ensure strong durability.<br>
> **Default Value**: all (Optional)<br>
> `Config Param: CALLBACK_KAFKA_ACKS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.bootstrap.servers
> Bootstrap servers of kafka cluster, to be used for publishing commit metadata.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: CALLBACK_KAFKA_BOOTSTRAP_SERVERS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.topic
> Kafka topic name to publish timeline activity into.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: CALLBACK_KAFKA_TOPIC`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.retries
> Times to retry the produce. 3 by default<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: CALLBACK_KAFKA_RETRIES`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.write.commit.callback.kafka.partition
> It may be desirable to serialize all changes into a single Kafka partition  for providing strict ordering. By default, Kafka messages are keyed by table name, which  guarantees ordering at the table level, but not globally (or when new partitions are added)<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: CALLBACK_KAFKA_PARTITION`<br>
> `Since Version: 0.7.0`<br>

---

### Locks Configurations {#Locks-Configurations}

Configs that control locking mechanisms required for concurrency control  between writers to a Hudi table. Concurrency between Hudi's own table services  are auto managed internally.

`Config Class`: org.apache.hudi.config.HoodieLockConfig<br>
> #### hoodie.write.lock.zookeeper.url
> Zookeeper URL to connect to.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: ZK_CONNECT_URL_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.max_wait_time_ms_between_retry
> Maximum amount of time to wait between retries by lock provider client. This bounds the maximum delay from the exponential backoff. Currently used by ZK based lock provider only.<br>
> **Default Value**: 5000 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.filesystem.path
> For DFS based lock providers, path to store the locks under.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: FILESYSTEM_LOCK_PATH_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.uris
> For Hive based lock provider, the Hive metastore URI to acquire locks against.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HIVE_METASTORE_URI_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.wait_time_ms_between_retry
> Initial amount of time to wait between retries to acquire locks,  subsequent retries will exponentially backoff.<br>
> **Default Value**: 5000 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.wait_time_ms
> Timeout in ms, to wait on an individual lock acquire() call, at the lock provider.<br>
> **Default Value**: 60000 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.conflict.resolution.strategy
> Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy<br>
> **Default Value**: org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy (Optional)<br>
> `Config Param: WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.num_retries
> Maximum number of times to retry lock acquire, at each lock provider<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_NUM_RETRIES_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.connection_timeout_ms
> Timeout in ms, to wait for establishing connection with Zookeeper.<br>
> **Default Value**: 15000 (Optional)<br>
> `Config Param: ZK_CONNECTION_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.client.num_retries
> Maximum number of times to retry to acquire lock additionally from the lock manager.<br>
> **Default Value**: 0 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.database
> For Hive based lock provider, the Hive database to acquire lock against<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HIVE_DATABASE_NAME_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.port
> Zookeeper port to connect to.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: ZK_PORT_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.client.wait_time_ms_between_retry
> Amount of time to wait between retries on the lock provider by the lock manager<br>
> **Default Value**: 10000 (Optional)<br>
> `Config Param: LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.lock_key
> Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. We recommend setting this to the table name<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: ZK_LOCK_KEY_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.base_path
> The base path on Zookeeper under which to create lock related ZNodes. This should be same for all concurrent writers to the same table<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: ZK_BASE_PATH_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.zookeeper.session_timeout_ms
> Timeout in ms, to wait after losing connection to ZooKeeper, before the session is expired<br>
> **Default Value**: 60000 (Optional)<br>
> `Config Param: ZK_SESSION_TIMEOUT_MS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.hivemetastore.table
> For Hive based lock provider, the Hive table to acquire lock against<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HIVE_TABLE_NAME_PROP`<br>
> `Since Version: 0.8.0`<br>

---

> #### hoodie.write.lock.provider
> Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider<br>
> **Default Value**: org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider (Optional)<br>
> `Config Param: LOCK_PROVIDER_CLASS_PROP`<br>
> `Since Version: 0.8.0`<br>

---

### Compaction Configs {#Compaction-Configs}

Configurations that control compaction (merging of log files onto a new base files) as well as  cleaning (reclamation of older/unused file groups/slices).

`Config Class`: org.apache.hudi.config.HoodieCompactionConfig<br>
> #### hoodie.compact.inline.trigger.strategy
> Controls how compaction scheduling is triggered, by time or num delta commits or combination of both. Valid options: NUM_COMMITS,TIME_ELAPSED,NUM_AND_TIME,NUM_OR_TIME<br>
> **Default Value**: NUM_COMMITS (Optional)<br>
> `Config Param: INLINE_COMPACT_TRIGGER_STRATEGY_PROP`<br>

---

> #### hoodie.cleaner.fileversions.retained
> When KEEP_LATEST_FILE_VERSIONS cleaning policy is used,  the minimum number of file slices to retain in each file group, during cleaning.<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: CLEANER_FILE_VERSIONS_RETAINED_PROP`<br>

---

> #### hoodie.cleaner.policy.failed.writes
> Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)<br>
> **Default Value**: EAGER (Optional)<br>
> `Config Param: FAILED_WRITES_CLEANER_POLICY_PROP`<br>

---

> #### hoodie.parquet.small.file.limit
> During upsert operation, we opportunistically expand existing small files on storage, instead of writing new files, to keep number of files to an optimum. This config sets the file size limit below which a file on storage  becomes a candidate to be selected as such a `small file`. By default, treat any file <= 100MB as a small file.<br>
> **Default Value**: 104857600 (Optional)<br>
> `Config Param: PARQUET_SMALL_FILE_LIMIT_BYTES`<br>

---

> #### hoodie.keep.max.commits
> Archiving service moves older entries from timeline into an archived log after each write, to  keep the metadata overhead constant, even as the table size grows.This config controls the maximum number of instants to retain in the active timeline. <br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: MAX_COMMITS_TO_KEEP_PROP`<br>

---

> #### hoodie.compaction.lazy.block.read
> When merging the delta log files, this config helps to choose whether the log blocks should be read lazily or not. Choose true to use lazy block reading (low memory usage, but incurs seeks to each block header) or false for immediate block read (higher memory usage)<br>
> **Default Value**: false (Optional)<br>
> `Config Param: COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP`<br>

---

> #### hoodie.clean.automatic
> When enabled, the cleaner table service is invoked immediately after each commit, to delete older file slices. It's recommended to enable this, to ensure metadata and data storage growth is bounded.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: AUTO_CLEAN_PROP`<br>

---

> #### hoodie.commits.archival.batch
> Archiving of instants is batched in best-effort manner, to pack more instants into a single archive log. This config controls such archival batch size.<br>
> **Default Value**: 10 (Optional)<br>
> `Config Param: COMMITS_ARCHIVAL_BATCH_SIZE_PROP`<br>

---

> #### hoodie.cleaner.parallelism
> Parallelism for the cleaning operation. Increase this if cleaning becomes slow.<br>
> **Default Value**: 200 (Optional)<br>
> `Config Param: CLEANER_PARALLELISM`<br>

---

> #### hoodie.cleaner.incremental.mode
> When enabled, the plans for each cleaner service run is computed incrementally off the events  in the timeline, since the last cleaner run. This is much more efficient than obtaining listings for the full table for each planning (even with a metadata table).<br>
> **Default Value**: true (Optional)<br>
> `Config Param: CLEANER_INCREMENTAL_MODE`<br>

---

> #### hoodie.cleaner.delete.bootstrap.base.file
> When set to true, cleaner also deletes the bootstrap base file when it's skeleton base file is  cleaned. Turn this to true, if you want to ensure the bootstrap dataset storage is reclaimed over time, as the table receives updates/deletes. Another reason to turn this on, would be to ensure data residing in bootstrap  base files are also physically deleted, to comply with data privacy enforcement processes.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: CLEANER_BOOTSTRAP_BASE_FILE_ENABLED`<br>

---

> #### hoodie.copyonwrite.insert.split.size
> Number of inserts assigned for each partition/bucket for writing. We based the default on writing out 100MB files, with at least 1kb records (100K records per file), and   over provision to 500K. As long as auto-tuning of splits is turned on, this only affects the first   write, where there is no history to learn record sizes from.<br>
> **Default Value**: 500000 (Optional)<br>
> `Config Param: COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE`<br>

---

> #### hoodie.compaction.strategy
> Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data<br>
> **Default Value**: org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy (Optional)<br>
> `Config Param: COMPACTION_STRATEGY_PROP`<br>

---

> #### hoodie.compaction.target.io
> Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.<br>
> **Default Value**: 512000 (Optional)<br>
> `Config Param: TARGET_IO_PER_COMPACTION_IN_MB_PROP`<br>

---

> #### hoodie.compaction.payload.class
> This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.<br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br>
> `Config Param: PAYLOAD_CLASS_PROP`<br>

---

> #### hoodie.cleaner.commits.retained
> Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br>
> **Default Value**: 10 (Optional)<br>
> `Config Param: CLEANER_COMMITS_RETAINED_PROP`<br>

---

> #### hoodie.record.size.estimation.threshold
> We use the previous commits' metadata to calculate the estimated record size and use it  to bin pack records into partitions. If the previous commit is too small to make an accurate estimation,  Hudi will search commits in the reverse order, until we find a commit that has totalBytesWritten  larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * this_threshold)<br>
> **Default Value**: 1.0 (Optional)<br>
> `Config Param: RECORD_SIZE_ESTIMATION_THRESHOLD_PROP`<br>

---

> #### hoodie.compaction.daybased.target.partitions
> Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.<br>
> **Default Value**: 10 (Optional)<br>
> `Config Param: TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP`<br>

---

> #### hoodie.keep.min.commits
> Similar to hoodie.keep.max.commits, but controls the minimum number ofinstants to retain in the active timeline.<br>
> **Default Value**: 20 (Optional)<br>
> `Config Param: MIN_COMMITS_TO_KEEP_PROP`<br>

---

> #### hoodie.compaction.reverse.log.read
> HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the reader reads the logfile in reverse direction, from pos=file_length to pos=0<br>
> **Default Value**: false (Optional)<br>
> `Config Param: COMPACTION_REVERSE_LOG_READ_ENABLED_PROP`<br>

---

> #### hoodie.compact.inline
> When set to true, compaction service is triggered after each write. While being  simpler operationally, this adds extra latency on the write path.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INLINE_COMPACT_PROP`<br>

---

> #### hoodie.clean.async
> Only applies when hoodie.clean.automatic is turned on. When turned on runs cleaner async with writing, which can speed up overall write performance.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: ASYNC_CLEAN_PROP`<br>

---

> #### hoodie.copyonwrite.insert.auto.split
> Config to control whether we control insert split sizes automatically based on average record sizes. It's recommended to keep this turned on, since hand tuning is otherwise extremely cumbersome.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS`<br>

---

> #### hoodie.copyonwrite.record.size.estimate
> The average record size. If not explicitly specified, hudi will compute the record size estimate compute dynamically based on commit metadata.  This is critical in computing the insert parallelism and bin-packing inserts into small files.<br>
> **Default Value**: 1024 (Optional)<br>
> `Config Param: COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE`<br>

---

> #### hoodie.compact.inline.max.delta.commits
> Number of delta commits after the last compaction, before scheduling of a new compaction is attempted.<br>
> **Default Value**: 5 (Optional)<br>
> `Config Param: INLINE_COMPACT_NUM_DELTA_COMMITS_PROP`<br>

---

> #### hoodie.compact.inline.max.delta.seconds
> Number of elapsed seconds after the last compaction, before scheduling a new one.<br>
> **Default Value**: 3600 (Optional)<br>
> `Config Param: INLINE_COMPACT_TIME_DELTA_SECONDS_PROP`<br>

---

> #### hoodie.cleaner.policy
> Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space. By default, cleaner spares the file slices written by the last N commits, determined by  hoodie.cleaner.commits.retained Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had   a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time<br>
> **Default Value**: KEEP_LATEST_COMMITS (Optional)<br>
> `Config Param: CLEANER_POLICY_PROP`<br>

---

### File System View Storage Configurations {#File-System-View-Storage-Configurations}

Configurations that control how file metadata is stored by Hudi, for transaction processing and queries.

`Config Class`: org.apache.hudi.common.table.view.FileSystemViewStorageConfig<br>
> #### hoodie.filesystem.view.spillable.replaced.mem.fraction
> Fraction of the file system view memory, to be used for holding replace commit related metadata.<br>
> **Default Value**: 0.01 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_REPLACED_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.incr.timeline.sync.enable
> Controls whether or not, the file system view is incrementally updated as new actions are performed on the timeline.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE`<br>

---

> #### hoodie.filesystem.view.secondary.type
> Specifies the secondary form of storage for file system view, if the primary (e.g timeline server)  is unavailable.<br>
> **Default Value**: MEMORY (Optional)<br>
> `Config Param: FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE`<br>

---

> #### hoodie.filesystem.view.spillable.compaction.mem.fraction
> Fraction of the file system view memory, to be used for holding compaction related metadata.<br>
> **Default Value**: 0.8 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.spillable.mem
> Amount of memory to be used for holding file system view, before spilling to disk.<br>
> **Default Value**: 104857600 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_SPILLABLE_MEM`<br>

---

> #### hoodie.filesystem.view.spillable.clustering.mem.fraction
> Fraction of the file system view memory, to be used for holding clustering related metadata.<br>
> **Default Value**: 0.01 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_PENDING_CLUSTERING_MEM_FRACTION`<br>

---

> #### hoodie.filesystem.view.spillable.dir
> Path on local storage to use, when file system view is held in a spillable map.<br>
> **Default Value**: /tmp/view_map/ (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_SPILLABLE_DIR`<br>

---

> #### hoodie.filesystem.view.remote.timeout.secs
> Timeout in seconds, to wait for API requests against a remote file system view. e.g timeline server.<br>
> **Default Value**: 300 (Optional)<br>
> `Config Param: FILESYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS`<br>

---

> #### hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction
> Fraction of the file system view memory, to be used for holding mapping to bootstrap base files.<br>
> **Default Value**: 0.05 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION`<br>

---

> #### hoodie.filesystem.view.remote.port
> Port to serve file system view queries, when remote. We expect this to be rarely hand configured.<br>
> **Default Value**: 26754 (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_REMOTE_PORT`<br>

---

> #### hoodie.filesystem.view.type
> File system view provides APIs for viewing the files on the underlying lake storage,  as file groups and file slices. This config controls how such a view is held. Options include MEMORY,SPILLABLE_DISK,EMBEDDED_KV_STORE,REMOTE_ONLY,REMOTE_FIRST which provide different trade offs for memory usage and API request performance.<br>
> **Default Value**: MEMORY (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_STORAGE_TYPE`<br>

---

> #### hoodie.filesystem.view.remote.host
> We expect this to be rarely hand configured.<br>
> **Default Value**: localhost (Optional)<br>
> `Config Param: FILESYSTEM_VIEW_REMOTE_HOST`<br>

---

> #### hoodie.filesystem.view.rocksdb.base.path
> Path on local storage to use, when storing file system view in embedded kv store/rocksdb.<br>
> **Default Value**: /tmp/hoodie_timeline_rocksdb (Optional)<br>
> `Config Param: ROCKSDB_BASE_PATH_PROP`<br>

---

> #### hoodie.filesystem.remote.backup.view.enable
> Config to control whether backup needs to be configured if clients were not able to reach timeline service.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: REMOTE_BACKUP_VIEW_HANDLER_ENABLE`<br>

---

### Table Configurations {#Table-Configurations}

Configurations that persist across writes and read on a Hudi table  like  base, log file formats, table name, creation schema, table version layouts.  Configurations are loaded from hoodie.properties, these properties are usually set during initializing a path as hoodie base path and rarely changes during the lifetime of the table. Writers/Queries' configurations are validated against these  each time for compatibility.

`Config Class`: org.apache.hudi.common.table.HoodieTableConfig<br>
> #### hoodie.table.recordkey.fields
> Columns used to uniquely identify the table. Concatenated values of these fields are used as  the record key component of HoodieKey.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TABLE_RECORDKEY_FIELDS`<br>

---

> #### hoodie.archivelog.folder
> path under the meta folder, to store archived timeline instants at.<br>
> **Default Value**: archived (Optional)<br>
> `Config Param: HOODIE_ARCHIVELOG_FOLDER_PROP`<br>

---

> #### hoodie.table.create.schema
> Schema used when creating the table, for the first time.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TABLE_CREATE_SCHEMA`<br>

---

> #### hoodie.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br>
> **Default Value**: COPY_ON_WRITE (Optional)<br>
> `Config Param: HOODIE_TABLE_TYPE_PROP`<br>

---

> #### hoodie.table.base.file.format
> Base file format to store all the base file data.<br>
> **Default Value**: PARQUET (Optional)<br>
> `Config Param: HOODIE_BASE_FILE_FORMAT_PROP`<br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_BOOTSTRAP_BASE_PATH_PROP`<br>

---

> #### hoodie.table.name
> Table name that will be used for registering with Hive. Needs to be same across runs.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TABLE_NAME_PROP`<br>

---

> #### hoodie.table.version
> Version of table, used for running upgrade/downgrade steps between releases with potentially breaking/backwards compatible changes.<br>
> **Default Value**: ZERO (Optional)<br>
> `Config Param: HOODIE_TABLE_VERSION_PROP`<br>

---

> #### hoodie.table.precombine.field
> Field used in preCombining before actual write. By default, when two records have the same key value, the largest value for the precombine field determined by Object.compareTo(..), is picked.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TABLE_PRECOMBINE_FIELD_PROP`<br>

---

> #### hoodie.bootstrap.index.class
> Implementation to use, for mapping base files to bootstrap base file, that contain actual data.<br>
> **Default Value**: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex (Optional)<br>
> `Config Param: HOODIE_BOOTSTRAP_INDEX_CLASS_PROP`<br>

---

> #### hoodie.table.log.file.format
> Log format used for the delta logs.<br>
> **Default Value**: HOODIE_LOG (Optional)<br>
> `Config Param: HOODIE_LOG_FILE_FORMAT_PROP`<br>

---

> #### hoodie.bootstrap.index.enable
> Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_BOOTSTRAP_INDEX_ENABLE_PROP`<br>

---

> #### hoodie.timeline.layout.version
> Version of timeline used, by the table.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TIMELINE_LAYOUT_VERSION_PROP`<br>

---

> #### hoodie.table.partition.fields
> Fields used to partition the table. Concatenated values of these fields are used as the partition path, by invoking toString()<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: HOODIE_TABLE_PARTITION_FIELDS_PROP`<br>

---

> #### hoodie.compaction.payload.class
> Payload class to use for performing compactions, i.e merge delta logs with current base file and then  produce a new base file.<br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br>
> `Config Param: HOODIE_PAYLOAD_CLASS_PROP`<br>

---

> #### hoodie.populate.meta.fields
> When enabled, populates all meta fields. When disabled, no meta fields are populated and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing<br>
> **Default Value**: true (Optional)<br>
> `Config Param: HOODIE_POPULATE_META_FIELDS`<br>

---

### Memory Configurations {#Memory-Configurations}

Controls memory usage for compaction and merges, performed internally by Hudi.

`Config Class`: org.apache.hudi.config.HoodieMemoryConfig<br>
> #### hoodie.memory.merge.fraction
> This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge<br>
> **Default Value**: 0.6 (Optional)<br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_MERGE_PROP`<br>

---

> #### hoodie.memory.compaction.max.size
> Maximum amount of memory used for compaction operations, before spilling to local storage.<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: MAX_MEMORY_FOR_COMPACTION_PROP`<br>

---

> #### hoodie.memory.spillable.map.path
> Default file path prefix for spillable map<br>
> **Default Value**: /tmp/ (Optional)<br>
> `Config Param: SPILLABLE_MAP_BASE_PATH_PROP`<br>

---

> #### hoodie.memory.compaction.fraction
> HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map<br>
> **Default Value**: 0.6 (Optional)<br>
> `Config Param: MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP`<br>

---

> #### hoodie.memory.writestatus.failure.fraction
> Property to control how what fraction of the failed record, exceptions we report back to driver. Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and mask actual data errors.<br>
> **Default Value**: 0.1 (Optional)<br>
> `Config Param: WRITESTATUS_FAILURE_FRACTION_PROP`<br>

---

> #### hoodie.memory.merge.max.size
> Maximum amount of memory used for merge operations, before spilling to local storage.<br>
> **Default Value**: 1073741824 (Optional)<br>
> `Config Param: MAX_MEMORY_FOR_MERGE_PROP`<br>

---

> #### hoodie.memory.dfs.buffer.max.size
> Property to control the max memory for dfs input stream buffer size<br>
> **Default Value**: 16777216 (Optional)<br>
> `Config Param: MAX_DFS_STREAM_BUFFER_SIZE_PROP`<br>

---

### Index Configs {#Index-Configs}

Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieIndexConfig<br>
> #### hoodie.index.type
> Type of index to use. Default is Bloom filter. Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: INDEX_TYPE_PROP`<br>

---

> #### hoodie.bloom.index.use.treebased.filter
> Only applies if index type is BLOOM. When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode<br>
> **Default Value**: true (Optional)<br>
> `Config Param: BLOOM_INDEX_TREE_BASED_FILTER_PROP`<br>

---

> #### hoodie.index.bloom.num_entries
> Only applies if index type is BLOOM. This is the number of entries to be stored in the bloom filter. The rationale for the default: Assume the maxParquetFileSize is 128MB and averageRecordSize is 1kb and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and setting this to a very high number will increase the size every base file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.<br>
> **Default Value**: 60000 (Optional)<br>
> `Config Param: BLOOM_FILTER_NUM_ENTRIES`<br>

---

> #### hoodie.bloom.index.bucketized.checking
> Only applies if index type is BLOOM. When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup<br>
> **Default Value**: true (Optional)<br>
> `Config Param: BLOOM_INDEX_BUCKETIZED_CHECKING_PROP`<br>

---

> #### hoodie.bloom.index.update.partition.path
> Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition<br>
> **Default Value**: false (Optional)<br>
> `Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.bloom.index.parallelism
> Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a shuffle. By default, this is auto computed based on input workload characteristics.<br>
> **Default Value**: 0 (Optional)<br>
> `Config Param: BLOOM_INDEX_PARALLELISM_PROP`<br>

---

> #### hoodie.bloom.index.input.storage.level
> Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br>
> `Config Param: BLOOM_INDEX_INPUT_STORAGE_LEVEL`<br>

---

> #### hoodie.bloom.index.keys.per.bucket
> Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. This configuration controls the “bucket” size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory.<br>
> **Default Value**: 10000000 (Optional)<br>
> `Config Param: BLOOM_INDEX_KEYS_PER_BUCKET_PROP`<br>

---

> #### hoodie.simple.index.update.partition.path
> Similar to Key: 'hoodie.bloom.index.update.partition.path' , default: false description: Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition since version: version is not defined deprecated after: version is not defined), but for simple index.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: SIMPLE_INDEX_UPDATE_PARTITION_PATH`<br>

---

> #### hoodie.simple.index.input.storage.level
> Only applies when #simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values<br>
> **Default Value**: MEMORY_AND_DISK_SER (Optional)<br>
> `Config Param: SIMPLE_INDEX_INPUT_STORAGE_LEVEL`<br>

---

> #### hoodie.index.bloom.fpp
> Only applies if index type is BLOOM. Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the configured value (hoodie.index.bloom.num_entries), then this fpp may not be honored.<br>
> **Default Value**: 0.000000001 (Optional)<br>
> `Config Param: BLOOM_FILTER_FPP`<br>

---

> #### hoodie.bloom.index.filter.dynamic.max.entries
> The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.<br>
> **Default Value**: 100000 (Optional)<br>
> `Config Param: HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES`<br>

---

> #### hoodie.index.class
> Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified<br>
> **Default Value**:  (Optional)<br>
> `Config Param: INDEX_CLASS_PROP`<br>

---

> #### hoodie.bloom.index.filter.type
> Filter type used. Default is BloomFilterTypeCode.SIMPLE. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.<br>
> **Default Value**: SIMPLE (Optional)<br>
> `Config Param: BLOOM_INDEX_FILTER_TYPE`<br>

---

> #### hoodie.global.simple.index.parallelism
> Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br>
> **Default Value**: 100 (Optional)<br>
> `Config Param: GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP`<br>

---

> #### hoodie.bloom.index.use.caching
> Only applies if index type is BLOOM.When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br>
> **Default Value**: true (Optional)<br>
> `Config Param: BLOOM_INDEX_USE_CACHING_PROP`<br>

---

> #### hoodie.bloom.index.prune.by.ranges
> Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off, since range pruning will only  add extra overhead to the index lookup.<br>
> **Default Value**: true (Optional)<br>
> `Config Param: BLOOM_INDEX_PRUNE_BY_RANGES_PROP`<br>

---

> #### hoodie.simple.index.use.caching
> Only applies if index type is SIMPLE. When true, the incoming writes will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br>
> **Default Value**: true (Optional)<br>
> `Config Param: SIMPLE_INDEX_USE_CACHING_PROP`<br>

---

> #### hoodie.simple.index.parallelism
> Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br>
> **Default Value**: 50 (Optional)<br>
> `Config Param: SIMPLE_INDEX_PARALLELISM_PROP`<br>

---

### Storage Configs {#Storage-Configs}

Configurations that control aspects around writing, sizing, reading base and log files.

`Config Class`: org.apache.hudi.config.HoodieStorageConfig<br>
> #### hoodie.parquet.compression.ratio
> Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files<br>
> **Default Value**: 0.1 (Optional)<br>
> `Config Param: PARQUET_COMPRESSION_RATIO`<br>

---

> #### hoodie.hfile.max.file.size
> Target file size for HFile base files.<br>
> **Default Value**: 125829120 (Optional)<br>
> `Config Param: HFILE_FILE_MAX_BYTES`<br>

---

> #### hoodie.logfile.data.block.max.size
> LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.<br>
> **Default Value**: 268435456 (Optional)<br>
> `Config Param: LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES`<br>

---

> #### hoodie.parquet.block.size
> Parquet RowGroup size. It's recommended to make this large enough that scan costs can be amortized by packing enough column values into a single row group.<br>
> **Default Value**: 125829120 (Optional)<br>
> `Config Param: PARQUET_BLOCK_SIZE_BYTES`<br>

---

> #### hoodie.orc.stripe.size
> Size of the memory buffer in bytes for writing<br>
> **Default Value**: 67108864 (Optional)<br>
> `Config Param: ORC_STRIPE_SIZE`<br>

---

> #### hoodie.orc.block.size
> ORC block size, recommended to be aligned with the target file size.<br>
> **Default Value**: 125829120 (Optional)<br>
> `Config Param: ORC_BLOCK_SIZE`<br>

---

> #### hoodie.orc.max.file.size
> Target file size for ORC base files.<br>
> **Default Value**: 125829120 (Optional)<br>
> `Config Param: ORC_FILE_MAX_BYTES`<br>

---

> #### hoodie.hfile.compression.algorithm
> Compression codec to use for hfile base files.<br>
> **Default Value**: GZ (Optional)<br>
> `Config Param: HFILE_COMPRESSION_ALGORITHM`<br>

---

> #### hoodie.parquet.max.file.size
> Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br>
> **Default Value**: 125829120 (Optional)<br>
> `Config Param: PARQUET_FILE_MAX_BYTES`<br>

---

> #### hoodie.orc.compression.codec
> Compression codec to use for ORC base files.<br>
> **Default Value**: ZLIB (Optional)<br>
> `Config Param: ORC_COMPRESSION_CODEC`<br>

---

> #### hoodie.logfile.max.size
> LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version.<br>
> **Default Value**: 1073741824 (Optional)<br>
> `Config Param: LOGFILE_SIZE_MAX_BYTES`<br>

---

> #### hoodie.parquet.compression.codec
> Compression Codec for parquet files<br>
> **Default Value**: gzip (Optional)<br>
> `Config Param: PARQUET_COMPRESSION_CODEC`<br>

---

> #### hoodie.logfile.to.parquet.compression.ratio
> Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.<br>
> **Default Value**: 0.35 (Optional)<br>
> `Config Param: LOGFILE_TO_PARQUET_COMPRESSION_RATIO`<br>

---

> #### hoodie.parquet.page.size
> Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed separately.<br>
> **Default Value**: 1048576 (Optional)<br>
> `Config Param: PARQUET_PAGE_SIZE_BYTES`<br>

---

> #### hoodie.hfile.block.size
> Lower values increase the size of metadata tracked within HFile, but can offer potentially faster lookup times.<br>
> **Default Value**: 1048576 (Optional)<br>
> `Config Param: HFILE_BLOCK_SIZE_BYTES`<br>

---

### Clustering Configs {#Clustering-Configs}

Configurations that control the clustering table service in hudi, which optimizes the storage layout for better query performance by sorting and sizing data files.

`Config Class`: org.apache.hudi.config.HoodieClusteringConfig<br>
> #### hoodie.clustering.updates.strategy
> Determines how to handle updates, deletes to file groups that are under clustering. Default strategy just rejects the update<br>
> **Default Value**: org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy (Optional)<br>
> `Config Param: CLUSTERING_UPDATES_STRATEGY_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.async.max.commits
> Config to control frequency of async clustering<br>
> **Default Value**: 4 (Optional)<br>
> `Config Param: ASYNC_CLUSTERING_MAX_COMMIT_PROP`<br>
> `Since Version: 0.9.0`<br>

---

> #### hoodie.clustering.plan.strategy.target.file.max.bytes
> Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br>
> **Default Value**: 1073741824 (Optional)<br>
> `Config Param: CLUSTERING_TARGET_FILE_MAX_BYTES`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.daybased.lookback.partitions
> Number of partitions to list to create ClusteringPlan<br>
> **Default Value**: 2 (Optional)<br>
> `Config Param: CLUSTERING_TARGET_PARTITIONS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.max.bytes.per.group
> Each clustering operation can create multiple output file groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). Max amount of data to be included in one group<br>
> **Default Value**: 2147483648 (Optional)<br>
> `Config Param: CLUSTERING_MAX_BYTES_PER_GROUP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.execution.strategy.class
> Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the  clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while  meeting the configured target file sizes.<br>
> **Default Value**: org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy (Optional)<br>
> `Config Param: CLUSTERING_EXECUTION_STRATEGY_CLASS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.max.num.groups
> Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism<br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: CLUSTERING_MAX_NUM_GROUPS`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.inline
> Turn on inline clustering - clustering will be run after each write operation is complete<br>
> **Default Value**: false (Optional)<br>
> `Config Param: INLINE_CLUSTERING_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.sort.columns
> Columns to sort the data by when clustering<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: CLUSTERING_SORT_COLUMNS_PROPERTY`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.inline.max.commits
> Config to control frequency of clustering planning<br>
> **Default Value**: 4 (Optional)<br>
> `Config Param: INLINE_CLUSTERING_MAX_COMMIT_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.small.file.limit
> Files smaller than the size specified here are candidates for clustering<br>
> **Default Value**: 629145600 (Optional)<br>
> `Config Param: CLUSTERING_PLAN_SMALL_FILE_LIMIT`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.async.enabled
> Enable running of clustering service, asynchronously as inserts happen on the table.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: ASYNC_CLUSTERING_ENABLE_OPT_KEY`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.clustering.plan.strategy.class
> Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan i.e select what file groups are being clustered. Default strategy, looks at the last N (determined by hoodie.clustering.plan.strategy.daybased.lookback.partitions) day based partitions picks the small file slices within those partitions.<br>
> **Default Value**: org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy (Optional)<br>
> `Config Param: CLUSTERING_PLAN_STRATEGY_CLASS`<br>
> `Since Version: 0.7.0`<br>

---

### Metadata Configs {#Metadata-Configs}

Configurations used by the Hudi Metadata Table. This table maintains the metadata about a given Hudi table (e.g file listings)  to avoid overhead of accessing cloud storage, during queries.

`Config Class`: org.apache.hudi.common.config.HoodieMetadataConfig<br>
> #### hoodie.metadata.enable
> Enable the internal metadata table which serves table metadata like level file listings<br>
> **Default Value**: false (Optional)<br>
> `Config Param: METADATA_ENABLE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.insert.parallelism
> Parallelism to use when inserting to the metadata table<br>
> **Default Value**: 1 (Optional)<br>
> `Config Param: METADATA_INSERT_PARALLELISM_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.compact.max.delta.commits
> Controls how often the metadata table is compacted.<br>
> **Default Value**: 24 (Optional)<br>
> `Config Param: METADATA_COMPACT_NUM_DELTA_COMMITS_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.cleaner.commits.retained
> Controls retention/history for metadata table.<br>
> **Default Value**: 3 (Optional)<br>
> `Config Param: CLEANER_COMMITS_RETAINED_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.keep.min.commits
> Controls the archival of the metadata table’s timeline.<br>
> **Default Value**: 20 (Optional)<br>
> `Config Param: MIN_COMMITS_TO_KEEP_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.metrics.enable
> Enable publishing of metrics around metadata table.<br>
> **Default Value**: false (Optional)<br>
> `Config Param: METADATA_METRICS_ENABLE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.assume.date.partitioning
> Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually<br>
> **Default Value**: false (Optional)<br>
> `Config Param: HOODIE_ASSUME_DATE_PARTITIONING_PROP`<br>
> `Since Version: 0.3.0`<br>

---

> #### hoodie.metadata.keep.max.commits
> Controls the archival of the metadata table’s timeline.<br>
> **Default Value**: 30 (Optional)<br>
> `Config Param: MAX_COMMITS_TO_KEEP_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.dir.filter.regex
> Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.<br>
> **Default Value**:  (Optional)<br>
> `Config Param: DIRECTORY_FILTER_REGEX`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.validate
> Validate contents of metadata table on each access; e.g against the actual listings from lake storage<br>
> **Default Value**: false (Optional)<br>
> `Config Param: METADATA_VALIDATE_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.metadata.clean.async
> Enable asynchronous cleaning for metadata table<br>
> **Default Value**: false (Optional)<br>
> `Config Param: METADATA_ASYNC_CLEAN_PROP`<br>
> `Since Version: 0.7.0`<br>

---

> #### hoodie.file.listing.parallelism
> Parallelism to use, when listing the table on lake storage.<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: FILE_LISTING_PARALLELISM_PROP`<br>
> `Since Version: 0.7.0`<br>

---

### Bootstrap Configs {#Bootstrap-Configs}

Configurations that control how you want to bootstrap your existing tables for the first time into hudi. The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing  writers and new hudi writers in parallel, to validate the migration.

`Config Class`: org.apache.hudi.config.HoodieBootstrapConfig<br>
> #### hoodie.bootstrap.partitionpath.translator.class
> Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.<br>
> **Default Value**: org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator (Optional)<br>
> `Config Param: BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.keygen.class
> Key generator implementation to be used for generating keys from the bootstrapped dataset<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BOOTSTRAP_KEYGEN_CLASS`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.mode.selector
> Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped<br>
> **Default Value**: org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector (Optional)<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.keygen.type
> Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br>
> **Default Value**: SIMPLE (Optional)<br>
> `Config Param: BOOTSTRAP_KEYGEN_TYPE`<br>
> `Since Version: 0.9.0`<br>

---

> #### hoodie.bootstrap.mode.selector.regex.mode
> Bootstrap mode to apply for partition paths, that match regex above. METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.<br>
> **Default Value**: METADATA_ONLY (Optional)<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX_MODE`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.index.class
> Implementation to use, for mapping a skeleton base file to a boostrap base file.<br>
> **Default Value**: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex (Optional)<br>
> `Config Param: BOOTSTRAP_INDEX_CLASS_PROP`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.full.input.provider
> Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD<br>
> **Default Value**: org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider (Optional)<br>
> `Config Param: FULL_BOOTSTRAP_INPUT_PROVIDER`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.parallelism
> Parallelism value to be used to bootstrap data into hudi<br>
> **Default Value**: 1500 (Optional)<br>
> `Config Param: BOOTSTRAP_PARALLELISM`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.mode.selector.regex
> Matches each bootstrap dataset partition against this regex and applies the mode below to it.<br>
> **Default Value**: .* (Optional)<br>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX`<br>
> `Since Version: 0.6.0`<br>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br>
> **Default Value**: N/A (Required)<br>
> `Config Param: BOOTSTRAP_BASE_PATH_PROP`<br>
> `Since Version: 0.6.0`<br>

---

