---
title: Basic Configurations
summary: This page covers the basic configurations you may use to write/read Hudi tables. This page only features a subset of the most frequently used configurations. For a full list of all configs, please visit the [All Configurations](/docs/configurations) page.
last_modified_at: 2024-06-06T12:05:06.444
---


This page covers the basic configurations you may use to write/read Hudi tables. This page only features a subset of the most frequently used configurations. For a full list of all configs, please visit the [All Configurations](/docs/configurations) page.

- [**Hudi Table Config**](#TABLE_CONFIG): Basic Hudi Table configuration parameters.
- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metastore and Catalog Sync Configs**](#META_SYNC): Configurations used by the Hudi to sync metadata to external metastores and catalogs.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of key Hudi stats and metrics.
- [**Kafka Connect Configs**](#KAFKA_CONNECT): These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables
- [**Hudi Streamer Configs**](#HUDI_STREAMER): These set of configs are used for Hudi Streamer utility which provides the way to ingest from different sources such as DFS or Kafka.

:::note 
In the tables below **(N/A)** means there is no default value set
:::

## Hudi Table Config {#TABLE_CONFIG}
Basic Hudi Table configuration parameters.


### Hudi Table Basic Configs {#Hudi-Table-Basic-Configs}
Configurations of the Hudi Table like type of ingestion, storage formats, hive table name etc. Configurations are loaded from hoodie.properties, these properties are usually set during initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.




[**Basic Configs**](#Hudi-Table-Basic-Configs-basic-configs)


| Config Name                                                                                      | Default                                                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.bootstrap.base.path](#hoodiebootstrapbasepath)                                           | (N/A)                                                             | Base path of the dataset that needs to be bootstrapped as a Hudi table<br />`Config Param: BOOTSTRAP_BASE_PATH`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.database.name](#hoodiedatabasename)                                                      | (N/A)                                                             | Database name that will be used for incremental query.If different databases have the same table name during incremental query, we can set it to limit the table name under a specific database<br />`Config Param: DATABASE_NAME`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [hoodie.table.checksum](#hoodietablechecksum)                                                    | (N/A)                                                             | Table checksum is used to guard against partial writes in HDFS. It is added as the last entry in hoodie.properties and then used to validate while reading table config.<br />`Config Param: TABLE_CHECKSUM`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| [hoodie.table.create.schema](#hoodietablecreateschema)                                           | (N/A)                                                             | Schema used when creating the table, for the first time.<br />`Config Param: CREATE_SCHEMA`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [hoodie.table.keygenerator.class](#hoodietablekeygeneratorclass)                                 | (N/A)                                                             | Key Generator class property for the hoodie table<br />`Config Param: KEY_GENERATOR_CLASS_NAME`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.table.metadata.partitions](#hoodietablemetadatapartitions)                               | (N/A)                                                             | Comma-separated list of metadata partitions that have been completely built and in-sync with data table. These partitions are ready for use by the readers<br />`Config Param: TABLE_METADATA_PARTITIONS`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [hoodie.table.metadata.partitions.inflight](#hoodietablemetadatapartitionsinflight)              | (N/A)                                                             | Comma-separated list of metadata partitions whose building is in progress. These partitions are not yet ready for use by the readers.<br />`Config Param: TABLE_METADATA_PARTITIONS_INFLIGHT`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [hoodie.table.name](#hoodietablename)                                                            | (N/A)                                                             | Table name that will be used for registering with Hive. Needs to be same across runs.<br />`Config Param: NAME`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.table.partition.fields](#hoodietablepartitionfields)                                     | (N/A)                                                             | Fields used to partition the table. Concatenated values of these fields are used as the partition path, by invoking toString()<br />`Config Param: PARTITION_FIELDS`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [hoodie.table.precombine.field](#hoodietableprecombinefield)                                     | (N/A)                                                             | Field used in preCombining before actual write. By default, when two records have the same key value, the largest value for the precombine field determined by Object.compareTo(..), is picked.<br />`Config Param: PRECOMBINE_FIELD`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [hoodie.table.recordkey.fields](#hoodietablerecordkeyfields)                                     | (N/A)                                                             | Columns used to uniquely identify the table. Concatenated values of these fields are used as  the record key component of HoodieKey.<br />`Config Param: RECORDKEY_FIELDS`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [hoodie.table.secondary.indexes.metadata](#hoodietablesecondaryindexesmetadata)                  | (N/A)                                                             | The metadata of secondary indexes<br />`Config Param: SECONDARY_INDEXES_METADATA`<br />`Since Version: 0.13.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [hoodie.timeline.layout.version](#hoodietimelinelayoutversion)                                   | (N/A)                                                             | Version of timeline used, by the table.<br />`Config Param: TIMELINE_LAYOUT_VERSION`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [hoodie.archivelog.folder](#hoodiearchivelogfolder)                                              | archived                                                          | path under the meta folder, to store archived timeline instants at.<br />`Config Param: ARCHIVELOG_FOLDER`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [hoodie.bootstrap.index.class](#hoodiebootstrapindexclass)                                       | org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex  | Implementation to use, for mapping base files to bootstrap base file, that contain actual data.<br />`Config Param: BOOTSTRAP_INDEX_CLASS_NAME`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.bootstrap.index.enable](#hoodiebootstrapindexenable)                                     | true                                                              | Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined, default true.<br />`Config Param: BOOTSTRAP_INDEX_ENABLE`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [hoodie.compaction.payload.class](#hoodiecompactionpayloadclass)                                 | org.apache.hudi.common.model.OverwriteWithLatestAvroPayload       | Payload class to use for performing compactions, i.e merge delta logs with current base file and then  produce a new base file.<br />`Config Param: PAYLOAD_CLASS_NAME`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [hoodie.compaction.record.merger.strategy](#hoodiecompactionrecordmergerstrategy)                | eeb8d96f-b1e4-49fd-bbf8-28ac514178e5                              | Id of merger strategy. Hudi will pick HoodieRecordMerger implementations in hoodie.datasource.write.record.merger.impls which has the same merger strategy id<br />`Config Param: RECORD_MERGER_STRATEGY`<br />`Since Version: 0.13.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false                                                             | Flag to indicate whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br />`Config Param: HIVE_STYLE_PARTITIONING_ENABLE`                                                                                                                                                                                                                                                                                                                                                                                                                        |
| [hoodie.partition.metafile.use.base.format](#hoodiepartitionmetafileusebaseformat)               | false                                                             | If true, partition metafiles are saved in the same format as base-files for this dataset (e.g. Parquet / ORC). If false (default) partition metafiles are saved as properties files.<br />`Config Param: PARTITION_METAFILE_USE_BASE_FORMAT`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [hoodie.populate.meta.fields](#hoodiepopulatemetafields)                                         | true                                                              | When enabled, populates all meta fields. When disabled, no meta fields are populated and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing<br />`Config Param: POPULATE_META_FIELDS`                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [hoodie.table.base.file.format](#hoodietablebasefileformat)                                      | PARQUET                                                           | Base file format to store all the base file data.<br />`Config Param: BASE_FILE_FORMAT`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [hoodie.table.cdc.enabled](#hoodietablecdcenabled)                                               | false                                                             | When enable, persist the change data if necessary, and can be queried as a CDC query mode.<br />`Config Param: CDC_ENABLED`<br />`Since Version: 0.13.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [hoodie.table.cdc.supplemental.logging.mode](#hoodietablecdcsupplementalloggingmode)             | DATA_BEFORE_AFTER                                                 | org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode: Change log capture supplemental logging mode. The supplemental log is used for accelerating the generation of change log details.     OP_KEY_ONLY: Only keeping record keys in the supplemental logs, so the reader needs to figure out the update before image and after image.     DATA_BEFORE: Keeping the before images in the supplemental logs, so the reader needs to figure out the update after images.     DATA_BEFORE_AFTER(default): Keeping the before and after images in the supplemental logs, so the reader can generate the details directly from the logs.<br />`Config Param: CDC_SUPPLEMENTAL_LOGGING_MODE`<br />`Since Version: 0.13.0` |
| [hoodie.table.log.file.format](#hoodietablelogfileformat)                                        | HOODIE_LOG                                                        | Log format used for the delta logs.<br />`Config Param: LOG_FILE_FORMAT`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [hoodie.table.timeline.timezone](#hoodietabletimelinetimezone)                                   | LOCAL                                                             | User can set hoodie commit timeline timezone, such as utc, local and so on. local is default<br />`Config Param: TIMELINE_TIMEZONE`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [hoodie.table.type](#hoodietabletype)                                                            | COPY_ON_WRITE                                                     | The table type for the underlying data, for this write. This can’t change between writes.<br />`Config Param: TYPE`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [hoodie.table.version](#hoodietableversion)                                                      | ZERO                                                              | Version of table, used for running upgrade/downgrade steps between releases with potentially breaking/backwards compatible changes.<br />`Config Param: VERSION`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
---

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.


### Read Options {#Read-Options}
Options useful for reading tables via `read.format.option(...)`





[**Basic Configs**](#Read-Options-basic-configs)


| Config Name                                                                       | Default   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.read.begin.instanttime](#hoodiedatasourcereadbegininstanttime) | (N/A)     | Required when `hoodie.datasource.query.type` is set to `incremental`. Represents the instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time &gt; BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM. Note that if `hoodie.read.timeline.holes.resolution.policy` set to USE_TRANSITION_TIME, will use instant's `stateTransitionTime` to perform comparison.<br />`Config Param: BEGIN_INSTANTTIME`     |
| [hoodie.datasource.read.end.instanttime](#hoodiedatasourcereadendinstanttime)     | (N/A)     | Used when `hoodie.datasource.query.type` is set to `incremental`. Represents the instant time to limit incrementally fetched data to. When not specified latest commit time from timeline is assumed by default. When specified, new data written with an instant_time &lt;= END_INSTANTTIME are fetched out. Point in time type queries make more sense with begin and end instant times specified. Note that if `hoodie.read.timeline.holes.resolution.policy` set to `USE_TRANSITION_TIME`, will use instant's `stateTransitionTime` to perform comparison.<br />`Config Param: END_INSTANTTIME` |
| [hoodie.datasource.query.type](#hoodiedatasourcequerytype)                        | snapshot  | Whether data needs to be read, in `incremental` mode (new data since an instantTime) (or) `read_optimized` mode (obtain latest view, based on base files) (or) `snapshot` mode (obtain latest view, by merging base and (if any) log files)<br />`Config Param: QUERY_TYPE`                                                                                                                                                                                                                                                                                                                         |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield) | ts        | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br />`Config Param: READ_PRE_COMBINE_FIELD`                                                                                                                                                                                                                                                                                                                                                      |
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





[**Basic Configs**](#Write-Options-basic-configs)


| Config Name                                                                                      | Default                       | Description                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------------------------------------------------ | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                               | (N/A)                         | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br />`Config Param: HIVE_SYNC_MODE`                                                                                                                                                                                            |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)          | (N/A)                         | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value obtained by invoking .toString()<br />`Config Param: PARTITIONPATH_FIELD`                                                                                                                         |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                  | (N/A)                         | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br />`Config Param: RECORDKEY_FIELD`                                   |
| [hoodie.clustering.async.enabled](#hoodieclusteringasyncenabled)                                 | false                         | Enable running of clustering service, asynchronously as inserts happen on the table.<br />`Config Param: ASYNC_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                      |
| [hoodie.clustering.inline](#hoodieclusteringinline)                                              | false                         | Turn on inline clustering - clustering will be run after each write operation is complete<br />`Config Param: INLINE_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                           | false                         | When set to true, register/sync the table to Apache Hive metastore.<br />`Config Param: HIVE_SYNC_ENABLED`                                                                                                                                                                                         |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)                         | jdbc:hive2://localhost:10000  | Hive metastore url<br />`Config Param: HIVE_URL`                                                                                                                                                                                                                                                   |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris)            | thrift://localhost:9083       | Hive metastore url<br />`Config Param: METASTORE_URIS`                                                                                                                                                                                                                                             |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                            | false                         | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED`                                                                                                                                                                                  |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false                         | Flag to indicate whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br />`Config Param: HIVE_STYLE_PARTITIONING` |
| [hoodie.datasource.write.operation](#hoodiedatasourcewriteoperation)                             | upsert                        | Whether to do upsert, insert or bulk_insert for the write operation. Use bulk_insert to load new data into a table, and there on use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br />`Config Param: OPERATION`                |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield)                | ts                            | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br />`Config Param: PRECOMBINE_FIELD`                                                           |
| [hoodie.datasource.write.table.type](#hoodiedatasourcewritetabletype)                            | COPY_ON_WRITE                 | The table type for the underlying data, for this write. This can’t change between writes.<br />`Config Param: TABLE_TYPE`                                                                                                                                                                          |
---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.


### Flink Options {#Flink-Options}
Flink jobs using the SQL can be configured through the options in WITH clause. The actual datasource level configs are listed below.




[**Basic Configs**](#Flink-Options-basic-configs)


| Config Name                                                                                      | Default                       | Description                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------------------ | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.database.name](#hoodiedatabasename)                                                      | (N/A)                         | Database name to register to Hive metastore<br /> `Config Param: DATABASE_NAME`                                                                                                                                                                                                                                                                                                                           |
| [hoodie.table.name](#hoodietablename)                                                            | (N/A)                         | Table name to register to Hive metastore<br /> `Config Param: TABLE_NAME`                                                                                                                                                                                                                                                                                                                                 |
| [path](#path)                                                                                    | (N/A)                         | Base path for the target hoodie table. The path would be created if it does not exist, otherwise a Hoodie table expects to be initialized successfully<br /> `Config Param: PATH`                                                                                                                                                                                                                         |
| [read.end-commit](#readend-commit)                                                               | (N/A)                         | End commit instant for reading, the commit time format should be 'yyyyMMddHHmmss'<br /> `Config Param: READ_END_COMMIT`                                                                                                                                                                                                                                                                                   |
| [read.start-commit](#readstart-commit)                                                           | (N/A)                         | Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant for streaming read<br /> `Config Param: READ_START_COMMIT`                                                                                                                                                                                                                |
| [archive.max_commits](#archivemax_commits)                                                       | 50                            | Max number of commits to keep before archiving older commits into a sequential log, default 50<br /> `Config Param: ARCHIVE_MAX_COMMITS`                                                                                                                                                                                                                                                                  |
| [archive.min_commits](#archivemin_commits)                                                       | 40                            | Min number of commits to keep before archiving older commits into a sequential log, default 40<br /> `Config Param: ARCHIVE_MIN_COMMITS`                                                                                                                                                                                                                                                                  |
| [cdc.enabled](#cdcenabled)                                                                       | false                         | When enable, persist the change data if necessary, and can be queried as a CDC query mode<br /> `Config Param: CDC_ENABLED`                                                                                                                                                                                                                                                                               |
| [cdc.supplemental.logging.mode](#cdcsupplementalloggingmode)                                     | DATA_BEFORE_AFTER             | Setting 'op_key_only' persists the 'op' and the record key only, setting 'data_before' persists the additional 'before' image, and setting 'data_before_after' persists the additional 'before' and 'after' images.<br /> `Config Param: SUPPLEMENTAL_LOGGING_MODE`                                                                                                                                       |
| [changelog.enabled](#changelogenabled)                                                           | false                         | Whether to keep all the intermediate changes, we try to keep all the changes of a record when enabled: 1). The sink accept the UPDATE_BEFORE message; 2). The source try to emit every changes of a record. The semantics is best effort because the compaction job would finally merge all changes of a record into one.  default false to have UPSERT semantics<br /> `Config Param: CHANGELOG_ENABLED` |
| [clean.async.enabled](#cleanasyncenabled)                                                        | true                          | Whether to cleanup the old commits immediately on new commits, enabled by default<br /> `Config Param: CLEAN_ASYNC_ENABLED`                                                                                                                                                                                                                                                                               |
| [clean.retain_commits](#cleanretain_commits)                                                     | 30                            | Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table, default 30<br /> `Config Param: CLEAN_RETAIN_COMMITS`                                                                                                                                                  |
| [clustering.async.enabled](#clusteringasyncenabled)                                              | false                         | Async Clustering, default false<br /> `Config Param: CLUSTERING_ASYNC_ENABLED`                                                                                                                                                                                                                                                                                                                            |
| [clustering.plan.strategy.small.file.limit](#clusteringplanstrategysmallfilelimit)               | 600                           | Files smaller than the size specified here are candidates for clustering, default 600 MB<br /> `Config Param: CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT`                                                                                                                                                                                                                                                  |
| [clustering.plan.strategy.target.file.max.bytes](#clusteringplanstrategytargetfilemaxbytes)      | 1073741824                    | Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB<br /> `Config Param: CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`                                                                                                                                                                                                                  |
| [compaction.async.enabled](#compactionasyncenabled)                                              | true                          | Async Compaction, enabled by default for MOR<br /> `Config Param: COMPACTION_ASYNC_ENABLED`                                                                                                                                                                                                                                                                                                               |
| [compaction.delta_commits](#compactiondelta_commits)                                             | 5                             | Max delta commits needed to trigger compaction, default 5 commits<br /> `Config Param: COMPACTION_DELTA_COMMITS`                                                                                                                                                                                                                                                                                          |
| [hive_sync.enabled](#hive_syncenabled)                                                           | false                         | Asynchronously sync Hive meta to HMS, default false<br /> `Config Param: HIVE_SYNC_ENABLED`                                                                                                                                                                                                                                                                                                               |
| [hive_sync.jdbc_url](#hive_syncjdbc_url)                                                         | jdbc:hive2://localhost:10000  | Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'<br /> `Config Param: HIVE_SYNC_JDBC_URL`                                                                                                                                                                                                                                                                                                   |
| [hive_sync.metastore.uris](#hive_syncmetastoreuris)                                              |                               | Metastore uris for hive sync, default ''<br /> `Config Param: HIVE_SYNC_METASTORE_URIS`                                                                                                                                                                                                                                                                                                                   |
| [hive_sync.mode](#hive_syncmode)                                                                 | HMS                           | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'hms'<br /> `Config Param: HIVE_SYNC_MODE`                                                                                                                                                                                                                                                                                    |
| [hoodie.datasource.query.type](#hoodiedatasourcequerytype)                                       | snapshot                      | Decides how data files need to be read, in 1) Snapshot mode (obtain latest view, based on row &amp; columnar data); 2) incremental mode (new data since an instantTime); 3) Read Optimized mode (obtain latest view, based on columnar data) .Default: snapshot<br /> `Config Param: QUERY_TYPE`                                                                                                          |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false                         | Whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br /> `Config Param: HIVE_STYLE_PARTITIONING`                                                                                                                        |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)          |                               | Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual value obtained by invoking .toString(), default ''<br /> `Config Param: PARTITION_PATH_FIELD`                                                                                                                                                                                                              |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                  | uuid                          | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br /> `Config Param: RECORD_KEY_FIELD`                                                                                                                                        |
| [index.type](#indextype)                                                                         | FLINK_STATE                   | Index type of Flink write job, default is using state backed index.<br /> `Config Param: INDEX_TYPE`                                                                                                                                                                                                                                                                                                      |
| [metadata.compaction.delta_commits](#metadatacompactiondelta_commits)                            | 10                            | Max delta commits for metadata table to trigger compaction, default 10<br /> `Config Param: METADATA_COMPACTION_DELTA_COMMITS`                                                                                                                                                                                                                                                                            |
| [metadata.enabled](#metadataenabled)                                                             | false                         | Enable the internal metadata table which serves table metadata like level file listings, default disabled<br /> `Config Param: METADATA_ENABLED`                                                                                                                                                                                                                                                          |
| [precombine.field](#precombinefield)                                                             | ts                            | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br /> `Config Param: PRECOMBINE_FIELD`                                                                                                                                                                 |
| [read.streaming.enabled](#readstreamingenabled)                                                  | false                         | Whether to read as streaming source, default false<br /> `Config Param: READ_AS_STREAMING`                                                                                                                                                                                                                                                                                                                |
| [table.type](#tabletype)                                                                         | COPY_ON_WRITE                 | Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ<br /> `Config Param: TABLE_TYPE`                                                                                                                                                                                                                                                                                                                 |
| [write.operation](#writeoperation)                                                               | upsert                        | The write operation, that this write should do<br /> `Config Param: OPERATION`                                                                                                                                                                                                                                                                                                                            |
| [write.parquet.max.file.size](#writeparquetmaxfilesize)                                          | 120                           | Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE`                                                                                                                                                                                       |
---

## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.


### Common Configurations {#Common-Configurations}
The following set of configurations are common across Hudi.




[**Basic Configs**](#Common-Configurations-basic-configs)


| Config Name                         | Default | Description                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.base.path](#hoodiebasepath) | (N/A)   | Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.<br />`Config Param: BASE_PATH` |
---


### Metadata Configs {#Metadata-Configs}
Configurations used by the Hudi Metadata Table. This table maintains the metadata about a given Hudi table (e.g file listings)  to avoid overhead of accessing cloud storage, during queries.




[**Basic Configs**](#Metadata-Configs-basic-configs)


| Config Name                                                                        | Default | Description                                                                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.metadata.enable](#hoodiemetadataenable)                                    | true    | Enable the internal metadata table which serves table metadata like level file listings<br />`Config Param: ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                                                    |
| [hoodie.metadata.index.bloom.filter.enable](#hoodiemetadataindexbloomfilterenable) | false   | Enable indexing bloom filters of user data files under metadata table. When enabled, metadata table will have a partition to store the bloom filter index and will be used during the index lookups.<br />`Config Param: ENABLE_METADATA_INDEX_BLOOM_FILTER`<br />`Since Version: 0.11.0`                          |
| [hoodie.metadata.index.column.stats.enable](#hoodiemetadataindexcolumnstatsenable) | false   | Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br />`Config Param: ENABLE_METADATA_INDEX_COLUMN_STATS`<br />`Since Version: 0.11.0` |
---


### Storage Configs {#Storage-Configs}
Configurations that control aspects around writing, sizing, reading base and log files.




[**Basic Configs**](#Storage-Configs-basic-configs)


| Config Name                                                        | Default    | Description                                                                                                                                                                                                           |
| ------------------------------------------------------------------ | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.parquet.compression.codec](#hoodieparquetcompressioncodec) | gzip       | Compression Codec for parquet files<br />`Config Param: PARQUET_COMPRESSION_CODEC_NAME`                                                                                                                               |
| [hoodie.parquet.max.file.size](#hoodieparquetmaxfilesize)          | 125829120  | Target size in bytes for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br />`Config Param: PARQUET_MAX_FILE_SIZE` |
---


### Archival Configs {#Archival-Configs}
Configurations that control archival.




[**Basic Configs**](#Archival-Configs-basic-configs)


| Config Name                                      | Default | Description                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.keep.max.commits](#hoodiekeepmaxcommits) | 30      | Archiving service moves older entries from timeline into an archived log after each write, to keep the metadata overhead constant, even as the table size grows. This config controls the maximum number of instants to retain in the active timeline. <br />`Config Param: MAX_COMMITS_TO_KEEP` |
| [hoodie.keep.min.commits](#hoodiekeepmincommits) | 20      | Similar to hoodie.keep.max.commits, but controls the minimum number of instants to retain in the active timeline.<br />`Config Param: MIN_COMMITS_TO_KEEP`                                                                                                                                       |
---


### Bootstrap Configs {#Bootstrap-Configs}
Configurations that control how you want to bootstrap your existing tables for the first time into hudi. The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing  writers and new hudi writers in parallel, to validate the migration.




[**Basic Configs**](#Bootstrap-Configs-basic-configs)


| Config Name                                            | Default | Description                                                                                                                       |
| ------------------------------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.bootstrap.base.path](#hoodiebootstrapbasepath) | (N/A)   | Base path of the dataset that needs to be bootstrapped as a Hudi table<br />`Config Param: BASE_PATH`<br />`Since Version: 0.6.0` |
---


### Clean Configs {#Clean-Configs}
Cleaning (reclamation of older/unused file groups/slices).




[**Basic Configs**](#Clean-Configs-basic-configs)


| Config Name                                                      | Default | Description                                                                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.clean.async](#hoodiecleanasync)                          | false   | Only applies when hoodie.clean.automatic is turned on. When turned on runs cleaner async with writing, which can speed up overall write performance.<br />`Config Param: ASYNC_CLEAN`                                                                                                                                                  |
| [hoodie.cleaner.commits.retained](#hoodiecleanercommitsretained) | 10      | When KEEP_LATEST_COMMITS cleaning policy is used, the number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br />`Config Param: CLEANER_COMMITS_RETAINED` |
---


### Clustering Configs {#Clustering-Configs}
Configurations that control the clustering table service in hudi, which optimizes the storage layout for better query performance by sorting and sizing data files.




[**Basic Configs**](#Clustering-Configs-basic-configs)


| Config Name                                                                                              | Default     | Description                                                                                                                                                                                |
| -------------------------------------------------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.clustering.async.enabled](#hoodieclusteringasyncenabled)                                         | false       | Enable running of clustering service, asynchronously as inserts happen on the table.<br />`Config Param: ASYNC_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                              |
| [hoodie.clustering.inline](#hoodieclusteringinline)                                                      | false       | Turn on inline clustering - clustering will be run after each write operation is complete<br />`Config Param: INLINE_CLUSTERING`<br />`Since Version: 0.7.0`                               |
| [hoodie.clustering.plan.strategy.small.file.limit](#hoodieclusteringplanstrategysmallfilelimit)          | 314572800   | Files smaller than the size in bytes specified here are candidates for clustering<br />`Config Param: PLAN_STRATEGY_SMALL_FILE_LIMIT`<br />`Since Version: 0.7.0`                          |
| [hoodie.clustering.plan.strategy.target.file.max.bytes](#hoodieclusteringplanstrategytargetfilemaxbytes) | 1073741824  | Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br />`Config Param: PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`<br />`Since Version: 0.7.0` |
---


### Compaction Configs {#Compaction-Configs}
Configurations that control compaction (merging of log files onto a new base files).




[**Basic Configs**](#Compaction-Configs-basic-configs)


| Config Name                                                                    | Default | Description                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.compact.inline](#hoodiecompactinline)                                  | false   | When set to true, compaction service is triggered after each write. While being  simpler operationally, this adds extra latency on the write path.<br />`Config Param: INLINE_COMPACT`                                                                                                                                                              |
| [hoodie.compact.inline.max.delta.commits](#hoodiecompactinlinemaxdeltacommits) | 5       | Number of delta commits after the last compaction, before scheduling of a new compaction is attempted. This config takes effect only for the compaction triggering strategy based on the number of commits, i.e., NUM_COMMITS, NUM_COMMITS_AFTER_LAST_REQUEST, NUM_AND_TIME, and NUM_OR_TIME.<br />`Config Param: INLINE_COMPACT_NUM_DELTA_COMMITS` |
---


### Error table Configs {#Error-table-Configs}
Configurations that are required for Error table configs




[**Basic Configs**](#Error-table-Configs-basic-configs)


| Config Name                                                                                       | Default          | Description                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.errortable.base.path](#hoodieerrortablebasepath)                                          | (N/A)            | Base path for error table under which all error records would be stored.<br />`Config Param: ERROR_TABLE_BASE_PATH`                                                                                                                                                                                                          |
| [hoodie.errortable.target.table.name](#hoodieerrortabletargettablename)                           | (N/A)            | Table name to be used for the error table<br />`Config Param: ERROR_TARGET_TABLE`                                                                                                                                                                                                                                            |
| [hoodie.errortable.write.class](#hoodieerrortablewriteclass)                                      | (N/A)            | Class which handles the error table writes. This config is used to configure a custom implementation for Error Table Writer. Specify the full class name of the custom error table writer as a value for this config<br />`Config Param: ERROR_TABLE_WRITE_CLASS`                                                            |
| [hoodie.errortable.enable](#hoodieerrortableenable)                                               | false            | Config to enable error table. If the config is enabled, all the records with processing error in DeltaStreamer are transferred to error table.<br />`Config Param: ERROR_TABLE_ENABLED`                                                                                                                                      |
| [hoodie.errortable.insert.shuffle.parallelism](#hoodieerrortableinsertshuffleparallelism)         | 200              | Config to set insert shuffle parallelism. The config is similar to hoodie.insert.shuffle.parallelism config but applies to the error table.<br />`Config Param: ERROR_TABLE_INSERT_PARALLELISM_VALUE`                                                                                                                        |
| [hoodie.errortable.upsert.shuffle.parallelism](#hoodieerrortableupsertshuffleparallelism)         | 200              | Config to set upsert shuffle parallelism. The config is similar to hoodie.upsert.shuffle.parallelism config but applies to the error table.<br />`Config Param: ERROR_TABLE_UPSERT_PARALLELISM_VALUE`                                                                                                                        |
| [hoodie.errortable.validate.recordcreation.enable](#hoodieerrortablevalidaterecordcreationenable) | true             | Records that fail to be created due to keygeneration failure or other issues will be sent to the Error Table<br />`Config Param: ERROR_ENABLE_VALIDATE_RECORD_CREATION`<br />`Since Version: 0.15.0`                                                                                                                         |
| [hoodie.errortable.validate.targetschema.enable](#hoodieerrortablevalidatetargetschemaenable)     | false            | Records with schema mismatch with Target Schema are sent to Error Table.<br />`Config Param: ERROR_ENABLE_VALIDATE_TARGET_SCHEMA`                                                                                                                                                                                            |
| [hoodie.errortable.write.failure.strategy](#hoodieerrortablewritefailurestrategy)                 | ROLLBACK_COMMIT  | The config specifies the failure strategy if error table write fails. Use one of - [ROLLBACK_COMMIT (Rollback the corresponding base table write commit for which the error events were triggered) , LOG_ERROR (Error is logged but the base table write succeeds) ]<br />`Config Param: ERROR_TABLE_WRITE_FAILURE_STRATEGY` |
---


### Write Configurations {#Write-Configurations}
Configurations that control write behavior on Hudi tables. These can be directly passed down from even higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g Hudi Streamer).




[**Basic Configs**](#Write-Configurations-basic-configs)


| Config Name                                                                       | Default        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| --------------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.base.path](#hoodiebasepath)                                               | (N/A)          | Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.<br />`Config Param: BASE_PATH`                                                                                                                      |
| [hoodie.table.name](#hoodietablename)                                             | (N/A)          | Table name that will be used for registering with metastores like HMS. Needs to be same across runs.<br />`Config Param: TBL_NAME`                                                                                                                                                                                                                                                                                                                |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield) | ts             | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br />`Config Param: PRECOMBINE_FIELD_NAME`                                                                                                                                                                                                     |
| [hoodie.write.concurrency.mode](#hoodiewriteconcurrencymode)                      | SINGLE_WRITER  | org.apache.hudi.common.model.WriteConcurrencyMode: Concurrency modes for write operations.     SINGLE_WRITER(default): Only one active writer to the table. Maximizes throughput.     OPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table with lazy conflict resolution using locks. This means that only one writer succeeds if multiple writers write to the same file group.<br />`Config Param: WRITE_CONCURRENCY_MODE` |
---


### Lock Configs {#LOCK}
Configurations that control locking mechanisms required for concurrency control  between writers to a Hudi table. Concurrency between Hudi's own table services  are auto managed internally.


#### Common Lock Configurations {#Common-Lock-Configurations}





[**Basic Configs**](#Common-Lock-Configurations-basic-configs)


| Config Name                                                                      | Default | Description                                                                                                                                                            |
| -------------------------------------------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.write.lock.heartbeat_interval_ms](#hoodiewritelockheartbeat_interval_ms) | 60000   | Heartbeat interval in ms, to send a heartbeat to indicate that hive client holding locks.<br />`Config Param: LOCK_HEARTBEAT_INTERVAL_MS`<br />`Since Version: 0.15.0` |
---


### Key Generator Configs {#KEY_GENERATOR}
Hudi maintains keys (record key + partition path) for uniquely identifying a particular record. These configs allow developers to setup the Key generator class that extracts these out of incoming records.


#### Key Generator Options {#Key-Generator-Options}





[**Basic Configs**](#Key-Generator-Options-basic-configs)


| Config Name                                                                                      | Default | Description                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)          | (N/A)   | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value obtained by invoking .toString()<br />`Config Param: PARTITIONPATH_FIELD_NAME`                                                                                                                           |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                  | (N/A)   | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br />`Config Param: RECORDKEY_FIELD_NAME`                                     |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false   | Flag to indicate whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br />`Config Param: HIVE_STYLE_PARTITIONING_ENABLE` |
---


### Index Configs {#INDEX}
Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.


#### Common Index Configs {#Common-Index-Configs}





[**Basic Configs**](#Common-Index-Configs-basic-configs)


| Config Name                           | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.index.type](#hoodieindextype) | (N/A)   | org.apache.hudi.index.HoodieIndex$IndexType: Determines how input records are indexed, i.e., looked up based on the key for the location in the existing table. Default is SIMPLE on Spark engine, and INMEMORY on Flink and Java engines.     HBASE: uses an external managed Apache HBase table to store record key to location mapping. HBase index is a global index, enforcing key uniqueness across all partitions in the table.     INMEMORY: Uses in-memory hashmap in Spark and Java engine and Flink in-memory state in Flink for indexing.     BLOOM: Employs bloom filters built out of the record keys, optionally also pruning candidate files using record key ranges. Key uniqueness is enforced inside partitions.     GLOBAL_BLOOM: Employs bloom filters built out of the record keys, optionally also pruning candidate files using record key ranges. Key uniqueness is enforced across all partitions in the table.      SIMPLE: Performs a lean join of the incoming update/delete records against keys extracted from the table on storage.Key uniqueness is enforced inside partitions.     GLOBAL_SIMPLE: Performs a lean join of the incoming update/delete records against keys extracted from the table on storage.Key uniqueness is enforced across all partitions in the table.     BUCKET: locates the file group containing the record fast by using bucket hashing, particularly beneficial in large scale. Use `hoodie.index.bucket.engine` to choose bucket engine type, i.e., how buckets are generated.     FLINK_STATE: Internal Config for indexing based on Flink state.     RECORD_INDEX: Index which saves the record key to location mappings in the HUDI Metadata Table. Record index is a global index, enforcing key uniqueness across all partitions in the table. Supports sharding to achieve very high scale.<br />`Config Param: INDEX_TYPE` |
---

## Metastore and Catalog Sync Configs {#META_SYNC}
Configurations used by the Hudi to sync metadata to external metastores and catalogs.


### Common Metadata Sync Configs {#Common-Metadata-Sync-Configs}





[**Basic Configs**](#Common-Metadata-Sync-Configs-basic-configs)


| Config Name                                                           | Default | Description                                                                                                       |
| --------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false   | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED` |
---


### Glue catalog sync based client Configurations {#Glue-catalog-sync-based-client-Configurations}
Configs that control Glue catalog sync based client.




[**Basic Configs**](#Glue-catalog-sync-based-client-Configurations-basic-configs)


| Config Name                                                                                                                 | Default | Description                                                                                                                                                                                                                                                                                                                                   |
| --------------------------------------------------------------------------------------------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.glue.partition_index_fields](#hoodiedatasourcemetasyncgluepartition_index_fields)              |         | Specify the partitions fields to index on aws glue. Separate the fields by semicolon. By default, when the feature is enabled, all the partition will be indexed. You can create up to three indexes, separate them by comma. Eg: col1;col2;col3,col2,col3<br />`Config Param: META_SYNC_PARTITION_INDEX_FIELDS`<br />`Since Version: 0.15.0` |
| [hoodie.datasource.meta.sync.glue.partition_index_fields.enable](#hoodiedatasourcemetasyncgluepartition_index_fieldsenable) | false   | Enable aws glue partition index feature, to speedup partition based query pattern<br />`Config Param: META_SYNC_PARTITION_INDEX_FIELDS_ENABLE`<br />`Since Version: 0.15.0`                                                                                                                                                                   |
---


### BigQuery Sync Configs {#BigQuery-Sync-Configs}
Configurations used by the Hudi to sync metadata to Google BigQuery.




[**Basic Configs**](#BigQuery-Sync-Configs-basic-configs)


| Config Name                                                           | Default | Description                                                                                                       |
| --------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false   | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED` |
---


### Hive Sync Configs {#Hive-Sync-Configs}
Configurations used by the Hudi to sync metadata to Hive Metastore.




[**Basic Configs**](#Hive-Sync-Configs-basic-configs)


| Config Name                                                                           | Default                       | Description                                                                                                       |
| ------------------------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                    | (N/A)                         | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br />`Config Param: HIVE_SYNC_MODE`           |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                | false                         | When set to true, register/sync the table to Apache Hive metastore.<br />`Config Param: HIVE_SYNC_ENABLED`        |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)              | jdbc:hive2://localhost:10000  | Hive metastore url<br />`Config Param: HIVE_URL`                                                                  |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris) | thrift://localhost:9083       | Hive metastore url<br />`Config Param: METASTORE_URIS`                                                            |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                 | false                         | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED` |
---


### Global Hive Sync Configs {#Global-Hive-Sync-Configs}
Global replication configurations used by the Hudi to sync metadata to Hive Metastore.




[**Basic Configs**](#Global-Hive-Sync-Configs-basic-configs)


| Config Name                                                                           | Default                       | Description                                                                                                       |
| ------------------------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                    | (N/A)                         | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br />`Config Param: HIVE_SYNC_MODE`           |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                | false                         | When set to true, register/sync the table to Apache Hive metastore.<br />`Config Param: HIVE_SYNC_ENABLED`        |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)              | jdbc:hive2://localhost:10000  | Hive metastore url<br />`Config Param: HIVE_URL`                                                                  |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris) | thrift://localhost:9083       | Hive metastore url<br />`Config Param: METASTORE_URIS`                                                            |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                 | false                         | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED` |
---


### DataHub Sync Configs {#DataHub-Sync-Configs}
Configurations used by the Hudi to sync metadata to DataHub.




[**Basic Configs**](#DataHub-Sync-Configs-basic-configs)


| Config Name                                                           | Default | Description                                                                                                       |
| --------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false   | Enable Syncing the Hudi Table with an external meta store or data catalog.<br />`Config Param: META_SYNC_ENABLED` |
---

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of key Hudi stats and metrics.


### Metrics Configurations {#Metrics-Configurations}
Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.




[**Basic Configs**](#Metrics-Configurations-basic-configs)


| Config Name                                                                   | Default   | Description                                                                                                                                                                  |
| ----------------------------------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.metrics.on](#hoodiemetricson)                                         | false     | Turn on/off metrics reporting. off by default.<br />`Config Param: TURN_METRICS_ON`<br />`Since Version: 0.5.0`                                                              |
| [hoodie.metrics.reporter.type](#hoodiemetricsreportertype)                    | GRAPHITE  | Type of metrics reporter.<br />`Config Param: METRICS_REPORTER_TYPE_VALUE`<br />`Since Version: 0.5.0`                                                                       |
| [hoodie.metricscompaction.log.blocks.on](#hoodiemetricscompactionlogblockson) | false     | Turn on/off metrics reporting for log blocks with compaction commit. off by default.<br />`Config Param: TURN_METRICS_COMPACTION_LOG_BLOCKS_ON`<br />`Since Version: 0.14.0` |
---


### Metrics Configurations for M3 {#Metrics-Configurations-for-M3}
Enables reporting on Hudi metrics using M3.  Hudi publishes metrics on every commit, clean, rollback etc.




[**Basic Configs**](#Metrics-Configurations-for-M3-basic-configs)


| Config Name                                          | Default     | Description                                                                                                                                   |
| ---------------------------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.metrics.m3.env](#hoodiemetricsm3env)         | production  | M3 tag to label the environment (defaults to 'production'), applied to all metrics.<br />`Config Param: M3_ENV`<br />`Since Version: 0.15.0`  |
| [hoodie.metrics.m3.host](#hoodiemetricsm3host)       | localhost   | M3 host to connect to.<br />`Config Param: M3_SERVER_HOST_NAME`<br />`Since Version: 0.15.0`                                                  |
| [hoodie.metrics.m3.port](#hoodiemetricsm3port)       | 9052        | M3 port to connect to.<br />`Config Param: M3_SERVER_PORT_NUM`<br />`Since Version: 0.15.0`                                                   |
| [hoodie.metrics.m3.service](#hoodiemetricsm3service) | hoodie      | M3 tag to label the service name (defaults to 'hoodie'), applied to all metrics.<br />`Config Param: M3_SERVICE`<br />`Since Version: 0.15.0` |
| [hoodie.metrics.m3.tags](#hoodiemetricsm3tags)       |             | Optional M3 tags applied to all metrics.<br />`Config Param: M3_TAGS`<br />`Since Version: 0.15.0`                                            |
---

## Kafka Connect Configs {#KAFKA_CONNECT}
These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables


### Kafka Sink Connect Configurations {#Kafka-Sink-Connect-Configurations}
Configurations for Kafka Connect Sink Connector for Hudi.




[**Basic Configs**](#Kafka-Sink-Connect-Configurations-basic-configs)


| Config Name                            | Default         | Description                                                                               |
| -------------------------------------- | --------------- | ----------------------------------------------------------------------------------------- |
| [bootstrap.servers](#bootstrapservers) | localhost:9092  | The bootstrap servers for the Kafka Cluster.<br />`Config Param: KAFKA_BOOTSTRAP_SERVERS` |
---

## Hudi Streamer Configs {#HUDI_STREAMER}
These set of configs are used for Hudi Streamer utility which provides the way to ingest from different sources such as DFS or Kafka.


### Hudi Streamer Configs {#Hudi-Streamer-Configs}





[**Basic Configs**](#Hudi-Streamer-Configs-basic-configs)


| Config Name                                                           | Default | Description                                                                                           |
| --------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------- |
| [hoodie.streamer.source.kafka.topic](#hoodiestreamersourcekafkatopic) | (N/A)   | Kafka topic name. The config is specific to HoodieMultiTableStreamer<br />`Config Param: KAFKA_TOPIC` |
---


### Hudi Streamer SQL Transformer Configs {#Hudi-Streamer-SQL-Transformer-Configs}
Configurations controlling the behavior of SQL transformer in Hudi Streamer.




[**Basic Configs**](#Hudi-Streamer-SQL-Transformer-Configs-basic-configs)


| Config Name                                                               | Default | Description                                                                                  |
| ------------------------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------- |
| [hoodie.streamer.transformer.sql](#hoodiestreamertransformersql)          | (N/A)   | SQL Query to be executed during write<br />`Config Param: TRANSFORMER_SQL`                   |
| [hoodie.streamer.transformer.sql.file](#hoodiestreamertransformersqlfile) | (N/A)   | File with a SQL script to be executed during write<br />`Config Param: TRANSFORMER_SQL_FILE` |
---


### Hudi Streamer Source Configs {#DELTA_STREAMER_SOURCE}
Configurations controlling the behavior of reading source data.


#### DFS Path Selector Configs {#DFS-Path-Selector-Configs}
Configurations controlling the behavior of path selector for DFS source in Hudi Streamer.




[**Basic Configs**](#DFS-Path-Selector-Configs-basic-configs)


| Config Name                                                     | Default | Description                                                         |
| --------------------------------------------------------------- | ------- | ------------------------------------------------------------------- |
| [hoodie.streamer.source.dfs.root](#hoodiestreamersourcedfsroot) | (N/A)   | Root path of the source on DFS<br />`Config Param: ROOT_INPUT_PATH` |
---


#### Hudi Incremental Source Configs {#Hudi-Incremental-Source-Configs}
Configurations controlling the behavior of incremental pulling from a Hudi table as a source in Hudi Streamer.




[**Basic Configs**](#Hudi-Incremental-Source-Configs-basic-configs)


| Config Name                                                                   | Default | Description                                                                   |
| ----------------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------- |
| [hoodie.streamer.source.hoodieincr.path](#hoodiestreamersourcehoodieincrpath) | (N/A)   | Base-path for the source Hudi table<br />`Config Param: HOODIE_SRC_BASE_PATH` |
---


#### Kafka Source Configs {#Kafka-Source-Configs}
Configurations controlling the behavior of Kafka source in Hudi Streamer.




[**Basic Configs**](#Kafka-Source-Configs-basic-configs)


| Config Name                                                                                                          | Default                                                      | Description                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.streamer.source.kafka.topic](#hoodiestreamersourcekafkatopic)                                                | (N/A)                                                        | Kafka topic name.<br />`Config Param: KAFKA_TOPIC_NAME`                                                                       |
| [hoodie.streamer.source.kafka.proto.value.deserializer.class](#hoodiestreamersourcekafkaprotovaluedeserializerclass) | org.apache.kafka.common.serialization.ByteArrayDeserializer  | Kafka Proto Payload Deserializer Class<br />`Config Param: KAFKA_PROTO_VALUE_DESERIALIZER_CLASS`<br />`Since Version: 0.15.0` |
---


#### Pulsar Source Configs {#Pulsar-Source-Configs}
Configurations controlling the behavior of Pulsar source in Hudi Streamer.




[**Basic Configs**](#Pulsar-Source-Configs-basic-configs)


| Config Name                                                                                         | Default                  | Description                                                                                                                 |
| --------------------------------------------------------------------------------------------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.streamer.source.pulsar.topic](#hoodiestreamersourcepulsartopic)                             | (N/A)                    | Name of the target Pulsar topic to source data from<br />`Config Param: PULSAR_SOURCE_TOPIC_NAME`                           |
| [hoodie.streamer.source.pulsar.endpoint.admin.url](#hoodiestreamersourcepulsarendpointadminurl)     | http://localhost:8080    | URL of the target Pulsar endpoint (of the form 'pulsar://host:port'<br />`Config Param: PULSAR_SOURCE_ADMIN_ENDPOINT_URL`   |
| [hoodie.streamer.source.pulsar.endpoint.service.url](#hoodiestreamersourcepulsarendpointserviceurl) | pulsar://localhost:6650  | URL of the target Pulsar endpoint (of the form 'pulsar://host:port'<br />`Config Param: PULSAR_SOURCE_SERVICE_ENDPOINT_URL` |
---


#### S3 Source Configs {#S3-Source-Configs}
Configurations controlling the behavior of S3 source in Hudi Streamer.




[**Basic Configs**](#S3-Source-Configs-basic-configs)


| Config Name                                                            | Default | Description                                                                |
| ---------------------------------------------------------------------- | ------- | -------------------------------------------------------------------------- |
| [hoodie.streamer.s3.source.queue.url](#hoodiestreamers3sourcequeueurl) | (N/A)   | Queue url for cloud object events<br />`Config Param: S3_SOURCE_QUEUE_URL` |
---


#### File-based SQL Source Configs {#File-based-SQL-Source-Configs}
Configurations controlling the behavior of File-based SQL Source in Hudi Streamer.




[**Basic Configs**](#File-based-SQL-Source-Configs-basic-configs)


| Config Name                                                     | Default | Description                                                                                                                   |
| --------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.streamer.source.sql.file](#hoodiestreamersourcesqlfile) | (N/A)   | SQL file path containing the SQL query to read source data.<br />`Config Param: SOURCE_SQL_FILE`<br />`Since Version: 0.14.0` |
---


#### SQL Source Configs {#SQL-Source-Configs}
Configurations controlling the behavior of SQL source in Hudi Streamer.




[**Basic Configs**](#SQL-Source-Configs-basic-configs)


| Config Name                                                              | Default | Description                                                         |
| ------------------------------------------------------------------------ | ------- | ------------------------------------------------------------------- |
| [hoodie.streamer.source.sql.sql.query](#hoodiestreamersourcesqlsqlquery) | (N/A)   | SQL query for fetching source data.<br />`Config Param: SOURCE_SQL` |
---


### Hudi Streamer Schema Provider Configs {#SCHEMA_PROVIDER}
Configurations that control the schema provider for Hudi Streamer.


#### Hudi Streamer Schema Provider Configs {#Hudi-Streamer-Schema-Provider-Configs}





[**Basic Configs**](#Hudi-Streamer-Schema-Provider-Configs-basic-configs)


| Config Name                                                                                         | Default | Description                                                                                                                         |
| --------------------------------------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.streamer.schemaprovider.registry.targetUrl](#hoodiestreamerschemaproviderregistrytargetUrl) | (N/A)   | The schema of the target you are writing to e.g. https://foo:bar@schemaregistry.org<br />`Config Param: TARGET_SCHEMA_REGISTRY_URL` |
| [hoodie.streamer.schemaprovider.registry.url](#hoodiestreamerschemaproviderregistryurl)             | (N/A)   | The schema of the source you are reading from e.g. https://foo:bar@schemaregistry.org<br />`Config Param: SRC_SCHEMA_REGISTRY_URL`  |
---


#### File-based Schema Provider Configs {#File-based-Schema-Provider-Configs}
Configurations for file-based schema provider.




[**Basic Configs**](#File-based-Schema-Provider-Configs-basic-configs)


| Config Name                                                                                        | Default | Description                                                                           |
| -------------------------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------- |
| [hoodie.streamer.schemaprovider.source.schema.file](#hoodiestreamerschemaprovidersourceschemafile) | (N/A)   | The schema of the source you are reading from<br />`Config Param: SOURCE_SCHEMA_FILE` |
| [hoodie.streamer.schemaprovider.target.schema.file](#hoodiestreamerschemaprovidertargetschemafile) | (N/A)   | The schema of the target you are writing to<br />`Config Param: TARGET_SCHEMA_FILE`   |
---

