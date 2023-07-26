---
title: Basic Configurations
summary: This page covers the basic configurations you may use to write/read Hudi tables. This page only features a subset of the most frequently used configurations. For a full list of all configs, please visit the [All Configurations](/docs/configurations) page.
last_modified_at: 2023-07-21T07:02:09.459
---


This page covers the basic configurations you may use to write/read Hudi tables. This page only features a subset of the most frequently used configurations. For a full list of all configs, please visit the [All Configurations](/docs/configurations) page.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metastore and Catalog Sync Configs**](#META_SYNC): Configurations used by the Hudi to sync metadata to external metastores and catalogs.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of key Hudi stats and metrics.
- [**Kafka Connect Configs**](#KAFKA_CONNECT): These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables
- [**Hudi Streamer Configs**](#HUDI_STREAMER): These set of configs are used for Hudi Streamer utility which provides the way to ingest from different sources such as DFS or Kafka.

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.


### Read Options {#Read-Options}
Options useful for reading tables via `read.format.option(...)`





[**Basic Configs**](#Read-Options-basic-configs)


| Config Name                                                                       | Default             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.read.begin.instanttime](#hoodiedatasourcereadbegininstanttime) | N/A **(Required)**  | Required when `hoodie.datasource.query.type` is set to `incremental`. Represents the instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time &gt; BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM. Note that if `hoodie.datasource.read.handle.hollow.commit` set to USE_STATE_TRANSITION_TIME, will use instant's `stateTransitionTime` to perform comparison.<br /><br />`Config Param: BEGIN_INSTANTTIME`     |
| [hoodie.datasource.read.end.instanttime](#hoodiedatasourcereadendinstanttime)     | N/A **(Required)**  | Used when `hoodie.datasource.query.type` is set to `incremental`. Represents the instant time to limit incrementally fetched data to. When not specified latest commit time from timeline is assumed by default. When specified, new data written with an instant_time &lt;= END_INSTANTTIME are fetched out. Point in time type queries make more sense with begin and end instant times specified. Note that if `hoodie.datasource.read.handle.hollow.commit` set to `USE_STATE_TRANSITION_TIME`, will use instant's `stateTransitionTime` to perform comparison.<br /><br />`Config Param: END_INSTANTTIME` |
| [hoodie.datasource.query.type](#hoodiedatasourcequerytype)                        | snapshot (Optional) | Whether data needs to be read, in `incremental` mode (new data since an instantTime) (or) `read_optimized` mode (obtain latest view, based on base files) (or) `snapshot` mode (obtain latest view, by merging base and (if any) log files)<br /><br />`Config Param: QUERY_TYPE`                                                                                                                                                                                                                                                                                                                              |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield) | ts (Optional)       | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br /><br />`Config Param: READ_PRE_COMBINE_FIELD`                                                                                                                                                                                                                                                                                                                                                           |
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


| Config Name                                                                                              | Default                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                                       | N/A **(Required)**                      | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br /><br />`Config Param: HIVE_SYNC_MODE`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)                  | N/A **(Required)**                      | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value obtained by invoking .toString()<br /><br />`Config Param: PARTITIONPATH_FIELD`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                          | N/A **(Required)**                      | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br /><br />`Config Param: RECORDKEY_FIELD`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [hoodie.clustering.async.enabled](#hoodieclusteringasyncenabled)                                         | false (Optional)                        | Enable running of clustering service, asynchronously as inserts happen on the table.<br /><br />`Config Param: ASYNC_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [hoodie.clustering.inline](#hoodieclusteringinline)                                                      | false (Optional)                        | Turn on inline clustering - clustering will be run after each write operation is complete<br /><br />`Config Param: INLINE_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                                   | false (Optional)                        | When set to true, register/sync the table to Apache Hive metastore.<br /><br />`Config Param: HIVE_SYNC_ENABLED`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)                                 | jdbc:hive2://localhost:10000 (Optional) | Hive metastore url<br /><br />`Config Param: HIVE_URL`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris)                    | thrift://localhost:9083 (Optional)      | Hive metastore url<br /><br />`Config Param: METASTORE_URIS`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [hoodie.datasource.insert.dup.policy](#hoodiedatasourceinsertduppolicy)                                  | none (Optional)                         | When operation type is set to "insert", users can optionally enforce a dedup policy. This policy will be employed  when records being ingested already exists in storage. Default policy is none and no action will be taken. Another option is to choose  "drop", on which matching records from incoming will be dropped and the rest will be ingested. Third option is "fail" which will fail the write operation when same records are re-ingested. In other words, a given record as deduced by the key generation policy can be ingested only once to the target table of interest.<br /><br />`Config Param: INSERT_DUP_POLICY`                                                                                                                                                                                                                            |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                                    | false (Optional)                        | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning)         | false (Optional)                        | Flag to indicate whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br /><br />`Config Param: HIVE_STYLE_PARTITIONING`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [hoodie.datasource.write.operation](#hoodiedatasourcewriteoperation)                                     | upsert (Optional)                       | Whether to do upsert, insert or bulk_insert for the write operation. Use bulk_insert to load new data into a table, and there on use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br /><br />`Config Param: OPERATION`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield)                        | ts (Optional)                           | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br /><br />`Config Param: PRECOMBINE_FIELD`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| [hoodie.datasource.write.streaming.disable.compaction](#hoodiedatasourcewritestreamingdisablecompaction) | false (Optional)                        | By default for MOR table, async compaction is enabled with spark streaming sink. By setting this config to true, we can disable it and the expectation is that, users will schedule and execute compaction in a different process/job altogether. Some users may wish to run it separately to manage resources across table services and regular ingestion pipeline and so this could be preferred on such cases.<br /><br />`Config Param: STREAMING_DISABLE_COMPACTION`<br />`Since Version: 0.14.0`                                                                                                                                                                                                                                                                                                                                                            |
| [hoodie.datasource.write.table.type](#hoodiedatasourcewritetabletype)                                    | COPY_ON_WRITE (Optional)                | The table type for the underlying data, for this write. This can’t change between writes.<br /><br />`Config Param: TABLE_TYPE`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [hoodie.sql.insert.mode](#hoodiesqlinsertmode)                                                           | upsert (Optional)                       | Insert mode when insert data to pk-table. The optional modes are: upsert, strict and non-strict.For upsert mode, insert statement do the upsert operation for the pk-table which will update the duplicate record.For strict mode, insert statement will keep the primary key uniqueness constraint which do not allow duplicate record.While for non-strict mode, hudi just do the insert operation for the pk-table. This config is deprecated as of 0.14.0. Please use hoodie.sql.write.operation and hoodie.datasource.insert.dup.policy as you see fit.<br /><br />`Config Param: SQL_INSERT_MODE`                                                                                                                                                                                                                                                           |
| [hoodie.sql.write.operation](#hoodiesqlwriteoperation)                                                   | insert (Optional)                       | Sql write operation to use with INSERT_INTO spark sql command. This comes with 3 possible values, bulk_insert, insert and upsert. bulk_insert is generally meant for initial loads and is known to be performant compared to insert. But bulk_insert may not do small file managmeent. If you prefer hudi to automatically managee small files, then you can go with "insert". There is no precombine (if there are duplicates within the same batch being ingested, same dups will be ingested) with bulk_insert and insert and there is no index look up as well. If you may use INSERT_INTO for mutable dataset, then you may have to set this config value to "upsert". With upsert, you will get both precombine and updates to existing records on storage is also honored. If not, you may see duplicates. <br /><br />`Config Param: SQL_WRITE_OPERATION` |
---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.


### Flink Options {#Flink-Options}
Flink jobs using the SQL can be configured through the options in WITH clause. The actual datasource level configs are listed below.




[**Basic Configs**](#Flink-Options-basic-configs)


| Config Name                                                                                      | Default                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------ | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.database.name](#hoodiedatabasename)                                                      | N/A **(Required)**                      | Database name to register to Hive metastore<br /><br /> `Config Param: DATABASE_NAME`                                                                                                                                                                                                                                                                                                                           |
| [hoodie.table.name](#hoodietablename)                                                            | N/A **(Required)**                      | Table name to register to Hive metastore<br /><br /> `Config Param: TABLE_NAME`                                                                                                                                                                                                                                                                                                                                 |
| [path](#path)                                                                                    | N/A **(Required)**                      | Base path for the target hoodie table. The path would be created if it does not exist, otherwise a Hoodie table expects to be initialized successfully<br /><br /> `Config Param: PATH`                                                                                                                                                                                                                         |
| [read.end-commit](#readend-commit)                                                               | N/A **(Required)**                      | End commit instant for reading, the commit time format should be 'yyyyMMddHHmmss'<br /><br /> `Config Param: READ_END_COMMIT`                                                                                                                                                                                                                                                                                   |
| [read.start-commit](#readstart-commit)                                                           | N/A **(Required)**                      | Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant for streaming read<br /><br /> `Config Param: READ_START_COMMIT`                                                                                                                                                                                                                |
| [archive.max_commits](#archivemax_commits)                                                       | 50 (Optional)                           | Max number of commits to keep before archiving older commits into a sequential log, default 50<br /><br /> `Config Param: ARCHIVE_MAX_COMMITS`                                                                                                                                                                                                                                                                  |
| [archive.min_commits](#archivemin_commits)                                                       | 40 (Optional)                           | Min number of commits to keep before archiving older commits into a sequential log, default 40<br /><br /> `Config Param: ARCHIVE_MIN_COMMITS`                                                                                                                                                                                                                                                                  |
| [cdc.enabled](#cdcenabled)                                                                       | false (Optional)                        | When enable, persist the change data if necessary, and can be queried as a CDC query mode<br /><br /> `Config Param: CDC_ENABLED`                                                                                                                                                                                                                                                                               |
| [cdc.supplemental.logging.mode](#cdcsupplementalloggingmode)                                     | DATA_BEFORE_AFTER (Optional)            | Setting 'op_key_only' persists the 'op' and the record key only, setting 'data_before' persists the additional 'before' image, and setting 'data_before_after' persists the additional 'before' and 'after' images.<br /><br /> `Config Param: SUPPLEMENTAL_LOGGING_MODE`                                                                                                                                       |
| [changelog.enabled](#changelogenabled)                                                           | false (Optional)                        | Whether to keep all the intermediate changes, we try to keep all the changes of a record when enabled: 1). The sink accept the UPDATE_BEFORE message; 2). The source try to emit every changes of a record. The semantics is best effort because the compaction job would finally merge all changes of a record into one.  default false to have UPSERT semantics<br /><br /> `Config Param: CHANGELOG_ENABLED` |
| [clean.async.enabled](#cleanasyncenabled)                                                        | true (Optional)                         | Whether to cleanup the old commits immediately on new commits, enabled by default<br /><br /> `Config Param: CLEAN_ASYNC_ENABLED`                                                                                                                                                                                                                                                                               |
| [clean.retain_commits](#cleanretain_commits)                                                     | 30 (Optional)                           | Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table, default 30<br /><br /> `Config Param: CLEAN_RETAIN_COMMITS`                                                                                                                                                  |
| [clustering.async.enabled](#clusteringasyncenabled)                                              | false (Optional)                        | Async Clustering, default false<br /><br /> `Config Param: CLUSTERING_ASYNC_ENABLED`                                                                                                                                                                                                                                                                                                                            |
| [clustering.plan.strategy.small.file.limit](#clusteringplanstrategysmallfilelimit)               | 600 (Optional)                          | Files smaller than the size specified here are candidates for clustering, default 600 MB<br /><br /> `Config Param: CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT`                                                                                                                                                                                                                                                  |
| [clustering.plan.strategy.target.file.max.bytes](#clusteringplanstrategytargetfilemaxbytes)      | 1073741824 (Optional)                   | Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB<br /><br /> `Config Param: CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`                                                                                                                                                                                                                  |
| [compaction.async.enabled](#compactionasyncenabled)                                              | true (Optional)                         | Async Compaction, enabled by default for MOR<br /><br /> `Config Param: COMPACTION_ASYNC_ENABLED`                                                                                                                                                                                                                                                                                                               |
| [compaction.delta_commits](#compactiondelta_commits)                                             | 5 (Optional)                            | Max delta commits needed to trigger compaction, default 5 commits<br /><br /> `Config Param: COMPACTION_DELTA_COMMITS`                                                                                                                                                                                                                                                                                          |
| [hive_sync.enabled](#hive_syncenabled)                                                           | false (Optional)                        | Asynchronously sync Hive meta to HMS, default false<br /><br /> `Config Param: HIVE_SYNC_ENABLED`                                                                                                                                                                                                                                                                                                               |
| [hive_sync.jdbc_url](#hive_syncjdbc_url)                                                         | jdbc:hive2://localhost:10000 (Optional) | Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'<br /><br /> `Config Param: HIVE_SYNC_JDBC_URL`                                                                                                                                                                                                                                                                                                   |
| [hive_sync.metastore.uris](#hive_syncmetastoreuris)                                              |  (Optional)                             | Metastore uris for hive sync, default ''<br /><br /> `Config Param: HIVE_SYNC_METASTORE_URIS`                                                                                                                                                                                                                                                                                                                   |
| [hive_sync.mode](#hive_syncmode)                                                                 | HMS (Optional)                          | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'hms'<br /><br /> `Config Param: HIVE_SYNC_MODE`                                                                                                                                                                                                                                                                                    |
| [hoodie.datasource.query.type](#hoodiedatasourcequerytype)                                       | snapshot (Optional)                     | Decides how data files need to be read, in 1) Snapshot mode (obtain latest view, based on row &amp; columnar data); 2) incremental mode (new data since an instantTime); 3) Read Optimized mode (obtain latest view, based on columnar data) .Default: snapshot<br /><br /> `Config Param: QUERY_TYPE`                                                                                                          |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false (Optional)                        | Whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br /><br /> `Config Param: HIVE_STYLE_PARTITIONING`                                                                                                                        |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)          |  (Optional)                             | Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual value obtained by invoking .toString(), default ''<br /><br /> `Config Param: PARTITION_PATH_FIELD`                                                                                                                                                                                                              |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                  | uuid (Optional)                         | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br /><br /> `Config Param: RECORD_KEY_FIELD`                                                                                                                                        |
| [index.type](#indextype)                                                                         | FLINK_STATE (Optional)                  | Index type of Flink write job, default is using state backed index.<br /><br /> `Config Param: INDEX_TYPE`                                                                                                                                                                                                                                                                                                      |
| [metadata.compaction.delta_commits](#metadatacompactiondelta_commits)                            | 10 (Optional)                           | Max delta commits for metadata table to trigger compaction, default 10<br /><br /> `Config Param: METADATA_COMPACTION_DELTA_COMMITS`                                                                                                                                                                                                                                                                            |
| [metadata.enabled](#metadataenabled)                                                             | true (Optional)                         | Enable the internal metadata table which serves table metadata like level file listings, default enabled<br /><br /> `Config Param: METADATA_ENABLED`                                                                                                                                                                                                                                                           |
| [precombine.field](#precombinefield)                                                             | ts (Optional)                           | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br /><br /> `Config Param: PRECOMBINE_FIELD`                                                                                                                                                                 |
| [read.streaming.enabled](#readstreamingenabled)                                                  | false (Optional)                        | Whether to read as streaming source, default false<br /><br /> `Config Param: READ_AS_STREAMING`                                                                                                                                                                                                                                                                                                                |
| [table.type](#tabletype)                                                                         | COPY_ON_WRITE (Optional)                | Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ<br /><br /> `Config Param: TABLE_TYPE`                                                                                                                                                                                                                                                                                                                 |
| [write.operation](#writeoperation)                                                               | upsert (Optional)                       | The write operation, that this write should do<br /><br /> `Config Param: OPERATION`                                                                                                                                                                                                                                                                                                                            |
| [write.parquet.max.file.size](#writeparquetmaxfilesize)                                          | 120 (Optional)                          | Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br /> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE`                                                                                                                                                                                       |
---

## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.


### Metadata Configs {#Metadata-Configs}
Configurations used by the Hudi Metadata Table. This table maintains the metadata about a given Hudi table (e.g file listings)  to avoid overhead of accessing cloud storage, during queries.




[**Basic Configs**](#Metadata-Configs-basic-configs)


| Config Name                                                                        | Default          | Description                                                                                                                                                                                                                                                                                                              |
| ---------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.metadata.enable](#hoodiemetadataenable)                                    | true (Optional)  | Enable the internal metadata table which serves table metadata like level file listings<br /><br />`Config Param: ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                                                    |
| [hoodie.metadata.index.bloom.filter.enable](#hoodiemetadataindexbloomfilterenable) | false (Optional) | Enable indexing bloom filters of user data files under metadata table. When enabled, metadata table will have a partition to store the bloom filter index and will be used during the index lookups.<br /><br />`Config Param: ENABLE_METADATA_INDEX_BLOOM_FILTER`<br />`Since Version: 0.11.0`                          |
| [hoodie.metadata.index.column.stats.enable](#hoodiemetadataindexcolumnstatsenable) | false (Optional) | Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br /><br />`Config Param: ENABLE_METADATA_INDEX_COLUMN_STATS`<br />`Since Version: 0.11.0` |
---


### Storage Configs {#Storage-Configs}
Configurations that control aspects around writing, sizing, reading base and log files.




[**Basic Configs**](#Storage-Configs-basic-configs)


| Config Name                                                        | Default              | Description                                                                                                                                                                                                                 |
| ------------------------------------------------------------------ | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.parquet.compression.codec](#hoodieparquetcompressioncodec) | gzip (Optional)      | Compression Codec for parquet files<br /><br />`Config Param: PARQUET_COMPRESSION_CODEC_NAME`                                                                                                                               |
| [hoodie.parquet.max.file.size](#hoodieparquetmaxfilesize)          | 125829120 (Optional) | Target size in bytes for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br />`Config Param: PARQUET_MAX_FILE_SIZE` |
---


### Archival Configs {#Archival-Configs}
Configurations that control archival.




[**Basic Configs**](#Archival-Configs-basic-configs)


| Config Name                                      | Default       | Description                                                                                                                                                                                                                                                                                            |
| ------------------------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.keep.max.commits](#hoodiekeepmaxcommits) | 30 (Optional) | Archiving service moves older entries from timeline into an archived log after each write, to keep the metadata overhead constant, even as the table size grows. This config controls the maximum number of instants to retain in the active timeline. <br /><br />`Config Param: MAX_COMMITS_TO_KEEP` |
| [hoodie.keep.min.commits](#hoodiekeepmincommits) | 20 (Optional) | Similar to hoodie.keep.max.commits, but controls the minimum number of instants to retain in the active timeline.<br /><br />`Config Param: MIN_COMMITS_TO_KEEP`                                                                                                                                       |
---


### Bootstrap Configs {#Bootstrap-Configs}
Configurations that control how you want to bootstrap your existing tables for the first time into hudi. The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing  writers and new hudi writers in parallel, to validate the migration.




[**Basic Configs**](#Bootstrap-Configs-basic-configs)


| Config Name                                            | Default            | Description                                                                                                                             |
| ------------------------------------------------------ | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.bootstrap.base.path](#hoodiebootstrapbasepath) | N/A **(Required)** | Base path of the dataset that needs to be bootstrapped as a Hudi table<br /><br />`Config Param: BASE_PATH`<br />`Since Version: 0.6.0` |
---


### Clean Configs {#Clean-Configs}
Cleaning (reclamation of older/unused file groups/slices).




[**Basic Configs**](#Clean-Configs-basic-configs)


| Config Name                                                      | Default          | Description                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.clean.async](#hoodiecleanasync)                          | false (Optional) | Only applies when hoodie.clean.automatic is turned on. When turned on runs cleaner async with writing, which can speed up overall write performance.<br /><br />`Config Param: ASYNC_CLEAN`                                                                                            |
| [hoodie.cleaner.commits.retained](#hoodiecleanercommitsretained) | 10 (Optional)    | Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br /><br />`Config Param: CLEANER_COMMITS_RETAINED` |
---


### Clustering Configs {#Clustering-Configs}
Configurations that control the clustering table service in hudi, which optimizes the storage layout for better query performance by sorting and sizing data files.




[**Basic Configs**](#Clustering-Configs-basic-configs)


| Config Name                                                                                              | Default               | Description                                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.clustering.async.enabled](#hoodieclusteringasyncenabled)                                         | false (Optional)      | Enable running of clustering service, asynchronously as inserts happen on the table.<br /><br />`Config Param: ASYNC_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                              |
| [hoodie.clustering.inline](#hoodieclusteringinline)                                                      | false (Optional)      | Turn on inline clustering - clustering will be run after each write operation is complete<br /><br />`Config Param: INLINE_CLUSTERING`<br />`Since Version: 0.7.0`                               |
| [hoodie.clustering.plan.strategy.small.file.limit](#hoodieclusteringplanstrategysmallfilelimit)          | 314572800 (Optional)  | Files smaller than the size in bytes specified here are candidates for clustering<br /><br />`Config Param: PLAN_STRATEGY_SMALL_FILE_LIMIT`<br />`Since Version: 0.7.0`                          |
| [hoodie.clustering.plan.strategy.target.file.max.bytes](#hoodieclusteringplanstrategytargetfilemaxbytes) | 1073741824 (Optional) | Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br /><br />`Config Param: PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`<br />`Since Version: 0.7.0` |
---


### Compaction Configs {#Compaction-Configs}
Configurations that control compaction (merging of log files onto a new base files).




[**Basic Configs**](#Compaction-Configs-basic-configs)


| Config Name                                                                    | Default          | Description                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------ | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.compact.inline](#hoodiecompactinline)                                  | false (Optional) | When set to true, compaction service is triggered after each write. While being  simpler operationally, this adds extra latency on the write path.<br /><br />`Config Param: INLINE_COMPACT`                                                                                                                                                              |
| [hoodie.compact.inline.max.delta.commits](#hoodiecompactinlinemaxdeltacommits) | 5 (Optional)     | Number of delta commits after the last compaction, before scheduling of a new compaction is attempted. This config takes effect only for the compaction triggering strategy based on the number of commits, i.e., NUM_COMMITS, NUM_COMMITS_AFTER_LAST_REQUEST, NUM_AND_TIME, and NUM_OR_TIME.<br /><br />`Config Param: INLINE_COMPACT_NUM_DELTA_COMMITS` |
---


### Write Configurations {#Write-Configurations}
Configurations that control write behavior on Hudi tables. These can be directly passed down from even higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g Hudi Streamer).




[**Basic Configs**](#Write-Configurations-basic-configs)


| Config Name                                                                       | Default                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| --------------------------------------------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.base.path](#hoodiebasepath)                                               | N/A **(Required)**       | Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.<br /><br />`Config Param: BASE_PATH`                                                                                                                      |
| [hoodie.table.name](#hoodietablename)                                             | N/A **(Required)**       | Table name that will be used for registering with metastores like HMS. Needs to be same across runs.<br /><br />`Config Param: TBL_NAME`                                                                                                                                                                                                                                                                                                                |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield) | ts (Optional)            | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br /><br />`Config Param: PRECOMBINE_FIELD_NAME`                                                                                                                                                                                                     |
| [hoodie.write.concurrency.mode](#hoodiewriteconcurrencymode)                      | SINGLE_WRITER (Optional) | org.apache.hudi.common.model.WriteConcurrencyMode: Concurrency modes for write operations.     SINGLE_WRITER(default): Only one active writer to the table. Maximizes throughput.     OPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table with lazy conflict resolution using locks. This means that only one writer succeeds if multiple writers write to the same file group.<br /><br />`Config Param: WRITE_CONCURRENCY_MODE` |
---


### Key Generator Configs {#KEY_GENERATOR}
Hudi maintains keys (record key + partition path) for uniquely identifying a particular record. These configs allow developers to setup the Key generator class that extracts these out of incoming records.


#### Key Generator Options {#Key-Generator-Options}





[**Basic Configs**](#Key-Generator-Options-basic-configs)


| Config Name                                                                                      | Default            | Description                                                                                                                                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------ | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.write.partitionpath.field](#hoodiedatasourcewritepartitionpathfield)          | N/A **(Required)** | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value obtained by invoking .toString()<br /><br />`Config Param: PARTITIONPATH_FIELD_NAME`                                                                                                                           |
| [hoodie.datasource.write.recordkey.field](#hoodiedatasourcewriterecordkeyfield)                  | N/A **(Required)** | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br /><br />`Config Param: RECORDKEY_FIELD_NAME`                                     |
| [hoodie.datasource.write.hive_style_partitioning](#hoodiedatasourcewritehive_style_partitioning) | false (Optional)   | Flag to indicate whether to use Hive style partitioning. If set true, the names of partition folders follow &lt;partition_column_name&gt;=&lt;partition_value&gt; format. By default false (the names of partition folders are only partition values)<br /><br />`Config Param: HIVE_STYLE_PARTITIONING_ENABLE` |
---


### Index Configs {#INDEX}
Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.


#### Common Index Configs {#Common-Index-Configs}





[**Basic Configs**](#Common-Index-Configs-basic-configs)


| Config Name                           | Default            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [hoodie.index.type](#hoodieindextype) | N/A **(Required)** | org.apache.hudi.index.HoodieIndex$IndexType: Determines how input records are indexed, i.e., looked up based on the key for the location in the existing table. Default is SIMPLE on Spark engine, and INMEMORY on Flink and Java engines.     HBASE: uses an external managed Apache HBase table to store record key to location mapping. HBase index is a global index, enforcing key uniqueness across all partitions in the table.     INMEMORY: Uses in-memory hashmap in Spark and Java engine and Flink in-memory state in Flink for indexing.     BLOOM: Employs bloom filters built out of the record keys, optionally also pruning candidate files using record key ranges. Key uniqueness is enforced inside partitions.     GLOBAL_BLOOM: Employs bloom filters built out of the record keys, optionally also pruning candidate files using record key ranges. Key uniqueness is enforced across all partitions in the table.      SIMPLE: Performs a lean join of the incoming update/delete records against keys extracted from the table on storage.Key uniqueness is enforced inside partitions.     GLOBAL_SIMPLE: Performs a lean join of the incoming update/delete records against keys extracted from the table on storage.Key uniqueness is enforced across all partitions in the table.     BUCKET: locates the file group containing the record fast by using bucket hashing, particularly beneficial in large scale. Use `hoodie.index.bucket.engine` to choose bucket engine type, i.e., how buckets are generated.     FLINK_STATE: Internal Config for indexing based on Flink state.     RECORD_INDEX: Index which saves the record key to location mappings in the HUDI Metadata Table. Record index is a global index, enforcing key uniqueness across all partitions in the table. Supports sharding to achieve very high scale.<br /><br />`Config Param: INDEX_TYPE` |
---

## Metastore and Catalog Sync Configs {#META_SYNC}
Configurations used by the Hudi to sync metadata to external metastores and catalogs.


### Common Metadata Sync Configs {#Common-Metadata-Sync-Configs}





[**Basic Configs**](#Common-Metadata-Sync-Configs-basic-configs)


| Config Name                                                           | Default          | Description                                                                                                             |
| --------------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false (Optional) | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED` |
---


### BigQuery Sync Configs {#BigQuery-Sync-Configs}
Configurations used by the Hudi to sync metadata to Google BigQuery.




[**Basic Configs**](#BigQuery-Sync-Configs-basic-configs)


| Config Name                                                           | Default          | Description                                                                                                             |
| --------------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false (Optional) | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED` |
---


### Hive Sync Configs {#Hive-Sync-Configs}
Configurations used by the Hudi to sync metadata to Hive Metastore.




[**Basic Configs**](#Hive-Sync-Configs-basic-configs)


| Config Name                                                                           | Default                                 | Description                                                                                                             |
| ------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                    | N/A **(Required)**                      | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br /><br />`Config Param: HIVE_SYNC_MODE`           |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                | false (Optional)                        | When set to true, register/sync the table to Apache Hive metastore.<br /><br />`Config Param: HIVE_SYNC_ENABLED`        |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)              | jdbc:hive2://localhost:10000 (Optional) | Hive metastore url<br /><br />`Config Param: HIVE_URL`                                                                  |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris) | thrift://localhost:9083 (Optional)      | Hive metastore url<br /><br />`Config Param: METASTORE_URIS`                                                            |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                 | false (Optional)                        | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED` |
---


### Global Hive Sync Configs {#Global-Hive-Sync-Configs}
Global replication configurations used by the Hudi to sync metadata to Hive Metastore.




[**Basic Configs**](#Global-Hive-Sync-Configs-basic-configs)


| Config Name                                                                           | Default                                 | Description                                                                                                             |
| ------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.hive_sync.mode](#hoodiedatasourcehive_syncmode)                    | N/A **(Required)**                      | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br /><br />`Config Param: HIVE_SYNC_MODE`           |
| [hoodie.datasource.hive_sync.enable](#hoodiedatasourcehive_syncenable)                | false (Optional)                        | When set to true, register/sync the table to Apache Hive metastore.<br /><br />`Config Param: HIVE_SYNC_ENABLED`        |
| [hoodie.datasource.hive_sync.jdbcurl](#hoodiedatasourcehive_syncjdbcurl)              | jdbc:hive2://localhost:10000 (Optional) | Hive metastore url<br /><br />`Config Param: HIVE_URL`                                                                  |
| [hoodie.datasource.hive_sync.metastore.uris](#hoodiedatasourcehive_syncmetastoreuris) | thrift://localhost:9083 (Optional)      | Hive metastore url<br /><br />`Config Param: METASTORE_URIS`                                                            |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable)                 | false (Optional)                        | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED` |
---


### DataHub Sync Configs {#DataHub-Sync-Configs}
Configurations used by the Hudi to sync metadata to DataHub.




[**Basic Configs**](#DataHub-Sync-Configs-basic-configs)


| Config Name                                                           | Default          | Description                                                                                                             |
| --------------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [hoodie.datasource.meta.sync.enable](#hoodiedatasourcemetasyncenable) | false (Optional) | Enable Syncing the Hudi Table with an external meta store or data catalog.<br /><br />`Config Param: META_SYNC_ENABLED` |
---

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.


### Metrics Configurations {#Metrics-Configurations}
Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.




[**Basic Configs**](#Metrics-Configurations-basic-configs)


| Config Name                                                                   | Default             | Description                                                                                                                                                                        |
| ----------------------------------------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.metrics.on](#hoodiemetricson)                                         | false (Optional)    | Turn on/off metrics reporting. off by default.<br /><br />`Config Param: TURN_METRICS_ON`<br />`Since Version: 0.5.0`                                                              |
| [hoodie.metrics.reporter.type](#hoodiemetricsreportertype)                    | GRAPHITE (Optional) | Type of metrics reporter.<br /><br />`Config Param: METRICS_REPORTER_TYPE_VALUE`<br />`Since Version: 0.5.0`                                                                       |
| [hoodie.metricscompaction.log.blocks.on](#hoodiemetricscompactionlogblockson) | false (Optional)    | Turn on/off metrics reporting for log blocks with compaction commit. off by default.<br /><br />`Config Param: TURN_METRICS_COMPACTION_LOG_BLOCKS_ON`<br />`Since Version: 0.14.0` |
---

## Kafka Connect Configs {#KAFKA_CONNECT}
These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables


### Kafka Sink Connect Configurations {#Kafka-Sink-Connect-Configurations}
Configurations for Kafka Connect Sink Connector for Hudi.




[**Basic Configs**](#Kafka-Sink-Connect-Configurations-basic-configs)


| Config Name                            | Default                   | Description                                                                                     |
| -------------------------------------- | ------------------------- | ----------------------------------------------------------------------------------------------- |
| [bootstrap.servers](#bootstrapservers) | localhost:9092 (Optional) | The bootstrap servers for the Kafka Cluster.<br /><br />`Config Param: KAFKA_BOOTSTRAP_SERVERS` |
---

## Hudi Streamer Configs {#HUDI_STREAMER}
These set of configs are used for Hudi Streamer utility which provides the way to ingest from different sources such as DFS or Kafka.


### Hudi Streamer Configs {#Hudi-Streamer-Configs}





[**Basic Configs**](#Hudi-Streamer-Configs-basic-configs)


| Config Name                                                                           | Default            | Description                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [hoodie.deltastreamer.source.kafka.topic](#hoodiedeltastreamersourcekafkatopic)       | N/A **(Required)** | Kafka topic name. The config is specific to HoodieMultiTableDeltaStreamer<br /><br />`Config Param: KAFKA_TOPIC`                                                                                                                                                                                                                                                      |
| [hoodie.deltastreamer.sample.writes.enabled](#hoodiedeltastreamersamplewritesenabled) | false (Optional)   | Set this to true to sample from the first batch of records and write to the auxiliary path, before writing to the table.The sampled records are used to calculate the average record size. The relevant write client will have `hoodie.copyonwrite.record.size.estimate` being overwritten by the calculated result.<br /><br />`Config Param: SAMPLE_WRITES_ENABLED` |
| [hoodie.deltastreamer.sample.writes.size](#hoodiedeltastreamersamplewritessize)       | 5000 (Optional)    | Number of records to sample from the first write. To improve the estimation's accuracy, for smaller or more compressable record size, set the sample size bigger. For bigger or less compressable record size, set smaller.<br /><br />`Config Param: SAMPLE_WRITES_SIZE`                                                                                             |
---

