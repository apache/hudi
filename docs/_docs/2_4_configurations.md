---
title: Configurations
keywords: garbage collection, hudi, jvm, configs, tuning
permalink: /docs/configurations.html
summary: "Here we list all possible configurations and what they mean"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

This page covers the different ways of configuring your job to write/read Hudi tables. 
At a high level, you can control behaviour at few levels. 

- **[Spark Datasource Configs](#spark-datasource)** : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- **[Flink SQL Configs](#flink-options)** : These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- **[WriteClient Configs](#writeclient-configs)** : Internally, the Hudi datasource uses a RDD based `HoodieWriteClient` api to actually perform writes to storage. These configs provide deep control over lower level aspects like 
   file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- **[RecordPayload Config](#PAYLOAD_CLASS_OPT_KEY)** : This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and 
   stored old record. Hudi provides default implementations such as `OverwriteWithLatestAvroPayload` which simply update table with the latest/last-written record. 
   This can be overridden to a custom class extending `HoodieRecordPayload` class, on both datasource and WriteClient levels.


## Spark Datasource Configs {#spark-datasource}

Spark jobs using the datasource can be configured by passing the below options into the `option(k,v)` method as usual.
The actual datasource level configs are listed below.


### Write Options

Additionally, you can pass down any of the WriteClient level configs directly using `options()` or `option(k,v)` methods.

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

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| TABLE_NAME_OPT_KEY | hoodie.datasource.write.table.name | YES | N/A | Hive table name, to register the table into. |
| OPERATION_OPT_KEY | hoodie.datasource.write.operation | NO | upsert | Whether to do upsert, insert or bulkinsert for the write operation. Use bulkinsert to load new data into a table, and there on use upsert/insert. Bulk insert uses a disk based write path to scale to load large inputs without need to cache it. |
| TABLE_TYPE_OPT_KEY | hoodie.datasource.write.table.type | NO | COPY_ON_WRITE | The table type for the underlying data, for this write. This can’t change between writes. |
| PRECOMBINE_FIELD_OPT_KEY | hoodie.datasource.write.precombine.field | NO | ts | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..) |
| PAYLOAD_CLASS_OPT_KEY | hoodie.datasource.write.payload.class | NO | org.apache.hudi.OverwriteWithLatestAvroPayload | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective |
| RECORDKEY_FIELD_OPT_KEY | hoodie.datasource.write.recordkey.field | NO | uuid | Record key field. Value to be used as the recordKey component of HoodieKey. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: a.b.c |
| PARTITIONPATH_FIELD_OPT_KEY | hoodie.datasource.write.partitionpath.field | NO | partitionpath | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString() |
| HIVE_STYLE_PARTITIONING_OPT_KEY | hoodie.datasource.write.hive_style_partitioning | NO | false | If set true, the names of partition folders follow <partition_column_name>=<partition_value> format. |
| KEYGENERATOR_CLASS_OPT_KEY | hoodie.datasource.write.keygenerator.class | NO | No default | Key generator class, that implements will extract the key out of incoming `Row` object. This config has higher precedence over keygen type, and it is used for user-defined KeyGenerator . |
| KEYGENERATOR_TYPE_OPT_KEY | hoodie.datasource.write.keygenerator.type | NO | SIMPLE | Key generator type, , that indicate which KeyGenerator to use. This is the recommended configuration option for key generator, and has lower priority than `KEYGENERATOR_CLASS_OPT_KEY` .  |
| COMMIT_METADATA_KEYPREFIX_OPT_KEY | hoodie.datasource.write.commitmeta.key.prefix | NO | _ | Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. This is useful to store checkpointing information, in a consistent way with the hudi timeline. |
| INSERT_DROP_DUPS_OPT_KEY | hoodie.datasource.write.insert.drop.duplicates | NO | false | If set to true, filters out all duplicate records from incoming dataframe, during insert operations. |
| ENABLE_ROW_WRITER_OPT_KEY | hoodie.datasource.write.row.writer.enable | NO | false | When set to true, will perform write operations directly using the spark native Row representation. This is expected to be faster by 20 to 30% than regular bulk_insert by setting this config. |
| HIVE_SYNC_ENABLED_OPT_KEY | hoodie.datasource.hive_sync.enable | NO | false | When set to true, register/sync the table to Apache Hive metastore. |
| HIVE_DATABASE_OPT_KEY | hoodie.datasource.hive_sync.database | NO | default | Database to sync to. |
| HIVE_TABLE_OPT_KEY | hoodie.datasource.hive_sync.table | YES | N/A | Table to sync to. |
| HIVE_USER_OPT_KEY | hoodie.datasource.hive_sync.username | NO | hive | Hive user name to use. |
| HIVE_PASS_OPT_KEY | hoodie.datasource.hive_sync.password | NO | hive | Hive password to use. |
| HIVE_URL_OPT_KEY | hoodie.datasource.hive_sync.jdbcurl | NO | jdbc:hive2://localhost:10000 | Hive metastore url. |
| HIVE_PARTITION_FIELDS_OPT_KEY | hoodie.datasource.hive_sync.partition_fields | NO |   | Field in the table to use for determining hive partition columns. |
| HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY | hoodie.datasource.hive_sync.partition_extractor_class | NO | org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor | Class used to extract partition field values into hive partition columns. |
| HIVE_ASSUME_DATE_PARTITION_OPT_KEY | hoodie.datasource.hive_sync.assume_date_partitioning | NO | false | Assume partitioning is yyyy/mm/dd . |
| HIVE_USE_JDBC_OPT_KEY | hoodie.datasource.hive_sync.use_jdbc | NO | true | Use JDBC when hive synchronization is enabled. |
| HIVE_AUTO_CREATE_DATABASE_OPT_KEY | hoodie.datasource.hive_sync.auto_create_database | NO | true | Auto create hive database if does not exists. Note: for versions 0.7 and 0.8 you will have to explicitly set this to true. |
| HIVE_SKIP_RO_SUFFIX | hoodie.datasource.hive_sync.skip_ro_suffix | NO | false | Skip the _ro suffix for Read optimized table, when registering. |
| HIVE_SUPPORT_TIMESTAMP | hoodie.datasource.hive_sync.support_timestamp | NO | false | ‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. Disabled by default for backward compatibility. |

</div>

### Read Options

Options useful for reading tables via `read.format.option(...)`

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| QUERY_TYPE_OPT_KEY | hoodie.datasource.query.type | NO | snapshot | Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on columnar data) (or) Snapshot mode (obtain latest view, based on row & columnar data). |
| BEGIN_INSTANTTIME_OPT_KEY | hoodie.datasource.read.begin.instanttime | Required in incremental mode | N/A | Instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM. |
| END_INSTANTTIME_OPT_KEY | hoodie.datasource.read.end.instanttime | NO | latest instant (i.e fetches all new data since begin instant time) | Instant time to limit incrementally fetched data to. New data written with an instant_time <= END_INSTANTTIME are fetched out. |
| INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY | hoodie.datasource.read.schema.use.end.instanttime | NO | false | Uses end instant schema when incrementally fetched data to. Default: users latest instant schema. |

</div>

## Flink SQL Config Options {#flink-options}

Flink jobs using the SQL can be configured through the options in `WITH` clause.
The actual datasource level configs are listed below.

### Write Options

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `path` | Y | N/A | <span style="color:grey"> Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully </span> |
| `table.type`  | N | COPY_ON_WRITE | <span style="color:grey"> Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ </span> |
| `write.operation` | N | upsert | <span style="color:grey"> The write operation, that this write should do (insert or upsert is supported) </span> |
| `write.precombine.field` | N | ts | <span style="color:grey"> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..) </span> |
| `write.payload.class` | N | OverwriteWithLatestAvroPayload.class | <span style="color:grey"> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective </span> |
| `write.insert.drop.duplicates` | N | false | <span style="color:grey"> Flag to indicate whether to drop duplicates upon insert. By default insert will accept duplicates, to gain extra performance </span> |
| `write.ignore.failed` | N | true | <span style="color:grey"> Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. By default true (in favor of streaming progressing over data integrity) </span> |
| `hoodie.datasource.write.recordkey.field` | N | uuid | <span style="color:grey"> Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c` </span> |
| `hoodie.datasource.write.keygenerator.class` | N | No default | <span style="color:grey"> Key generator class, that implements will extract the key out of incoming `Row` object. This config has higher precedence over keygen type, and it is used for user-defined KeyGenerator </span> |
| `hoodie.datasource.write.keygenerator.type` | N | SIMPLE | <span style="color:grey"> Key generator type, that indicate which KeyGenerator to use. This is the recommended configuration option for key generator, and has lower priority than `KEYGENERATOR_CLASS_OPT_KEY`  </span> |
| `write.partition.url_encode` | N | false | <span style="color:grey"> Whether to encode the partition path url, default false </span> |
| `write.log.max.size` | N | 1024 | <span style="color:grey"> Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB </span> |

</div>

If the table type is MERGE_ON_READ, you can also specify the asynchronous compaction strategy through options:

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `compaction.tasks` | N | 10 | <span style="color:grey"> Parallelism of tasks that do actual compaction, default is 10 </span> |
| `compaction.async.enabled` | N | true | <span style="color:grey"> Async Compaction, enabled by default for MOR </span> |
| `compaction.trigger.strategy` | N | num_commits | <span style="color:grey"> Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits; 'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction; 'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; 'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied. Default is 'num_commits' </span> |
| `compaction.delta_commits` | N | 5 | <span style="color:grey"> Max delta commits needed to trigger compaction, default 5 commits </span> |
| `compaction.delta_seconds` | N | 3600 | <span style="color:grey"> Max delta seconds time needed to trigger compaction, default 1 hour </span> |
| `compaction.max_memory` | N | 100 | <span style="color:grey"> Max memory in MB for compaction spillable map, default 100MB </span> |
| `clean.async.enabled` | N | true | <span style="color:grey"> Whether to cleanup the old commits immediately on new commits, enabled by default </span> |
| `clean.retain_commits` | N | 10 | <span style="color:grey"> Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table, default 10 </span> |

</div>

Options about memory consumption:

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.rate.limit` | N | -1 | <span style="color:grey"> Write records rate limit per second to reduce risk of OOM, default -1 (no limit) </span> |
| `write.batch.size` | N | 64 | <span style="color:grey"> Batch size per bucket in MB to flush data into the underneath filesystem, default 64MB </span> |
| `write.log_block.size` | N | 128 | <span style="color:grey"> Max log block size in MB for log file, default 128MB </span> |
| `compaction.max_memory` | N | 100 | <span style="color:grey"> Max memory in MB for compaction spillable map, default 100MB </span> |

</div>

### Read Options

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `path` | Y | N/A | <span style="color:grey"> Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully </span> |
| `table.type`  | N | COPY_ON_WRITE | <span style="color:grey"> Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ </span> |
| `read.tasks` | N | 4 | <span style="color:grey"> Parallelism of tasks that do actual read, default is 4 </span> |
| `read.avro-schema.path` | N | N/A | <span style="color:grey"> Avro schema file path, the parsed schema is used for deserialization, if not specified, the avro schema is inferred from the table DDL </span> |
| `read.avro-schema` | N | N/A | <span style="color:grey"> Avro schema string, the parsed schema is used for deserialization, if not specified, the avro schema is inferred from the table DDL </span> |
| `hoodie.datasource.query.type` | N | snapshot | <span style="color:grey"> Decides how data files need to be read, in 1) Snapshot mode (obtain latest view, based on row & columnar data); 2) incremental mode (new data since an instantTime), not supported yet; 3) Read Optimized mode (obtain latest view, based on columnar data). Default: snapshot </span> |
| `hoodie.datasource.merge.type` | N | payload_combine | <span style="color:grey"> For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in 1) skip_merge: read the base file records plus the log file records; 2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the log file records(combines the two records with same key for base and log file records), then read the left log file records </span> |
| `hoodie.datasource.hive_style_partition` | N | false | <span style="color:grey"> Whether the partition path is with Hive style, e.g. '{partition key}={partition value}', default false </span> |
| `read.utc-timezone` | N | true | <span style="color:grey"> Use UTC timezone or local timezone to the conversion between epoch time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone, by default true </span> |

</div>

Streaming read is supported through options:

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.streaming.enabled` | N | false | <span style="color:grey"> Whether to read as streaming source, default false </span> |
| `read.streaming.check-interval` | N | 60 | <span style="color:grey"> Check interval for streaming read of SECOND, default 1 minute </span> |
| `read.streaming.start-commit` | N | N/A | <span style="color:grey"> Start commit instant for streaming read, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant </span> |

</div>

### Index sync options

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `index.bootstrap.enabled` | N | false | <span style="color:grey"> Whether to bootstrap the index state from existing hoodie table, default false </span> |
| `index.state.ttl` | N | 1.5 | <span style="color:grey"> Index state ttl in days, default 1.5 day </span> |

</div>

### Hive sync options

<div class="table-wrapper" markdown="block">

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `hive_sync.enable` | N | false | <span style="color:grey"> Asynchronously sync Hive meta to HMS, default false </span> |
| `hive_sync.db` | N | default | <span style="color:grey"> Database name for hive sync, default 'default' </span> |
| `hive_sync.table` | N | unknown | <span style="color:grey"> Table name for hive sync, default 'unknown' </span> |
| `hive_sync.file_format` | N | PARQUET | <span style="color:grey"> File format for hive sync, default 'PARQUET' </span> |
| `hive_sync.username` | N | hive | <span style="color:grey"> Username for hive sync, default 'hive' </span> |
| `hive_sync.password` | N | hive | <span style="color:grey"> Password for hive sync, default 'hive' </span> |
| `hive_sync.jdbc_url` | N | jdbc:hive2://localhost:10000 | <span style="color:grey"> Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000' </span> |
| `hive_sync.partition_fields` | N | '' | <span style="color:grey"> Partition fields for hive sync, default '' </span> |
| `hive_sync.partition_extractor_class` | N | SlashEncodedDayPartitionValueExtractor.class | <span style="color:grey"> Tool to extract the partition value from HDFS path, default 'SlashEncodedDayPartitionValueExtractor' </span> |
| `hive_sync.assume_date_partitioning` | N | false | <span style="color:grey"> Assume partitioning is yyyy/mm/dd, default false </span> |
| `hive_sync.use_jdbc` | N | true | <span style="color:grey"> Use JDBC when hive synchronization is enabled, default true </span> |
| `hive_sync.auto_create_db` | N | true | <span style="color:grey"> Auto create hive database if it does not exists, default true </span> |
| `hive_sync.ignore_exceptions` | N | false | <span style="color:grey"> Ignore exceptions during hive synchronization, default false </span> |
| `hive_sync.skip_ro_suffix` | N | false | <span style="color:grey"> Skip the _ro suffix for Read optimized table when registering, default false </span> |
| `hive_sync.support_timestamp` | N | false | <span style="color:grey"> INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type. Disabled by default for backward compatibility </span> |

</div>

## WriteClient Configs {#writeclient-configs}

Jobs programming directly against the RDD level apis can build a `HoodieWriteConfig` object and pass it in to the `HoodieWriteClient` constructor. 
HoodieWriteConfig can be built using a builder pattern as below. 

```java
HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable(tableName)
        .withSchema(schemaStr)
        .withProps(props) // pass raw k,v pairs from a property file.
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withXXX(...).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withXXX(...).build())
        ...
        .build();
```

Following subsections go over different aspects of write configs, explaining most important configs with their property names, default values.

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withPath(hoodie_base_path) | hoodie.base.path | YES | N/A | Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory. |
| withSchema(schema_str) | hoodie.avro.schema | YES | N/A | This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. |
| forTable(table_name) | hoodie.table.name | YES | N/A | Table name that will be used for registering with Hive. Needs to be same across runs. |
| withBulkInsertParallelism(bulk_insert_parallelism) | hoodie.bulkinsert.shuffle.parallelism | NO | 1500 | Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your table. Tune this to achieve a desired optimal size during initial import. |
| withUserDefinedBulkInsertPartitionerClass(className) | hoodie.bulkinsert.user.defined.partitioner.class | NO | Pattern like x.y.z.UserDefinedPatitionerClass | If specified, this class will be used to re-partition input records before they are inserted. |
| withBulkInsertSortMode(mode) | hoodie.bulkinsert.sort.mode | NO | BulkInsertSortMode.GLOBAL_SORT | Sorting modes to use for sorting records for bulk insert. This is leveraged when user defined partitioner is not configured. Default is GLOBAL_SORT. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches spark.write.parquet() in terms of number of files, overheads. |
| withParallelism(insert_shuffle_parallelism, upsert_shuffle_parallelism) | hoodie.insert.shuffle.parallelism, hoodie.upsert.shuffle.parallelism | NO | insert_shuffle_parallelism = 1500, upsert_shuffle_parallelism = 1500 | Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data. |
| withDeleteParallelism(parallelism) | hoodie.delete.shuffle.parallelism | NO | 1500 | This parallelism is Used for “delete” operation while deduping or repartioning. |
| combineInput(on_insert, on_update) | hoodie.combine.before.insert, hoodie.combine.before.upsert | NO | on_insert = false, on_update=true | Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS. |
| combineDeleteInput(on_Delete) | hoodie.combine.before.delete | NO | true | Flag which first combines the input RDD and merges multiple partial records into a single record before deleting in DFS. |
| withMergeAllowDuplicateOnInserts(mergeAllowDuplicateOnInserts） | hoodie.merge.allow.duplicate.on.inserts | NO | false | When enabled, will route new records as inserts and will not merge with existing records. Result could contain duplicate entries. |
| withWriteStatusStorageLevel(level） | hoodie.write.status.storage.level | NO | MEMORY_AND_DISK_SER | HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD. |
| withAutoCommit(autoCommit） | hoodie.auto.commit | NO | true | Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a “defined success condition”. |
| withConsistencyCheckEnabled(enabled） | hoodie.consistency.check.enabled | NO | false | Should HoodieWriteClient perform additional checks to ensure written files' are listable on the underlying filesystem/storage. Set this to true, to workaround S3's eventual consistency model and ensure all data written as a part of a commit is faithfully available for queries. |
| withRollbackParallelism(rollbackParallelism） | hoodie.rollback.parallelism | NO | 100 | Determine the parallelism for rollback of commits. |
| withRollbackUsingMarkers(rollbackUsingMarkers） | hoodie.rollback.using.markers | NO | false | Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned off by default. |
| withMarkersDeleteParallelism(parallelism） | hoodie.markers.delete.parallelism | NO | 100 | Determines the parallelism for deleting marker files. |

</div>

### Index configs
Following configs control indexing behavior, which tags incoming records as either inserts or updates to older records. 

[withIndexConfig](#index-configs) (HoodieIndexConfig) <br/>
<span style="color:grey">This is pluggable to have a external index (HBase) or use the default bloom filter stored in the Parquet files</span>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withIndexClass(indexClass) | hoodie.index.class | NO | Index class path, like x.y.z.UserDefinedIndex | Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified. |
| withIndexType(indexType) | hoodie.index.type | NO | BLOOM | Type of index to use. Default is Bloom filter. Possible options are [BLOOM, GLOBAL_BLOOM, SIMPLE, GLOBAL_SIMPLE, INMEMORY, HBASE]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files. |

</div>

#### Bloom Index configs

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| bloomIndexFilterType(bucketizedChecking) | hoodie.bloom.index.filter.type | NO | BloomFilterTypeCode.SIMPLE | Filter type used. Default is BloomFilterTypeCode.SIMPLE. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys. |
| bloomFilterNumEntries(numEntries) | hoodie.index.bloom.num_entries | NO | 60000 | Only applies if index type is BLOOM. <br/>This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNNAMIC bloom filter which determines the initial size for the bloom.|
| bloomFilterFPP(fpp) | hoodie.index.bloom.fpp | NO | 0.000000001 | Only applies if index type is BLOOM.Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the congfigured value (hoodie.index.bloom.num_entries), then this fpp may not be honored. |
| bloomIndexParallelism(parallelism) | hoodie.bloom.index.parallelism | NO | 0 | Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics. |
| bloomIndexPruneByRanges(pruneRanges) | hoodie.bloom.index.prune.by.ranges | NO | true | Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off. |
| bloomIndexUseCaching(useCaching) | hoodie.bloom.index.use.caching | NO | true | Only applies if index type is BLOOM. When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions. |
| bloomIndexTreebasedFilter(useTreeFilter) | hoodie.bloom.index.use.treebased.filter | NO | true | When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode. |
| bloomIndexBucketizedChecking(bucketizedChecking) | hoodie.bloom.index.bucketized.checking | NO | true | When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup. |
| bloomIndexFilterDynamicMaxEntries(maxNumberOfEntries) | hoodie.bloom.index.filter.dynamic.max.entries | NO | 100000 | The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0. |
| bloomIndexKeysPerBucket(keysPerBucket) | hoodie.bloom.index.keys.per.bucket | NO | 10000000 | Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. This configuration controls the “bucket” size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory. |
| withBloomIndexInputStorageLevel(level) | hoodie.bloom.index.input.storage.level | NO | MEMORY_AND_DISK_SER | Only applies when bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values. |
| bloomIndexUpdatePartitionPath(updatePartitionPath) | hoodie.bloom.index.update.partition.path | NO | false | Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition. |

</div>

#### HBase Index configs

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| hbaseZkQuorum(zkString) | hoodie.index.hbase.zkquorum | YES | N/A | Only applies if index type is HBASE. HBase ZK Quorum url to connect to. |
| hbaseZkPort(port) | hoodie.index.hbase.zkport | YES | N/A | Only applies if index type is HBASE. HBase ZK Quorum port to connect to. |
| hbaseZkZnodeParent(zkZnodeParent) | hoodie.index.hbase.zknode.path | YES | N/A | Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase. |
| hbaseTableName(tableName) | hoodie.index.hbase.table | YES | N/A | Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table. |
| hbaseIndexUpdatePartitionPath(updatePartitionPath) | hoodie.hbase.index.update.partition.path | NO | false | Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set true, will delete old record in old paritition and will insert it as new record in new partition. |

</div>

#### Simple Index configs

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| simpleIndexUseCaching(useCaching) | hoodie.simple.index.use.caching | NO | true | Only applies if index type is SIMPLE. When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions. |
| withSimpleIndexInputStorageLevel(level) | hoodie.simple.index.input.storage.level | NO | true | Only applies when simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values. |
| withSimpleIndexParallelism(parallelism) | hoodie.simple.index.parallelism | NO | 50 | Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle. |
| withGlobalSimpleIndexParallelism(parallelism) | hoodie.global.simple.index.parallelism | NO | 100 | Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle. |

</div>

### Storage configs
Controls aspects around sizing parquet and log files.

[withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| limitFileSize(size) | hoodie.parquet.max.file.size | NO | 125829120(120MB) | Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance. |
| parquetBlockSize(rowgroupsize) | hoodie.parquet.block.size | NO | 125829120(120MB) | Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk. |
| parquetPageSize(pagesize) | hoodie.parquet.page.size | NO | 1048576(1MB) | Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately. |
| parquetCompressionRatio(parquetCompressionRatio) | hoodie.parquet.compression.ratio | NO | 0.1 | Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files. |
| parquetCompressionCodec(parquetCompressionCodec) | hoodie.parquet.compression.codec | NO | gzip | Parquet compression codec name. Default is gzip. Possible options are [gzip, snappy, uncompressed, lzo]. |
| logFileMaxSize(logFileSize) | hoodie.logfile.max.size | NO | 1073741824(1GB) | LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version. |
| logFileDataBlockMaxSize(dataBlockSize) | hoodie.logfile.data.block.max.size | NO | 268435456(256MB) | LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory. |
| logFileToParquetCompressionRatio(logFileToParquetCompressionRatio) | hoodie.logfile.to.parquet.compression.ratio | NO | 0.35 | Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file. |

</div>

### Compaction configs
Configs that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).
[withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withCleanerPolicy(policy) | hoodie.cleaner.policy | NO | KEEP_LATEST_COMMITS | Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time. |
| withFailedWritesCleaningPolicy(policy) | hoodie.cleaner.policy.failed.writes | NO | HoodieFailedWritesCleaningPolicy.EAGER | Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers) |
| retainCommits(num_of_commits_to_retain) | hoodie.cleaner.commits.retained | NO | 24 | Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers) |
| withAutoClean(autoClean) | hoodie.clean.automatic | NO | true | Should cleanup if there is anything to cleanup immediately after the commit |
| withAsyncClean(asyncClean) | hoodie.clean.async | NO | false | Only applies when withAutoClean is turned on. When true, turned on cleaner async with writing. |
| archiveCommitsWith(minCommits, maxCommits) | hoodie.keep.min.commits, hoodie.keep.max.commits | NO | hoodie.keep.min.commits = 96, hoodie.keep.max.commits = 128 | Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file. |
| withCommitsArchivalBatchSize(batch) | hoodie.commits.archival.batch | NO | 10 | This controls the number of commit instants read in memory as a batch and archived together. |
| compactionSmallFileSize(size) | hoodie.parquet.small.file.limit | NO | 104857600(100MB) | This should be less < maxFileSize and setting it to 0, turns off this feature. Small files can always happen because of the number of insert records in a partition in a batch. Hudi has an option to auto-resolve small files by masking inserts into this partition as updates to existing small files. The size here is the minimum file size considered as a “small file size”. |
| insertSplitSize(size) | hoodie.copyonwrite.insert.split.size | NO | 500000 | Insert Write Parallelism. Number of inserts grouped for a single partition. Writing out 100MB files, with atleast 1kb records, means 100K records per file. Default is to overprovision to 500K. To improve insert latency, tune this to match the number of records in a single file. Setting this to a low number, will result in small files (particularly when compactionSmallFileSize is 0). |
| autoTuneInsertSplits(autoSplit) | hoodie.copyonwrite.insert.auto.split | NO | true | Should hudi dynamically compute the insertSplitSize based on the last 24 commit’s metadata. Turned on by default. |
| approxRecordSize(size) | hoodie.copyonwrite.record.size.estimate | NO | 1024 | The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit’s metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above. |
| withInlineCompaction(inlineCompaction) | hoodie.compact.inline | NO | false | When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert. |
| withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction) | hoodie.compact.inline.max.delta.commits | NO | 10 | Number of max delta commits to keep before triggering an inline compaction. |
| withCompactionLazyBlockReadEnabled(CompactionLazyBlockRead) | hoodie.compaction.lazy.block.read | NO | true | When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage). |
| withCompactionReverseLogReadEnabled(CompactionReverseLog) | hoodie.compaction.reverse.log.read | NO | false | HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0. |
| withCleanerParallelism(cleanerParallelism) | hoodie.cleaner.parallelism | NO | 200 | Increase this if cleaning becomes slow. |
| withCompactionStrategy(compactionStrategy) | hoodie.compaction.strategy | NO | org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy | Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged dataAmount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode. |
| withTargetIOPerCompactionInMB(targetIOPerCompactionInMB) | hoodie.compaction.target.io | NO | 500000 | Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode. |
| withTargetPartitionsPerDayBasedCompaction(targetPartitionsPerCompaction) | hoodie.compaction.daybased.target | NO | 10 | Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run. |
| withPayloadClass(payloadClassName) | hoodie.compaction.payload.class | NO | org.apache.hudi.common.model.HoodieAvroPayload | This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction. |

</div>

### Bootstrap Configs
Controls bootstrap related configs. If you want to bootstrap your data for the first time into hudi, this bootstrap operation will come in handy as you don't need to wait for entire data to be loaded into hudi to start leveraging hudi. 

[withBootstrapConfig](#withBootstrapConfig) (HoodieBootstrapConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withBootstrapBasePath(basePath) | hoodie.bootstrap.base.path | YES | N/A | Base path of the dataset that needs to be bootstrapped as a Hudi table. |
| withBootstrapParallelism(parallelism) | hoodie.bootstrap.parallelism | NO | 1500 | Parallelism value to be used to bootstrap data into hudi. |
| withBootstrapKeyGenClass(keyGenClass)) | hoodie.bootstrap.keygen.class | NO | N/A | Key generator implementation to be used for generating keys from the bootstrapped dataset. |
| withBootstrapKeyGenTYpe(keyGenType)) | hoodie.bootstrap.keygen.type | NO | SIMPLE | Key generator type, indicating which KeyGenerator to be used for generating keys from the bootstrapped dataset. |
| withBootstrapModeSelector(partitionSelectorClass)) | hoodie.bootstrap.mode.selector | NO | org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector | Bootstap Mode Selector class. By default, Hudi employs METADATA_ONLY boostrap for all partitions. |
| withBootstrapPartitionPathTranslatorClass(partitionPathTranslatorClass) | hoodie.bootstrap.partitionpath.translator.class | NO | org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector | For METADATA_ONLY bootstrap, this class allows customization of partition paths used in Hudi target dataset. By default, no customization is done and the partition paths reflects what is available in source parquet table. |
| withFullBootstrapInputProvider(partitionSelectorClass) | hoodie.bootstrap.full.input.provider | NO | org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider | For FULL_RECORD bootstrap, this class use for reading the bootstrap dataset partitions/files and provides the input RDD of Hudi records to write. |
| withBootstrapModeSelectorRegex(regex) | hoodie.bootstrap.mode.selector.regex | NO | .* | Partition Regex used when hoodie.bootstrap.mode.selector set to BootstrapRegexModeSelector. Matches each bootstrap dataset partition against this regex and applies the mode below to it. |
| withBootstrapModeForRegexMatch(modeForRegexMatch) | hoodie.bootstrap.mode.selector.regex.mode | NO | org.apache.hudi.client.bootstrap.METADATA_ONLY | Bootstrap Mode used when the partition matches the regex pattern in hoodie.bootstrap.mode.selector.regex . Used only when hoodie.bootstrap.mode.selector set to BootstrapRegexModeSelector. METADATA_ONLY will generate just skeleton base files with key

</div>

### Metadata Config
Configurations used by the HUDI Metadata Table. This table maintains the meta information stored in hudi dataset so that listing can be avoided during queries. 

[withMetadataConfig](#withMetadataConfig) (HoodieMetadataConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| enable(enable) | hoodie.metadata.enable | NO | false | Enable the internal Metadata Table which stores table level metadata such as file listings. |
| enableReuse(enable) | hoodie.metadata.reuse.enable | NO | true | Enable reusing of opened file handles/merged logs, across multiple fetches from metadata table. |
| enableFallback(enable) | hoodie.metadata.fallback.enable | NO | true | Fallback to listing from DFS, if there are any errors in fetching from metadata table. |
| validate(validate) | hoodie.metadata.validate | NO | false | Validate contents of Metadata Table on each access against the actual listings from DFS. |
| withInsertParallelism(parallelism) | hoodie.metadata.insert.parallelism | NO | 1 | Parallelism to use when writing to the metadata table. |
| withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction) | hoodie.metadata.compact.max.delta.commits | NO | 24 | Controls how often the metadata table is compacted. |
| archiveCommitsWith(minToKeep, maxToKeep) | hoodie.metadata.keep.min.commits, hoodie.metadata.keep.max.commits | NO | minToKeep = 20, maxToKeep = 30 | Controls the archival of the metadata table’s timeline. |
| withAssumeDatePartitioning(assumeDatePartitioning) | hoodie.assume.date.partitioning | NO | false | Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually. |

</div>

### Clustering Configs
Controls clustering operations in hudi. Each clustering has to be configured for its strategy, and config params. This config drives the same. 

[withClusteringConfig](#withClusteringConfig) (HoodieClusteringConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withClusteringPlanStrategyClass(clusteringStrategyClass) | hoodie.clustering.plan.strategy.class | NO | org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy | Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy. |
| withClusteringExecutionStrategyClass(runClusteringStrategyClass) | hoodie.clustering.execution.strategy.class | NO | org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy | Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy. |
| withClusteringTargetPartitions(clusteringTargetPartitions) | hoodie.clustering.plan.strategy.daybased.lookback.partitions | NO | 2 | Number of partitions to list to create ClusteringPlan. |
| withClusteringPlanSmallFileLimit(clusteringSmallFileLimit) | hoodie.clustering.plan.strategy.small.file.limit | NO | 629145600(600Mb) | Files smaller than the size specified here are candidates for clustering. |
| withClusteringMaxBytesInGroup(clusteringMaxGroupSize) | hoodie.clustering.plan.strategy.max.bytes.per.group | NO | 2147483648(2Gb) | Max amount of data to be included in one group. Each clustering operation can create multiple groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). |
| withClusteringMaxNumGroups(maxNumGroups) | hoodie.clustering.plan.strategy.max.num.groups | NO | 30 | Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism. |
| withClusteringTargetFileMaxBytes(targetFileSize) | hoodie.clustering.plan.strategy.target.file.max.bytes | NO | 1073741824(1Gb) | Each group can produce ‘N’ (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups. |

</div>

### Payload Configs
Payload related configs. This config can be leveraged by payload implementations to determine their business logic. 

[withPayloadConfig](#withPayloadConfig) (HoodiePayloadConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withPayloadOrderingField(payloadOrderingField) | hoodie.payload.ordering.field | NO | ts | Property to hold the payload ordering field name. |

</div>

### Metrics configs

Enables reporting on Hudi metrics.
[withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
<span style="color:grey">Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.</span>

#### GRAPHITE

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| on(metricsOn) | hoodie.metrics.on | NO | false | Turn on/off metrics reporting. off by default. |
| withReporterType(reporterType) | hoodie.metrics.reporter.type | NO | GRAPHITE | Type of metrics reporter. |
| toGraphiteHost(host) | hoodie.metrics.graphite.host | NO | localhost | Graphite host to connect to. |
| onGraphitePort(port) | hoodie.metrics.graphite.port | NO | 4756 | Graphite port to connect to. |
| usePrefix(prefix) | hoodie.metrics.graphite.metric.prefix | NO | "" | Standard prefix applied to all metrics. This helps to add datacenter, environment information |

</div>

#### JMX

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| on(metricsOn) | hoodie.metrics.on | NO | false | Turn on/off metrics reporting. off by default. |
| withReporterType(reporterType) | hoodie.metrics.reporter.type | NO | Here use JMX to enable JMX reporter. | Type of metrics reporter. |
| toJmxHost(host) | hoodie.metrics.jmx.host | NO | localhost | Jmx host to connect to. |
| onJmxPort(port) | hoodie.metrics.jmx.port | NO | 9889 | Jmx port to connect to. |

</div>

#### DATADOG

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| on(metricsOn) | hoodie.metrics.on | NO | false | Turn on/off metrics reporting. off by default. |
| withReporterType(reporterType) | hoodie.metrics.reporter.type | NO | Here use DATADOG to enable DATADOG reporter. | Type of metrics reporter. |
| withDatadogReportPeriodSeconds(period) | hoodie.metrics.datadog.report.period.seconds | NO | 30 | Datadog report period in seconds. Default to 30. |
| withDatadogApiSite(apiSite) | hoodie.metrics.datadog.api.site | YES | N/A | Choose EU or US. Datadog API site: EU or US |
| withDatadogApiKeySkipValidation(skip) | hoodie.metrics.datadog.api.key.skip.validation | NO | false | Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false. |
| withDatadogApiKey(apiKey) | hoodie.metrics.datadog.api.key | YES if apiKeySupplier is not set | N/A | Datadog API key. |
| withDatadogApiKeySupplier(apiKeySupplier) | hoodie.metrics.datadog.api.key.supplier | YES if apiKey is not set  | N/A | Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set. |
| withDatadogApiTimeoutSeconds(timeout) | hoodie.metrics.datadog.api.timeout.seconds | NO | 3 | Datadog API timeout in seconds. Default to 3. |
| withDatadogPrefix(prefix) | hoodie.metrics.datadog.metric.prefix | NO |  | Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended. |
| withDatadogHost(host) | hoodie.metrics.datadog.metric.host | NO |  | Datadog metric host to be sent along with metrics data. |
| withDatadogTags(tags) | hoodie.metrics.datadog.metric.tags | NO |  | Datadog metric tags (comma-delimited) to be sent along with metrics data. |

</div>

#### USER DEFINED REPORTER

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| on(metricsOn) | hoodie.metrics.on | NO | false | Turn on/off metrics reporting. off by default. |
| withReporterClass(className) | hoodie.metrics.reporter.class | NO | "" | User-defined class used to report metrics, must be a subclass of AbstractUserDefinedMetricsReporter. |

</div>


### Memory configs
Controls memory usage for compaction and merges, performed internally by Hudi
[withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
<span style="color:grey">Memory related configs</span>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withMaxMemoryFractionPerPartitionMerge(maxMemoryFractionPerPartitionMerge) | hoodie.memory.merge.fraction | NO | 0.6 | This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge. |
| withMaxMemorySizePerCompactionInBytes(maxMemorySizePerCompactionInBytes) | hoodie.memory.compaction.fraction | NO | 1073741824(1Gb) | HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map. |
| withWriteStatusFailureFraction(failureFraction) | hoodie.memory.writestatus.failure.fraction | NO | 0.1 | This property controls what fraction of the failed record, exceptions we report back to driver. |

</div>

### Write commit callback configs
Controls callback behavior on write commit. Exception will be thrown if user enabled the callback service and errors occurred during the process of callback. Currently support HTTP, Kafka type. 
[withCallbackConfig](#withCallbackConfig) (HoodieWriteCommitCallbackConfig) <br/>
<span style="color:grey">Callback related configs</span>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| writeCommitCallbackOn(callbackOn) | hoodie.write.commit.callback.on | NO | false | Turn callback on/off. off by default. |
| withCallbackClass(callbackClass) | hoodie.write.commit.callback.class | NO | org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback | Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default. |

</div>

#### HTTP CALLBACK
Callback via HTTP, User does not need to specify this way explicitly, it is the default type.

##### withCallbackHttpUrl(url) {#withCallbackHttpUrl} 

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withCallbackHttpUrl(url) | hoodie.write.commit.callback.http.url | YES | N/A | Callback host to be sent along with callback messages. |
| withCallbackHttpTimeoutSeconds(timeoutSeconds) | hoodie.write.commit.callback.http.timeout.seconds | NO | 3 | Callback timeout in seconds. 3 by default. |
| withCallbackHttpApiKey(apiKey) | hoodie.write.commit.callback.http.api.key | NO | hudi_write_commit_http_callback | Http callback API key. hudi_write_commit_http_callback by default. |

</div>

#### KAFKA CALLBACK
To use kafka callback, User should set `hoodie.write.commit.callback.class` = `org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback`

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| CALLBACK_KAFKA_BOOTSTRAP_SERVERS | hoodie.write.commit.callback.kafka.bootstrap.servers | YES | N/A | Bootstrap servers of kafka callback cluster. |
| CALLBACK_KAFKA_TOPIC | hoodie.write.commit.callback.kafka.topic | YES | N/A | Kafka topic to be sent along with callback messages. |
| CALLBACK_KAFKA_PARTITION | hoodie.write.commit.callback.kafka.partition | NO | 0 | partition of CALLBACK_KAFKA_TOPIC, 0 by default. |
| CALLBACK_KAFKA_ACKS | hoodie.write.commit.callback.kafka.acks | NO | All | kafka acks level, all by default. |
| CALLBACK_KAFKA_RETRIES | hoodie.write.commit.callback.kafka.retries | NO | 3 | Times to retry. 3 by default. |

</div>

### Locking configs
Configs that control locking mechanisms if [WriteConcurrencyMode=optimistic_concurrency_control](#WriteConcurrencyMode) is enabled
[withLockConfig](#withLockConfig) (HoodieLockConfig) <br/>

<div class="table-wrapper" markdown="block">

|  Option Name  | Property | Required | Default | Remarks |
|  -----------  | -------- | -------- | ------- | ------- |
| withLockProvider(lockProvider) | hoodie.write.lock.provider | NO | org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider | Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider. |
| withZkQuorum(zkQuorum) | hoodie.write.lock.provider | NO |  | Set the list of comma separated servers to connect to. |
| withZkBasePath(zkBasePath) | hoodie.write.lock.zookeeper.base_path | YES | N/A | The base path on Zookeeper under which to create a ZNode to acquire the lock. This should be common for all jobs writing to the same table. |
| withZkPort(zkPort) | hoodie.write.lock.zookeeper.port | YES | N/A | The connection port to be used for Zookeeper. |
| withZkLockKey(zkLockKey) | hoodie.write.lock.zookeeper.lock_key | YES | N/A | Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. We recommend setting this to the table name. |
| withZkConnectionTimeoutInMs(connectionTimeoutInMs) | hoodie.write.lock.zookeeper.connection_timeout_ms | NO | 15000 | How long to wait when connecting to ZooKeeper before considering the connection a failure. |
| withZkSessionTimeoutInMs(sessionTimeoutInMs) | hoodie.write.lock.zookeeper.session_timeout_ms | NO | 60000 | How long to wait after losing a connection to ZooKeeper before the session is expired. |
| withNumRetries(num_retries) | hoodie.write.lock.num_retries | NO | 3 | Maximum number of times to retry by lock provider client. |
| withRetryWaitTimeInMillis(retryWaitTimeInMillis) | hoodie.write.lock.wait_time_ms_between_retry | NO | 5000 | Initial amount of time to wait between retries by lock provider client. |
| withHiveDatabaseName(hiveDatabaseName) | hoodie.write.lock.hivemetastore.database | YES | N/A | The Hive database to acquire lock against. |
| withHiveTableName(hiveTableName) | hoodie.write.lock.hivemetastore.table | YES | N/A | The Hive table under the hive database to acquire lock against. |
| withClientNumRetries(clientNumRetries) | hoodie.write.lock.client.num_retries | NO | 0 | Maximum number of times to retry to acquire lock additionally from the hudi client. |
| withRetryWaitTimeInMillis(retryWaitTimeInMillis) | hoodie.write.lock.client.wait_time_ms_between_retry | NO | 10000 | Amount of time to wait between retries from the hudi client. |
| withConflictResolutionStrategy(lockProvider) | hoodie.write.lock.conflict.resolution.strategy | NO | org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy | Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy. |

</div>
