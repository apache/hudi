---
version: 0.8.0
title: Configurations
keywords: [ garbage collection, hudi, jvm, configs, tuning]
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


## Spark Datasource Configs

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

#### TABLE_NAME_OPT_KEY
  Property: `hoodie.datasource.write.table.name` [Required]<br/>
  <span >Hive table name, to register the table into.</span>
  
#### OPERATION_OPT_KEY
  Property: `hoodie.datasource.write.operation`, Default: `upsert`<br/>
  <span >whether to do upsert, insert or bulkinsert for the write operation. Use `bulkinsert` to load new data into a table, and there on use `upsert`/`insert`. 
  bulk insert uses a disk based write path to scale to load large inputs without need to cache it.</span>
  
#### TABLE_TYPE_OPT_KEY
  Property: `hoodie.datasource.write.table.type`, Default: `COPY_ON_WRITE` <br/>
  <span >The table type for the underlying data, for this write. This can't change between writes.</span>
  
#### PRECOMBINE_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.precombine.field`, Default: `ts` <br/>
  <span >Field used in preCombining before actual write. When two records have the same key value,
we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)</span>

#### PAYLOAD_CLASS_OPT_KEY
  Property: `hoodie.datasource.write.payload.class`, Default: `org.apache.hudi.OverwriteWithLatestAvroPayload` <br/>
  <span >Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. 
  This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective</span>
  
#### RECORDKEY_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.recordkey.field`, Default: `uuid` <br/>
  <span >Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`</span>

#### PARTITIONPATH_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.partitionpath.field`, Default: `partitionpath` <br/>
  <span >Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
Actual value ontained by invoking .toString()</span>

#### HIVE_STYLE_PARTITIONING_OPT_KEY
  Property: `hoodie.datasource.write.hive_style_partitioning`, Default: `false` <br/>
  <span >When set to true, partition folder names follow the format of Hive partitions: [partition_column_name]=[partition_value]</span>

#### KEYGENERATOR_CLASS_OPT_KEY
  Property: `hoodie.datasource.write.keygenerator.class`, Default: `org.apache.hudi.keygen.SimpleKeyGenerator` <br/>
  <span >Key generator class, that implements will extract the key out of incoming `Row` object</span>
  
#### COMMIT_METADATA_KEYPREFIX_OPT_KEY
  Property: `hoodie.datasource.write.commitmeta.key.prefix`, Default: `_` <br/>
  <span >Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
This is useful to store checkpointing information, in a consistent way with the hudi timeline</span>

#### INSERT_DROP_DUPS_OPT_KEY
  Property: `hoodie.datasource.write.insert.drop.duplicates`, Default: `false` <br/>
  <span >If set to true, filters out all duplicate records from incoming dataframe, during insert operations. </span>

#### ENABLE_ROW_WRITER_OPT_KEY
Property: `hoodie.datasource.write.row.writer.enable`, Default: `false` <br/>
<span >When set to true, will perform write operations directly using the spark native `Row`
representation. This is expected to be faster by 20 to 30% than regular bulk_insert by setting this config</span>

#### HIVE_SYNC_ENABLED_OPT_KEY
  Property: `hoodie.datasource.hive_sync.enable`, Default: `false` <br/>
  <span >When set to true, register/sync the table to Apache Hive metastore</span>
  
#### HIVE_DATABASE_OPT_KEY
  Property: `hoodie.datasource.hive_sync.database`, Default: `default` <br/>
  <span >database to sync to</span>
  
#### HIVE_TABLE_OPT_KEY
  Property: `hoodie.datasource.hive_sync.table`, [Required] <br/>
  <span >table to sync to</span>
  
#### HIVE_USER_OPT_KEY
  Property: `hoodie.datasource.hive_sync.username`, Default: `hive` <br/>
  <span >hive user name to use</span>
  
#### HIVE_PASS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.password`, Default: `hive` <br/>
  <span >hive password to use</span>
  
#### HIVE_URL_OPT_KEY
  Property: `hoodie.datasource.hive_sync.jdbcurl`, Default: `jdbc:hive2://localhost:10000` <br/>
  <span >Hive metastore url</span>
  
#### HIVE_PARTITION_FIELDS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.partition_fields`, Default: ` ` <br/>
  <span >field in the table to use for determining hive partition columns.</span>
  
#### HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.partition_extractor_class`, Default: `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor` <br/>
  <span >Class used to extract partition field values into hive partition columns.</span>
  
#### HIVE_ASSUME_DATE_PARTITION_OPT_KEY
  Property: `hoodie.datasource.hive_sync.assume_date_partitioning`, Default: `false` <br/>
  <span >Assume partitioning is yyyy/mm/dd</span>
  
#### HIVE_USE_JDBC_OPT_KEY
  Property: `hoodie.datasource.hive_sync.use_jdbc`, Default: `true` <br/>
  <span >Use JDBC when hive synchronization is enabled</span>

#### HIVE_AUTO_CREATE_DATABASE_OPT_KEY
Property: `hoodie.datasource.hive_sync.auto_create_database` Default: `true` <br/>
<span > Auto create hive database if does not exists </span>

#### HIVE_SKIP_RO_SUFFIX
Property: `hoodie.datasource.hive_sync.skip_ro_suffix` Default: `false` <br/>
<span > Skip the `_ro` suffix for Read optimized table, when registering</span>

#### HIVE_SUPPORT_TIMESTAMP
Property: `hoodie.datasource.hive_sync.support_timestamp` Default: `false` <br/>
<span > 'INT64' with original type TIMESTAMP_MICROS is converted to hive 'timestamp' type. Disabled by default for backward compatibility. </span>

### Read Options

Options useful for reading tables via `read.format.option(...)`

#### QUERY_TYPE_OPT_KEY
Property: `hoodie.datasource.query.type`, Default: `snapshot` <br/>
<span >Whether data needs to be read, in incremental mode (new data since an instantTime)
(or) Read Optimized mode (obtain latest view, based on columnar data)
(or) Snapshot mode (obtain latest view, based on row & columnar data)</span>

#### BEGIN_INSTANTTIME_OPT_KEY 
Property: `hoodie.datasource.read.begin.instanttime`, [Required in incremental mode] <br/>
<span >Instant time to start incrementally pulling data from. The instanttime here need not
necessarily correspond to an instant on the timeline. New data written with an
 `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
 all new data written after Sep 1, 2017 08:00AM.</span>
 
#### END_INSTANTTIME_OPT_KEY
Property: `hoodie.datasource.read.end.instanttime`, Default: latest instant (i.e fetches all new data since begin instant time) <br/>
<span > Instant time to limit incrementally fetched data to. New data written with an
`instant_time &lt;= END_INSTANTTIME` are fetched out.</span>

#### INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY
Property: `hoodie.datasource.read.schema.use.end.instanttime`, Default: false <br/>
<span > Uses end instant schema when incrementally fetched data to. Default: users latest instant schema. </span>

## Flink SQL Config Options

Flink jobs using the SQL can be configured through the options in `WITH` clause.
The actual datasource level configs are listed below.

### Write Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `path` | Y | N/A | <span > Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully </span> |
| `table.type`  | N | COPY_ON_WRITE | <span > Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ </span> |
| `write.operation` | N | upsert | <span > The write operation, that this write should do (insert or upsert is supported) </span> |
| `write.precombine.field` | N | ts | <span > Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..) </span> |
| `write.payload.class` | N | OverwriteWithLatestAvroPayload.class | <span > Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective </span> |
| `write.insert.drop.duplicates` | N | false | <span > Flag to indicate whether to drop duplicates upon insert. By default insert will accept duplicates, to gain extra performance </span> |
| `write.ignore.failed` | N | true | <span > Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. By default true (in favor of streaming progressing over data integrity) </span> |
| `hoodie.datasource.write.recordkey.field` | N | uuid | <span > Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c` </span> |
| `hoodie.datasource.write.keygenerator.class` | N | SimpleAvroKeyGenerator.class | <span > Key generator class, that implements will extract the key out of incoming record </span> |
| `write.tasks` | N | 4 | <span > Parallelism of tasks that do actual write, default is 4 </span> |
| `write.batch.size.MB` | N | 128 | <span > Batch buffer size in MB to flush data into the underneath filesystem </span> |

If the table type is MERGE_ON_READ, you can also specify the asynchronous compaction strategy through options:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `compaction.async.enabled` | N | true | <span > Async Compaction, enabled by default for MOR </span> |
| `compaction.trigger.strategy` | N | num_commits | <span > Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits; 'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction; 'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; 'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied. Default is 'num_commits' </span> |
| `compaction.delta_commits` | N | 5 | <span > Max delta commits needed to trigger compaction, default 5 commits </span> |
| `compaction.delta_seconds` | N | 3600 | <span > Max delta seconds time needed to trigger compaction, default 1 hour </span> |

### Read Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `path` | Y | N/A | <span > Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully </span> |
| `table.type`  | N | COPY_ON_WRITE | <span > Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ </span> |
| `read.tasks` | N | 4 | <span > Parallelism of tasks that do actual read, default is 4 </span> |
| `read.avro-schema.path` | N | N/A | <span > Avro schema file path, the parsed schema is used for deserialization, if not specified, the avro schema is inferred from the table DDL </span> |
| `read.avro-schema` | N | N/A | <span > Avro schema string, the parsed schema is used for deserialization, if not specified, the avro schema is inferred from the table DDL </span> |
| `hoodie.datasource.query.type` | N | snapshot | <span > Decides how data files need to be read, in 1) Snapshot mode (obtain latest view, based on row & columnar data); 2) incremental mode (new data since an instantTime), not supported yet; 3) Read Optimized mode (obtain latest view, based on columnar data). Default: snapshot </span> |
| `hoodie.datasource.merge.type` | N | payload_combine | <span > For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in 1) skip_merge: read the base file records plus the log file records; 2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the log file records(combines the two records with same key for base and log file records), then read the left log file records </span> |
| `hoodie.datasource.hive_style_partition` | N | false | <span > Whether the partition path is with Hive style, e.g. '{partition key}={partition value}', default false </span> |
| `read.utc-timezone` | N | true | <span > Use UTC timezone or local timezone to the conversion between epoch time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone, by default true </span> |

If the table type is MERGE_ON_READ, streaming read is supported through options:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.streaming.enabled` | N | false | <span > Whether to read as streaming source, default false </span> |
| `read.streaming.check-interval` | N | 60 | <span > Check interval for streaming read of SECOND, default 1 minute </span> |
| `read.streaming.start-commit` | N | N/A | <span > Start commit instant for streaming read, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant </span> |

## WriteClient Configs

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

#### withPath(hoodie_base_path)
Property: `hoodie.base.path` [Required] <br/>
<span >Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory. </span>

#### withSchema(schema_str) 
Property: `hoodie.avro.schema` [Required]<br/>
<span >This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. </span>

#### forTable(table_name) 
Property: `hoodie.table.name` [Required] <br/>
 <span >Table name that will be used for registering with Hive. Needs to be same across runs.</span>

#### withBulkInsertParallelism(bulk_insert_parallelism = 1500) 
Property: `hoodie.bulkinsert.shuffle.parallelism`<br/>
<span >Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your table. Tune this to achieve a desired optimal size during initial import.</span>

#### withUserDefinedBulkInsertPartitionerClass(className = x.y.z.UserDefinedPatitionerClass) 
Property: `hoodie.bulkinsert.user.defined.partitioner.class`<br/>
<span >If specified, this class will be used to re-partition input records before they are inserted.</span>

#### withBulkInsertSortMode(mode = BulkInsertSortMode.GLOBAL_SORT) 
Property: `hoodie.bulkinsert.sort.mode`<br/>
<span >Sorting modes to use for sorting records for bulk insert. This is leveraged when user defined partitioner is not configured. Default is GLOBAL_SORT. 
   Available values are - **GLOBAL_SORT**:  this ensures best file sizes, with lowest memory overhead at cost of sorting. 
  **PARTITION_SORT**: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. 
  **NONE**: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads 
</span>

#### withParallelism(insert_shuffle_parallelism = 1500, upsert_shuffle_parallelism = 1500) 
Property: `hoodie.insert.shuffle.parallelism`, `hoodie.upsert.shuffle.parallelism`<br/>
<span >Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data</span>

#### withDeleteParallelism(parallelism = 1500)
Property: `hoodie.delete.shuffle.parallelism`<br/>
<span >This parallelism is Used for "delete" operation while deduping or repartioning. </span>

#### combineInput(on_insert = false, on_update=true) 
Property: `hoodie.combine.before.insert`, `hoodie.combine.before.upsert`<br/>
<span >Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS</span>

#### combineDeleteInput(on_Delete = true)
Property: `hoodie.combine.before.delete`<br/>
<span >Flag which first combines the input RDD and merges multiple partial records into a single record before deleting in DFS</span>

#### withMergeAllowDuplicateOnInserts(mergeAllowDuplicateOnInserts = false)
Property: `hoodie.merge.allow.duplicate.on.inserts` <br/>
<span > When enabled, will route new records as inserts and will not merge with existing records.
Result could contain duplicate entries. </span>

#### withWriteStatusStorageLevel(level = MEMORY_AND_DISK_SER) 
Property: `hoodie.write.status.storage.level`<br/>
<span >HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD </span>

#### withAutoCommit(autoCommit = true) 
Property: `hoodie.auto.commit`<br/>
<span >Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a "defined success condition"</span>

#### withAssumeDatePartitioning(assumeDatePartitioning = false) 
Property: `hoodie.assume.date.partitioning`<br/>
<span >Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions &lt; 0.3.1. Will be removed eventually </span>

#### withConsistencyCheckEnabled(enabled = false) 
Property: `hoodie.consistency.check.enabled`<br/>
<span >Should HoodieWriteClient perform additional checks to ensure written files' are listable on the underlying filesystem/storage. Set this to true, to workaround S3's eventual consistency model and ensure all data written as a part of a commit is faithfully available for queries. </span>

#### withRollbackParallelism(rollbackParallelism = 100) 
Property: `hoodie.rollback.parallelism`<br/>
<span >Determines the parallelism for rollback of commits.</span>

#### withRollbackUsingMarkers(rollbackUsingMarkers = false) 
Property: `hoodie.rollback.using.markers`<br/>
<span >Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned off by default.</span>

#### withMarkersDeleteParallelism(parallelism = 100) 
Property: `hoodie.markers.delete.parallelism`<br/>
<span >Determines the parallelism for deleting marker files.</span>

### Index configs
Following configs control indexing behavior, which tags incoming records as either inserts or updates to older records. 

[withIndexConfig](#index-configs) (HoodieIndexConfig) <br/>
<span >This is pluggable to have a external index (HBase) or use the default bloom filter stored in the Parquet files</span>

#### withIndexClass(indexClass = "x.y.z.UserDefinedIndex")
Property: `hoodie.index.class` <br/>
<span >Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the `hoodie.index.type` configuration if specified</span>

#### withIndexType(indexType = BLOOM)
Property: `hoodie.index.type` <br/>
<span >Type of index to use. Default is Bloom filter. Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files</span>

#### Bloom Index configs

#### bloomIndexFilterType(bucketizedChecking = BloomFilterTypeCode.SIMPLE)
Property: `hoodie.bloom.index.filter.type` <br/>
<span >Filter type used. Default is BloomFilterTypeCode.SIMPLE. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.</span>

#### bloomFilterNumEntries(numEntries = 60000)
Property: `hoodie.index.bloom.num_entries` <br/>
<span >Only applies if index type is BLOOM. <br/>This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNNAMIC bloom filter which determines the initial size for the bloom. </span>

#### bloomFilterFPP(fpp = 0.000000001)
Property: `hoodie.index.bloom.fpp` <br/>
<span >Only applies if index type is BLOOM. <br/> Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the congfigured value (`hoodie.index.bloom.num_entries`), then this fpp may not be honored.</span>

#### bloomIndexParallelism(0)
Property: `hoodie.bloom.index.parallelism` <br/>
<span >Only applies if index type is BLOOM. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics</span>

#### bloomIndexPruneByRanges(pruneRanges = true)
Property: `hoodie.bloom.index.prune.by.ranges` <br/>
<span >Only applies if index type is BLOOM. <br/> When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off.</span>

#### bloomIndexUseCaching(useCaching = true)
Property: `hoodie.bloom.index.use.caching` <br/>
<span >Only applies if index type is BLOOM. <br/> When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions</span>

#### bloomIndexTreebasedFilter(useTreeFilter = true)
Property: `hoodie.bloom.index.use.treebased.filter` <br/>
<span >Only applies if index type is BLOOM. <br/> When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode</span>

#### bloomIndexBucketizedChecking(bucketizedChecking = true)
Property: `hoodie.bloom.index.bucketized.checking` <br/>
<span >Only applies if index type is BLOOM. <br/> When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup</span>

#### bloomIndexFilterDynamicMaxEntries(maxNumberOfEntries = 100000)
Property: `hoodie.bloom.index.filter.dynamic.max.entries` <br/>
<span >The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.</span>

#### bloomIndexKeysPerBucket(keysPerBucket = 10000000)
Property: `hoodie.bloom.index.keys.per.bucket` <br/>
<span >Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. <br/> This configuration controls the "bucket" size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory. </span>

##### withBloomIndexInputStorageLevel(level = MEMORY_AND_DISK_SER)
Property: `hoodie.bloom.index.input.storage.level` <br/>
<span >Only applies when [#bloomIndexUseCaching](#bloomIndexUseCaching) is set. Determine what level of persistence is used to cache input RDDs.<br/> Refer to org.apache.spark.storage.StorageLevel for different values</span>

##### bloomIndexUpdatePartitionPath(updatePartitionPath = false)
Property: `hoodie.bloom.index.update.partition.path` <br/>
<span >Only applies if index type is GLOBAL_BLOOM. <br/>When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition.</span>

#### HBase Index configs

#### hbaseZkQuorum(zkString) [Required]  
Property: `hoodie.index.hbase.zkquorum` <br/>
<span >Only applies if index type is HBASE. HBase ZK Quorum url to connect to.</span>

#### hbaseZkPort(port) [Required]  
Property: `hoodie.index.hbase.zkport` <br/>
<span >Only applies if index type is HBASE. HBase ZK Quorum port to connect to.</span>

#### hbaseZkZnodeParent(zkZnodeParent)  [Required]
Property: `hoodie.index.hbase.zknode.path` <br/>
<span >Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase.</span>

#### hbaseTableName(tableName)  [Required]
Property: `hoodie.index.hbase.table` <br/>
<span >Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table.</span>

#### hbaseIndexUpdatePartitionPath(updatePartitionPath)
Property: `hoodie.hbase.index.update.partition.path` <br/>
<span >Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old paritition and will insert it as new record in new partition. </span>

#### Simple Index configs

#### simpleIndexUseCaching(useCaching = true)
Property: `hoodie.simple.index.use.caching` <br/>
<span >Only applies if index type is SIMPLE. <br/> When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions</span>

##### withSimpleIndexInputStorageLevel(level = MEMORY_AND_DISK_SER)
Property: `hoodie.simple.index.input.storage.level` <br/>
<span >Only applies when [#simpleIndexUseCaching](#simpleIndexUseCaching) is set. Determine what level of persistence is used to cache input RDDs.<br/> Refer to org.apache.spark.storage.StorageLevel for different values</span>

#### withSimpleIndexParallelism(parallelism = 50)
Property: `hoodie.simple.index.parallelism` <br/>
<span >Only applies if index type is SIMPLE. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle.</span>

#### withGlobalSimpleIndexParallelism(parallelism = 100)
Property: `hoodie.global.simple.index.parallelism` <br/>
<span >Only applies if index type is GLOBAL_SIMPLE. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle.</span>

### Storage configs
Controls aspects around sizing parquet and log files.

[withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>

#### limitFileSize (size = 120MB)
Property: `hoodie.parquet.max.file.size` <br/>
<span >Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance. </span>

#### parquetBlockSize(rowgroupsize = 120MB) 
Property: `hoodie.parquet.block.size` <br/>
<span >Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk</span>

#### parquetPageSize(pagesize = 1MB) 
Property: `hoodie.parquet.page.size` <br/>
<span >Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately. </span>

#### parquetCompressionRatio(parquetCompressionRatio = 0.1) 
Property: `hoodie.parquet.compression.ratio` <br/>
<span >Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files</span>

#### parquetCompressionCodec(parquetCompressionCodec = gzip)
Property: `hoodie.parquet.compression.codec` <br/>
<span >Parquet compression codec name. Default is gzip. Possible options are [gzip | snappy | uncompressed | lzo]</span>

#### logFileMaxSize(logFileSize = 1GB) 
Property: `hoodie.logfile.max.size` <br/>
<span >LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version. </span>

#### logFileDataBlockMaxSize(dataBlockSize = 256MB) 
Property: `hoodie.logfile.data.block.max.size` <br/>
<span >LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory. </span>

#### logFileToParquetCompressionRatio(logFileToParquetCompressionRatio = 0.35) 
Property: `hoodie.logfile.to.parquet.compression.ratio` <br/>
<span >Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.</span>
 
#### parquetCompressionCodec(parquetCompressionCodec = gzip) 
Property: `hoodie.parquet.compression.codec` <br/>
<span >Compression Codec for parquet files </span>

### Compaction configs
Configs that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).
[withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>

#### withCleanerPolicy(policy = KEEP_LATEST_COMMITS) 
Property: `hoodie.cleaner.policy` <br/>
<span > Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.</span>

#### withFailedWritesCleaningPolicy(policy = HoodieFailedWritesCleaningPolicy.EAGER) 
Property: `hoodie.cleaner.policy.failed.writes` <br/>
<span > Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes `eagerly` before every writer starts (only supported for single writer) or `lazily` by the cleaner (required for multi-writers)</span>

#### retainCommits(no_of_commits_to_retain = 24) 
Property: `hoodie.cleaner.commits.retained` <br/>
<span >Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table</span>

#### withAutoClean(autoClean = true) 
Property: `hoodie.clean.automatic` <br/>
<span >Should cleanup if there is anything to cleanup immediately after the commit</span>

#### withAsyncClean(asyncClean = false) 
Property: `hoodie.clean.async` <br/>
<span >Only applies when [#withAutoClean](#withAutoClean) is turned on. When turned on runs cleaner async with writing. </span>

#### archiveCommitsWith(minCommits = 96, maxCommits = 128) 
Property: `hoodie.keep.min.commits`, `hoodie.keep.max.commits` <br/>
<span >Each commit is a small file in the `.hoodie` directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.</span>

#### withCommitsArchivalBatchSize(batch = 10)
Property: `hoodie.commits.archival.batch` <br/>
<span >This controls the number of commit instants read in memory as a batch and archived together.</span>

#### compactionSmallFileSize(size = 100MB) 
Property: `hoodie.parquet.small.file.limit` <br/>
<span >This should be less &lt; maxFileSize and setting it to 0, turns off this feature. Small files can always happen because of the number of insert records in a partition in a batch. Hudi has an option to auto-resolve small files by masking inserts into this partition as updates to existing small files. The size here is the minimum file size considered as a "small file size".</span>

#### insertSplitSize(size = 500000) 
Property: `hoodie.copyonwrite.insert.split.size` <br/>
<span >Insert Write Parallelism. Number of inserts grouped for a single partition. Writing out 100MB files, with atleast 1kb records, means 100K records per file. Default is to overprovision to 500K. To improve insert latency, tune this to match the number of records in a single file. Setting this to a low number, will result in small files (particularly when compactionSmallFileSize is 0)</span>

#### autoTuneInsertSplits(true) 
Property: `hoodie.copyonwrite.insert.auto.split` <br/>
<span >Should hudi dynamically compute the insertSplitSize based on the last 24 commit's metadata. Turned on by default. </span>

#### approxRecordSize(size = 1024) 
Property: `hoodie.copyonwrite.record.size.estimate` <br/>
<span >The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit's metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.</span>

#### withInlineCompaction(inlineCompaction = false) 
Property: `hoodie.compact.inline` <br/>
<span >When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert</span>

#### withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction = 10) 
Property: `hoodie.compact.inline.max.delta.commits` <br/>
<span >Number of max delta commits to keep before triggering an inline compaction</span>

#### withCompactionLazyBlockReadEnabled(true) 
Property: `hoodie.compaction.lazy.block.read` <br/>
<span >When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)</span>

#### withCompactionReverseLogReadEnabled(false) 
Property: `hoodie.compaction.reverse.log.read` <br/>
<span >HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0</span>

#### withCleanerParallelism(cleanerParallelism = 200) 
Property: `hoodie.cleaner.parallelism` <br/>
<span >Increase this if cleaning becomes slow.</span>

#### withCompactionStrategy(compactionStrategy = org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy) 
Property: `hoodie.compaction.strategy` <br/>
<span >Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data</span>

#### withTargetIOPerCompactionInMB(targetIOPerCompactionInMB = 500000) 
Property: `hoodie.compaction.target.io` <br/>
<span >Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.</span>

#### withTargetPartitionsPerDayBasedCompaction(targetPartitionsPerCompaction = 10) 
Property: `hoodie.compaction.daybased.target` <br/>
<span >Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.</span>    

#### withPayloadClass(payloadClassName = org.apache.hudi.common.model.HoodieAvroPayload) 
Property: `hoodie.compaction.payload.class` <br/>
<span >This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.</span>

### Bootstrap Configs
Controls bootstrap related configs. If you want to bootstrap your data for the first time into hudi, this bootstrap operation will come in handy as you don't need to wait for entire data to be loaded into hudi to start leveraging hudi. 

[withBootstrapConfig](#withBootstrapConfig) (HoodieBootstrapConfig) <br/>

#### withBootstrapBasePath(basePath)
Property: `hoodie.bootstrap.base.path` <br/>
<span > Base path of the dataset that needs to be bootstrapped as a Hudi table </span> 

#### withBootstrapParallelism(parallelism = 1500)
Property: `hoodie.bootstrap.parallelism` <br/>
<span > Parallelism value to be used to bootstrap data into hudi </span>

#### withBootstrapKeyGenClass(keyGenClass) (#withBootstrapKeyGenClass)
Property: `hoodie.bootstrap.keygen.class` <br/>
<span > Key generator implementation to be used for generating keys from the bootstrapped dataset </span>

#### withBootstrapModeSelector(partitionSelectorClass = org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector)
Property: `hoodie.bootstrap.mode.selector` <br/>
<span > Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped</span>

#### withBootstrapPartitionPathTranslatorClass(partitionPathTranslatorClass = org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator)
Property: `hoodie.bootstrap.partitionpath.translator.class` <br/>
<span > Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table. </span>

#### withFullBootstrapInputProvider(partitionSelectorClass = org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider)
Property: `hoodie.bootstrap.full.input.provider` <br/>
<span > Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode `FULL_RECORD` </span>

#### withBootstrapModeSelectorRegex(regex = ".*")
Property: `hoodie.bootstrap.mode.selector.regex` <br/>
<span > Matches each bootstrap dataset partition against this regex and applies the mode below to it. </span>

#### withBootstrapModeForRegexMatch(modeForRegexMatch = org.apache.hudi.client.bootstrap.METADATA_ONLY) 
Property: `withBootstrapModeForRegexMatch` <br/>
<span > Bootstrap mode to apply for partition paths, that match regex above. `METADATA_ONLY` will generate just skeleton base files
with keys/footers, avoiding full cost of rewriting the dataset. `FULL_RECORD` will perform a full copy/rewrite of the data as a Hudi table. </span>

### Metadata Config
Configurations used by the HUDI Metadata Table. This table maintains the meta information stored in hudi dataset so that listing can be avoided during queries. 

[withMetadataConfig](#withMetadataConfig) (HoodieMetadataConfig) <br/>

#### enable(enable = false)
Property: `hoodie.metadata.enable` <br/>
<span > Enable the internal Metadata Table which stores table level metadata such as file listings </span>

#### enableReuse(enable = true)
Property: `hoodie.metadata.reuse.enable` <br/>
<span > Enable reusing of opened file handles/merged logs, across multiple fetches from metadata table. </span>

#### enableFallback(enable = true)
Property: `hoodie.metadata.fallback.enable` <br/>
<span > Fallback to listing from DFS, if there are any errors in fetching from metadata table </span>

#### validate(validate = false)
Property: `hoodie.metadata.validate` <br/>
<span > Validate contents of Metadata Table on each access against the actual listings from DFS</span>

#### withInsertParallelism(parallelism = 1)
Property: `hoodie.metadata.insert.parallelism` <br/>
<span > Parallelism to use when writing to the metadata table </span>

#### withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction = 24)
Property: `hoodie.metadata.compact.max.delta.commits` <br/>
<span > Controls how often the metadata table is compacted.</span>

#### archiveCommitsWith(minToKeep = 30, maxToKeep = 20)
Property: `hoodie.metadata.keep.min.commits`, `hoodie.metadata.keep.max.commits` <br/>
<span > Controls the archival of the metadata table's timeline </span>

### Clustering Configs
Controls clustering operations in hudi. Each clustering has to be configured for its strategy, and config params. This config drives the same. 

[withClusteringConfig](#withClusteringConfig) (HoodieClusteringConfig) <br/>

#### withClusteringPlanStrategyClass(clusteringStrategyClass = org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy)
Property: `hoodie.clustering.plan.strategy.class` <br/>
<span > Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy </span>

#### withClusteringExecutionStrategyClass(runClusteringStrategyClass = org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy)
Property: `hoodie.clustering.execution.strategy.class` <br/>
<span > Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy </span>

#### withClusteringTargetPartitions(clusteringTargetPartitions = 2)
Property: `hoodie.clustering.plan.strategy.daybased.lookback.partitions` <br/>
<span > Number of partitions to list to create ClusteringPlan </span>

#### withClusteringPlanSmallFileLimit(clusteringSmallFileLimit = 600Mb)
Property: `hoodie.clustering.plan.strategy.small.file.limit` <br/>
<span > Files smaller than the size specified here are candidates for clustering </span>

#### withClusteringMaxBytesInGroup(clusteringMaxGroupSize = 2Gb)
Property: `hoodie.clustering.plan.strategy.max.bytes.per.group` <br/>
<span > Max amount of data to be included in one group
Each clustering operation can create multiple groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). </span>

#### withClusteringMaxNumGroups(maxNumGroups = 30)
Property : `hoodie.clustering.plan.strategy.max.num.groups` <br/>
<span > Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism. </span>

#### withClusteringTargetFileMaxBytes(targetFileSize = 1Gb )
Property: `hoodie.clustering.plan.strategy.target.file.max.bytes` <br/>
<span > Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups </span>

### Payload Configs
Payload related configs. This config can be leveraged by payload implementations to determine their business logic. 

[withPayloadConfig](#withPayloadConfig) (HoodiePayloadConfig) <br/>

#### withPayloadOrderingField(payloadOrderingField = "ts")
Property: `hoodie.payload.ordering.field` <br/>
<span > Property to hold the payload ordering field name. </span>

### Metrics configs

Enables reporting on Hudi metrics.
[withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
<span >Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.</span>

#### GRAPHITE

##### on(metricsOn = false)
`hoodie.metrics.on` <br/>
<span >Turn on/off metrics reporting. off by default.</span>

##### withReporterType(reporterType = GRAPHITE)
Property: `hoodie.metrics.reporter.type` <br/>
<span >Type of metrics reporter.</span>

##### toGraphiteHost(host = localhost)
Property: `hoodie.metrics.graphite.host` <br/>
<span >Graphite host to connect to</span>

##### onGraphitePort(port = 4756)
Property: `hoodie.metrics.graphite.port` <br/>
<span >Graphite port to connect to</span>

##### usePrefix(prefix = "")
Property: `hoodie.metrics.graphite.metric.prefix` <br/>
<span >Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g</span>

#### JMX

##### on(metricsOn = false)
`hoodie.metrics.on` <br/>
<span >Turn on/off metrics reporting. off by default.</span>

##### withReporterType(reporterType = JMX)
Property: `hoodie.metrics.reporter.type` <br/>
<span >Type of metrics reporter.</span>

##### toJmxHost(host = localhost)
Property: `hoodie.metrics.jmx.host` <br/>
<span >Jmx host to connect to</span>

##### onJmxPort(port = 1000-5000)
Property: `hoodie.metrics.jmx.port` <br/>
<span >Jmx port to connect to</span>

#### DATADOG

##### on(metricsOn = false)
`hoodie.metrics.on` <br/>
<span >Turn on/off metrics reporting. off by default.</span>

##### withReporterType(reporterType = DATADOG)
Property: `hoodie.metrics.reporter.type` <br/>
<span >Type of metrics reporter.</span>

##### withDatadogReportPeriodSeconds(period = 30)
Property: `hoodie.metrics.datadog.report.period.seconds` <br/>
<span >Datadog report period in seconds. Default to 30.</span>

##### withDatadogApiSite(apiSite)
Property: `hoodie.metrics.datadog.api.site` <br/>
<span >Datadog API site: EU or US</span>

##### withDatadogApiKey(apiKey)
Property: `hoodie.metrics.datadog.api.key` <br/>
<span >Datadog API key</span>

##### withDatadogApiKeySkipValidation(skip = false)
Property: `hoodie.metrics.datadog.api.key.skip.validation` <br/>
<span >Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.</span>

##### withDatadogApiKeySupplier(apiKeySupplier)
Property: `hoodie.metrics.datadog.api.key.supplier` <br/>
<span >Datadog API key supplier to supply the API key at runtime. This will take effect if `hoodie.metrics.datadog.api.key` is not set.</span>

##### withDatadogApiTimeoutSeconds(timeout = 3)
Property: `hoodie.metrics.datadog.metric.prefix` <br/>
<span >Datadog API timeout in seconds. Default to 3.</span>

##### withDatadogPrefix(prefix)
Property: `hoodie.metrics.datadog.metric.prefix` <br/>
<span >Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to `foo`, `foo.` will be prepended.</span>

##### withDatadogHost(host)
Property: `hoodie.metrics.datadog.metric.host` <br/>
<span >Datadog metric host to be sent along with metrics data.</span>

##### withDatadogTags(tags)
Property: `hoodie.metrics.datadog.metric.tags` <br/>
<span >Datadog metric tags (comma-delimited) to be sent along with metrics data.</span>

#### USER DEFINED REPORTER

##### on(metricsOn = false)
`hoodie.metrics.on` <br/>
<span >Turn on/off metrics reporting. off by default.</span>

##### withReporterClass(className = "")
Property: `hoodie.metrics.reporter.class` <br/>
<span >User-defined class used to report metrics, must be a subclass of AbstractUserDefinedMetricsReporter.</span>


### Memory configs
Controls memory usage for compaction and merges, performed internally by Hudi
[withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
<span >Memory related configs</span>

#### withMaxMemoryFractionPerPartitionMerge(maxMemoryFractionPerPartitionMerge = 0.6) 
Property: `hoodie.memory.merge.fraction` <br/>
<span >This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge </span>

#### withMaxMemorySizePerCompactionInBytes(maxMemorySizePerCompactionInBytes = 1GB) 
Property: `hoodie.memory.compaction.fraction` <br/>
<span >HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map.</span>

#### withWriteStatusFailureFraction(failureFraction = 0.1)
Property: `hoodie.memory.writestatus.failure.fraction` <br/>
<span >This property controls what fraction of the failed record, exceptions we report back to driver</span>

### Write commit callback configs
Controls callback behavior on write commit. Exception will be thrown if user enabled the callback service and errors occurred during the process of callback. Currently support HTTP, Kafka type. 
[withCallbackConfig](#withCallbackConfig) (HoodieWriteCommitCallbackConfig) <br/>
<span >Callback related configs</span>

##### writeCommitCallbackOn(callbackOn = false) 
Property: `hoodie.write.commit.callback.on` <br/>
<span >Turn callback on/off. off by default.</span>

##### withCallbackClass(callbackClass) 
Property: `hoodie.write.commit.callback.class` <br/>
<span >Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default</span>

#### HTTP CALLBACK
Callback via HTTP, User does not need to specify this way explicitly, it is the default type.

##### withCallbackHttpUrl(url) 
Property: `hoodie.write.commit.callback.http.url` <br/>
<span >Callback host to be sent along with callback messages</span>

##### withCallbackHttpTimeoutSeconds(timeoutSeconds = 3) 
Property: `hoodie.write.commit.callback.http.timeout.seconds` <br/>
<span >Callback timeout in seconds. 3 by default</span>

##### withCallbackHttpApiKey(apiKey) 
Property: `hoodie.write.commit.callback.http.api.key` <br/>
<span >Http callback API key. hudi_write_commit_http_callback by default</span>

#### KAFKA CALLBACK
To use kafka callback, User should set `hoodie.write.commit.callback.class` = `org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback`

##### CALLBACK_KAFKA_BOOTSTRAP_SERVERS
Property: `hoodie.write.commit.callback.kafka.bootstrap.servers` <br/>
<span >Bootstrap servers of kafka callback cluster</span>

##### CALLBACK_KAFKA_TOPIC
Property: `hoodie.write.commit.callback.kafka.topic` <br/>
<span >Kafka topic to be sent along with callback messages</span>

##### CALLBACK_KAFKA_PARTITION
Property: `hoodie.write.commit.callback.kafka.partition` <br/>
<span >partition of `CALLBACK_KAFKA_TOPIC`, 0 by default</span>

##### CALLBACK_KAFKA_ACKS
Property: `hoodie.write.commit.callback.kafka.acks` <br/>
<span >kafka acks level, `all` by default</span>

##### CALLBACK_KAFKA_RETRIES
Property: `hoodie.write.commit.callback.kafka.retries` <br/>
<span >Times to retry. 3 by default</span>

### Locking configs
Configs that control locking mechanisms if [WriteConcurrencyMode=optimistic_concurrency_control](#WriteConcurrencyMode) is enabled
[withLockConfig](#withLockConfig) (HoodieLockConfig) <br/>

#### withLockProvider(lockProvider = org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider)
Property: `hoodie.write.lock.provider` <br/>
<span >Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider</span>

#### withZkQuorum(zkQuorum)
Property: `hoodie.write.lock.zookeeper.url` <br/>
<span >Set the list of comma separated servers to connect to</span>

#### withZkBasePath(zkBasePath)
Property: `hoodie.write.lock.zookeeper.base_path` [Required] <br/>
<span >The base path on Zookeeper under which to create a ZNode to acquire the lock. This should be common for all jobs writing to the same table</span>

#### withZkPort(zkPort)
Property: `hoodie.write.lock.zookeeper.port` [Required] <br/>
<span >The connection port to be used for Zookeeper</span>

#### withZkLockKey(zkLockKey)
Property: `hoodie.write.lock.zookeeper.lock_key` [Required] <br/>
<span >Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. We recommend setting this to the table name</span>

#### withZkConnectionTimeoutInMs(connectionTimeoutInMs = 15000)
Property: `hoodie.write.lock.zookeeper.connection_timeout_ms` <br/>
<span >How long to wait when connecting to ZooKeeper before considering the connection a failure</span>

#### withZkSessionTimeoutInMs(sessionTimeoutInMs = 60000)
Property: `hoodie.write.lock.zookeeper.session_timeout_ms` <br/>
<span >How long to wait after losing a connection to ZooKeeper before the session is expired</span>

#### withNumRetries(num_retries = 3)
Property: `hoodie.write.lock.num_retries` <br/>
<span >Maximum number of times to retry by lock provider client</span>

#### withRetryWaitTimeInMillis(retryWaitTimeInMillis = 5000)
Property: `hoodie.write.lock.wait_time_ms_between_retry` <br/>
<span >Initial amount of time to wait between retries by lock provider client</span>

#### withHiveDatabaseName(hiveDatabaseName)
Property: `hoodie.write.lock.hivemetastore.database` [Required] <br/>
<span >The Hive database to acquire lock against</span>

#### withHiveTableName(hiveTableName)
Property: `hoodie.write.lock.hivemetastore.table` [Required] <br/>
<span >The Hive table under the hive database to acquire lock against</span>

#### withClientNumRetries(clientNumRetries = 0)
Property: `hoodie.write.lock.client.num_retries` <br/>
<span >Maximum number of times to retry to acquire lock additionally from the hudi client</span>

#### withRetryWaitTimeInMillis(retryWaitTimeInMillis = 10000)
Property: `hoodie.write.lock.client.wait_time_ms_between_retry` <br/>
<span >Amount of time to wait between retries from the hudi client</span>

#### withConflictResolutionStrategy(lockProvider = org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy)
Property: `hoodie.write.lock.conflict.resolution.strategy` <br/>
<span >Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy</span>



