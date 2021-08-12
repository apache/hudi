---
version: 0.5.1
title: Configurations
keywords: [ garbage collection, hudi, jvm, configs, tuning]
summary: "Here we list all possible configurations and what they mean"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

This page covers the different ways of configuring your job to write/read Hudi tables. 
At a high level, you can control behaviour at few levels. 

- **[Spark Datasource Configs](#spark-datasource)** : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- **[WriteClient Configs](#writeclient-configs)** : Internally, the Hudi datasource uses a RDD based `HoodieWriteClient` api to actually perform writes to storage. These configs provide deep control over lower level aspects like 
   file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- **[RecordPayload Config](#PAYLOAD_CLASS_OPT_KEY)** : This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and 
   stored old record. Hudi provides default implementations such as `OverwriteWithLatestAvroPayload` which simply update table with the latest/last-written record. 
   This can be overridden to a custom class extending `HoodieRecordPayload` class, on both datasource and WriteClient levels.
 
## Talking to Cloud Storage

Immaterial of whether RDD/WriteClient APIs or Datasource is used, the following information helps configure access
to cloud stores.

 * [AWS S3](/docs/s3_hoodie) <br/>
   Configurations required for S3 and Hudi co-operability.
 * [Google Cloud Storage](/docs/gcs_hoodie) <br/>
   Configurations required for GCS and Hudi co-operability.

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
  Hive table name, to register the table into.
  
#### OPERATION_OPT_KEY
  Property: `hoodie.datasource.write.operation`, Default: `upsert`<br/>
  whether to do upsert, insert or bulkinsert for the write operation. Use `bulkinsert` to load new data into a table, and there on use `upsert`/`insert`. 
  bulk insert uses a disk based write path to scale to load large inputs without need to cache it.
  
#### TABLE_TYPE_OPT_KEY
  Property: `hoodie.datasource.write.table.type`, Default: `COPY_ON_WRITE` <br/>
  The table type for the underlying data, for this write. This can't change between writes.
  
#### PRECOMBINE_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.precombine.field`, Default: `ts` <br/>
  Field used in preCombining before actual write. When two records have the same key value,
we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)

#### PAYLOAD_CLASS_OPT_KEY
  Property: `hoodie.datasource.write.payload.class`, Default: `org.apache.hudi.OverwriteWithLatestAvroPayload` <br/>
  Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. 
  This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
  
#### RECORDKEY_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.recordkey.field`, Default: `uuid` <br/>
  Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`

#### PARTITIONPATH_FIELD_OPT_KEY
  Property: `hoodie.datasource.write.partitionpath.field`, Default: `partitionpath` <br/>
  Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
Actual value ontained by invoking .toString()

#### KEYGENERATOR_CLASS_OPT_KEY
  Property: `hoodie.datasource.write.keygenerator.class`, Default: `org.apache.hudi.SimpleKeyGenerator` <br/>
  Key generator class, that implements will extract the key out of incoming `Row` object
  
#### COMMIT_METADATA_KEYPREFIX_OPT_KEY
  Property: `hoodie.datasource.write.commitmeta.key.prefix`, Default: `_` <br/>
  Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
This is useful to store checkpointing information, in a consistent way with the hudi timeline

#### INSERT_DROP_DUPS_OPT_KEY
  Property: `hoodie.datasource.write.insert.drop.duplicates`, Default: `false` <br/>
  If set to true, filters out all duplicate records from incoming dataframe, during insert operations. 
  
#### HIVE_SYNC_ENABLED_OPT_KEY
  Property: `hoodie.datasource.hive_sync.enable`, Default: `false` <br/>
  When set to true, register/sync the table to Apache Hive metastore
  
#### HIVE_DATABASE_OPT_KEY
  Property: `hoodie.datasource.hive_sync.database`, Default: `default` <br/>
  database to sync to
  
#### HIVE_TABLE_OPT_KEY
  Property: `hoodie.datasource.hive_sync.table`, [Required] <br/>
  table to sync to
  
#### HIVE_USER_OPT_KEY
  Property: `hoodie.datasource.hive_sync.username`, Default: `hive` <br/>
  hive user name to use
  
#### HIVE_PASS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.password`, Default: `hive` <br/>
  hive password to use
  
#### HIVE_URL_OPT_KEY
  Property: `hoodie.datasource.hive_sync.jdbcurl`, Default: `jdbc:hive2://localhost:10000` <br/>
  Hive metastore url
  
#### HIVE_PARTITION_FIELDS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.partition_fields`, Default: ` ` <br/>
  field in the table to use for determining hive partition columns.
  
#### HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY
  Property: `hoodie.datasource.hive_sync.partition_extractor_class`, Default: `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor` <br/>
  Class used to extract partition field values into hive partition columns.
  
#### HIVE_ASSUME_DATE_PARTITION_OPT_KEY
  Property: `hoodie.datasource.hive_sync.assume_date_partitioning`, Default: `false` <br/>
  Assume partitioning is yyyy/mm/dd

### Read Options

Options useful for reading tables via `read.format.option(...)`

#### QUERY_TYPE_OPT_KEY
Property: `hoodie.datasource.query.type`, Default: `snapshot` <br/>
Whether data needs to be read, in incremental mode (new data since an instantTime)
(or) Read Optimized mode (obtain latest view, based on columnar data)
(or) Snapshot mode (obtain latest view, based on row & columnar data)

#### BEGIN_INSTANTTIME_OPT_KEY 
Property: `hoodie.datasource.read.begin.instanttime`, [Required in incremental mode] <br/>
Instant time to start incrementally pulling data from. The instanttime here need not
necessarily correspond to an instant on the timeline. New data written with an
 `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
 all new data written after Sep 1, 2017 08:00AM.
 
#### END_INSTANTTIME_OPT_KEY
Property: `hoodie.datasource.read.end.instanttime`, Default: latest instant (i.e fetches all new data since begin instant time) <br/>
 Instant time to limit incrementally fetched data to. New data written with an
`instant_time &lt;= END_INSTANTTIME` are fetched out.


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
Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory. 

#### withSchema(schema_str) 
Property: `hoodie.avro.schema` [Required]<br/>
This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. 

#### forTable(table_name) 
Property: `hoodie.table.name` [Required] <br/>
 Table name that will be used for registering with Hive. Needs to be same across runs.

#### withBulkInsertParallelism(bulk_insert_parallelism = 1500) 
Property: `hoodie.bulkinsert.shuffle.parallelism`<br/>
Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your table. Tune this to achieve a desired optimal size during initial import.

#### withParallelism(insert_shuffle_parallelism = 1500, upsert_shuffle_parallelism = 1500) 
Property: `hoodie.insert.shuffle.parallelism`, `hoodie.upsert.shuffle.parallelism`<br/>
Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data

#### combineInput(on_insert = false, on_update=true) 
Property: `hoodie.combine.before.insert`, `hoodie.combine.before.upsert`<br/>
Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS

#### withWriteStatusStorageLevel(level = MEMORY_AND_DISK_SER) 
Property: `hoodie.write.status.storage.level`<br/>
HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD 

#### withAutoCommit(autoCommit = true) 
Property: `hoodie.auto.commit`<br/>
Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a "defined success condition"

#### withAssumeDatePartitioning(assumeDatePartitioning = false) 
Property: `hoodie.assume.date.partitioning`<br/>
Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions &lt; 0.3.1. Will be removed eventually 

#### withConsistencyCheckEnabled(enabled = false) 
Property: `hoodie.consistency.check.enabled`<br/>
Should HoodieWriteClient perform additional checks to ensure written files' are listable on the underlying filesystem/storage. Set this to true, to workaround S3's eventual consistency model and ensure all data written as a part of a commit is faithfully available for queries. 

### Index configs
Following configs control indexing behavior, which tags incoming records as either inserts or updates to older records. 

[withIndexConfig](#withIndexConfig) (HoodieIndexConfig) <br/>
This is pluggable to have a external index (HBase) or use the default bloom filter stored in the Parquet files

#### withIndexType(indexType = BLOOM)
Property: `hoodie.index.type` <br/>
Type of index to use. Default is Bloom filter. Possible options are [BLOOM | HBASE | INMEMORY]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files

#### bloomFilterNumEntries(numEntries = 60000)
Property: `hoodie.index.bloom.num_entries` <br/>
Only applies if index type is BLOOM. <br/>This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries).

#### bloomFilterFPP(fpp = 0.000000001)
Property: `hoodie.index.bloom.fpp` <br/>
Only applies if index type is BLOOM. <br/> Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives

#### bloomIndexPruneByRanges(pruneRanges = true)
Property: `hoodie.bloom.index.prune.by.ranges` <br/>
Only applies if index type is BLOOM. <br/> When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp.

#### bloomIndexUseCaching(useCaching = true)
Property: `hoodie.bloom.index.use.caching` <br/>
Only applies if index type is BLOOM. <br/> When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions

#### bloomIndexTreebasedFilter(useTreeFilter = true)
Property: `hoodie.bloom.index.use.treebased.filter` <br/>
Only applies if index type is BLOOM. <br/> When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode

#### bloomIndexBucketizedChecking(bucketizedChecking = true)
Property: `hoodie.bloom.index.bucketized.checking` <br/>
Only applies if index type is BLOOM. <br/> When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup

#### bloomIndexKeysPerBucket(keysPerBucket = 10000000)
Property: `hoodie.bloom.index.keys.per.bucket` <br/>
Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. <br/> This configuration controls the "bucket" size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory. 

#### bloomIndexParallelism(0)
Property: `hoodie.bloom.index.parallelism` <br/>
Only applies if index type is BLOOM. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics

#### hbaseZkQuorum(zkString) [Required]  
Property: `hoodie.index.hbase.zkquorum` <br/>
Only applies if index type is HBASE. HBase ZK Quorum url to connect to.

#### hbaseZkPort(port) [Required]  
Property: `hoodie.index.hbase.zkport` <br/>
Only applies if index type is HBASE. HBase ZK Quorum port to connect to.

#### hbaseZkZnodeParent(zkZnodeParent)  [Required]
Property: `hoodie.index.hbase.zknode.path` <br/>
Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase.

#### hbaseTableName(tableName)  [Required]
Property: `hoodie.index.hbase.table` <br/>
Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table.

    
### Storage configs
Controls aspects around sizing parquet and log files.

[withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>

#### limitFileSize (size = 120MB)
Property: `hoodie.parquet.max.file.size` <br/>
Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance. 

#### parquetBlockSize(rowgroupsize = 120MB) 
Property: `hoodie.parquet.block.size` <br/>
Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk

#### parquetPageSize(pagesize = 1MB) 
Property: `hoodie.parquet.page.size` <br/>
Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately. 

#### parquetCompressionRatio(parquetCompressionRatio = 0.1) 
Property: `hoodie.parquet.compression.ratio` <br/>
Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files

#### parquetCompressionCodec(parquetCompressionCodec = gzip)
Property: `hoodie.parquet.compression.codec` <br/>
Parquet compression codec name. Default is gzip. Possible options are [gzip | snappy | uncompressed | lzo]

#### logFileMaxSize(logFileSize = 1GB) 
Property: `hoodie.logfile.max.size` <br/>
LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version. 

#### logFileDataBlockMaxSize(dataBlockSize = 256MB) 
Property: `hoodie.logfile.data.block.max.size` <br/>
LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory. 

#### logFileToParquetCompressionRatio(logFileToParquetCompressionRatio = 0.35) 
Property: `hoodie.logfile.to.parquet.compression.ratio` <br/>
Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.
 
#### parquetCompressionCodec(parquetCompressionCodec = gzip) 
Property: `hoodie.parquet.compression.codec` <br/>
Compression Codec for parquet files 

### Compaction configs
Configs that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).
[withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>

#### withCleanerPolicy(policy = KEEP_LATEST_COMMITS) 
Property: `hoodie.cleaner.policy` <br/>
 Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.

#### retainCommits(no_of_commits_to_retain = 24) 
Property: `hoodie.cleaner.commits.retained` <br/>
Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table

#### archiveCommitsWith(minCommits = 96, maxCommits = 128) 
Property: `hoodie.keep.min.commits`, `hoodie.keep.max.commits` <br/>
Each commit is a small file in the `.hoodie` directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.

#### withCommitsArchivalBatchSize(batch = 10)
Property: `hoodie.commits.archival.batch` <br/>
This controls the number of commit instants read in memory as a batch and archived together.

#### compactionSmallFileSize(size = 100MB) 
Property: `hoodie.parquet.small.file.limit` <br/>
This should be less &lt; maxFileSize and setting it to 0, turns off this feature. Small files can always happen because of the number of insert records in a partition in a batch. Hudi has an option to auto-resolve small files by masking inserts into this partition as updates to existing small files. The size here is the minimum file size considered as a "small file size".

#### insertSplitSize(size = 500000) 
Property: `hoodie.copyonwrite.insert.split.size` <br/>
Insert Write Parallelism. Number of inserts grouped for a single partition. Writing out 100MB files, with atleast 1kb records, means 100K records per file. Default is to overprovision to 500K. To improve insert latency, tune this to match the number of records in a single file. Setting this to a low number, will result in small files (particularly when compactionSmallFileSize is 0)

#### autoTuneInsertSplits(true) 
Property: `hoodie.copyonwrite.insert.auto.split` <br/>
Should hudi dynamically compute the insertSplitSize based on the last 24 commit's metadata. Turned off by default. 

#### approxRecordSize(size = 1024) 
Property: `hoodie.copyonwrite.record.size.estimate` <br/>
The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit's metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.

#### withInlineCompaction(inlineCompaction = false) 
Property: `hoodie.compact.inline` <br/>
When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert

#### withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction = 10) 
Property: `hoodie.compact.inline.max.delta.commits` <br/>
Number of max delta commits to keep before triggering an inline compaction

#### withCompactionLazyBlockReadEnabled(true) 
Property: `hoodie.compaction.lazy.block.read` <br/>
When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)

#### withCompactionReverseLogReadEnabled(false) 
Property: `hoodie.compaction.reverse.log.read` <br/>
HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0

#### withCleanerParallelism(cleanerParallelism = 200) 
Property: `hoodie.cleaner.parallelism` <br/>
Increase this if cleaning becomes slow.

#### withCompactionStrategy(compactionStrategy = org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy) 
Property: `hoodie.compaction.strategy` <br/>
Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data

#### withTargetIOPerCompactionInMB(targetIOPerCompactionInMB = 500000) 
Property: `hoodie.compaction.target.io` <br/>
Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.

#### withTargetPartitionsPerDayBasedCompaction(targetPartitionsPerCompaction = 10) 
Property: `hoodie.compaction.daybased.target` <br/>
Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.    

#### withPayloadClass(payloadClassName = org.apache.hudi.common.model.HoodieAvroPayload) 
Property: `hoodie.compaction.payload.class` <br/>
This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.


### Metrics configs
Enables reporting of Hudi metrics to graphite.
[withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
Hudi publishes metrics on every commit, clean, rollback etc.

#### on(metricsOn = true) 
Property: `hoodie.metrics.on` <br/>
Turn sending metrics on/off. on by default.

#### withReporterType(reporterType = GRAPHITE) 
Property: `hoodie.metrics.reporter.type` <br/>
Type of metrics reporter. Graphite is the default and the only value suppported.

#### toGraphiteHost(host = localhost) 
Property: `hoodie.metrics.graphite.host` <br/>
Graphite host to connect to

#### onGraphitePort(port = 4756) 
Property: `hoodie.metrics.graphite.port` <br/>
Graphite port to connect to

#### usePrefix(prefix = "") 
Property: `hoodie.metrics.graphite.metric.prefix` <br/>
Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g
    
### Memory configs
Controls memory usage for compaction and merges, performed internally by Hudi
[withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
Memory related configs

#### withMaxMemoryFractionPerPartitionMerge(maxMemoryFractionPerPartitionMerge = 0.6) 
Property: `hoodie.memory.merge.fraction` <br/>
This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge 

#### withMaxMemorySizePerCompactionInBytes(maxMemorySizePerCompactionInBytes = 1GB) 
Property: `hoodie.memory.compaction.fraction` <br/>
HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map.

#### withWriteStatusFailureFraction(failureFraction = 0.1)
Property: `hoodie.memory.writestatus.failure.fraction` <br/>
This property controls what fraction of the failed record, exceptions we report back to driver
