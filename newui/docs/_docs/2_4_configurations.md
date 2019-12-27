---
title: Configurations
keywords: garbage collection, hudi, jvm, configs, tuning
permalink: /docs/configurations.html
summary: "Here we list all possible configurations and what they mean"
toc: false
---
This page covers the different ways of configuring your job to write/read Hudi datasets. 
At a high level, you can control behaviour at few levels. 

- **[Spark Datasource Configs](#spark-datasource)** : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing view type to read.
- **[WriteClient Configs](#writeclient-configs)** : Internally, the Hudi datasource uses a RDD based `HoodieWriteClient` api to actually perform writes to storage. These configs provide deep control over lower level aspects like 
   file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- **[RecordPayload Config](#PAYLOAD_CLASS_OPT_KEY)** : This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and 
   stored old record. Hudi provides default implementations such as `OverwriteWithLatestAvroPayload` which simply update storage with the latest/last-written record. 
   This can be overridden to a custom class extending `HoodieRecordPayload` class, on both datasource and WriteClient levels.
 
### Talking to Cloud Storage

Immaterial of whether RDD/WriteClient APIs or Datasource is used, the following information helps configure access
to cloud stores.

 * [AWS S3](/docs/s3_hoodie) <br/>
   Configurations required for S3 and Hudi co-operability.
 * [Google Cloud Storage](/docs/gcs_hoodie) <br/>
   Configurations required for GCS and Hudi co-operability.

### Spark Datasource Configs {#spark-datasource}

Spark jobs using the datasource can be configured by passing the below options into the `option(k,v)` method as usual.
The actual datasource level configs are listed below.





#### Write Options

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

Options useful for writing datasets via `write.format.option(...)`

##### TABLE_NAME_OPT_KEY {#TABLE_NAME_OPT_KEY}
  Property: `hoodie.datasource.write.table.name` [Required]<br/>
  <span style="color:grey">Hive table name, to register the dataset into.</span>
  
##### OPERATION_OPT_KEY {#OPERATION_OPT_KEY}
  Property: `hoodie.datasource.write.operation`, Default: `upsert`<br/>
  <span style="color:grey">whether to do upsert, insert or bulkinsert for the write operation. Use `bulkinsert` to load new data into a table, and there on use `upsert`/`insert`. 
  bulk insert uses a disk based write path to scale to load large inputs without need to cache it.</span>
  
##### STORAGE_TYPE_OPT_KEY {#STORAGE_TYPE_OPT_KEY}
  Property: `hoodie.datasource.write.storage.type`, Default: `COPY_ON_WRITE` <br/>
  <span style="color:grey">The storage type for the underlying data, for this write. This can't change between writes.</span>
  
##### PRECOMBINE_FIELD_OPT_KEY {#PRECOMBINE_FIELD_OPT_KEY}
  Property: `hoodie.datasource.write.precombine.field`, Default: `ts` <br/>
  <span style="color:grey">Field used in preCombining before actual write. When two records have the same key value,
we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)</span>

##### PAYLOAD_CLASS_OPT_KEY {#PAYLOAD_CLASS_OPT_KEY}
  Property: `hoodie.datasource.write.payload.class`, Default: `org.apache.hudi.OverwriteWithLatestAvroPayload` <br/>
  <span style="color:grey">Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. 
  This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective</span>
  
##### RECORDKEY_FIELD_OPT_KEY {#RECORDKEY_FIELD_OPT_KEY}
  Property: `hoodie.datasource.write.recordkey.field`, Default: `uuid` <br/>
  <span style="color:grey">Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`</span>

##### PARTITIONPATH_FIELD_OPT_KEY {#PARTITIONPATH_FIELD_OPT_KEY}
  Property: `hoodie.datasource.write.partitionpath.field`, Default: `partitionpath` <br/>
  <span style="color:grey">Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
Actual value ontained by invoking .toString()</span>

##### KEYGENERATOR_CLASS_OPT_KEY {#KEYGENERATOR_CLASS_OPT_KEY}
  Property: `hoodie.datasource.write.keygenerator.class`, Default: `org.apache.hudi.SimpleKeyGenerator` <br/>
  <span style="color:grey">Key generator class, that implements will extract the key out of incoming `Row` object</span>
  
##### COMMIT_METADATA_KEYPREFIX_OPT_KEY {#COMMIT_METADATA_KEYPREFIX_OPT_KEY}
  Property: `hoodie.datasource.write.commitmeta.key.prefix`, Default: `_` <br/>
  <span style="color:grey">Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
This is useful to store checkpointing information, in a consistent way with the hudi timeline</span>

##### INSERT_DROP_DUPS_OPT_KEY {#INSERT_DROP_DUPS_OPT_KEY}
  Property: `hoodie.datasource.write.insert.drop.duplicates`, Default: `false` <br/>
  <span style="color:grey">If set to true, filters out all duplicate records from incoming dataframe, during insert operations. </span>
  
##### HIVE_SYNC_ENABLED_OPT_KEY {#HIVE_SYNC_ENABLED_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.enable`, Default: `false` <br/>
  <span style="color:grey">When set to true, register/sync the dataset to Apache Hive metastore</span>
  
##### HIVE_DATABASE_OPT_KEY {#HIVE_DATABASE_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.database`, Default: `default` <br/>
  <span style="color:grey">database to sync to</span>
  
##### HIVE_TABLE_OPT_KEY {#HIVE_TABLE_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.table`, [Required] <br/>
  <span style="color:grey">table to sync to</span>
  
##### HIVE_USER_OPT_KEY {#HIVE_USER_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.username`, Default: `hive` <br/>
  <span style="color:grey">hive user name to use</span>
  
##### HIVE_PASS_OPT_KEY {#HIVE_PASS_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.password`, Default: `hive` <br/>
  <span style="color:grey">hive password to use</span>
  
##### HIVE_URL_OPT_KEY {#HIVE_URL_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.jdbcurl`, Default: `jdbc:hive2://localhost:10000` <br/>
  <span style="color:grey">Hive metastore url</span>
  
##### HIVE_PARTITION_FIELDS_OPT_KEY {#HIVE_PARTITION_FIELDS_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.partition_fields`, Default: ` ` <br/>
  <span style="color:grey">field in the dataset to use for determining hive partition columns.</span>
  
##### HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY {#HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.partition_extractor_class`, Default: `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor` <br/>
  <span style="color:grey">Class used to extract partition field values into hive partition columns.</span>
  
##### HIVE_ASSUME_DATE_PARTITION_OPT_KEY {#HIVE_ASSUME_DATE_PARTITION_OPT_KEY}
  Property: `hoodie.datasource.hive_sync.assume_date_partitioning`, Default: `false` <br/>
  <span style="color:grey">Assume partitioning is yyyy/mm/dd</span>

#### Read Options

Options useful for reading datasets via `read.format.option(...)`

##### VIEW_TYPE_OPT_KEY {#VIEW_TYPE_OPT_KEY}
Property: `hoodie.datasource.view.type`, Default: `read_optimized` <br/>
<span style="color:grey">Whether data needs to be read, in incremental mode (new data since an instantTime)
(or) Read Optimized mode (obtain latest view, based on columnar data)
(or) Real time mode (obtain latest view, based on row & columnar data)</span>

##### BEGIN_INSTANTTIME_OPT_KEY {#BEGIN_INSTANTTIME_OPT_KEY} 
Property: `hoodie.datasource.read.begin.instanttime`, [Required in incremental mode] <br/>
<span style="color:grey">Instant time to start incrementally pulling data from. The instanttime here need not
necessarily correspond to an instant on the timeline. New data written with an
 `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
 all new data written after Sep 1, 2017 08:00AM.</span>
 
##### END_INSTANTTIME_OPT_KEY {#END_INSTANTTIME_OPT_KEY}
Property: `hoodie.datasource.read.end.instanttime`, Default: latest instant (i.e fetches all new data since begin instant time) <br/>
<span style="color:grey"> Instant time to limit incrementally fetched data to. New data written with an
`instant_time <= END_INSTANTTIME` are fetched out.</span>


### WriteClient Configs {#writeclient-configs}

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

##### withPath(hoodie_base_path) {#withPath}
Property: `hoodie.base.path` [Required] <br/>
<span style="color:grey">Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory. </span>

##### withSchema(schema_str) {#withSchema} 
Property: `hoodie.avro.schema` [Required]<br/>
<span style="color:grey">This is the current reader avro schema for the dataset. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. </span>

##### forTable(table_name) {#forTable} 
Property: `hoodie.table.name` [Required] <br/>
 <span style="color:grey">Table name for the dataset, will be used for registering with Hive. Needs to be same across runs.</span>

##### withBulkInsertParallelism(bulk_insert_parallelism = 1500) {#withBulkInsertParallelism} 
Property: `hoodie.bulkinsert.shuffle.parallelism`<br/>
<span style="color:grey">Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your dataset. Tune this to achieve a desired optimal size during initial import.</span>

##### withParallelism(insert_shuffle_parallelism = 1500, upsert_shuffle_parallelism = 1500) {#withParallelism} 
Property: `hoodie.insert.shuffle.parallelism`, `hoodie.upsert.shuffle.parallelism`<br/>
<span style="color:grey">Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data</span>

##### combineInput(on_insert = false, on_update=true) {#combineInput} 
Property: `hoodie.combine.before.insert`, `hoodie.combine.before.upsert`<br/>
<span style="color:grey">Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS</span>

##### withWriteStatusStorageLevel(level = MEMORY_AND_DISK_SER) {#withWriteStatusStorageLevel} 
Property: `hoodie.write.status.storage.level`<br/>
<span style="color:grey">HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD </span>

##### withAutoCommit(autoCommit = true) {#withAutoCommit} 
Property: `hoodie.auto.commit`<br/>
<span style="color:grey">Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a "defined success condition"</span>

##### withAssumeDatePartitioning(assumeDatePartitioning = false) {#withAssumeDatePartitioning} 
Property: `hoodie.assume.date.partitioning`<br/>
<span style="color:grey">Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually </span>

##### withConsistencyCheckEnabled(enabled = false) {#withConsistencyCheckEnabled} 
Property: `hoodie.consistency.check.enabled`<br/>
<span style="color:grey">Should HoodieWriteClient perform additional checks to ensure written files' are listable on the underlying filesystem/storage. Set this to true, to workaround S3's eventual consistency model and ensure all data written as a part of a commit is faithfully available for queries. </span>

#### Index configs
Following configs control indexing behavior, which tags incoming records as either inserts or updates to older records. 

[withIndexConfig](#withIndexConfig) (HoodieIndexConfig) <br/>
<span style="color:grey">This is pluggable to have a external index (HBase) or use the default bloom filter stored in the Parquet files</span>
        
##### withIndexType(indexType = BLOOM) {#withIndexType}
Property: `hoodie.index.type` <br/>
<span style="color:grey">Type of index to use. Default is Bloom filter. Possible options are [BLOOM | HBASE | INMEMORY]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files</span>

##### bloomFilterNumEntries(numEntries = 60000) {#bloomFilterNumEntries}
Property: `hoodie.index.bloom.num_entries` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/>This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries).</span>

##### bloomFilterFPP(fpp = 0.000000001) {#bloomFilterFPP}
Property: `hoodie.index.bloom.fpp` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives</span>

##### bloomIndexPruneByRanges(pruneRanges = true) {#bloomIndexPruneByRanges}
Property: `hoodie.bloom.index.prune.by.ranges` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp.</span>

##### bloomIndexUseCaching(useCaching = true) {#bloomIndexUseCaching}
Property: `hoodie.bloom.index.use.caching` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions</span>

##### bloomIndexTreebasedFilter(useTreeFilter = true) {#bloomIndexTreebasedFilter}
Property: `hoodie.bloom.index.use.treebased.filter` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode</span>

##### bloomIndexBucketizedChecking(bucketizedChecking = true) {#bloomIndexBucketizedChecking}
Property: `hoodie.bloom.index.bucketized.checking` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup</span>

##### bloomIndexKeysPerBucket(keysPerBucket = 10000000) {#bloomIndexKeysPerBucket}
Property: `hoodie.bloom.index.keys.per.bucket` <br/>
<span style="color:grey">Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. <br/> This configuration controls the "bucket" size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory. </span>

##### bloomIndexParallelism(0) {#bloomIndexParallelism}
Property: `hoodie.bloom.index.parallelism` <br/>
<span style="color:grey">Only applies if index type is BLOOM. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics</span>

##### hbaseZkQuorum(zkString) [Required] {#hbaseZkQuorum}  
Property: `hoodie.index.hbase.zkquorum` <br/>
<span style="color:grey">Only applies if index type is HBASE. HBase ZK Quorum url to connect to.</span>

##### hbaseZkPort(port) [Required] {#hbaseZkPort}  
Property: `hoodie.index.hbase.zkport` <br/>
<span style="color:grey">Only applies if index type is HBASE. HBase ZK Quorum port to connect to.</span>

##### hbaseZkZnodeParent(zkZnodeParent)  [Required] {#hbaseTableName}
Property: `hoodie.index.hbase.zknode.path` <br/>
<span style="color:grey">Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase.</span>

##### hbaseTableName(tableName)  [Required] {#hbaseTableName}
Property: `hoodie.index.hbase.table` <br/>
<span style="color:grey">Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table.</span>

    
#### Storage configs
Controls aspects around sizing parquet and log files.

[withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>

##### limitFileSize (size = 120MB) {#limitFileSize}
Property: `hoodie.parquet.max.file.size` <br/>
<span style="color:grey">Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance. </span>

##### parquetBlockSize(rowgroupsize = 120MB) {#parquetBlockSize} 
Property: `hoodie.parquet.block.size` <br/>
<span style="color:grey">Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk</span>

##### parquetPageSize(pagesize = 1MB) {#parquetPageSize} 
Property: `hoodie.parquet.page.size` <br/>
<span style="color:grey">Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately. </span>

##### parquetCompressionRatio(parquetCompressionRatio = 0.1) {#parquetCompressionRatio} 
Property: `hoodie.parquet.compression.ratio` <br/>
<span style="color:grey">Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files</span>

##### parquetCompressionCodec(parquetCompressionCodec = gzip) {#parquetCompressionCodec}
Property: `hoodie.parquet.compression.codec` <br/>
<span style="color:grey">Parquet compression codec name. Default is gzip. Possible options are [gzip | snappy | uncompressed | lzo]</span>

##### logFileMaxSize(logFileSize = 1GB) {#logFileMaxSize} 
Property: `hoodie.logfile.max.size` <br/>
<span style="color:grey">LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version. </span>

##### logFileDataBlockMaxSize(dataBlockSize = 256MB) {#logFileDataBlockMaxSize} 
Property: `hoodie.logfile.data.block.max.size` <br/>
<span style="color:grey">LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory. </span>

##### logFileToParquetCompressionRatio(logFileToParquetCompressionRatio = 0.35) {#logFileToParquetCompressionRatio} 
Property: `hoodie.logfile.to.parquet.compression.ratio` <br/>
<span style="color:grey">Expected additional compression as records move from log files to parquet. Used for merge_on_read storage to send inserts into log files & control the size of compacted parquet file.</span>
 
##### parquetCompressionCodec(parquetCompressionCodec = gzip) {#parquetCompressionCodec} 
Property: `hoodie.parquet.compression.codec` <br/>
<span style="color:grey">Compression Codec for parquet files </span>

#### Compaction configs
Configs that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).
[withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>

##### withCleanerPolicy(policy = KEEP_LATEST_COMMITS) {#withCleanerPolicy} 
Property: `hoodie.cleaner.policy` <br/>
<span style="color:grey"> Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.</span>

##### retainCommits(no_of_commits_to_retain = 24) {#retainCommits} 
Property: `hoodie.cleaner.commits.retained` <br/>
<span style="color:grey">Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this dataset</span>

##### archiveCommitsWith(minCommits = 96, maxCommits = 128) {#archiveCommitsWith} 
Property: `hoodie.keep.min.commits`, `hoodie.keep.max.commits` <br/>
<span style="color:grey">Each commit is a small file in the `.hoodie` directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.</span>

##### withCommitsArchivalBatchSize(batch = 10) {#withCommitsArchivalBatchSize}
Property: `hoodie.commits.archival.batch` <br/>
<span style="color:grey">This controls the number of commit instants read in memory as a batch and archived together.</span>

##### compactionSmallFileSize(size = 100MB) {#compactionSmallFileSize} 
Property: `hoodie.parquet.small.file.limit` <br/>
<span style="color:grey">This should be less < maxFileSize and setting it to 0, turns off this feature. Small files can always happen because of the number of insert records in a partition in a batch. Hudi has an option to auto-resolve small files by masking inserts into this partition as updates to existing small files. The size here is the minimum file size considered as a "small file size".</span>

##### insertSplitSize(size = 500000) {#insertSplitSize} 
Property: `hoodie.copyonwrite.insert.split.size` <br/>
<span style="color:grey">Insert Write Parallelism. Number of inserts grouped for a single partition. Writing out 100MB files, with atleast 1kb records, means 100K records per file. Default is to overprovision to 500K. To improve insert latency, tune this to match the number of records in a single file. Setting this to a low number, will result in small files (particularly when compactionSmallFileSize is 0)</span>

##### autoTuneInsertSplits(true) {#autoTuneInsertSplits} 
Property: `hoodie.copyonwrite.insert.auto.split` <br/>
<span style="color:grey">Should hudi dynamically compute the insertSplitSize based on the last 24 commit's metadata. Turned off by default. </span>

##### approxRecordSize(size = 1024) {#approxRecordSize} 
Property: `hoodie.copyonwrite.record.size.estimate` <br/>
<span style="color:grey">The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit's metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.</span>

##### withInlineCompaction(inlineCompaction = false) {#withInlineCompaction} 
Property: `hoodie.compact.inline` <br/>
<span style="color:grey">When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert</span>

##### withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction = 10) {#withMaxNumDeltaCommitsBeforeCompaction} 
Property: `hoodie.compact.inline.max.delta.commits` <br/>
<span style="color:grey">Number of max delta commits to keep before triggering an inline compaction</span>

##### withCompactionLazyBlockReadEnabled(true) {#withCompactionLazyBlockReadEnabled} 
Property: `hoodie.compaction.lazy.block.read` <br/>
<span style="color:grey">When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)</span>

##### withCompactionReverseLogReadEnabled(false) {#withCompactionReverseLogReadEnabled} 
Property: `hoodie.compaction.reverse.log.read` <br/>
<span style="color:grey">HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0</span>

##### withCleanerParallelism(cleanerParallelism = 200) {#withCleanerParallelism} 
Property: `hoodie.cleaner.parallelism` <br/>
<span style="color:grey">Increase this if cleaning becomes slow.</span>

##### withCompactionStrategy(compactionStrategy = org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy) {#withCompactionStrategy} 
Property: `hoodie.compaction.strategy` <br/>
<span style="color:grey">Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data</span>

##### withTargetIOPerCompactionInMB(targetIOPerCompactionInMB = 500000) {#withTargetIOPerCompactionInMB} 
Property: `hoodie.compaction.target.io` <br/>
<span style="color:grey">Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.</span>

##### withTargetPartitionsPerDayBasedCompaction(targetPartitionsPerCompaction = 10) {#withTargetPartitionsPerDayBasedCompaction} 
Property: `hoodie.compaction.daybased.target` <br/>
<span style="color:grey">Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.</span>    

##### withPayloadClass(payloadClassName = org.apache.hudi.common.model.HoodieAvroPayload) {#payloadClassName} 
Property: `hoodie.compaction.payload.class` <br/>
<span style="color:grey">This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.</span>


    
#### Metrics configs
Enables reporting of Hudi metrics to graphite.
[withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
<span style="color:grey">Hudi publishes metrics on every commit, clean, rollback etc.</span>

##### on(metricsOn = true) {#on} 
Property: `hoodie.metrics.on` <br/>
<span style="color:grey">Turn sending metrics on/off. on by default.</span>

##### withReporterType(reporterType = GRAPHITE) {#withReporterType} 
Property: `hoodie.metrics.reporter.type` <br/>
<span style="color:grey">Type of metrics reporter. Graphite is the default and the only value suppported.</span>

##### toGraphiteHost(host = localhost) {#toGraphiteHost} 
Property: `hoodie.metrics.graphite.host` <br/>
<span style="color:grey">Graphite host to connect to</span>

##### onGraphitePort(port = 4756) {#onGraphitePort} 
Property: `hoodie.metrics.graphite.port` <br/>
<span style="color:grey">Graphite port to connect to</span>

##### usePrefix(prefix = "") {#usePrefix} 
Property: `hoodie.metrics.graphite.metric.prefix` <br/>
<span style="color:grey">Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g</span>
    
#### Memory configs
Controls memory usage for compaction and merges, performed internally by Hudi
[withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
<span style="color:grey">Memory related configs</span>

##### withMaxMemoryFractionPerPartitionMerge(maxMemoryFractionPerPartitionMerge = 0.6) {#withMaxMemoryFractionPerPartitionMerge} 
Property: `hoodie.memory.merge.fraction` <br/>
<span style="color:grey">This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge </span>

##### withMaxMemorySizePerCompactionInBytes(maxMemorySizePerCompactionInBytes = 1GB) {#withMaxMemorySizePerCompactionInBytes} 
Property: `hoodie.memory.compaction.fraction` <br/>
<span style="color:grey">HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map.</span>

##### withWriteStatusFailureFraction(failureFraction = 0.1) {#withWriteStatusFailureFraction}
Property: `hoodie.memory.writestatus.failure.fraction` <br/>
<span style="color:grey">This property controls what fraction of the failed record, exceptions we report back to driver</span>
