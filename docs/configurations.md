---
title: Configurations
keywords: configurations
sidebar: mydoc_sidebar
permalink: configurations.html
toc: false
summary: "Here we list all possible configurations and what they mean"
---

### Configuration

* [HoodieWriteConfig](#HoodieWriteConfig) <br/>
<span style="color:grey">Top Level Config which is passed in when HoodieWriteClent is created.</span>
    - [withPath](#withPath) (hoodie_base_path) <br/>
    <span style="color:grey">Base HDFS path under which all the data partitions are created. Hoodie stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory. </span>
    - [withSchema](#withSchema) (schema_str) <br/>
    <span style="color:grey">This is the current reader avro schema for the Hoodie Dataset. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. </span>
    - [withParallelism](#withParallelism) (insert_shuffle_parallelism = 200, upsert_shuffle_parallelism = 200) <br/>
    <span style="color:grey">Insert DAG uses the insert_parallelism in every shuffle. Upsert DAG uses the upsert_parallelism in every shuffle. Typical workload is profiled and once a min parallelism is established, trade off between latency and cluster usage optimizations this is tuned and have a conservatively high number to optimize for latency and  </span>
    - [combineInput](#combineInput) (on_insert = false, on_update=true) <br/>
    <span style="color:grey">Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in HDFS</span>
    - [withWriteStatusStorageLevel](#withWriteStatusStorageLevel) (level = MEMORY_AND_DISK_SER) <br/>
    <span style="color:grey">HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD </span>
    - [withAutoCommit](#withAutoCommit) (autoCommit = true) <br/>
    <span style="color:grey">Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a "defined success condition"</span>
    - [withAssumeDatePartitioning](#withAssumeDatePartitioning) (assumeDatePartitioning = false) <br/>
        <span style="color:grey">Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually </span>

    - [withIndexConfig](#withIndexConfig) (HoodieIndexConfig) <br/>
    <span style="color:grey">Hoodie uses a index to help find the FileID which contains an incoming record key. This is pluggable to have a external index (HBase) or use the default bloom filter stored in the Parquet files</span>
        - [withIndexType](#withIndexType) (indexType = BLOOM) <br/>
        <span style="color:grey">Type of index to use. Default is Bloom filter. Possible options are [BLOOM | HBASE | INMEMORY]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files</span>
        - [bloomFilterNumEntries](#bloomFilterNumEntries) (60000) <br/>
        <span style="color:grey">Only applies if index type is BLOOM. <br/>This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. [#70](https://github.com/uber/hoodie/issues/70) tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries).</span>
        - [bloomFilterFPP](#bloomFilterFPP) (0.000000001) <br/>
        <span style="color:grey">Only applies if index type is BLOOM. <br/> Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives</span>
        - [bloomIndexPruneByRanges](#bloomIndexPruneByRanges) (true) <br/>
        <span style="color:grey">Only applies if index type is BLOOM. <br/> When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp.</span>
        - [bloomIndexUseCaching](#bloomIndexUseCaching) (true) <br/>
        <span style="color:grey">Only applies if index type is BLOOM. <br/> When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions</span>
        - [bloomIndexParallelism](#bloomIndexParallelism) (0) <br/>
        <span style="color:grey">Only applies if index type is BLOOM. <br/> This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics</span>
        - [hbaseZkQuorum](#hbaseZkQuorum) (zkString) <br/>
        <span style="color:grey">Only application if index type is HBASE. HBase ZK Quorum url to connect to.</span>
        - [hbaseZkPort](#hbaseZkPort) (port) <br/>
        <span style="color:grey">Only application if index type is HBASE. HBase ZK Quorum port to connect to.</span>
        - [hbaseTableName](#hbaseTableName) (tableName) <br/>
        <span style="color:grey">Only application if index type is HBASE. HBase Table name to use as the index. Hoodie stores the row_key and [partition_path, fileID, commitTime] mapping in the table.</span>

    - [withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>
    <span style="color:grey">Storage related configs</span>
        - [limitFileSize](#limitFileSize) (size = 120MB) <br/>
        <span style="color:grey">Hoodie re-writes a single file during update (copy_on_write) or a compaction (merge_on_read). This is fundamental unit of parallelism. It is important that this is aligned with the underlying filesystem block size. </span>
        - [parquetBlockSize](#parquetBlockSize) (rowgroupsize = 120MB) <br/>
        <span style="color:grey">Parquet RowGroup size. Its better than this is aligned with the file size, so that a single column within a file is stored continuously on disk</span>
        - [parquetPageSize](#parquetPageSize) (pagesize = 1MB) <br/>
        <span style="color:grey">Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately. </span>
        - [logFileMaxSize](#logFileMaxSize) (logFileSize = 1GB) <br/>
        <span style="color:grey">LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version. </span>
        - [logFileDataBlockMaxSize](#logFileDataBlockMaxSize) (dataBlockSize = 256MB) <br/>
        <span style="color:grey">LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory. </span>

    - [withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>
    <span style="color:grey">Cleaning and configurations related to compaction techniques</span>
        - [withCleanerPolicy](#withCleanerPolicy) (policy = KEEP_LATEST_COMMITS) <br/>
        <span style="color:grey">Hoodie Cleaning policy. Hoodie will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.</span>
        - [retainCommits](#retainCommits) (no_of_commits_to_retain = 24) <br/>
        <span style="color:grey">Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this dataset</span>
        - [archiveCommitsWith](#archiveCommitsWith) (minCommits = 96, maxCommits = 128) <br/>
        <span style="color:grey">Each commit is a small file in the .hoodie directory. Since HDFS is not designed to handle multiple small files, hoodie archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.</span>
        - [compactionSmallFileSize](#compactionSmallFileSize) (size = 0) <br/>
        <span style="color:grey">Small files can always happen because of the number of insert records in a paritition in a batch. Hoodie has an option to auto-resolve small files by masking inserts into this partition as updates to existing small files. The size here is the minimum file size considered as a "small file size". This should be less < maxFileSize and setting it to 0, turns off this feature. </span>
        - [insertSplitSize](#insertSplitSize) (size = 500000) <br/>
        <span style="color:grey">Insert Write Parallelism. Number of inserts grouped for a single partition. Writing out 100MB files, with atleast 1kb records, means 100K records per file. Default is to overprovision to 500K. To improve insert latency, tune this to match the number of records in a single file. Setting this to a low number, will result in small files (particularly when compactionSmallFileSize is 0)</span>
        - [autoTuneInsertSplits](#autoTuneInsertSplits) (false) <br/>
        <span style="color:grey">Should hoodie dynamically compute the insertSplitSize based on the last 24 commit's metadata. Turned off by default. </span>
        - [approxRecordSize](#approxRecordSize) () <br/>
        <span style="color:grey">The average record size. If specified, hoodie will use this and not compute dynamically based on the last 24 commit's metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.</span>
        - [withCompactionLazyBlockReadEnabled](#withCompactionLazyBlockReadEnabled) (true) <br/>
        <span style="color:grey">When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)</span>
        - [withMaxNumDeltaCommitsBeforeCompaction](#withMaxNumDeltaCommitsBeforeCompaction) (maxNumDeltaCommitsBeforeCompaction = 10) <br/>
        <span style="color:grey">Number of max delta commits to keep before triggering an inline compaction</span>
        - [withCompactionReverseLogReadEnabled](#withCompactionReverseLogReadEnabled) (false) <br/>
        <span style="color:grey">HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0</span>

    - [withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
    <span style="color:grey">Hoodie publishes metrics on every commit, clean, rollback etc.</span>
        - [on](#on) (true) <br/>
        <span style="color:grey">Turn sending metrics on/off. on by default.</span>
        - [withReporterType](#withReporterType) (GRAPHITE) <br/>
        <span style="color:grey">Type of metrics reporter. Graphite is the default and the only value suppported.</span>
        - [toGraphiteHost](#toGraphiteHost) () <br/>
        <span style="color:grey">Graphite host to connect to</span>
        - [onGraphitePort](#onGraphitePort) () <br/>
        <span style="color:grey">Graphite port to connect to</span>
        - [usePrefix](#usePrefix) () <br/>
        <span style="color:grey">Standard prefix for all metrics</span>

    - [withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
    <span style="color:grey">Memory related configs</span>
        - [withMaxMemoryFractionPerPartitionMerge](#withMaxMemoryFractionPerPartitionMerge) (maxMemoryFractionPerPartitionMerge = 0.6) <br/>
        <span style="color:grey">This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge </span>
        - [withMaxMemorySizePerCompactionInBytes](#withMaxMemorySizePerCompactionInBytes) (maxMemorySizePerCompactionInBytes = 1GB) <br/>
        <span style="color:grey">HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map.</span>

    - [S3Configs](s3_hoodie.html) (Hoodie S3 Configs) <br/>
    <span style="color:grey">Configurations required for S3 and Hoodie co-operability.</span>

    - [GCSConfigs](gcs_hoodie.html) (Hoodie GCS Configs) <br/>
    <span style="color:grey">Configurations required for GCS and Hoodie co-operability.</span>

* [Hoodie Datasource](#datasource) <br/>
<span style="color:grey">Configs for datasource</span>
    - [write options](#writeoptions) (write.format.option(...)) <br/>
    <span style="color:grey"> Options useful for writing datasets </span>
        - [OPERATION_OPT_KEY](#OPERATION_OPT_KEY) (Default: upsert) <br/>
        <span style="color:grey">whether to do upsert, insert or bulkinsert for the write operation</span>
        - [STORAGE_TYPE_OPT_KEY](#STORAGE_TYPE_OPT_KEY) (Default: COPY_ON_WRITE) <br/>
        <span style="color:grey">The storage type for the underlying data, for this write. This can't change between writes.</span>
        - [TABLE_NAME_OPT_KEY](#TABLE_NAME_OPT_KEY) (Default: None (mandatory)) <br/>
        <span style="color:grey">Hive table name, to register the dataset into.</span>
        - [PRECOMBINE_FIELD_OPT_KEY](#PRECOMBINE_FIELD_OPT_KEY) (Default: ts) <br/>
        <span style="color:grey">Field used in preCombining before actual write. When two records have the same key value,
        we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)</span>
        - [PAYLOAD_CLASS_OPT_KEY](#PAYLOAD_CLASS_OPT_KEY) (Default: com.uber.hoodie.OverwriteWithLatestAvroPayload) <br/>
        <span style="color:grey">Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
        This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective</span>
        - [RECORDKEY_FIELD_OPT_KEY](#RECORDKEY_FIELD_OPT_KEY) (Default: uuid) <br/>
        <span style="color:grey">Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
        will be obtained by invoking .toString() on the field value. Nested fields can be specified using
        the dot notation eg: `a.b.c`</span>
        - [PARTITIONPATH_FIELD_OPT_KEY](#PARTITIONPATH_FIELD_OPT_KEY) (Default: partitionpath) <br/>
        <span style="color:grey">Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.
        Actual value ontained by invoking .toString()</span>
        - [KEYGENERATOR_CLASS_OPT_KEY](#KEYGENERATOR_CLASS_OPT_KEY) (Default: com.uber.hoodie.SimpleKeyGenerator) <br/>
        <span style="color:grey">Key generator class, that implements will extract the key out of incoming `Row` object</span>
        - [COMMIT_METADATA_KEYPREFIX_OPT_KEY](#COMMIT_METADATA_KEYPREFIX_OPT_KEY) (Default: _) <br/>
        <span style="color:grey">Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
        This is useful to store checkpointing information, in a consistent way with the hoodie timeline</span>

    - [read options](#readoptions) (read.format.option(...)) <br/>
    <span style="color:grey">Options useful for reading datasets</span>
        - [VIEW_TYPE_OPT_KEY](#VIEW_TYPE_OPT_KEY) (Default:  = read_optimized) <br/>
        <span style="color:grey">Whether data needs to be read, in incremental mode (new data since an instantTime)
        (or) Read Optimized mode (obtain latest view, based on columnar data)
        (or) Real time mode (obtain latest view, based on row & columnar data)</span>
        - [BEGIN_INSTANTTIME_OPT_KEY](#BEGIN_INSTANTTIME_OPT_KEY) (Default: None (Mandatory in incremental mode)) <br/>
        <span style="color:grey">Instant time to start incrementally pulling data from. The instanttime here need not
        necessarily correspond to an instant on the timeline. New data written with an
         `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
         all new data written after Sep 1, 2017 08:00AM.</span>
        - [END_INSTANTTIME_OPT_KEY](#END_INSTANTTIME_OPT_KEY) (Default: latest instant (i.e fetches all new data since begin instant time)) <br/>
        <span style="color:grey"> Instant time to limit incrementally fetched data to. New data written with an
        `instant_time <= END_INSTANTTIME` are fetched out.</span>


### Tuning

Writing data via Hoodie happens as a Spark job and thus general rules of spark debugging applies here too. Below is a list of things to keep in mind, if you are looking to improving performance or reliability.

 - **Right operations** : Use `bulkinsert` to load new data into a table, and there on use `upsert`/`insert`. Difference between them is that bulk insert uses a disk based write path to scale to load large inputs without need to cache it.
 - **Input Parallelism** : By default, Hoodie tends to over-partition input (i.e `withParallelism(1500)`), to ensure each Spark partition stays within the 2GB limit for inputs upto 500GB. Bump this up accordingly if you have larger inputs. We recommend having shuffle parallelism `hoodie.[insert|upsert|bulkinsert].shuffle.parallelism` such that its atleast input_data_size/500MB
 - **Off-heap memory** : Hoodie writes parquet files and that needs good amount of off-heap memory proportional to schema width. Consider setting something like `spark.yarn.executor.memoryOverhead` or `spark.yarn.driver.memoryOverhead`, if you are running into such failures.
 - **Spark Memory** : Typically, hoodie needs to be able to read a single file into memory to perform merges or compactions and thus the executor memory should be sufficient to accomodate this. In addition, Hoodie caches the input to be able to intelligently place data and thus leaving some `spark.storage.memoryFraction` will generally help boost performance.
 - **Sizing files** : Set `limitFileSize` above judiciously, to balance ingest/write latency vs number of files & consequently metadata overhead associated with it.
 - **Timeseries/Log data** : Default configs are tuned for database/nosql changelogs where individual record sizes are large. Another very popular class of data is timeseries/event/log data that tends to be more volumnious with lot more records per partition. In such cases
    - Consider tuning the bloom filter accuracy via `.bloomFilterFPP()/bloomFilterNumEntries()` to achieve your target index look up time
    - Consider making a key that is prefixed with time of the event, which will enable range pruning & significantly speeding up index lookup.
 - **GC Tuning** : Please be sure to follow garbage collection tuning tips from Spark tuning guide to avoid OutOfMemory errors
    - [Must] Use G1/CMS Collector. Sample CMS Flags to add to spark.executor.extraJavaOptions : ``-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof`
    - If it keeps OOMing still, reduce spark memory conservatively: `spark.memory.fraction=0.2, spark.memory.storageFraction=0.2` allowing it to spill rather than OOM. (reliably slow vs crashing intermittently)

 Below is a full working production config

 ```
 spark.driver.extraClassPath    /etc/hive/conf
 spark.driver.extraJavaOptions    -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
 spark.driver.maxResultSize    2g
 spark.driver.memory    4g
 spark.executor.cores    1
 spark.executor.extraJavaOptions    -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
 spark.executor.id    driver
 spark.executor.instances    300
 spark.executor.memory    6g
 spark.rdd.compress true

 spark.kryoserializer.buffer.max    512m
 spark.serializer    org.apache.spark.serializer.KryoSerializer
 spark.shuffle.memoryFraction    0.2
 spark.shuffle.service.enabled    true
 spark.sql.hive.convertMetastoreParquet    false
 spark.storage.memoryFraction    0.6
 spark.submit.deployMode    cluster
 spark.task.cpus    1
 spark.task.maxFailures    4

 spark.yarn.driver.memoryOverhead    1024
 spark.yarn.executor.memoryOverhead    3072
 spark.yarn.max.executor.failures    100
 ```
