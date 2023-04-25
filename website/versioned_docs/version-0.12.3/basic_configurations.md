---
title: Basic Configurations
toc: true
---

This page covers the basic configurations you may use to write/read Hudi tables. This page only features a subset of the
most frequently used configurations. For a full list of all configs, please visit the [All Configurations](/docs/configurations) page.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of key Hudi stats and metrics.
- [**Record Payload Config**](#RECORD_PAYLOAD): This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.

### Read Options {#Read-Options}

Options useful for reading tables via `read.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br></br>
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

> #### hoodie.datasource.write.operation
> Whether to do upsert, insert or bulkinsert for the write operation. Use bulkinsert to load new data into a table, and there after use upsert/insert. bulk insert uses a disk based write path to scale to load large inputs without need to cache it.<br></br>
> **Default Value**: upsert (Optional)<br></br>
> `Config Param: OPERATION`<br></br>

---

> #### hoodie.datasource.write.table.type
> The table type for the underlying data, for this write. This can’t change between writes.<br></br>
> **Default Value**: COPY_ON_WRITE (Optional)<br></br>
> `Config Param: TABLE_TYPE`<br></br>

---

> #### hoodie.datasource.write.table.name
> Table name for the datasource write. Also used to register the table into meta stores.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_NAME`<br></br>

---

> #### hoodie.datasource.write.recordkey.field
> Record key field. Value to be used as the `recordKey` component of `HoodieKey`.
Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using
the dot notation eg: `a.b.c`<br></br>
> **Default Value**: uuid (Optional)<br></br>
> `Config Param: RECORDKEY_FIELD`<br></br>

---

> #### hoodie.datasource.write.partitionpath.field
> Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PARTITIONPATH_FIELD`<br></br>

---

> #### hoodie.datasource.write.keygenerator.class
> Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator`<br></br>
> **Default Value**: org.apache.hudi.keygen.SimpleKeyGenerator (Optional)<br></br>
> `Config Param: KEYGENERATOR_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: PRECOMBINE_FIELD`<br></br>

---

> #### hoodie.datasource.write.payload.class
> Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br></br>
> **Default Value**: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional)<br></br>
> `Config Param: PAYLOAD_CLASS_NAME`<br></br>

---

> #### hoodie.datasource.write.partitionpath.urlencode
> Should we url encode the partition path value, before creating the folder structure.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: URL_ENCODE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.hive_sync.enable
> When set to true, register/sync the table to Apache Hive metastore<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_ENABLED`<br></br>

---

> #### hoodie.datasource.hive_sync.mode
> Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: HIVE_SYNC_MODE`<br></br>

---

> #### hoodie.datasource.write.hive_style_partitioning
> Flag to indicate whether to use Hive style partitioning.
If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
By default false (the names of partition folders are only partition values)<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_STYLE_PARTITIONING`<br></br>

---

> #### hoodie.datasource.hive_sync.partition_fields
> Field in the table to use for determining hive partition columns.<br></br>
> **Default Value**: (Optional)<br></br>
> `Config Param: HIVE_PARTITION_FIELDS`<br></br>

---

> #### hoodie.datasource.hive_sync.partition_extractor_class
> Class which implements PartitionValueExtractor to extract the partition values, default 'MultiPartKeysValueExtractor'.<br></br>
> **Default Value**: org.apache.hudi.hive.MultiPartKeysValueExtractor (Optional)<br></br>
> `Config Param: HIVE_PARTITION_EXTRACTOR_CLASS`<br></br>

---

## Flink Sql Configs {#FLINK_SQL}
These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.

### Flink Options {#Flink-Options}

> #### path
> Base path for the target hoodie table.
The path would be created if it does not exist,
otherwise a Hoodie table expects to be initialized successfully<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: PATH`<br></br>

---

> #### hoodie.table.name
> Table name to register to Hive metastore<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: TABLE_NAME`<br></br>

---


> #### table.type
> Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ<br></br>
> **Default Value**: COPY_ON_WRITE (Optional)<br></br>
> `Config Param: TABLE_TYPE`<br></br>

---

> #### write.operation
> The write operation, that this write should do<br></br>
> **Default Value**: upsert (Optional)<br></br>
> `Config Param: OPERATION`<br></br>

---

> #### write.tasks
> Parallelism of tasks that do actual write, default is 4<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: WRITE_TASKS`<br></br>

---

> #### write.bucket_assign.tasks
> Parallelism of tasks that do bucket assign, default is the parallelism of the execution environment<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: BUCKET_ASSIGN_TASKS`<br></br>

---

> #### write.precombine
> Flag to indicate whether to drop duplicates before insert/upsert.
By default these cases will accept duplicates, to gain extra performance:
1) insert operation;
2) upsert for MOR table, the MOR table deduplicate on reading<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: PRE_COMBINE`<br></br>

---

> #### read.tasks
> Parallelism of tasks that do actual read, default is 4<br></br>
> **Default Value**: 4 (Optional)<br></br>
> `Config Param: READ_TASKS`<br></br>

---

> #### read.start-commit
> Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss', by default reading from the latest instant for streaming read<br></br>
> **Default Value**: N/A (Required)<br></br>
> `Config Param: READ_START_COMMIT`<br></br>

---

> #### read.streaming.enabled
> Whether to read as streaming source, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: READ_AS_STREAMING`<br></br>

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

> #### hive_sync.enable
> Asynchronously sync Hive meta to HMS, default false<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: HIVE_SYNC_ENABLED`<br></br>

---

> #### hive_sync.mode
> Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'jdbc'<br></br>
> **Default Value**: jdbc (Optional)<br></br>
> `Config Param: HIVE_SYNC_MODE`<br></br>

---

> #### hive_sync.table
> Table name for hive sync, default 'unknown'<br></br>
> **Default Value**: unknown (Optional)<br></br>
> `Config Param: HIVE_SYNC_TABLE`<br></br>

---

> #### hive_sync.db
> Database name for hive sync, default 'default'<br></br>
> **Default Value**: default (Optional)<br></br>
> `Config Param: HIVE_SYNC_DB`<br></br>

---

> #### hive_sync.partition_extractor_class
> Tool to extract the partition value from HDFS path, default 'SlashEncodedDayPartitionValueExtractor'<br></br>
> **Default Value**: org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor (Optional)<br></br>
> `Config Param: HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME`<br></br>

---
> #### hive_sync.metastore.uris
> Metastore uris for hive sync, default ''<br></br>
> **Default Value**: (Optional)<br></br>
> `Config Param: HIVE_SYNC_METASTORE_URIS`<br></br>

---


## Write Client Configs {#WRITE_CLIENT}
Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.

### Storage Configs

Configurations that control aspects around writing, sizing, reading base and log files.

`Config Class`: org.apache.hudi.config.HoodieStorageConfig<br></br>

> #### write.parquet.block.size
> Parquet RowGroup size. It's recommended to make this large enough that scan costs can be amortized by packing enough column values into a single row group.<br></br>
> **Default Value**: 120 (Optional)<br></br>
> `Config Param: WRITE_PARQUET_BLOCK_SIZE`<br></br>

---

> #### write.parquet.max.file.size
> Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br></br>
> **Default Value**: 120 (Optional)<br></br>
> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE`<br></br>

---

### Metadata Configs

Configurations used by the Hudi Metadata Table. This table maintains the metadata about a given Hudi table (e.g file listings) to avoid overhead of accessing cloud storage, during queries.

`Config Class`: org.apache.hudi.common.config.HoodieMetadataConfig<br></br>

> #### hoodie.metadata.enable
> Enable the internal metadata table which serves table metadata like level file listings<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ENABLE`<br></br>
> `Since Version: 0.7.0`<br></br>

---

### Write Configurations

Configurations that control write behavior on Hudi tables. These can be directly passed down from even higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g DeltaStreamer).

`Config Class`: org.apache.hudi.config.HoodieWriteConfig<br></br>

> #### hoodie.combine.before.upsert
> When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming, otherwise it can lead to duplicate keys and violate the uniqueness guarantees.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_UPSERT`<br></br>

---

> #### hoodie.write.markers.type
> Marker type to use. Two modes are supported: - DIRECT: individual marker file corresponding to each data file is directly created by the writer. - TIMELINE_SERVER_BASED: marker operations are all handled at the timeline service which serves as a proxy. New marker entries are batch processed and stored in a limited number of underlying files for efficiency. If HDFS is used or timeline server is disabled, DIRECT markers are used as fallback even if this is configured. For Spark structured streaming, this configuration does not take effect, i.e., DIRECT markers are always used for Spark structured streaming.<br></br>
> **Default Value**: TIMELINE_SERVER_BASED (Optional)<br></br>
> `Config Param: MARKERS_TYPE`<br></br>
> `Since Version: 0.9.0`<br></br>

---

> #### hoodie.insert.shuffle.parallelism
> Parallelism for inserting records into the table. Inserts can shuffle data before writing to tune file sizes and optimize the storage layout.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: INSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.rollback.parallelism
> Parallelism for rollback of commits. Rollbacks perform delete of files or logging delete blocks to file groups on storage in parallel.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: ROLLBACK_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.combine.before.delete
> During delete operations, controls whether we should combine deletes (and potentially also upserts) before writing to storage.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_DELETE`<br></br>

---

> #### hoodie.combine.before.insert
> When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before writing to storage. When set to true the 
> precombine field value is used to reduce all records that share the same key. <br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: COMBINE_BEFORE_INSERT`<br></br>

---

> #### hoodie.bulkinsert.shuffle.parallelism
> For large initial imports using bulk_insert operation, controls the parallelism to use for sort modes or custom partitioning done before writing records to the table.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: BULKINSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.delete.shuffle.parallelism
> Parallelism used for “delete” operation. Delete operations also perform shuffles, similar to upsert operation.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: DELETE_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.bulkinsert.sort.mode
> Sorting modes to use for sorting records for bulk insert. This is used when user hoodie.bulkinsert.user.defined.partitioner.class is not configured. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads<br></br>
> **Default Value**: GLOBAL_SORT (Optional)<br></br>
> `Config Param: BULK_INSERT_SORT_MODE`<br></br>

---

> #### hoodie.embed.timeline.server
> When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),running on each writer's driver process, accepting requests during the write from executors.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: EMBEDDED_TIMELINE_SERVER_ENABLE`<br></br>

---

> #### hoodie.upsert.shuffle.parallelism
> Parallelism to use for upsert operation on the table. Upserts can shuffle data to perform index lookups, file sizing, bin packing records optimally into file groups.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: UPSERT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.rollback.using.markers
> Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned on by default.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ROLLBACK_USING_MARKERS_ENABLE`<br></br>

---

> #### hoodie.finalize.write.parallelism
> Parallelism for the write finalization internal operation, which involves removing any partially written files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables or low latency writes.<br></br>
> **Default Value**: 200 (Optional)<br></br>
> `Config Param: FINALIZE_WRITE_PARALLELISM_VALUE`<br></br>

---

### Compaction Configs {#Compaction-Configs}

Configurations that control compaction (merging of log files onto a new base files).

`Config Class`: org.apache.hudi.config.HoodieCompactionConfig<br></br>
> #### hoodie.compaction.lazy.block.read
> When merging the delta log files, this config helps to choose whether the log blocks should be read lazily or not. Choose true to use lazy block reading (low memory usage, but incurs seeks to each block header) or false for immediate block read (higher memory usage)<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: COMPACTION_LAZY_BLOCK_READ_ENABLE`<br></br>

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

> #### hoodie.copyonwrite.record.size.estimate
> The average record size. If not explicitly specified, hudi will compute the record size estimate compute dynamically based on commit metadata.  This is critical in computing the insert parallelism and bin-packing inserts into small files.<br></br>
> **Default Value**: 1024 (Optional)<br></br>
> `Config Param: COPY_ON_WRITE_RECORD_SIZE_ESTIMATE`<br></br>

---

> #### hoodie.compact.inline.max.delta.seconds
> Number of elapsed seconds after the last compaction, before scheduling a new one.<br></br>
> **Default Value**: 3600 (Optional)<br></br>
> `Config Param: INLINE_COMPACT_TIME_DELTA_SECONDS`<br></br>

---

> #### hoodie.compaction.target.io
> Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.<br></br>
> **Default Value**: 512000 (Optional)<br></br>
> `Config Param: TARGET_IO_PER_COMPACTION_IN_MB`<br></br>

---

> #### hoodie.compaction.logfile.size.threshold
> Only if the log file size is greater than the threshold in bytes, the file group will be compacted.<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: COMPACTION_LOG_FILE_SIZE_THRESHOLD`<br></br>

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

> #### hoodie.record.size.estimation.threshold
> We use the previous commits' metadata to calculate the estimated record size and use it  to bin pack records into partitions. If the previous commit is too small to make an accurate estimation,  Hudi will search commits in the reverse order, until we find a commit that has totalBytesWritten  larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * this_threshold)<br></br>
> **Default Value**: 1.0 (Optional)<br></br>
> `Config Param: RECORD_SIZE_ESTIMATION_THRESHOLD`<br></br>

---

> #### hoodie.compact.inline.trigger.strategy
> Controls how compaction scheduling is triggered, by time or num delta commits or combination of both. Valid options: NUM_COMMITS,NUM_COMMITS_AFTER_LAST_REQUEST,TIME_ELAPSED,NUM_AND_TIME,NUM_OR_TIME<br></br>
> **Default Value**: NUM_COMMITS (Optional)<br></br>
> `Config Param: INLINE_COMPACT_TRIGGER_STRATEGY`<br></br>

---

> #### hoodie.compaction.reverse.log.read
> HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the reader reads the logfile in reverse direction, from pos=file_length to pos=0<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: COMPACTION_REVERSE_LOG_READ_ENABLE`<br></br>

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

> #### hoodie.compact.inline
> When set to true, compaction service is triggered after each write. While being  simpler operationally, this adds extra latency on the write path.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: INLINE_COMPACT`<br></br>

---

### Clean Configs {#Clean-Configs}

Cleaning (reclamation of older/unused file groups/slices).

`Config Class`: org.apache.hudi.config.HoodieCleanConfig<br></br>
> #### hoodie.cleaner.fileversions.retained
> When KEEP_LATEST_FILE_VERSIONS cleaning policy is used,  the minimum number of file slices to retain in each file group, during cleaning.<br></br>
> **Default Value**: 3 (Optional)<br></br>
> `Config Param: CLEANER_FILE_VERSIONS_RETAINED`<br></br>

---

> #### hoodie.clean.max.commits
> Number of commits after the last clean operation, before scheduling of a new clean is attempted.<br></br>
> **Default Value**: 1 (Optional)<br></br>
> `Config Param: CLEAN_MAX_COMMITS`<br></br>

---

> #### hoodie.clean.allow.multiple
> Allows scheduling/executing multiple cleans by enabling this config. If users prefer to strictly ensure clean requests should be mutually exclusive, .i.e. a 2nd clean will not be scheduled if another clean is not yet completed to avoid repeat cleaning of same files, they might want to disable this config.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: ALLOW_MULTIPLE_CLEANS`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.clean.automatic
> When enabled, the cleaner table service is invoked immediately after each commit, to delete older file slices. It's recommended to enable this, to ensure metadata and data storage growth is bounded.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: AUTO_CLEAN`<br></br>

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

> #### hoodie.clean.async
> Only applies when hoodie.clean.automatic is turned on. When turned on runs cleaner async with writing, which can speed up overall write performance.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_CLEAN`<br></br>

---

> #### hoodie.clean.trigger.strategy
> Controls how cleaning is scheduled. Valid options: NUM_COMMITS<br></br>
> **Default Value**: NUM_COMMITS (Optional)<br></br>
> `Config Param: CLEAN_TRIGGER_STRATEGY`<br></br>

---

> #### hoodie.cleaner.delete.bootstrap.base.file
> When set to true, cleaner also deletes the bootstrap base file when it's skeleton base file is  cleaned. Turn this to true, if you want to ensure the bootstrap dataset storage is reclaimed over time, as the table receives updates/deletes. Another reason to turn this on, would be to ensure data residing in bootstrap  base files are also physically deleted, to comply with data privacy enforcement processes.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: CLEANER_BOOTSTRAP_BASE_FILE_ENABLE`<br></br>

---

> #### hoodie.cleaner.hours.retained
> Number of hours for which commits need to be retained. This config provides a more flexible option ascompared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group, corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.<br></br>
> **Default Value**: 24 (Optional)<br></br>
> `Config Param: CLEANER_HOURS_RETAINED`<br></br>

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

> #### hoodie.cleaner.policy
> Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space. By default, cleaner spares the file slices written by the last N commits, determined by  hoodie.cleaner.commits.retained Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had   a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time<br></br>
> **Default Value**: KEEP_LATEST_COMMITS (Optional)<br></br>
> `Config Param: CLEANER_POLICY`<br></br>

---

### Archival Configs {#Archival-Configs}

Configurations that control archival.

`Config Class`: org.apache.hudi.config.HoodieArchivalConfig<br></br>
> #### hoodie.archive.merge.small.file.limit.bytes
> This config sets the archive file size limit below which an archive file becomes a candidate to be selected as such a small file.<br></br>
> **Default Value**: 20971520 (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES`<br></br>

---

> #### hoodie.keep.max.commits
> Archiving service moves older entries from timeline into an archived log after each write, to  keep the metadata overhead constant, even as the table size grows.This config controls the maximum number of instants to retain in the active timeline. <br></br>
> **Default Value**: 30 (Optional)<br></br>
> `Config Param: MAX_COMMITS_TO_KEEP`<br></br>

---

> #### hoodie.archive.merge.enable
> When enable, hoodie will auto merge several small archive files into larger one. It's useful when storage scheme doesn't support append operation.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_ENABLE`<br></br>

---

> #### hoodie.archive.automatic
> When enabled, the archival table service is invoked immediately after each commit, to archive commits if we cross a maximum value of commits. It's recommended to enable this, to ensure number of active commits is bounded.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: AUTO_ARCHIVE`<br></br>

---

> #### hoodie.archive.delete.parallelism
> Parallelism for deleting archived hoodie commits.<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE`<br></br>

---

> #### hoodie.archive.beyond.savepoint
> If enabled, archival will proceed beyond savepoint, skipping savepoint commits. If disabled, archival will stop at the earliest savepoint commit.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ARCHIVE_BEYOND_SAVEPOINT`<br></br>
> `Since Version: 0.12.0`<br></br>

---

> #### hoodie.commits.archival.batch
> Archiving of instants is batched in best-effort manner, to pack more instants into a single archive log. This config controls such archival batch size.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: COMMITS_ARCHIVAL_BATCH_SIZE`<br></br>

---

> #### hoodie.archive.async
> Only applies when hoodie.archive.automatic is turned on. When turned on runs archiver async with writing, which can speed up overall write performance.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: ASYNC_ARCHIVE`<br></br>
> `Since Version: 0.11.0`<br></br>

---

> #### hoodie.keep.min.commits
> Similar to hoodie.keep.max.commits, but controls the minimum number ofinstants to retain in the active timeline.<br></br>
> **Default Value**: 20 (Optional)<br></br>
> `Config Param: MIN_COMMITS_TO_KEEP`<br></br>

---

> #### hoodie.archive.merge.files.batch.size
> The number of small archive files to be merged at once.<br></br>
> **Default Value**: 10 (Optional)<br></br>
> `Config Param: ARCHIVE_MERGE_FILES_BATCH_SIZE`<br></br>

---

### Index Configs

Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.

`Config Class`: org.apache.hudi.config.HoodieIndexConfig<br></br>

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

> #### hoodie.index.bloom.num_entries
> Only applies if index type is BLOOM. This is the number of entries to be stored in the bloom filter. The rationale for the default: Assume the maxParquetFileSize is 128MB and averageRecordSize is 1kb and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and setting this to a very high number will increase the size every base file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.<br></br>
> **Default Value**: 60000 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_NUM_ENTRIES_VALUE`<br></br>

---

> #### hoodie.bloom.index.update.partition.path
> Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE`<br></br>

---

> #### hoodie.bloom.index.use.caching
> Only applies if index type is BLOOM. When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_USE_CACHING`<br></br>

---

> #### hoodie.bloom.index.parallelism
> Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a shuffle. By default, this is auto computed based on input workload characteristics.<br></br>
> **Default Value**: 0 (Optional)<br></br>
> `Config Param: BLOOM_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.bloom.index.prune.by.ranges
> Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off, since range pruning will only add extra overhead to the index lookup.<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: BLOOM_INDEX_PRUNE_BY_RANGES`<br></br>

---

> #### hoodie.bloom.index.filter.type
> Filter type used. Default is BloomFilterTypeCode.DYNAMIC_V0. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.<br></br>
> **Default Value**: DYNAMIC_V0 (Optional)<br></br>
> `Config Param: BLOOM_FILTER_TYPE`<br></br>

---

> #### hoodie.simple.index.parallelism
> Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br></br>
> **Default Value**: 50 (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.simple.index.use.caching
> Only applies if index type is SIMPLE. When true, the incoming writes will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_USE_CACHING`<br></br>

---

> #### hoodie.global.simple.index.parallelism
> Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle<br></br>
> **Default Value**: 100 (Optional)<br></br>
> `Config Param: GLOBAL_SIMPLE_INDEX_PARALLELISM`<br></br>

---

> #### hoodie.simple.index.update.partition.path
> Similar to Key: 'hoodie.bloom.index.update.partition.path' , default: true but for simple index. Since version: 0.6.0<br></br>
> **Default Value**: true (Optional)<br></br>
> `Config Param: SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE`<br></br>

---

### Common Configurations {#Common-Configurations}

The following set of configurations are common across Hudi.

`Config Class`: org.apache.hudi.common.config.HoodieCommonConfig<br></br>

> #### hoodie.common.spillable.diskmap.type
> When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed. By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.<br></br>
> **Default Value**: BITCASK (Optional)<br></br>
> `Config Param: SPILLABLE_DISK_MAP_TYPE`<br></br>

---

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of key Hudi stats and metrics.

### Metrics Configurations for Datadog reporter {#Metrics-Configurations-for-Datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig<br></br>

> #### hoodie.metrics.on
> Turn on/off metrics reporting. off by default.<br></br>
> **Default Value**: false (Optional)<br></br>
> `Config Param: TURN_METRICS_ON`<br></br>
> `Since Version: 0.5.0`<br></br>

---

> #### hoodie.metrics.reporter.type
> Type of metrics reporter.<br></br>
> **Default Value**: GRAPHITE (Optional)<br></br>
> `Config Param: METRICS_REPORTER_TYPE_VALUE`<br></br>
> `Since Version: 0.5.0`<br></br>

---

> #### hoodie.metrics.reporter.class
> <br></br>
> **Default Value**: (Optional)<br></br>
> `Config Param: METRICS_REPORTER_CLASS_NAME`<br></br>
> `Since Version: 0.6.0`<br></br>

---

## Record Payload Config {#RECORD_PAYLOAD}
This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

### Payload Configurations {#Payload-Configurations}

Payload related configs, that can be leveraged to control merges based on specific business fields in the data.

`Config Class`: org.apache.hudi.config.HoodiePayloadConfig<br></br>
> #### hoodie.payload.event.time.field
> Table column/field name to derive timestamp associated with the records. This can be useful for e.g, determining the freshness of the table.<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: EVENT_TIME_FIELD`<br></br>

---

> #### hoodie.payload.ordering.field
> Table column/field name to order records that have the same key, before merging and writing to storage.<br></br>
> **Default Value**: ts (Optional)<br></br>
> `Config Param: ORDERING_FIELD`<br></br>

---
