---
title: Indexes
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

In databases, indexes are auxiliary data structures maintained to quickly locate records needed, without reading unnecessary data
from storage. Given that Hudi’s design has been heavily optimized for handling mutable change streams, with different
write patterns, Hudi considers [indexing](#indexing) as an integral part of its design and has uniquely supported
[indexing capabilities](https://hudi.apache.org/blog/2020/11/11/hudi-indexing-mechanisms/) from its inception, to speed
up writes on the [data lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/), while still providing
columnar query performance.

## Mapping keys to file groups
The most foundational index mechanism in Hudi tracks a mapping from a given key (record key + optionally partition path) consistently to a file id. Other types of indexes like secondary indexes,
build on this foundation. This mapping between record key and file group/file id rarely changes once the first version of a record has been written to a file group.
Only clustering or cross-partition updates that are implemented as deletes + inserts remap the record key to a different file group. Even then, a given record key is associated with exactly one
file group at any completed instant on the timeline.

## Need for indexing
For [Copy-On-Write tables](table_types.md#copy-on-write-table), indexing enables fast upsert/delete operations, by avoiding the need to join against the entire dataset to determine which files to rewrite.
For [Merge-On-Read tables](table_types.md#merge-on-read-table), indexing allows Hudi to bound the amount of change records any given base file needs to be merged against. Specifically, a given base file needs to merged
only against updates for records that are part of that base file.

![Fact table](/assets/images/blog/hudi-indexes/with_without_index.png)
<p align = "center">Figure: Comparison of merge cost for updates (dark blue blocks) against base files (light blue blocks)</p>

In contrast,
- Designs without an indexing component (e.g: [Apache Hive/Apache Iceberg](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions)) end up having to merge all the base files against all incoming updates/delete records
  (10-100x more [read amplification](table_types.md#comparison)).
- Designs that implement heavily write-optimized OLTP data structures like LSM trees do not require an indexing component. But they perform poorly scan heavy workloads
  against cloud storage making them unsuitable for serving analytical queries.

Hudi shines by achieving both great write performance and read performance, at the extra storage costs of an index, which can however unlock a lot more, as we explore below.

## Multi-modal Indexing
[Multi-modal indexing](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi),
introduced in [0.11.0 Hudi release](https://hudi.apache.org/releases/release-0.11.0/#multi-modal-index),
is a re-imagination of what a general purpose indexing subsystem should look like for the lake. Multi-modal indexing is
implemented by enhancing the metadata table with the flexibility to extend to new index types as new partitions,
along with an [asynchronous index](https://hudi.apache.org/docs/metadata_indexing/#setup-async-indexing) building

Hudi supports a multi-modal index by augmenting the metadata table with the capability to incorporate new types of indexes, complemented by an
asynchronous mechanism for [index construction](metadata_indexing.md). This enhancement supports a range of indexes within
the [metadata table](metadata.md#metadata-table), significantly improving the efficiency of both writing to and reading from the table.

![Indexes](/assets/images/hudi-stack-indexes.png)
<p align = "center">Figure: Indexes in Hudi</p>

### Bloom Filters

[Bloom filter](https://github.com/apache/hudi/blob/46f41d186c6c84a6af2c54a907ff2736b6013e15/rfc/rfc-37/rfc-37.md) indexes as *bloom_filter* partition in the metadata table.
This index employs range-based pruning on the minimum and maximum values of the record keys and bloom-filter-based lookups to tag incoming records. For large tables, this
involves reading the footers of all matching data files for bloom filters, which can be expensive in the case of random
updates across the entire dataset. This index stores bloom filters of all data files centrally to avoid scanning the
footers directly from all data files.

Following are configurations that control enabling and configuring bloom filters.
| Config Name                                  | Default                                   | Description                                                                                                                                                                                                                                                                                     |
|----------------------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.index.bloom.filter.enable       | false   | Enable indexing bloom filters of user data files under metadata table. When enabled, metadata table will have a partition to store the bloom filter index and will be used during the index lookups.<br />`Config Param: ENABLE_METADATA_INDEX_BLOOM_FILTER`<br />`Since Version: 0.11.0`                                                                                                                                                                                                         |
| hoodie.bloom.index.prune.by.ranges                | true                 | Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off, since range pruning will only  add extra overhead to the index lookup.<br />`Config Param: BLOOM_INDEX_PRUNE_BY_RANGES`                                                                                                                                                                                                                                                      |
| hoodie.bloom.index.update.partition.path    | true                 | Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition<br />`Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE`                                                                                                                                                                                                                                                  |
| hoodie.bloom.index.use.metadata                     | false                | Only applies if index type is BLOOM.When true, the index lookup uses bloom filters and column stats from metadata table when available to speed up the process.<br />`Config Param: BLOOM_INDEX_USE_METADATA`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                      |
| hoodie.metadata.index.bloom.filter.column.list                                    | (N/A)       | Comma-separated list of columns for which bloom filter index will be built. If not set, only record key will be indexed.<br />`Config Param: BLOOM_FILTER_INDEX_FOR_COLUMNS`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                 |
| hoodie.metadata.index.bloom.filter.file.group.count                           | 4           | Metadata bloom filter index partition file group count. This controls the size of the base and log files and read parallelism in the bloom filter index partition. The recommendation is to size the file group count such that the base files are under 1GB.<br />`Config Param: METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT`<br />`Since Version: 0.11.0`                                                                                              |


### Record Indexes

[Record indexes](https://cwiki.apache.org/confluence/display/HUDI/RFC-08++Record+level+indexing+mechanisms+for+Hudi+datasets) as *record_index* partition in the metadata table.
Contains the mapping of the record key to location. Record index is a global index, enforcing key uniqueness across all partitions in the table. This index aids in locating records faster than
other existing indexes and can provide a speedup orders of magnitude faster in large deployments where index lookup dominates write latencies. To accommodate very high scales, it utilizes hash-based
sharding of the key space. Additionally, when it comes to reading data, the index allows for point lookups significantly speeding up index mapping retrieval process.

Following are configurations that control enabling record index building and maintenance on the writer.

| Config Name                                  | Default                                   | Description                                                                                                                                                                                                                                                                                     |
|----------------------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.record.index.enable                                                  | false       | Create the HUDI Record Index within the Metadata Table<br />`Config Param: RECORD_INDEX_ENABLE_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                                                                                                                                                                         |
| hoodie.metadata.record.index.growth.factor                                         | 2.0         | The current number of records are multiplied by this number when estimating the number of file groups to create automatically. This helps account for growth in the number of records in the dataset.<br />`Config Param: RECORD_INDEX_GROWTH_FACTOR_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                   |
| hoodie.metadata.record.index.max.filegroup.count                              | 10000       | Maximum number of file groups to use for Record Index.<br />`Config Param: RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                                                                                                                                                           |
| hoodie.metadata.record.index.max.filegroup.size                               | 1073741824  | Maximum size in bytes of a single file group. Large file group takes longer to compact.<br />`Config Param: RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                                                                                                                     |
| hoodie.metadata.record.index.min.filegroup.count                            | 10          | Minimum number of file groups to use for Record Index.<br />`Config Param: RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                                                                                                                                                           |
| hoodie.record.index.update.partition.path  | false                | Similar to Key: 'hoodie.bloom.index.update.partition.path' , default: true , isAdvanced: true , description: Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition since version: version is not defined deprecated after: version is not defined, but for record index.<br />`Config Param: RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE`<br />`Since Version: 0.14.0` |


### Expression Index
An [expression index](https://github.com/apache/hudi/blob/3789840be3d041cbcfc6b24786740210e4e6d6ac/rfc/rfc-63/rfc-63.md) is an index on a function of a column. If a query has a predicate on a function of a column, the expression index can
be used to speed up the query. Expression index is stored in *expr_index_* prefixed partitions (one for each
expression index) under metadata table. Expression index can be created using SQL syntax. Please checkout SQL DDL
docs [here](sql_ddl.md#create-expression-index) for more details.

Following are configurations that control enabling expression index building and maintenance on the writer.

| Config Name                                  | Default                                   | Description                                                                                                                                                                                                                                                                                     |
|----------------------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.index.expression.enable          | false   | Enable expression index within the metadata table.  When this configuration property is enabled (`true`), the Hudi writer automatically  keeps all expression indexes consistent with the data table.  When disabled (`false`), all expression indexes are deleted.  Note that individual expression index can only be created through a `CREATE INDEX`  and deleted through a `DROP INDEX` statement in Spark SQL.<br />`Config Param: EXPRESSION_INDEX_ENABLE_PROP`<br />`Since Version: 1.0.0` |
| hoodie.metadata.index.expression.file.group.count                           | 2           | Metadata expression index partition file group count.<br />`Config Param: EXPRESSION_INDEX_FILE_GROUP_COUNT`<br />`Since Version: 1.0.0`                                                                                                                                                                                                                                                                                                                  |
| hoodie.expression.index.function | (N/A)         | Function to be used for building the expression index.<br />`Config Param: INDEX_FUNCTION`<br />`Since Version: 1.0.0`                                                                                                                                                                                                                                                                   |
| hoodie.expression.index.name        | (N/A)         | Name of the expression index. This is also used for the partition name in the metadata table.<br />`Config Param: INDEX_NAME`<br />`Since Version: 1.0.0`                                                                                                                                                                                                                                |
| hoodie.expression.index.type      | COLUMN_STATS  | Type of the expression index. Default is `column_stats` if there are no functions and expressions in the command. Valid options could be BITMAP, COLUMN_STATS, LUCENE, etc. If index_type is not provided, and there are functions or expressions in the command then a expression index using column stats will be created.<br />`Config Param: INDEX_TYPE`<br />`Since Version: 1.0.0` |

### Secondary Index

Secondary indexes allow users to create indexes on columns that are not part of record key columns in Hudi tables (for
record key fields, Hudi supports [Record-level Index](/blog/2023/11/01/record-level-index). Secondary indexes
can be used to speed up queries with predicate on columns other than record key columns.

Following are configurations that control enabling secondary index building and maintenance on the writer.

| Config Name                                  | Default                                   | Description                                                                                                                                                                                                                                                                                     |
|----------------------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.index.secondary.enable    | true (Optional)                          | Enable secondary index within the metadata table.  When this configuration property is enabled (`true`), the Hudi writer automatically  keeps all secondary indexes consistent with the data table.  When disabled (`false`), all secondary indexes are deleted.  Note that individual secondary index can only be created through a `CREATE INDEX`  and deleted through a `DROP INDEX` statement in Spark SQL. <br />`Config Param: SECONDARY_INDEX_ENABLE_PROP`<br />`Since Version: 1.0.0`     |
| hoodie.datasource.write.secondarykey.column        | (N/A)                        | Columns that constitute the secondary key component. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`<br />`Config Param: SECONDARYKEY_COLUMN_NAME`                                                            |

## Additional writer-side indexes

All the indexes discussed above are available both readers/writers using integration with metadata table. There are also indexing mechanisms
implemented by the storage engine, by efficiently reading/joining/processing incoming input records against information stored in base/log
files themselves (e.g. bloom filters stored in parquet file footers) or intelligent data layout (e.g. bucket index).

Currently, Hudi supports the following index types. Default is SIMPLE on Spark engine, and INMEMORY on Flink and Java
engines. Writers can pick one of these options using `hoodie.index.type` config option.

- **SIMPLE (default for Spark engines)**: This is the standard index type for the Spark engine. It executes an efficient join of incoming records with keys retrieved from the table stored on disk. It requires keys to be partition-level unique so it can function correctly.

- **RECORD_INDEX** : Use the record index from section above as the writer side index.

- **BLOOM**: Uses bloom filters generated from record keys, with the option to further narrow down candidate files based on the ranges of the record keys. It requires keys to be partition-level unique so it can function correctly.

- **GLOBAL_BLOOM**: Utilizes bloom filters created from record keys, and may also refine the selection of candidate files by using the ranges of record keys. It requires keys to be table/global-level unique so it can function correctly.

- **GLOBAL_SIMPLE**: Performs a lean join of the incoming records against keys extracted from the table on storage. It requires keys to be table/global-level unique so it can function correctly.

- **HBASE**: Mangages the index mapping through an external table in Apache HBase.

- **INMEMORY (default for Flink and Java)**: Uses in-memory hashmap in Spark and Java engine and Flink in-memory state in Flink for indexing.

- **BUCKET**: Utilizes bucket hashing to identify the file group that houses the records, which proves to be particularly advantageous on a large scale. To select the type of bucket engine—that is, the method by which buckets are created—use the `hoodie.index.bucket.engine` configuration option.
  - **SIMPLE(default)**: This index employs a fixed number of buckets for file groups within each partition, which do not have the capacity to decrease or increase in size. It is applicable to both COW and MOR tables. Due to the unchangeable number of buckets and the design principle of mapping each bucket to a single file group, this indexing method may not be ideal for partitions with significant data skew.

  - **CONSISTENT_HASHING**: This index accommodates a dynamic number of buckets, with the capability for bucket resizing to ensure each bucket is sized appropriately. This addresses the issue of data skew in partitions with a high volume of data by allowing these partitions to be dynamically resized. As a result, partitions can have multiple reasonably sized buckets, unlike the fixed bucket count per partition seen in the SIMPLE bucket engine type. This feature is exclusively compatible with MOR tables.

- **Bring your own implementation:** You can extend this [public API](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java)
  and supply a subclass of `SparkHoodieIndex` (for Apache Spark writers) using `hoodie.index.class` to implement custom indexing.

### Global and Non-Global Indexes

Another key aspect worth understanding is the difference between global and non-global indexes. Both bloom and simple index have
global options - `hoodie.index.type=GLOBAL_BLOOM` and `hoodie.index.type=GLOBAL_SIMPLE` - respectively. Record index and
HBase index are by nature a global index.

- **Global index:**  Global indexes enforce uniqueness of keys across all partitions of a table i.e guarantees that exactly
  one record exists in the table for a given record key. Global indexes offer stronger guarantees, but the update/delete
  cost can still grow with size of the table `O(size of table)`, since the record could belong to any partition in storage.
  In case of non-global index, lookup involves file groups only for the matching partitions from the incoming records and
  so its not impacted by the total size of the table. These global indexes(GLOBAL_SIMPLE or GLOBAL_BLOOM), might be
  acceptable for decent sized tables, but for large tables, a newly added index (0.14.0) called Record Level Index (RLI),
  can offer pretty good index lookup performance compared to other global indexes(GLOBAL_SIMPLE or GLOBAL_BLOOM) or
  Hbase and also avoids the operational overhead of maintaining external systems.
- **Non Global index:** On the other hand, the default index implementations enforce this constraint only within a specific partition.
  As one might imagine, non global indexes depends on the writer to provide the same consistent partition path for a given record key during update/delete,
  but can deliver much better performance since the index lookup operation becomes `O(number of records updated/deleted)` and
  scales well with write volume.

### Configs

#### Spark based configs

For Spark DataSource, Spark SQL, DeltaStreamer and Structured Streaming following are the key configs that control
indexing behavior. Please refer to [Advanced Configs](https://hudi.apache.org/docs/next/configurations#Common-Index-Configs-advanced-configs)
for more details. All these, support the index types mentioned [above](#index-types-in-hudi).

| Config Name                                                                          | Default                                                                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.index.type| N/A **(Required)** | org.apache.hudi.index.HoodieIndex$IndexType: Determines how input records are indexed, i.e., looked up based on the key for the location in the existing table. Default is SIMPLE on Spark engine, and INMEMORY on Flink and Java engines. Possible Values: <br /> <ul><li>BLOOM</li><li>GLOBAL_BLOOM</li><li>SIMPLE</li><li>GLOBAL_SIMPLE</li><li>HBASE</li><li>INMEMORY</li><li>FLINK_STATE</li><li>BUCKET</li><li>RECORD_INDEX</li></ul><br />`Config Param: INDEX_TYPE`                                                                                                                                            |
| hoodie.index.bucket.engine            | SIMPLE (Optional)              | org.apache.hudi.index.HoodieIndex$BucketIndexEngineType: Determines the type of bucketing or hashing to use when `hoodie.index.type` is set to `BUCKET`.    Possible Values: <br /> <ul><li>SIMPLE</li><li>CONSISTENT_HASHING</li></ul> <br />`Config Param: BUCKET_INDEX_ENGINE_TYPE`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                    |
| hoodie.index.class                    |  (Optional)                    | Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified<br /><br />`Config Param: INDEX_CLASS_NAME`                                                                                                                                                                                                                                                                                                                                                                                                           |
| hoodie.simple.index.update.partition.path      | true (Optional)                | Similar to Key: 'hoodie.bloom.index.update.partition.path' , Only applies if index type is GLOBAL_SIMPLE. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition, ignoring the new incoming partition if there is a mis-match between partition value for an incoming record with whats in storage. <br /><br />`Config Param: SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE`                                                                                                                                 |
| hoodie.hbase.index.update.partition.path       | false (Optional)                                                        | Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old partition and will insert it as new record in new partition.<br /><br />`Config Param: UPDATE_PARTITION_PATH_ENABLE`                                                                                                                                                                                                                                                                                                             |

#### Flink based configs

For Flink DataStream and Flink SQL only support Bucket Index and internal Flink state store backed in memory index.
Following are the basic configs that control the indexing behavior. Please refer [here](https://hudi.apache.org/docs/next/configurations#Flink-Options-advanced-configs)
for advanced configs.

| Config Name                                                                       | Default                                                                                         | Description                                                                                                                                                                                                                                                                            |
| ----------------------------------------------------------------------------------| ----------------------------------------------------------------------------------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| index.type                                                                        | FLINK_STATE (Optional)                  | Index type of Flink write job, default is using state backed index. Possible values:<br /> <ul><li>FLINK_STATE</li><li>BUCKET</li></ul><br />  `Config Param: INDEX_TYPE`                                                                                                              |
| hoodie.index.bucket.engine                                                        | SIMPLE (Optional)                                                                               | org.apache.hudi.index.HoodieIndex$BucketIndexEngineType: Determines the type of bucketing or hashing to use when `hoodie.index.type` is set to `BUCKET`.    Possible Values: <br /> <ul><li>SIMPLE</li><li>CONSISTENT_HASHING</li></ul>|




### Picking Indexing Strategies

Since data comes in at different volumes, velocity and has different access patterns, different indexes could be used for different workload types.
Let’s walk through some typical workload types and see how to leverage the right Hudi index for such use-cases.
This is based on our experience and you should diligently decide if the same strategies are best for your workloads.

#### Workload 1: Late arriving updates to fact tables
Many companies store large volumes of transactional data in NoSQL data stores. For eg, trip tables in case of ride-sharing, buying and selling of shares,
orders in an e-commerce site. These tables are usually ever growing with random updates on most recent data with long tail updates going to older data, either
due to transactions settling at a later date/data corrections. In other words, most updates go into the latest partitions with few updates going to older ones.

![Fact table](/assets/images/blog/hudi-indexes/nosql.png)
<p align = "center">Figure: Typical update pattern for Fact tables</p>

For such workloads, the `BLOOM` index performs well, since index look-up will prune a lot of data files based on a well-sized bloom filter.
Additionally, if the keys can be constructed such that they have a certain ordering, the number of files to be compared is further reduced by range pruning.
Hudi constructs an interval tree with all the file key ranges and efficiently filters out the files that don't match any key ranges in the updates/deleted records.

In order to efficiently compare incoming record keys against bloom filters i.e with minimal number of bloom filter reads and uniform distribution of work across
the executors, Hudi leverages caching of input records and employs a custom partitioner that can iron out data skews using statistics. At times, if the bloom filter
false positive ratio is high, it could increase the amount of data shuffled to perform the lookup. Hudi supports dynamic bloom filters
(enabled using `hoodie.bloom.index.filter.type=DYNAMIC_V0`), which adjusts its size based on the number of records stored in a given file to deliver the
configured false positive ratio.

#### Workload 2: De-Duplication in event tables
Event Streaming is everywhere. Events coming from Apache Kafka or similar message bus are typically 10-100x the size of fact tables and often treat "time" (event's arrival time/processing
time) as a first class citizen. For eg, IoT event stream, click stream data, ad impressions etc. Inserts and updates only span the last few partitions as these are mostly append only data.
Given duplicate events can be introduced anywhere in the end-end pipeline, de-duplication before storing on the data lake is a common requirement.

![Event table](/assets/images/blog/hudi-indexes/event_bus.png)
<p align = "center">Figure showing the spread of updates for Event table.</p>

In general, this is a very challenging problem to solve at lower cost. Although, we could even employ a key value store to perform this de-duplication with HBASE index, the index storage
costs would grow linear with number of events and thus can be prohibitively expensive. In fact, `BLOOM` index with range pruning is the optimal solution here. One can leverage the fact
that time is often a first class citizen and construct a key such as `event_ts + event_id` such that the inserted records have monotonically increasing keys. This yields great returns
by pruning large amounts of files even within the latest table partitions.

#### Workload 3: Random updates/deletes to a dimension table
These types of tables usually contain high dimensional data and hold reference data e.g user profile, merchant information. These are high fidelity tables where the updates are often small but also spread
across a lot of partitions and data files ranging across the dataset from old to new. Often times, these tables are also un-partitioned, since there is also not a good way to partition these tables.

![Dimensions table](/assets/images/blog/hudi-indexes/dimension.png)
<p align = "center">Figure showing the spread of updates for Dimensions table.</p>

As discussed before, the `BLOOM` index may not yield benefits if a good number of files cannot be pruned out by comparing ranges/filters. In such a random write workload, updates end up touching
most files within in the table and thus bloom filters will typically indicate a true positive for all files based on some incoming update. Consequently, we would end up comparing ranges/filter, only
to finally check the incoming updates against all files. The `SIMPLE` Index will be a better fit as it does not do any upfront pruning based, but directly joins with interested fields from every data file.
`HBASE` index can be employed, if the operational overhead is acceptable and would provide much better lookup times for these tables.

When using a global index, users should also consider setting `hoodie.bloom.index.update.partition.path=true` or `hoodie.simple.index.update.partition.path=true` to deal with cases where the
partition path value could change due to an update e.g users table partitioned by home city; user relocates to a different city. These tables are also excellent candidates for the Merge-On-Read table type.


## Related Resources

<h3>Blogs</h3>

* [Introducing Multi-Modal Index for the Lakehouse in Apache Hudi](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
* [Global vs Non-global index in Apache Hudi](https://medium.com/@simpsons/global-vs-non-global-index-in-apache-hudi-ac880b031cbc)

<h3>Videos</h3>

* [Global Bloom Index: Remove duplicates & guarantee uniquness - Hudi Labs](https://youtu.be/XlRvMFJ7g9c)
* [Multi-Modal Index for the Lakehouse in Apache Hudi](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)