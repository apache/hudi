---
title: Indexing
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

## Indexing

Hudi provides efficient upserts, by mapping a given hoodie key (record key + partition path) consistently to a file id, via an indexing mechanism.
This mapping between record key and file group/file id, never changes once the first version of a record has been written to a file. In short, the
mapped file group contains all versions of a group of records.

For [Copy-On-Write tables](table_types.md#copy-on-write-table), this enables fast upsert/delete operations, by 
avoiding the need to join against the entire dataset to determine which files to rewrite.
For [Merge-On-Read tables](table_types.md#merge-on-read-table), this design allows Hudi to bound the amount of 
records any given base file needs to be merged against.
Specifically, a given base file needs to merged only against updates for records that are part of that base file. In contrast,
designs without an indexing component (e.g: [Apache Hive ACID](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions)),
could end up having to merge all the base files against all incoming updates/delete records:

![Fact table](/assets/images/blog/hudi-indexes/with_without_index.png)
<p align = "center">Figure: Comparison of merge cost for updates (dark blue blocks) against base files (light blue blocks)</p>

## Multi-modal Indexing
Hudi supoprts a multi-modal index by augmenting the metadata table with the capability to incorporate new types of indexes, complemented by an asynchronous mechanism for index construction. This enhancement supports a range of indexes within the metadata table, significantly improving the efficiency of both writing to and reading from the table. For additional information on Hudi's metadata table, check out Hudi's [metadata table](https://hudi.apache.org/docs/next/metadata) documentation.

## Index Types in Hudi

Currently, Hudi supports the following index types. Default is SIMPLE on Spark engine, and INMEMORY on Flink and Java 
engines.

- **BLOOM:** Uses bloom filters generated from record keys, with the option to further narrow down candidate files based on the ranges of the record keys. It requires keys to be partition-level unique so it can function correctly.
- **GLOBAL_BLOOM:** Utilizes bloom filters created from record keys, and may also refine the selection of candidate files by using the ranges of record keys. It requires keys to be table/global-level unique so it can function correctly.
- **SIMPLE (default for Spark engines):** This is the standard index type for the Spark engine. It executes an efficient join of incoming records with keys retrieved from the table stored on disk. It requires keys to be partition-level unique so it can function correctly. 
- **GLOBAL_SIMPLE:** Performs a lean join of the incoming records against keys extracted from the table on storage. It requires keys to be table/global-level unique so it can function correctly.
- **HBASE:** Mangages the index mapping through an external table in Apache HBase.
- **INMEMORY (default for Flink and Java):** Uses in-memory hashmap in Spark and Java engine and Flink in-memory state in Flink for indexing.
- **BUCKET:** Utilizes bucket hashing to identify the file group that houses the records, which proves to be particularly advantageous on a large scale. To select the type of bucket engine—that is, the method by which buckets are created—use the `hoodie.index.bucket.engine` configuration option.
  - `SIMPLE(default)`: This index employs a fixed number of buckets for file groups within each partition, which do not have the capacity to decrease or increase in size. It is applicable to both COW and MOR tables. Due to the unchangeable number of buckets and the design principle of mapping each bucket to a single file group, this indexing method may not be ideal for partitions with significant data skew.
  - `CONSISTENT_HASHING`: This index accommodates a dynamic number of buckets, with the capability for bucket resizing to ensure each bucket is sized appropriately. This addresses the issue of data skew in partitions with a high volume of data by allowing these partitions to be dynamically resized. As a result, partitions can have multiple reasonably sized buckets, unlike the fixed bucket count per partition seen in the SIMPLE bucket engine type. This feature is exclusively compatible with MOR tables.
  
- **RECORD_INDEX:** This index saves the record key to location mappings in the HUDI Metadata Table. It functions as a global index, requiring keys to be unique across all partitions within the table. To accommodate very high scales, it utilizes sharding. The record index is specifically optimized to enable fast upserts. Additionally, when it comes to reading data, the index is crafted to allow for rapid point lookups, significantly speeding up data retrieval processes.

- **Bring your own implementation:** You can extend this [public API](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java) 
to implement custom indexing.

Writers can pick one of these options using `hoodie.index.type` config option. Additionally, a custom index implementation can also be employed
using `hoodie.index.class` and supplying a subclass of `SparkHoodieIndex` (for Apache Spark writers)

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
  can offer pretty good index lookup performance compared to other global indices(GLOBAL_SIMPLE or GLOBAL_BLOOM) or 
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
| hoodie.bloom.index.update.partition.path       | true (Optional)                | Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition, ignoring the new incoming partition if there is a mis-match between partition value for an incoming record with whats in storage.<br /><br />`Config Param: BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE`                                                                |
| hoodie.record.index.update.partition.path      | false (Optional)               | Similar to Key: 'hoodie.bloom.index.update.partition.path' , Only applies if index type is RECORD_INDEX. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition, ignoring the new incoming partition if there is a mis-match between partition value for an incoming record with whats in storage. <br /><br />`Config Param: RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE` |
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




## Indexing Strategies

Since data comes in at different volumes, velocity and has different access patterns, different indices could be used for different workload types.
Let’s walk through some typical workload types and see how to leverage the right Hudi index for such use-cases.
This is based on our experience and you should diligently decide if the same strategies are best for your workloads.

### Workload 1: Late arriving updates to fact tables
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

### Workload 2: De-Duplication in event tables
Event Streaming is everywhere. Events coming from Apache Kafka or similar message bus are typically 10-100x the size of fact tables and often treat "time" (event's arrival time/processing
time) as a first class citizen. For eg, IoT event stream, click stream data, ad impressions etc. Inserts and updates only span the last few partitions as these are mostly append only data.
Given duplicate events can be introduced anywhere in the end-end pipeline, de-duplication before storing on the data lake is a common requirement.

![Event table](/assets/images/blog/hudi-indexes/event_bus.png)
<p align = "center">Figure showing the spread of updates for Event table.</p>

In general, this is a very challenging problem to solve at lower cost. Although, we could even employ a key value store to perform this de-duplication with HBASE index, the index storage
costs would grow linear with number of events and thus can be prohibitively expensive. In fact, `BLOOM` index with range pruning is the optimal solution here. One can leverage the fact
that time is often a first class citizen and construct a key such as `event_ts + event_id` such that the inserted records have monotonically increasing keys. This yields great returns
by pruning large amounts of files even within the latest table partitions.

### Workload 3: Random updates/deletes to a dimension table
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
<h3>Videos</h3>

* [Global Bloom Index: Remove duplicates & guarantee uniquness - Hudi Labs](https://youtu.be/XlRvMFJ7g9c)
