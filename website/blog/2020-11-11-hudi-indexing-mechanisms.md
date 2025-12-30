---
title: "Employing the right indexes for fast updates, deletes in Apache Hudi"
excerpt: "Detailing different indexing mechanisms in Hudi and when to use each of them"
authors: [vinoth-chandar]
category: blog
image: /assets/images/blog/hudi-indexes/with-and-without-index.png
tags:
- how-to
- indexing
- apache hudi
---

Apache Hudi employs an index to locate the file group, that an update/delete belongs to. For Copy-On-Write tables, this enables
fast upsert/delete operations, by avoiding the need to join against the entire dataset to determine which files to rewrite.
For Merge-On-Read tables, this design allows Hudi to bound the amount of records any given base file needs to be merged against.
Specifically, a given base file needs to merged only against updates for records that are part of that base file. In contrast,
designs without an indexing component (e.g: [Apache Hive ACID](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions)),
could end up having to merge all the base files against all incoming updates/delete records.
<!--truncate-->
At a high level, an index maps a record key + an optional partition path to a file group ID on storage (explained
more in detail [here](/docs/concepts)) and during write operations, we lookup this mapping to route an incoming update/delete
to a log file attached to the base file (MOR) or to the latest base file that now needs to be merged against (COW). The index also enables 
Hudi to enforce unique constraints based on the record keys.

![Fact table](/assets/images/blog/hudi-indexes/with-and-without-index.png)
_Figure: Comparison of merge cost for updates (yellow blocks) against base files (white blocks)_

Given that Hudi already supports few different indexing techniques and is also continuously improving/adding more to its toolkit, the rest of the blog 
attempts to explain different categories of workloads, from our experience and suggests what index types to use for each. We will also interlace 
commentary on existing limitations, upcoming work and optimizations/tradeoffs along the way. 

## Index Types in Hudi

Currently, Hudi supports the following indexing options. 

- **Bloom Index (default):** Employs bloom filters built out of the record keys, optionally also pruning candidate files using record key ranges.
- **Simple Index:** Performs a lean join of the incoming update/delete records against keys extracted from the table on storage.
- **HBase Index:** Manages the index mapping in an external Apache HBase table.

Writers can pick one of these options using `hoodie.index.type` config option. Additionally, a custom index implementation can also be employed
using `hoodie.index.class` and supplying a subclass of `SparkHoodieIndex` (for Apache Spark writers) 

Another key aspect worth understanding is the difference between global and non-global indexes. Both bloom and simple index have 
global options - `hoodie.index.type=GLOBAL_BLOOM` and `hoodie.index.type=GLOBAL_SIMPLE` - respectively. HBase index is by nature a global index.

- **Global index:**  Global indexes enforce uniqueness of keys across all partitions of a table i.e guarantees that exactly 
one record exists in the table for a given record key. Global indexes offer stronger guarantees, but the update/delete cost grows
with size of the table `O(size of table)`, which might still be acceptable for smaller tables.

- **Non Global index:** On the other hand, the default index implementations enforce this constraint only within a specific partition. 
As one might imagine, non global indexes depends on the writer to provide the same consistent partition path for a given record key during update/delete, 
but can deliver much better performance since the index lookup operation becomes `O(number of records updated/deleted)` and 
scales well with write volume.

Since data comes in at different volumes, velocity and has different access patterns, different indices could be used for different workloads. 
Next, let’s walk through some typical workloads and see how to leverage the right Hudi index for such use-cases.

## Workload: Late arriving updates to fact tables

Many companies store large volumes of transactional data in NoSQL data stores. For eg, trip tables in case of ride-sharing, buying and selling of shares, 
orders in an e-commerce site. These tables are usually ever growing with random updates on most recent data with long tail updates going to older data, either
due to transactions settling at a later date/data corrections. In other words, most updates go into the latest partitions with few updates going to older ones.

![Fact table](/assets/images/blog/hudi-indexes/Fact20tables.gif)
_Figure: Typical update pattern for Fact tables_

For such workloads, the `BLOOM` index performs well, since index look-up will prune a lot of data files based on a well-sized bloom filter.
Additionally, if the keys can be constructed such that they have a certain ordering, the number of files to be compared is further reduced by range pruning. 
Hudi constructs an interval tree with all the file key ranges and efficiently filters out the files that don't match any key ranges in the updates/deleted records.

In order to efficiently compare incoming record keys against bloom filters i.e with minimal number of bloom filter reads and uniform distribution of work across
the executors, Hudi leverages caching of input records and employs a custom partitioner that can iron out data skews using statistics. At times, if the bloom filter 
false positive ratio is high, it could increase the amount of data shuffled to perform the lookup. Hudi supports dynamic bloom filters 
(enabled using `hoodie.bloom.index.filter.type=DYNAMIC_V0`), which adjusts its size based on the number of records stored in a given file to deliver the 
configured false positive ratio. 

In the near future, we plan to introduce a much speedier version of the BLOOM index that tracks bloom filters/ranges in an internal Hudi metadata table, indexed for fast 
point lookups. This would avoid any current limitations around reading bloom filters/ranges from the base files themselves, to perform the lookup. (see 
[RFC-15](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements?src=contextnavpagetreemode) for the general design)

## Workload: De-Duplication in event tables

Event Streaming is everywhere. Events coming from Apache Kafka or similar message bus are typically 10-100x the size of fact tables and often treat "time" (event's arrival time/processing 
time) as a first class citizen. For eg, IoT event stream, click stream data, ad impressions etc. Inserts and updates only span the last few partitions as these are mostly append only data. 
Given duplicate events can be introduced anywhere in the end-end pipeline, de-duplication before storing on the data lake is a common requirement. 

![Event table](/assets/images/blog/hudi-indexes/Event20tables.gif)
_Figure showing the spread of updates for Event table._

In general, this is a very challenging problem to solve at lower cost. Although, we could even employ a key value store to perform this de-duplication ala HBASE index, the index storage
costs would grow linear with number of events and thus can be prohibitively expensive. In fact, `BLOOM` index with range pruning is the optimal solution here. One can leverage the fact
that time is often a first class citizen and construct a key such as `event_ts + event_id` such that the inserted records have monotonically increasing keys. This yields great returns
by pruning large amounts of files even within the latest table partitions. 

## Workload: Random updates/deletes to a dimension table

These types of tables usually contain high dimensional data and hold reference data e.g user profile, merchant information. These are high fidelity tables where the updates are often small but also spread 
across a lot of partitions and data files ranging across the dataset from old to new. Often times, these tables are also un-partitioned, since there is also not a good way to partition these tables.

![Dimensions table](/assets/images/blog/hudi-indexes/Dimension20tables.gif)
_Figure showing the spread of updates for Dimensions table._

As discussed before, the `BLOOM` index may not yield benefits if a good number of files cannot be pruned out by comparing ranges/filters. In such a random write workload, updates end up touching 
most files within in the table and thus bloom filters will typically indicate a true positive for all files based on some incoming update. Consequently, we would end up comparing ranges/filter, only
to finally check the incoming updates against all files. The `SIMPLE` Index will be a better fit as it does not do any upfront pruning based, but directly joins with interested fields from every data file. 
`HBASE` index can be employed, if the operational overhead is acceptable and would provide much better lookup times for these tables. 

When using a global index, users should also consider setting `hoodie.bloom.index.update.partition.path=true` or `hoodie.simple.index.update.partition.path=true` to deal with cases where the 
partition path value could change due to an update e.g users table partitioned by home city; user relocates to a different city. These tables are also excellent candidates for the Merge-On-Read table type.

Going forward, we plan to build [record level indexing](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+08+%3A+Record+level+indexing+mechanisms+for+Hudi+datasets?src=contextnavpagetreemode)
right within Hudi, which will improve the index look-up time and will also avoid additional overhead of maintaining an external system like hbase. 

## Summary 

Without the indexing capabilities in Hudi, it would not been possible to make upserts/deletes happen at [very large scales](https://eng.uber.com/apache-hudi-graduation/). 
Hopefully this post gave you good enough context on the indexing mechanisms today and how different tradeoffs play out. 

Some interesting work underway in this area:

- Apache Flink based writing with a RocksDB state store backed indexing mechanism, unlocking true streaming upserts on data lakes. 
- A brand new MetadataIndex, which reimagines the bloom index today on top of the metadata table in Hudi.
- Record level index implementation, as a secondary index using another Hudi table.

Going forward, this will remain an area of active investment for the project. we are always looking for contributors who can drive these roadmap items forward.
Please [engage](/community/get-involved) with our community if you want to get involved.
 


