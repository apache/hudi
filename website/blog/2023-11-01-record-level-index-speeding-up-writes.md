---
title: "Record Level Index: Speeding up writes and reads for large-scale datasets in Hud"
excerpt: "Blog on Record Level Index with Apache Hudi"
author: Raymond Xu and Sivabalan Narayanan
category: blog
image: /assets/images/blog/debezium.png
tags:
- design
- index
- speedup
- metadata
- improve write latency
- improve read latency
- apache hudi
---

As of Hudi v0.14.0, we are excited to announce the fast ever index with Apache Hudi, Record Level Index which can boost not only the write latencies, but 
also read latencies for applicable queries. 

<!--truncate-->

## Introduction

Index is a critical component that facilitates quick updates and deletions in Hudi's write paths, and it plays a pivotal 
role in enhancing the efficiency of query engines as well. In the recent release 0.14.0, a new Index type, known as the 
Record Level Index, has been introduced, elevating Hudi’s indexing capabilities to unprecedented level.

Hudi supports several index types: Bloom and Simple indexes with global variations, the HBase Index that leverages a HBase 
server, the hash-based Bucket index, and the multi-modal index realized through the metadata table. For more information 
about indexes and best practices regarding index selection and configuration, please refer to the blogs linked here.

## Metadata table 
The Record Level Index (RLI) operates as an integral component of the metadata table. Therefore, it would be more 
comprehensible to introduce the metadata table before delving into the design of RLI. The metadata table is a 
Merge-on-Read (MoR) table within the `.hoodie/metadata/` directory. It contains various metadata pertaining to records, 
seamlessly integrated into both the writer and reader paths to improve indexing efficiency. The metadata is segregated
into four partitions: files, column stats, bloom filters, and record level index.

<img src="/assets/images/blog/record-level-index/metadata-rli.png" alt="metadata-rli" width="600" align="right"/>

The metadata table is updated synchronously with each commit action on the Timeline, in other words, the commits to the 
metadata table are part of the transactions to the Hudi data table. With four partitions containing different types of 
metadata, this layout serves the purpose of a multi-modal index:

`files` partition keeps track of the Hudi data table’s partitions, and data files of each partition
`column stats` partition records statistics about each column of the data table
`bloom filter` partition stores serialized bloom filters for base files
`record level index` partition contains mappings of individual record key and the corresponding file group id

Users can activate the metadata table by setting `hoodie.metadata.enable=true`. Once activated, the `files` partition 
will always be enabled. Other partitions can be enabled and configured individually to harness additional indexing 
capabilities.


## Record Level Index
Starting from release 0.14.0, the Record Level Index (RLI) can be activated by setting `hoodie.metadata.record.index.enable=true`
and `hoodie.index.type=RECORD_INDEX`. The core concept behind RLI is the ability to determine location of records, thus 
reducing the number of files that need to be scanned to extract the desired data(looking up an index is any day faster 
than scanning the entire dataset). Hudi employs a primary-key model, requiring each record to be associated with a key 
to satisfy the uniqueness constraint. Consequently, we can establish one-to-one mappings between record keys and file groups, 
precisely the data we intend to store within the "record level index" partition.

Performance is paramount when it comes to indexes. The metadata table, which includes the RLI partition, chooses HFile, 
HBase’s file format that utilizes B+ tree-like structures for fast lookup, as the file format. Real-world benchmarking 
has shown that an HFile containing 1 million RLI mappings can look up a batch of 100k records in just 600 ms. 
We will cover the performance topic in a later section.

### Initialization 
Initializing the RLI partition for an existing Hudi table can be a laborious and time-consuming task, contingent on the number 
of records. Just like with a typical database, building indexes takes time, but the investment ultimately pays off by speeding up 
numerous queries in the future.

<img src="/assets/images/blog/record-level-index/rli-skeleton.png" alt="metadata-rli" width="400"/>


The diagram above shows the high-level steps of RLI initialization. Since these jobs are all parallelizable, users can 
scale the cluster and configure relevant parallelism settings (e.g., `hoodie.metadata.max.init.parallelism`) accordingly
to meet their time requirement.

Focusing on the final step, "Bulk insert to RLI partition," the metadata table writer employs a hash function to 
partition the RLI records, ensuring that the number of resulting file groups aligns with the number of partitions. 
This guarantees consistent record key lookups.


<img src="/assets/images/blog/record-level-index/rli-arch.png" alt="metadata-rli" width="800" align="middle"/>

It’s important to note that the current implementation fixes the number of file groups in the RLI partition once it’s initialized. Therefore, users should lean towards over-provisioning the file groups and adjust these configurations accordingly.
hoodie.metadata.record.index.max.filegroup.count
hoodie.metadata.record.index.min.filegroup.count
hoodie.metadata.record.index.max.filegroup.size
hoodie.metadata.record.index.growth.factor

In future development iterations, RLI should be able to overcome this limitation by dynamically rebalancing file groups to 
accommodate the ever-increasing number of records.

### Updating RLI upon data table writes
During regular writes, the RLI partition will be updated as part of the transactions. Metadata records will be generated 
using the incoming record keys with their corresponding location info. Given that the RLI partition contains the exact 
mappings of record keys and locations, upserts to the data table will result in upsertion of the corresponding keys to the 
RLI partition, The hash function employed will guarantee that identical keys are routed to the same file group.

### Writer Indexing
Being part of the write flow, RLI follows the high-level indexing flow, similar to any other global index: for a given 
set of records, it tags each record with location information if the index finds them present in any existing file group. 
The key distinction lies in the source of truth for the existence test—the RLI partition. The diagram below illustrates 
the tagging flow with detailed steps.

<img src="/assets/images/blog/record-level-index/rli-indexing.png" alt="metadata-rli" width="800" align="middle"/>

The tagged records will be passed to Hudi write handles and will undergo write operations to their respective file groups. 
The indexing process is a critical step in applying updates to the table, as its efficiency directly influences the write 
latency. In a later section, we will demonstrate the Record Level Index performance using benchmarking results.


### Read Flow
The Record Level Index is also integrated on the query side. In queries that involve equality check (e.g., EqualTo or IN) 
against the record key column, Hudi’s file index implementation optimizes the file pruning process. This optimization is 
achieved by leveraging the RLI to precisely locate the file groups that need to be read for completing the queries.


### Storage
Storage efficiency is another vital aspect of the design. Each RLI mapping entry must include some necessary information 
to precisely locate files, such as record key, partition path, file group id, etc. To optimize the storage, RLI adopts 
some compression techniques such as encoding file group id (in the form of UUID) into 2 Longs to represent the high and 
low bits. Using Gzip compression and a 4MB block size, an individual RLI record averages only 48 bytes in size. To 
illustrate this more practically, let’s assume we have a table of 100TB data with about 1 billion records (average record size = 100Kb). 
The storage space required by the RLI partition will be approximately 48 Gb, which is less than 0.05% of the total data size. 
Since RLI contains the same number of entries as the data table, storage optimization is crucial to make RLI practical, 
especially for tables of petabyte size and beyond.

RLI exploits the low cost of storage to enable the rapid lookup process similar to the HBase index, while avoiding the 
operational overhead of running an extra server. In the next section, we will review some benchmarking results to demonstrate 
its performance advantages.

## Performance
We conducted a comprehensive benchmarking analysis of the Record Level Index evaluating aspects such write latency, 
index look-up latency, and data shuffling in comparison to existing indexing mechanisms in Hudi. In addition to the 
benchmarks for write operations, we will also showcase the reduction in query latencies for point look-ups. Hudi 0.14.0 
and Spark 3.2.1 were used throughout the experiments.

In comparison to the Global Simple Index (GSI) in Hudi, Record Level Index (RLI) is crafted for significant performance 
advantages stemming from a greatly reduced scan space and minimized data shuffling. GSI conducts join operations between 
incoming records and existing data across all partitions of the data table, resulting in substantial data shuffling and 
computational overhead to pinpoint the records. On the other hand, RLI efficiently extracts location info through a 
hash function, leading to a considerably smaller amount of data shuffling by only loading the file groups of interest 
from the metadata table.

### Write latency
In the first set of experiments, we established two pipelines: one configured using GSI, and the other configured with RLI. 
Each pipeline was executed on an EMR cluster of 10 m5.4xlarge core instances, and was set to ingest batches of 200Mb data 
into a 1TB dataset of 2 billion records. The RLI partition was configured with 1000 file groups. For N batches of ingestion, 
the average write latency using RLI showed a remarkable 72% improvement over GSI.

<img src="/assets/images/blog/record-level-index/write-latency.png" alt="metadata-rli" width="600" align="middle"/>

Note: Between Global Simple Index and Global Bloom Index in Hudi, the former yielded better results due to the randomness 
of record keys. Therefore, we omitted the presentation of the Global Bloom Index in the chart.

### Index look-up latency
We also isolated the index look-up step using HoodieReadClient to accurately gauge indexing efficiency. Through 
experiments involving the look-up of 400,000 records (0.02%) in a 1TB dataset of 2 billion records, RLI showcased a 
72% improvement over GSI, consistent with the end-to-end write latency results.

<img src="/assets/images/blog/record-level-index/write-latency.png" alt="index-latency" width="600" align="middle"/>

Data shuffling
In the index look-up experiments, we observed that around 85Gb of data was shuffled for GSI, whereas only 700Mb was shuffled 
for RLI. This reflects an impressive 92% reduction in data shuffling when using RLI compared to GSI.
Query latency
The Record Level Index will greatly boost Spark queries with “EqualTo” and “IN” predicates on record key columns. 
We created a 400GB Hudi table comprising 20,000 file groups. When we executed a query predicated on a single record key, 
we observed a significant improvement in query time. With RLI enabled, the query time decreased from 977 seconds to just 
12 seconds, representing an impressive 98% reduction in latency.


## When to Use
One of the common workload patterns we have seen in datalake and data engineering community is, ingesting a small 
percentage of data (may be single digit %) compared to total table size. For eg, ingesting 1 to 10GB of data into a 1TB table. 
This is what a typical streaming pipeline will look like and any incremental pipeline looking for fresher availability of 
data might resonate well. For such large workloads, where global record key uniqueness is guaranteed, Record Level Index 
should be the right pick among all other indices. As seen earlier in the blog, Global Simple and Global Bloom are coarse 
grained index and might involve data shuffling relative to the entire dataset size. Bucket index has data skew limitations. 
And HbaseIndex is the only index which is fine grained, but comes with the operational burden of managing and maintaining 
Hbase cluster and introduces external dependency. Hence here we offer Record Level Index to bring down your write latencies 
for these workloads. Global unique record key constraints might be relaxed in a future release and RLI could be a 
replacement for any index Hudi has to offer.

Having said this and shown significant performance improvement in our Performance section, we understand each workload 
is idiosyncratic in its own way. For some skewed workloads, RLI may not be the right choice (for eg, if your entire 
dataset is few GBs in size and amount of data shuffle and file groups are manageable with available compute resources, 
RLI's improvement might be less). So, we would recommend users to enable RLI based on workload characteristics.

## Future Work
In this initial version of the Record Level Index, certain limitations are acknowledged. As mentioned in the 
"Initialization" section, the number of file groups must be predetermined during the creation of the RLI partition. 
Hudi does use some heuristics and a growth factor for an existing table, but for a new table, it is recommended to 
set appropriate file group configs for RLI. As the data volume increases, the RLI partition requires re-bootstrapping 
when additional file groups are needed for scaling out. To address the need for rebalancing, a consistent hashing 
technique could be employed.

Another valuable enhancement would involve supporting the indexing of secondary columns alongside the record key 
fields, thus catering to a broader range of queries. On the reader side, there is a plan to integrate more query 
engines, such as Presto and Trino, with the Record Level Index to fully leverage the performance benefits offered 
by Hudi metadata tables.


[1] Other formats like Parquet can also be supported in the future.
[2] As of now, query engine integration is only available for Spark, with plans to support additional engines in the future.
[3] The query improvement is specific to record-key-matching queries and does not reflect a general reduction in latency by 
enabling Record Level Index. In the case of the single record-key query, 99.995% of file groups (19999 out of 20000) 
were pruned during query execution.

