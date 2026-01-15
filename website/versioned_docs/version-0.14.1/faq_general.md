---
title: General
keywords: [hudi, writing, reading]
---
# General FAQ

### When is Hudi useful for me or my organization?

If you are looking to quickly ingest data onto HDFS or cloud storage, Hudi can provide you tools to [help](writing_data.md). Also, if you have ETL/hive/spark jobs which are slow/taking up a lot of resources, Hudi can potentially help by providing an incremental approach to reading and writing data.

As an organization, Hudi can help you build an [efficient data lake](https://docs.google.com/presentation/d/1FHhsvh70ZP6xXlHdVsAI0g__B_6Mpto5KQFlZ0b8-mM/edit#slide=id.p), solving some of the most complex, low-level storage management problems, while putting data into hands of your data analysts, engineers and scientists much quicker.

### What are some non-goals for Hudi?

Hudi is not designed for any OLTP use-cases, where typically you are using existing NoSQL/RDBMS data stores. Hudi cannot replace your in-memory analytical database (at-least not yet!). Hudi support near-real time ingestion in the order of few minutes, trading off latency for efficient batching. If you truly desirable sub-minute processing delays, then stick with your favorite stream processing solution.

### What is incremental processing? Why does Hudi docs/talks keep talking about it?

Incremental processing was first introduced by Vinoth Chandar, in the O'reilly [blog](https://www.oreilly.com/content/ubers-case-for-incremental-processing-on-hadoop/), that set off most of this effort. In purely technical terms, incremental processing merely refers to writing mini-batch programs in streaming processing style. Typical batch jobs consume **all input** and recompute **all output**, every few hours. Typical stream processing jobs consume some **new input** and recompute **new/changes to output**, continuously/every few seconds. While recomputing all output in batch fashion can be simpler, it's wasteful and resource expensive. Hudi brings ability to author the same batch pipelines in streaming fashion, run every few minutes.

While we can merely refer to this as stream processing, we call it _incremental processing_, to distinguish from purely stream processing pipelines built using Apache Flink or Apache Kafka Streams.

### How is Hudi optimized for CDC and streaming use cases?

One of the core use-cases for Apache Hudi is enabling seamless, efficient database ingestion to your lake, and change data capture is a direct application of that. Hudi’s core design primitives support fast upserts and deletes of data that are suitable for CDC and streaming use cases. Here is a glimpse of some of the challenges accompanying streaming and cdc workloads that Hudi handles efficiently out of the box.

*   **_Processing of deletes:_** Deletes are treated no differently than updates and are logged with the same filegroups where the corresponding keys exist. This helps process deletes faster same like regular inserts and updates and Hudi processes deletes at file group level using compaction in MOR tables. This can be very expensive in other open source systems that store deletes as separate files than data files and incur N(Data files)\*N(Delete files) merge cost to process deletes every time, soon lending into a complex graph problem to solve whose planning itself is expensive. This gets worse with volume, especially when dealing with CDC style workloads that streams changes to records frequently.
*   **_Operational overhead of merging deletes at scale:_** When deletes are stored as separate files without any notion of data locality, the merging of data and deletes can become a run away job that cannot complete in time due to various reasons (Spark retries, executor failure, OOM, etc.). As more data files and delete files are added, the merge becomes even more expensive and complex later on, making it hard to manage in practice causing operation overhead. Hudi removes this complexity from users by treating deletes similarly to any other write operation.
*   **_File sizing with updates:_** Other open source systems, process updates by generating new data files for inserting the new records after deletion, where both data files and delete files get introduced for every batch of updates. This yields to small file problem and requires file sizing. Whereas, Hudi embraces mutations to the data, and manages the table automatically by keeping file sizes in check without passing the burden of file sizing to users as manual maintenance.
*   **_Support for partial updates and payload ordering:_** Hudi support partial updates where already existing record can be updated for specific fields that are non null from newer records (with newer timestamps). Similarly, Hudi supports payload ordering with timestamp through specific payload implementation where late-arriving data with older timestamps will be ignored or dropped. Users can even implement custom logic and plug in to handle what they want.

### How do I choose a storage type for my workload?

A key goal of Hudi is to provide **upsert functionality** that is orders of magnitude faster than rewriting entire tables or partitions.

Choose Copy-on-write storage if :

*   You are looking for a simple alternative, that replaces your existing parquet tables without any need for real-time data.
*   Your current job is rewriting entire table/partition to deal with updates, while only a few files actually change in each partition.
*   You are happy keeping things operationally simpler (no compaction etc), with the ingestion/write performance bound by the [parquet file size](configurations.md#hoodieparquetmaxfilesize) and the number of such files affected/dirtied by updates
*   Your workload is fairly well-understood and does not have sudden bursts of large amount of update or inserts to older partitions. COW absorbs all the merging cost on the writer side and thus these sudden changes can clog up your ingestion and interfere with meeting normal mode ingest latency targets.

Choose merge-on-read storage if :

*   You want the data to be ingested as quickly & queryable as much as possible.
*   Your workload can have sudden spikes/changes in pattern (e.g bulk updates to older transactions in upstream database causing lots of updates to old partitions on DFS). Asynchronous compaction helps amortize the write amplification caused by such scenarios, while normal ingestion keeps up with incoming stream of changes.

Immaterial of what you choose, Hudi provides

*   Snapshot isolation and atomic write of batch of records
*   Incremental pulls
*   Ability to de-duplicate data

Find more [here](concepts.md).

### Is Hudi an analytical database?

A typical database has a bunch of long running storage servers always running, which takes writes and reads. Hudi's architecture is very different and for good reasons. It's highly decoupled where writes and queries/reads can be scaled independently to be able to handle the scale challenges. So, it may not always seems like a database.

Nonetheless, Hudi is designed very much like a database and provides similar functionality (upserts, change capture) and semantics (transactional writes, snapshot isolated reads).

### How do I model the data stored in Hudi?

When writing data into Hudi, you model the records like how you would on a key-value store - specify a key field (unique for a single partition/across table), a partition field (denotes partition to place key into) and preCombine/combine logic that specifies how to handle duplicates in a batch of records written. This model enables Hudi to enforce primary key constraints like you would get on a database table. See [here](writing_data.md) for an example.

When querying/reading data, Hudi just presents itself as a json-like hierarchical table, everyone is used to querying using Hive/Spark/Presto over Parquet/Json/Avro.

### Why does Hudi require a key field to be configured?

Hudi was designed to support fast record level Upserts and thus requires a key to identify whether an incoming record is
an insert or update or delete, and process accordingly. Additionally, Hudi automatically maintains indexes on this primary
key and for many use-cases like CDC, ensuring such primary key constraints is crucial to ensure data quality. In this context,
pre combine key helps reconcile multiple records with same key in a single batch of input records. Even for append-only data
streams, Hudi supports key based de-duplication before inserting records. For e-g; you may have atleast once data integration
systems like Kafka MirrorMaker that can introduce duplicates during failures. Even for plain old batch pipelines, keys
help eliminate duplication that could be caused by backfill pipelines, where commonly it's unclear what set of records
need to be re-written. We are actively working on making keys easier by only requiring them for Upsert and/or automatically
generate the key internally (much like RDBMS row\_ids)

### How does Hudi actually store data inside a table?

At a high level, Hudi is based on MVCC design that writes data to versioned parquet/base files and log files that contain changes to the base file. All the files are stored under a partitioning scheme for the table, which closely resembles how Apache Hive tables are laid out on DFS. Please refer [here](concepts.md) for more details.

### How Hudi handles partition evolution requirements ?

Hudi recommends keeping coarse grained top level partition paths e.g date(ts) and within each such partition do clustering in a flexible way to z-order, sort data based on interested columns. This provides excellent performance by : minimzing the number of files in each partition, while still packing data that will be queried together physically closer (what partitioning aims to achieve).

Let's take an example of a table, where we store log\_events with two fields `ts` (time at which event was produced) and `cust_id` (user for which event was produced) and a common option is to partition by both date(ts) and cust\_id.
Some users may want to start granular with hour(ts) and then later evolve to new partitioning scheme say date(ts). But this means, the number of partitions in the table could be very high - 365 days x 1K customers = at-least 365K potentially small parquet files, that can significantly slow down queries, facing throttling issues on the actual S3/DFS reads.

For the afore mentioned reasons, we don't recommend mixing different partitioning schemes within the same table, since it adds operational complexity, and unpredictable performance.
Old data stays in old partitions and only new data gets into newer evolved partitions. If you want to tidy up the table, one has to rewrite all partition/data anwyay! This is where we suggest start with coarse grained partitions
and lean on clustering techniques to optimize for query performance.

We find that most datasets have at-least one high fidelity field, that can be used as a coarse partition. Clustering strategies in Hudi provide a lot of power - you can alter which partitions to cluster, and which fields to cluster each by etc.
Unlike Hive partitioning, Hudi does not remove the partition field from the data files i.e if you write new partition paths, it does not mean old partitions need to be rewritten.
Partitioning by itself is a relic of the Hive era; Hudi is working on replacing partitioning with database like indexing schemes/functions,
for even more flexibility and get away from Hive-style partition evol route.
