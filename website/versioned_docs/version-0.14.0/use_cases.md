---
title: "Use Cases"
keywords: [ hudi, data ingestion, etl, real time, use cases]
summary: "Following are some sample use-cases for Hudi, which illustrate the benefits in terms of faster processing & increased efficiency"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Apache Hudi provides the foundational features required to build a state-of-the-art Lakehouse. 
The following are examples of use cases for why many choose to use Apache Hudi:

## A Streaming Data Lake
Apache Hudi is a Streaming Data Lake Platform that unlocks near real-time data ingestion and incremental processing pipelines with ease.
This blog post outlines this use case in more depth - https://hudi.apache.org/blog/2021/07/21/streaming-data-lake-platform

### Near Real-Time Ingestion

Ingesting data from OLTP sources like (event logs, databases, external sources) into a [Data Lake](http://martinfowler.com/bliki/DataLake.html) is a common problem,
that is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools. This "raw data" layer of the data lake often forms the bedrock on which
more value is created.

For RDBMS ingestion, Hudi provides __faster loads via Upserts__, as opposed costly & inefficient bulk loads. It's very common to use a change capture solution like
[Debezium](http://debezium.io/) or [Kafka Connect](https://docs.confluent.io/platform/current/connect/index) or 
[Sqoop Incremental Import](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide#_incremental_imports) and apply them to an
equivalent Hudi table on DFS. For NoSQL datastores like [Cassandra](http://cassandra.apache.org/) / [HBase](https://hbase.apache.org/), 
even moderately big installations store billions of rows. It goes without saying that __full bulk loads are simply infeasible__ and more efficient approaches 
are needed if ingestion is to keep up with the typically high update volumes.

Even for immutable data sources like [Kafka](https://kafka.apache.org), there is often a need to de-duplicate the incoming events against what's stored on DFS.
Hudi achieves this by [employing indexes](http://hudi.apache.org/blog/2020/11/11/hudi-indexing-mechanisms/) of different kinds, quickly and efficiently.

All of this is seamlessly achieved by the Hudi Streamer tool, which is maintained in tight integration with rest of the code 
and we are always trying to add more capture sources, to make this easier for the users. The tool also has a continuous mode, where it
can self-manage clustering/compaction asynchronously, without blocking ingestion, significantly improving data freshness.

### Incremental Processing Pipelines

Data Lake ETL typically involves building a chain of tables derived from each other via DAGs expressed as workflows. Workflows often depend on new data being output by
multiple upstream workflows and traditionally, availability of new data is indicated by a new DFS Folder/Hive Partition.
Let's take a concrete example to illustrate this. An upstream workflow `U` can create a Hive partition for every hour, with data for that hour (event_time) at the end of each hour (processing_time), providing effective freshness of 1 hour.
Then, a downstream workflow `D`, kicks off immediately after `U` finishes, and does its own processing for the next hour, increasing the effective latency to 2 hours.

The above paradigm simply ignores late arriving data i.e when `processing_time` and `event_time` drift apart.
Unfortunately, in today's post-mobile & pre-IoT world, __late data from intermittently connected mobile devices & sensors are the norm, not an anomaly__.
In such cases, the only remedy to guarantee correctness is to reprocess the last few hours worth of data, over and over again each hour,
which can significantly hurt the efficiency across the entire ecosystem. For e.g; imagine reprocessing TBs worth of data every hour across hundreds of workflows.

Hudi comes to the rescue again, by providing a way to consume new data (including late data) from an upstream Hudi table `HU` at a record granularity (not folders/partitions),
apply the processing logic, and efficiently update/reconcile late data with a downstream Hudi table `HD`. Here, `HU` and `HD` can be continuously scheduled at a much more frequent schedule
like 15 mins, and providing an end-end latency of 30 mins at `HD`.

To achieve this, Hudi has embraced similar concepts from stream processing frameworks like [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide#join-operations) , Pub/Sub systems like [Kafka](http://kafka.apache.org/documentation/#theconsumer)
[Flink](https://flink.apache.org) or database replication technologies like [Oracle XStream](https://docs.oracle.com/cd/E11882_01/server.112/e16545/xstrm_cncpt.htm#XSTRM187).
For the more curious, a more detailed explanation of the benefits of Incremental Processing can be found [here](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)

### Unified Batch and Streaming

The world we live in is polarized - even on data analytics storage - into real-time and offline/batch storage. Typically, real-time [datamarts](https://en.wikipedia.org/wiki/Data_mart)
are powered by specialized analytical stores such as [Druid](http://druid.io/) or [Memsql](http://www.memsql.com/) or [Clickhouse](https://clickhouse.tech/), fed by event buses like
[Kafka](https://kafka.apache.org) or [Pulsar](https://pulsar.apache.org). This model is prohibitively expensive, unless a small fraction of your data lake data
needs sub-second query responses such as system monitoring or interactive real-time analysis.

The same data gets ingested into data lake storage much later (say every few hours or so) and then runs through batch ETL pipelines, with intolerable data freshness
to do any kind of near-realtime analytics. On the other hand, the data lakes provide access to interactive SQL engines like Presto/SparkSQL, which can horizontally scale
easily and provide return even more complex queries, within few seconds.

By bringing streaming primitives to data lake storage, Hudi opens up new possibilities by being able to ingest data within few minutes and also author incremental data
pipelines that are orders of magnitude faster than traditional batch processing. By bringing __data freshness to a few minutes__, Hudi can provide a much efficient alternative,
for a large class of data applications, compared to real-time datamarts. Also, Hudi has no upfront server infrastructure investments
and thus enables faster analytics on much fresher analytics, without increasing the operational overhead. This external [article](https://www.analyticsinsight.net/can-big-data-solutions-be-affordable/)
further validates this newer model.

## Cloud-Native Tables
Apache Hudi makes it easy to define tables, manage schema, metadata, and bring SQL semantics to cloud file storage.
Some may first hear about Hudi as an "open table format". While this is true, it is just one layer the full Hudi stack.
The term “table format” is new and still means many things to many people. Drawing an analogy to file formats, a table 
format simply consists of : the file layout of the table, table’s schema and metadata tracking changes to the table. 
Hudi is not a table format alone, but it does implement one internally. 

### Schema Management
A key component of a table is the schema of that table. Apache Hudi provides flexibility to enforce schemas, but also allow 
schema evolution to ensure pipeline resilience to changes. Hudi uses Avro schemas to store, manage and evolve a table’s 
schema. Currently, Hudi enforces schema-on-write, which although stricter than schema-on-read, is adopted widely in the 
stream processing world to ensure pipelines don't break from non backwards compatible changes.

### ACID Transactions
Along with a table, Apache Hudi brings ACID transactional guarantees to a data lake.
Hudi ensures atomic writes, by way of publishing commits atomically to a [timeline](/docs/timeline), stamped with an 
instant time that denotes the time at which the action 
is deemed to have occurred. Unlike general purpose file version control, Hudi draws clear distinction between writer processes 
(that issue user’s upserts/deletes), table services (that write data/metadata to optimize/perform bookkeeping) and readers 
(that execute queries and read data). Hudi provides snapshot isolation between all three types of processes, meaning they 
all operate on a consistent snapshot of the table. Hudi provides [optimistic concurrency control](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers) 
(OCC) between writers, while providing lock-free, non-blocking MVCC based concurrency control between writers and 
table-services and between different table services.

Projects that solely rely on OCC deal with competing operations, by either implementing a lock or relying on atomic renames. 
Such approaches are optimistic that real contention never happens and resort to failing one of the writer operations if 
conflicts occur, which can cause significant resource wastage or operational overhead. Imagine a scenario of two writer 
processes : an ingest writer job producing new data every 30 minutes and a deletion writer job that is enforcing GDPR 
taking 2 hours to issue deletes. If there were to overlap on the same files (very likely to happen in real situations 
with random deletes), the deletion job is almost guaranteed to starve and fail to commit each time, wasting tons of 
cluster resources. Hudi takes a very different approach that we believe is more apt for lake transactions, which are 
typically long-running. For e.g async compaction that can keep deleting records in the background without blocking the ingest job. 
This is implemented via a file level, log based concurrency control protocol which orders actions based on their start instant times on the timeline.

### Efficient Upserts and Deletes
While ACID transactions opens the door for Upserts and Deletes, Hudi also unlocks special capabilities like clustering, 
indexing, and z-ordering which allows users to optimize for efficiency in Deletions and Upserts. Specifically, users can 
cluster older event log data based on user_id, such that, queries that evaluate candidates for data deletion can do so, while
more recent partitions are optimized for query performance and clustered on say timestamp. 

Hudi also offers efficient ways of dealing with large write amplification, resulting from random deletes based on user_id
(or any secondary key), by way of the `Merge On Read` table types. Hudi's elegant log based concurrency control, ensures 
that the ingestion/writing can continue happening, as a background compaction job amortizes the cost of rewriting data to enforce deletes.

### Time-Travel
Apache Hudi unlocks the ability to write time travel queries, which means you can query the previous state of the data. 
This is particularly useful for a few use cases. 
- Rollbacks - Easily revert back to a previous version of the table.
- Debugging - Inspect previous versions of data to understand how it has changed over time.
- Audit History - Have a trail of commits that helps you see how, who, and when altered the data over time.

## Data Lake Performance Optimizations
Apache Hudi offers several cutting edge services which help you achieve industry leading performance and significant 
cost savings for your data lake.

Some examples of the Apache Hudi services that make this performance optimization easy include: 

- [Auto File Sizing](/docs/file_sizing) - to solve the "small files" problem.
- [Clustering](/docs/clustering) - to co-locate data next to each other.
- [Compaction](/docs/compaction) - to allow tuning of low latency ingestion and fast read queries. 
- [Indexing](indexing) - for efficient upserts and deletes.
- Multi-Dimensional Partitioning (Z-Ordering) - Traditional folder style partitioning on low-cardinality, while also 
Z-Ordering data within files based on high-cardinality
- Metadata Table - No more slow S3 file listings or throttling.
- [Auto Cleaning](hoodie_cleaner) - Keeps your storage costs in check by automatically removing old versions of files.

