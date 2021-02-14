---
title: "Apache Hudi - The Data Lake Platform"
excerpt: "It's been called many things. But, we have always been building a data lake platform"
author: vinoth
category: blog
---
Back in 2016, Apache Hudi pioneered the transactional data lake paradigm, as we know it today, by bringing upserts, deletes, transactions and
event streams to data lake across all three major lake query engines of that time - Hive, Spark and Presto. Right out
of the gate, Hudi powered all of Uber's data lake, which is still the largest such data lake deployment anywhere.
We believed in a more general purpose, cross-engine design, which has now stood the test of time. We also experience a lot 
of joy to see that similar systems (Delta Lake for e.g) have also adopted the same "serverless" transaction layer model, 
that we originally shared back in Spark Summit '17. We consciously introduced two table types **Copy On Write** (with simpler operability) 
and **Merge On Read** (for greater flexibility) and now these terms are used in [projects](https://github.com/apache/iceberg/pull/1862) 
outside Hudi, to refer to similar ideas being borrowed from Hudi.

So, why are we writing a post defining the project after 4+ years of existence as an open source project? 
With the exciting developments in the past year or so, that have propelled transactional data 
lakes mainstream, we thought some perspective can help users see the project with the right lens.

**TODO: insert stats on community etc**

At this time, we also wanted to shine some light on all the great work done by 150+ contributors on the 
project, working with more than 1000 unique users over slack/github/jira, contributing all the different capabilities
Hudi has gained over the past years, from its humble beginnings at Uber. This blog is both a backward-looking statement - that gives due credit to the community for all that's 
been built in this time, and a forward-looking statement - that puts forward a vision for what we are building towards.

# Project Evolution

Data lake workloads can be broadly classified into the two major categories - **Pipelines** and **Queries**. Pipelines extract data 
from upstream sources or other data lake table, apply transformations and write results back to the lake. Queries written in 
SQL or code, typically sift through large amount of data and quickly compute results for analytics. Our initial vision for the project, 
was around making incremental/streaming data pipelines at large scale/complexity, a reality for data engineers at large, leaving the query
performance largely to individual query engines. We drew inspiration from database design (indexes, change data capture) and 
stream processing (tying together columnar data and streams), from the get-go. Our goal was that Hudi pipelines support writing and
extracting streams of data just like an OLTP database, while remaining optimized for large analytical OLAP scans.

![whats-hudi](/assets/images/blog/datalake-platform/hudi-comic.png)

Given our goals, we designed everything around events and logs, that let us efficiently deal with updates to even table 
metadata or reduce contention points for doing large scale concurrent
actions to the table (more on this below). We have also built tools/services on top of this foundation, 
to ultimately help users do meaningful things out-of-box without integrating different open source systems themselves. Over the
past year or so, we have also leveraged this machinery to fundamentally improve query performance as well, e.g data clustering, 
file sizing, fast listing of cloud storage.

# Data Lake Platform 

Today, the best way to describe Apache Hudi is as a _Data Lake Platform_ built on a _streaming_ friendly design. 
The words carry significant meaning.

**Streaming**: At its core, by optimizing for fast upserts & change streams, Hudi provides the primitives to data 
lake workloads that are comparable to what Apache Kafka does for event-streaming (namely, incremental produce/consume of 
events and a state-store for interactive querying). Our goal is that most data lake pipelines can run in streaming fashion,
right on top of lake storage, drastically improving data freshness and resource utilization.

**Data Lake**: Hudi balances the needs of data lake workloads, by optimizing storage layout/metadata for queries
and existing batch pipelines, while providing transactional and data management capabilities. While Hudi is a 
"lakehouse" technology (as cited by Databricks), Hudi's implementation makes incremental/streaming friendly
design choices, that anticipate the underlying data changing more often.

**Platform**: Often times in open source, there is great tech, but there is just too many of them - all differing ever
so slightly in their opinionated ways, ultimately making the integration task onerous on the end user. Would n't
we all enjoy great usability from a single platform, in addition to the freedom and transparency of a true open source
community? That's exactly the approach to build out our data and table services, while tightly integrated
with the Hudi "kernel" (if you will), gives us the ability to deliver much more reliability and usability.

# Hudi Stack
The following stack captures layers of functionality offered by Apache Hudi, which each layer depending on and drawing
strength from the layer below.

![streaming-data-lake-platform](/assets/images/blog/datalake-platform/hudi-data-lake-platform.png)

The features annotated with `*` represent work in progress and dotted boxes represent planned future work, to complete
our vision for the project. Rest of the blog will delve in each layer, in our stack - explaining what it does, how its 
designed and how it will evolve in the future. 

# Lake Storage
Hudi interacts with lake storage using the Hadoop FileSystem API, which makes it compatible with all of its implementations
ranging from HDFS to Cloud Stores to even in-memory filesystems like Alluxio/Ignite. Hudi internally implements its own
wrapper filesystem on top to provide additional storage optimizations (e.g: file sizing), performance optimizations (e.g:
buffering) and metrics. Uniquely, Hudi takes full advantage of append support, for storage schemes that support it, like
HDFS. This helps Hudi deliver streaming writes without causing an explosion in file counts. Unfortunately, most cloud/object 
storages do not offer append capability today (except may be Azure). In the future, we plan to integrate with even the 
lower level APIs of major cloud object stores, to provide similar controls over file counts at streaming ingest latencies.

# File Format
Hudi is designed around the notion of base file and delta log files that store updates/deltas to a given base file. These
formats are pluggable, with Parquet (columnar access) and HFile (indexed access) being the supported base file formats today.
The delta logs, encode data in Avro (row oriented) format for speedier logging (just like Kafka topics for e.g). Going forward, we 
plan to inline any base file format into log blocks in the coming releases. Future plans also include Orc base/log file formats,
unstructured data formats (csv,json, images) and even tiered storage layers in event-streaming systems/OLAP engines/warehouses.

# Table Format
The term "table format" is new and still means many things to many people. Drawing an analogy to file formats, a table format 
simply consists of : the file layout of the table, table's schema and metadata needed for accessing the table efficiently. 
**Hudi is not a table format, it implements one internally**. Hudi uses Avro schemas, to store, manage and evolve a table's schema. 
Currently, Hudi enforces `schema-on-write`, which although stricter than schema-on-read, is adopted widely in the stream processing 
world to improve data quality overall. 

Hudi consciously groups files within a table/partition into groups and maintains a mapping between an incoming record's
key to an existing file group. All updates are records into delta log files specific to a given file group and this design 
ensures low merge overhead compared to approaches like Hive ACID/Iceberg, which have to merge all delta records against all
base files to satisfy queries. Once again, Hudi makes such fundamental design choices that anticipate fast upserts/deletes, 
as opposed to being optimized for mostly append-only workloads.

The "timeline" is the source of truth for all Hudi's table metadata. It's an event log stored under the `.hoodie` folder, 
that provides an ordered log of all actions performed on the table. New events on the timeline are then 
consumed and reflected onto an internal metadata table (implemented as a merge-on-read table), that currently stores all the 
physical file paths are part of the table, to avoid expensive cloud file listings. Given the log centric timeline design and the fact that 
merge-on-read tables can offer low write amplification, Hudi is able to absorb quick/rapid changes to table's metadata, unlike table 
formats designed for slow-moving data like Iceberg. Additionally, the metadata table uses the HFile file format, which provides indexed lookups of 
keys avoiding the need for reading the entire metadata table to satisfy metadata reads. In the future, we intend to add many more forms of 
table metadata like column/range statistics, which is as simple as adding new partitions to the metadata table. We are also open to
plugging in other open table formats inside Hudi as well, so users can benefit from the rest of the Hudi stack as well.

Storing and serving table metadata right on the lake storage is scalable, but can be much less performant compared
to talking to a meta server. Most cloud warehouses internally are built on a metadata layer that leverages an external database. 
Hudi also provides a metadata server, called the "Timeline server", which offers an alternative backing store for Hudi's table metadata. 
Currently, the timeline server runs embedded in the Hudi writer processes, serving file listings out of a local rocksDB store/javalin 
REST API during the write process, without needing to repeatedly list the cloud storage. In the future, we want to invest deeply into 
the standalone timeline server installations, with support for horizontal scaling, database/table mappings, security and all the 
features necessary to turn it into a highly performant next generation lake metastore.

# Indexes
Table metadata about file listings and column statistics are often enough for lake query engines to generate optimized, engine specific query plans
quickly. This is however not sufficient for Hudi, to realize fast upserts. Like mentioned above, Hudi upsert/delete operations have quickly map
incoming record keys into the file group they reside in. For this purpose, Hudi exposes a pluggable indexing layer to the writer implementations,
with built-in support for range pruning (when keys are ordered and largely arrive in order) and bloom filters (e.g: for uuid based keys where 
ordering is of very little help). Hudi also implements a HBase backed external index which is much more performant although more expensive to operate.
Hudi also consciously exploits the partitioning scheme of the table to implement global and non-global indexing schemes. Users can choose to
enforce key constraints only within a partition, in return for `O(num_affected_partitions)` upsert performance as opposed to `O(total_partitions)`
in the global indexing scenarios. We refer you this blog, that goes over indexing in detail.

In the future, we intend to add additional forms of indexing (e.g: bitmaps) and also improve efficiency of existing implementations, with support
for leveraging them from the query engines as well. Specifically, we would like to support point-lookup-ish queries right on top of lake storage,
which helps avoid the overhead of an additional database for many classes of data applications. We also anticipate that uuid/key based joins to
be sped up a lot, by leveraging record level indexing schemes, we build out for fast upsert performance.

# Concurrency Control 
Concurrency control defines how different writes/readers coordinate access to the table. Hudi ensures atomic writes, by way of publishing 
commits atomically to the timeline, stamped with an instant time that denotes the time at which the action is deemed to have occurred.
Hudi draws clear distinction between writer processes (that issue user's upserts/deletes), table services (that write data/metadata to optimize/perform bookkeeping) and 
readers (that execute queries and read data). Hudi provides snapshot isolation between all three types of processes, meaning they all operate on
a consistent snapshot of the table. Hudi supports a `HoodieRecordPayload` API, that define how records belonging to the same key are merged together
to produce the final value, at a given point-in-time.

Projects like Delta Lake/Iceberg have implemented "optimisitic concurrency control" to deal with competing operations, by either implementing a lock 
or relying on atomic renames (e.g for delta that only works on HDFS). Such approaches are optimisitic that real contention never happens and resort 
to failing one of the writer operations if conflicts occur. Imagine a scenario of two writer processes : an ingest writer job producing new data every 30 minutes
and a deletion writer job that is enforcing GDPR taking 4 hours to issue deletes. If there were to overlap on the same files (very likely to happen in real situations),
the deletion job is almost guaranteed to starve and fail to commit each time, wasting tons of cluster resources. Hudi takes a very different approach that we believe is 
more apt for lake workloads. Hudi supports fully non-blocking, async execution of all table services concurrently as data is written into the table. 
For e.g async compaction that can keep deleting records in the background without blocking the ingest job. This is implemented via a file level, log based concurrency 
control protocol which orders actions based on their start instant times on the timeline.

Thus far, Hudi had supported a single-writer process given the log based concurrency can deal with most common causes of contention in a much efficient manner. However,
there are situations like back-filling where multiple writers are still beneficial. We are hard at work, supporting multiple writers to Hudi table, with both 
optimistic and log-based concurrency control models. While we still believe log based concurrency control is adequate for most use-cases and is vastly more efficient,
there are situations where the user may not like model their merges using a log, but prefer optimistic concurrency control using locks. But, we are focussing on adding 
lot more guards around early detection of conflicts for concurrent writers and terminate early without burning up CPU resources.

# Writers



# Readers


# Table Services


# Data Services

 

# Summary

We hope that this blog painted a complete picture of Apache Hudi, staying true to its founding principles. Interested users 
and readers can expect blogs delving into each layer of the stack and an overhaul of our docs along these lines in the coming
weeks/months.

We view the current efforts around table formats as merely removing decade old bottlenecks in data lake storage/query planes, 
problems which have been already solved very well in cloud warehouses like Big Query/Snowflake. We would like to underscore 
that our vision here is much greater, much more technically challenging. We as an industry are just wrapping our heads
around many of these deep, open-ended problems, that need to be solved to marry stream processing and data lakes, with
scale and simplicity. We hope to continue to put community first and build/solve these hard problems together. If these
challenges excite you and you would like to build for that exciting future, please come join our community.