---
title: "Use Cases"
keywords: [ hudi, data ingestion, etl, real time, use cases]
summary: "Following are some sample use-cases for Hudi, which illustrate the benefits in terms of faster processing & increased efficiency"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Apache Hudi is a powerful [data lakehouse platform](https://hudi.apache.org/blog/2021/07/21/streaming-data-lake-platform) that shines in a variety of use cases due to its high-performance design, rich feature set, and 
unique strengths tailored to modern data engineering needs. This document explores its key use cases and differentiation, to help you understand when and why Hudi is an excellent choice for your data lakehouse.

## Streaming/CDC data ingestion to Data Lakehouse

Hudi excels at handling incremental data updates, making it a perfect fit for CDC pipelines which replicate frequent updates, inserts, and deletes from an upstream database
 like MySQL or PostgresSQL to a downstream data lakehouse table. This "raw data" layer of the data lake often forms the foundation on which all subsequent data workloads 
from BI to AI are built. Though ingesting data from OLTP sources like (event logs, databases, external sources) into a [Data Lake](http://martinfowler.com/bliki/DataLake.html) is an important problem,
it is unfortunately often solved in a piecemeal fashion, using a medley of ingestion tools.

### Why Hudi? 

- Unique design choices like Merge-On-Read tables, record-level indexes and asynchronous compaction, approach theoretical optimality for absorbing changes to tables quickly and efficiently.
- Built-in ingestion tools on [Spark](hoodie_streaming_ingestion.md), [Flink](ingestion_flink.md) and [Kafka Connect](ingestion_kafka_connect.md), that let you ingest data with a single command.
- Support for incremental ingestion with automatic checkpoint management from streaming sources (Kafka, Pulsar, ...), Cloud storage (S3, GCS, ADLS, etc.) and even JDBC.
- Support for widely used data formats (Protobuf, Avro, JSON), file formats (parquet, orc, avro, etc.) and change log formats like [Debezium](http://debezium.io/).
- Even for scalable de-duplication for high-volume append-only streaming data, by employing bloom filter indexes and advanced data structures like interval trees for efficient range pruning.
- Integration with popular schema registries, to automatically and safely evolve tables to new schemas on-the-fly as they change in the source system.
- Hudi supports event time ordering and late data handling for streaming workloads using RecordPayload/RecordMerger APIs let you merge updates in the database LSN order, in addition to latest writer wins semantics. Without this capability, the table can go back in (event) time, if the input records are out-of-order/late-arriving (which will inevitably happen in real life).

## Offloading from expensive Data Warehouses

As organizations scale, traditional ETL operations and data storage in data warehouses become prohibitively expensive. Hudi offers an efficient way to migrate these workloads 
to a data lakehouse, significantly reducing costs without compromising on performance. 

### Why Hudi?

 - Hudi lets you store data in your own cloud accounts or storage systems in open data formats, away from vendor lock-in and avoiding additional storage costs from vendors. This also lets you open up data to other compute engines, including a plethora of open-source query engines like Presto, Trino, Starrocks.
- Tools like [hudi-dbt](https://docs.getdbt.com/reference/resource-configs/spark-configs#incremental-models) adapter plugin makes it easy to migrate existing SQL ETL pipelines over to Apache Spark SQL. Users can then take advantage fast/efficient write performance of Hudi to cut down cost of '_L_' in ETL pipelines.
- Hudi's storage format is optimized to efficiently compute "diffs" between two points in time on a table, allowing large SQL joins to be re-written efficiently by eliminating costly scans of large fact tables. This cuts down cost of '_E_' in ETL pipelines. 
- Additionally, Hudi offers a fully-fledged set of table services, that can automatically optimize, cluster, and compact data in the background, resulting in significant cost savings over using proprietary compute services from a data warehouse.
- Hudi combined with a stream processing like Flink and Dynamic Tables, can help replace slow, expensive warehouse ETLs, while also dramatically improving data freshness.

## High Performance Open Table Format

Over the past couple of years, there is a growing trend with data warehouses to support reads/writes on top of an "open table format" layer. The Table Format consists of one or more open 
file formats, metadata around how the files constitute the table and a protocol for concurrently reading/writing to such tables. Though Hudi offers more than such a table format layer, 
it packs a powerful native open table format designed for high performance even on the largest tables in the world.

### Why Hudi?

- Hudi format stores metadata in both an event log (timeline) and snapshot representations (metadata table), allowing for minimal storage overhead for keeping lots of versions of table, while still offering fast access for planning snapshot queries. 
- Metadata about the table is also stored in an indexed fashion, conducive to efficient query processing. For e.g. statistics about columns, partitions are stored using an SSTable like file format, to ensure only smaller amounts of metadata, relevant to columns part of a query are read.
- Hudi is designed from ground up with an indexing component that improves write/query performance, at the cost of relatively small increased storage overhead. Various indexes like hash-based record indexes, bloom filter indexes are available, with more on the way.
- When it comes to concurrency control (CC), Hudi judiciously treats writers, readers and table services maintaining the table as separate entities. This design enables Hudi helps achieve multi-version concurrency control (MVCC) between writer and compaction/indexing, that allows writers to safely write without getting blocked or retrying on conflicts which waste a lot of compute resources in other approaches.
- Between two writers, Hudi uses Optimistic Concurrency Control (OCC) to provide serializability on write completion time (commit time ordering) and a novel non-blocking concurrency control (NBCC) with record merging based on event-time (event-time processing).
- With these design choices and interoperability provided with [Apache XTable](https://xtable.apache.org/) to other table formats, Hudi tables are quite often the fastest backing tables for other table formats like Delta Lake or Apache Iceberg.

## Open Data Platform

Many organizations seek to build a data platform that is open, future-proof and extensible. This requires open-source components that provide data formats, APIs and data compute services, that can be mixed and matched 
together to build out the platform. Such an open platform is also essential for organizations to take advantage of the latest technologies and tools, without being beholden to a single vendor's roadmap.

### Why Hudi?

- Hudi only operates on data in open data, file and table formats. Hudi is not locked to any particular data format or storage system.
- While open data formats help, Hudi unlocks complete freedom by also providing open compute services for ingesting, optimizing, indexing and querying data. For e.g Hudi's writers come with 
  a self-managing table service runtime that can maintain tables automatically in the background on each write. Often times, Hudi and your favorite open query engine is all 
  you need to get an open data platform up and running.
- Examples of open services that make performance optimization or management easy include: [auto file sizing](file_sizing.md) to solve the "small files" problem,
  [clustering](clustering.md) to co-locate data next to each other, [compaction](compaction.md) to allow tuning of low latency ingestion + fast read queries, 
  [indexing](indexes.md) - for faster writes/queries, Multi-Dimensional Partitioning (Z-Ordering), automatic cleanup of uncommitted data with marker mechanism, 
  [auto cleaning](cleaning.md) to automatically removing old versions of files.
- Hudi provides rich options for pre-sorting/loading data efficiently and then follow on with rich set of data clustering techniques to manage file sizes and data distribution within a table. In each case, Hudi provides high-degree of configurability in terms of when/how often these services are scheduled, planned and executed. For e.g. Hudi ships with a handful of common planning strategies for compaction and clustering.
- Along with compatibility with other open table formats like [Apache Iceberg](https://iceberg.apache.org/)/[Delta Lake](https://delta.io/), and catalog sync services to various data catalogs, Hudi is one of the most open choices for your data foundation.


## Efficient Data lakes with Incremental Processing

Organizations spend close to 50% of their budgets on data pipelines, that transform and prepare data for consumption. As data volumes increase, so does the cost of running these pipelines.
Hudi has a unique combination of features that make it a very efficient choice for data pipelines, by introducing a new paradigm for incremental processing of data. The current state-of-the-art 
prescribes two completely different data stacks for data processing. Batch processing stack stores data as files/objects on or cloud storage, processed by engines such as Spark, Hive and so on. On the other hand, the 
stream processing stack stores data as events in independent storage systems like Kafka, processed by engines such as Flink. Even as processing engines provide unified APIs for these two styles of data processing, 
the underlying storage differences make it impossible to use one stack for the other. Hudi offers a unified data lakehouse stack that can be used for both batch and streaming processing models. 

Hudi introduces "incremental processing" to bring stream processing model (i.e. processing only newly added or changed data every X seconds/minutes) on top of batch storage (i.e. data lakehouse built on open data formats
on the cloud), combining the best of both worlds. Incremental processing requires the ability to write changes quickly into tables using indexes, while also making the data available for querying efficiently.
Another requirement is to be able to efficiently compute the exact set of changes to a table between two points in time for pipelines to efficiently only process new data each run, without having to scan the entire table.
For the more curious, a more detailed explanation of the benefits of _incremental processing_ can be found [here](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop).

### Why Hudi?

- By bringing streaming primitives to data lake storage, Hudi opens up new possibilities by being able to ingest/process data within few minutes and eliminate need for specialized real-time analytics systems.
- Hudi groups records into file groups, with updates being tied to the same file group, limiting the amount of data scanned for the query i.e only log files within the same file group need to be scanned for a given base file
- Hudi adds low-overhead record level metadata and supplemental logging of metadata to compute CDC streams, to track how a given changes/moves within the table, in the face of writes and background table services. For e.g. Hudi is able to preserve change history even if many small files are combined into another file due to clustering 
  and does not have any dependency on how table snapshots are maintained. In snapshot based approaches to tracking metadata, expiring a single snapshot can lead to loss of change history.
- Hudi can encode updates natively without being forced to turn them into deletes and inserts, which tends to continuously redistribute records randomly across files, reducing data skipping efficiency. Hudi associates a given delete or update to the original file group that the record was inserted to (or latest clustered to), which preserves the spatial locality of clustered data or temporal order in which record were inserted. As a result, queries that filter on time (e.g querying events/logs by time window), can efficiently only scan few file groups to return results.
- Building on top of this, Hudi also supports partial update encoding for encoding partial updates efficiently into delta logs. For columnar data, this means write/merge costs are proportional to number of columns in a merge/update statement.
- The idea with MoR is to reduce write costs/latencies, by writing delta logs (Hudi), positional delete files (iceberg). Hudi employs about 4 types of indexing to quickly locate the file that the updates records belong to. Formats relying on a scan of the table can quickly bottleneck on write performance. e.g updating 1GB into a 1TB table every 5-10 mins.
- Hudi is the only lakehouse storage system that natively supports event time ordering and late data handling for streaming workloads where MoR is employed heavily. 





