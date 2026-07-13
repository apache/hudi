---
title: "What is a Streaming Data Lake?"
excerpt: "Defines the streaming data lake: a data lake that ingests and serves data continuously with minute-level freshness, and the storage building blocks that make it possible."
description: "What a streaming data lake is, how it differs from batch data lakes and Kafka + warehouse stacks, and the storage building blocks that make it work."
authors: [vinoth-chandar]
category: how-to
image: /assets/images/blog/hudi_streaming.png
tags:
- streaming
- data lakehouse
- mor
- cdc
- incremental processing
---

A streaming data lake is a data lake that ingests and serves data continuously — with minute-level freshness — instead of through periodic batch loads. Batch-era data lakes refresh on a schedule, typically hourly or daily, which was acceptable when the lake mostly fed reports and offline model training. Today, operational analytics, change data capture (CDC) from transactional databases, and AI applications all want much fresher data — and the traditional answers, standing up a separate warehouse for the fresh slice or keeping everything in Kafka, either duplicate data into proprietary systems or become prohibitively expensive to retain at scale. A streaming data lake closes that gap: one copy of the data, on open formats in cloud storage, written to continuously and readable within minutes.

<!--truncate-->

This post is the definitional companion to the original 2021 essay, [Apache Hudi - The Data Lake Platform](/blog/2021/07/21/streaming-data-lake-platform), which first articulated this vision in depth. Here we focus on what the term means in 2026, what it takes to build one, and how it relates to the data lakehouse.

## From Batch Data Lakes to Streaming Data Lakes

A batch data lake and a streaming data lake can look identical on the surface — Parquet files on object storage, queried by Spark, Trino, or Presto. The difference lies in three operating assumptions.

**Ingestion cadence.** A batch lake receives data in large, scheduled loads: a nightly dump, an hourly partition. A streaming data lake receives data continuously, committing every few minutes. This is not just a smaller batch interval. Committing hundreds of times a day instead of once means the storage layer must absorb many small writes without accumulating thousands of tiny files, must keep table metadata cheap to update on every commit, and must reconcile new data against old data constantly rather than once a night.

**Mutation support.** Batch lakes were built around append-only, immutable files, and rewrote whole partitions to "update" anything. Streaming sources are dominated by mutation: CDC streams from databases are sequences of inserts, updates, and deletes keyed by primary key; late-arriving events revise records that were already written. A streaming data lake must treat record-level upserts and deletes as first-class write operations, applied continuously.

**Read patterns.** Batch consumers scan the latest partition after the nightly load lands. Streaming consumers need more: dashboards want the freshest consistent snapshot, downstream pipelines want to ask "what changed since I last ran?" and process only that delta. A streaming data lake serves both the classic full-table scan and this incremental, change-oriented consumption.

None of these can be bolted onto a batch lake with a faster scheduler. They require rethinking storage itself.

![The incremental stack blends the latency profile of stream processing with the efficiency and scale of batch storage and compute.](/assets/images/blog/datalake-platform/hudi-data-lake-platform_-_Page_2_4.png)
<p align = "center">Figure: The incremental stack, alongside batch and stream processing</p>

## The Required Building Blocks

Five capabilities separate a lake that can operate in streaming mode from one that cannot.

**Continuous upserts, backed by indexes.** Applying a CDC stream means locating, for every incoming record, the file that holds the previous version of that key — millions of times per commit. Doing this by scanning data is hopeless; a streaming data lake needs indexes that map record keys to file locations so upserts and deletes touch only the files that actually change. This is why indexing sits in the write path of Apache Hudi rather than being only a query-side optimization.

**Merge-on-Read storage.** With frequent commits, rewriting columnar base files on every write (Copy-on-Write) multiplies write amplification: a handful of changed rows can force rewriting an entire file, hundreds of times a day. The [Merge-on-Read (MOR) table type](/docs/table_types) instead absorbs changes into compact delta log files and merges them with base files at query time or during compaction. Write cost then scales with the amount of change, not with the size of accumulated state — the property that makes minute-level commits economical. The MOR series on this blog makes the deeper case that this is [an architectural shift, not a storage tuning knob](/blog/2026/05/14/mor-isnt-a-storage-optimization-its-an-architectural-shift), one that extends all the way into [the metadata layer itself](/blog/2026/06/05/why-metadata-has-to-be-mutation-friendly).

![Merge-on-Read table with base files, delta log files, periodic compaction, and both snapshot and read-optimized queries.](/assets/images/MOR_new.png)
<p align = "center">Figure: A Merge-on-Read table absorbing frequent commits into delta logs</p>

**Asynchronous table services.** Deferring merge work is only viable if something reliably performs it — without stopping ingestion. A streaming data lake needs built-in table services: compaction to fold delta logs into new base files, clustering to reorganize data layout for query performance, and cleaning to reclaim old file versions. Crucially, these must run asynchronously, concurrently with the ingestion stream. If maintenance requires pausing the writer, the lake is not really streaming; it is alternating between ingesting and repairing itself. Hudi's services are aware of each other and of active writers through a shared timeline, which is what allows [even indexes to be built on tables that never stop changing](/blog/2026/06/25/building-indexes-on-a-moving-target).

**Change streams.** Once tables update continuously, downstream pipelines should not recompute from full scans. A streaming data lake exposes record-level change streams — the ability to query exactly the records that changed between two points in time — so that derived tables can be maintained incrementally. This is what turns a single fast table into an end-to-end incremental architecture, where each layer consumes the changes of the previous one, much like chained stream processing jobs but over columnar lake storage.

**Concurrency control.** Continuous writers, background services, and readers all operate on the same table at once. Readers need snapshot isolation so queries always see a consistent, committed view. Writers and table services need to proceed without blocking one another — an optimistic model that simply fails one side wastes enormous resources when a long-running compaction overlaps a stream that commits every minute. Streaming-first designs favor non-blocking, MVCC-style coordination between the writer and table services, reserving conflict resolution for the cases that genuinely need it.

## The Write Path: Streaming Ingestion

In practice, the write path of a streaming data lake is a stream processing job that terminates in lake storage. [Apache Flink can write continuously into Hudi tables](/docs/flink-quick-start-guide), applying CDC streams from Kafka or Debezium as upserts; Spark Structured Streaming and Hudi's own streaming ingestion utilities do the same. The writer commits at a chosen interval — commonly every one to five minutes — and each commit is atomic: queries see all of it or none of it.

Two streaming realities shape this path. First, **ordering**: events for the same key can arrive out of order, so the writer resolves collisions deterministically using an ordering field (typically event time), ensuring an older update never overwrites a newer one regardless of arrival order. Second, **late data**: an event may arrive hours or days after the period it belongs to. In a batch lake this forces re-running jobs over old partitions; in a streaming data lake the index locates the affected records wherever they live, and the late event is applied as a normal upsert — late data is just another update.

## The Read Path: Snapshot, Read-Optimized, and Incremental Queries

A streaming data lake gives readers an explicit choice of freshness versus cost, expressed as [three query types](/docs/table_types):

- **Snapshot queries** merge base files with delta logs at read time, returning the freshest committed state — data written minutes ago. This is the near-real-time analytics path.
- **Read-optimized queries** read only compacted base files, giving pure columnar scan performance at the price of bounded staleness (data freshness tracks the compaction schedule). Latency-tolerant reporting can run here at lower cost.
- **Incremental queries** return the records that changed between two commit times, powering the incremental pipelines described above.

The same table serves all three, from the same single copy of data, through standard engines. There is no separate "speed layer" holding fresh data and "batch layer" holding history to reconcile — the failure mode that made Lambda architectures notoriously hard to operate.

## Streaming Data Lake vs. Kafka + Warehouse

The common alternative for fresh analytics is to keep data in Kafka and continuously load it into a cloud warehouse. It is worth being honest about the trade-offs in both directions.

**Where Kafka + warehouse wins.** Kafka delivers events in milliseconds to seconds, and a stream processor reading Kafka can react at that speed — a streaming data lake, committing every few minutes to object storage, cannot. If the use case is sub-second serving, alerting, or event-driven applications, Kafka (plus a stream processor or real-time OLAP store) is the right tool. Warehouses also offer a highly polished, fully managed SQL experience.

**Where the streaming data lake wins.** Retention and scale economics. Keeping weeks or months of history in Kafka is expensive, and Kafka is not queryable with analytical SQL — so data ends up copied into a warehouse anyway, where storage and compute are metered at warehouse prices and the data lands in a proprietary format usable only by that warehouse's engine. A streaming data lake stores one copy on cheap object storage in open formats, retains history indefinitely, serves large scans, joins, and ML training workloads directly, and remains accessible to every engine. For the broad middle ground — data that must be fresh within minutes and queryable for years — it removes both the Kafka retention bill and the warehouse copy.

The two are complementary rather than exclusive: a common pattern keeps Kafka as the transport at the edge, with the streaming data lake as the durable, queryable system of record a few minutes behind it.

## How This Connects to the Lakehouse

A [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse) brings warehouse capabilities — transactions, schema management, mutability — to data lake storage. A streaming data lake is best understood as **a lakehouse operated in streaming mode**: the same open storage and transactional foundation, but with continuous writes instead of scheduled loads, change streams instead of full refreshes, and table services that run concurrently with ingestion instead of in maintenance windows. Every streaming data lake is a lakehouse; not every lakehouse is operated — or architecturally able to operate — as a streaming data lake, because the building blocks above (write-path indexing, MOR, async services, record-level change streams, non-blocking concurrency) must be designed in rather than added on.

This is where Apache Hudi's history matters. Hudi was created at Uber in 2016 precisely for this problem — applying massive upsert streams to petabyte-scale tables on lake storage with minute-level freshness — and its architecture follows from that origin: pluggable indexing in the write path, Merge-on-Read storage, a suite of built-in asynchronous table services, and incremental queries as a core primitive. The [2021 essay](/blog/2021/07/21/streaming-data-lake-platform) laid out this "streaming data lake platform" framing when much of the industry was still discussing table formats for batch workloads; the intervening years of CDC, near-real-time analytics, and AI-driven freshness demands have made it the mainstream requirement it is today.

## Conclusion

A streaming data lake is not a faster batch lake or a bigger Kafka cluster. It is a data lake whose storage layer is designed for continuous change: upserts resolved through indexes, writes absorbed by Merge-on-Read, maintenance performed asynchronously by table services, changes exposed as first-class streams, and all of it coordinated safely under concurrent access. In return, it delivers minute-level data freshness on open formats and cheap storage, serving snapshot, read-optimized, and incremental consumers from a single copy of data. If the lakehouse defined *what* modern lake storage can do, the streaming data lake defines *how continuously* it can do it — and it is increasingly the default way fresh data platforms are built.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'What is a streaming data lake?',
    answer: 'A streaming data lake is a data lake that ingests and serves data continuously, with minute-level freshness, instead of through periodic batch loads. It keeps data in open formats on cloud object storage while supporting continuous upserts, incremental change streams, and background table maintenance that runs without stopping ingestion.'
  },
  {
    question: 'What is the difference between a streaming data lake and a data lakehouse?',
    answer: 'A data lakehouse brings warehouse capabilities like transactions and mutability to data lake storage, while a streaming data lake is a lakehouse operated in streaming mode. It adds continuous minute-level ingestion, record-level change streams for incremental consumers, and asynchronous table services that run alongside live writers. Every streaming data lake is a lakehouse, but many lakehouses are only operated as batch systems.'
  },
  {
    question: 'Can a data lake handle real-time data?',
    answer: 'Yes, within limits. A streaming data lake built on storage like Apache Hudi can commit continuously and make data queryable within minutes, which covers most operational analytics and CDC replication needs. For sub-second latency, such as event-driven applications or instant alerting, a stream processor or real-time OLAP system reading from Kafka is still the better fit.'
  },
  {
    question: 'Do I still need Kafka with a streaming data lake?',
    answer: 'Usually yes, but in a narrower role. Kafka remains an excellent transport for moving events from producers to consumers in seconds, and it commonly feeds the streaming data lake. What changes is retention and analytics: instead of keeping long histories in Kafka or copying everything into a warehouse, the lake becomes the durable, cheaply stored, SQL-queryable system of record a few minutes behind the stream.'
  },
  {
    question: 'Why is Merge-on-Read important for streaming ingestion?',
    answer: 'Merge-on-Read absorbs updates and deletes into small delta log files instead of rewriting large columnar files on every commit, so write cost scales with the amount of change rather than the size of the table. That is what makes committing every few minutes economical. A background compaction service later folds the logs into optimized base files without pausing ingestion.'
  },
  {
    question: 'How fresh can data in a streaming data lake be?',
    answer: 'Typically within a few minutes of the source event, determined by the commit interval of the streaming writer. Snapshot queries see every committed change immediately, while read-optimized queries trade some freshness for faster scans by reading only compacted files. The freshness floor comes from object storage and columnar file economics, which favor batching writes into small intervals rather than per-event commits.'
  }
]} />
