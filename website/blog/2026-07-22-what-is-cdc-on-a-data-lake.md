---
title: "What is CDC on a Data Lake?"
excerpt: "A beginner-friendly explainer of change data capture (CDC) on a data lake: what it is, why it needs a table layer with upserts, and how a Debezium-to-Hudi pipeline fits together."
description: "Learn what change data capture (CDC) on a data lake is, why it requires upserts and deletes, and how tools like Debezium and Apache Hudi make it work."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/data-lake-cdc/hudi-cdc.jpg
tags:
- cdc
- streaming
- debezium
- data lakehouse
- beginner
---

Change data capture (CDC) on a data lake is the practice of continuously replicating inserts, updates and deletes from operational databases into tables on cloud storage, so that analytics, machine learning and downstream pipelines always work against a fresh, queryable copy of the source data. Teams adopt it for three main reasons: it offloads heavy analytical queries from transaction-processing (OLTP) databases that were never built for them, it consolidates data from many operational systems into a single source of truth, and it shrinks data freshness from "last night's batch" to minutes. The catch is that plain data lakes — collections of immutable files on object storage — historically could not absorb a change stream at all, because applying an update or a delete requires rewriting data in place, and raw file storage offers no way to do that safely. This post explains, from first principles, what CDC is, why the data lake became its natural destination, and what has to sit in between to make the combination work.

If you already know the basics and want a deeper treatment of capture mechanisms and architecture trade-offs, read the companion deep dive, [Understanding Data Lake Change Data Capture](/blog/2024/07/30/data-lake-cdc), after this one.

<!--truncate-->

## What is change data capture?

Every operational database is constantly changing: orders are placed, profiles are edited, rows are deleted. Change data capture is any technique that identifies those row-level changes as they happen and delivers them, in order, to another system. Instead of periodically copying a whole table, CDC ships only the delta — each event typically carrying the operation type (insert, update or delete), the affected row, and often its before and after state.

There are two broad families of capture:

- **Query-based capture** polls the source tables for changes, usually by filtering on a `last_updated` timestamp column. It is simple to set up but has real gaps: it misses hard deletes entirely (a deleted row simply stops appearing in query results), it puts recurring query load on the production database, and it requires the schema to cooperate.
- **Log-based capture** reads the database's own transaction log — the binlog in MySQL, the write-ahead log (WAL) in PostgreSQL — which records every committed change in commit order. Because it taps a log the database writes anyway, it captures every insert, update and delete with minimal impact on the source, and it is the approach virtually all serious CDC deployments use today.

![Log-based CDC](/assets/images/blog/data-lake-cdc/log-based-cdc.png)

[Debezium](https://debezium.io/) is the most widely used open source log-based CDC tool. It runs as a Kafka Connect connector, tails the binlog or WAL, and publishes one change event per row modification to a Kafka topic per table, typically alongside a schema registry that tracks the shape of those events. Cloud services such as AWS DMS and various managed connectors play the same role. Whatever the tool, the output is the same in spirit: an ordered stream of row-level change events that some downstream system must now apply.

## Why the data lake is the natural CDC destination

The destination question — where should that change stream land? — has an economic answer and an architectural one.

Economically, cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage) is the cheapest place to keep large volumes of data, and it scales without capacity planning. Replicating dozens or hundreds of operational tables, with full history, is affordable there in a way it is not in a warehouse whose storage and compute pricing were designed for curated, aggregated data.

Architecturally, data stored in open file formats like Apache Parquet on object storage is not locked to any one engine. The same replicated tables can serve Spark for ETL, Trino or Presto for interactive SQL, and Python-based ML frameworks — without making another copy per engine. That "one copy, many engines" property is the core promise of the [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse), and a continuously replicated mirror of your operational databases is one of the most valuable datasets you can put at its base.

But there is a fundamental mismatch. A CDC stream is, by definition, mostly *mutations* — updates and deletes to existing rows. A plain data lake is a pile of immutable files: you can add new files, but you cannot change a row inside one. Without something extra, teams historically resorted to painful workarounds — appending every change event and forcing every query to deduplicate, or periodically rewriting entire partitions to fold changes in. Both are slow, expensive and error-prone. Closing this gap is exactly why transactional table layers for the lake emerged; Apache Hudi, notably, grew out of the need to apply database change streams to lake storage at scale at Uber.

## What applying CDC requires from the table layer

To turn a change stream into a correct, queryable table on object storage, the layer above the raw files needs a specific set of capabilities:

- **Upserts and deletes.** Each change event must be applied against the existing table: an update replaces the current version of a row identified by its primary key, and a delete removes it. Doing this efficiently means quickly finding which file holds a given key — Hudi does so with pluggable [indexes](/docs/indexes), including a record-level index, so it does not have to scan the table on every write.
- **Ordering and late events.** Change events can arrive out of order, especially after retries or repartitioning in the transport layer. If an older update is applied after a newer one, the table silently travels back in time. The table layer must merge records by an ordering field — the source database's log sequence number or position — so the correct final state wins regardless of arrival order. Hudi supports this natively through event-time ordered merging via its [record merger](/docs/record_merger) APIs.
- **Schema evolution.** Source tables change: columns get added, types get widened. A CDC pipeline that fails on every upstream `ALTER TABLE` is not operable. The table layer needs to evolve the target [schema safely](/docs/schema_evolution), usually in coordination with a schema registry.
- **Transactional guarantees.** Writers apply changes continuously while queries run concurrently. ACID transactions ensure readers never see a half-applied batch, and failed writes can be rolled back cleanly rather than leaving corrupt partial files behind.

One more design choice matters for CDC specifically: how the table absorbs writes. Copy-on-write tables rewrite whole data files for every batch of updates, which is simple but costly when updates are frequent. Merge-on-read tables, by contrast, log changes as compact delta files and compact them into columnar base files asynchronously — which is why [merge-on-read](/docs/table_types) is generally the recommended table type for update-heavy CDC ingestion.

## The two sides of CDC on a lakehouse

It is worth separating two roles that often get blurred together, because a lakehouse can — and usually should — play both.

**Consuming CDC** is the direction discussed so far: the lakehouse table is the *destination*, continuously applying a change stream from an operational database so it stays an up-to-date mirror.

**Producing change streams** is the reverse: the lakehouse table acts as a *source* of changes for whatever sits downstream. Once your bronze tables absorb CDC, the next question is how the silver and gold tables built on top of them stay fresh. If each downstream job re-reads the whole upstream table, you have merely moved the batch-processing problem one hop over. Hudi addresses this with incremental queries — a consumer can ask a table for "everything that changed since my last checkpoint" and receive just those records, with commit-time watermarks serving as reliable checkpoints. Hudi can also emit full before/after images of changed records, database-changelog style, for consumers that need them. This is what turns a single CDC feed into a chain of [incremental pipelines](/docs/use_cases), where every layer processes only deltas.

## A reference architecture: MySQL/Postgres to a Hudi table

Here is what a typical end-to-end CDC pipeline onto a data lake looks like, using widely deployed open source components:

![CDC architecture with Apache Hudi](/assets/images/blog/data-lake-cdc/hudi-cdc.jpg)

1. **Source database.** MySQL or PostgreSQL, configured to expose its change log (binlog or logical-replication WAL).
2. **Capture: Debezium on Kafka Connect.** A Debezium connector tails the log and writes one change event per row modification into a Kafka topic per table, registering event schemas with a schema registry (Confluent or Apicurio).
3. **Apply: Hudi Streamer or Flink.** [Hudi Streamer](/docs/hoodie_streaming_ingestion) is a self-contained ingestion utility that ships with Hudi and includes a Debezium source out of the box: running in continuous mode, it reads the change events from Kafka and upserts them into a Hudi table on cloud storage, using the table's record key (the source primary key) to route updates and the log position to order them. Teams standardized on Flink can use Hudi's Flink integration for the same job. For a hands-on walkthrough with configurations, see [Change Data Capture with Debezium and Apache Hudi](/blog/2022/01/14/change-data-capture-with-debezium-and-apache-hudi).
4. **Bronze table.** The result is a "raw" replicated table — typically merge-on-read — that mirrors the source, kept fresh within minutes, registered in a catalog (Glue, Hive Metastore, etc.) and queryable from Spark, Trino, Presto and other engines.
5. **Incremental ETL downstream.** Silver and gold tables subscribe to the bronze table via incremental queries, pulling and transforming only the records that changed since their last run.

Getting an existing table's history in before streaming begins — the bootstrap — is handled either by letting Debezium take an initial consistent snapshot or by bulk-loading the table directly (for example over JDBC) and then starting the change stream from the matching checkpoint.

## Common pitfalls, and how the table layer absorbs them

A few failure modes recur in almost every CDC-on-the-lake deployment. Knowing them up front is most of the battle:

- **Silently dropped deletes.** Query-based capture cannot see hard deletes, and some pipelines ignore delete events on the sink side. The mirror then diverges from the source, one deleted row at a time — a correctness and compliance problem (GDPR erasure requests must propagate). The fix is log-based capture plus a sink that applies deletes as first-class operations, as Hudi's Debezium source does by honoring the delete marker in each event.
- **Out-of-order events.** Retries and parallelism can deliver an old version of a row after a newer one. A sink that blindly takes the last arrival will regress data. Merging by the source log position, rather than arrival time, makes the pipeline immune to this.
- **Small files.** Frequent micro-batches naturally produce many tiny files, and query performance on object storage degrades sharply as file counts explode. Hudi manages [file sizing](/docs/file_sizing) during writes and runs compaction and clustering as background table services, so ingestion frequency does not have to be traded against query speed.
- **Backfills colliding with the stream.** Sooner or later you must reload history — a missed window, a schema fix — while live ingestion continues. Without transactions, a backfill job and a streaming writer stepping on each other corrupt the table. A transactional table layer with concurrency control lets the backfill run alongside the stream safely.

## Conclusion

CDC on a data lake pairs the most valuable data most organizations have — the live state of their operational databases — with the cheapest, most open place to analyze it. The capture side is a solved problem thanks to log-based tools like Debezium; the hard part has always been the destination, because applying a stream of updates and deletes to immutable cloud storage requires upserts, ordering, schema evolution and transactions that raw file storage lacks. That is precisely the gap transactional lakehouse layers like Apache Hudi close, while adding the second half of the story: tables that can *emit* change streams so everything downstream becomes incremental too. To go deeper on capture mechanisms and architecture patterns, continue with [Understanding Data Lake Change Data Capture](/blog/2024/07/30/data-lake-cdc); to build one yourself, start with the [Debezium how-to](/blog/2022/01/14/change-data-capture-with-debezium-and-apache-hudi) and the [Hudi Streamer docs](/docs/hoodie_streaming_ingestion).

## FAQ

<PostFAQ heading={null} items={[
  {question: 'What is the difference between CDC and ETL?', answer: 'ETL is the general process of extracting data from sources, transforming it and loading it into a target system, traditionally as periodic bulk jobs that reprocess full tables. CDC is a specific extraction technique that captures only the row-level inserts, updates and deletes as they happen in the source database. In practice CDC often serves as the extract step of a continuous, incremental ETL pipeline, replacing full-table copies with a stream of deltas.'},
  {question: 'Can you do CDC without Kafka?', answer: 'Yes. Kafka is a popular transport for change events, but it is not required. Debezium can run as a standalone server that writes to other messaging systems, cloud services like AWS DMS can deliver changes directly to object storage, and ingestion tools can pull changes from a database over JDBC. Kafka earns its place in larger deployments because it buffers events durably and lets multiple consumers replay the same stream independently.'},
  {question: 'How are deletes handled in CDC on a data lake?', answer: 'Log-based CDC tools emit an explicit delete event when a row is removed from the source database. A transactional table layer such as Apache Hudi applies that event by locating the record via its key and removing it from the table, so the lake copy stays consistent with the source. This matters for correctness and for compliance, since regulations like GDPR require deletions to propagate to downstream copies.'},
  {question: 'What is the difference between log-based and query-based CDC?', answer: 'Query-based CDC repeatedly queries source tables for rows with a recent last-updated timestamp, which is simple but misses hard deletes and adds load to the production database. Log-based CDC reads the database\'s own transaction log, such as the MySQL binlog or PostgreSQL WAL, capturing every insert, update and delete in commit order with minimal source impact. Log-based capture is the standard choice for production pipelines.'},
  {question: 'Why can\'t a plain data lake handle CDC?', answer: 'A plain data lake stores immutable files on object storage, so there is no built-in way to update or delete an individual record, which is exactly what most CDC events require. Teams either appended all events and deduplicated at query time or rewrote whole partitions, both slow and error-prone. Transactional table layers like Apache Hudi add upserts, deletes, indexing and ACID guarantees on top of the same storage, making CDC ingestion practical.'},
  {question: 'How fresh can data be with CDC on a data lake?', answer: 'End-to-end latency from a database commit to a queryable row on the lake is typically measured in minutes with continuously running ingestion. Merge-on-read tables help by logging changes cheaply on write and compacting them in the background, instead of rewriting large files on every batch. That is near real-time for analytics, though not a replacement for the millisecond latencies of an operational database.'},
]} />
