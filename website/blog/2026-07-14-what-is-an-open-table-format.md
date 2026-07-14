---
title: "What is an Open Table Format?"
excerpt: "What open table formats like Apache Iceberg, Apache Hudi and Delta Lake actually are — table metadata plus a transaction protocol that turns files on object storage into transactional tables."
description: "An open table format is table metadata plus a transaction protocol that turns files on cloud storage into database tables with ACID commits, schema evolution and time travel — Apache Iceberg, Hudi, Delta Lake and Paimon compared."
authors: [vinoth-chandar]
category: how-to
image: /assets/images/blog/dlh_1200.png
tags:
- table format
- data lakehouse
- apache iceberg
- delta lake
- open architecture
- apache xtable
---

An open table format is an openly governed specification with two inseparable parts: **table metadata** — which files make up the table, what schema they follow, what statistics they carry, at every version — and a **transaction protocol** — the rules by which writers commit changes atomically and readers see consistent snapshots. Together they turn a collection of files on cloud object storage into a database-like table with ACID transactions, schema evolution, and time travel. Apache Hudi, Apache Iceberg, Delta Lake, and Apache Paimon are the major open table formats; in all of them the data itself stays in open file formats like [Apache Parquet](https://parquet.apache.org) or [Apache ORC](https://orc.apache.org).
Because the specification is open, any query engine — Apache Spark, Apache Flink, Trino, and many others — can read and write the same table without going through a proprietary system.

Open table formats emerged to fix a specific gap. Cloud object stores such as Amazon S3, Azure Blob Storage, and Google Cloud Storage made it cheap to store virtually unlimited data, and columnar file formats made it efficient to scan. But a pile of Parquet files is not a table: no atomic commits, no record-level updates or deletes, no history to query, and no protection for a reader scanning a directory mid-rewrite.

Table formats close that gap by layering a transaction log and rich table metadata over the files, and in doing so became the foundational component of the [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse) architecture — warehouse-grade reliability and management over open data on the lake.

This post covers the problem, the anatomy of a table format, the major open projects, and how the pieces fit together.

## The Problem Before Table Formats

Until roughly a decade ago, the de-facto "table" abstraction on data lakes was the Apache Hive model: a table was a directory, partitions were subdirectories, and the Hive Metastore recorded the schema and the list of partitions. This worked well enough on HDFS for append-only batch workloads, but it had structural weaknesses that cloud object storage made acute.

- **No atomicity.** A failed job left partial files behind, and readers could not distinguish committed data from garbage; jobs resorted to fragile rename tricks that object stores do not support atomically.
- **No record-level mutations.** Updating or deleting a record meant rewriting entire partitions, making change data capture (CDC) from operational databases and GDPR deletions painful and expensive.
- **Coarse, expensive planning.** Query planning relied on listing directories — slow and costly on object stores — and on partition values as the only pruning mechanism; statistics about the actual data in each file lived nowhere.
- **No isolation or history.** A query could read an inconsistent mix of old and new files while a pipeline rewrote the table, and with no table versions there was no time travel, rollback, or incremental "what changed since yesterday" queries.

The fix was to stop treating a table as a directory listing and treat it instead as versioned metadata plus a commit protocol: a transactionally maintained record of the files that make up the table. Apache Hudi pioneered this idea in production, built at Uber in 2016 for near-real-time ingestion with updates, deletes, and incremental processing on one of the world's largest data lakes, and described publicly in [Uber's engineering blog](https://www.uber.com/blog/hoodie/) in early 2017. Hudi solved the transaction problem as one piece of a broader goal — what the industry now calls the **transactional data lake**, a term AWS itself uses when [comparing these formats](https://aws.amazon.com/blogs/big-data/choosing-an-open-table-format-for-your-transactional-data-lake-on-aws/) — so it shipped table maintenance, indexing, and record-level updates alongside the format from the start. Apache Iceberg arrived independently from Netflix's need for correct, scalable table metadata, and positioned itself squarely around the table format core: the specification and libraries, with maintenance left to engines. Delta Lake came out of Databricks' Spark workloads and, like Hudi, always paired its format with broader capabilities such as updates and table maintenance on its platform. Apache Paimon is a later entrant with a narrower focus: LSM-tree storage on the lake for high-frequency streaming writes from Apache Flink. A useful shorthand: Iceberg *is* a table format; Hudi and Delta Lake are more, with a table format inside.

## What a Table Format Actually Consists Of

Strip away the branding and every open table format is a specification for a handful of cooperating pieces of metadata stored alongside the data files.

![Table format layer in the Apache Hudi stack](/assets/images/blog/hudistack/table_format_1.png)
<p align = "center">Figure: The table format as a metadata layer above open data file formats</p>

**A log of changes to the table.** The heart of a table format is an ordered record of every commit — data writes, deletes, schema changes, and maintenance operations. In Hudi this is the [timeline](/docs/timeline); Delta Lake calls it the transaction log; Iceberg maintains a chain of snapshots referenced from table metadata files. Whatever the name, this log is the source of truth: a change is part of the table if and only if it is committed to the log, and commit is an atomic operation on storage. The designs are converging here: proposals in the Iceberg community for format version 4 — single-file commits and an adaptive root manifest — would move its commit metadata toward the log-structured approach Hudi's timeline and Delta's transaction log have used from the beginning, though these are not yet part of the Iceberg specification.

**Snapshots and versioning.** Every commit produces a new consistent view of the table, so readers can query it "as of" any retained version — enabling time travel for debugging and audits, rollback of bad writes, and, where the format tracks record-level changes, incremental queries that return only what changed between two points in time.

**File-level statistics and indexes.** Table formats track per-file and per-column statistics — min/max values, null counts, record counts — that engines use to skip files that cannot contain matching rows, often eliminating most I/O for selective queries. Hudi generalizes this into a [multi-modal indexing subsystem](/docs/indexes) stored in an internal [metadata table](/docs/metadata), including column statistics, bloom filters, and record-level indexes that also accelerate the write path for updates and deletes.

**Schema tracking and evolution.** The format stores the table's schema and defines rules for [evolving it](/docs/schema_evolution) — adding, renaming, or promoting column types — without rewriting existing data files, plus schema enforcement on write to catch pipeline breakages before bad data lands in the table.

**Concurrency control.** Finally, the specification defines how multiple writers and background jobs coexist. All major formats give readers snapshot isolation — a query sees a consistent version of the table regardless of in-flight writes — while for concurrent writers they define mechanisms from optimistic concurrency control to non-blocking approaches for jobs touching disjoint or reconcilable data; Hudi's [concurrency control](/docs/concurrency_control) documentation covers these trade-offs in depth.

How data is physically organized under this metadata varies too. Hudi, for example, defines two [table types](/docs/table_types): Copy-on-Write, which rewrites base files on update for maximum read performance, and Merge-on-Read, which writes changes to compact log files that are periodically merged — a trade-off between write latency and read cost that other formats have since adopted variants of.

Note, though, that everything above is *specification* — passive metadata and protocol rules. A table format defines what a valid table looks like; it does not keep the table healthy. In database terms, that job belongs to a **storage engine**: the machinery that compacts small files, clusters data, cleans up old versions, and builds and maintains indexes. Every table on a lake needs this running somewhere — built into the project, supplied by the query engine, or bought from a vendor — and this distinction, more than any metadata layout detail, is where the major projects genuinely differ.

## The Major Open Table Formats

Four open source projects dominate the space today. All are openly governed (three at the Apache Software Foundation, one at the Linux Foundation), and all deliver the core capabilities above; they differ in design center and scope.

**[Apache Hudi](https://hudi.apache.org)** originated at Uber in 2016 for fast, mutable ingestion — upserts, deletes, and incremental processing — on the data lake. Hudi is accurately described as a table format plus a storage engine: beyond the storage specification (timeline, file layout, metadata table), it ships a writer runtime with pluggable indexing for fast updates, and built-in [table services](/docs/hudi_stack) — compaction, clustering, cleaning, and indexing — that run inline or asynchronously to keep tables optimized without separate orchestration. Its design center is incremental: the table as a stream of changes that downstream pipelines can efficiently consume.

**[Apache Iceberg](https://iceberg.apache.org)** originated at Netflix and was donated to the ASF in 2018. Iceberg's design center is an engine-agnostic specification for table metadata, with immutable snapshots, hidden partitioning (partition values derived from column transforms), and well-defined metadata evolution. It deliberately scopes itself to the format and libraries, leaving table maintenance to engines and vendors, and has attracted the broadest catalog and vendor ecosystem via its REST catalog specification.

**[Delta Lake](https://delta.io)** was created at Databricks and open-sourced in 2019; it is governed under the Linux Foundation. Delta centers on a JSON-based transaction log with periodic Parquet checkpoints, offering ACID transactions, schema enforcement, and time travel. Like Hudi, it has always been more than a format on its home platform, pairing the log with maintenance operations such as optimize and vacuum. It is the native format of Databricks and has first-class Spark integration, with a growing standalone ecosystem through the Delta Kernel and Rust implementations.

**[Apache Paimon](https://paimon.apache.org)** is the newest of the four, grown out of the Flink Table Store subproject and a top-level Apache project since 2024. Paimon is narrower in scope than the other three: its design center is LSM-tree (log-structured merge tree) storage on the lake, optimizing write speed for high-frequency updates and changelog-producing tables consumed by Apache Flink, while remaining readable by batch engines like Spark and Trino. The trade-off is a classic one from storage theory (the [RUM conjecture](https://bytearray.substack.com/p/computer-science-behind-lakehouse)): LSM-style layouts defer merge work to read time, so scan performance degrades unless compaction keeps pace — a consideration that matters on lakes, where reads are the dominant workload.

An honest summary: the formats have converged substantially on core capabilities — ACID commits, schema evolution, time travel, statistics-based pruning — and continue to borrow ideas from one another. But write performance and read performance remain distinct problems with different trade-offs, even today: a design that makes writes cheap taxes readers unless compaction and read-side indexing keep pace, while one that optimizes purely for scans makes every update a rewrite. Because reads dominate lake workloads, a format has to be engineered for both sides, and this is where the remaining differences live: write-path design (indexing, merge strategies), how much storage-engine machinery is built in versus left to the engine or vendor, and ecosystem depth per engine. Hudi's long-standing bet is performance across both paths — indexed writes to make updates fast, automated compaction and clustering to keep reads fast — which is why it remains the strongest fit for high-volume mutable and incremental workloads.


## How Query Engines Integrate

A table format is only useful insofar as engines speak it. Integration happens at two levels.

At the **data plane**, each format ships libraries or specifications that engines embed. Apache Spark and Apache Flink integrate with all four formats for both reading and writing; distributed SQL engines such as Trino and Presto ship native connectors. Cloud warehouses — Amazon Athena and Redshift, Google BigQuery, Snowflake — can query open table formats directly, with capabilities varying by format and vendor, and Python-native engines (DuckDB, Daft, Ray) extend the same reach to lightweight workloads.

At the **control plane**, a catalog tracks which tables exist and points engines at each table's current metadata. Options range from the venerable Hive Metastore and AWS Glue Data Catalog to newer catalogs like Apache Polaris, Unity Catalog, and Apache Gravitino. The catalog is what turns `SELECT * FROM orders` into "resolve `orders` to a storage location and metadata pointer, then plan the scan." Hudi syncs its table metadata to these catalogs through catalog sync tools, keeping tables discoverable across engines.

## Interoperability: You Don't Have to Pick One Forever

A common source of anxiety is that choosing a table format is a one-way door. Increasingly, it is not. [Apache XTable](https://xtable.apache.org) (incubating) is an open source project that translates table metadata between Hudi, Iceberg, and Delta Lake — in any direction — without copying or rewriting the underlying data files. Because all three formats ultimately describe Parquet files plus metadata, XTable can read one format's metadata and write out the equivalent for another, as a one-time conversion or continuous incremental sync — so a table written with Hudi's ingestion and indexing capabilities can be exposed as an Iceberg table to an Iceberg-only service, or a Delta table queried by a Hudi-native pipeline.

This is how major vendors ship interoperability today. XTable was co-launched by Microsoft, Google, and Onehouse before its donation to the ASF; [Microsoft OneLake](https://xtable.apache.org/docs/fabric/) in Fabric uses it under the hood to surface tables across Delta and Iceberg formats, and Onehouse uses it to keep customer tables continuously queryable as Hudi, Iceberg, and Delta at once. Databricks takes the same one-copy approach within Delta Lake through UniForm, which generates Iceberg metadata alongside the Delta transaction log so one copy of data registers and reads as either format. Format choice becomes a per-workload decision about write-path and management capabilities, rather than a permanent bet that fragments your data.

## Table Format vs File Format vs Lakehouse

These three terms get conflated constantly, so a short disambiguation is worth the space.

- A **file format** (Apache Parquet, ORC, Avro) defines how records are encoded *inside a single file* — columnar layout, compression, per-file statistics. It knows nothing about other files.
- A **table format** (Hudi, Iceberg, Delta Lake, Paimon) defines how *many files plus metadata* form a versioned, transactional table. It sits directly above the file format.
- A **data lakehouse** is the overall *architecture* that combines cloud object storage, open file formats, a table format, catalogs, and query engines into a warehouse-like platform on the lake. The table format is its load-bearing component, alongside storage engines/table services, catalogs, and compute.

![Data lakehouse architecture](/assets/images/blog/dlh_new.png)
<p align = "center">Figure: Where the table format sits in a lakehouse architecture</p>

So Parquet is not a table format, and a table format alone is not a lakehouse — it is the layer that makes one possible. For the architectural view, see the companion post on [what a data lakehouse is](/blog/2024/07/11/what-is-a-data-lakehouse).

## Conclusion

An open table format — table metadata plus a transaction protocol — turns cheap, scalable object storage into a real analytical database substrate: files become tables, jobs become transactions, and a directory of Parquet becomes something you can update, evolve, audit, and query as of any point in time, from any engine that implements the open specification. Apache Hudi proved the idea in production first while building the transactional data lake; Apache Iceberg, Delta Lake, and Apache Paimon arrived from different starting workloads and remain strongest nearest their design centers. But the format is only the specification — someone still has to run the storage engine that keeps tables fast, and reads and writes pull that engineering in different directions. With Apache XTable translating metadata between formats, the practical takeaway is straightforward: pick the format whose write path and management capabilities fit each workload, keep your data in open formats, and let the specification — not a vendor — define what your tables are.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'What is an open table format?', answer: 'An open table format is an openly governed specification combining table metadata (which files form the table, their schema and statistics, at every version) with a transaction protocol (how writers commit atomically and readers see consistent snapshots). Together they turn files on cloud object storage into a database-like table with ACID transactions, schema evolution, and time travel. Apache Hudi, Apache Iceberg, Delta Lake, and Apache Paimon are the major open table formats.'},
  {question: 'Is Parquet an open table format?', answer: 'No. Apache Parquet is an open file format: it defines how records are encoded inside a single file. A table format like Apache Hudi or Apache Iceberg sits above Parquet and defines how many files plus transactional metadata form a versioned table.'},
  {question: 'What is the difference between a table format and a file format?', answer: 'A file format defines the byte layout of one file, including encoding, compression, and per-file statistics. A table format defines how a collection of such files behaves as a single transactional table, tracking committed files, schema, versions, and statistics across the whole table. They are complementary layers, not alternatives.'},
  {question: 'Which open table format should I use?', answer: 'It depends on your workloads, and note that write and read performance involve different trade-offs. Hudi is strongest for mutable, incremental ingestion at high volume, with built-in indexing for fast writes and automated compaction and clustering to keep reads fast; Iceberg emphasizes an engine-neutral specification with broad catalog support; Delta Lake offers deep Spark and Databricks integration; Paimon targets LSM-based streaming writes with Flink. Apache XTable can translate metadata between Hudi, Iceberg, and Delta, so the choice is not permanent.'},
  {question: 'Do open table formats support ACID transactions?', answer: 'Yes. All major open table formats provide atomic commits and snapshot isolation for readers, so queries always see a consistent version of the table. They differ in how they handle concurrent writers, using techniques such as optimistic concurrency control and non-blocking concurrency mechanisms.'},
  {question: 'Is Apache Hudi just a table format?', answer: 'No. Hudi includes an open table format, but pairs it with a built-in storage engine: a writer runtime with pluggable indexing plus table services such as compaction, clustering, and cleaning that keep tables optimized automatically. Hudi was also the first open table format in production, built at Uber in 2016 for its transactional data lake.'},
]} />
