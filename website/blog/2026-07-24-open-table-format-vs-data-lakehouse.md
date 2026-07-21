---
title: "Open Table Format vs Data Lakehouse: What's the Difference?"
excerpt: "Untangles two constantly conflated terms: the open table format is one layer of the stack; the data lakehouse is the whole architecture built around it."
description: "An open table format is a component; a data lakehouse is an architecture. This post walks the lakehouse stack layer by layer and corrects common conflations."
authors: [vinoth-chandar]
category: how-to
image: /assets/images/blog/hudistack/hstck_new.png
tags:
- table format
- data lakehouse
- open architecture
- architecture
---

An open table format is a component; a data lakehouse is an architecture. The table format is the metadata layer that turns files on object storage into transactional tables — the lakehouse is the complete system built around it: storage, table format, catalog, table services and query engines. The two terms get used interchangeably in vendor marketing and conference talks, and the conflation causes real confusion: teams "adopt a lakehouse" by picking a table format and then discover that most of the operational work still lies ahead, or dismiss an entire architecture because of an opinion about one project's metadata layout.

This post untangles the two terms. We will walk the lakehouse stack from the bottom up, pin down exactly what a table format contributes at its layer, spell out what a table format alone does *not* give you, and correct the most common conflations along the way.

## The Lakehouse Stack, Layer by Layer

A [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse) is best understood as a stack of open, modular layers. Each layer has one job, and the table format is exactly one of them. From the bottom up:

**Cloud object storage.** Amazon S3, Azure Blob Storage, Google Cloud Storage. This layer stores bytes durably and cheaply at effectively unlimited scale, and decouples storage cost from compute cost. It knows nothing about tables — it serves GETs, PUTs and LISTs.

**Open file formats.** [Apache Parquet](https://parquet.apache.org) and [Apache ORC](https://orc.apache.org) define how records are encoded *inside a single file*: columnar layout, compression, encodings, and per-file statistics. A file format makes one file efficient to scan; it has no concept of the other files around it.

**The open table format.** [Apache Hudi](https://hudi.apache.org), [Apache Iceberg](https://iceberg.apache.org), and [Delta Lake](https://delta.io) live here. A table format is versioned metadata over many files: an ordered log of commits (in Hudi, the [timeline](/docs/timeline)), a record of which files constitute the table at each version, the schema and its evolution history, and file-level statistics for pruning. This is the layer that makes a pile of Parquet behave like a database table — atomic commits, snapshot isolation, record-level updates and deletes, time travel. For a full treatment of this one layer, see the companion post on [what an open table format is](/blog/2026/07/14/what-is-an-open-table-format).

**Storage engine and table services.** Metadata alone does not keep a table healthy. Something has to compact log files into base files, cluster data for query locality, size small files, clean up old versions, and build and maintain indexes. In some stacks this is the query engine's job or a vendor service; in Hudi it is a built-in layer of table services.

**Catalogs and metadata services.** A catalog (Hive Metastore, AWS Glue, Apache Polaris, Unity Catalog) tracks which tables exist, their schemas, and where each table's metadata lives — plus, increasingly, access control and lineage. It is what lets an engine resolve `orders` to something it can plan a scan against.

**Query and compute engines.** Apache Spark, Apache Flink, Trino, Presto, plus warehouses and Python-native engines. Engines do the actual reading, writing and computing, speaking to tables through the table format's specification and finding them through the catalog.

![Data lakehouse architecture](/assets/images/blog/dlh_new.png)
<p align = "center">Figure: The layered data lakehouse architecture</p>

Assemble all of these — storage, file format, table format, table services, catalog, engines — and *that* is a data lakehouse. The table format is the load-bearing middle of the stack, arguably the layer that made the architecture possible at all. But it is one layer, the way a storage engine is one layer of a database, not the database.

## What a Table Format Alone Does Not Give You

Here is where the conflation bites in practice. A table format specification tells you what a valid table looks like on storage. It does not, by itself, operate anything. Adopt a bare table format and you still own:

- **Table maintenance.** Someone has to run compaction, clustering, cleaning of old file versions, and file sizing — continuously, for every table, coordinated with in-flight writers. Skip it and small files and stale metadata quietly erode query performance and inflate storage bills.
- **Ingestion.** The format defines how commits land; it does not build the pipelines that pull from Kafka, CDC streams, or upstream databases, handle schema drift, deduplicate, and checkpoint.
- **Catalog sync and discovery.** Tables must be registered and kept current in one or more catalogs so every engine can find them.
- **Access control and governance.** The format stores data; deciding who can read which columns of it is a platform concern layered above.
- **Monitoring and operations.** Commit failures, clustering backlogs, concurrency conflicts — someone has to watch them.

This is the "platform gap" between a format and a lakehouse, and it is exactly where projects differ in scope. Apache Iceberg deliberately scopes itself to the specification and libraries, leaving maintenance to engines and vendors. Hudi deliberately scopes itself wider: built-in table services (compaction, clustering, cleaning, indexing) that run inline or asynchronously without external orchestration, ingestion utilities like [Hudi Streamer](/docs/hoodie_streaming_ingestion) for pulling from Kafka and CDC sources, catalog sync tools, and a [multi-modal indexing subsystem](/docs/indexes) that accelerates both writes and reads. Neither scoping is wrong — but they are different amounts of the lakehouse, and comparing them as if both were "just formats" misprices the operational work each leaves on your plate.

![Apache Hudi stack](/assets/images/blog/hudistack/hstck_new.png)
<p align = "center">Figure: The Apache Hudi stack — a table format plus table services and platform components</p>

## Common Conflations, Corrected

A few statements you will hear regularly, each slightly wrong in an instructive way:

**"Iceberg is a lakehouse."** No — Apache Iceberg is a table format a lakehouse can use. An Iceberg table sitting in S3 with no catalog, no maintenance jobs and no engines wired up is not a lakehouse; it is well-organized metadata. The same is true of Hudi and Delta Lake at the format layer: the lakehouse is what you get after you assemble the rest of the stack around the tables.

**"Databricks' lakehouse is the lakehouse."** Databricks popularized the term and built an excellent platform around Delta Lake, but the lakehouse is an open architectural pattern, not one vendor's product. Uber was running Hudi in what we would now call a lakehouse architecture in 2016–2017, and today you can assemble a lakehouse from entirely open components — or buy one from any of several vendors — on any of the three major formats.

**"Choosing a table format means choosing a vendor."** The formats are openly governed (Hudi and Iceberg at the ASF, Delta Lake at the Linux Foundation), and every major engine reads more than one of them. Vendors have preferred formats, but the specification belongs to the community — that openness is the entire point of the "open" in open table format.

**"Parquet is our table format."** A file format is not a table format. Parquet defines the bytes inside one file; a table format defines how thousands of Parquet files plus a transaction log behave as a single versioned table. They are adjacent layers of the stack, not alternatives.

For where the lakehouse itself sits relative to the architectures it evolved from, see the companion comparison of [lakehouse vs data warehouse vs data lake](/blog/2026/07/23/lakehouse-vs-data-warehouse-vs-data-lake).

## The Layers Interoperate — You Are Not Locked In

Because the layers are modular and the specifications open, the mapping between them is many-to-many. Spark, Flink and Trino each read and write all three major formats. A single lakehouse can, and often does, contain tables in more than one format, discovered through the same catalog and queried by the same engines.

Interoperability now extends to the format layer itself. [Apache XTable](https://xtable.apache.org) (incubating) translates table metadata between Hudi, Iceberg and Delta Lake — in any direction — without copying or rewriting data files, either as a one-time conversion or as continuous incremental sync. A table written with Hudi's ingestion and indexing capabilities can be exposed as an Iceberg table to a service that only reads Iceberg. The practical consequence is that the table format is a per-workload choice inside your lakehouse, not a one-way architectural door: the architecture outlives any single format decision.

## Practical Guidance: Architecture First, Then Format

The right order of decisions falls straight out of the layering. First design the architecture: what workloads (BI, ML, streaming, ad-hoc SQL) must the system serve? Which engines? What ingestion latency? Who operates table maintenance — your team, built-in services, or a vendor? What are the governance requirements? That exercise defines the lakehouse you need.

Only then choose the table format — as a component decision driven by workload fit. Mutable, incremental, streaming-heavy ingestion favors Hudi's write path, indexing and [table types](/docs/table_types) (Copy-on-Write vs Merge-on-Read let you tune the write/read trade-off per table). Engine-neutral batch analytics with broad catalog support is Iceberg's design center; deep Spark and Databricks integration is Delta's. If the operational gap matters — no team to babysit compaction jobs — weight built-in table services heavily. And with XTable, a wrong-for-one-workload choice is a translation, not a migration.

Teams that pick a format first and call it a lakehouse strategy usually end up rediscovering the rest of the stack the hard way, one missing layer at a time.

## Conclusion

Keep the two terms at their own altitudes and the confusion disappears. The open table format is a layer: versioned, transactional metadata that turns files on object storage into tables. The data lakehouse is the architecture: that layer plus storage, file formats, table services, catalogs and engines, assembled into a system that does warehouse-grade work on open data. A table format cannot be a lakehouse any more than a storage engine can be a database — but no lakehouse stands without one. Evaluate architectures as architectures, formats as components, and remember that the projects in this space scope themselves differently: some stop at the specification, while Apache Hudi ships much of the surrounding platform — table services, ingestion, indexing — out of the box. Get the architecture right, pick formats per workload, and let open specifications keep every layer replaceable.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'What is the difference between an open table format and a data lakehouse?', answer: 'An open table format is a single component: the metadata layer that turns files on object storage into transactional tables with ACID commits, schema evolution and time travel. A data lakehouse is the complete architecture built around it, combining object storage, open file formats, the table format, table services, catalogs and query engines into one system.'},
  {question: 'Is Apache Iceberg a data lakehouse?', answer: 'No. Apache Iceberg is an open table format — one layer of a lakehouse architecture. A lakehouse using Iceberg still needs object storage, a catalog, query engines, and services to maintain the tables. Iceberg is a table format a lakehouse can use, not a lakehouse itself.'},
  {question: 'Is Delta Lake a table format or a lakehouse?', answer: 'Delta Lake is an open table format governed by the Linux Foundation. Databricks builds its lakehouse platform on top of Delta Lake, which is why the two are often conflated, but Delta Lake itself is the table layer, and the lakehouse is the surrounding platform of storage, catalogs, engines and services.'},
  {question: 'Do I need a table format to build a lakehouse?', answer: 'Yes, in practice. The table format is what provides atomic commits, snapshot isolation, updates and deletes, and versioning over files in object storage. Without one you have a data lake of raw files, not a lakehouse. Apache Hudi, Apache Iceberg and Delta Lake are the major open options.'},
  {question: 'Is Apache Hudi a table format or a lakehouse platform?', answer: 'Both, by design. Hudi includes an open table format built around its timeline and file layout, but the project also ships built-in table services like compaction, clustering and cleaning, ingestion utilities such as Hudi Streamer, and a multi-modal indexing subsystem. That puts Hudi closer to a lakehouse platform than a bare format specification.'},
  {question: 'Does choosing a table format lock me into one vendor?', answer: 'No. The major table formats are openly governed, and engines like Apache Spark, Apache Flink and Trino support all of them. Apache XTable can also translate table metadata between Hudi, Iceberg and Delta Lake without rewriting data files, so the format choice is a per-workload decision rather than a permanent commitment.'},
]} />
