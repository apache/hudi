---
title: "Apache Hudi vs Apache Iceberg for CDC Workloads"
excerpt: "A mechanism-level comparison of how Apache Hudi and Apache Iceberg handle change data capture: record lookup, deletes, small files, ordering and change streams."
description: "Hudi vs Iceberg for CDC: how each handles record lookups, deletes, small files, ordering and change streams — and a framework for choosing between them."
authors: [sivabalan]
category: deep-dive
image: /assets/images/blog/2024-12-03-apache-iceberg-vs-apache-hudi.jpeg
tags:
- comparison
- apache iceberg
- cdc
- data lakehouse
---

Both Apache Hudi and Apache Iceberg can ingest change data capture (CDC) streams, but they were designed around different problems. Hudi was built for mutation-heavy ingestion: a record-level index that locates any key without scanning the table, merge-on-read delta logs that absorb frequent updates and deletes cheaply, and built-in deduplication, ordering and file sizing. Iceberg was designed first for large, reliable analytical scans; its copy-on-write and delete-file-based merge-on-read paths make row-level mutations possible, but high-frequency CDC merges are more expensive to plan, execute and maintain. This post walks through the mechanisms behind that difference — record lookup, delete handling, commit frequency, ordering and change streams — so you can judge the trade-offs against your own workload rather than take anyone's word for it.

<!--truncate-->

## What CDC demands from a table format

A CDC stream is, by construction, mostly mutations. If you replicate an operational database to the lake (the basics are covered in [What is CDC on a Data Lake?](/blog/2026/07/22/what-is-cdc-on-a-data-lake) and the deeper [Understanding Data Lake Change Data Capture](/blog/2024/07/30/data-lake-cdc)), the table format underneath has to sustain a specific set of pressures:

- **Fast key-based upserts and deletes.** Every change event targets one row identified by a primary key. The write path must find where that key lives and apply the change without rewriting or rescanning unrelated data.
- **Ordering and late data.** Events arrive out of order after retries and repartitioning. The format must merge by the source's log position or event time, not arrival time, or the table silently regresses.
- **Frequent small commits.** Minute-level freshness means committing every few minutes, around the clock. That stresses metadata, file counts and any background maintenance.
- **Change streams for downstream consumers.** Bronze tables that absorb CDC should also *emit* changes, so silver and gold tables can be built incrementally instead of re-reading everything.

Hold each format against these four demands and the architectural differences become concrete.

## Finding the record: indexes vs. joins

The first job in applying an upsert is answering "which file contains this key?"

**Hudi** treats indexing as a core component of the write path. Every record key maps to exactly one file group, and Hudi maintains [pluggable indexes](/docs/indexes) — bloom-filter, simple-join, bucket-hash and, since 0.14.0, a record-level index stored inside the table's metadata table — that resolve incoming keys to their file groups directly. With the record-level index, lookup cost is proportional to the number of records changed, not the size of the table: the writer consults hash-sharded key-to-location mappings, tags each incoming record with its target file group, and only touches those file groups. For merge-on-read tables this also bounds read cost, because a base file only ever needs to merge against log records belonging to that same file group. The same [multi-modal indexing subsystem](/docs/indexes#multi-modal-indexing) extends beyond keys — secondary indexes and expression indexes on non-key columns, borrowed straight from relational database design — which is one of the deeper architectural choices catalogued in [21 unique differentiators of Apache Hudi](/blog/2025/03/05/hudi-21-unique-differentiators).

**Iceberg** deliberately has no record-level index; its metadata tree (manifests, partition values, column min/max stats) is built for pruning scans, not locating keys. A `MERGE INTO` in Spark plans a join between the incoming batch and the target table to discover which files hold matching rows — column statistics can prune files, but for keys spread across a table the merge effectively scans and joins against much of it. Streaming writers such as Flink's Iceberg sink in upsert mode sidestep the lookup entirely by writing *equality delete files* — "delete any row where id = 5" — which is cheap at write time precisely because nobody found the row. The cost doesn't disappear; it moves to every subsequent read and to compaction, which is the next section's subject.

This difference is measurable, and you can reproduce it. In benchmarks run with the open-source [LakeLoader framework](https://github.com/onehouseinc/lake-loader) (Spark 3.5, Hudi 1.1.1, Iceberg 1.10.0), we observed Hudi delivering roughly 4× lower incremental write latency than Iceberg on a 10 TB partitioned table under skewed CDC-style updates — the record-level index shuffled around 250 MB per commit where the merge join shuffled hundreds of gigabytes — and roughly 8× lower steady-state latency on merge-on-read tables with sparse column-level updates. Run the framework against your own workload shape before trusting anyone's numbers, including these.

## How deletes actually work

Deletes are where CDC pipelines live or die, since every update in a changelog is logically a delete plus an insert of the new version.

**Hudi** logs a delete into the same file group where the key lives, exactly like an update. On a merge-on-read table this is an append to that file group's delta log; queries merge base file plus log at read time within the file group, and compaction later rewrites the base file with the deletes applied. Because deletes are colocated with the data they affect, the merge scope is always one base file against its own changes — never a table-wide reconciliation. Data locality is preserved by construction.

**Iceberg** (format v2) represents row-level deletes as separate delete files of two kinds. *Position deletes* name a row by data file path and row ordinal — precise and cheap to apply on read, but the writer must first find the row, which without an index means the join described above. *Equality deletes* record column predicates and are cheap to write, but every reader must check each equality delete file against every data file with an older sequence number in its scan. When many data files and many delete files accumulate — the normal state of a high-frequency CDC stream — this approaches an N data files × M delete files reconciliation problem: query planning has to associate delete files with data files, and compaction jobs that merge them can balloon into runaway executions as both N and M grow between maintenance runs.

The Iceberg community has been working on this. Format v3 replaces position delete files with binary *deletion vectors* — at most one compact bitmap per data file — which removes much of the position-delete proliferation and read-time association cost, and engines are adopting it. But deletion vectors help the position-delete path; a writer still needs to determine which positions to delete, so the no-index lookup cost remains, and equality deletes still defer their cost to readers and compaction. The structural contrast stands: Iceberg records that a delete happened and reconciles later; Hudi routes the delete to where the record lives at write time.

## Commit frequency and small files

CDC means committing every few minutes, forever. Two things degrade under that regime: file sizes and metadata.

**Hudi** performs automatic file sizing during writes — small inserts are bin-packed into existing under-sized file groups, so frequent commits do not proliferate small files in the first place. Its cleaner bounds how many old file versions are retained, and timeline archival keeps the active timeline small, all running as managed table services alongside ingestion.

**Iceberg** produces at least one new data file (and, in merge-on-read, delete files) plus a new snapshot per commit. Nothing in the write path resizes files, so a minute-level stream steadily accumulates small data files, delete files and snapshot metadata. The remedies exist — `rewrite_data_files` for compaction, `expire_snapshots`, `rewrite_manifests`, `rewrite_position_delete_files` — but they are maintenance procedures you schedule and size yourself, and under optimistic concurrency a long compaction can conflict with the streaming writer touching the same files and be forced to retry. The faster you commit, the faster maintenance debt accrues and the tighter that scheduling loop becomes.

## Ordering, dedup and partial updates

Changelogs carry semantics beyond "latest write wins."

**Hudi** requires a record key and supports an ordering (precombine) field as first-class table concepts. Multiple events for the same key within a batch are deduplicated by ordering value before write; a late-arriving event with an older ordering value than what's stored is dropped rather than applied, so out-of-order delivery cannot regress the table. Record merger and payload APIs go further: partial updates merge only the non-null incoming fields into the existing row, and custom merge logic (say, database-specific changelog semantics) can be plugged in. These behaviors were built for exactly the shape of a Debezium stream.

**Iceberg** does not define keys, dedup or ordered merging at the format level — it stores what engines write. Spark's `MERGE INTO` will fail if multiple source rows match one target row, so deduplication and event-time ordering must be implemented upstream in the pipeline; Flink's upsert mode assumes the stream itself is correctly ordered per key within its checkpoint discipline. That is a deliberate design choice — the format stays engine-neutral — but it means every CDC pipeline re-implements reconciliation logic the format doesn't provide.

## Producing change streams downstream

A CDC-fed bronze table is usually the head of a pipeline, so how each format *emits* changes matters as much as how it absorbs them.

**Hudi** keeps commit-time metadata on every record and supports [incremental queries](/docs/table_types) as a native query type: give a begin instant, get exactly the records that changed since, at any commit granularity. Since 0.13.0 a CDC query mode also returns database-style before/after images with the operation type, logged at write time. Downstream jobs chain these into incremental ETL, each layer consuming only deltas.

**Iceberg** supports incremental scans between snapshots, which cover append-only snapshots; consuming row-level changes from snapshots that carry updates and deletes is harder, and Spark's `create_changelog_view` procedure computes a changelog view by diffing snapshots — with pre/post images available as an optional, more expensive computation — rather than reading changes that were logged at write time. For append-mostly tables the two are comparable; for update-heavy tables, deriving the change stream costs more than reading one that was recorded as it happened.

## Operational reality

What do you actually run in production?

A Hudi ingestion job carries its table services with it: compaction, clustering, cleaning, file sizing and archival are scheduled and executed by the platform, inline or async, with [MVCC coordinating writers and table services](/docs/concurrency_control) so compaction does not block ingestion — plus non-blocking concurrency control (since 1.0) for multiple writers, and early conflict detection that aborts a doomed write mid-flight instead of at commit, saving the wasted compute. The ingestion job itself is often off-the-shelf: [Hudi Streamer](/docs/hoodie_streaming_ingestion) natively understands Debezium, AWS DMS and Mongo changelog formats and reads from Kafka, Pulsar, S3 and GCS, so a CDC pipeline can be a single command rather than custom Spark code. The knobs are there to tune, but the default posture is self-managing.

An Iceberg deployment composes the equivalent from parts: an orchestrator (or a managed catalog service) running compaction, snapshot expiry, delete-file rewrites and manifest rewrites at the right cadence for your commit rate, with conflict-retry behavior tuned so maintenance and streaming writers coexist. None of this is exotic, and managed platforms increasingly automate it — but on a high-frequency CDC table the maintenance cadence is unforgiving, and it is your pager when it falls behind.

## A decision framework

Honest summary rather than a scorecard:

**Iceberg is a workable choice when** your CDC is low-frequency (hourly or slower batches), tables are append-mostly with modest update ratios, you already operate an Iceberg estate with maintenance automation in place, or organizational standardization on one format outweighs per-workload optimization. The v3 deletion-vector work also means the read-side delete overhead is shrinking release by release.

**Hudi pulls ahead when** update/delete ratios are high (mirroring OLTP tables, not appending events), you need sub-hour freshness with commits every few minutes, keys are spread across the table so per-write lookup cost dominates, or downstream consumers need true record-level change streams. These are exactly the pressures Hudi's index, delta logs, precombine semantics and managed services were designed against.

## It's Not Either/Or: Interoperability via Apache XTable

The framing of this post — pick one format — understates a practical third option. The formats' metadata layers describe data files; the data files themselves are Parquet either way. [Apache XTable](https://xtable.apache.org/) (incubating) exploits this by translating table metadata between Hudi, Iceberg and Delta Lake without copying data, so a single physical table can be read through more than one format's metadata.

For CDC specifically, that enables a concrete pattern: ingest with Hudi to get the record-level index, log-based deletes, auto file sizing and incremental queries on the write-heavy bronze layer, then run XTable to expose the same data as Iceberg metadata for engines and catalogs that only speak Iceberg — Snowflake, BigQuery, or an existing Iceberg-standardized analytics stack. The translation is incremental (it processes only new commits) and involves no data rewrite, since both formats point at the same Parquet files.

This decouples the write-path decision from the read-path decision. Choose the ingestion machinery on the merits of your CDC workload, and serve readers in whatever format they require. We cover the setup end-to-end in [Using Hudi with Apache Iceberg via XTable](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable).

## Conclusion

Hudi and Iceberg will both land a CDC stream. The difference is where the cost of mutation is paid. Hudi pays it at write time with machinery built for the purpose — an index that finds keys in O(changes), delta logs that keep deletes local to their file group, ordering and dedup in the format, file sizing and compaction as built-in services. Iceberg defers it — to read-time delete reconciliation, to maintenance jobs, to pipeline-level dedup logic — a trade that suits scan-heavy, append-mostly tables, and one that v3 continues to soften, but that compounds against you as update frequency rises. Map your workload's update ratio, freshness target and downstream consumption pattern onto those mechanisms, and the choice — or the XTable-enabled combination — usually makes itself.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Is Apache Iceberg good for CDC?', answer: 'Iceberg can handle CDC, and it works well when changes arrive in low-frequency batches or tables are append-mostly. For high-frequency, update-heavy streams, the lack of a record-level index makes merges join against the table, delete files accumulate between compactions, and you must schedule maintenance jobs aggressively. Iceberg v3\'s deletion vectors improve the read-side cost, but the write-side lookup and maintenance burden remain.'},
  {question: 'Why is Hudi faster for upserts than Iceberg?', answer: 'Hudi maintains indexes, including a record-level index, that map each record key directly to the file group holding it, so an upsert touches only the files that actually contain changed keys and lookup cost scales with the size of the change batch. Iceberg has no record index, so finding the rows to update requires joining the incoming batch against the target table. On merge-on-read tables Hudi also appends changes to delta logs instead of rewriting base files, deferring the rewrite to background compaction.'},
  {question: 'How do deletes differ between Hudi and Iceberg?', answer: 'Hudi logs a delete into the same file group where the record lives, and compaction later applies it while rewriting that one base file, so merge scope stays local. Iceberg v2 writes separate position or equality delete files that readers and compaction jobs must reconcile against data files, a cost that grows as both accumulate. Iceberg v3 replaces position delete files with per-file deletion vectors, which meaningfully reduces the read-side overhead.'},
  {question: 'Can I use Hudi for CDC ingestion and still serve Iceberg readers?', answer: 'Yes. Apache XTable translates Hudi table metadata into Iceberg metadata incrementally, without copying or rewriting the underlying Parquet data files. You get Hudi\'s indexed upserts, delete handling and file sizing on the write path, while Iceberg-only engines and catalogs read the same table through Iceberg metadata.'},
  {question: 'Does Iceberg support incremental reads like Hudi?', answer: 'Partially. Iceberg supports incremental scans over append snapshots, and Spark can build a changelog view by diffing snapshots, with before/after images as an extra computation. Hudi records commit metadata on every record and logs change data at write time, so incremental and CDC queries read changes directly instead of deriving them, which matters most on update-heavy tables.'},
]} />
