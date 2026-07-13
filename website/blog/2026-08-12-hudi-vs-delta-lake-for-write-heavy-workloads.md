---
title: "Apache Hudi vs Delta Lake for Write-Heavy Workloads"
excerpt: "A mechanism-level comparison of how Hudi and Delta Lake handle update- and delete-heavy tables: indexing vs file-scan merge planning, MOR logs vs Parquet rewrites, and how their costs scale as write rates climb."
description: "How Apache Hudi and Delta Lake handle write-heavy workloads: indexed upserts and MOR logs vs MERGE joins and file rewrites, compaction, concurrency, streaming."
authors: [sivabalan]
category: deep-dive
image: /assets/images/blog/2024-05-27-apache-hudi-vs-delta-lake-choosing-the-right-tool-for-your-data-lake-on-aws.png
tags:
- comparison
- delta lake
- upsert
- performance
---

For update- and delete-heavy tables, the core architectural difference is this: Apache Hudi locates the records being changed through pluggable [indexes](/docs/indexes) — including a record-level index that maps each key to its file group — and can absorb changes into Merge-on-Read delta logs instead of rewriting columnar files, while Delta Lake locates records by joining the incoming batch against the table's data files (pruned by file statistics and, more recently, softened by deletion vectors) and applies changes by rewriting Parquet files. Both designs work, but their costs grow differently as update rates rise: Hudi's write cost scales primarily with the size of the *change*, while Delta's MERGE cost carries a scan-and-rewrite component that scales with the amount of *table data* the changes touch. That difference is small at low update rates and compounds at high ones — which is exactly what "write-heavy" means.

This post compares the two systems at the mechanism level: how each plans a merge, what each does at write time, who cleans up afterward, and how they behave under concurrency and streaming. It deliberately avoids benchmark numbers — hardware, versions, and configurations change too fast for them to stay honest — and focuses instead on the cost model each architecture implies.

## What "Write-Heavy" Actually Means

"Write-heavy" is not one workload; it is a cluster of characteristics that stress a table format in different ways:

- **High update/delete ratios.** The batch being written is mostly *changes to existing records* rather than new inserts — CDC streams mirroring an OLTP database, order-status updates, inventory corrections, GDPR deletes. Insert-only workloads are easy for every format; it is mutation that separates them.
- **Frequent commits.** Writes land every few minutes (or faster) rather than every few hours, so per-commit overheads — merge planning, file rewriting, metadata churn — are paid dozens or hundreds of times a day.
- **Random-key updates vs recent-partition updates.** If updates cluster in the newest partitions (event tables), any format can prune its way to a small working set. If updates scatter across the whole keyspace (dimension tables, user profiles, unpartitioned tables), pruning breaks down and the mechanism for *finding* records dominates cost.
- **Concurrent writers.** Multiple pipelines — an ingestion job, a backfill, a GDPR delete job — writing the same table at once, where conflict handling decides how much work gets thrown away and retried.

Keep these four dimensions in mind; the two systems diverge on each of them by different amounts.

## The MERGE Path, Compared

**Delta Lake** expresses row-level mutation primarily through `MERGE INTO`. The engine joins the source batch against the target table to identify which data files contain matching rows, using file-level min/max statistics (and data layout from Z-ordering or liquid clustering) to prune files where possible. Matched files are then rewritten: a file containing even one matched row is read, merged, and written out again as a new Parquet file. Deletion vectors (Delta's merge-on-read-style feature) soften this meaningfully for deletes and updates — instead of immediately rewriting a file, the engine can write a compact vector marking rows as removed, deferring the rewrite. But the deferred rewrite still happens: `OPTIMIZE` or a subsequent write eventually materializes new files, and the *find phase* — joining the source against candidate files — still runs against data files on every MERGE. When update keys are random and statistics can't prune much, that join effectively scans the table's key columns per write.

**Hudi** treats [upsert as a first-class write operation](/docs/write_operations), not a SQL statement compiled into a join. Incoming records carry a record key; the write path runs an [index lookup](/docs/indexes) to tag each record as an insert or an update and route it to the file group that already holds its key. What happens next depends on the [table type](/docs/table_types): a Copy-on-Write (COW) table rewrites the affected base files, much like Delta; a Merge-on-Read (MOR) table appends the changed records to delta log files attached to each file group and defers the columnar rewrite to compaction. Hudi also supports SQL `MERGE INTO`, but it sits on top of the same indexed machinery — the primitive underneath is the keyed upsert (explained in depth in [What is Upsert on a Data Lake?](/blog/2026/07/15/what-is-upsert-on-a-data-lake)).

The structural difference: Delta's merge planning cost lives in a join against data files; Hudi's lives in an index lookup against metadata. As table size grows and update keys scatter, the first grows with the table; the second grows with the change batch (for non-global indexes) or stays a sharded point-lookup (for the record-level index).

## Indexing: The Core Difference

Hudi maintains a persistent mapping from record keys to file groups and lets you [pick the index](/docs/indexes) that matches your write pattern:

- **Bloom index**: bloom filters plus key-range pruning, stored in file footers or centrally in the metadata table. Excellent when keys have ordering (e.g., timestamp-prefixed event keys), so most files are pruned before any data is read.
- **Record-level index (RLI)**: an exact key-to-file-group mapping in Hudi's metadata table, hash-sharded for scale. It turns "which file holds this key?" into a point lookup, which matters most on large tables with random-key updates — precisely where probabilistic pruning fails.
- **Bucket index**: hashes keys directly to file groups, eliminating the lookup step entirely at the cost of a fixed (or consistent-hashed) bucket layout. This is the workhorse for very high-throughput streaming upserts, especially with Flink.
- **Simple/global variants**: lean joins against existing keys, with global versions that enforce uniqueness across partitions.

Delta Lake's counterpart — and it is a real counterpart, not an absence — is **data skipping**: per-file min/max statistics collected in the transaction log, made more effective by clustering the data so related keys co-locate (Z-ordering historically, liquid clustering more recently). When keys correlate with layout, statistics prune candidate files well and MERGE touches little. The honest framing is that Delta prunes *files by value ranges*, while Hudi locates *records by key*. The former is free to maintain but degrades toward a full scan when updates are random relative to layout; the latter costs extra storage and index maintenance on every write, but keeps the locate cost bounded regardless of where updates land. For write-heavy tables with scattered keys, that bound is the difference that shows up in write latency.

## Write Amplification and File Management

Once records are located, the second question is how many bytes hit storage per byte of change.

On **Delta**, a MERGE that touches a file rewrites the file (deletion vectors defer this, as noted). Write amplification is therefore proportional to the number of *files* touched, and random small updates against large files are the worst case. Compaction is handled by `OPTIMIZE` (with Z-order/clustering options) and auto-compaction/optimized-writes features that coalesce small files; on Databricks much of this is managed for you (predictive optimization), while open-source deployments schedule `OPTIMIZE` themselves.

On **Hudi**, a COW table has the same rewrite profile — with one mitigation: Hudi's automatic file sizing packs inserts into under-sized file groups on every write, keeping file counts and rewrite units under control without a separate job. A MOR table changes the equation more fundamentally: updates are appended to logs, so write amplification per commit is proportional to *records changed*, not files touched; the rewrite cost is batched and amortized by [compaction](/docs/compaction), which runs asynchronously without blocking ingestion — scheduled and executed by Hudi's built-in table services, either inline, async in the same writer, or as a separate job. The COW/MOR trade-off is covered in detail in [Understanding COW and MOR in Apache Hudi](/blog/2024/11/12/understanding-cow-and-mor-in-apache-hudi).

The operational difference is who runs the cleanup and whether writers wait for it. Hudi ships cleaning, compaction, clustering, and file sizing as self-managing services wired into the write path; Delta provides the equivalents as commands and (on Databricks) managed features. For a write-heavy table, the key property is that MOR moves the expensive columnar rewrite *off* the ingestion path entirely — a batch of deletes trickling in all day can be absorbed cheaply in logs and reconciled by one compaction.

## Concurrency Under Heavy Writes

Both systems use optimistic concurrency control (OCC) between writers, and both resolve conflicts at commit time — but the failure modes under heavy write traffic differ.

**Delta** detects conflicts by transaction-log versions: concurrent commits that logically overlap (e.g., two MERGEs that may have read files the other rewrote) surface as `ConcurrentAppend`/`ConcurrentDeleteRead`-class exceptions, and the losing transaction retries. Partitioning and careful predicate scoping reduce collisions, and this works well when writers touch disjoint partitions.

**Hudi** offers file-level OCC with the same optimistic semantics (two writers touching disjoint file groups both succeed), plus early conflict detection that can abort a doomed writer mid-write rather than at commit, saving the wasted compute. More distinctively, Hudi's [concurrency model](/docs/concurrency_control) separates *writers* from *table services*: compaction and clustering run under MVCC without competing in the OCC conflict path, so a heavy ingestion writer is not fighting its own maintenance jobs. And for genuinely concurrent mutation streams, Hudi 1.0 added **non-blocking concurrency control (NBCC)**: multiple writers can commit into the *same file group* without serializing, with conflicts resolved deterministically at read/compaction time by commit-completion ordering. For multi-writer CDC topologies, that removes the retry storms OCC produces when writers hot-spot on the same data — a scenario Delta currently handles only by retrying one of the writers.

## Streaming Writes

Both formats ingest from Spark Structured Streaming, and both can sustain minute-level micro-batches. The differences are in what surrounds that path. Hudi grew up as a streaming ingestion system (built at Uber to keep lake tables in sync with upstream databases), and it shows in the toolchain: Hudi Streamer is a ready-made continuous ingestion service with source connectors, schema-registry integration, checkpointing, and async table services in one process; MOR tables give streaming writes a log-structured landing zone; incremental queries let downstream jobs consume exactly what changed. Hudi's **Flink** integration is a first-class writer with its own state-backed and bucket indexes for streaming upserts — relevant because high-volume CDC pipelines are often Flink-based. Delta's streaming story is strongest inside Spark (Structured Streaming plus Delta Live Tables on Databricks); its Flink connector supports appends but not keyed streaming upserts, so update-heavy streaming into Delta generally routes through Spark MERGE micro-batches.

For a dated but methodologically transparent data point on batch write/query performance, see the 2022 [TPC-DS benchmark comparison](/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks) — treat it strictly as a historical reference, since both systems have shipped major releases since then.

## A Decision Framework

Delta Lake is a workable choice when:

- You are **Databricks-centric** — the managed platform runs the compaction/clustering machinery for you, and the ecosystem integration is tightest there.
- **Update rates are moderate** and mostly cluster in recent partitions, so statistics-based pruning keeps MERGE cheap.
- Your mutation pattern is **periodic batch MERGE** (hourly/daily), where per-commit overhead is paid rarely.

Hudi pulls ahead as workloads get write-heavier:

- **Random-key upserts at scale** — dimension tables, user profiles, unpartitioned tables — where the record-level or bucket index bounds locate cost and MOR bounds rewrite cost.
- **CDC replication** of operational databases, where upsert-as-a-primitive, ordering-field-based merging of out-of-order events, and incremental pulls map directly onto the problem.
- **Sub-hour freshness targets** with frequent commits, where async compaction keeps the ingestion path light.
- **Open/non-Databricks deployments** — Spark, Flink, EMR, Dataproc, on-prem — where Hudi's self-managing table services replace what a managed platform would otherwise do for you.
- **Multi-writer mutation**, where NBCC avoids OCC retry churn.

## It's Not Either/Or: Interoperability via Apache XTable

Choosing Hudi as the write layer does not cut you off from Delta-reading tools. [Apache XTable](https://xtable.apache.org/) (incubating) translates table metadata between Hudi, Delta Lake, and Iceberg over the *same* Parquet data files — no data copying, just an additional metadata representation. You can write with Hudi's indexed, MOR-backed ingestion path and expose the table as Delta for Databricks SQL, or as Iceberg for engines that prefer it, keeping a one-copy story much like Delta's own UniForm feature — but starting from the format with the strongest write machinery. We cover the mechanics — sync modes, catalog wiring, and what does and doesn't translate — in [Using Hudi with Apache Iceberg via XTable](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable); the same setup applies to Delta targets. For write-heavy tables, this is often the best of both: Hudi mechanics on the write path, format-agnostic access on the read path.

## Conclusion

Delta Lake and Apache Hudi can both run update-heavy tables; the question is how their costs scale as "update-heavy" gets heavier. Delta plans merges by joining against data files and applies them by rewriting Parquet — a model that stays cheap while pruning works and update rates are moderate, and that Databricks increasingly automates. Hudi plans merges through indexes and can apply them as log appends — a model whose write cost tracks the size of the change rather than the size of the table, backed by self-managing compaction, file sizing, and streaming-native tooling. If your table sees random-key upserts, continuous CDC, tight freshness SLAs, or concurrent mutation, those mechanisms are the difference you will feel in write latency and compute bills. And with XTable, adopting Hudi's write path no longer means giving up Delta-compatible reads — so pick the write mechanics your workload needs, and interoperate for the rest. Writes are only half the story, of course: a companion analysis of the read side — how Hudi's indexes speed up point queries and selective lookups on high-cardinality columns from Spark SQL and Trino — is coming in a follow-up post.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'Is Hudi faster than Delta Lake for updates?',
    answer: 'It depends on the update pattern, so be wary of any blanket claim. For updates that scatter randomly across a large table, Hudi\'s record-level and bucket indexes locate affected file groups without joining against data files, and Merge-on-Read absorbs changes as log appends instead of Parquet rewrites, which typically means lower write latency and less compute per commit. For moderate update rates clustered in recent partitions, Delta\'s statistics-based pruning keeps MERGE efficient and the two are much closer. Test with your own keys, update distribution, and commit frequency.',
  },
  {
    question: 'Does Delta Lake have an index?',
    answer: 'Delta Lake does not maintain a record-level index that maps keys to files. It relies on per-file min/max statistics in the transaction log for data skipping, made more effective by clustering the data with Z-ordering or liquid clustering, and deletion vectors reduce immediate file rewrites for deletes and updates. This works well when update keys correlate with data layout, but degrades toward scanning key columns across the table when updates are random. Hudi instead maintains pluggable indexes, including an exact record-level index, as part of its metadata table.',
  },
  {
    question: 'Can I use Apache Hudi outside Databricks?',
    answer: 'Yes, and that is one of its main draws. Hudi is a community-run Apache project that works with open-source Spark, Flink, Presto, Trino, and cloud services like Amazon EMR, Google Dataproc, and AWS Glue, with no managed platform required. Its table services, such as compaction, clustering, cleaning, and file sizing, are built into the writers and run automatically, covering the operational work a managed platform would otherwise do. Delta Lake is also open source, but several of its automated optimization features are Databricks-specific.',
  },
  {
    question: 'What is the difference between Hudi Merge-on-Read and Delta deletion vectors?',
    answer: 'Both defer expensive file rewrites, but at different granularity. Deletion vectors mark individual rows in a Parquet file as removed, so a delete or update avoids an immediate rewrite, while the new version of an updated row is still written to a data file and the marked file is eventually rewritten by OPTIMIZE. Hudi\'s Merge-on-Read appends the full changed records to delta log files attached to each file group, so arbitrary updates, not just row removals, are absorbed cheaply, and asynchronous compaction later folds logs into new base files off the ingestion path.',
  },
  {
    question: 'Can Delta Lake tools read a Hudi table?',
    answer: 'Yes, through Apache XTable, an incubating project that translates table metadata between Hudi, Delta Lake, and Iceberg without copying data files. You write with Hudi and run XTable sync to produce Delta transaction log metadata over the same Parquet files, which Delta-compatible engines can then read like any Delta table. This gives a one-copy, multi-format story similar to Delta\'s UniForm feature, while keeping Hudi\'s indexing and Merge-on-Read machinery on the write path.',
  },
]} />
