---
title: "Migrating from Delta Lake to Apache Hudi"
excerpt: "A practical guide to moving Delta Lake tables to Apache Hudi — metadata-only conversion with Apache XTable, a one-time Spark rewrite, validation, and rollback."
description: "How to migrate Delta Lake tables to Apache Hudi: XTable metadata translation with no data rewrite, or a one-time Spark bulk insert — plus validation and rollback."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/2023-08-09-Lakehouse-Trifecta-Delta-Lake-Apache-Iceberg-and-Apache-Hudi.png
tags:
- migration
- apache xtable
- delta lake
- guide
---

You can migrate a Delta Lake table to Apache Hudi either by translating its metadata with [Apache XTable](https://xtable.apache.org) — no data rewrite required, since both formats store data as Apache Parquet — or by rewriting the table once with a Spark `bulk_insert`; many teams start by running both formats side by side via XTable and cut writers over only after validating the Hudi side.

That single sentence is the whole decision in miniature, but a production migration deserves more care than a summary. This guide walks through why teams make this move, the three migration strategies and when each fits, the exact XTable and Spark commands involved, the Delta-specific features that need per-table attention, and how to validate and — if necessary — roll back. Throughout, the framing to keep in mind is that both Delta Lake and Hudi are [open table formats](/blog/2026/07/14/what-is-an-open-table-format): metadata layers over Parquet files. That shared foundation is precisely what makes a metadata-level migration possible.

## Why Teams Move from Delta Lake to Hudi

Delta Lake takes a deliberately simple approach on disk and is closely integrated with Spark. Teams that migrate to Hudi are usually not fleeing Delta so much as reaching for write-side machinery that Hudi builds in:

- **Record-level indexing.** Hudi maintains a [multi-modal indexing subsystem](/docs/indexes) — record-level indexes, bloom filters, column statistics, expression indexes — inside an internal metadata table. For update-heavy workloads such as CDC ingestion, an index that maps record keys to file groups means the writer can locate the files affected by an update without scanning or joining against the whole table.
- **Streaming-first Merge-on-Read design.** Hudi's MOR table type absorbs updates into compact log files that are compacted asynchronously, decoupling write latency from file rewrite cost. Workloads that need minute-level freshness under continuous upserts tend to be the strongest motivation for the move.
- **Built-in table services.** Compaction, clustering, cleaning, and indexing ship with the project and run inline or asynchronously, without a separate orchestration layer or a commercial service to keep tables healthy.
- **Built-in ingestion tooling.** Hudi Streamer provides a self-contained ingestion utility with sources for Kafka, DFS, and JDBC, checkpoint management, transformations, and catalog syncing.

These differences are measurable, not just architectural. In benchmarks run with the open-source [LakeLoader](https://github.com/onehouseinc/lake-loader) framework — Spark 3.5, Hudi 1.1.1, Delta Lake 3.3.2, on S3 — we observed roughly 6× lower incremental write latency for Hudi on a 10 TB partitioned fact table with skewed updates, and about 5× lower steady-state latency on Merge-on-Read tables taking sparse, column-level updates. The workload definitions are format-agnostic and repeatable, so you can rerun the comparison against your own update patterns before committing to a migration.

None of this is a knock on Delta — if your workload is mostly appends with occasional merges, run entirely on Spark, and you are happy with your current operational model, you may not need to migrate at all. The rest of this guide assumes you have concluded the write-side capabilities matter for your workload.

## Understand the Three Migration Options

| | A. XTable metadata translation | B. Full rewrite (Spark bulk insert) | C. Incremental dual-write cutover |
|---|---|---|---|
| **Data movement** | None — metadata only | Full copy of the table | Full copy, spread over time |
| **Downtime for writers** | None during sync; brief pause at cutover | Pause writes during the rewrite (or reconcile a delta) | None — new writer runs in parallel |
| **Resulting table** | Hudi metadata over existing Parquet files | Native Hudi table, freshly laid out | Native Hudi table |
| **Lets you re-key / re-partition / resize files** | No — inherits Delta's layout | Yes | Yes |
| **Reversible** | Trivially — source metadata untouched | Source table left intact until decommission | Source table left intact until decommission |
| **Best for** | Large tables, fast side-by-side evaluation, low-risk cutover | Small-to-medium tables, or when you want a clean re-layout | Very large, hot tables that cannot pause and need a new layout |

A few rules of thumb. If the table is large and its current Parquet layout is acceptable, start with **Option A** — it costs almost nothing to try and keeps both formats readable while you evaluate. If the table is small enough that a rewrite finishes in an acceptable window, or you want to change record keys, partitioning, or file sizes as part of the move, **Option B** is simpler to reason about. **Option C** — standing up a parallel Hudi pipeline fed from the same upstream source, backfilling history, then switching consumers — is really Option B with a longer runway, and is worth the extra coordination only for tables that can neither pause nor tolerate an inherited layout. Hudi's [migration guide](/docs/migration_guide) covers additional bootstrapping modes (such as metadata-only bootstrap) that occupy a middle ground for plain Parquet sources.

## Option A: Convert with Apache XTable

[Apache XTable](https://xtable.apache.org) (incubating) is an open source project that translates table metadata between Delta Lake, Hudi, and Iceberg in any direction, without copying or rewriting data files. For this migration, Delta is the source and Hudi is the target. XTable reads the Delta transaction log and writes out the equivalent Hudi metadata — schema, commit history, partition information, and column statistics — alongside the existing Parquet files. (The same tool also works in the other directions; see the companion post on [using Hudi with Apache Iceberg via XTable](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable) for the interop angle.)

Create a config file describing the source and target:

```yaml
# my_config.yaml
sourceFormat: DELTA
targetFormats:
  - HUDI
datasets:
  - tableBasePath: s3://bucket/warehouse/orders
    tableName: orders
```

Then run the sync with the bundled XTable jar (built from [source](https://github.com/apache/incubator-xtable) or downloaded from the project's GitHub packages):

```shell
java -jar path/to/xtable-utilities-bundled.jar --datasetConfig my_config.yaml
```

When the sync completes, the table's base path contains a `.hoodie` directory with Hudi's timeline and metadata, side by side with Delta's `_delta_log`. No Parquet file was read or written — the job's runtime scales with the amount of metadata (number of files and commits), not with data volume. The same directory is now readable as a Delta table *and* as a Hudi table.

Two operational notes, faithful to the [XTable documentation](https://xtable.apache.org/docs/how-to):

- **Syncs are repeatable and incremental.** XTable supports incremental sync (translating only new commits since the last run) with a fallback to full sync, so you can run it on a schedule — or after each Delta commit — to keep the Hudi metadata current while the Delta writer keeps running.
- **Catalog registration is a separate step.** XTable produces metadata in storage; to query the table as Hudi from your engines, register it in your catalog (Hive Metastore, AWS Glue) using Hudi's catalog sync tools or XTable's own catalog sync support. Hudi's [XTable page](/docs/syncing_xtable) shows the reverse direction and the Hudi Streamer integration.

## The Catch: Converted vs Native Tables

Here is the honest fine print. A converted table is *readable* as a Hudi table — snapshot queries, engine integrations, and catalog syncing all work. But most of the reasons you are migrating live on the **write path**: record-level indexes are built and maintained by Hudi writers; streaming upserts, MOR log files, and table services all require Hudi to be the one committing to the table. XTable gives you a Hudi-readable table; it does not retroactively give your Delta writer Hudi's write-side machinery.

So a metadata conversion is the first half of the migration, not the whole thing. The second half is the writer cutover, which follows a simple sequence:

1. **Stop the Delta writer.** Pause the job or pipeline committing to the Delta table. In-flight data can queue upstream (e.g., in Kafka) during the brief window.
2. **Run a final XTable sync.** Translate the last Delta commits so the Hudi metadata reflects the table's final Delta-written state.
3. **Start the Hudi writer.** Point your pipeline — Spark structured streaming, Hudi Streamer, or batch jobs following the [quick start guide](/docs/quick-start-guide) — at the same base path, configured with the record key, ordering field, and table type you validated beforehand. From this commit forward, Hudi owns the table and begins building its indexes and running table services on new data.

Until step 1, you can run the two formats side by side indefinitely: Delta writers keep writing, XTable keeps both metadata layers in sync, and your Hudi-native engines and pipelines read the converted table. That side-by-side period is where the de-risking happens — you validate reads, permissions, catalog integration, and downstream jobs against real data before any writer changes.

## Option B: Full Rewrite with Spark

If the table is modest in size, or you want to change its physical layout — different partitioning, tuned file sizes, a proper record key for upserts — a one-time rewrite is the simplest path. Read the Delta table with Spark, write it back as Hudi using `bulk_insert`, the write operation designed for exactly this initial-load case:

```scala
// spark-shell with both Delta and Hudi bundles on the classpath
val df = spark.read.format("delta").load("s3://bucket/warehouse/orders")

df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field", "order_id").
  option("hoodie.datasource.write.partitionpath.field", "order_date").
  option("hoodie.table.ordering.fields", "updated_at").
  option("hoodie.datasource.write.operation", "bulk_insert").
  option("hoodie.table.name", "orders").
  mode("overwrite").
  save("s3://bucket/warehouse/orders_hudi")
```

Note that the rewrite lands in a *new* base path, leaving the Delta table untouched — that is your rollback story. Choose the record key and ordering field deliberately here: the record key drives Hudi's indexing and upsert semantics, and the ordering field resolves conflicts between multiple versions of the same record (essential for CDC-style sources). Both are covered in the [quick start guide](/docs/quick-start-guide). For very large tables, the same pattern can be applied partition by partition, as described in the [migration guide](/docs/migration_guide).

If writers must keep running during a long rewrite, capture the cut point (a Delta version), rewrite up to it, then apply the trailing changes to the Hudi table before cutover — or accept a short write pause and skip the reconciliation entirely.

## Handling Delta-Specific Features Honestly

Metadata translation is possible because both formats describe Parquet files — but the formats are not feature-identical, and the mapping has edge cases. Per XTable's documented [features and limitations](https://xtable.apache.org/docs/features-and-limitations), audit each table for the following before choosing Option A:

- **Deletion vectors.** XTable currently syncs Copy-on-Write / read-optimized views of tables; Delta deletion vectors are *not* captured by the sync. If a Delta table uses deletion vectors, a converted Hudi view could include deleted rows. Purge deletion vectors on the Delta side first (rewriting affected files so deletes are physically applied), or use the full-rewrite path for those tables.
- **Generated columns.** Generated columns on a Delta source do not carry over to the target schema, and partitioning on generated columns has restricted support (common date transformations, such as deriving a date partition from a timestamp, are handled). Tables that partition on other generated expressions need per-table verification or a rewrite.
- **Column mapping.** Delta's column mapping decouples logical column names from the physical names inside Parquet files. Since a converted table reads the same physical files, tables with column mapping enabled (typically after column renames or drops) deserve explicit schema validation on the Hudi side before you rely on them.

The general rule: XTable is faithful for the mainstream case — Parquet data files, identity or date-derived partitioning, no unapplied deletion vectors — and conservative engineering means *validating each table* rather than assuming the mainstream case. The validation checklist below is not optional garnish; it is how you catch the exceptions.

## Validation Checklist

Run this per table, during the side-by-side period (Option A) or after the rewrite (Option B), before any writer cutover or consumer switch:

1. **Row counts.** `SELECT COUNT(*)` through the Delta path and the Hudi path must match at the same sync point.
2. **Checksums on sample partitions.** Compare aggregate fingerprints — sums, min/max of key columns, or a hash aggregate — on a handful of partitions, including at least one recently written partition and one old one.
3. **Schema comparison.** Diff the schemas reported by both formats, paying attention to nullability, nested fields, and any renamed columns (see column mapping above).
4. **Query-engine smoke tests.** Run representative queries through every engine that will read the Hudi table — Spark, Trino, Presto, Athena, etc. — including partition-pruned queries and, if relevant, time travel and incremental queries.
5. **Catalog re-registration.** Register the Hudi table in your catalog and confirm downstream tools resolve it: BI connections, dbt sources, permissions/grants, and any data-quality jobs pointing at the catalog entry.
6. **Write-path rehearsal.** Before the real cutover, run the intended Hudi writer against a cloned or staging copy and confirm upserts, deletes, and table services behave as expected with your chosen keys and configs.

## Rollback: The Source Table Stays Intact

The most underrated property of both migration paths is that they are non-destructive. With XTable, the Delta transaction log is never modified — Hudi metadata is written *alongside* it, and both remain readable throughout the transition. With a full rewrite, the Delta table sits untouched at its original path. In either case, rollback before cutover is simply "keep using the Delta table," and rollback shortly after cutover means pointing writers back at Delta and replaying the handful of commits made in between (straightforward if your ingestion source, like Kafka or CDC logs, retains data past the cutover window).

Keep the source Delta table — and its `_delta_log` — until the Hudi table has run in production long enough to cover your validation and audit horizon. Only then decommission it. Migrations fail safe when deletion is the last step, not a side effect.

## Conclusion

The strongest reason this migration is more approachable than it used to be is that it is no longer a leap of faith. Because Delta Lake and Hudi both store data as Parquet, Apache XTable turns "migrate" into "add a second metadata layer and evaluate" — you can read your existing tables as Hudi today, run both formats side by side for weeks, validate every engine and consumer, and cut writers over only when the evidence says to. And if the evidence says otherwise, the Delta table never stopped working. Format choice on the lakehouse has stopped being a one-way door; migrate the tables where Hudi's [indexing](/docs/indexes), streaming write path, and built-in table services earn their keep, and take the reversible path to get there.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Do I have to rewrite my data to move from Delta Lake to Hudi?', answer: 'No. Because both formats store data as Apache Parquet, Apache XTable can translate the Delta transaction log into Hudi metadata alongside the same data files, with no data copying. A full rewrite is only needed if you want to change the physical layout, such as partitioning or file sizes, or if the table uses features like deletion vectors that the metadata sync does not capture.'},
  {question: 'Can I run Delta Lake and Hudi side by side on the same table?', answer: 'Yes. XTable writes Hudi metadata next to the existing Delta log, so the same directory is readable as both a Delta table and a Hudi table. You can keep the Delta writer running and re-run XTable incrementally to keep both metadata layers in sync while you validate the Hudi side, which is the recommended de-risking approach before any writer cutover.'},
  {question: 'Will my Databricks jobs still work after migrating to Hudi?', answer: 'Read-only jobs can keep working if you sync the Hudi table back to Delta with XTable, since Databricks then reads familiar Delta metadata. Jobs that write to the table must be moved to Hudi writers, and Databricks-specific features tied to Delta, such as deletion vectors or certain Unity Catalog integrations, do not carry over. Treat every Databricks job as something to test explicitly during the side-by-side period rather than assume compatibility.'},
  {question: 'How long does an XTable conversion take?', answer: 'The sync reads and writes table metadata only, so its runtime scales with the number of files and commits rather than with data volume. That makes it dramatically faster than rewriting the data, and incremental sync keeps subsequent runs short by translating only new commits.'},
  {question: 'What happens to my Delta table history and time travel?', answer: 'The Delta transaction log is left untouched, so Delta-side history remains fully intact and queryable until you decommission the table. The Hudi table maintains its own timeline going forward, and it is prudent to keep the original Delta log around through your audit and validation horizon before deleting anything.'},
  {question: 'What if my Delta table uses deletion vectors?', answer: 'XTable currently syncs Copy-on-Write or read-optimized views, and Delta deletion vectors are not captured by the sync, so a converted view could expose deleted rows. Purge the deletion vectors on the Delta side first so deletes are physically applied to the Parquet files, or migrate that table with a full Spark rewrite instead.'},
]} />
