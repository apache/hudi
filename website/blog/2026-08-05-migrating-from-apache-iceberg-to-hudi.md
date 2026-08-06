---
title: "Migrating from Apache Iceberg to Apache Hudi"
excerpt: "A practical guide to adopting Apache Hudi on existing Iceberg tables — via XTable metadata translation or a one-time rewrite — while keeping every Iceberg-based reader working through reverse sync."
description: "How to migrate Iceberg tables to Apache Hudi with XTable metadata conversion or a Spark rewrite, keeping Snowflake, BigQuery and Trino Iceberg readers working."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/2025-07-02-Lakehouse-Architecture-apache-hudi-and-apache-iceberg.png
tags:
- migration
- apache xtable
- apache iceberg
- guide
---

You can adopt Apache Hudi on an existing Apache Iceberg table either by translating its metadata with [Apache XTable](https://xtable.apache.org) — no data rewrite, since both formats store data as Parquet files — or via a one-time rewrite; and because XTable also works in the reverse direction, projecting a Hudi table back out as Iceberg, your existing Iceberg readers can keep working after the switch. That second point changes the shape of the whole exercise. "Migrating from Iceberg to Hudi" does not have to mean a hard cutover of every writer, reader, catalog entry and dashboard on the same weekend. It can mean moving just the *write path* to Hudi — to get record-level indexes, streaming upserts and built-in table services — while every Snowflake, BigQuery or Trino consumer that speaks Iceberg continues reading the same table, unaware anything changed underneath.

This guide walks through both migration options with working configuration and code, the cutover sequence that de-risks the switch, an honest look at which Iceberg features do not map one-to-one, and a validation checklist with a rollback story. It is the Iceberg-side companion to our guides on [using Hudi with Apache Iceberg via XTable](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable) and [migrating from Delta Lake to Hudi](/blog/2026/08/04/migrating-from-delta-lake-to-hudi).

## Why Teams Move from Iceberg to Hudi

The migrations we see are almost always driven by the write side. Apache Iceberg has broad catalog and engine support, and for append-mostly batch analytics it serves many teams fine. The friction shows up when workloads become mutation-heavy or latency-sensitive:

- **Record-level indexes for fast upserts.** Hudi maintains a [multi-modal indexing subsystem](/docs/indexes) — record-level index, bloom filters, expression and secondary indexes — that maps record keys to file groups. An upsert locates exactly the files it must touch instead of planning a join or scan against the target to find matching rows. For CDC pipelines applying millions of scattered updates, this is routinely the difference between minutes and hours.
- **Merge-on-Read designed for streaming ingest.** Hudi's MOR tables absorb updates as compact log files merged on read, so writers sustain high-frequency commits — minute-level or faster from Kafka, Flink or Spark Structured Streaming — without churning out rewritten Parquet on every batch.
- **Built-in table services.** Compaction, clustering, cleaning and file sizing are part of the Hudi runtime and run inline or asynchronously without external orchestration. With Iceberg, that maintenance is left to engines, scheduled Spark procedures or a vendor service — someone has to own it.
- **CDC-grade change streams.** Hudi tables serve [incremental queries](/docs/sql_queries#incremental-query): give me exactly the records that changed between two points on the timeline, including before/after images in CDC mode. Downstream pipelines chain off tables directly instead of re-reading snapshots and diffing.

The gap is measurable. In benchmarks run with the open-source [LakeLoader](https://github.com/onehouseinc/lake-loader) framework — Spark 3.5, Hudi 1.1.1, Iceberg 1.10.0, on S3 — we observed roughly 4× lower incremental write latency for Hudi on a 10 TB partitioned fact table with skewed updates, and about 8× lower steady-state latency on Merge-on-Read tables taking sparse, column-level updates, with Hudi's record-level index sidestepping the full-table-scan merge joins that dominate the alternative write path. The workload definitions are format-agnostic and repeatable, so you can rerun the comparison on your own update patterns before deciding.

The point is not that Iceberg cannot handle these workloads — it is that Hudi's write path, indexing and self-managing services are built for mutable, streaming-oriented workloads. If that describes the tables you are running, here is how to move them.

## Migrate the Writer, Keep the Readers

The biggest source of migration risk is rarely the table itself — it is the long tail of consumers. A warehouse reading through an Iceberg catalog, BI dashboards, other teams' Spark jobs. A migration plan that requires all of them to change on cutover day is fragile enough that most teams never start.

The two-way XTable pattern removes that requirement:

1. **Writers move to Hudi.** Your ingestion pipeline gains Hudi's upsert indexes, MOR streaming writes and table services.
2. **XTable continuously projects the Hudi table back out as Iceberg.** After each Hudi commit (or on a schedule), XTable translates the Hudi timeline into Iceberg metadata over the *same* Parquet data files, and its catalog sync can keep Hive Metastore or AWS Glue entries current.
3. **Readers keep reading Iceberg.** Snowflake, BigQuery, Trino Iceberg catalogs, anything else that only speaks Iceberg — all keep working. They can each move to native Hudi reads later, on their own schedule, or never.

Because the data files are shared and only lightweight metadata is generated, the reverse projection is cheap and stays fresh. The migration decision decomposes: the writer cutover is one contained change, and every reader migration becomes optional and independent. We cover the reader-side mechanics in depth in the [Hudi + Iceberg interoperability guide](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable); the rest of this post focuses on getting the table and the writer onto Hudi.

## Option A: Convert Iceberg Metadata to Hudi with Apache XTable

XTable (incubating) translates table metadata between Hudi, Iceberg and Delta Lake in any direction. Pointed at an Iceberg table, it reads the Iceberg snapshot and writes Hudi metadata — a `.hoodie` timeline with schema, commit history, partition and column statistics — referencing the existing Parquet files in place. Nothing is copied or rewritten.

Grab the XTable bundled jar (build from [source](https://github.com/apache/incubator-xtable) or download from GitHub packages) and create a config:

```yaml md title="iceberg_to_hudi.yaml"
sourceFormat: ICEBERG
targetFormats:
  - HUDI
datasets:
  -
    tableBasePath: s3://warehouse/orders
    tableDataPath: s3://warehouse/orders/data
    tableName: orders
    partitionSpec: order_date:VALUE
```

`tableDataPath` is needed for Iceberg sources when data files live under a subdirectory (the layout Iceberg warehouses typically use) rather than directly under the base path. Then run the sync:

```shell
java -jar path/to/xtable-utilities-bundled.jar --datasetConfig iceberg_to_hudi.yaml
```

The run produces Hudi metadata under the table's base path. Any Hudi-capable engine can now read the table — the same files your Iceberg readers are still using. XTable syncs incrementally by default (translating only new commits, falling back to a full sync when needed), so re-running it on a schedule keeps the Hudi view current for as long as the Iceberg writer remains active. Registering the table in your catalog of choice makes it visible to Hudi readers alongside the existing Iceberg entry.

At this stage you have a zero-copy, read-ready Hudi table and have changed nothing about production. That alone is useful — you can benchmark Hudi readers against real data before committing to anything.

## Converted vs Native: Cutting the Writer Over

Metadata translation gets you Hudi *reads*. The features that motivate the migration — record-level index lookups, streaming upserts, change streams, self-managing table services — come from Hudi *writing* the table: assigning record keys, maintaining indexes and the metadata table, running services against its own timeline. To get them, you cut the writer over. The sequence:

1. **Stop the Iceberg writer.** Pause ingestion at a clean commit boundary.
2. **Run a final XTable sync** (Iceberg → Hudi) so the Hudi metadata reflects the last Iceberg commit exactly.
3. **Start the Hudi writer** against the table's base path, configuring the record key, ordering fields (`hoodie.table.ordering.fields`) and partitioning to match your workload. From here, commits land natively on the Hudi timeline, indexes are built and maintained, and table services take over maintenance.
4. **Enable reverse sync** (Hudi → Iceberg) — via the XTable job on a schedule with `sourceFormat: HUDI`, or per-commit through the Hudi Streamer sync extension ([docs](/docs/syncing_xtable)) — so Iceberg readers see every new Hudi commit.

The gap between steps 1 and 4 is minutes of paused ingestion, not a data-copy window — for most tables the downtime is a single deferred micro-batch. Validate the native-write onboarding on a staging copy first: depending on the table's layout and key structure, some tables are better served by Hudi's [bootstrap mechanism](/docs/migration_guide) or a full rewrite (Option B below) to get a fully native file layout with populated Hudi metadata fields, rather than writing directly on top of converted metadata. Either way, the end state is the same: Hudi owns the write path, Iceberg remains a continuously refreshed read projection.

## Option B: One-Time Rewrite with Spark

If the table is modest in size, or you want to change its physical layout anyway — new partitioning, clustering by query predicates, cleaning out accumulated small files — a full rewrite is the simpler and sometimes better move. Read the Iceberg table with Spark, write a Hudi table:

```scala
// spark-shell with both Iceberg and Hudi bundles on the classpath
val df = spark.read.format("iceberg").load("s3://warehouse/orders")

df.write.format("hudi").
  option("hoodie.table.name", "orders").
  option("hoodie.datasource.write.recordkey.field", "order_id").
  option("hoodie.datasource.write.partitionpath.field", "order_date").
  option("hoodie.table.ordering.fields", "updated_at").
  option("hoodie.datasource.write.operation", "bulk_insert").
  mode("overwrite").
  save("s3://warehouse/orders_hudi")
```

This is a plain batch job — parallelize by partition for very large tables, as shown in the [migration guide](/docs/migration_guide), which also covers Hudi Streamer's bootstrap mode (including METADATA_ONLY, which builds skeleton files instead of rewriting data). The rewrite costs a full pass over the data but produces a completely native Hudi table with no conversion caveats, and cutover is just repointing the writer and backfilling the delta that accrued during the copy. See the [Spark quick start](/docs/quick-start-guide) for current write configurations. And note the reverse-sync pattern applies here identically: run XTable over the new Hudi table and your Iceberg readers follow along to the new location.

## What May Not Map One-to-One

An honest migration plan checks feature parity per table rather than assuming it. Watch for:

- **Merge-on-read delete files.** XTable syncs the underlying Parquet data files; Iceberg v2 position/equality delete files are not carried through the conversion. Compact them away first (e.g. Iceberg's `rewrite_data_files` / `rewrite_position_delete_files` procedures) so the source snapshot is materialized in data files before converting. The same applies to newer v3 constructs such as deletion vectors.
- **Hidden partitioning and partition evolution.** Iceberg partitions by column transforms (`days(ts)`, `bucket(n, id)`) invisible to queries, and lets the partition spec evolve over time. Hudi partitions by explicit partition path fields. Simple value and date-based partitioning translate cleanly; bucket transforms and tables carrying multiple historical partition specs need per-table validation, and a rewrite (Option B) into a layout of your choosing is often the cleaner answer for them.
- **Iceberg-specific column types and metadata.** Features tied to recent Iceberg spec versions — row lineage, variant type in v3, engine-specific catalog behaviors — have no direct Hudi equivalent or map differently. Inventory what each table actually uses; most analytics tables use none of these.
- **Snapshot history.** The converted Hudi table's timeline starts at conversion. Old Iceberg snapshots remain time-travelable through the Iceberg metadata (which stays intact on storage), but do not appear as Hudi commits.

None of these block a migration; they determine which tables take the metadata-translation fast path and which deserve a rewrite.

## Validation Checklist and Rollback

Before decommissioning anything, verify per table:

- **Row counts and checksums** match between the Iceberg source and Hudi target (and, after cutover, between the Hudi table and its reverse-synced Iceberg projection).
- **Schema fidelity** — column types, nullability and nested structures survived translation.
- **Partition pruning** works: run a partition-filtered query against the Hudi table and confirm file skipping.
- **Reader smoke tests** from every consumer that matters: native Hudi reads from Spark/Trino, and Iceberg reads from the warehouse or catalog your downstream teams use.
- **Writer dry run** on staging: upsert, delete, then read back and confirm index behavior and table service activity on the timeline.

The rollback story is what makes this migration low-stakes. Until you delete it, the original Iceberg metadata is untouched — XTable conversion writes new metadata alongside, and Option B writes to a new path entirely — so reverting before cutover means simply not proceeding. After cutover, reverse sync means there is a live, continuously updated Iceberg view of the table at all times: if a downstream Iceberg consumer misbehaves, it keeps reading Iceberg while you debug, and in the worst case you can resume Iceberg-native writes from the current state rather than restoring backups. Keep the old metadata around until the new write path has run through a full cycle of your table services and audits.

## Conclusion

Table format choice used to be a one-way architectural door; XTable makes it a revolving one. Adopting Hudi on an Iceberg estate is not a monolithic rewrite: translate metadata in place to light up Hudi reads, cut writers over table-by-table to gain [indexes](/docs/indexes), streaming upserts and built-in table services, and let reverse sync keep every Iceberg-based consumer running exactly as before. Rewrite the tables that want a new layout; translate the rest. Start with one mutation-heavy table where Hudi's write path pays off most, prove the loop end to end — convert, cut over, reverse-sync, validate — and expand from there, knowing the decision stays reversible the whole way.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Can Snowflake still read my table after migrating to Hudi?', answer: 'Yes. Apache XTable can continuously project a Hudi table back out as Iceberg metadata over the same Parquet data files, and can sync catalogs like AWS Glue or Hive Metastore. Snowflake, BigQuery and other engines that read Iceberg keep working against that projection, so reader migrations become optional and can happen on their own schedule.'},
  {question: 'Does migrating from Iceberg to Hudi require rewriting data?', answer: 'Not necessarily. Both formats store data as Parquet files, so Apache XTable can translate the Iceberg metadata into a Hudi timeline referencing the existing files in place, with no data copy. A full rewrite via Spark is only needed when you want to change the physical layout, such as new partitioning or clustering, or for tables whose Iceberg features don\'t translate cleanly.'},
  {question: 'What do I gain by moving the write path to Hudi?', answer: 'Hudi\'s writer maintains record-level and bloom indexes that make scattered upserts fast, supports Merge-on-Read tables built for high-frequency streaming ingestion, runs compaction, clustering and cleaning as built-in table services, and serves incremental and CDC queries off its timeline. These come from Hudi writing the table, which is why the cutover targets writers first.'},
  {question: 'Can I go back to Iceberg if the migration doesn\'t work out?', answer: 'Yes. The original Iceberg metadata stays intact on storage until you delete it, so before cutover, rollback means simply not proceeding. After cutover, reverse sync maintains a live Iceberg view of the table at all times, so in the worst case you can resume Iceberg-native writes from the current state rather than restoring from backups.'},
  {question: 'How much downtime does the writer cutover take?', answer: 'Minutes, not hours. The sequence is: pause the Iceberg writer at a commit boundary, run a final XTable sync so the Hudi metadata matches exactly, start the Hudi writer, and enable reverse sync from Hudi back to Iceberg. There is no data copy in that window, so the downtime is typically a single deferred micro-batch.'},
  {question: 'Do Iceberg v2 delete files survive the conversion?', answer: 'No. XTable syncs the underlying Parquet data files, and Iceberg position or equality delete files are not carried through. Run Iceberg\'s compaction procedures to materialize deletes into data files before converting, so the snapshot XTable translates is fully represented in Parquet.'},
]} />
