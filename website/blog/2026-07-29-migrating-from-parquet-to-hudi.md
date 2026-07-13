---
title: "Migrating from Parquet to Apache Hudi: A Practical Guide"
excerpt: "A step-by-step guide to migrating existing Parquet datasets to Apache Hudi — choosing between in-place bootstrap and full rewrite, running the migration, and validating the result."
description: "How to migrate Parquet tables to Apache Hudi: in-place bootstrap vs full rewrite with bulk_insert, key and partitioning choices, code examples, validation and rollback."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/2024-01-20-Data-Engineering-Bootstrapping-Data-lake-with-Apache-Hudi.png
tags:
- migration
- bootstrap
- apache parquet
- apache spark
- guide
---

You can migrate existing Parquet datasets to Apache Hudi either in place — using Hudi's bootstrap operation, which adds Hudi metadata without rewriting data files — or by rewriting the data through a one-time bulk insert. Which path you take comes down to how large the table is, how much rewrite cost and downtime you can absorb, and how the table will be written to afterwards. This guide walks through both paths end to end: choosing a strategy, making the up-front design decisions that are hard to reverse, running the migration with current APIs, validating the result, and what to do if you need to back out.

<!--truncate-->

Why migrate at all? A directory of Parquet files is a perfectly good archive, but it is not a table. It cannot accept updates or deletes without rewriting whole partitions, it offers no transactional guarantees to concurrent readers and writers, and downstream consumers must rescan everything to find what changed. Moving that data under Hudi management gets you [upserts and deletes](/blog/2026/07/15/what-is-upsert-on-a-data-lake) keyed on individual records, ACID commits on the [timeline](/docs/timeline), incremental queries that expose only what changed since a point in time, and automatic file-size management — on the data you already have, queryable by the engines you already use.

## Choose Your Migration Strategy

Hudi gives you three ways to bring an existing Parquet table under management, described in detail in the [migration guide](/docs/migration_guide). Two of them are variants of the *bootstrap* operation, which builds Hudi's timeline and metadata over data that stays where it is; the third is a plain rewrite.

- **METADATA_ONLY bootstrap (in place).** Hudi generates *skeleton* files containing only the record-level metadata columns and key information, one skeleton per original Parquet file, and links them to your original data files. The original data is never copied, so a multi-terabyte table can be migrated in a fraction of the time a rewrite would take. The trade-off: Hudi continues to rely on the original files, so if they are deleted or modified outside Hudi, the table is corrupted. The mechanics of skeleton files are covered in the original deep-dive, [Efficient Migration of Large Parquet Tables to Apache Hudi](/blog/2020/08/20/efficient-migration-of-large-parquet-tables).
- **FULL_RECORD bootstrap.** Hudi copies the full records into new Hudi files, adding the metadata columns as it goes. This is functionally equivalent to a bulk insert — after it completes, the table is self-contained and the original files can eventually be retired. You pay the full rewrite cost.
- **Full rewrite with `bulk_insert`.** Read the Parquet data with Spark and write it out as a new Hudi table using the [`bulk_insert` write operation](/docs/write_operations#bulk_insert). Operationally this is the simplest path — it is just a Spark job using the same datasource API you will use for all subsequent writes — and it gives you a chance to re-layout the data (sorting, partitioning) as you write.

The bootstrap modes can be mixed *within one table, per partition*: a common pattern is FULL_RECORD for a small set of hot, frequently-updated partitions and METADATA_ONLY for the long tail of cold history.

| | METADATA_ONLY bootstrap | FULL_RECORD bootstrap | Full rewrite (`bulk_insert`) |
|---|---|---|---|
| Data files rewritten | No — skeleton files only | Yes | Yes |
| Migration time / cost | Low, proportional to metadata | High, proportional to data size | High, proportional to data size |
| Original Parquet files after migration | Still required — must not be modified or deleted | Independent; originals can be retired | Independent; originals can be retired |
| Resulting table | Hudi table referencing external base files | Self-contained Hudi table | Self-contained Hudi table |
| Chance to re-sort / re-layout data | No | No | Yes (`hoodie.bulkinsert.sort.mode`) |
| Best for | Very large tables where rewrite is impractical | Hot partitions of a large table (via regex selector) | Small-to-medium tables, or when you want a clean re-layout |

Decision rule of thumb: if the table is small enough that rewriting it is an overnight Spark job you can afford to run once, take the full rewrite — it leaves no strings attached to the old files. If the table is tens of terabytes or more and a rewrite is measured in days and real money, bootstrap in place with METADATA_ONLY, optionally upgrading hot partitions to FULL_RECORD.

## Before You Start: Three Decisions That Are Hard to Change

However you migrate, you are about to give this dataset a table identity. Three choices deserve deliberate thought *before* you run anything, because changing them later effectively means migrating again.

1. **Record key.** The field (or comma-separated fields) that uniquely identifies each record — set via `hoodie.datasource.write.recordkey.field`. Every upsert and delete is keyed on it, and [indexes](/docs/indexes) map keys to file groups. Pick something genuinely unique and stable: a natural business key or an existing surrogate key. See [key generation](/docs/key_generation) for composite and timestamp-based options.
2. **Ordering field(s).** Set via `hoodie.table.ordering.fields`. When two records with the same key collide — within one batch or across writes — the record with the larger ordering value wins. An event timestamp or a monotonically increasing version column is the usual choice.
3. **Partitioning.** Set via `hoodie.datasource.write.partitionpath.field`. If you bootstrap in place, your existing directory layout *is* your partitioning, so make peace with it first. If you do a full rewrite, this is your one cheap opportunity to fix a bad partition scheme — too many tiny partitions, or partitioning on a column no query filters on. Also decide on `hoodie.datasource.write.hive_style_partitioning` (`city=chennai/` style paths) now, for catalog compatibility.

You will also choose a [table type](/docs/table_types) — Copy-on-Write is the right default for a freshly migrated table; you can adopt Merge-on-Read where write latency demands it.

## Path A: Full Rewrite with bulk_insert

For small and medium tables, migration is a single Spark job: read Parquet, write Hudi. The `bulk_insert` operation exists precisely for this — unlike `upsert`/`insert`, it uses a disk-based, sort-oriented write path that scales to very large initial loads without caching input in memory (see [write operations](/docs/write_operations) for the full comparison).

```scala
// spark-shell with the Hudi bundle, per the quick start guide
val df = spark.read.format("parquet").load("s3://my-bucket/warehouse/trips_parquet")

df.write.format("hudi").
  option("hoodie.table.name", "trips").
  option("hoodie.datasource.write.operation", "bulk_insert").
  option("hoodie.datasource.write.recordkey.field", "trip_id").
  option("hoodie.datasource.write.partitionpath.field", "city").
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.hive_style_partitioning", "true").
  mode("overwrite").
  save("s3://my-bucket/warehouse/trips")
```

Two knobs worth knowing:

- `hoodie.bulkinsert.sort.mode` controls layout of the written files. The default `NONE` matches plain `spark.write.parquet()` in speed and file count; `GLOBAL_SORT` costs a sort but produces the best file sizes and, if you sort by a commonly filtered column, better data skipping forever after.
- Note that `bulk_insert` does a best-effort job at file sizing rather than guaranteeing it; ongoing `upsert`/`insert` writes will progressively correct small files.

From here on, the table behaves exactly like any Hudi table in the [quick start guide](/docs/quick-start-guide): switch subsequent writes to `upsert` with `mode("append")`, and query it from Spark, Trino, Presto, or Hive.

## Path B: In-Place Bootstrap for Large Tables

When rewriting is off the table, use bootstrap. The two most convenient front-ends are the Hudi Streamer utility and the Spark SQL `CALL` procedure; both drive the same underlying mechanism, and both are documented in the [migration guide](/docs/migration_guide).

### Using Hudi Streamer

[Hudi Streamer](/docs/hoodie_streaming_ingestion#hudi-streamer) supports bootstrap via the `--run-bootstrap` flag. This example applies FULL_RECORD mode to all partitions matching the regex `.*` using the regex mode selector — change the regex to bootstrap only hot partitions as FULL_RECORD, and unmatched partitions get the other mode:

```bash
spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--class org.apache.hudi.utilities.streamer.HoodieStreamer /path/to/hudi-utilities-bundle.jar \
--run-bootstrap \
--target-base-path s3://my-bucket/warehouse/trips \
--target-table trips \
--table-type COPY_ON_WRITE \
--hoodie-conf hoodie.bootstrap.base.path=s3://my-bucket/warehouse/trips_parquet \
--hoodie-conf hoodie.datasource.write.recordkey.field=trip_id \
--hoodie-conf hoodie.datasource.write.partitionpath.field=city \
--hoodie-conf hoodie.table.ordering.fields=ts \
--hoodie-conf hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector \
--hoodie-conf hoodie.bootstrap.mode.selector.regex='.*' \
--hoodie-conf hoodie.bootstrap.mode.selector.regex.mode=FULL_RECORD \
--hoodie-conf hoodie.datasource.write.hive_style_partitioning=true
```

The key config is `hoodie.bootstrap.base.path` — the location of the existing Parquet dataset. If you supply *only* that config, Hudi defaults to METADATA_ONLY mode for every partition (`hoodie.bootstrap.mode.selector` defaults to `MetadataOnlyBootstrapModeSelector`), which is exactly the cheap in-place migration most large tables want.

### Using the Spark SQL CALL procedure

If you live in Spark SQL, the [`run_bootstrap` procedure](/docs/procedures#run_bootstrap) does the same job:

```sql
CALL run_bootstrap(
  table => 'trips',
  table_type => 'COPY_ON_WRITE',
  bootstrap_path => 's3://my-bucket/warehouse/trips_parquet',
  base_path => 's3://my-bucket/warehouse/trips',
  rowKey_field => 'trip_id',
  partition_path_field => 'city'
);
```

`bootstrap_path` points at the existing Parquet data and `base_path` at the new Hudi table location. The default `selector_class` is again `MetadataOnlyBootstrapModeSelector`; pass `selector_class => 'org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector'` for a full-record bootstrap. There is also a [Hudi CLI path](/docs/migration_guide#using-hudi-cli) (`bootstrap run`) if you prefer driving it from the CLI, and a plain Spark datasource loop for partition-by-partition conversion.

One operational note for METADATA_ONLY tables: from this point on, the original Parquet location is part of your table. Lock it down — revoke write access, exclude it from retention/lifecycle policies — because any modification or deletion there corrupts the Hudi table.

## Migrating New Partitions Only

There is a zero-migration option worth knowing: keep historical partitions as plain Parquet and let Hudi manage only *new* partitions, as described in the [migration guide](/docs/migration_guide#use-hudi-for-new-partitions-alone). Hudi tolerates such mixed tables as long as each partition is entirely Hudi-managed or entirely not. Start writing new partitions (or convert just the last N) through Hudi, and leave the rest untouched.

The caveat is fundamental: none of Hudi's primitives work on the unmanaged partitions — no upserts, no deletes, no incremental pull there. This pattern fits genuinely append-only tables (immutable event logs partitioned by date) where old partitions will never be touched again. If there is any chance of updates landing in history — CDC replication, GDPR deletes, late corrections — migrate the whole table.

## Validation Checklist

Before pointing production at the new table, verify it. Bootstrap and bulk insert are one-shot operations; catching a wrong key choice now costs minutes, catching it in three months costs a re-migration.

1. **Row counts match.** `spark.read.format("hudi").load(basePath).count()` against the source Parquet count, overall and per partition.
2. **Keys are actually unique.** Group by your record key and confirm no count exceeds 1 — duplicate keys silently collapse into one record on the first upsert.
3. **Sample lookups.** Pick a handful of known records and confirm field-level equality between source and target, including partition placement.
4. **Exercise a write.** Upsert a few records and delete one on a staging copy or a scratch partition; confirm the change is visible and row counts move as expected.
5. **Query engines see the table.** Sync the table to your catalog (Hive Metastore, AWS Glue, etc. — see [syncing to catalogs](/docs/syncing_metastore)) and run representative queries from every engine that matters — Spark, Trino, Presto. This matters doubly for METADATA_ONLY tables, where engines read merged skeleton + original data.
6. **Incremental query sanity.** Run an incremental query from the bootstrap commit forward and confirm it returns your test writes.

## Rollback Plan

A comforting property of both paths: the original Parquet data is never mutated. Bootstrap writes skeleton files and a timeline in a *new* base path; a full rewrite writes an entirely new copy. Your rollback plan is therefore simple:

- Keep the source Parquet dataset intact and readable until validation completes and the new table has served real workloads for an agreed soak period.
- If something is wrong — bad record key, wrong partitioning — delete the Hudi base path, fix the configuration, and re-run the migration. Nothing about the source has changed.
- For METADATA_ONLY bootstrap, remember the inverse: you can always abandon the Hudi table, but you can never abandon the original files while keeping it.

The one thing to avoid during the migration window is dual-writing to both the old Parquet location and the new Hudi table without a plan for reconciling them. Freeze writes to the source, migrate, validate, then cut writers over to Hudi.

## Operational Follow-Ups

Migration makes the data Hudi-managed; a few follow-ups make it fast.

- **Metadata table and indexes.** Hudi's [metadata table](/docs/metadata) eliminates file listings, and [indexes](/docs/indexes) — column stats for data skipping, record level index for point lookups — determine upsert performance. Review which indexes fit your workload once real update traffic starts.
- **Compaction, if you adopt Merge-on-Read.** MoR tables need [compaction](/docs/compaction) to fold log files into base files; make sure it is scheduled and running.
- **Clustering.** If you bootstrapped in place (no chance to sort at write time) or bulk-inserted with `NONE` sort mode, [clustering](/docs/clustering) can reorganize and sort data in the background later — regaining the layout benefits without another migration.
- **Cleaning and file sizing.** The defaults are sane, but confirm cleaner retention matches your longest-running queries, and let ongoing writes correct any small files the initial load produced.

## Conclusion

Migrating Parquet to Hudi is not a leap; it is a choice between two well-worn paths. Small and medium tables: one `bulk_insert` Spark job and you have a self-contained Hudi table, with a free chance to fix layout on the way through. Large tables: bootstrap in place with METADATA_ONLY (upgrading hot partitions to FULL_RECORD) and skip the rewrite entirely. In both cases the original data survives untouched, so the risk profile is a validation exercise, not a bet. Decide your record key, ordering field, and partitioning deliberately, validate before cutover, and keep the source until you trust the result. Start with the [migration guide](/docs/migration_guide) and the [quick start](/docs/quick-start-guide), and if you want the design rationale behind skeleton files, the [deep-dive on efficient migration](/blog/2020/08/20/efficient-migration-of-large-parquet-tables) covers it.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'Do I have to rewrite my Parquet files to use Hudi?',
    answer: 'No. Hudi\'s bootstrap operation in METADATA_ONLY mode generates small skeleton files containing only record-level metadata and links them to your existing Parquet files, so the original data is never copied or modified. A FULL_RECORD bootstrap or a bulk_insert rewrite is only needed if you want the table to be fully self-contained or want to change its layout.',
  },
  {
    question: 'How long does a Parquet to Hudi migration take?',
    answer: 'A METADATA_ONLY bootstrap only writes metadata, so it runs in a small fraction of the time of a rewrite, even on multi-terabyte tables. A full rewrite with bulk_insert or a FULL_RECORD bootstrap is proportional to data size, roughly comparable to a Spark job that reads and rewrites the whole dataset. Many teams mix the two, fully rewriting a few hot partitions and metadata-bootstrapping the cold history.',
  },
  {
    question: 'Can I roll back a migration to Hudi?',
    answer: 'Yes. Neither bootstrap nor a bulk_insert rewrite modifies the original Parquet files, so rolling back means deleting the new Hudi base path and continuing to use the source data. Keep the source dataset intact until you have validated row counts, key uniqueness, and queries from all your engines against the new table.',
  },
  {
    question: 'What is the difference between METADATA_ONLY and FULL_RECORD bootstrap?',
    answer: 'METADATA_ONLY writes skeleton files with just the Hudi metadata columns and keeps relying on your original Parquet files, so it is fast but the originals must never be modified or deleted. FULL_RECORD copies the complete records into new Hudi files, which costs a full rewrite but produces a self-contained table. You can apply different modes to different partitions of the same table using a regex-based selector.',
  },
  {
    question: 'Can I keep old partitions as plain Parquet and only use Hudi for new data?',
    answer: 'Yes, as long as each partition is entirely Hudi-managed or entirely not. This works well for append-only tables where historical partitions never change. The catch is that upserts, deletes, and incremental queries only work on the Hudi-managed partitions, so if updates might ever touch history, migrate the whole table instead.',
  },
  {
    question: 'What should I decide before migrating a table to Hudi?',
    answer: 'Three things: the record key that uniquely identifies each record, the ordering field used to pick a winner when the same key appears twice, and the partitioning scheme. These shape every subsequent write and are hard to change without re-migrating. If you bootstrap in place, the existing directory layout becomes your partitioning, so review it before you start.',
  },
]} />
