---
title: "Migrating from Apache Hive Tables to Apache Hudi"
excerpt: "A practical guide to converting existing Apache Hive tables into Apache Hudi tables — in place via bootstrap or by rewriting — and re-registering them in your metastore with Hudi's catalog sync."
description: "Step-by-step guide to migrating Apache Hive tables to Apache Hudi: in-place bootstrap vs full rewrite, Hive metastore sync, validation and safe decommissioning."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/2024-01-20-Data-Engineering-Bootstrapping-Data-lake-with-Apache-Hudi.png
tags:
- migration
- bootstrap
- apache hive
- guide
---

Migrating an Apache Hive table to Apache Hudi means converting its underlying data files into a Hudi table — either in place with Hudi's [bootstrap](/docs/migration_guide) mechanism or by rewriting the data with Spark — and re-registering it in your metastore through Hudi's [catalog sync](/docs/syncing_metastore). Your existing Hive, Spark, Presto and Trino queries keep working against the same metastore entry, and the table gains upserts, deletes, ACID transactions and incremental reads that a plain Hive table never had.

This guide walks through the whole journey: why teams outgrow plain Hive tables, what actually changes on disk and in the metastore, how to plan key and partition mapping, the two migration paths (in-place bootstrap and full rewrite), re-pointing the metastore, validating with the queries you already run, and finally decommissioning the old table safely.

## Why Teams Move Off Plain Hive Tables

A classic Hive external table is a directory convention plus a metastore entry. That simplicity served the Hadoop era well, but it carries structural costs that get worse as tables grow and freshness expectations tighten:

- **No transactions on the data lake.** A plain Hive table over Parquet or ORC has no atomic commit protocol. A failed `INSERT OVERWRITE` can leave partial files behind, and readers racing a writer can see half-written partitions. Every team ends up building `_SUCCESS`-file conventions or staging-directory dances around this gap.
- **Updates mean rewriting partitions.** There is no record-level update or delete. Fixing one late-arriving record, applying a CDC stream, or servicing a GDPR deletion request means rewriting every affected partition wholesale — slow, expensive, and disruptive to readers.
- **Slow, listing-bound query planning.** Query engines discover data in Hive tables by listing directories. On cloud object stores, listing millions of files across thousands of partitions dominates planning time, and historically the eventual consistency of object-store listings caused correctness surprises too.
- **No incremental consumption.** Downstream pipelines cannot ask a Hive table "what changed since yesterday?" — they re-scan whole partitions on a schedule, multiplying compute costs.

Hive ACID (transactional ORC tables) brings transactions and row-level updates to Hive, but it is tied to ORC, to Hive's own compactor running inside the metastore ecosystem, and its delta files are not first-class citizens in Spark, Trino and the broader lakehouse engine ecosystem the way an open table format is. Lakehouse formats like Hudi were built to provide transactions, mutability and incremental processing directly on open columnar files, queryable from every major engine. If you want the deeper background on table types, see [Table & Query Types](/docs/table_types).

## What Changes — and What Doesn't

It helps to be precise about what a migration touches:

**What stays the same.** Your data remains in open columnar files — Hudi writes Parquet base files (and can bootstrap existing Parquet in place). The data keeps living on the same HDFS or object storage. Analysts keep querying through the same metastore-backed catalog, from the same engines.

**What changes.** The table gains a Hudi timeline (a commit log recording every action), metadata and indexes mapping record keys to files, and Hudi-managed file layout with automatic file sizing. The metastore entry is re-registered through Hudi's sync so that engines route reads through Hudi's integrations — Hive via the Hudi input format, Spark and Trino/Presto via their native Hudi connectors. From then on, writes go through Hudi (Spark, Flink, or [Hudi Streamer](/docs/hoodie_streaming_ingestion)) and become atomic, and the table supports upserts, deletes and incremental queries.

If your estate also includes raw Parquet directories that were never registered in Hive, the companion post on [migrating from Parquet to Hudi](/blog/2026/07/29/migrating-from-parquet-to-hudi) covers that path; this post focuses on tables living in the Hive metastore.

## Planning the Migration

Before running anything, map three concepts from your Hive schema onto Hudi's table configs:

**Record key.** Hudi identifies each record by a key so it can find and update it later. Pick the column (or comma-separated columns) that uniquely identifies a row — an id column, or a composite of natural keys. This becomes `hoodie.datasource.write.recordkey.field` (or `primaryKey` in Spark SQL table properties). If the Hive table genuinely has no key and is append-only, you can migrate without one, but you give up upserts on that table.

**Ordering field.** When two versions of the same key arrive, Hudi resolves the winner using ordering fields (`hoodie.table.ordering.fields`). An `updated_at`-style timestamp is the usual choice; it prevents older records from overwriting newer ones.

**Partition mapping.** Hudi partitioning maps directly from Hive partitioning:

- *Single-level partitions* (`ds=2026-08-01`) map to a single `hoodie.datasource.write.partitionpath.field` with `hoodie.datasource.write.hive_style_partitioning=true` to preserve the `key=value` directory style your Hive table already uses.
- *Multi-level partitions* (`year=2026/month=08/day=01`) map to a comma-separated partition path field list (e.g. `year,month,day`); on the sync side, the default `MultiPartKeysValueExtractor` splits partition values on `/` correctly.
- *Non-partitioned tables* simply omit the partition path field; during metastore sync Hudi infers `NonPartitionedExtractor` automatically.

Also decide the table type now: Copy-on-Write (CoW) is the simplest starting point for formerly-Hive analytics tables; Merge-on-Read (MoR) suits update-heavy or streaming write patterns. The [table types guide](/docs/table_types) covers the trade-off; note that MoR changes what appears in your metastore after sync (more below).

## Path A: In-Place Bootstrap for Large Tables

For big tables, rewriting terabytes of perfectly good Parquet just to adopt a new format is wasteful. Hudi's [bootstrap](/docs/migration_guide) converts a table in place, with two modes you can mix per partition:

- **METADATA_ONLY** generates skeleton files containing only Hudi's metadata columns and record keys, alongside pointers to your original data files — avoiding the full cost of rewriting the dataset. Queries and upserts work normally. The caveat: Hudi still relies on the original files, so they must not be deleted or modified.
- **FULL_RECORD** copies the full record data into Hudi-managed files, functionally equivalent to a bulk insert. After it completes, the Hudi table is fully self-contained.

A common large-table strategy is METADATA_ONLY for cold historical partitions and FULL_RECORD for recent, actively-updated partitions — selected with a regex over partition paths.

### With Hudi Streamer

Hudi Streamer supports bootstrap via the `--run-bootstrap` flag. This example applies FULL_RECORD to all partitions matching the regex `.*`, keeping hive-style partitioning:

```bash
spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--class org.apache.hudi.utilities.streamer.HoodieStreamer /path/to/hudi-utilities-bundle.jar \
--run-bootstrap \
--target-base-path hdfs://ns1/warehouse/hudi/trips \
--target-table trips \
--table-type COPY_ON_WRITE \
--hoodie-conf hoodie.bootstrap.base.path=hdfs://ns1/hive/warehouse/mydb.db/trips \
--hoodie-conf hoodie.datasource.write.recordkey.field=trip_id \
--hoodie-conf hoodie.datasource.write.partitionpath.field=ds \
--hoodie-conf hoodie.table.ordering.fields=updated_at \
--hoodie-conf hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector \
--hoodie-conf hoodie.bootstrap.mode.selector.regex='.*' \
--hoodie-conf hoodie.bootstrap.mode.selector.regex.mode=FULL_RECORD \
--hoodie-conf hoodie.datasource.write.hive_style_partitioning=true
```

`hoodie.bootstrap.base.path` points at the existing Hive table's location — the warehouse directory the metastore already knows about. To split modes by partition, keep the `BootstrapRegexModeSelector` and set the regex to match the partitions that should get the mode named in `hoodie.bootstrap.mode.selector.regex.mode`; unmatched partitions get the other mode. With only `hoodie.bootstrap.base.path` provided and no selector configs, METADATA_ONLY is the default for everything.

### With the Spark SQL CALL procedure

If you prefer staying in Spark SQL, the [`run_bootstrap` procedure](/docs/procedures#run_bootstrap) does the same job:

```sql
CALL run_bootstrap(
  table => 'trips',
  table_type => 'COPY_ON_WRITE',
  bootstrap_path => 'hdfs://ns1/hive/warehouse/mydb.db/trips',
  base_path => 'hdfs://ns1/warehouse/hudi/trips',
  rowKey_field => 'trip_id',
  partition_path_field => 'ds'
);
```

By default this uses the METADATA_ONLY selector; pass `selector_class => 'org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector'` for a full-record bootstrap.

A third option worth knowing: Hudi does not require converting the entire table at once. Because the lowest granularity Hudi manages is a Hive partition, an append-only table can simply start writing *new* partitions through Hudi while old partitions stay as-is — with the caveat that upserts and incremental pulls do not work on the unconverted partitions.

## Path B: Rewrite via Spark

For small and medium tables — or whenever you want a clean, fully Hudi-managed copy with per-record ordering applied — a straight rewrite is the simplest path. Since the source is already a metastore table, Spark SQL can read it directly:

```sql
-- define the Hudi table with key, ordering and partitioning mapped from the Hive schema
CREATE TABLE hudi_trips (
  trip_id STRING,
  rider STRING,
  fare DOUBLE,
  updated_at BIGINT,
  ds STRING
) USING HUDI
TBLPROPERTIES (
  primaryKey = 'trip_id',
  orderingFields = 'updated_at'
)
PARTITIONED BY (ds)
LOCATION 'hdfs://ns1/warehouse/hudi/trips';

-- use bulk_insert for the one-time load: fastest, skips upsert-path indexing work
SET hoodie.spark.sql.insert.into.operation = bulk_insert;

INSERT INTO hudi_trips
SELECT trip_id, rider, fare, updated_at, ds FROM mydb.trips;
```

The `bulk_insert` operation is the right choice for the initial load — it is optimized for large one-time writes and lets Hudi apply its file-sizing configs so the new table starts life with well-sized files rather than inheriting whatever small-file sprawl the Hive table had. The equivalent DataFrame API flow (read the Hive table with `spark.table(...)`, write with `format("hudi")`) is shown in the [quick start guide](/docs/quick-start-guide). For very large tables where a single job is impractical, you can loop partition by partition, reading each partition path and appending it to the Hudi base path, as described in the [migration guide](/docs/migration_guide).

## Re-Pointing the Metastore

The step that makes the migration invisible to downstream users is [syncing the Hudi table to the Hive metastore](/docs/syncing_metastore). Hudi's Hive sync registers the table (schema, partitions, location, input format) in the metastore and keeps it current as new commits add columns or partitions.

The easiest way is to enable sync on the writer itself, so every write keeps the catalog fresh:

```
hoodie.datasource.meta.sync.enable=true
hoodie.datasource.hive_sync.mode=hms
hoodie.datasource.hive_sync.metastore.uris=thrift://hive-metastore:9083
hoodie.datasource.hive_sync.database=mydb
hoodie.datasource.hive_sync.table=trips
```

Alternatively, run the standalone `HiveSyncTool` from the command line for a one-shot or externally scheduled sync:

```bash
cd hudi-hive
./run_sync_tool.sh --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive --pass hive \
  --partitioned-by ds --base-path hdfs://ns1/warehouse/hudi/trips \
  --database mydb --table trips
```

Sync supports three modes — `hms` (metastore thrift APIs, recommended), `jdbc` (via HiveServer2) and `hiveql` — detailed in the [sync doc](/docs/syncing_metastore). A few migration-specific notes:

- **Partition values** are extracted by `hoodie.datasource.hive_sync.partition_value_extractor`. The default `MultiPartKeysValueExtractor` handles multi-level partitions split on `/`; hive-style partitioned and non-partitioned tables get `HiveStylePartitionValueExtractor` and `NonPartitionedExtractor` inferred automatically.
- **CoW tables** appear as a single table under the same name your users expect — one metastore entry, drop-in replacement.
- **MoR tables** sync as two views: `<table>_ro` (read-optimized — queries only the compacted columnar base files) and `<table>_rt` (real-time/snapshot — merges in the latest log files for the freshest data). Point latency-sensitive dashboards at `_ro` and freshness-sensitive consumers at `_rt`. If you migrate a Hive table to MoR, plan for downstream queries to pick one of the two names.
- If you sync to AWS Glue instead of a self-managed metastore, the flow is the same with a Glue-specific sync client — see [Syncing to AWS Glue Data Catalog](/docs/syncing_aws_glue_data_catalog).

## Validating with Your Existing Queries

Before flipping any traffic, verify the migrated table from every engine you actually use:

**Row counts and spot checks.** Compare `SELECT COUNT(*)` between old and new, overall and per partition. Spot-check a sample of records by key. Note that Hudi adds meta columns (`_hoodie_commit_time`, `_hoodie_record_key`, etc.) — `SELECT *` outputs will include them, so audit any consumers doing positional column access.

**Hive.** Query through beeline; ensure the `hudi-hadoop-mr-bundle` jar is available to Hive (e.g. in its `auxlib/` directory) and set the input format so splits are computed correctly:

```
beeline -u jdbc:hive2://hiveserver:10000/mydb \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat
```

**Spark and Trino/Presto.** These engines have native Hudi support and resolve the synced table straight from the metastore — run your representative dashboards and pipeline queries against the new table name. For MoR, run them against both `_ro` and `_rt` so you understand the freshness difference before choosing defaults.

**Exercise the new capabilities.** Do a small `UPDATE`/`DELETE` or an upsert write, confirm it lands atomically, and try an incremental query — this is, after all, why you migrated.

## Decommissioning the Old Table Safely

Once validation passes and writers have been cut over to Hudi, retire the old table deliberately:

1. **Freeze writes to the old Hive table first.** Run the final bootstrap or rewrite *after* the freeze so no records land in the old location and get missed. (For rewrites you can also do an initial bulk load, then a short catch-up pass for late data before cutover.)
2. **Repoint consumers, then watch.** Keep the old table readable during a burn-in period while dashboards and pipelines run against the Hudi table.
3. **Mind the METADATA_ONLY dependency.** If you bootstrapped with METADATA_ONLY, the Hudi table still references the original data files — deleting or modifying them causes data loss or corruption. Either keep the original files permanently (drop only the old metastore *entry*, not the data), or convert to fully Hudi-managed files before deleting anything.
4. **Drop the metastore entry last.** For an external Hive table, `DROP TABLE` removes only metadata; delete the underlying directory separately, and only when rule 3 allows.

## Conclusion

Migrating from Hive to Hudi is less a leap than a re-registration: the files stay open columnar, the catalog stays the metastore, the engines stay Spark, Hive, Trino and Presto. What changes is the table layer in between — and with it, everything a plain Hive table couldn't do. Choose in-place [bootstrap](/docs/migration_guide) (METADATA_ONLY, FULL_RECORD, or a regex-selected mix) for large tables, a `bulk_insert` rewrite for smaller ones, wire up [metastore sync](/docs/syncing_metastore) so the table appears exactly where your users already look, validate from every engine, and decommission the old table with the METADATA_ONLY caveat in mind. From that point on, your formerly static Hive table takes upserts, deletes, ACID commits and incremental reads like any other Hudi table.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Can Hive still query a table after it\'s migrated to Hudi?', answer: 'Yes. Hudi\'s Hive sync registers the table in the Hive metastore with Hudi\'s input format, and Hive queries it through the hudi-hadoop-mr bundle. Spark, Presto and Trino also query the synced table through their native Hudi integrations, so existing SQL keeps working.'},
  {question: 'Do I need to stop writes during the migration?', answer: 'Only briefly. Freeze writes to the old Hive table, run the final bootstrap or catch-up rewrite pass, then cut writers over to the Hudi table. For rewrite migrations you can bulk load most of the data ahead of time and keep the write freeze down to a short catch-up window.'},
  {question: 'What happens to my Hive partitions?', answer: 'They map directly. Hudi preserves hive style key=value partition directories when hive_style_partitioning is enabled, multi level partitions map to a comma separated partition path field list, and Hive sync re-registers all partitions in the metastore using a partition value extractor that is inferred automatically in common cases.'},
  {question: 'Should I use METADATA_ONLY or FULL_RECORD bootstrap?', answer: 'METADATA_ONLY writes only skeleton files with record keys and avoids rewriting data, making it ideal for large cold partitions, but the original files must never be deleted or modified. FULL_RECORD copies data into fully Hudi managed files, equivalent to a bulk insert. A common pattern applies FULL_RECORD to recent hot partitions and METADATA_ONLY to the rest using a regex mode selector.'},
  {question: 'What are the _ro and _rt tables that appear after migrating to Merge-on-Read?', answer: 'For a Merge-on-Read table, Hive sync registers two views in the metastore: a read optimized view suffixed _ro that queries only compacted columnar base files, and a real time view suffixed _rt that merges in the latest log files for the freshest data. Copy-on-Write tables sync as a single table under the original name.'},
  {question: 'Can I migrate only part of a Hive table to Hudi?', answer: 'Yes. Because Hudi manages data at Hive partition granularity, an append only table can start writing new partitions through Hudi while historical partitions stay untouched. The caveat is that upserts and incremental queries only work on the Hudi managed partitions, so update heavy tables should be converted fully.'},
]} />
