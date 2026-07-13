---
title: "Using Apache Hudi with Apache Iceberg: Interoperability via Apache XTable"
excerpt: "Choosing Apache Hudi does not mean giving up Apache Iceberg compatibility. Apache XTable translates Hudi table metadata into Iceberg in place, so one copy of data serves every engine."
description: "How Apache XTable lets Iceberg engines and catalogs read Hudi tables as Iceberg by translating table metadata in place — no data copies, no rewrites."
authors: [sivabalan]
category: how-to
image: /assets/images/hudi_stack/pluggable_tf.png
tags:
- apache xtable
- apache iceberg
- interoperability
- data lakehouse
- table format
---

Yes — Apache Iceberg engines and catalogs can read Apache Hudi tables. [Apache XTable](https://xtable.apache.org) (incubating) translates Hudi table metadata into Apache Iceberg (and Delta Lake) metadata in place, so a single copy of data on object storage is readable as any of the three formats. No data is copied or rewritten; XTable reads the Hudi table's metadata and writes out the equivalent Iceberg metadata alongside the same Parquet files.

This matters because many teams believe they face a binary choice. Their streaming ingestion, CDC pipelines, and update-heavy workloads point toward Hudi's write-side strengths, but a query engine, BI tool, or managed service in their stack only speaks Iceberg — so they conclude they must give up one to keep the other. They don't. This post explains how XTable dissolves that constraint, walks through exposing a Hudi table as Iceberg step by step, and covers the limitations you should understand before putting it in production.

## The Format Lock-In Problem

An [open table format](/blog/2026/07/14/what-is-an-open-table-format) is the metadata layer that turns Parquet files on object storage into a transactional table — the load-bearing component of the [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse) architecture. Apache Hudi, Apache Iceberg, and Delta Lake all implement this idea, and all three ultimately describe the same thing: a set of Parquet data files plus metadata recording which files form the table, what schema they follow, and how the table has changed over time.

The friction arises at the edges of the ecosystem. Some engines and managed services support only one format for external tables — most commonly Iceberg, given its broad catalog and vendor ecosystem. If your organization has standardized its serving layer on such an engine, every table you want to query there apparently needs to be an Iceberg table.

At the same time, Hudi's design center is the write path. Teams pick Hudi for fast upserts and deletes backed by [pluggable, multi-modal indexing](/docs/indexes), streaming and CDC ingestion, incremental processing, and built-in [table services](/docs/hudi_stack) — compaction, clustering, cleaning — that keep tables optimized without separate orchestration. Giving those up to satisfy a read-side format requirement is a real cost, paid on every pipeline, every day.

The choice is false because the formats differ in metadata, not data. If the Parquet files can stay put and only the metadata needs a second representation, no migration is required — just a translation. That is precisely what Apache XTable does.

## What Is Apache XTable?

[Apache XTable](https://xtable.apache.org) is an open source project incubating at the Apache Software Foundation (it was previously known as OneTable). In the project's own words, it provides "cross-table omni-directional interop between lakehouse table formats." Two properties define it:

- **It is not another table format.** XTable introduces no new specification for engines to adopt. It provides abstractions and tools for translating existing table format metadata between Apache Hudi, Apache Iceberg, and Delta Lake.
- **It translates metadata, not data.** XTable reads the existing metadata of your table and writes out metadata for one or more other table formats into the same table directory. The Parquet data files are never copied, moved, or rewritten.

Any of the three formats can act as the source, and you can target one or several formats in a single sync. For this post's scenario, the source is Hudi and the target is Iceberg — but the same mechanics work Iceberg-to-Hudi, Delta-to-Iceberg, and every other direction.

The Hudi community treats this as a first-class interoperability path: the [XTable sync documentation](/docs/syncing_xtable) covers it, and Hudi 1.1's pluggable table format work goes further by letting XTable supply format adapters inside Hudi's own write path (more on that below).

## How It Works: Metadata Translation, Not Data Movement

Conceptually, every sync run does three things:

1. **Read the source metadata.** XTable reads the Hudi table's timeline and metadata to determine the current state of the table: which data files are live, the schema, partition information, and file-level column statistics.
2. **Translate to the target model.** That state is mapped into Iceberg's metadata model — snapshots, manifests, and table metadata files describing the very same Parquet files.
3. **Write target metadata alongside.** The Iceberg metadata is written under the table's base path (in the standard `metadata/` directory Iceberg readers expect), next to Hudi's `.hoodie` directory. One directory on storage, two formats' views of it.

The translation carries more than the file list. Data files are synced along with their column-level statistics and partition metadata, so Iceberg readers keep the file-skipping benefits they expect from Iceberg's own statistics. Schema updates in the source are reflected in the target table metadata. XTable also performs format-appropriate metadata maintenance on the target — for example, snapshot expiration for Iceberg targets — so translated metadata doesn't grow without bound.

XTable supports two sync modes. **Full sync** recomputes the target metadata from the source table's current state. **Incremental sync** translates only the changes since the last sync, which is more lightweight and performs better, especially on large tables; if incremental sync can't proceed for some reason, XTable automatically falls back to a full sync. In practice you run incremental sync on a schedule (or per commit), and correctness is preserved either way.

## Hands-On: Exposing a Hudi Table as Iceberg

Suppose you have a Hudi table of orders at `s3://warehouse/orders`, written by a streaming pipeline (Hudi tables created with 0.14.0 or later are supported). Exposing it as Iceberg takes a config file and one command.

First, get the XTable utilities jar — build it from [source](https://github.com/apache/incubator-xtable) or download it from the project's GitHub packages. Then describe the translation in a YAML file:

```yaml title="my_config.yaml"
sourceFormat: HUDI
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://warehouse/orders
    tableName: orders
    partitionSpec: order_date:VALUE
```

`sourceFormat` names the format to read, `targetFormats` lists one or more formats to produce (you could add `DELTA` to the list to get both at once), and each entry under `datasets` points at a table. `partitionSpec` tells XTable how the Hudi table is partitioned so it can map partitions into Iceberg's partition spec.

Run the sync:

```shell
java -jar path/to/xtable-utilities-bundled.jar --datasetConfig my_config.yaml
```

When the run completes, the table's base path contains Iceberg metadata files — schema, commit history, partitions, and column statistics — describing the same Parquet files Hudi manages. Re-running the command picks up new Hudi commits incrementally. You can batch many tables into one config, and schedule the job with whatever orchestrator you already use.

### Continuous sync from the ingestion pipeline

If the table is fed by [Hudi Streamer](/docs/hoodie_streaming_ingestion), you can sync every commit as it lands instead of running a separate job. Add the XTable [extensions jar](https://github.com/apache/incubator-xtable/tree/main/hudi-support/extensions) to the classpath, add `org.apache.xtable.hudi.sync.OneTableSyncTool` to the pipeline's sync classes, and configure the targets:

```
hoodie.onetable.formats.to.sync=ICEBERG
hoodie.onetable.target.metadata.retention.hr=168
```

With this in place, the Iceberg view advances in lockstep with Hudi commits, and the freshness gap between the two views effectively disappears.

## Registering with Catalogs So Iceberg Engines Find the Table

Producing Iceberg metadata is half the job; Iceberg-speaking engines discover tables through a catalog, so the synced table needs to be registered. XTable's [catalog integration docs](https://xtable.apache.org/docs/catalogs-index) cover registering synced target tables with Hive Metastore, AWS Glue Data Catalog, Unity Catalog, and BigLake Metastore — registration is an explicit step after the sync. XTable also supports keeping table metadata synchronized across catalogs such as Hive Metastore and Glue.

Once registered, the table is, from the engine's point of view, simply an Iceberg table. Cloud warehouses and services that can query Iceberg tables — Snowflake, Google BigQuery, Amazon Athena and Redshift, among others — can read it through their Iceberg support, subject to each vendor's own capabilities and requirements. Distributed SQL engines like Trino and Presto query it through their Iceberg connectors. None of them need to know the table is written by Hudi.

Hudi separately ships its own [catalog sync tools](/docs/syncing_metastore) for the Hudi-native view of the table, so the same data can be discoverable both as a Hudi table (for engines with Hudi support) and as an Iceberg table (for everything else).

## What This Architecture Unlocks

Put together, the pattern looks like this: write once with Hudi, serve everywhere.

- **Ingestion** uses Hudi's strengths — record-level indexes for fast upserts and deletes, streaming and CDC ingestion, non-blocking table services, incremental pulls for downstream pipelines. See [table types](/docs/table_types) for how Copy-on-Write and Merge-on-Read trade write latency against read cost.
- **Storage** holds exactly one copy of the data, as Parquet files on object storage, with both Hudi and Iceberg metadata describing it.
- **Serving** spans every engine in the organization: Hudi-native engines read the Hudi view; Iceberg-only engines and services read the Iceberg view; nothing is exported, duplicated, or re-ingested.

There is no second pipeline to operate, no divergence between "the Hudi copy" and "the Iceberg copy," and no per-engine storage bill. Format choice stops being an organization-wide standardization battle and becomes a per-workload decision about write-path capabilities.

This direction is also converging with Hudi itself. Starting with Hudi 1.1, a [pluggable table format framework](/docs/hudi_stack) decouples Hudi's storage engine — timeline, indexes, concurrency control, table services — from the metadata format written for data files, with XTable supplying format adapters for Iceberg and Delta Lake. The trajectory is that maintaining Iceberg metadata becomes part of the Hudi write path itself, rather than a companion sync.

![Hudi storage engine with pluggable format adapters via Apache XTable](/assets/images/hudi_stack/pluggable_tf.png)
<p align = "center">Figure: Hudi's storage engine with format adapters for Iceberg and Delta Lake via Apache XTable</p>

## Honest Limitations

XTable is a pragmatic tool, and it's worth being precise about its boundaries.

- **It's read-side interoperability.** The translated Iceberg metadata gives Iceberg engines a read view of the table. Writes should continue to go through Hudi; XTable does not merge concurrent writes made independently in two formats to the same files.
- **Freshness trails the sync cadence.** The Iceberg view reflects the table as of the last sync, so it lags the latest Hudi commit by up to the sync interval. Per-commit sync via the Hudi Streamer extension closes this gap; a nightly cron does not. Decide the cadence based on how fresh the Iceberg consumers need to be.
- **Merge-on-Read log files aren't translated.** XTable syncs the underlying Parquet base files; Hudi log files (and, in the other directions, Delta deletion vectors and Iceberg delete files) are not captured. For a Hudi MoR table, the target format therefore sees the read-optimized view — records still sitting in log files become visible to Iceberg readers after compaction. Copy-on-Write tables don't have this caveat.
- **Source table requirements.** XTable requires Hudi 0.14.0 or later and depends on Hudi's metadata table; check the [XTable docs](https://xtable.apache.org/docs/features-and-limitations) for current prerequisites and configuration.
- **Feature-mapping edge cases exist.** The formats are not feature-identical, and format-specific constructs don't always have an exact counterpart on the other side (for example, Delta generated columns as a source have limited support). Each format's advanced features work fully only in that format's native engines.

None of these undermine the core pattern — most production uses are exactly "Hudi writes, Iceberg engines read" — but they should inform table type, sync cadence, and which features you rely on across the boundary.

## XTable vs Actually Migrating Formats

XTable also answers the "convert Hudi to Iceberg" question, but it's worth distinguishing two intents:

- **You need Iceberg read compatibility while keeping Hudi's write path.** Use XTable in continuous incremental sync mode, as described above. This is the common case, and nothing is migrated — both views persist indefinitely.
- **You genuinely want to switch formats.** XTable can serve as a low-risk migration mechanism: sync the table, repoint readers at the Iceberg view, validate, and only then move writers to an Iceberg-native write path — all without rewriting data files. Because the translation is omni-directional, the door swings both ways: teams migrating an Iceberg or Delta table *to* Hudi to gain its ingestion and table-service capabilities follow the same steps with source and target swapped.

The practical takeaway from the [open table format landscape](/blog/2026/07/14/what-is-an-open-table-format) applies here: pick the format whose write path fits each workload, and let interoperability handle the rest. Migration becomes a repointing exercise, not a data-movement project.

## Conclusion

The premise behind "we can't use Hudi because our engine only reads Iceberg" no longer holds. Hudi, Iceberg, and Delta Lake all describe Parquet files with metadata, and Apache XTable translates that metadata in any direction — incrementally, in place, without copying data. A table written with Hudi's indexing, streaming ingestion, and table services can be registered in a catalog and queried by any Iceberg-capable engine as an ordinary Iceberg table, while Hudi-native engines keep the full-fidelity view. With Hudi's pluggable table format framework pushing this further into the write path itself, choosing Hudi is not a bet against Iceberg — it's a way to get Hudi's write-side strengths and Iceberg's read-side reach from a single copy of your data.

To try it, start with the [Hudi XTable sync guide](/docs/syncing_xtable) and the [Apache XTable documentation](https://xtable.apache.org).

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Can Iceberg engines read Hudi tables?', answer: 'Yes. Apache XTable translates a Hudi table\'s metadata into Apache Iceberg metadata written alongside the same Parquet data files. Once that Iceberg metadata is registered in a catalog, any engine with Iceberg read support can query the table as an ordinary Iceberg table.'},
  {question: 'Can Snowflake read Hudi tables?', answer: 'Not directly, but Snowflake can query Iceberg tables, and Apache XTable can expose a Hudi table as Iceberg by translating its metadata in place. After syncing with XTable and registering the Iceberg metadata, Snowflake reads the table through its Iceberg support, subject to Snowflake\'s own Iceberg capabilities and requirements.'},
  {question: 'Does XTable copy my data?', answer: 'No. XTable translates only table metadata. It reads the source format\'s metadata and writes out the target format\'s metadata into the same table directory, while the Parquet data files stay exactly where they are. One copy of data serves all synced formats.'},
  {question: 'Can I convert a Hudi table to Iceberg?', answer: 'Yes. Running XTable with sourceFormat HUDI and targetFormats ICEBERG produces complete Iceberg metadata for the table without rewriting any data files. You can keep both views in sync continuously, or use the translation as a migration step by repointing readers and writers to the Iceberg view after validating it.'},
  {question: 'Is Apache XTable production ready?', answer: 'Apache XTable is an incubating project at the Apache Software Foundation, which is a statement about ASF governance maturity rather than code quality. It is used in real deployments and its metadata-only design is low risk since source data and metadata are never modified, but you should validate it against your own tables and review the documented limitations, such as Merge-on-Read log files not being captured, before relying on it in production.'},
  {question: 'Does XTable work with Hudi Merge-on-Read tables?', answer: 'Partially. XTable syncs the Parquet base files but not Hudi log files, so the Iceberg view of a Merge-on-Read table corresponds to the read-optimized view. Records still in log files become visible to Iceberg readers after compaction. Copy-on-Write tables do not have this caveat.'},
]} />
