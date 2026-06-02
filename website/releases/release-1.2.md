---
title: "Release 1.2"
layout: releases
toc: true
last_modified_at: 2026-06-02T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page contains release notes for all Apache Hudi 1.2.x releases, including:

- [Release 1.2.0](#release-120)

---

## [Release 1.2.0](https://github.com/apache/hudi/releases/tag/release-1.2.0)

Apache Hudi 1.2.0 is a major release that opens the lakehouse to AI/ML workloads while continuing to strengthen Hudi's core ingestion, indexing, and table-services story. New native types (VECTOR, VARIANT, BLOB), Spark SQL functions for vector search and BLOB materialization, and Lance file format support let embeddings, semi-structured payloads, and binary objects live alongside structured data. On the streaming side, Flink gains full Record Level Index support and an opt-in FLIP-27 source with stronger push-down.

## Highlights

- **Unstructured data types**: Three new logical types (VECTOR, VARIANT, BLOB) and two Spark SQL functions `read_blob()`, `hudi_vector_search()` bring embeddings, semi-structured payloads, and binary objects into Hudi as first-class data.
- **Lance File Format Support**: Native read/write integration with the Lance columnar format, optimized for AI/ML access patterns.
- **Flink Record-Level Index**: Streaming writers gain full RLI support (RECORD_LEVEL_INDEX, GLOBAL_RECORD_LEVEL_INDEX) with optional RocksDB-backed bootstrap for high throughput long-running pipelines.
- **Flink Source V2 (FLIP-27)**: New opt-in source with resumable split assignment, checkpoint alignment, and stronger predicate / partition / LIMIT push-down.
- **Azure Storage-Based Lock Provider**: Multi-writer concurrency on ADLS Gen2 and Azure Blob Storage with no external dependencies (ZooKeeper, HMS).

---

## Migration Guide

This release keeps the same table version (`9`) as [1.1.1 release](/releases/release-1.1#release-111), and there is no need for a table version upgrade if you are upgrading from 1.1.1. There are a few [module and API changes](#api-changes--deprecations) and [behavior changes](#breaking-changes) as described below, and users are expected to take action accordingly before using 1.2.0 release.

:::caution
If migrating from an older release (pre-1.1.1), please also check the upgrade instructions from each older release in sequence.
:::

---

## Bundle Updates

- Support for Spark 4.1 bundles are added in Hudi 1.2.0.
- Support for Flink 2.1 bundles are added.

---

## New Features

### Unstructured Storage for AI/ML

Hudi 1.2.0 expands the lakehouse from purely structured tabular data into the data shapes that AI/ML workloads actually use: high-dimensional embeddings, semi-structured event payloads, and raw binary objects like images and audio. Three new logical types (VECTOR, VARIANT, BLOB), two new Spark SQL functions (`read_blob()` and `hudi_vector_search()`), and integration with the Lance file format land together so these workloads can live next to the rest of your data, with no separate vector database, document store, or object-tracking layer required.

#### New Logical Types

##### VECTOR

Embeddings are how AI/ML systems represent meaning, and almost every retrieval-augmented generation (RAG) or similarity-search workload depends on them. Most lakehouse formats treat embeddings as generic arrays, which means every reader has to revalidate dimension and element type, and storage-format-level optimizations like vector indices have nothing reliable to bind to.

Hudi 1.2.0 introduces `VECTOR` as a first-class logical type, parameterized by a fixed dimension and an element type (`FLOAT`, `DOUBLE`, or `INT8`). On Parquet, VECTOR is stored as a `FIXED_LEN_BYTE_ARRAY` annotated with `hudi_type`; on Lance, it maps natively to Lance's `FixedSizeList`.

```sql
CREATE TABLE products (
  id        STRING,
  name      STRING,
  embedding VECTOR(768)
) USING hudi
TBLPROPERTIES (primaryKey = 'id');
```

Engine support: Spark (Spark 3.4 and greater). VECTOR must be a top-level column. Nesting inside STRUCT, ARRAY, or MAP is not supported, and dimension and element type cannot be changed after table creation.

##### VARIANT

Many AI/ML and observability workloads (tool-call traces, event logs, request/response payloads) produce heterogeneous JSON-like documents where the schema varies row to row but a handful of fields are queried hot. Forcing this data into a strict schema is fragile; storing it as a string column kills query performance.

Hudi 1.2.0 introduces a `VARIANT` logical type that stores semi-structured data as a binary-encoded value payload plus a key-dictionary metadata blob. The binary encoding preserves the original document structure and types without requiring a fixed schema, so each row can carry a different shape.

```sql
-- Spark 4.0+: native VARIANT type
CREATE TABLE event_log (
  event_id STRING,
  payload  VARIANT
) USING hudi
TBLPROPERTIES (primaryKey = 'event_id');
```

Engine support: native `VARIANT` keyword on Spark 4.0+; on Spark 3.x, VARIANT surfaces as `STRUCT<metadata: BINARY, value: BINARY>` and remains readable. On Flink, the column is accessible as `ROW<metadata BYTES, value BYTES>` but cannot be decoded natively. VARIANT is not supported on Lance-backed tables. Use Parquet for tables that contain VARIANT columns.

**Limitations as of 1.2.0**: Shredding is not supported in this release. Hudi 1.2.0 stores the entire VARIANT as the binary value payload plus its key-dictionary metadata; frequently accessed fields are not projected into native typed columns. As a result, predicates and projections over fields inside a VARIANT are not pushed down at the column level. A query that filters on a nested field (for example `WHERE tool_calls[0].name = 'search'`) reads and decodes the binary payload rather than skipping it via a shredded column. Shredding and column-level pushdown for shredded fields is planned for a future release.

##### BLOB

Images, PDFs, audio, video, and model artifacts are increasingly first-class inputs and outputs of data pipelines, but storing them in a separate object store and joining by path is awkward, error-prone, and breaks transactional guarantees. Putting raw bytes into a `BINARY` column works for small payloads but bloats every scan that doesn't need the bytes.

Hudi 1.2.0 introduces a `BLOB` logical type that carries a storage mode (`INLINE` or `OUT_OF_LINE`), an inline `data` field for small payloads, and an out-of-line `reference` carrying `(external_path, offset, length, managed)`. Readers return blob *references* lazily by default, deferring byte materialization until a query explicitly asks for it via `read_blob()` (see below). This means a shuffle-heavy query that joins or filters on metadata never has to drag gigabytes of pixel data through stages that don't touch it.

```sql
CREATE TABLE media_assets (
  asset_id STRING,
  url      STRING,
  content  BLOB
) USING hudi
TBLPROPERTIES (primaryKey = 'asset_id');
```

The default read mode for inline BLOBs is controlled by `hoodie.read.blob.inline.mode` (defaults to `DESCRIPTOR`; set to `CONTENT` to materialize inline bytes on every scan). Engine support: Spark for read and write. Hive and BigQuery sync map the BLOB struct to the catalog's native struct type, but `read_blob()` is a Spark SQL function only.

#### New Spark SQL Functions

##### `read_blob()`

By default, BLOB scans return descriptors rather than bytes, which keeps queries that filter or join on metadata cheap. To actually pull the bytes — for downstream model inference, transcoding, or serving — call `read_blob()`:

```sql
SELECT id, url, read_blob(content) AS image_bytes
FROM media_assets
WHERE category = 'product_image';
```

`read_blob()` materializes the underlying bytes in a single pass, transparently handling both inline reads and external calls for out-of-line references. It is governed by `hoodie.read.blob.inline.mode`: under the default `DESCRIPTOR` mode, `read_blob()` on an `INLINE` BLOB returns a descriptor rather than the raw bytes, so set this config to `CONTENT` if you need eager byte materialization on every read.

##### `hudi_vector_search()`

Once you have a VECTOR column populated with embeddings, you typically want to ask "given a query vector, find the k nearest rows." Most data teams reach for a separate vector database for this, which means duplicating data and reconciling it across two systems.

`hudi_vector_search()` is a Spark table-valued function that does k-nearest-neighbor search directly over a Hudi table. It returns the matching rows along with a synthesized `_hudi_distance` column, so retrieval logic and downstream business rules can live in the same SQL query:

```sql
SELECT *
FROM hudi_vector_search(
  table           => 'products',
  embedding_col   => 'embedding',
  query_vector    => ARRAY(0.12F, -0.03F, 0.81F, ...),
  k               => 10,
  distance_metric => 'cosine'
)
WHERE in_stock = true
ORDER BY _hudi_distance;
```

Supported distance metrics include `cosine`, `dot_product`, and `euclidean`. A batch form of the function takes a `base_table` / `query_table` pair and returns, for each row in the base table, the top-k nearest rows from the query table — useful for bulk recommendation, deduplication, and offline retrieval-evaluation jobs.

The current implementation is a distributed brute-force scan and assumes embeddings are already populated. A native ANN index in Hudi is planned in a follow-up release.

#### Lance File Format

The new logical types are useful on Parquet, but vector and blob workloads have access patterns that columnar formats designed for analytical scans don't optimize for. [Lance](https://lancedb.github.io/lance/) is a modern columnar format built around fast random access and efficient encoding of large fixed-size arrays.

Hudi 1.2.0 adds Lance as a supported base-file format alongside Parquet, ORC, and HFile. VECTOR columns on Lance use Lance's native `FixedSizeList<Float32/Float64, dim>` encoding. BLOB columns on Lance use Lance's native blob-encoding and integrate with the `DESCRIPTOR` read mode so byte materialization stays deferred.

```sql
CREATE TABLE ai_corpus (
  id        STRING,
  embedding VECTOR(768),
  metadata  STRING
) USING hudi
TBLPROPERTIES (
  primaryKey = 'id',
  hoodie.table.base.file.format = 'lance',
  hoodie.write.record.merge.custom.implementation.classes = 'org.apache.hudi.DefaultSparkRecordMerger'
);
```

Required dependency: the Lance JAR is not bundled in the Hudi distribution. Add the Lance Spark bundle matching your Spark version (`org.lance:lance-spark-bundle-3.5_2.12:0.4.0`) to your classpath.

Engine support: **Spark only**. Other notes: `FLOAT → DOUBLE` and `FLOAT → STRING` type promotions are not supported on Lance (use `DOUBLE` from the start if you anticipate needing higher precision); VARIANT columns are not supported on Lance; column stats and partition stats are automatically disabled on Lance tables.

For a hands-on walkthrough, see the [Unstructured Data Quick Start Guide](/docs/next/unstructured-data-quick-start-guide). For the underlying specifications, see [RFC-99](https://github.com/apache/hudi/blob/master/rfc/rfc-99/rfc-99.md) (Type system and Variant), [RFC-100](https://github.com/apache/hudi/blob/master/rfc/rfc-100/rfc-100.md) (BLOB storage and access), and [RFC-102](https://github.com/apache/hudi/blob/master/rfc/rfc-102/rfc-102.md) (vector search).

### Writers and Indexes

Writers and indexing see broad improvements in 1.2.0. Flink streaming writers gain full Record-Level Index support with multiple deployment modes, a new rolling-metadata mechanism carries forward arbitrary commit-metadata keys across writes, a pluggable Parquet config injector lets users tune writer properties without modifying Hudi, and disabled MDT partitions are now auto-cleaned by default.

#### Flink Record-Level Index

Flink streaming writers can now use the Record Level Index for bucket assignment, in addition to the existing `FLINK_STATE` and `BUCKET` indexes. Two flavors are available:

- `RECORD_LEVEL_INDEX`: partitioned RLI; uniqueness enforced per (partition_path, record_key)
- `GLOBAL_RECORD_LEVEL_INDEX`: global RLI; uniqueness enforced across the whole table

Both require `metadata.enabled=true`; the global variant additionally needs `index.global.enabled=true`.

**Global RLI with local RocksDB cache.** For long-running streaming jobs, Flink can bootstrap the global RLI from the metadata table into a task-local RocksDB cache via `index.bootstrap.enabled=true` and `index.bootstrap.rocksdb.path=<local-path>`. This reduces repeated MDT lookups during bucket assignment at the cost of local disk and bootstrap time.

**Dynamic bucket scaling with partitioned RLI.** Partitioned RLI tracks record-to-file-group routing per partition, allowing bucket counts to evolve as partitions grow. Tune `hoodie.metadata.record.level.index.min.filegroup.count` (default 1) and `hoodie.metadata.record.level.index.max.filegroup.count` (default 10). Pairs well with bucket pruning and split-distribution improvements on the read side.

For more details, see [Flink Index Configs](/docs/indexes#flink-based-configs).

#### Rolling Extra Metadata

A new mechanism to carry forward selected commit-metadata keys (e.g. Kafka offsets, Flink checkpoint IDs) to every subsequent commit and clean instant, so downstream consumers can read them off the latest commit without walking the timeline. Configure via `hoodie.write.rolling.metadata.keys` (comma-separated); tune `hoodie.write.rolling.metadata.timeline.lookback.commits` (default 10) for resilience.

#### Parquet Config Injector

A pluggable extension point, `hoodie.parquet.write.config.injector.class`, lets users inject custom Parquet writer properties (e.g. dictionary encoding flags, bloom filter sizing, page sizes) without modifying Hudi. Implementations must implement `org.apache.hudi.io.HoodieParquetConfigInjector`.

#### Auto-Deletion of Disabled MDT Partitions

When an index is removed from the write config, Hudi now automatically drops the corresponding metadata-table partition rather than leaving it stale. Set `hoodie.metadata.auto.delete.partitions=false` to require explicit `DROP INDEX` / CLI cleanup. Recommended in multi-writer environments where writer configs may be heterogeneous, to avoid one writer dropping an index that another still uses.

### Concurrency Control

Concurrency Control sees a focused set of improvements in 1.2.0. The storage-based lock provider now supports Azure (alongside S3 and GCS), optional audit logging makes lock acquisition and release events debuggable after the fact, and clustering can yield to in-flight ingestion under the prefer-writer strategy.

#### Storage-Based Lock Provider

##### Lock Audit Logging

Optional audit logging records lock acquisition and release events (written to a `.hoodie/lock/audit_enabled.json` marker plus an audit log) for post-hoc debugging of multi-writer conflicts.

##### Azure Storage-Based Lock Provider

The storage-based lock provider now supports Azure base paths (`abfs://`, `abfss://`, `wasb://`, `wasbs://`), bringing multi-writer support to Azure-native deployments without ZooKeeper or HMS. To enable, add `hudi-azure-bundle` to the classpath and set `hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.StorageBasedLockProvider` in the writer properties. Authentication supports connection string, SAS token, managed identity, service principal, and the `DefaultAzureCredential` chain, resolved in that precedence order. For more details, see [Azure Storage-Based Lock Provider](/docs/concurrency_control#azure-storage-based-lock).

#### Clustering Aborts on Pending Ingestion

When using `PreferWriterConflictResolutionStrategy` as the conflict resolution strategy for clustering, clustering commits can now self-abort if a heartbeat-active ingestion write is in `.requested` state but has not yet transitioned to `.inflight`. This gives ingestion precedence over clustering and avoids late-arriving conflicts.

### Table Services

This release adds a substantial set of features in this domain, strengthening Hudi's vision of a self-managed lakehouse platform. Cleaning gains pre-write hooks, partition filtering, and bounded clean runs. Clustering picks up new plan strategies, finer-grained group controls, driver-local planning, and stale-instant cleanup. The metadata table compactor reaches feature parity with the data table compactor on trigger strategies, and archival gets a manifest retention knob.

#### Cleaning

##### Pre-Write Cleaner Policy

`hoodie.prewrite.cleaner.policy` lets you force a clean (or rollback of failed writes) before a write starts, instead of after. Values are `NONE` (default), `CLEAN`, and `ROLLBACK_FAILED_WRITES`. Useful in multi-writer pipelines where you want a deterministic table state at the start of every write, or where a writer keeps crashing before reaching the post-commit clean.

##### Max Commits to Clean per Run

For large-scale deployments, `hoodie.clean.max.commits.to.clean` (default unbounded) caps how many commits are cleaned in a single clean instant under `KEEP_LATEST_COMMITS` or `KEEP_LATEST_BY_HOURS`. Useful for bounding clean-job duration on tables that have fallen significantly behind.

##### Empty Clean Commits for Append-Only Tables

On append-only tables the cleaner's `earliest_commit_to_retain` pointer never advances, forcing a full-history scan on every run and high clean-planning latencies. Hudi 1.2.0 introduces `hoodie.write.empty.clean.interval.hours` (default `-1`, disabled), which emits an empty clean at the configured interval to advance the pointer so subsequent cleans only scan recent partitions.

##### Full-Clean Partition Filtering

When the optimization above is not enabled, Hudi has to plan a clean across all partitions in the table. For tables with thousands of partitions this can cause OOMs at planning time. Users can now configure `hoodie.clean.partition.filter.regex` (Java regex) and `hoodie.clean.partition.filter.selected` (explicit list, takes precedence) to scope cleaning to a subset of partitions when incremental cleaning is disabled.

##### MDT Cleaner Inherits Data-Table Policy

The metadata table cleaner now derives its retention configuration dynamically from the data table cleaner. `hoodie.metadata.derive.from.datatable.clean.policy` (default `true`) governs this behavior. Set to `false` to retain the prior behavior, where the MDT cleaner used its own independent retention configuration.

#### Compaction

##### Metadata Table Compaction Trigger Strategy

The MDT compactor now supports the same trigger-strategy options as the data table compactor: `NUM_COMMITS`, `NUM_COMMITS_AFTER_LAST_REQUEST`, `TIME_ELAPSED`, `NUM_AND_TIME`, and `NUM_OR_TIME`. Configure via `hoodie.metadata.compact.trigger.strategy` (default `NUM_COMMITS`), with `hoodie.metadata.compact.max.delta.commits` (default 10) and `hoodie.metadata.compact.max.delta.seconds` (default 7200) controlling the thresholds.

#### Clustering

Clustering in Hudi 1.2.0 gets a substantial refresh: new plan strategies for commit-based and binary-stitched coalescing, finer-grained controls over group composition and ordering, and a driver-local planning path for sparse plans. Stale-instant cleanup and a Flink single-file skip strategy also land, cutting wasted work in long-running deployments.

##### CommitBasedClusteringPlanStrategy

A new clustering plan strategy, `org.apache.hudi.table.action.cluster.strategy.CommitBasedClusteringPlanStrategy`, groups file slices by the commits that produced them rather than purely by size. Use `hoodie.clustering.plan.strategy.earliest.commit.to.cluster` to incrementally cluster only newer commits while skipping already-clustered history.

##### SparkStreamCopyClusteringPlanStrategy

Building on the binary stream-copy execution strategy introduced in 1.1.1, Hudi 1.2.0 adds `SparkStreamCopyClusteringPlanStrategy`, a schema-aware planner that groups file slices by schema hash so each binary-stitch group is guaranteed schema-compatible. Toggle `hoodie.clustering.plan.strategy.binary.copy.schema.evolution.enable` to switch between strict schema-grouping (default) and size-only grouping with a runtime compatibility check. Limitations: Spark COW + Parquet only; sort columns still unsupported.

##### Single-Group Clustering Control

Users can now control whether to emit a clustering plan when only one file group is eligible. On Spark, `hoodie.clustering.plan.strategy.single.group.clustering.enabled` suppresses no-op clustering when a partition already has a single file group. On Flink, the new `org.apache.hudi.client.clustering.plan.strategy.FlinkSkipSingleFileClusteringPlanStrategy` skips file groups already consisting of a single file, eliminating unnecessary rewrites.

##### File-Slice Sort Order in Clustering Plan Generation

`hoodie.clustering.plan.strategy.file.slices.sort.by` (default `SIZE`) gives more control over which files are colocated within a clustering group. `SIZE` packs largest first; `INSTANT_TIME` packs oldest first; comma-combine for tie-breaking (e.g. `INSTANT_TIME,SIZE`).

##### Driver-Side Plan Generation for Clustering

When triggering clustering for a handful of partitions at large scale, it can be more efficient to plan on the driver than to spin up executor resources. `hoodie.clustering.plan.generation.use.local.engine.context` (default `false`) runs clustering-group computation on the driver instead of distributing across executors.

##### Automatic Expiration of Stale Clustering Instants

Hudi 1.2.0 adds automatic expiration of stale clustering plans. When `hoodie.clustering.enable.expirations=true`, the rollback-failed-writes path also rolls back `replacecommit` instants whose heartbeat has expired (older than `hoodie.clustering.expiration.threshold.mins`, default 60). Requires `hoodie.clean.failed.writes.policy=LAZY`. Removes a long-standing source of stuck clustering instants caused by driver failures.

#### Archival

##### Timeline Manifest Retention

Users can now configure the number of manifest versions to retain for the LSM timeline via `hoodie.timeline.manifest.retained.versions` (default 3).

### Spark

Spark sees a mix of platform, language, and reader improvements in 1.2.0. A full Spark 4.1 adapter and bundle ship alongside Spark 4.0; session-level Hudi options can now be set with the `spark.hoodie.*` prefix. Slash-separated date partitioning makes ingestion more flexible, and incremental queries gain column pruning push-down for I/O efficiency.

#### Spark 4.1 Support

Hudi 1.2.0 ships a full Spark 4.1 adapter and bundle (`hudi-spark4.1-bundle_2.13`), with new grammar, Avro de/serializer, Parquet reader/writer, and Catalyst extensions. Requires Java 17+ at runtime. Spark 4.0 is also supported; both 4.x bundles use Scala 2.13.

#### Session-Level Hudi Options via `spark.hoodie.*`

Any Hudi option can now be set at the Spark session level using the `spark.hoodie.*` prefix (e.g. `spark.conf.set("spark.hoodie.metadata.column.stats.enable", true)` or `--conf spark.hoodie.…`). Hudi normalizes these to `hoodie.*` at read time. Explicit data-source options or per-query `SET hoodie.X` still win. Useful for setting org-wide defaults without modifying every query.

#### Slash-Separated Date Partitioning

Hudi 1.2.0 adds support for slash-separated date partitioning. Setting `hoodie.datasource.write.slash.separated.date.partitioning=true` writes `yyyy-MM-dd` partition values as `yyyy/MM/dd` directory hierarchies (e.g. `2024/03/15` instead of `2024-03-15`). Useful for compatibility with downstream tools that expect Hive-style date hierarchies. This feature is supported only for new tables and cannot be combined with `hoodie.datasource.write.hive_style_partitioning=true`. Enabling it on an existing table with pre-existing flat-date partitions is not supported.

#### Incremental Query Column Pruning

Incremental relations now implement Spark's `PrunedScan` interface, allowing the planner to push column pruning into incremental reads. Reduces I/O for incremental queries that select a subset of columns.

Beyond the headline features, this release lands a broad set of stability fixes, new metrics, and additional SQL procedures and CLI commands across the Spark stack to make day-to-day operations easier.

### Flink

Flink gets one of its largest releases in 1.2.0. The headline addition is a new FLIP-27-based Source V2 with resumable splits, checkpoint alignment, and stronger push-down. Alongside it, Flink picks up Record-Level Index support (covered under [Writers and Indexes](#writers-and-indexes)), a richer set of append write-buffer modes, off-heap managed-memory buffering, RocksDB-backed lookup joins, virtual metadata columns, and Flink 2.1 bundle support.

#### Flink 2.1 Support

`hudi-flink2.1-bundle` is now published alongside 1.17–1.20 and 2.0 bundles. Flink users should run and build Hudi 1.2.0 with a Java 11+ environment (see [Breaking Changes](#breaking-changes)).

#### Flink Source V2 (FLIP-27)

New Flink source built on Flink's FLIP-27 API with static and continuous split enumerators, CDC split support, state serialization, and read-lag metrics. Enables resumable split assignment, checkpoint alignment, and stronger push-down (predicate, partition pruning, LIMIT). Opt in with `read.source-v2.enabled=true`.

:::note
Savepoints from the legacy source are not compatible with Source V2. Start fresh when switching, or use `read.start-commit` to resume from a specific instant.
:::

#### Append Write Buffer Modes

Append write workloads on Flink now have a consolidated, tunable buffering layer to improve throughput and Parquet compression. `write.buffer.type` selects the strategy:

- `NONE` — no buffering
- `BOUNDED_IN_MEMORY` — double buffer with async write
- `DISRUPTOR` — ring buffer with async write (recommended for high throughput)
- `CONTINUOUS_SORT` — TreeMap-based incremental drain for sorted output

Replaces the deprecated `write.buffer.sort.enabled` flag (see [API Changes](#api-changes--deprecations)).

#### Managed-Memory Write Buffer

Reduces GC pressure and OOM risk in containerized Flink deployments by moving the write buffer off the JVM heap. Set `write.buffer.memory.type=MANAGED` to back the buffer with Flink's managed (off-heap) memory pool; size `taskmanager.memory.managed.size` accordingly.

#### Virtual Metadata Columns

Flink queries can now access Hudi's `_hoodie_*` meta fields (commit time, commit seqno, record key, partition path, file name, and `_hoodie_operation` when changelog is enabled) without declaring them as ordinary columns. Declare them in the table DDL using Flink's standard metadata-column syntax:

```sql
_hoodie_commit_time STRING METADATA VIRTUAL
```

They become available in SELECTs while being automatically excluded from INSERTs. Useful for debugging, CDC analytics, and lineage queries from SQL.

#### Timeline-Server-Based Markers

Flink writers now support `hoodie.write.markers.type=TIMELINE_SERVER_BASED`. Strongly recommended over `DIRECT` on object stores (S3, GCS, ADLS), where directory listings used by DIRECT markers are expensive.

#### Read-Side Pruning Enhancements

Flink read queries scan less data and process fewer splits in 1.2.0. RLI-based data skipping, bucket pruning under Source V2, and a new `read.commits.limit` (complements `read.splits.limit` for tables with many small commits) land together. Existing pushdowns like predicate, partition, limit, and column-stats data skipping continue to work.

#### Lookup Join with RocksDB Cache

Lookup joins against large Hudi dimension tables are no longer bound by JVM heap. Set `lookup.join.cache.type=rocksdb` to back the cache with an embedded RocksDB instance instead of heap memory; combine with `lookup.async=true` for high-latency lookup paths.

Beyond the highlights above, the release also lands a wide set of operational fixes, checkpoint-metadata handling, table-service pipeline cleanup, compaction/clean/clustering alignment, Kafka offset tracking inside commits, and async lookup join, making long-running streaming pipelines noticeably more robust.

### Catalogs

Catalog sync gets broader coverage and better operational ergonomics in 1.2.0: a standalone Hive sync job, automatic HMS 4.x compatibility, partition change notifications, writer-version stamping, and classloader-conflict avoidance for Hive-on-Spark deployments.

#### HudiHiveSyncJob Standalone Sync

`org.apache.hudi.utilities.HudiHiveSyncJob` is a standalone Spark job that syncs a Hudi table's metadata to Hive metastore independently of any ingestion workflow, useful for backfills, manual corrections, or reconciling metastore state after direct writes. Takes the standard `hoodie.datasource.hive_sync.*` configs.

#### HMS 4.x Support via Automatic JDBC Fallback

HMS 4.x changed several Thrift API signatures (e.g., `get_table → get_table_req`), breaking the standard Thrift client. Hudi 1.2.0 detects `TApplicationException` in the cause chain and automatically routes the rest of the sync run through the JDBC path. Requires `hoodie.datasource.hive_sync.mode=jdbc` plus a valid JDBC URL; otherwise Hudi logs an explicit error and surfaces the original exception.

#### Other Sync-Time Improvements

A few additional sync-time conveniences land alongside: partition TOUCH events can be emitted via `hoodie.meta.sync.touch.partitions.enabled=true` for downstream catalog-notification systems; every sync now stamps `hudi_writer_version` on the catalog entry (HMS and Glue) so admins can identify the writing Hudi version; and `hoodie.datasource.hive_sync.use_spark_catalog=true` makes Hudi reuse Spark's Hive-enabled `IMetaStoreClient` to avoid classloader conflicts in Hive-on-Spark deployments.

### Platform Enhancements

Platform improvements in 1.2.0 span three areas: new ingestion paths, a richer pre-commit/pre-write validation framework, and a substantially expanded set of operational metrics.

#### JsonKinesisSource

`org.apache.hudi.utilities.sources.JsonKinesisSource` ingests JSON records from an AWS Kinesis Data Stream. Reads every shard in parallel, tracks per-shard progress in the Hudi Streamer checkpoint (encoded as `streamName,shardId:lastSeq,…`), automatically handles shard splits/merges, and de-aggregates KPL records. Common configs are under `hoodie.streamer.source.kinesis.*`; uses the default AWS credential chain.

#### Schema Evolution: Make All Columns Nullable

For row-based Hudi Streamer sources (SQLSource, SQLFileBasedSource), `hoodie.streamer.schema.make.columns.nullable=true` automatically makes every column in the incoming schema nullable. New columns must be nullable for existing records to remain readable, and this config automates that for smooth schema evolution.

#### Validators

The pre-commit validator framework gets three improvements that make it more flexible and engine-agnostic:

- `hoodie.precommit.validators.failure.policy` (default `FAIL`) makes failure handling configurable. `WARN_LOG` lets the commit proceed with a warning, useful for soft-monitoring rollouts of new validators.
- Flink writers now honor `hoodie.precommit.validators` using the same key as Spark. Two new built-in validators, `FlinkKafkaOffsetValidator` and `SparkKafkaOffsetValidator`, verify that records written match the expected Kafka offset delta (with `hoodie.precommit.validators.streaming.offset.tolerance.percentage`, default `0.0` for exact match).
- A new pre-write extension point (`hoodie.prewrite.validators`, implementing `org.apache.hudi.client.validator.PreWriteValidator`) lets validators reject invalid operations before any write I/O occurs. No built-ins yet; designed for custom user checks that should fail fast.

#### Metrics

A broad set of new metrics improves observability across the commit lifecycle. `clean.duration`, `archive.duration`, `rollback.failure.counter`, and `postCommit.success/failure.counter` plus `postCommit.duration` cover previously unmetered phases. A full archival metrics family lands too, making archival health debuggable from dashboards. Compaction log-block metrics are emitted when `hoodie.metrics.compaction.log.blocks.on=true`. Source V2 also emits two read-lag metrics on Flink for streaming-pipeline health. Finally, in multi-tenant Spark jobs that write to multiple Hudi tables, each table now gets its own isolated MetricRegistry scoped as `<tableName>.<registryName>` — no configuration required — eliminating cross-table metric collisions.

---

## API Changes & Deprecations

### `write.buffer.sort.enabled` → `write.buffer.type` (Flink)

The boolean `write.buffer.sort.enabled` is deprecated. Use `write.buffer.type=DISRUPTOR` for equivalent behavior, or `CONTINUOUS_SORT` for the incremental-drain variant. Both modes require `write.buffer.sort.keys` to be set.

### `hoodie.datasource.write.base.file.format` → `hoodie.table.base.file.format`

Base-file-format configuration (Parquet, ORC, HFile, Lance) now uses `hoodie.table.base.file.format`. Existing tables and writers using the older key continue to work.

### `hoodie.metadata.record.index.enable` → `hoodie.metadata.global.record.level.index.enable`

The global-RLI enable flag was renamed to disambiguate from the partitioned RLI, which has its own independent flag (`hoodie.metadata.record.level.index.enable`). Use the new names for clarity.

---

## Breaking Changes

### Java 11 is the Default Build/Runtime

Hudi now builds and runs on Java 11 by default. Flink users must run and build Hudi 1.2.0 with a Java 11-compatible environment. Spark 4.0 / 4.1 bundles additionally require Java 17+ at runtime; Spark 3.x bundles continue to support Java 8+.

### Flink Source V1 ↔ V2 Savepoints Are Not Compatible

If you opt into `read.source-v2.enabled=true`, you cannot restore from a savepoint taken with the legacy source (and vice versa). Start a fresh Flink job when switching; use `read.start-commit` if you need to resume from a specific instant.

### `HoodieSchema` Replaces `org.apache.avro.Schema` Across Core Extension APIs

Hudi 1.2.0 introduces `org.apache.hudi.common.schema.HoodieSchema` as the canonical schema abstraction, decoupling Hudi's internals from a direct Avro dependency. This change is source-breaking for any user code that extends core Hudi abstractions, including custom `HoodieRecord`, `HoodieRecordMerger`, `SchemaProvider`, transformers, or key generators. Common affected signatures: `getOrderingValue`, `getRecordKey`, `rewriteRecordWithNewSchema`, `partialMerge`, `getMandatoryFieldsForMerging`. `HoodieAvroUtils` lost ~28 public methods that migrated to `HoodieSchema`.

**Migration**: `HoodieSchema` wraps Avro Schema and is binary-compatible. Adapt call sites with `new HoodieSchema(avroSchema)` when passing into Hudi, and `hoodieSchema.getAvroSchema()` / `hoodieSchema.toAvroSchema()` when reading back. Standard DataSource / SQL / Streamer / Flink SQL users are unaffected.
