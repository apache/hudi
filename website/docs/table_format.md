---
title: Table Format
summary: "In this page, we describe Hudi's native table format, the file formats it stores data in, and how Hudi writes other table formats such as Apache Iceberg."
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

A **_table format_** defines how the files on storage make up a table: how they are laid out, which of them belong to each
version of the table, and how changes are committed. Hudi keeps the table format separate from its
[storage engine](hudi_stack.md#storage-engine), so the same writers and table services can work with more than one table
format. The format is a property of the table, set with [hoodie.table.format](configurations.md#Hudi-Table-Basic-Configs) when
the table is created.

| Table format                                             | `hoodie.table.format` | Implementation                                                    |
|:---------------------------------------------------------|:----------------------|:------------------------------------------------------------------|
| [Native table format](#native-table-format)              | `native` (default)    | Built into Hudi                                                   |
| [Apache Iceberg](#apache-iceberg), as a pluggable format | `ICEBERG`             | [Apache XTable (incubating)](https://xtable.apache.org/)          |

## Native Table Format

The following describes the general organization of files in storage for a Hudi table.

* Hudi organizes data tables into a directory structure under a **_base path_** on a storage.
* Tables are optionally broken up into **_partitions_**, based on partition columns defined in the table schema.
* Within each partition, files are organized into **_file groups_**, uniquely identified by a file ID (uuid)
* Each file group contains several **_file slices_**.
* Each slice contains a **_base file_** (parquet/orc/hfile) (defined by the config - [hoodie.table.base.file.format](https://hudi.apache.org/docs/next/configurations/#hoodietablebasefileformat) ) 
  written by a commit that completed at a certain instant, along with set of **_log files_** (*.log.*) written by commits that completed 
  before the next base file's requested instant.
* Hudi employs Multiversion Concurrency Control (MVCC), where [compaction](compaction.md) action merges logs and base files to produce new
  file slices and [cleaning](cleaning.md) action gets rid of unused/older file slices to reclaim space on the file system.
* All metadata including timeline, metadata table are stored in a special `.hoodie` directory under the base path.

![file groups in a table partition](/assets/images/MOR_new.png)

Please refer the **[tech spec](https://hudi.apache.org/tech-specs#file-layout-hierarchy)** for a more detailed description of the file layouts.

### File Formats

A table format tracks files; the **_file format_** is how records are stored inside them. Hudi uses the file
formats below for base files and for the data blocks in log files. Tables with Parquet base files can be queried from engines such as Spark, Flink, Hive, Presto and Trino
(see [SQL Queries](sql_queries.md) for what each engine supports).

| File format    | Used for                                                  | Characteristics                                                               |
|:---------------|:----------------------------------------------------------|:------------------------------------------------------------------------------|
| Apache Parquet | Base files (default), data blocks in log files            | Columnar, for vectorized reads and compression                                |
| Apache ORC     | Base files                                                | Columnar, originally developed for Apache Hive                                |
| HFile          | Base files and log data blocks of the [metadata table](metadata.md) | Sorted key-value pairs, for fast lookups by key                     |
| Lance          | Base files ([Spark only](#lance-base-file-format))        | Columnar, for random access, `VECTOR` columns and AI/ML workloads             |
| Apache Avro    | Data blocks in log files                                  | Row-oriented, for fast writes                                                 |

The base file format is set per table with [hoodie.table.base.file.format](configurations.md#Hudi-Table-Basic-Configs).
See [Base Files](#base-files) and [Log Files](#log-files) below for how the native table format uses them.

### Base Files

Base files store full records, against which change records are stored in delta log files below. Hudi currently supports following 
base file formats. 

* columnar formats for vectorized reads, columnar compression and efficient column based access for analytics/data science.
* row-oriented avro files for fast scans for reading whole records.
* random access optimized HFiles for efficient searching for indexed records (based on [SSTable](https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats) format)
* [Lance](#lance-base-file-format) files for native `VECTOR` storage and AI/ML workloads (Spark-only).

![Row vs Columnar File Format](/assets/images/row-col-based-base-file-formats.png)

#### Lance base file format {#lance-base-file-format}

[Lance](https://github.com/lance-format/lance) is a pluggable base file format selected per table via
`hoodie.table.base.file.format = 'lance'`. Hudi manages the table layer
(timeline, metadata, schema, file groups, table services); Lance is the on-disk file format for
base files. Log files for MOR tables remain Avro; log compaction merges Avro logs into Lance base
files.

```sql
-- COW
CREATE TABLE my_ai_table (
    id        STRING,
    embedding VECTOR(768),
    metadata  STRING
) USING hudi
TBLPROPERTIES (
    primaryKey = 'id',
    type = 'cow',
    hoodie.record.merger.impls = 'org.apache.hudi.DefaultSparkRecordMerger',
    hoodie.table.base.file.format = 'lance'
);

-- MOR (Lance base + Avro logs)
CREATE TABLE my_ai_table_mor (
    id        STRING,
    embedding VECTOR(768),
    metadata  STRING
) USING hudi
TBLPROPERTIES (
    primaryKey = 'id',
    type = 'mor',
    hoodie.record.merger.impls = 'org.apache.hudi.DefaultSparkRecordMerger',
    hoodie.table.base.file.format = 'lance'
);
```

Hudi table services on Lance-backed tables behave as follows. Compaction merges Avro log files
into Lance base files. Clustering reorganizes records into new Lance files. Cleaning removes
obsolete Lance file slices. Bloom filter indexing is supported; column-stats and partition-stats
indices are automatically disabled for Lance base files. See [Indexes](indexes.md) for the
supported set.

Type-specific behavior on Lance:

- `VECTOR` columns are stored natively as Lance `FixedSizeList<Float32/Float64, dim>` (FLOAT or
  DOUBLE only; `INT8` is not supported on Lance and fails fast at write).
- `BLOB` columns default to `DESCRIPTOR` read mode, same as Parquet.
- `VARIANT` columns are not supported on Lance. Writing a table with a VARIANT column to Lance
  throws `HoodieNotSupportedException`. Use Parquet for VARIANT tables.
- Complex types (`STRUCT`, `ARRAY`, `MAP`) are supported as Lance columns.
- `populateMetaFields=false` is supported. User-defined key generators work normally with
  Lance-backed tables.

Lance is Spark-only. Reading a Lance-backed table from Flink, Hive, Presto, or Trino throws
`HoodieValidationException`. Lance files are also non-splittable: a single Spark task reads one
Lance base file.

The Lance JAR is not bundled in the Hudi distribution. Add the Lance Spark bundle that matches
your Spark version to the Spark classpath:

| Spark version | Bundle (Maven Central) |
|:--------------|:-----------------------|
| Spark 3.4 | `org.lance:lance-spark-bundle-3.4_2.12:0.4.0` |
| Spark 3.5 | `org.lance:lance-spark-bundle-3.5_2.12:0.4.0` |
| Spark 4.0 | `org.lance:lance-spark-bundle-4.0_2.13:0.4.0` |
| Spark 4.1 | `org.lance:lance-spark-bundle-4.1_2.13:0.4.0` |

```bash
export LANCE_BUNDLE_JAR=/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar
spark-shell --jars $HUDI_BUNDLE_JAR,$LANCE_BUNDLE_JAR
```

##### File sizing and memory

| Property | Default | Description |
|:---------|:--------|:------------|
| `hoodie.lance.max.file.size` | `125829120` (120 MiB) | Target file size in bytes for Lance base files (analogous to `hoodie.parquet.max.file.size`). |
| `hoodie.lance.write.allocator.size.bytes` | `268435456` (256 MiB) | Maximum size of the Arrow child allocator used for buffering in-flight batch data. Increase for tables with very large `BLOB` columns. |
| `hoodie.lance.write.flush.byte.watermark` | `100663296` (96 MiB) | Byte-size threshold at which the current write batch is flushed. Must be less than `hoodie.lance.write.allocator.size.bytes`. |

Arrow uses power-of-2 buffer doubling; the default 256 MiB allocator accommodates a 128 MiB doubling
step with headroom. The default 96 MiB watermark (~3/8 of the allocator cap) leaves room for offset
and validity buffers to double without exceeding the allocator limit. For tables with large `BLOB`
columns, increase the allocator and watermark together (keep watermark ≈ 3/8 of allocator).

##### Mixed-format tables

`hoodie.table.base.file.format` is set per table, so different tables in the same lakehouse can use
different base file formats (Parquet, ORC, HFile, Lance) under a shared Hudi catalog and metadata
table.

### Log Files 

Log files store incremental changes (partial or full) to a base file, such as updates, inserts, and deletes, after the base file was created. 
Log files contain different blocks (data, command, delete blocks etc.) that encode specific changes to the base file. The data block
encodes updates/inserts to the base file, with customizability to support different needs. 

* row-oriented avro files for fast/lightweight writing 
* random access optimized HFiles for efficient searching for indexed records (based on [SSTable](https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats) format)
* columnar parquet files for vectorized log merging.

### Storage Format Versioning

Elements of Hudi's storage format like log format, log block structure, timeline file/data schema are all versioned and tied to a given
table version. The **_table version_** is a monotonically increasing number that is bumped up everytime some bits produced in storage change. 

**Backwards compatible reading**: Hudi releases are backwards compatible to ensure new software releases can read recent older table versions. The recommended way to upgrade 
Hudi across different engines, is by first upgrading all readers (e.g. interactive query engines that consume tables) and then upgrading 
any/all writers and table services. Hudi storage engine also implements auto upgrade capability that can gracefully perform a table version
upgrade on the subsequent write operation, by automatically performing any necessary steps without downtime to queries/reads.

**Backwards compatible writing**: However, this may not be possible at all times given data platforms built on Hudi could have multi-stage pipelines that can act as readers and 
writers at the same time. In such cases, Hudi upgrade needs to be performed by upgrading the most downstream jobs first, tracking all the way to
the first Hudi tables written possibly by ingestion systems. To ease this process, Hudi allows also writing recent older table versions, such that
the new Hudi software binaries can first be rolled out across the entire deployment on top of the same older table version. Once all jobs and engines
have the new binary, then upgrade to newer table version can happen in any order and readers will dynamically adapt.

### Configs

The following writer configs control writing older table versions and auto upgrade behavior.

| Config Name                | Default           | Description                                                                                                                                                                               |
|----------------------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.table.version | latest (Optional) | The table version this writer is storing the table in. This should match the current table version, if table already exists. Set this to a lower version when upgrade as described above. |
| hoodie.write.auto.upgrade  | true (Optional)   | If enabled, writers automatically migrate the table to the specified write table version if the current table version is lower.                                                           |

Please refer [here](https://hudi.apache.org/docs/next/configurations/#Layout-Configs) for additional configs that control storage layout and data distribution, which defines how the files are organized within a table.

## Pluggable Table Format

Starting with Hudi 1.1, an external table format can be plugged in under the storage engine
(see the [Hudi stack](hudi_stack.md#pluggable-table-format) and
[RFC-93](https://github.com/apache/hudi/blob/master/rfc/rfc-93/rfc-93.md)). Hudi still organizes the data into file groups,
records its actions on the [timeline](timeline.md), and runs its indexes and table services. When a write, compaction,
clustering, clean, rollback or savepoint completes on the timeline, Hudi calls the plugin, which records the action in the
metadata of the external format. The plugin's timeline reports an action as complete only after both steps, so Hudi and the
readers of the external format see the same table.

A plugin implements the `HoodieTableFormat` interface and is loaded from the classpath by the name in `hoodie.table.format`.
Its settings are table properties that start with `hoodie.table.format.`: Hudi stores them in `hoodie.properties` when the
table is created, so every writer and table service uses the same ones.

:::caution
When no plugin on the classpath matches `hoodie.table.format`, Hudi fails with an error that lists the formats it found. Every
job that writes or reads the table through Hudi needs the plugin jar, including table services that run on their own.
:::

### Apache Iceberg

With the Iceberg plugin, available from Hudi 1.3, every Hudi action also commits an Iceberg snapshot that follows the
[Iceberg table spec](https://iceberg.apache.org/spec/), at format version 3 by default. Engines that read Iceberg see each write
as soon as it completes, with no sync job and no second copy of the data, while Hudi keeps its timeline,
[metadata table](metadata.md), [indexes](indexes.md) and table services. [Apache XTable (incubating)](https://xtable.apache.org/)
provides the plugin; see [Write Hudi tables as Iceberg tables](https://xtable.apache.org/docs/how-to-iceberg-table-format) in
the XTable docs for the jars to add.

| Feature                        | Support                                                                                                     |
|:-------------------------------|:------------------------------------------------------------------------------------------------------------|
| Iceberg format version         | 3 (default) or 2, chosen when the table is created                                                          |
| Copy-on-write tables           | Format version 2 or 3                                                                                       |
| Merge-on-read tables           | Format version 3. Updates and deletes are published as deletion vectors on the base files                   |
| Row lineage                    | At format version 3, a row keeps its row ID when Hudi rewrites its file or updates it                       |
| Catalogs                       | Any Iceberg catalog, such as AWS Glue, Hive Metastore or an Iceberg REST catalog, or none                   |
| Hudi metadata table and indexes | Supported. Merge-on-read tables need one of the indexes listed under [Iceberg Configs](#iceberg-configs)   |
| Partitioning                   | Partition paths that are plain column values. Timestamp-based key generators are not supported              |

Hudi actions map to Iceberg as follows.

| Hudi action                                            | Iceberg metadata                                                                        |
|:-------------------------------------------------------|:----------------------------------------------------------------------------------------|
| Write                                                  | A snapshot that adds the new data files and deletes, and removes the files they replace |
| [Compaction](compaction.md), [clustering](clustering.md) | A snapshot that replaces data files and keeps the rows the same                       |
| [Clean](cleaning.md), savepoint                        | A snapshot with the same data files as the one before it                                |
| [Rollback](rollbacks.md)                               | A new snapshot that removes the rolled back files and brings back the previous ones     |
| Archival                                               | The snapshots of the archived instants are expired                                      |

The Iceberg metadata is the source of truth. If a Hudi commit has no Iceberg snapshot, the commit stays pending and the next
write rolls it back. If the Iceberg commit fails, the Hudi operation fails too. Snapshots are expired at archival, while a
clean deletes replaced files earlier, so Iceberg time travel to a snapshot whose files were cleaned fails.

#### Iceberg Configs

These table properties are set on the first write.

| Config Name                                         | Default             | Description                                                                                                  |
|:----------------------------------------------------|:--------------------|:-------------------------------------------------------------------------------------------------------------|
| `hoodie.table.format`                               | `native`            | Set to `ICEBERG` to write the table as an Iceberg table.                                                     |
| `hoodie.table.format.iceberg.format-version`        | `3`                 | Iceberg format version of the table, `2` or `3`. Merge-on-read tables need `3`.                              |
| `hoodie.table.format.iceberg.catalog.impl`          | (none)              | Iceberg catalog class, for example `org.apache.iceberg.aws.glue.GlueCatalog`. Without a catalog, the Iceberg metadata is stored under `metadata/` in the base path. |
| `hoodie.table.format.iceberg.catalog.name`          | `iceberg`           | Name of the catalog.                                                                                         |
| `hoodie.table.format.iceberg.catalog.option.<key>`  | (none)              | Catalog properties, such as `warehouse` or `uri`.                                                            |
| `hoodie.table.format.iceberg.namespace`             | (none)              | Namespace of the table in the catalog, for example `db`. The table name is the Hudi table name.              |

The catalog can instead be set in the Hadoop configuration with `xtable.iceberg.catalog.impl`, `xtable.iceberg.catalog.name` and
`xtable.iceberg.catalog.option.<key>`, which keeps credentials out of `hoodie.properties`. It is used only when the table
properties name no catalog, and it is not stored with the table, so every job needs the same setting.

A merge-on-read table also needs `hoodie.write.updates.as.deletes.and.inserts=true` on every write. An update is then written as
a positional delete of the old row plus an insert of the new one, so the log files hold only deletes. This mode needs:

* Commit-time ordering: set no ordering field, or set `hoodie.record.merge.mode=COMMIT_TIME_ORDERING`.
* An index that supplies record positions: `GLOBAL_SIMPLE` (the Spark default for a partitioned table of a pluggable format),
  `SIMPLE`, `BLOOM`, `GLOBAL_BLOOM` or `GLOBAL_RECORD_LEVEL_INDEX`.

#### Usage

A Spark write that creates a merge-on-read table registered in AWS Glue as `db.trips`:

```scala
df.write.format("hudi")
  .option("hoodie.table.name", "trips")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "city")
  .option("hoodie.write.updates.as.deletes.and.inserts", "true")
  .option("hoodie.table.format", "ICEBERG")
  .option("hoodie.table.format.iceberg.catalog.impl", "org.apache.iceberg.aws.glue.GlueCatalog")
  .option("hoodie.table.format.iceberg.catalog.option.warehouse", "s3://example-bucket/warehouse")
  .option("hoodie.table.format.iceberg.namespace", "db")
  .mode("append")
  .save("s3://example-bucket/warehouse/db/trips")
```

Later writes, including upserts and deletes, use the same options without the `hoodie.table.format` ones, which are now stored
in the table. Hudi reads the table from its base path as usual, and Iceberg engines read `db.trips` from the catalog.

#### Limitations

* Hudi must be the only writer. Iceberg engines can read the table, but a snapshot committed by another Iceberg writer stops
  Hudi reads and writes.
* Only one job can write the table at a time, including table services.
* The table must be at table version 8 or later.
* Restore, schema evolution on read, and Iceberg branches and tags are not supported.
* On merge-on-read tables, Flink writes, partial updates and log compaction are not supported.
* Spark writes check duplicates by default on these tables: an insert fails when a record key repeats among the incoming rows,
  and a `MERGE INTO` fails when more than one source row matches the same target row.
* Engines that need field IDs in the Parquet files, such as BigQuery and Snowflake, also need the
  [XTable Hudi writer extension](https://github.com/apache/incubator-xtable/tree/main/xtable-hudi-support/xtable-hudi-support-extensions).

To add Iceberg metadata to an existing native table instead, see [syncing with XTable](syncing_xtable.md).
