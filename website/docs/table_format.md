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
| [Native format](#native-format)                          | `native` (default)    | Built into Hudi                                                   |
| [Apache Iceberg](#apache-iceberg), as a pluggable format | `ICEBERG`             | [Apache XTable (incubating)](https://xtable.apache.org/), since 0.5.0 |

## File Formats

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
See [Base Files](#base-files) and [Log Files](#log-files) for how the native format uses them.

## Native Format

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
[RFC-93](https://github.com/apache/hudi/blob/master/rfc/rfc-93/rfc-93.md)). Hudi still organizes the data into file groups and
records its actions on the [timeline](timeline.md). When a write, clustering, clean, rollback or savepoint completes on the
timeline, Hudi calls the plugin, which records the action in the metadata of the external format. The plugin's timeline reports
an action as complete only after both steps, so Hudi jobs that have the plugin and readers of the external format see the same
table.

A plugin implements the `HoodieTableFormat` interface and is loaded from the classpath by the name in `hoodie.table.format`,
which is case-sensitive.

:::caution
When no plugin on the classpath matches `hoodie.table.format`, Hudi does not fail and uses the native format. Commits written
this way are not recorded in the external format, and a Hudi reader without the plugin can see commits that the external format
does not have. Put the plugin on the classpath of every job that writes the table, including table services that run on their
own, and of every Hudi reader.
:::

### Apache Iceberg

With the Iceberg plugin, a Hudi write also commits an Iceberg snapshot that follows the
[Iceberg table spec](https://iceberg.apache.org/spec/), so an Iceberg engine that reads the table by path sees the write as
soon as it completes, with no separate sync job and no second copy of the data. [Apache XTable (incubating)](https://xtable.apache.org/)
provides the plugin. See [Write Hudi tables as Iceberg tables](https://xtable.apache.org/docs/how-to-iceberg-table-format) in
the XTable docs for the jars and settings.

The Iceberg metadata is stored under `metadata/` in the base path and refers to the same Parquet base files that Hudi writes.
The plugin does not update a catalog: to read through one, register the latest `metadata.json` file again after new writes.
Hudi actions map to Iceberg as follows.

| Hudi action                   | Iceberg metadata                                                                                         |
|:------------------------------|:---------------------------------------------------------------------------------------------------------|
| Write, [clustering](clustering.md) | A snapshot that adds the new data files and removes the ones they replace                           |
| [Clean](cleaning.md), savepoint | A snapshot with the same data files as the one before it                                               |
| [Rollback](rollbacks.md)      | A new snapshot that removes the rolled back files and brings back the previous ones                      |
| Archival                      | The snapshots of the archived instants are expired                                                       |

The Iceberg metadata is the source of truth. If a Hudi commit has no Iceberg snapshot, the commit stays pending and the next
write rolls it back. If the Iceberg commit fails, the Hudi operation fails too. Snapshots are expired at archival, while a
clean deletes replaced files earlier, so Iceberg time travel to a snapshot whose files were cleaned fails.

The plugin in XTable 0.5.0 requires Hudi 1.2.x and has the following limitations.

* Only [copy-on-write](table_types.md#copy-on-write-table) tables are supported.
* The Hudi [metadata table](metadata.md) must be off (`hoodie.metadata.enable=false`) on every write, so the indexes stored in
  it are not available.
* Only identity partitioning is supported.
* Restore is not supported.
* Only one job can write the table at a time, including table services. All writes must go through Hudi; other engines can
  only read the table as Iceberg.
* The Iceberg table uses format version 2.
* The plugin does not add field IDs to the Parquet files, which some engines such as BigQuery and Snowflake require.

See the [XTable docs](https://xtable.apache.org/docs/how-to-iceberg-table-format#limitations) for the full list. To add Iceberg
metadata to an existing native table instead, see [syncing with XTable](syncing_xtable.md).
