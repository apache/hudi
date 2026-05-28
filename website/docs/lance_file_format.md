---
title: "Lance File Format"
keywords: [ hudi, lance, file format, vector, columnar, ANN, indexing]
summary: "Use the Lance columnar file format with Hudi for vector-friendly storage and efficient ML workloads"
toc: true
last_modified_at: 2026-05-27T00:00:00-00:00
---

[Lance](https://lancedb.github.io/lance/) is a columnar file format. Hudi 1.2.0 supports Lance as a
pluggable base file format alongside Parquet, ORC, and HFile.

:::caution Engine Support
Lance file format support is **Spark-only**. Attempting to read a Lance-backed table from Flink or Hive throws a
`HoodieValidationException`:
> Lance base file format is currently only supported with the Spark engine. Please use Parquet, ORC, or HFile
> for non-Spark engines (Flink, Hive, Presto, Trino).

The Lance JAR is **not bundled** in the Hudi distribution — you must add it to your Spark classpath
(see [Required Dependencies](#required-dependencies)).
:::

## Enabling Lance in Hudi

### Table Creation (COW)

Set the base file format to `lance` in table properties:

```sql
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
```

### Table Creation (MOR)

Lance base files work with MOR tables — Lance files act as base files while Avro log files capture
incremental changes. Log compaction merges the delta log back into Lance base files.

```sql
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

### DataFrame API

```python
(df.write
   .format("hudi")
   .option("hoodie.table.name", "my_ai_table")
   .option("hoodie.datasource.write.recordkey.field", "id")
   .option("hoodie.record.merger.impls",
           "org.apache.hudi.DefaultSparkRecordMerger")
   .option("hoodie.table.base.file.format", "lance")
   .mode("overwrite")
   .save("/path/to/my_ai_table"))
```

### Required Dependencies

The Lance JAR is not bundled in Hudi. Add the appropriate Lance Spark bundle JAR to your Spark classpath:

| Spark Version | Bundle JAR (Maven Central) |
|:--------------|:---------------------------|
| Spark 3.4 | `org.lance:lance-spark-bundle-3.4_2.12:0.4.0` |
| Spark 3.5 | `org.lance:lance-spark-bundle-3.5_2.12:0.4.0` |
| Spark 4.0 | `org.lance:lance-spark-bundle-4.0_2.13:0.4.0` |
| Spark 4.1 | `org.lance:lance-spark-bundle-4.1_2.13:0.4.0` |

```bash
export LANCE_BUNDLE_JAR=/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar

# Include both Hudi and Lance JARs
spark-shell --jars $HUDI_BUNDLE_JAR,$LANCE_BUNDLE_JAR
```

## Layering

Hudi manages the table layer (timeline, metadata, schema, file groups, table services). Lance is the
on-disk file format for base files. Log files for MOR tables remain Avro.

Table-service behavior on Lance-backed tables:

- **Compaction** — merges Avro log files into Lance base files.
- **Clustering** — reorganizes records into new Lance files.
- **Cleaning** — removes obsolete Lance file slices.
- **Metadata indexing** — bloom filter indexing is supported. Column-stats and partition-stats
  indices are automatically disabled for Lance base files.

## VECTOR Storage on Lance

VECTOR columns are stored natively in Lance as `FixedSizeList<Float32/Float64, dim>` — Lance's own
vector column encoding, so embeddings are written without conversion overhead at the file-format
layer.

Only **FLOAT** and **DOUBLE** element types are supported as VECTOR columns on Lance. INT8 vectors
are not yet supported and will fail fast at write time.

See [Vector Search](vector_search.md) for the `hudi_vector_search` TVF that queries VECTOR columns.

## BLOB Columns on Lance

INLINE BLOB columns on Lance default to `DESCRIPTOR` read mode — standard queries return an
out-of-line-shaped reference descriptor rather than materializing the raw bytes. To read inline
byte content via `read_blob()`, set `hoodie.read.blob.inline.mode=CONTENT`. See
[Unstructured Data](blob_unstructured_data.md) for full documentation.

## Schema Evolution

Lance supports the following schema changes at the Hudi layer:

| Operation | Supported? |
|:----------|:-----------|
| Add column | Yes |
| Rename column | Yes (via Hudi schema evolution) |
| Promote `FLOAT` → `DOUBLE` | **No** — not supported on Lance |
| Promote `FLOAT` → `STRING` | **No** — not supported on Lance |
| Drop column | Yes |

:::caution
`FLOAT → DOUBLE` and `FLOAT → STRING` type promotions are supported for Parquet tables but not
for Lance. Attempting these on a Lance table fails at write time.
:::

## Vector Search with Lance

Use the `hudi_vector_search` TVF to run vector similarity queries against VECTOR columns on a
Lance-backed table:

```sql
SELECT id, metadata, _hudi_distance
FROM hudi_vector_search(
    'my_ai_table', 'embedding',
    ARRAY(0.1, 0.2, ...),  -- query vector
    10, 'cosine'
)
ORDER BY _hudi_distance;
```

See [Vector Search](vector_search.md) for full documentation on the TVF and distance metrics.

## Configuration Reference

| Property | Default | Description |
|:---------|:--------|:------------|
| `hoodie.table.base.file.format` | `parquet` | Set to `lance` to use Lance as the base file format. |
| `hoodie.record.merger.impls` | — | Must be `org.apache.hudi.DefaultSparkRecordMerger` for Lance. |
| `hoodie.lance.max.file.size` | `125829120` (120 MiB) | Target file size in bytes for Lance base files. |
| `hoodie.lance.write.allocator.size.bytes` | `268435456` (256 MiB) | Maximum size of the Arrow child allocator used for buffering in-flight batch data. Increase for tables with very large BLOB columns. |
| `hoodie.lance.write.flush.byte.watermark` | `100663296` (96 MiB) | Byte-size threshold at which the current write batch is flushed. Must be less than `hoodie.lance.write.allocator.size.bytes`. |

### File Sizing and Memory

The three sizing configs work together:

- **`hoodie.lance.max.file.size`** controls when Hudi rolls over to a new Lance file, similar to
  `hoodie.parquet.max.file.size` for Parquet tables.
- **`hoodie.lance.write.allocator.size.bytes`** caps the Arrow allocator's in-flight memory. Arrow
  uses power-of-2 buffer doubling; the default 256 MiB accommodates the 128 MiB doubling step with
  headroom.
- **`hoodie.lance.write.flush.byte.watermark`** triggers an early batch flush when Arrow buffers
  approach the cap. The default 96 MiB (≈ 3/8 of the allocator cap) leaves room for offset and
  validity buffers to double without exceeding the allocator limit.

For tables with large BLOB columns, increase `hoodie.lance.write.allocator.size.bytes` and
`hoodie.lance.write.flush.byte.watermark` together (keep watermark at roughly 3/8 of allocator size
so Arrow's power-of-2 doubling stays within the allocator cap).

## Additional Notes

- **`populateMetaFields=false`** is supported. User-defined key generators work normally with Lance
  tables.
- **Complex types** (struct, array, map) are supported as Lance columns.
- **VARIANT columns** are **not supported** on Lance. Attempting to write a table with VARIANT columns
  to Lance throws a `HoodieNotSupportedException`. Use Parquet for tables with VARIANT columns.

## Mixed-Format Tables

`hoodie.table.base.file.format` is set per table, so different tables in the same lakehouse can use
different base file formats (Parquet, ORC, HFile, Lance) under a shared Hudi catalog and metadata
table.
