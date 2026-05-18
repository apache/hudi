---
title: "Lance File Format"
keywords: [ hudi, lance, file format, vector, AI, ML, columnar, ANN, indexing]
summary: "Use the Lance columnar file format with Hudi for vector-optimized storage, ANN indexing, and efficient ML workloads"
toc: true
last_modified_at: 2026-04-25T00:00:00-00:00
---

[Lance](https://lancedb.github.io/lance/) is a modern columnar data format designed for AI and machine learning
workloads. Hudi's pluggable storage architecture lets you use Lance as the base file format alongside Parquet
and ORC, unlocking vector indexing, fast random access, and optimized high-dimensional array storage.

## Enabling Lance in Hudi

### Table Creation

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
    hoodie.datasource.write.base.file.format = 'lance'
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
   .option("hoodie.datasource.write.base.file.format", "lance")
   .mode("overwrite")
   .save("/path/to/my_ai_table"))
```

### Required Dependencies

Add the Lance Spark bundle to your Spark classpath:

| Component | Maven Coordinates |
|:----------|:-----------------|
| Lance Spark Bundle (Spark 3.5) | `org.lance:lance-spark-bundle-3.5_2.12:0.4.0` |

```bash
export LANCE_BUNDLE_JAR=/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar

# Include both Hudi and Lance JARs
spark-shell --jars $HUDI_BUNDLE_JAR,$LANCE_BUNDLE_JAR
```

## How Hudi + Lance Work Together

Hudi manages the table layer — transactions, schema, timeline, table services — while Lance handles the
file-level storage:

```
┌───────────────────────────────────┐
│         Hudi Table Layer          │
│  Timeline, Metadata, Indexing     │
│  Transactions, Schema Evolution   │
├───────────────────────────────────┤
│     File Group / File Slice       │
│  (same Hudi concepts as Parquet)  │
├───────────────────────────────────┤
│     Lance Data Files (.lance)     │
│  IVF-PQ vector index              │
│  Columnar storage                 │
│  Fragment-based layout            │
├───────────────────────────────────┤
│   Storage (S3, GCS, HDFS, FS)    │
└───────────────────────────────────┘
```

All Hudi table services work with Lance-backed tables:

- **Compaction** — merges log files into Lance base files
- **Clustering** — reorganizes Lance files for better data locality
- **Cleaning** — removes old Lance file versions
- **Metadata indexing** — column stats and bloom filters work across Lance files

## Vector Search with Lance

The `hudi_vector_search` TVF leverages Lance's built-in IVF-PQ index for approximate nearest neighbor search:

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

| Property | Description | Default |
|:---------|:------------|:--------|
| `hoodie.datasource.write.base.file.format` | Set to `lance` to use Lance as the base file format | `parquet` |
| `hoodie.record.merger.impls` | Must be `org.apache.hudi.DefaultSparkRecordMerger` for Lance | — |

## Mixed-Format Tables

Hudi does not require all tables in a lakehouse to use the same file format. You can have:

- **Analytical tables** on Parquet — for traditional BI/SQL workloads
- **AI tables** on Lance — for embeddings, vector search, and ML feature storage
- **Archival tables** on ORC — for compressed long-term storage

All share the same Hudi catalog, metadata, and tooling.
