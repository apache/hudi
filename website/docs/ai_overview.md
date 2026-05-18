---
title: "AI-Native Lakehouse"
keywords: [ hudi, ai, machine learning, vector search, embeddings, unstructured data, blob, lance, multimodal, lakehouse]
summary: "Apache Hudi brings first-class AI and unstructured data support to the data lakehouse — store embeddings, images, audio, and run vector search natively"
toc: true
last_modified_at: 2026-04-25T00:00:00-00:00
---

Modern AI workloads demand more from the data lakehouse than traditional analytics ever did. Teams building
retrieval-augmented generation (RAG) pipelines, recommendation systems, content moderation, and multimodal
search need to store and query **embeddings**, **images**, **audio**, **video**, and **documents** alongside
structured data — all with the transactional guarantees, incremental processing, and table services that
Hudi is known for.

Apache Hudi's AI-native capabilities bring this vision to life with four foundational building blocks:

## Core Capabilities

### VECTOR Type and Similarity Search

Store high-dimensional embedding vectors as first-class column types and run approximate nearest neighbor (ANN)
search directly in Spark SQL.

```sql
-- Declare an embedding column
CREATE TABLE products (
    id        STRING,
    name      STRING,
    embedding VECTOR(768)
) USING hudi;

-- Find the 10 most similar products
SELECT id, name, _hudi_distance
FROM hudi_vector_search('products', 'embedding', ARRAY(...), 10, 'cosine');
```

No external vector database required. Your embeddings live in the same table as your structured data, governed
by the same transactions, schemas, and access controls.

**[Learn more about Vector Search &#8594;](vector_search.md)**

### BLOB Type for Unstructured Data

Store raw binary data (images, PDFs, audio clips, model weights) directly in Hudi tables using the `BLOB` type.
Choose between two storage modes:

| Mode | How it works | Best for |
|:-----|:-------------|:---------|
| **Inline** | Bytes stored directly in the table row | Small objects (thumbnails, short audio clips) |
| **Out-of-line** | Table stores a pointer; `read_blob()` resolves it on demand | Large objects (high-res images, video, model checkpoints) |

```sql
CREATE TABLE documents (
    doc_id   STRING,
    content  BLOB,
    summary  STRING
) USING hudi;

-- Read the raw bytes on demand
SELECT doc_id, read_blob(content) AS raw_bytes FROM documents WHERE doc_id = 'doc_001';
```

Out-of-line BLOBs keep your table footprint small (often less than 1% of total data size) while maintaining
full queryability.

**[Learn more about Unstructured Data &#8594;](blob_unstructured_data.md)**

### VARIANT Type for Semi-Structured Data

AI pipelines often deal with data whose shape is not known in advance: LLM outputs, model metadata,
feature dictionaries, API responses. The `VARIANT` type stores semi-structured, JSON-like data with
full transactional support — no rigid schema required.

```sql
CREATE TABLE llm_outputs (
    request_id  STRING,
    response    VARIANT,
    ts          BIGINT
) USING hudi;

-- Store any JSON structure
INSERT INTO llm_outputs VALUES (
    'req_001',
    parse_json('{"text": "...", "stop_reason": "end_turn", "tokens": 700}'),
    1000
);

-- Query back as JSON
SELECT request_id, cast(response as STRING) FROM llm_outputs;
```

VARIANT supports optional **shredding** to extract hot fields into typed columnar storage for better
query performance, while keeping the flexibility for everything else.

**[Learn more about Semi-Structured Data &#8594;](variant_type.md)**

### Lance File Format

Hudi's pluggable file format architecture supports **Lance**, a modern columnar format purpose-built for
AI/ML workloads. Lance provides:

- Efficient vector indexing and ANN search
- Fast random access for training data sampling
- Optimized storage for high-dimensional arrays and nested structures

```sql
CREATE TABLE embeddings (...) USING hudi
TBLPROPERTIES (
    hoodie.datasource.write.base.file.format = 'lance'
);
```

Lance integrates seamlessly with Hudi's table services (compaction, clustering, cleaning) and
works alongside existing Parquet and ORC tables.

**[Learn more about the Lance File Format &#8594;](lance_file_format.md)**

## Why Hudi for AI Workloads?

### Unified Storage for Structured + Unstructured Data

Most AI pipelines today span multiple systems: a data warehouse for metadata, an object store for raw files,
a vector database for embeddings, and custom glue code to keep them in sync. Hudi collapses this into a single
table:

```
┌─────────────────────────────────────────────────┐
│                  Hudi Table                     │
│                                                 │
│  image_id │ breed │ embedding    │ image_bytes  │
│  (STRING) │(STRING)│(VECTOR(1024))│   (BLOB)    │
│───────────┼───────┼──────────────┼──────────────│
│  pet_001  │ Corgi │ [0.12, ...]  │ <137 KB PNG> │
│  pet_002  │ Tabby │ [-.03, ...]  │ <89 KB JPEG> │
└─────────────────────────────────────────────────┘
```

One table. One set of transactions. One schema. One set of access controls.

### Incremental Processing for Embedding Pipelines

When new data arrives, you do not need to re-embed your entire corpus. Hudi's incremental query capabilities
let you process only new or changed records:

```sql
-- Get only new images since the last embedding run
SELECT * FROM hudi_table_changes('product_images', 'latest_state', '20260101000000');
```

This can reduce embedding pipeline costs by 10-100x compared to full reprocessing.

### Transactional Guarantees

Embedding updates, metadata changes, and raw data writes happen atomically. No more inconsistent states where
your vector index points to deleted images or stale embeddings.

### Table Services

Hudi's background table services work on AI tables just like any other:

- **Clustering** — co-locate similar vectors for better search locality
- **Compaction** — merge incremental updates efficiently
- **Cleaning** — reclaim storage from old versions
- **Indexing** — maintain metadata indexes for fast lookups

### Open Ecosystem

Hudi tables are readable by Spark, Flink, Presto, Trino, and the native Python/Rust client (hudi-rs). Your AI
tables are not locked into a single engine or vendor.

## Use Cases

| Use Case | Hudi Capabilities Used |
|:---------|:----------------------|
| **Image/Video Search** | VECTOR embeddings + BLOB storage + cosine similarity search |
| **RAG (Retrieval-Augmented Generation)** | VECTOR search to retrieve relevant document chunks for LLM context |
| **LLM Output Management** | VARIANT for flexible response storage, VECTOR for semantic indexing |
| **Recommendation Systems** | VECTOR similarity for collaborative filtering, incremental re-embedding |
| **Content Moderation** | BLOB for raw content + VECTOR for content embeddings + incremental processing |
| **Multimodal Analytics** | Structured metadata + VECTOR embeddings + BLOB raw data in one table |
| **ML Feature Store** | VECTOR for feature embeddings, VARIANT for sparse feature maps, time-travel for point-in-time retrieval |
| **Experiment Tracking** | VARIANT for heterogeneous model configs and metrics, incremental queries for latest runs |
| **Data Labeling Pipelines** | BLOB for raw data, incremental queries for unlabeled data, ACID for label updates |

## Getting Started

The fastest way to try these features is the **[AI Quick Start Guide](ai-quick-start-guide.md)**, which walks
you through an end-to-end image similarity search pipeline in under 30 minutes.
