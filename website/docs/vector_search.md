---
title: "Vector Search"
keywords: [ hudi, vector, search, embeddings, similarity, cosine, nearest neighbor, VECTOR type]
summary: "Store embedding vectors in Hudi tables and run vector similarity search using the VECTOR type and hudi_vector_search TVF"
toc: true
last_modified_at: 2026-05-27T00:00:00-00:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `VECTOR` type stores fixed-dimension embedding vectors as a column. The `hudi_vector_search`
table-valued function (TVF) returns the top-K rows from a Hudi table whose VECTOR column is closest
to a given query vector.

## Storage Format

VECTOR columns are stored in Parquet as `FIXED_LEN_BYTE_ARRAY` — a fixed-length binary encoding of the
float array. Hudi stamps `hudi_type` metadata on the column so the Spark reader knows to decode the
bytes back into a typed array.

On **Lance** tables, VECTOR columns are stored natively as Lance `FixedSizeList<Float32/Float64, dim>`,
so embeddings are written without conversion overhead at the file-format layer. See
[Lance File Format](lance_file_format.md) for details.

The `VECTOR(dim[, elementType])` DDL syntax works across Spark 3.4, 3.5, 4.0, and 4.1. Hudi's SQL
parser normalizes `VECTOR(128, FLOAT)` to `VECTOR(128)` (FLOAT is the default element type).
Nesting VECTOR inside STRUCT, ARRAY, or MAP is not supported.

## VECTOR Type

The `VECTOR(dim[, elementType])` type declares a column that stores fixed-dimensional embedding vectors.
Dimension metadata enables the query engine to validate inputs and optimize search.

### Element Types

| Element Type | Description | Storage |
|:-------------|:------------|:--------|
| **FLOAT** (default) | 32-bit float | `ArrayType(FloatType)` |
| **DOUBLE** | 64-bit double | `ArrayType(DoubleType)` |
| **INT8** / **BYTE** | 8-bit signed integer | `ArrayType(ByteType)` |

```sql
-- Default (FLOAT)
embedding VECTOR(768)

-- Explicit element types
embedding VECTOR(768, FLOAT)
embedding VECTOR(768, DOUBLE)
embedding VECTOR(256, INT8)
```

### Declaring VECTOR Columns

<Tabs
groupId="api-style"
defaultValue="sql"
values={[
{ label: 'Spark SQL', value: 'sql', },
{ label: 'DataFrame API', value: 'dataframe', },
]}
>
<TabItem value="sql">

```sql
CREATE TABLE products (
    product_id   STRING,
    name         STRING,
    description  STRING,
    embedding    VECTOR(768)
) USING hudi
TBLPROPERTIES (
    primaryKey = 'product_id',
    type = 'cow',
    hoodie.record.merger.impls = 'org.apache.hudi.DefaultSparkRecordMerger',
    hoodie.datasource.write.base.file.format = 'parquet'
);
```

When using SQL DDL, Hudi's parser automatically stamps the `VECTOR(dim)` metadata on the column.

</TabItem>
<TabItem value="dataframe">

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("product_id", pa.string()),
    pa.field("name",       pa.string()),
    pa.field("embedding",  pa.list_(pa.float32()),
             metadata={b"hudi_type": b"VECTOR(768)"}),
])
```

When using the DataFrame API, you must manually stamp `hudi_type` metadata on the column via PyArrow.
This metadata is what distinguishes a `VECTOR` column from a regular array column.

</TabItem>
</Tabs>

### Writing Vectors

Vectors are written as arrays of floats. Both the DataFrame API and SQL accept standard array syntax:

```sql
INSERT INTO products VALUES (
    'prod_001', 'Running Shoes', 'Lightweight trail runner',
    ARRAY(0.123, -0.456, 0.789, ...)   -- 768 floats
);
```

## hudi_vector_search TVF

The `hudi_vector_search` table-valued function returns the `top_k` rows from a Hudi table whose
VECTOR column is closest to a given query vector under a chosen distance metric.

### Syntax

```sql
SELECT *
FROM hudi_vector_search(
    table_name,       -- STRING: name of the Hudi table
    vector_column,    -- STRING: name of the VECTOR column
    query_vector,     -- ARRAY<FLOAT>: the query embedding
    top_k,            -- INT: number of nearest neighbors to return
    [distance_metric], -- STRING: 'cosine' (default), 'l2', or 'dot_product'
    [algorithm]        -- STRING: 'brute_force' (default)
)
```

### Parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `table_name` | STRING | (required) | The Hudi table to search. Can be a registered table name or a path. |
| `vector_column` | STRING | (required) | The name of the VECTOR column to search against. |
| `query_vector` | ARRAY&lt;FLOAT&gt; | (required) | The query embedding. Must match the declared dimension and element type of the VECTOR column. |
| `top_k` | INT | (required) | The number of nearest neighbors to return. Must be a positive integer. |
| `distance_metric` | STRING | `'cosine'` | Distance metric: `'cosine'`, `'l2'`, or `'dot_product'`. |
| `algorithm` | STRING | `'brute_force'` | Search algorithm. Currently only `'brute_force'` is supported. |

### Return Schema

The TVF returns all columns from the source table (excluding the embedding column) plus:

| Column | Type | Description |
|:-------|:-----|:------------|
| `_hudi_distance` | DOUBLE | The computed distance between the query vector and each result. Lower values indicate greater similarity. |

Results are ordered by `_hudi_distance` ascending — closest matches first.

## hudi_vector_search_batch TVF

For searching with multiple query vectors at once, use the batch variant:

### Syntax

```sql
SELECT *
FROM hudi_vector_search_batch(
    corpus_table,           -- STRING: table to search
    corpus_embedding_col,   -- STRING: VECTOR column in corpus
    query_table,            -- STRING: table containing query vectors
    query_embedding_col,    -- STRING: VECTOR column in query table
    top_k,                  -- INT: neighbors per query
    [distance_metric],      -- STRING: 'cosine' (default)
    [algorithm]             -- STRING: 'brute_force' (default)
)
```

### Return Schema (Batch)

Returns corpus columns + query columns + distance info:

| Column | Type | Description |
|:-------|:-----|:------------|
| `_hudi_distance` | DOUBLE | Distance between query and corpus vector |
| `_hudi_query_index` | LONG | Index identifying which query vector produced this result |

If corpus and query tables share column names, query columns are prefixed with `_hudi_query_`.

### Distance Metrics

| Metric | Formula | Range | Notes |
|:-------|:--------|:------|:------|
| **cosine** | 1 - cos(a, b), clamped to [0, 2] | [0, 2] | Returns 1.0 for zero vectors. |
| **l2** | sqrt(sum((a[i] - b[i])^2)) | [0, +inf) | — |
| **dot_product** | -(a &middot; b) | (-inf, +inf) | Negated so ascending sort surfaces the most similar rows first. |

:::note
`cosine` distance computes `1 - cos(a, b)`. If embeddings are not L2-normalized before write,
results reflect both vector direction and magnitude.
:::

### Examples

**Find similar products:**

```sql
SELECT product_id, name, _hudi_distance AS distance
FROM hudi_vector_search(
    'products', 'embedding',
    ARRAY(0.12, -0.03, 0.87, ...),  -- query embedding
    10,                               -- top 10
    'cosine'
)
ORDER BY distance;
```

**RAG context retrieval:**

```sql
-- Retrieve the 5 most relevant document chunks for an LLM prompt
SELECT chunk_id, text_content, _hudi_distance
FROM hudi_vector_search(
    'document_chunks', 'embedding',
    ARRAY(...),  -- embedding of the user's question
    5, 'cosine'
)
WHERE _hudi_distance < 0.3;  -- optional distance threshold
```

**Cross-modal search (text-to-image):**

```sql
-- Using CLIP embeddings, find images matching a text query
SELECT image_id, caption, _hudi_distance
FROM hudi_vector_search(
    'image_catalog', 'clip_embedding',
    ARRAY(...),  -- text embedding from CLIP
    20, 'cosine'
);
```

## Constraints

- VECTOR columns must be **top-level fields** — nesting inside STRUCT, ARRAY, or MAP is not supported.
- The query vector's element type must **exactly match** the corpus embedding's element type (no implicit casting).
- VECTOR dimension and element type **cannot be changed** after table creation via schema evolution.
- **Flink cannot read VECTOR columns.** VECTOR data is stored as Parquet `FIXED_LEN_BYTE_ARRAY`, which
  Flink's Parquet reader does not decode back into a typed array. Flink can still read all **other**
  columns in a table that contains a VECTOR column — only the VECTOR column itself is inaccessible.
  Use Spark to query VECTOR columns.

## Metastore Sync

When syncing VECTOR column schemas to external catalogs, Hudi maps the binary encoding to the
target catalog's native binary type, preserving the original VECTOR metadata in table properties:

| Catalog | VECTOR representation |
|:--------|:---------------------|
| Hive | `BINARY` |
| BigQuery | `BYTES` |

The `VECTOR(dim, elementType)` dimension and element-type metadata is preserved in
`TBLPROPERTIES`/table descriptions so the table can be correctly reconstructed by Spark even after
a metastore round-trip.
