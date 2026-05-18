---
title: "Unstructured Data"
keywords: [ hudi, blob, unstructured data, images, binary, pdf, audio, video, inline, out-of-line, read_blob]
summary: "Store and query unstructured data (images, PDFs, audio, video) in Hudi tables using the BLOB type with inline or out-of-line storage"
toc: true
last_modified_at: 2026-04-25T00:00:00-00:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `BLOB` type lets you store raw binary data — images, PDFs, audio clips, video files, model weights —
directly in Hudi tables. Combined with Hudi's transactional guarantees and table services, BLOB makes
the data lakehouse a single source of truth for both structured and unstructured data.

## BLOB Type Overview

A BLOB column stores binary data in one of two modes:

| Mode | Storage | Table Footprint | Read Pattern |
|:-----|:--------|:----------------|:-------------|
| **Inline** | Raw bytes embedded in the table row | Larger (bytes stored in data files) | Direct read — no external fetch |
| **Out-of-line** | Pointer to external storage location | Very small (< 1% of data size) | On-demand via `read_blob()` |

Choose **inline** for small objects that are frequently read together (thumbnails, short audio clips,
small documents). Choose **out-of-line** for large objects where you typically query metadata first and
fetch raw data selectively (high-res images, video, model checkpoints).

## Creating Tables with BLOB Columns

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
CREATE TABLE media_assets (
    asset_id    STRING,
    file_name   STRING,
    mime_type   STRING,
    file_size   BIGINT,
    content     BLOB
) USING hudi
TBLPROPERTIES (
    primaryKey = 'asset_id',
    type = 'cow'
);
```

The `BLOB` keyword in DDL automatically configures the column with the correct internal structure.

</TabItem>
<TabItem value="dataframe">

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("asset_id",  pa.string()),
    pa.field("file_name", pa.string()),
    pa.field("mime_type", pa.string()),
    pa.field("file_size", pa.int64()),
    pa.field("content",   pa.struct([
        pa.field("type",      pa.string()),
        pa.field("data",      pa.binary()),
        pa.field("reference", pa.struct([
            pa.field("external_path", pa.string()),
            pa.field("offset",        pa.int64()),
            pa.field("length",        pa.int64()),
        ])),
    ]), metadata={b"hudi_type": b"BLOB"}),
])
```

The BLOB internal structure is a struct with three fields:
- `type` — `"INLINE"` or `"OUT_OF_LINE"`
- `data` — raw bytes (populated for inline, null for out-of-line)
- `reference` — external storage pointer with subfields:
  - `external_path` — file path for out-of-line data
  - `offset` — byte offset in the file (null means read from start)
  - `length` — byte length to read (null means read to end of file)
  - `managed` — boolean indicating whether Hudi manages the external file

</TabItem>
</Tabs>

## Writing Inline BLOBs

Inline BLOBs embed the raw bytes directly in the table row.

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
INSERT INTO media_assets VALUES (
    'asset_001',
    'logo.png',
    'image/png',
    45230,
    named_struct(
        'type',      'INLINE',
        'data',      /* binary literal or column reference */,
        'reference', CAST(NULL AS STRUCT<external_path: STRING, offset: BIGINT, length: BIGINT>)
    )
);
```

</TabItem>
<TabItem value="dataframe">

```python
from pyspark.sql import Row
from pyspark.sql.types import *

# Read raw bytes
with open("logo.png", "rb") as f:
    raw_bytes = f.read()

row = Row(
    asset_id="asset_001",
    file_name="logo.png",
    mime_type="image/png",
    file_size=len(raw_bytes),
    content=Row(type="INLINE", data=raw_bytes, reference=None)
)
```

</TabItem>
</Tabs>

## Writing Out-of-Line BLOBs

Out-of-line BLOBs store a pointer to data in external storage. The actual bytes live elsewhere (e.g.,
a binary container file on S3, a separate object store path, or a shared filesystem). The table only
stores the reference metadata.

```sql
INSERT INTO media_assets VALUES (
    'asset_002',
    'video.mp4',
    'video/mp4',
    1073741824,  -- 1 GB
    named_struct(
        'type',      'OUT_OF_LINE',
        'data',      CAST(NULL AS BINARY),
        'reference', named_struct(
            'external_path', 's3://my-bucket/media/container_001.bin',
            'offset',        8388608,       -- byte offset in the container
            'length',        1073741824     -- number of bytes
        )
    )
);
```

### Container File Pattern

A common pattern for out-of-line storage is to pack multiple objects into a single binary container file:

```
container_001.bin
├── [offset=0,       len=45230]   → logo.png
├── [offset=45230,   len=89012]   → photo.jpg
├── [offset=134242,  len=1073741824] → video.mp4
└── ...
```

Each BLOB row stores the `(external_path, offset, length)` triple. This avoids creating millions of
small files on object storage and enables efficient batch access.

## Reading BLOBs

### Querying Metadata (No Fetch)

Standard queries on a BLOB column return the descriptor — not the raw bytes:

```sql
SELECT asset_id, file_name, content.type, content.reference.external_path
FROM media_assets;
```

```
+----------+-----------+-------------+----------------------------------------+
|  asset_id|  file_name|         type|                           external_path|
+----------+-----------+-------------+----------------------------------------+
| asset_001|   logo.png|       INLINE|                                    null|
| asset_002|  video.mp4|  OUT_OF_LINE| s3://my-bucket/media/container_001.bin |
+----------+-----------+-------------+----------------------------------------+
```

This is fast and lightweight — no binary data is transferred.

### Resolving Raw Bytes with read_blob()

Use the `read_blob()` SQL function to materialize the actual bytes:

```sql
-- Returns raw binary data
SELECT asset_id, read_blob(content) AS raw_bytes
FROM media_assets
WHERE asset_id = 'asset_001';
```

For **inline** BLOBs, `read_blob()` simply extracts the embedded bytes.

For **out-of-line** BLOBs, `read_blob()` reads from the external path at the specified offset and length,
transparently fetching the data on demand.

:::tip
Use `read_blob()` selectively — filter first, then resolve. Avoid `SELECT read_blob(content) FROM large_table`
without a WHERE clause, as this will fetch all raw data.
:::

## Use Cases

### Image Datasets for Computer Vision

Store training images alongside metadata and embeddings:

```sql
CREATE TABLE training_images (
    image_id   STRING,
    label      STRING,
    split      STRING,           -- 'train', 'val', 'test'
    embedding  VECTOR(1024),
    raw_image  BLOB
) USING hudi TBLPROPERTIES (...);

-- Get raw images for a specific label
SELECT image_id, read_blob(raw_image) AS pixels
FROM training_images
WHERE label = 'cat' AND split = 'train';
```

### Document Store for RAG Pipelines

Store PDF documents alongside their chunk embeddings:

```sql
CREATE TABLE knowledge_base (
    doc_id     STRING,
    chunk_id   STRING,
    source_url STRING,
    text       STRING,
    embedding  VECTOR(1536),
    original   BLOB              -- original PDF bytes
) USING hudi TBLPROPERTIES (...);

-- Retrieve full document after vector search
SELECT doc_id, source_url, read_blob(original) AS pdf_bytes
FROM knowledge_base
WHERE doc_id IN (SELECT doc_id FROM top_matches);
```

### Audio/Video Processing Pipelines

```sql
CREATE TABLE audio_clips (
    clip_id    STRING,
    transcript STRING,
    duration   DOUBLE,
    embedding  VECTOR(512),
    audio      BLOB
) USING hudi TBLPROPERTIES (...);
```

## Storage Efficiency

Out-of-line BLOBs keep the Hudi table footprint extremely small:

| Metric | Inline | Out-of-Line |
|:-------|:-------|:------------|
| Table size vs. raw data | ~100% | < 1% |
| Query metadata without fetch | Requires reading data files | Only reads pointer columns |
| Random access to raw data | Read full row | Seek to (offset, length) |
| Best for object size | < 1 MB | > 1 MB |

## Configuration Reference

| Property | Default | Description |
|:---------|:--------|:------------|
| `hoodie.read.blob.inline.mode` | `CONTENT` | Controls how INLINE BLOBs are read. `CONTENT` materializes raw bytes in the `data` column. `DESCRIPTOR` surfaces `(position, size)` coordinates rewritten as OUT_OF_LINE references. |
| `hoodie.blob.batching.max.gap.bytes` | `4096` | Maximum gap (in bytes) between consecutive byte ranges before they are merged into a single read. Larger values reduce I/O calls at the cost of reading some unused bytes. |
| `hoodie.blob.batching.lookahead.size` | `50` | Number of rows to buffer for batch read detection. Larger values improve batching for sorted data but increase memory usage. |

:::note
DESCRIPTOR mode is only supported on Lance-backed tables. CONTENT mode is always used for internal
operations (compaction, merge, log replay) regardless of this setting.
:::

## Best Practices

1. **Choose the right mode** — Use inline for small, frequently-accessed objects. Use out-of-line for
   anything over 1 MB.

2. **Filter before resolving** — Always apply WHERE predicates before calling `read_blob()` to avoid
   unnecessary data transfer.

3. **Batch container files** — When using out-of-line mode, pack multiple objects into container files
   rather than storing one file per object.

4. **Combine with VECTOR** — Pair BLOB columns with VECTOR columns for powerful "search then retrieve"
   workflows: vector search narrows candidates, then `read_blob()` fetches just the winners.

5. **Use incremental queries** — Process only new BLOBs by leveraging Hudi's incremental query support:
   ```sql
   SELECT * FROM hudi_table_changes('media_assets', 'latest_state', '20260401000000');
   ```
