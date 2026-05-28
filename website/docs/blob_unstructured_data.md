---
title: "Unstructured Data"
keywords: [ hudi, blob, unstructured data, images, binary, pdf, audio, video, inline, out-of-line, read_blob]
summary: "Store and query unstructured data (images, PDFs, audio, video) in Hudi tables using the BLOB type with inline or out-of-line storage"
toc: true
last_modified_at: 2026-05-27T00:00:00-00:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `BLOB` type stores raw binary data — images, PDFs, audio clips, video files, model weights —
as a column in a Hudi table.

## BLOB Type Overview

A BLOB column stores binary data in one of two modes:

| Mode | Storage | Read Pattern |
|:-----|:--------|:-------------|
| **Inline** | Raw bytes embedded in the table row | Direct read; no external fetch |
| **Out-of-line** | Pointer to external storage location | On-demand via `read_blob()` |

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
            pa.field("managed",       pa.bool_()),
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
  - `managed` — boolean. Only meaningful for `OUT_OF_LINE` blobs. Marks whether Hudi owns the lifecycle of the referenced external file. **Not consumed by the cleaner yet** — set the value to record intent, and a future cleaner implementation will use it: `true` → cleaner may delete the external file when the blob row is no longer referenced; `false` → cleaner will leave the external file in place.

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
        'reference', CAST(NULL AS STRUCT<external_path: STRING, offset: BIGINT, length: BIGINT, managed: BOOLEAN>)
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
            'length',        1073741824,    -- number of bytes
            'managed',       false          -- intent flag; not consumed by the cleaner yet
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

:::note
`read_blob()` materializes raw bytes per row, including external fetches for `OUT_OF_LINE` blobs.
Apply predicates before calling `read_blob()` to bound the bytes resolved.
:::

## Storage Characteristics

| Aspect | Inline | Out-of-Line |
|:-------|:-------|:------------|
| Where bytes live | In the table row | At `reference.external_path[offset, length]` |
| Pointer-only query (no `read_blob()`) | Reads data files | Reads pointer columns only |
| Random access pattern | Full row read | Seek to `(offset, length)` |

## Configuration Reference

| Property | Default | Description |
|:---------|:--------|:------------|
| `hoodie.read.blob.inline.mode` | `DESCRIPTOR` | Controls how INLINE BLOBs are read. `DESCRIPTOR` (default) returns an out-of-line-shaped reference pointing at the in-file coordinates of the bytes — no bytes are materialized. `CONTENT` materializes the raw inline bytes directly in the `data` field on every read. |
| `hoodie.blob.batching.max.gap.bytes` | `4096` | Maximum gap (in bytes) between consecutive byte ranges before they are merged into a single read. Larger values reduce I/O calls at the cost of reading some unused bytes. |
| `hoodie.blob.batching.lookahead.size` | `50` | Number of rows to buffer for batch read detection. Larger values improve batching for sorted data but increase memory usage. |

:::note
`DESCRIPTOR` mode is the default for all storage formats including Lance. `CONTENT` mode is always
used for internal operations (compaction, merge, log replay) regardless of this setting.
:::

:::caution Calling read_blob() on INLINE columns under DESCRIPTOR mode
Under the default `DESCRIPTOR` mode, calling `read_blob()` on an INLINE BLOB column **throws** —
the raw bytes are not materialized in the scan, so there is nothing for `read_blob()` to return.
To read inline bytes with `read_blob()`, switch to `CONTENT` mode first:

```sql
SET hoodie.read.blob.inline.mode=CONTENT;
SELECT asset_id, read_blob(content) AS raw_bytes
FROM media_assets
WHERE asset_id = 'asset_001';
```

This setting affects only INLINE columns — OUT_OF_LINE columns always fetch from the external path
regardless of mode.
:::

## Metastore Sync

When syncing BLOB column schemas to Hive or BigQuery, Hudi maps the BLOB struct to the target
catalog's native struct type:

| Catalog | BLOB representation |
|:--------|:-------------------|
| Hive | `STRUCT<type:STRING, data:BINARY, reference:STRUCT<external_path:STRING, offset:BIGINT, length:BIGINT, managed:BOOLEAN>>` |
| BigQuery | Equivalent `STRUCT` fields |

The raw binary payload is preserved in the struct representation, but `read_blob()` is a Spark SQL
function and is not available in Hive or BigQuery directly.

## Notes

- `read_blob()` is a Spark SQL function. It is not available from Hive, BigQuery, or other engines
  reading the underlying struct directly.
- For OUT_OF_LINE blobs, multiple rows can reference different `(offset, length)` ranges within the
  same `external_path`. Hudi reads the configured byte ranges; it does not own the lifecycle of the
  external file unless `reference.managed=true` is set (currently advisory; see the struct definition
  above).
- BLOB columns are excluded from column-stats indexing.
