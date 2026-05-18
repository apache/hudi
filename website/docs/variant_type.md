---
title: "Semi-Structured Data (VARIANT)"
keywords: [ hudi, variant, semi-structured, json, schemaless, shredding, parse_json, flexible schema]
summary: "Store and query semi-structured JSON-like data in Hudi tables using the VARIANT type, with optional shredding for query performance"
toc: true
last_modified_at: 2026-04-25T00:00:00-00:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `VARIANT` type stores semi-structured, JSON-like data directly in Hudi tables. Unlike rigid schemas
where every column and type must be declared upfront, VARIANT columns accept arbitrary nested structures —
objects, arrays, scalars — and store them efficiently in a self-describing binary format.

This is particularly useful for AI and data engineering workloads where data shapes evolve rapidly:
model metadata, LLM outputs, feature dictionaries, API responses, and event payloads.

## Overview

VARIANT represents semi-structured data as two binary fields:

| Field | Description |
|:------|:------------|
| `metadata` | Encodes field names, types, and structure for efficient access |
| `value` | The actual data payload |

This encoding supports any JSON-compatible value: objects, arrays, strings, numbers, booleans, and null.
Unlike storing raw JSON strings, VARIANT provides type-aware binary storage with optional **shredding**
for columnar query performance.

## Creating Tables with VARIANT Columns

<Tabs
groupId="api-style"
defaultValue="sql4"
values={[
{ label: 'Spark SQL (4.0+)', value: 'sql4', },
{ label: 'Spark SQL (3.x)', value: 'sql3', },
{ label: 'DataFrame API', value: 'dataframe', },
]}
>
<TabItem value="sql4">

Spark 4.0+ has native `VARIANT` type support:

```sql
CREATE TABLE events (
    event_id  STRING,
    payload   VARIANT,
    ts        BIGINT
) USING hudi
TBLPROPERTIES (
    primaryKey = 'event_id',
    preCombineField = 'ts'
);
```

</TabItem>
<TabItem value="sql3">

On Spark 3.x, use the struct representation with metadata tagging:

```sql
CREATE TABLE events (
    event_id  STRING,
    payload   STRUCT<value: BINARY, metadata: BINARY>,
    ts        BIGINT
) USING hudi
TBLPROPERTIES (
    primaryKey = 'event_id',
    preCombineField = 'ts'
);
```

Hudi recognizes this struct pattern and treats it as a logical VARIANT.

</TabItem>
<TabItem value="dataframe">

```python
from pyspark.sql.types import *

# Option 1: Spark 4.0+ native VariantType
# schema = StructType([
#     StructField("event_id", StringType()),
#     StructField("payload", VariantType()),
#     StructField("ts", LongType()),
# ])

# Option 2: Struct with metadata tag (works on Spark 3.x and 4.0+)
from pyspark.sql.types import MetadataBuilder
variant_metadata = MetadataBuilder() \
    .putString("hudi_type", "VARIANT") \
    .build()
variant_struct = StructType([
    StructField("metadata", BinaryType()),
    StructField("value", BinaryType()),
])
schema = StructType([
    StructField("event_id", StringType()),
    StructField("payload", variant_struct, metadata=variant_metadata),
    StructField("ts", LongType()),
])
```

</TabItem>
</Tabs>

## Writing VARIANT Data

<Tabs
groupId="api-style"
defaultValue="sql4"
values={[
{ label: 'Spark SQL (4.0+)', value: 'sql4', },
{ label: 'DataFrame API', value: 'dataframe', },
]}
>
<TabItem value="sql4">

Use `parse_json()` to convert JSON strings into VARIANT values:

```sql
INSERT INTO events VALUES
    ('evt_001', parse_json('{"action": "click", "x": 120, "y": 450}'), 1000),
    ('evt_002', parse_json('{"action": "purchase", "items": ["sku_a", "sku_b"], "total": 59.99}'), 1001),
    ('evt_003', parse_json('"simple string value"'), 1002);
```

VARIANT accepts any valid JSON: objects, arrays, strings, numbers, booleans, and null.

</TabItem>
<TabItem value="dataframe">

```python
df = spark.sql("""
    SELECT
        'evt_001' as event_id,
        parse_json('{"action": "click", "x": 120, "y": 450}') as payload,
        1000 as ts
""")

df.write.format("hudi") \
    .option("hoodie.table.name", "events") \
    .option("hoodie.datasource.write.recordkey.field", "event_id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .mode("append") \
    .save("/path/to/events")
```

</TabItem>
</Tabs>

## Querying VARIANT Data

### Reading VARIANT as JSON

Cast VARIANT to string for JSON output:

```sql
SELECT event_id, cast(payload as STRING) as payload_json
FROM events;
```

```
+----------+--------------------------------------------------------------+
|  event_id|                                                  payload_json|
+----------+--------------------------------------------------------------+
|   evt_001|                    {"action":"click","x":120,"y":450}        |
|   evt_002| {"action":"purchase","items":["sku_a","sku_b"],"total":59.99}|
|   evt_003|                                      "simple string value"   |
+----------+--------------------------------------------------------------+
```

### Full DML Support

VARIANT columns support all standard DML operations:

```sql
-- UPDATE
UPDATE events SET payload = parse_json('{"action": "click", "x": 200}')
WHERE event_id = 'evt_001';

-- DELETE
DELETE FROM events WHERE event_id = 'evt_003';

-- MERGE
MERGE INTO events target
USING new_events source ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET payload = source.payload, ts = source.ts
WHEN NOT MATCHED THEN INSERT *;
```

### Table Type Support

VARIANT works with both COW (Copy-on-Write) and MOR (Merge-on-Read) table types. MOR tables
generate log files for VARIANT changes, which are merged during compaction.

## VARIANT Shredding

Shredding extracts frequently-accessed fields from VARIANT into dedicated typed Parquet columns. This
gives columnar query performance for known fields while retaining the flexibility of VARIANT for
the rest.

### Unshredded (Default)

```
group variant {
  required binary metadata;
  required binary value;
}
```

All data is in the opaque binary fields. Every query must decode the full VARIANT.

### Shredded (Scalar)

```
group variant {
  required binary metadata;
  optional binary value;
  optional int64  typed_value;   -- extracted scalar field
}
```

The `typed_value` column stores a typed extraction that Spark can read directly without decoding
the binary payload.

### Shredded (Object)

```
group variant {
  required binary metadata;
  optional binary value;
  optional group typed_value {
    optional group action {
      optional binary value;
      optional string typed_value;    -- "click", "purchase", etc.
    }
    optional group total {
      optional binary value;
      optional double typed_value;    -- 59.99
    }
  }
}
```

Each known field gets its own sub-column. Fields not present in a given row fall back to the
binary `value` field.

## Cross-Engine Compatibility

| Engine | VARIANT Support |
|:-------|:---------------|
| **Spark 4.0+** | Native `VariantType` — full read/write/query |
| **Spark 3.x** | Reads as `STRUCT<value: BINARY, metadata: BINARY>` — backward compatible |
| **Flink** | Reads as `ROW<metadata BYTES, value BYTES>` — cross-engine compatible |

A VARIANT table written by Spark 4.0 can be read by Spark 3.x or Flink, and vice versa. The
binary encoding is engine-independent.

## Use Cases for AI Workloads

### LLM Output Storage

Store raw LLM responses with varying structures:

```sql
CREATE TABLE llm_outputs (
    request_id  STRING,
    model       STRING,
    response    VARIANT,
    tokens_used INT,
    ts          BIGINT
) USING hudi TBLPROPERTIES (primaryKey = 'request_id', preCombineField = 'ts');

INSERT INTO llm_outputs VALUES (
    'req_001', 'claude-sonnet',
    parse_json('{"text": "...", "stop_reason": "end_turn", "usage": {"input": 500, "output": 200}}'),
    700, 1000
);
```

### Model Metadata & Experiment Tracking

Store heterogeneous model configurations and metrics:

```sql
CREATE TABLE experiments (
    run_id        STRING,
    model_config  VARIANT,    -- hyperparameters, architecture, etc.
    metrics       VARIANT,    -- loss, accuracy, custom metrics
    ts            BIGINT
) USING hudi TBLPROPERTIES (primaryKey = 'run_id', preCombineField = 'ts');
```

### Feature Dictionaries

Store sparse or variable-length feature maps for ML:

```sql
CREATE TABLE user_features (
    user_id    STRING,
    features   VARIANT,       -- {"age": 25, "interests": [...], "embedding": [...]}
    updated_at BIGINT
) USING hudi TBLPROPERTIES (primaryKey = 'user_id', preCombineField = 'updated_at');
```

### API Response Archival

Ingest and store API responses with evolving schemas:

```sql
CREATE TABLE api_responses (
    request_id  STRING,
    endpoint    STRING,
    status_code INT,
    body        VARIANT,
    ts          BIGINT
) USING hudi TBLPROPERTIES (primaryKey = 'request_id', preCombineField = 'ts');
```

## Best Practices

1. **Use VARIANT for evolving schemas** — When the data shape changes frequently or varies across
   records, VARIANT avoids constant schema migrations.

2. **Prefer typed columns for frequently-queried fields** — If you always filter on a specific field,
   make it a top-level typed column rather than burying it in VARIANT. Combine: use typed columns for
   stable fields and VARIANT for the rest.

3. **Consider shredding for hot fields** — If certain VARIANT fields are queried heavily, shredding
   extracts them into columnar storage for better performance.

4. **Use `parse_json()` for ingestion** — On Spark 4.0+, `parse_json()` is the standard way to create
   VARIANT values from JSON strings.

5. **Use `cast(v as STRING)` for export** — Convert VARIANT back to JSON for downstream consumers
   that expect text.

## Limitations

- Native `VARIANT` keyword in DDL requires Spark 4.0+. On Spark 3.x, use the struct representation.
- VARIANT shredding configuration is determined at write time based on the schema definition.
- Complex path expressions within VARIANT may require casting to STRING and then using JSON functions.
