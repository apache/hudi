---
title: "Python/Rust Quick Start"
toc: true
last_modified_at: 2026-10-29T00:00:00-07:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

[hudi-rs](https://github.com/apache/hudi-rs) is the native Rust implementation of Apache Hudi with Python bindings.
It creates Hudi tables and runs upserts, updates, and deletes on them from a single process, with no JVM and no
Spark. Every write maintains the table's metadata table and record-level index, so updating or deleting a handful
of rows in a large table costs a few index lookups and the rewrite of the file groups that hold those rows, not a
scan of the table.

This page gets you from `pip install` to a table you can upsert into, delete from, and read incrementally, then
shows how to query it from Python, Rust, DuckDB, Polars, and DataFusion.

## Installation

```bash
# Python (3.10+)
pip install hudi

# Rust
cargo add hudi tokio
```

## Write a table in Python

The complete loop for a retrieval or training pipeline: a table of document chunks with their embeddings, keyed by
`chunk_id`. Ingest, re-sync the documents that changed, drop the ones that were removed, and read back only what
changed since the last run.

```python
import numpy as np
import pyarrow as pa
from hudi import HudiTable, HudiReadOptions, HudiQueryType

def chunks(doc_ids, version):
    rows = [(f"{d}#{i}", d, i, f"chunk {i} of {d}, version {version}") for d in doc_ids for i in range(4)]
    embeddings = np.random.default_rng(version).random((len(rows), 384), dtype=np.float32)
    return pa.table({
        "chunk_id":   [r[0] for r in rows],
        "doc_id":     [r[1] for r in rows],
        "pos":        pa.array([r[2] for r in rows], pa.int32()),
        "text":       [r[3] for r in rows],
        "embedding":  pa.array(embeddings.tolist(), pa.list_(pa.float32())),
        "updated_at": pa.array([version] * len(rows), pa.int64()),
    })

table = HudiTable.create(
    "/tmp/contract_chunks",
    record_key="chunk_id",        # the record-level index is built on this key
    ordering_field="updated_at",  # the later version wins when a key arrives twice
)

base = table.append(chunks(["doc1", "doc2", "doc3"], version=1))

# doc2 changed: re-embed its chunks and upsert them. The index finds the file
# groups holding these keys; nothing else in the table is read.
result = table.upsert(chunks(["doc2"], version=2))
print(result.num_updates, result.num_inserts)   # 4 0

# doc3 was removed: delete its chunks.
table.delete("doc_id = 'doc3'")

# Train on what changed after the initial load, not on the whole table.
changed = table.read(
    HudiReadOptions()
    .with_query_type(HudiQueryType.Incremental)
    .with_start_timestamp(base.completion_time)
)
print(pa.Table.from_batches(changed).num_rows)   # 4, the re-embedded chunks of doc2

# The current snapshot, as Arrow, for DuckDB or anything else.
snapshot = pa.Table.from_batches(table.read())
```

`append`, `upsert`, and `overwrite` accept a `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`,
or any object that implements the Arrow PyCapsule interface, so a Polars DataFrame or a pandas 2.2+ DataFrame can
be passed directly. Embeddings may arrive as `list<float32>` or `fixed_size_list<float32>`; both are stored as lists.

`update` sets columns by predicate and `delete_keys` deletes by key:

```python
table.update("doc_id = 'doc1'", {"text": "redacted"})
table.delete_keys(["doc1#0", "doc1#1"])
```

## Write a table in Rust

The same loop with the `hudi` crate. Batches are Arrow `RecordBatch`es.

```rust
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi::config::table::TableTypeValue;
use hudi::error::Result;
use hudi::table::{ReadOptions, Table};
use std::sync::Arc;

fn trips(rows: &[(&str, &str, i64, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("fare", DataType::Float64, false),
    ]));
    RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>())),
    ]).unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut table = Table::create("/tmp/trips")
        .with_table_name("trips")
        .with_table_type(TableTypeValue::CopyOnWrite)   // or MergeOnRead
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .create()
        .await?;

    table.append([trips(&[("t1", "sf", 1, 30.0), ("t2", "sf", 1, 45.0), ("t3", "nyc", 1, 20.0)])]).await?;

    let result = table.upsert([trips(&[("t2", "sf", 2, 50.0), ("t4", "nyc", 2, 15.0)])]).await?;
    println!("instant={} updates={} inserts={}", result.instant, result.num_updates, result.num_inserts);

    table.delete("id = 't1'").await?;

    let batches = table.read(&ReadOptions::new()).await?;
    println!("{} rows", batches.iter().map(|b| b.num_rows()).sum::<usize>());
    Ok(())
}
```

Both writers produce the same on-disk layout as the Spark writer: a table written here can be read, compacted,
and clustered by Spark, and vice versa.

## Why upserts and deletes are fast: the record-level index

A table format with no index answers "which file holds key `doc2#1`?" by scanning every file, or at best every
file whose min/max statistics admit the key. With keys like `<doc>#<pos>` or UUIDs that is the whole table on every
sync, however few documents changed.

Hudi keeps a [record-level index](indexes#record-index) in the table's metadata table: a persistent map from record
key to file group, sharded by key hash. hudi-rs writes it on `create` and updates it on every commit. A sync that
touches 10,000 chunks in a table of millions costs 10,000 index lookups, which name the file groups to rewrite (or,
on a merge-on-read table, to append a log block to). The rest of the table is never read.

## Use cases and numbers

The [`benchmark/upsert`](https://github.com/apache/hudi-rs/tree/main/benchmark/upsert) harness in the hudi-rs
repository runs five single-node workloads against hudi-rs; the numbers below also include delta-rs and PyIceberg
run through the same commits on the same machine. Every end state is checked against a plain Parquet copy of the
expected table, and the machine's load, memory, and disk are recorded next to every timing. Each scenario below links to
its definition so the numbers can be reproduced on your own machine.

| Scenario | Models | Table | Each commit |
|---|---|---|---|
| `rag-chunks` | Re-syncing a retrieval corpus | chunks with a 384-float embedding, keyed by `chunk_id` | re-ingest 1,000 changed documents (10,000 chunks), delete 100 removed documents |
| `training-set` | Curating a fine-tuning set | training examples keyed by `example_id` | upsert corrected examples, delete rejected ones, then read what changed since the last run and time-travel to the first version |
| `uuid` | Random-key upserts | 10M rows with UUID keys | upsert 10,000 existing keys plus 1,000 new, delete 1,000 |
| `nyc-taxi` | Correcting a public dataset | NYC yellow taxi, January 2022 | upsert 1% of trips with new fares, delete 0.1% |
| `tpcds-web-returns` | A warehouse fact table | TPC-DS `web_returns`, composite key | upsert 1% matched plus 10% new rows, delete 5% |

{/* BENCHMARK_TABLE_START */}
Median seconds per commit over three runs, five upsert and five delete commits per run, on one Apple M3 Max laptop
(14 cores, 36 GiB, local SSD). Each library is used through its own Python API with default settings: hudi-rs 0.6.0
(merge-on-read with the record-level index), deltalake 1.6.6 (delta-rs) and pyiceberg 0.12.0. Lower is better.

| Scenario | Rows | hudi-rs upsert / delete | delta-rs upsert / delete | PyIceberg upsert / delete |
|---|---|---|---|---|
| `rag-chunks` | 1M | **0.29 / 0.18** | 2.78 / 4.49 | 11.48 / 9.41 |
| `training-set` | 1M | **0.25 / 0.20** | 1.10 / 1.83 | 2.88 / 2.19 |
| `uuid` | 10M | **1.67 / 1.70** | 3.20 / 4.53 | 7.73 / 4.38 |
| `nyc-taxi` | 2.5M | 0.72 / **0.35** | **0.64** / 1.02 | 2.98 / 1.00 |
| `tpcds-web-returns` | 0.7M | **0.55** / 1.02 | 0.74 / **0.56** | failed (see below) |

A merge-on-read commit appends only the changed rows. Per upsert commit, hudi-rs wrote 19 MiB on `rag-chunks` where
delta-rs rewrote 1,689 MiB and PyIceberg 1,449 MiB, and 2 MiB on `uuid` against 671 MiB and 388 MiB.

Bulk load and full-table reads are not where hudi-rs leads today. Loading the 10M-row `uuid` table takes 12.5 s with
the record-level index (4.8 s without it), against 3.1 s for delta-rs and 0.7 s for PyIceberg; reading it back in full
takes 0.50 s, against 0.43 s and 0.18 s.

Notes: PyIceberg 0.12 crashed in Arrow while building its filter for the 79,113 composite keys of the first
`tpcds-web-returns` upsert. Enabling deletion vectors in delta-rs 1.6 and the merge-on-read write modes in PyIceberg
0.12 did not change their numbers: both still rewrite the files a change touches.
{/* BENCHMARK_TABLE_END */}

## Read a table

<Tabs>
<TabItem value="python" label="Python" default>

```python
from hudi import HudiReadOptions, HudiTableBuilder
import pyarrow as pa

table = HudiTableBuilder.from_base_uri("/tmp/trips").build()

# Snapshot, with partition and row filters pushed down
batches = table.read(HudiReadOptions(filters=[("city", "=", "sf")]))
print(pa.Table.from_batches(batches))

# Time travel
batches = table.read(HudiReadOptions().with_as_of_timestamp("20261029120000000"))

# Incremental: rows from commits that completed after a point in time, such as
# the completion_time of an earlier write
from hudi import HudiQueryType
since = "20261029120000000"
batches = table.read(HudiReadOptions().with_query_type(HudiQueryType.Incremental).with_start_timestamp(since))

# Stream batches instead of materializing the result
for batch in table.read_stream(HudiReadOptions(projection=["id", "fare"]).with_batch_size(4096)):
    print(batch.num_rows)
```

</TabItem>
<TabItem value="rust" label="Rust">

```rust
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use hudi::table::{QueryType, ReadOptions};

let table = HudiTableBuilder::from_base_uri("/tmp/trips").build().await?;

// Snapshot, with partition and row filters pushed down
let batches = table.read(&ReadOptions::new().with_filters([("city", "=", "sf")])?).await?;

// Time travel
let batches = table.read(&ReadOptions::new().with_as_of_timestamp("20261029120000000")).await?;

// Incremental: rows from commits that completed after a point in time, such as
// the completion_time of an earlier write
let since = "20261029120000000";
let options = ReadOptions::new().with_query_type(QueryType::Incremental).with_start_timestamp(since);
let batches = table.read(&options).await?;
```

</TabItem>
</Tabs>

Timestamps accept the Hudi timeline format (`yyyyMMddHHmmssSSS`), Unix epoch in seconds through nanoseconds, and
RFC 3339 with a timezone offset.

To run a read-optimized query on a merge-on-read table (base files only, no log merge), set
`hoodie.read.use.read_optimized.mode` to `true` in the read options.

## Query with DuckDB, Polars, and DataFusion

hudi-rs returns Arrow, so anything that consumes Arrow can query a Hudi table in Python.

```python
import duckdb, polars as pl, pyarrow as pa
from hudi import HudiTableBuilder

trips = pa.Table.from_batches(HudiTableBuilder.from_base_uri("/tmp/trips").build().read())

duckdb.sql("SELECT city, avg(fare) FROM trips GROUP BY city").show()
print(pl.from_arrow(trips).group_by("city").agg(pl.col("fare").mean()))
```

In Rust, the `datafusion` feature of the `hudi` crate registers a Hudi table as a DataFusion table provider.

<details>
<summary>Add the hudi crate with the datafusion feature to your application.</summary>

```shell
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion
cargo add hudi --features datafusion
```

</details>

```rust
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new_with_options("/tmp/trips", [("hoodie.read.input.partitions", "5")]).await?;
    ctx.register_table("trips", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * FROM trips WHERE city = 'sf'").await?;
    df.show().await?;
    Ok(())
}
```

The same provider is available in Python through `HudiDataFusionDataSource` when the `datafusion` extra of the
`hudi` package is installed. Hudi tables can also be read by [Daft](https://docs.daft.ai/en/stable/io/hudi/) and
[Ray](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_hudi.html#ray.data.read_hudi).

## Work with cloud storage

Set the usual cloud credentials as environment variables (`AWS_*`, `AZURE_*`, `GOOGLE_*`) and use an `s3://`,
`az://`, or `gs://` base URI. Storage options can also be passed explicitly.

<Tabs>
<TabItem value="python" label="Python" default>

```python
from hudi import HudiTable, HudiTableBuilder

table = HudiTable.create("s3://bucket/trips", record_key="id", storage_options={"aws_region": "us-west-2"})

reader = HudiTableBuilder.from_base_uri("s3://bucket/trips").with_option("aws_region", "us-west-2").build()
```

</TabItem>
<TabItem value="rust" label="Rust">

```rust
use hudi::table::Table;
use hudi::table::builder::TableBuilder as HudiTableBuilder;

let mut table = Table::create("s3://bucket/trips")
    .with_table_name("trips")
    .with_record_key_fields(["id"])
    .with_storage_option("aws_region", "us-west-2")
    .create()
    .await?;

let reader = HudiTableBuilder::from_base_uri("s3://bucket/trips")
    .with_option("aws_region", "us-west-2")
    .build()
    .await?;
```

</TabItem>
</Tabs>

## Query engine integration

For engines that plan their own scans, hudi-rs exposes the planning and execution primitives directly.

| Stage           | API                                       | Description                                                                    |
|-----------------|-------------------------------------------|--------------------------------------------------------------------------------|
| Query planning  | `get_file_slices()`                       | For snapshot query, get a list of file slices.                                 |
|                 | `get_file_slices_splits()`                | For snapshot query, get a list of file slices in splits.                       |
|                 | `get_file_slices_as_of()`                 | For time-travel query, get a list of file slices at a given time.              |
|                 | `get_file_slices_splits_as_of()`          | For time-travel query, get a list of file slices in splits at a given time.    |
|                 | `get_file_slices_between()`               | For incremental query, get a list of changed file slices between a time range. |
| Query execution | `create_file_group_reader_with_options()` | Create a file group reader instance with the table instance's configs.         |
| Query execution | `read_file_slice()`                       | Read a file slice, merging base and log files per the table's merge strategy.  |

## Contributing

Check out the [contributing guide](https://github.com/apache/hudi-rs/blob/main/CONTRIBUTING.md) for all the details about making contributions to the project.
