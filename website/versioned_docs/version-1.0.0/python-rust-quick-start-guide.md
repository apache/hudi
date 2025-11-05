---
title: "Python/Rust Quick Start"
toc: true
last_modified_at: 2024-11-28T12:53:57+08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you get started with [Hudi-rs](https://github.com/apache/hudi-rs), the native Rust implementation for Apache Hudi with Python bindings. Learn how to install, set up, and perform basic operations using both Python and Rust interfaces.

## Installation

```bash
# Python
pip install hudi

# Rust
cargo add hudi
```

## Usage Examples

> [!NOTE]
> These examples expect a Hudi table exists at `/tmp/trips_table`, created using
> the [quick start guide](quick-start-guide).

### Snapshot Query

Snapshot query reads the latest version of the data from the table. The table API also accepts partition filters.

#### Python

```python
from hudi import HudiTableBuilder
import pyarrow as pa

hudi_table = HudiTableBuilder.from_base_uri("/tmp/trips_table").build()
batches = hudi_table.read_snapshot(filters=[("city", "=", "san_francisco")])

# convert to PyArrow table
arrow_table = pa.Table.from_batches(batches)
result = arrow_table.select(["rider", "city", "ts", "fare"])
print(result)
```

#### Rust

```rust
use hudi::error::Result;
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use arrow::compute::concat_batches;

#[tokio::main]
async fn main() -> Result<()> {
    let hudi_table = HudiTableBuilder::from_base_uri("/tmp/trips_table").build().await?;
    let batches = hudi_table.read_snapshot(&[("city", "=", "san_francisco")]).await?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    let columns = vec!["rider", "city", "ts", "fare"];
    for col_name in columns {
        let idx = batch.schema().index_of(col_name).unwrap();
        println!("{}: {}", col_name, batch.column(idx));
    }
    Ok(())
}
```

To run read-optimized (RO) query on Merge-on-Read (MOR) tables, set `hoodie.read.use.read_optimized.mode` when creating the table.

#### Python

```python
hudi_table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.use.read_optimized.mode", "true")
    .build()
)
```

#### Rust

```rust
let hudi_table = 
    HudiTableBuilder::from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.use.read_optimized.mode", "true")
    .build().await?;
```

> [!NOTE]
> Currently reading MOR tables is limited to tables with Parquet data blocks.

### Time-Travel Query

Time-travel query reads the data at a specific timestamp from the table. The table API also accepts partition filters.

#### Python

```python
batches = (
    hudi_table
    .read_snapshot_as_of("20241231123456789", filters=[("city", "=", "san_francisco")])
)
```

#### Rust

```rust
let batches = 
    hudi_table
    .read_snapshot_as_of("20241231123456789", &[("city", "=", "san_francisco")]).await?;
```

### Incremental Query

Incremental query reads the changed data from the table for a given time range.

#### Python

```python
# read the records between t1 (exclusive) and t2 (inclusive)
batches = hudi_table.read_incremental_records(t1, t2)

# read the records after t1
batches = hudi_table.read_incremental_records(t1)
```

#### Rust

```rust
// read the records between t1 (exclusive) and t2 (inclusive)
let batches = hudi_table.read_incremental_records(t1, Some(t2)).await?;

// read the records after t1
let batches = hudi_table.read_incremental_records(t1, None).await?;
```

> [!NOTE]
> Currently the only supported format for the timestamp arguments is Hudi Timeline format: `yyyyMMddHHmmssSSS` or `yyyyMMddHHmmss`.

## Query Engine Integration

Hudi-rs provides APIs to support integration with query engines. The sections below highlight some commonly used APIs.

### Table API

Create a Hudi table instance using its constructor or the `TableBuilder` API.

| Stage           | API                                       | Description                                                                    |
|-----------------|-------------------------------------------|--------------------------------------------------------------------------------|
| Query planning  | `get_file_slices()`                       | For snapshot query, get a list of file slices.                                 |
|                 | `get_file_slices_splits()`                | For snapshot query, get a list of file slices in splits.                       |
|                 | `get_file_slices_as_of()`                 | For time-travel query, get a list of file slices at a given time.              |
|                 | `get_file_slices_splits_as_of()`          | For time-travel query, get a list of file slices in splits at a given time.    |
|                 | `get_file_slices_between()`               | For incremental query, get a list of changed file slices between a time range. |
| Query execution | `create_file_group_reader_with_options()` | Create a file group reader instance with the table instance's configs.         |

### File Group API

Create a Hudi file group reader instance using its constructor or the Hudi table API `create_file_group_reader_with_options()`.

| Stage           | API                                   | Description                                                                                                                                                                        |
|-----------------|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Query execution | `read_file_slice()`                   | Read records from a given file slice; based on the configs, read records from only base file, or from base file and log files, and merge records based on the configured strategy. |


### Apache DataFusion

Enabling the `hudi` crate with `datafusion` feature will provide a [DataFusion](https://datafusion.apache.org/) 
extension to query Hudi tables.

<details>
<summary>Add crate hudi with datafusion feature to your application to query a Hudi table.</summary>

```shell
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion@43
cargo add hudi --features datafusion
```

Update `src/main.rs` with the code snippet below then `cargo run`.

</details>

```rust
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new_with_options(
        "/tmp/trips_table",
        [("hoodie.read.input.partitions", "5")]).await?;
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
    df.show().await?;
    Ok(())
}
```

### Other Integrations

Hudi is also integrated with

- [Daft](https://www.getdaft.io/projects/docs/en/stable/integrations/hudi/)
- [Ray](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_hudi.html#ray.data.read_hudi)

### Work with cloud storage

Ensure cloud storage credentials are set properly as environment variables, e.g., `AWS_*`, `AZURE_*`, or `GOOGLE_*`.
Relevant storage environment variables will then be picked up. The target table's base uri with schemes such
as `s3://`, `az://`, or `gs://` will be processed accordingly.

Alternatively, you can pass the storage configuration as options via Table APIs.

#### Python

```python
from hudi import HudiTableBuilder

hudi_table = (
    HudiTableBuilder
    .from_base_uri("s3://bucket/trips_table")
    .with_option("aws_region", "us-west-2")
    .build()
)
```

#### Rust

```rust
use hudi::table::builder::TableBuilder as HudiTableBuilder;

async fn main() -> Result<()> {
    let hudi_table = 
        HudiTableBuilder::from_base_uri("s3://bucket/trips_table")
        .with_option("aws_region", "us-west-2")
        .build().await?;
}
```

## Contributing

Check out the [contributing guide](https://github.com/apache/hudi-rs/blob/main/CONTRIBUTING.md) for all the details about making contributions to the project.
