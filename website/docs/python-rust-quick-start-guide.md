---
title: "Python/Rust Quick Start"
toc: true
last_modified_at: 2024-11-28T12:53:57+08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you get started with [hudi-rs](https://github.com/apache/hudi-rs), a native Rust library for Apache Hudi with Python bindings. Learn how to install, set up, and perform basic operations using both Python and Rust interfaces.

## Installation

```bash
# Python
pip install hudi

# Rust
cargo add hudi
```

## Basic Usage

:::note
Currently, write capabilities and reading from MOR tables are not supported.

The examples below expect a Hudi table exists at `/tmp/trips_table`, created using the [quick start guide](/docs/quick-start-guide).
:::

### Python Example

```python
from hudi import HudiTableBuilder
import pyarrow as pa

hudi_table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .build()
)

# Read with partition filters
records = hudi_table.read_snapshot(filters=[("city", "=", "san_francisco")])

# Convert to PyArrow table
arrow_table = pa.Table.from_batches(records)
result = arrow_table.select(["rider", "city", "ts", "fare"])
```

### Rust Example (with DataFusion)

1. Set up your project:

```bash
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion@42
cargo add hudi --features datafusion
```

1. Add code to `src/main.rs`:

```rust
use std::sync::Arc;
use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new_with_options("/tmp/trips_table", []).await?;
    ctx.register_table("trips_table", Arc::new(hudi))?;
    // Read with partition filters
    let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
    df.show().await?;
    Ok(())
}
```

## Cloud Storage Integration

### Python

```python
from hudi import HudiTableBuilder

hudi_table = (
    HudiTableBuilder
    .from_base_uri("s3://bucket/trips_table")
    .with_option("aws_region", "us-west-2")
    .build()
)
```

### Rust

```rust
use hudi::HudiDataSource;

let hudi = HudiDataSource::new_with_options(
    "s3://bucket/trips_table",
    [("aws_region", "us-west-2")]
).await?;
```

### Supported Cloud Storage

- AWS S3 (`s3://`)
- Azure Storage (`az://`)
- Google Cloud Storage (`gs://`)

Set appropriate environment variables (`AWS_*`, `AZURE_*`, or `GOOGLE_*`) for authentication, or pass through the `option()` API.

## Read with Timestamp

Add timestamp option for time-travel queries:

```python
.with_option("hoodie.read.as.of.timestamp", "20241122010827898")
```
