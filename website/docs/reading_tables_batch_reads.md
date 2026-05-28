---
title: Batch Reads
keywords: [hudi, spark, flink, batch, processing]
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Spark DataSource API

The `hudi-spark` module offers the DataSource API to read a Hudi table into a Spark DataFrame.

A time-travel query example:

```Scala
val tripsDF = spark.read.
    option("as.of.instant", "2021-07-28 14:11:08.000").
    format("hudi").
    load(basePath)
tripsDF.where(tripsDF.fare > 20.0).show()
```

### VECTOR, BLOB, and VARIANT Columns

Hudi exposes two Spark SQL extensions for reading the 1.2.0 column types:

- [`hudi_vector_search`](sql_queries.md#vector-similarity-search) runs top-K similarity search over
  a [`VECTOR`](sql_ddl.md#vector) column.
- [`read_blob()`](sql_queries.md#reading-blob-columns) materializes raw bytes from a
  [`BLOB`](sql_ddl.md#blob) column. Under the default `hoodie.read.blob.inline.mode=DESCRIPTOR`,
  calling `read_blob()` on an `INLINE` column throws; set the mode to `CONTENT` to read inline
  bytes.

[`VARIANT`](sql_ddl.md#variant) columns are read like ordinary columns. Cast to `STRING` for JSON
output.

## Flink Batch (Snapshot) Read

Flink can read a Hudi table as a snapshot (batch) query by leaving `read.streaming.enabled` at its default value of `false`.

```sql
CREATE TABLE hudi_table (
  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = '${path}',
  'table.type' = 'MERGE_ON_READ'
  -- read.streaming.enabled defaults to false → batch/snapshot read
);

-- Snapshot query
SELECT * FROM hudi_table WHERE age > 25;
```

For more Flink read options, see [Using Flink](ingestion_flink.md).

## Daft

[Daft](https://www.daft.ai/) supports reading Hudi tables using `daft.read_hudi()` function.

```Python
# Read Apache Hudi table into a Daft DataFrame.
import daft

df = daft.read_hudi("some-table-uri")
df = df.where(df["foo"] > 5)
df.show()
```

Check out the Daft docs for [Hudi integration](https://docs.daft.ai/en/latest/io/hudi/).
