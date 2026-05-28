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

### LIMIT Push-Down with Source V2

When [Source V2](ingestion_flink.md#flink-source-v2) is enabled (`read.source-v2.enabled=true`), `LIMIT` clauses are pushed down to the source, reducing the number of files scanned:

```sql
SELECT * FROM hudi_table LIMIT 100;
```

Without Source V2, the `LIMIT` is applied after all data is read from storage. With Source V2 it is pushed to the split enumeration layer, stopping file scanning early.

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
