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

## Daft

[Daft](https://www.getdaft.io/) supports reading Hudi tables using `daft.read_hudi()` function.

```Python
# Read Apache Hudi table into a Daft DataFrame.
import daft

df = daft.read_hudi("some-table-uri")
df = df.where(df["foo"] > 5)
df.show()
```

Check out the Daft docs for [Hudi integration](https://docs.daft.ai/en/stable/connectors/hudi/).
