---
title: Encryption
keywords: [ hudi, security ]
summary: This section offers an overview of encryption feature in Hudi
toc: true
last_modified_at: 2022-02-14T15:59:57-04:00
---

Since Hudi 0.11.0, Spark 3.2 support has been added and accompanying that, Parquet 1.12 has been included, which brings encryption feature to Hudi. In this section, we will show a guide on how to enable encryption in Hudi tables.

## Encrypt Copy-on-Write tables

First, make sure Hudi Spark 3.2 bundle jar is used.

Then, set the following Parquet configurations to make data written to Hudi COW tables encrypted.

```

```

An example table's result is shown below.



Read more from [Spark docs](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#columnar-encryption) and [Parquet docs](https://github.com/apache/parquet-format/blob/master/Encryption.md).

### Note

This feature is currently only available for COW tables due to only Parquet base files present there.