---
title: Microsoft Azure
keywords: [ hudi, hive, azure, spark, presto]
summary: In this page, we go over how to configure Hudi with Azure filesystem.
last_modified_at: 2020-05-25T19:00:57-04:00
---
In this page, we explain how to use Hudi on Microsoft Azure.

## Disclaimer

This page is maintained by the Hudi community.
If the information is inaccurate or you have additional information to add.
Please feel free to create a GitHub issue. Contribution is highly appreciated.

## Supported Storage System

There are two storage systems support Hudi .

- Azure Blob Storage
- Azure Data Lake Gen 2

## Verified Combination of Spark and storage system

#### HDInsight Spark2.4 on Azure Data Lake Storage Gen 2
This combination works out of the box. No extra config needed.

#### Databricks Spark2.4 on Azure Data Lake Storage Gen 2
- Import Hudi jar to databricks workspace

- Mount the file system to dbutils.
  ```scala
  dbutils.fs.mount(
    source = "abfss://xxx@xxx.dfs.core.windows.net",
    mountPoint = "/mountpoint",
    extraConfigs = configs)
  ```
- When writing Hudi dataset, use abfss URL
  ```scala
  inputDF.write
    .format("org.apache.hudi")
    .options(opts)
    .mode(SaveMode.Append)
    .save("abfss://<<storage-account>>.dfs.core.windows.net/hudi-tables/customer")
  ```
- When reading Hudi dataset, use the mounting point
  ```scala
  spark.read
    .format("org.apache.hudi")
    .load("/mountpoint/hudi-tables/customer")
  ```
