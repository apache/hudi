---
title: Azure 文件系统
keywords: [ hudi, hive, azure, spark, presto]
summary: 在本页中，我们讨论如何在 Azure 文件系统中配置 Hudi 。
last_modified_at: 2020-05-25T19:00:57-04:00
language: cn
---
在本页中，我们解释如何在 Microsoft Azure 上使用 Hudi 。

## 声明

本页面由 Hudi 社区维护。
如果信息不准确，或者你有信息要补充，请尽管创建 JIRA ticket。
对此贡献高度赞赏。

## 支持的存储系统

Hudi 支持两种存储系统。

- Azure Blob 存储
- Azure Data Lake Gen 2

## 经过验证的 Spark 与存储系统的组合

#### Azure Data Lake Storage Gen 2 上的 HDInsight Spark 2.4
This combination works out of the box. No extra config needed.
这种组合开箱即用，不需要额外的配置。

#### Azure Data Lake Storage Gen 2 上的 Databricks Spark 2.4
- 将 Hudi jar 包导入到 databricks 工作区 。

- 将文件系统挂载到 dbutils 。
  ```scala
  dbutils.fs.mount(
    source = "abfss://xxx@xxx.dfs.core.windows.net",
    mountPoint = "/mountpoint",
    extraConfigs = configs)
  ```
- 当写入 Hudi 数据集时，使用 abfss URL
  ```scala
  inputDF.write
    .format("org.apache.hudi")
    .options(opts)
    .mode(SaveMode.Append)
    .save("abfss://<<storage-account>>.dfs.core.windows.net/hudi-tables/customer")
  ```
- 当读取 Hudi 数据集时，使用挂载点
  ```scala
  spark.read
    .format("org.apache.hudi")
    .load("/mountpoint/hudi-tables/customer")
  ```
