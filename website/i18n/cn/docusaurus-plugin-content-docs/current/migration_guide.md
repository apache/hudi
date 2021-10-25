---
title: 迁移指南
keywords: [ hudi, migration, use case, 迁移, 用例]
summary: 在本页中，我们将讨论有效的工具，他们能将你的现有数据集迁移到 Hudi 数据集。
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

Hudi 维护了元数据，包括提交的时间线和索引，来管理一个数据集。提交的时间线帮助理解一个数据集上发生的操作，以及数据集的当前状态。索引则被 Hudi 用来维护一个映射到文件 ID 的记录键，它能高效地定位一条记录。目前， Hudi 仅支持写 Parquet 列式格式 。

为了在你的现有数据集上开始使用 Hudi ，你需要将你的现有数据集迁移到 Hudi 管理的数据集中。以下有多种方法实现这个目的。


## 方法


### 将 Hudi 仅用于新分区

Hudi 可以被用来在不影响/改变数据集历史数据的情况下管理一个现有的数据集。 Hudi 已经实现为能够兼容这样的数据集，不论整个 Hive 分区是否由 Hudi 管理。因此， Hudi 管理一个数据集的最低粒度是一个 Hive 分区。使用数据源 API 或 WriteClient 来写入数据集，并确保你开始写入的是一个新分区，或者将过去的 N 个分区而非整张表转换为 Hudi 。需要注意的是，由于历史分区不是由 Hudi 管理的， Hudi 提供的任何操作在那些分区上都不生效。更具体地说，无法在这些非 Hudi 管理的旧分区上进行插入更新或增量拉取。

如果你的数据集是追加型的数据集，并且你不指望在已经存在的（或者非 Hudi 管理的）分区上进行更新操作，就使用这个方法。

### 将现有的数据集转换为 Hudi

将你的现有数据集导入到一个 Hudi 管理的数据集。由于全部数据都是 Hudi 管理的，方法 1 的任何限制在这里都无效了。跨分区的更新可以被应用到这个数据集，而 Hudi 会高效地让这些更新对查询可用。值得注意的是，你不仅可以在这个数据集上使用所有 Hudi 提供的操作，这样做还有额外的好处。 Hudi 会自动管理受管数据集的文件大小。你可以在转换数据集的时候设置期望的文件大小， Hudi 将确保它写出的文件符合这个配置。Hudi 还会确保小文件在后续被修正，这个过程是通过将新的插入引导到这些小文件而不是写入新的小文件来实现的，这样能维持你的集群的健康度。

选择这个方法后，有几种选择。

**选择 1**
使用 HDFSParquetImporter 工具。正如名字表明的那样，这仅仅适用于你的现有数据集是 Parquet 文件格式的。
这个工具本质上是启动一个 Spark 作业来读取现有的 Parquet 数据集，并通过重写全部记录的方式将它转换为 HUDI 管理的数据集。

**选择 2**
对于大数据集，这可以简单地： 
```java
for partition in [list of partitions in source dataset] {
        val inputDF = spark.read.format("any_input_format").load("partition_path")
        inputDF.write.format("org.apache.hudi").option()....save("basePath")
}
```  

**选择 3**
写下你自定义的逻辑来定义如何将现有数据集加载到一个 Hudi 管理的数据集中。请在 [这里](/cn/docs/quick-start-guide) 阅读 RDD API 的相关资料。使用 HDFSParquetImporter 工具。一旦 Hudi 通过 `mvn clean install -DskipTests` 被构建了， Shell 将被 `cd hudi-cli && ./hudi-cli.sh` 调启。

```java
hudi->hdfsparquetimport
        --upsert false
        --srcPath /user/parquet/dataset/basepath
        --targetPath
        /user/hoodie/dataset/basepath
        --tableName hoodie_table
        --tableType COPY_ON_WRITE
        --rowKeyField _row_key
        --partitionPathField partitionStr
        --parallelism 1500
        --schemaFilePath /user/table/schema
        --format parquet
        --sparkMemory 6g
        --retry 2
```
