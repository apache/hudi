---
version: 0.5.2
title: 写入 Hudi 数据集
keywords: hudi, incremental, batch, stream, processing, Hive, ETL, Spark SQL
permalink: /cn/docs/0.5.2-writing_data.html
summary: In this page, we will discuss some available tools for incrementally ingesting & storing data.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

这一节我们将介绍使用[DeltaStreamer](#deltastreamer)工具从外部源甚至其他Hudi数据集摄取新更改的方法，
以及通过使用[Hudi数据源](#datasource-writer)的upserts加快大型Spark作业的方法。
对于此类数据集，我们可以使用各种查询引擎[查询](/cn/docs/0.5.2-querying_data.html)它们。

## 写操作

在此之前，了解Hudi数据源及delta streamer工具提供的三种不同的写操作以及如何最佳利用它们可能会有所帮助。
这些操作可以在针对数据集发出的每个提交/增量提交中进行选择/更改。

 - **UPSERT（插入更新）** ：这是默认操作，在该操作中，通过查找索引，首先将输入记录标记为插入或更新。
 在运行启发式方法以确定如何最好地将这些记录放到存储上，如优化文件大小之类后，这些记录最终会被写入。
 对于诸如数据库更改捕获之类的用例，建议该操作，因为输入几乎肯定包含更新。
 - **INSERT（插入）** ：就使用启发式方法确定文件大小而言，此操作与插入更新（UPSERT）非常相似，但此操作完全跳过了索引查找步骤。
 因此，对于日志重复数据删除等用例（结合下面提到的过滤重复项的选项），它可以比插入更新快得多。
 插入也适用于这种用例，这种情况数据集可以允许重复项，但只需要Hudi的事务写/增量提取/存储管理功能。
 - **BULK_INSERT（批插入）** ：插入更新和插入操作都将输入记录保存在内存中，以加快存储优化启发式计算的速度（以及其它未提及的方面）。
 所以对Hudi数据集进行初始加载/引导时这两种操作会很低效。批量插入提供与插入相同的语义，但同时实现了基于排序的数据写入算法，
 该算法可以很好地扩展数百TB的初始负载。但是，相比于插入和插入更新能保证文件大小，批插入在调整文件大小上只能尽力而为。

## DeltaStreamer

`HoodieDeltaStreamer`实用工具 (hudi-utilities-bundle中的一部分) 提供了从DFS或Kafka等不同来源进行摄取的方式，并具有以下功能。

 - 从Kafka单次摄取新事件，从Sqoop、HiveIncrementalPuller输出或DFS文件夹中的多个文件
 [增量导入](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html#_incremental_imports)
 - 支持json、avro或自定义记录类型的传入数据
 - 管理检查点，回滚和恢复
 - 利用DFS或Confluent [schema注册表](https://github.com/confluentinc/schema-registry)的Avro模式。
 - 支持自定义转换操作

命令行选项更详细地描述了这些功能：

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help
Usage: <main class> [options]
  Options:
    --commit-on-errors
        Commit even when some records failed to be written
      Default: false
    --enable-hive-sync
          Enable syncing to hive
       Default: false
    --filter-dupes
          Should duplicate records from source be dropped/filtered outbefore 
          insert/bulk-insert 
      Default: false
    --help, -h
    --hudi-conf
          Any configuration that can be set in the properties file (using the CLI 
          parameter "--propsFilePath") can also be passed command line using this 
          parameter 
          Default: []
    --op
      Takes one of these values : UPSERT (default), INSERT (use when input is
      purely new data/inserts to gain speed)
      Default: UPSERT
      Possible Values: [UPSERT, INSERT, BULK_INSERT]
    --payload-class
      subclass of HoodieRecordPayload, that works off a GenericRecord.
      Implement your own, if you want to do something other than overwriting
      existing value
      Default: org.apache.hudi.OverwriteWithLatestAvroPayload
    --props
      path to properties file on localfs or dfs, with configurations for
      Hudi client, schema provider, key generator and data source. For
      Hudi client props, sane defaults are used, but recommend use to
      provide basic things like metrics endpoints, hive configs etc. For
      sources, referto individual classes, for supported properties.
      Default: file:///Users/vinoth/bin/hoodie/src/test/resources/delta-streamer-config/dfs-source.properties
    --schemaprovider-class
      subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach
      schemas to input & target table data, built in options:
      FilebasedSchemaProvider
      Default: org.apache.hudi.utilities.schema.FilebasedSchemaProvider
    --source-class
      Subclass of org.apache.hudi.utilities.sources to read data. Built-in
      options: org.apache.hudi.utilities.sources.{JsonDFSSource (default),
      AvroDFSSource, JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}
      Default: org.apache.hudi.utilities.sources.JsonDFSSource
    --source-limit
      Maximum amount of data to read from source. Default: No limit For e.g:
      DFSSource => max bytes to read, KafkaSource => max events to read
      Default: 9223372036854775807
    --source-ordering-field
      Field within source record to decide how to break ties between records
      with same key in input data. Default: 'ts' holding unix timestamp of
      record
      Default: ts
    --spark-master
      spark master to use.
      Default: local[2]
  * --target-base-path
      base path for the target Hudi dataset. (Will be created if did not
      exist first time around. If exists, expected to be a Hudi dataset)
  * --target-table
      name of the target table in Hive
    --transformer-class
      subclass of org.apache.hudi.utilities.transform.Transformer. UDF to
      transform raw source dataset to a target dataset (conforming to target
      schema) before writing. Default : Not set. E:g -
      org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which
      allows a SQL query template to be passed as a transformation function)
```

该工具采用层次结构组成的属性文件，并具有可插拔的接口，用于提取数据、生成密钥和提供模式。
从Kafka和DFS摄取数据的示例配置在这里：`hudi-utilities/src/test/resources/delta-streamer-config`。

例如：当您让Confluent Kafka、Schema注册表启动并运行后，可以用这个命令产生一些测试数据
（[impressions.avro](https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data.html)，
由schema-registry代码库提供）

```java
[confluent-5.0.0]$ bin/ksql-datagen schema=../impressions.avro format=avro topic=impressions key=impressionid
```

然后用如下命令摄取这些数据。

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:///tmp/hudi-deltastreamer-op --target-table uber.impressions \
  --op BULK_INSERT
```

在某些情况下，您可能需要预先将现有数据集迁移到Hudi。 请参考[迁移指南](/cn/docs/0.5.2-migration_guide.html)。

## Datasource Writer

`hudi-spark`模块提供了DataSource API，可以将任何DataFrame写入（也可以读取）到Hudi数据集中。
以下是在指定需要使用的字段名称的之后，如何插入更新DataFrame的方法，这些字段包括
`recordKey => _row_key`、`partitionPath => partition`和`precombineKey => timestamp`

```java
inputDF.write()
       .format("org.apache.hudi")
       .options(clientOpts) // 可以传入任何Hudi客户端参数
       .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
       .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
       .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
       .option(HoodieWriteConfig.TABLE_NAME, tableName)
       .mode(SaveMode.Append)
       .save(basePath);
```

## 与Hive同步

上面的两个工具都支持将数据集的最新模式同步到Hive Metastore，以便查询新的列和分区。
如果需要从命令行或在独立的JVM中运行它，Hudi提供了一个`HiveSyncTool`，
在构建了hudi-hive模块之后，可以按以下方式调用它。

```java
cd hudi-hive
./run_sync_tool.sh
 [hudi-hive]$ ./run_sync_tool.sh --help
Usage: <main class> [options]
  Options:
  * --base-path
       Basepath of Hudi dataset to sync
  * --database
       name of the target database in Hive
    --help, -h
       Default: false
  * --jdbc-url
       Hive jdbc connect url
  * --pass
       Hive password
  * --table
       name of the target table in Hive
  * --user
       Hive username
```

## 删除数据 

通过允许用户指定不同的数据记录负载实现，Hudi支持对存储在Hudi数据集中的数据执行两种类型的删除。

 - **Soft Deletes（软删除）** ：使用软删除时，用户希望保留键，但仅使所有其他字段的值都为空。
 通过确保适当的字段在数据集模式中可以为空，并在将这些字段设置为null之后直接向数据集插入更新这些记录，即可轻松实现这一点。
 - **Hard Deletes（硬删除）** ：这种更强形式的删除是从数据集中彻底删除记录在存储上的任何痕迹。 
 这可以通过触发一个带有自定义负载实现的插入更新来实现，这种实现可以使用总是返回Optional.Empty作为组合值的DataSource或DeltaStreamer。 
 Hudi附带了一个内置的`org.apache.hudi.EmptyHoodieRecordPayload`类，它就是实现了这一功能。
 
```java
 deleteDF // 仅包含要删除的记录的DataFrame
   .write().format("org.apache.hudi")
   .option(...) // 根据设置需要添加HUDI参数，例如记录键、分区路径和其他参数
   // 指定record_key，partition_key，precombine_fieldkey和常规参数
   .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
 
```

## 存储管理

Hudi还对存储在Hudi数据集中的数据执行几个关键的存储管理功能。在DFS上存储数据的关键方面是管理文件大小和数量以及回收存储空间。 
例如，HDFS在处理小文件上性能很差，这会对Name Node的内存及RPC施加很大的压力，并可能破坏整个集群的稳定性。
通常，查询引擎可在较大的列文件上提供更好的性能，因为它们可以有效地摊销获得列统计信息等的成本。
即使在某些云数据存储上，列出具有大量小文件的目录也常常比较慢。

以下是一些有效管理Hudi数据集存储的方法。

 - Hudi中的[小文件处理功能](/cn/docs/0.5.2-configurations.html#compactionSmallFileSize)，可以分析传入的工作负载并将插入内容分配到现有文件组中，
 而不是创建新文件组。新文件组会生成小文件。
 - 可以[配置](/cn/docs/0.5.2-configurations.html#retainCommits)Cleaner来清理较旧的文件片，清理的程度可以调整，
 具体取决于查询所需的最长时间和增量拉取所需的回溯。
 - 用户还可以调整[基础/parquet文件](/cn/docs/0.5.2-configurations.html#limitFileSize)、[日志文件](/cn/docs/0.5.2-configurations.html#logFileMaxSize)的大小
 和预期的[压缩率](/cn/docs/0.5.2-configurations.html#parquetCompressionRatio)，使足够数量的插入被分到同一个文件组中，最终产生大小合适的基础文件。
 - 智能调整[批插入并行度](/cn/docs/0.5.2-configurations.html#withBulkInsertParallelism)，可以产生大小合适的初始文件组。
 实际上，正确执行此操作非常关键，因为文件组一旦创建后就不能删除，只能如前所述对其进行扩展。
 - 对于具有大量更新的工作负载，[读取时合并存储](/cn/docs/0.5.2-concepts.html#merge-on-read-storage)提供了一种很好的机制，
 可以快速将其摄取到较小的文件中，之后通过压缩将它们合并为较大的基础文件。
