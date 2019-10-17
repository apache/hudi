---
title: 查询 Hudi 数据集
keywords: hudi, hive, spark, sql, presto
sidebar: mydoc_sidebar
permalink: querying_data.html
toc: false
summary: 在这一页里，我们介绍了如何在Hudi构建的表上启用SQL查询。
---

从概念上讲，Hudi物理存储一次数据到DFS上，同时在其上提供三个逻辑视图，如[之前](concepts.html#views)所述。
数据集同步到Hive Metastore后，它将提供由Hudi的自定义输入格式支持的Hive外部表。一旦提供了适当的Hudi捆绑包，
就可以通过Hive、Spark和Presto之类的常用查询引擎来查询数据集。

具体来说，在写入过程中传递了两个由[table name](configurations.html#TABLE_NAME_OPT_KEY)命名的Hive表。
例如，如果`table name = hudi_tbl`，我们得到

 - `hudi_tbl` 实现了由 `HoodieParquetInputFormat` 支持的数据集的读优化视图，从而提供了纯列式数据。
 - `hudi_tbl_rt` 实现了由 `HoodieParquetRealtimeInputFormat` 支持的数据集的实时视图，从而提供了基础数据和日志数据的合并视图。

如概念部分所述，[增量处理](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)所需要的
一个关键原语是`增量拉取`（以从数据集中获取更改流/日志）。您可以增量提取Hudi数据集，这意味着自指定的即时时间起，
您可以只获得全部更新和新行。 这与插入更新一起使用，对于构建某些数据管道尤其有用，包括将1个或多个源Hudi表（数据流/事实）以增量方式拉出（流/事实）
并与其他表（数据集/维度）结合以[写出增量](write_data.html)到目标Hudi数据集。增量视图是通过查询上表之一实现的，具有特殊配置，
该特殊配置指示查询计划仅需要从数据集中获取增量数据。

接下来，我们将详细讨论在每个查询引擎上如何访问所有三个视图。

## Hive

为了使Hive能够识别Hudi数据集并正确查询，
HiveServer2需要在其[辅助jars路径](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr)中提供`hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar`。 
这将确保输入格式类及其依赖项可用于查询计划和执行。

### 读优化表 {#hive-ro-view}
除了上述设置之外，对于beeline cli访问，还需要将`hive.input.format`变量设置为`org.apache.hudi.hadoop.HoodieParquetInputFormat`输入格式的完全限定路径名。
对于Tez，还需要将`hive.tez.input.format`设置为`org.apache.hadoop.hive.ql.io.HiveInputFormat`。

### 实时表 {#hive-rt-view}
除了在HiveServer2上安装Hive捆绑jars之外，还需要将其放在整个集群的hadoop/hive安装中，这样查询也可以使用自定义RecordReader。

### 增量拉取 {#hive-incr-pull}

`HiveIncrementalPuller`允许通过HiveQL从大型事实/维表中增量提取更改，
结合了Hive（可靠地处理复杂的SQL查询）和增量原语的好处（通过增量拉取而不是完全扫描来加快查询速度）。
该工具使用Hive JDBC运行hive查询并将其结果保存在临时表中，这个表可以被插入更新。
Upsert实用程序（`HoodieDeltaStreamer`）具有目录结构所需的所有状态，以了解目标表上的提交时间应为多少。
例如：`/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}`。
已注册的Delta Hive表的格式为`{tmpdb}.{source_table}_{last_commit_included}`。

以下是HiveIncrementalPuller的配置选项

| **配置** | **描述** | **默认值** |
|hiveUrl| 要连接的Hive Server 2的URL |  |
|hiveUser| Hive Server 2 用户名 |  |
|hivePass| Hive Server 2 密码 |  |
|queue| YARN 队列名称 |  |
|tmp| DFS中存储临时增量数据的目录。目录结构将遵循约定。请参阅以下部分。  |  |
|extractSQLFile| 在源表上要执行的提取数据的SQL。提取的数据将是自特定时间点以来已更改的所有行。 |  |
|sourceTable| 源表名称。在Hive环境属性中需要设置。 |  |
|targetTable| 目标表名称。中间存储目录结构需要。  |  |
|sourceDataPath| 源DFS基本路径。这是读取Hudi元数据的地方。 |  |
|targetDataPath| 目标DFS基本路径。 这是计算fromCommitTime所必需的。 如果显式指定了fromCommitTime，则不需要设置这个参数。 |  |
|tmpdb| 用来创建中间临时增量表的数据库 | hoodie_temp |
|fromCommitTime| 这是最重要的参数。 这是从中提取更改的记录的时间点。 |  |
|maxCommits| 要包含在拉取中的提交数。将此设置为-1将包括从fromCommitTime开始的所有提交。将此设置为大于0的值，将包括在fromCommitTime之后仅更改指定提交次数的记录。如果您需要一次赶上两次提交，则可能需要这样做。| 3 |
|help| 实用程序帮助 |  |


设置fromCommitTime=0和maxCommits=-1将提取整个源数据集，可用于启动回填。
如果目标数据集是Hudi数据集，则该实用程序可以确定目标数据集是否没有提交或延迟超过24小时（这是可配置的），
它将自动使用回填配置，因为增量应用最近24小时的更改会比回填花费更多的时间。
该工具当前的局限性在于缺乏在混合模式（正常模式和增量模式）下自联接同一表的支持。

**关于使用Fetch任务执行的Hive查询的说明：**
由于Fetch任务为每个分区调用InputFormat.listStatus()，每个listStatus()调用都会列出Hoodie元数据。
为了避免这种情况，如下操作可能是有用的，即使用Hive session属性对增量查询禁用Fetch任务：
`set hive.fetch.task.conversion = none;`。这将确保Hive查询使用Map Reduce执行，
合并分区（用逗号分隔），并且对所有这些分区仅调用一次InputFormat.listStatus()。

## Spark

Spark可将Hudi jars和捆绑包轻松部署和管理到作业/笔记本中。简而言之，通过Spark有两种方法可以访问Hudi数据集。

 - **Hudi DataSource**：支持读取优化和增量拉取，类似于标准数据源（例如：`spark.read.parquet`）的工作方式。
 - **以Hive表读取**：支持所有三个视图，包括实时视图，依赖于自定义的Hudi输入格式（再次类似Hive）。
 
通常，您的spark作业需要依赖`hudi-spark`或`hudi-spark-bundle-x.y.z.jar`，
它们必须位于驱动程序和执行程序的类路径上（提示：使用`--jars`参数）。
 
### 读优化表 {#spark-ro-view}

要使用SparkSQL将RO表读取为Hive表，只需按如下所示将路径过滤器推入sparkContext。
对于Hudi表，该方法保留了Spark内置的读取Parquet文件的优化功能，例如进行矢量化读取。

```
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```

如果您希望通过数据源在DFS上使用全局路径，则只需执行以下类似操作即可得到Spark数据帧。

```
Dataset<Row> hoodieROViewDF = spark.read().format("org.apache.hudi")
// pass any path glob, can include hudi & non-hudi datasets
.load("/glob/path/pattern");
```
 
### 实时表 {#spark-rt-view}
当前，实时表只能在Spark中作为Hive表进行查询。为了做到这一点，设置`spark.sql.hive.convertMetastoreParquet = false`，
迫使Spark回退到使用Hive Serde读取数据（计划/执行仍然是Spark）。

```
$ spark-shell --jars hudi-spark-bundle-x.y.z-SNAPSHOT.jar --driver-class-path /etc/hive/conf  --packages com.databricks:spark-avro_2.11:4.0.0 --conf spark.sql.hive.convertMetastoreParquet=false --num-executors 10 --driver-memory 7g --executor-memory 2g  --master yarn-client

scala> sqlContext.sql("select count(*) from hudi_rt where datestr = '2016-10-02'").show()
```

### 增量拉取 {#spark-incr-pull}
`hudi-spark`模块提供了DataSource API，这是一种从Hudi数据集中提取数据并通过Spark处理数据的更优雅的方法。
如下所示是一个示例增量拉取，它将获取自`beginInstantTime`以来写入的所有记录。

```
 Dataset<Row> hoodieIncViewDF = spark.read()
     .format("org.apache.hudi")
     .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(),
             DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
     .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),
            <beginInstantTime>)
     .load(tablePath); // For incremental view, pass in the root/base path of dataset
```

请参阅[设置](configurations.html#spark-datasource)部分，以查看所有数据源选项。

另外，`HoodieReadClient`通过Hudi的隐式索引提供了以下功能。

| **API** | **描述** |
| read(keys) | 使用Hudi自己的索通过快速查找将与键对应的数据作为DataFrame读出 |
| filterExists() | 从提供的RDD[HoodieRecord]中过滤出已经存在的记录。对删除重复数据有用 |
| checkExists(keys) | 检查提供的键是否存在于Hudi数据集中 |


## Presto

Presto是一种常用的查询引擎，可提供交互式查询性能。 Hudi RO表可以在Presto中无缝查询。
这需要在整个安装过程中将`hudi-presto-bundle` jar放入`<presto_install>/plugin/hive-hadoop2/`中。
