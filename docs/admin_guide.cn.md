---
title: 管理 Hudi Pipelines
keywords: hudi, administration, operation, devops
sidebar: mydoc_sidebar
permalink: admin_guide.html
toc: true
summary: 本节概述了可用于操作Hudi数据集生态系统的工具
---

管理员/运维人员可以通过以下方式了解Hudi数据集/管道

 - [通过Admin CLI进行管理](#admin-cli)
 - [Graphite指标](#metrics)
 - [Hudi应用程序的Spark UI](#spark-ui)

本节简要介绍了每一种方法，并提供了有关[故障排除](#troubleshooting)的一些常规指南

## Admin CLI {#admin-cli}

一旦构建了hudi，就可以通过`cd hudi-cli && ./hudi-cli.sh`启动shell。
一个hudi数据集位于DFS上的**basePath**位置，我们需要该位置才能连接到Hudi数据集。
Hudi库使用.hoodie子文件夹跟踪所有元数据，从而有效地在内部管理该数据集。

初始化hudi表，可使用如下命令。

```Java
18/09/06 15:56:52 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring
============================================
*                                          *
*     _    _           _   _               *
*    | |  | |         | | (_)              *
*    | |__| |       __| |  -               *
*    |  __  ||   | / _` | ||               *
*    | |  | ||   || (_| | ||               *
*    |_|  |_|\___/ \____/ ||               *
*                                          *
============================================

Welcome to Hoodie CLI. Please type help if you are looking for help.
hudi->create --path /user/hive/warehouse/table1 --tableName hoodie_table_1 --tableType COPY_ON_WRITE
.....
18/09/06 15:57:15 INFO table.HoodieTableMetaClient: Finished Loading Table of type COPY_ON_WRITE from ...
```

使用desc命令可以查看hudi表的描述信息:

```Java
hoodie:hoodie_table_1->desc
18/09/06 15:57:19 INFO timeline.HoodieActiveTimeline: Loaded instants []
    _________________________________________________________
    | Property                | Value                        |
    |========================================================|
    | basePath                | ...                          |
    | metaPath                | ...                          |
    | fileSystem              | hdfs                         |
    | hoodie.table.name       | hoodie_table_1               |
    | hoodie.table.type       | COPY_ON_WRITE                |
    | hoodie.archivelog.folder|                              |
```

以下是连接到包含uber trips的Hudi数据集的示例命令。

```Java
hoodie:trips->connect --path /app/uber/trips

16/10/05 23:20:37 INFO model.HoodieTableMetadata: Attempting to load the commits under /app/uber/trips/.hoodie with suffix .commit
16/10/05 23:20:37 INFO model.HoodieTableMetadata: Attempting to load the commits under /app/uber/trips/.hoodie with suffix .inflight
16/10/05 23:20:37 INFO model.HoodieTableMetadata: All commits :HoodieCommits{commitList=[20161002045850, 20161002052915, 20161002055918, 20161002065317, 20161002075932, 20161002082904, 20161002085949, 20161002092936, 20161002105903, 20161002112938, 20161002123005, 20161002133002, 20161002155940, 20161002165924, 20161002172907, 20161002175905, 20161002190016, 20161002192954, 20161002195925, 20161002205935, 20161002215928, 20161002222938, 20161002225915, 20161002232906, 20161003003028, 20161003005958, 20161003012936, 20161003022924, 20161003025859, 20161003032854, 20161003042930, 20161003052911, 20161003055907, 20161003062946, 20161003065927, 20161003075924, 20161003082926, 20161003085925, 20161003092909, 20161003100010, 20161003102913, 20161003105850, 20161003112910, 20161003115851, 20161003122929, 20161003132931, 20161003142952, 20161003145856, 20161003152953, 20161003155912, 20161003162922, 20161003165852, 20161003172923, 20161003175923, 20161003195931, 20161003210118, 20161003212919, 20161003215928, 20161003223000, 20161003225858, 20161004003042, 20161004011345, 20161004015235, 20161004022234, 20161004063001, 20161004072402, 20161004074436, 20161004080224, 20161004082928, 20161004085857, 20161004105922, 20161004122927, 20161004142929, 20161004163026, 20161004175925, 20161004194411, 20161004203202, 20161004211210, 20161004214115, 20161004220437, 20161004223020, 20161004225321, 20161004231431, 20161004233643, 20161005010227, 20161005015927, 20161005022911, 20161005032958, 20161005035939, 20161005052904, 20161005070028, 20161005074429, 20161005081318, 20161005083455, 20161005085921, 20161005092901, 20161005095936, 20161005120158, 20161005123418, 20161005125911, 20161005133107, 20161005155908, 20161005163517, 20161005165855, 20161005180127, 20161005184226, 20161005191051, 20161005193234, 20161005203112, 20161005205920, 20161005212949, 20161005223034, 20161005225920]}
Metadata for table trips loaded
hoodie:trips->
```

连接到数据集后，便可使用许多其他命令。该shell程序具有上下文自动完成帮助(按TAB键)，下面是所有命令的列表，本节中对其中的一些命令进行了详细示例。


```Java
hoodie:trips->help
* ! - Allows execution of operating system (OS) commands
* // - Inline comment markers (start of line only)
* ; - Inline comment markers (start of line only)
* addpartitionmeta - Add partition metadata to a dataset, if not present
* clear - Clears the console
* cls - Clears the console
* commit rollback - Rollback a commit
* commits compare - Compare commits with another Hoodie dataset
* commit showfiles - Show file level details of a commit
* commit showpartitions - Show partition level details of a commit
* commits refresh - Refresh the commits
* commits show - Show the commits
* commits sync - Compare commits with another Hoodie dataset
* connect - Connect to a hoodie dataset
* date - Displays the local date and time
* exit - Exits the shell
* help - List all commands usage
* quit - Exits the shell
* records deduplicate - De-duplicate a partition path contains duplicates & produce repaired files to replace with
* script - Parses the specified resource file and executes its commands
* stats filesizes - File Sizes. Display summary stats on sizes of files
* stats wa - Write Amplification. Ratio of how many records were upserted to how many records were actually written
* sync validate - Validate the sync by counting the number of records
* system properties - Shows the shell's properties
* utils loadClass - Load a class
* version - Displays shell version

hoodie:trips->
```


#### 检查提交

在Hudi中，更新或插入一批记录的任务被称为**提交**。提交可提供基本的原子性保证，即只有提交的数据可用于查询。
每个提交都有一个单调递增的字符串/数字，称为**提交编号**。通常，这是我们开始提交的时间。

查看有关最近10次提交的一些基本信息，


```Java
hoodie:trips->commits show --sortBy "Total Bytes Written" --desc true --limit 10
    ________________________________________________________________________________________________________________________________________________________________________
    | CommitTime    | Total Bytes Written| Total Files Added| Total Files Updated| Total Partitions Written| Total Records Written| Total Update Records Written| Total Errors|
    |=======================================================================================================================================================================|
    ....
    ....
    ....
hoodie:trips->
```

在每次写入开始时，Hudi还将.inflight提交写入.hoodie文件夹。您可以使用那里的时间戳来估计正在进行的提交已经花费的时间

```Java
$ hdfs dfs -ls /app/uber/trips/.hoodie/*.inflight
-rw-r--r--   3 vinoth supergroup     321984 2016-10-05 23:18 /app/uber/trips/.hoodie/20161005225920.inflight
```


#### 深入到特定的提交

了解写入如何分散到特定分区，


```Java
hoodie:trips->commit showpartitions --commit 20161005165855 --sortBy "Total Bytes Written" --desc true --limit 10
    __________________________________________________________________________________________________________________________________________
    | Partition Path| Total Files Added| Total Files Updated| Total Records Inserted| Total Records Updated| Total Bytes Written| Total Errors|
    |=========================================================================================================================================|
     ....
     ....
```

如果您需要文件级粒度，我们可以执行以下操作

```Java
hoodie:trips->commit showfiles --commit 20161005165855 --sortBy "Partition Path"
    ________________________________________________________________________________________________________________________________________________________
    | Partition Path| File ID                             | Previous Commit| Total Records Updated| Total Records Written| Total Bytes Written| Total Errors|
    |=======================================================================================================================================================|
    ....
    ....
```


#### 文件系统视图

Hudi将每个分区视为文件组的集合，每个文件组包含按提交顺序排列的文件切片列表(请参阅概念)。以下命令允许用户查看数据集的文件切片。

```Java
 hoodie:stock_ticks_mor->show fsview all
 ....
  _______________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
 | Partition | FileId | Base-Instant | Data-File | Data-File Size| Num Delta Files| Total Delta File Size| Delta Files |
 |==============================================================================================================================================================================================================================================================================================================================================================================================================|
 | 2018/08/31| 111415c3-f26d-4639-86c8-f9956f245ac3| 20181002180759| hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/111415c3-f26d-4639-86c8-f9956f245ac3_0_20181002180759.parquet| 432.5 KB | 1 | 20.8 KB | [HoodieLogFile {hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/.111415c3-f26d-4639-86c8-f9956f245ac3_20181002180759.log.1}]|



 hoodie:stock_ticks_mor->show fsview latest --partitionPath "2018/08/31"
 ......
 __________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
 | Partition | FileId | Base-Instant | Data-File | Data-File Size| Num Delta Files| Total Delta Size| Delta Size - compaction scheduled| Delta Size - compaction unscheduled| Delta To Base Ratio - compaction scheduled| Delta To Base Ratio - compaction unscheduled| Delta Files - compaction scheduled | Delta Files - compaction unscheduled|
 |=================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================|
 | 2018/08/31| 111415c3-f26d-4639-86c8-f9956f245ac3| 20181002180759| hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/111415c3-f26d-4639-86c8-f9956f245ac3_0_20181002180759.parquet| 432.5 KB | 1 | 20.8 KB | 20.8 KB | 0.0 B | 0.0 B | 0.0 B | [HoodieLogFile {hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/.111415c3-f26d-4639-86c8-f9956f245ac3_20181002180759.log.1}]| [] |

 hoodie:stock_ticks_mor->
```


#### 统计信息

由于Hudi直接管理DFS数据集的文件大小，这些信息会帮助你全面了解Hudi的运行状况


```Java
hoodie:trips->stats filesizes --partitionPath 2016/09/01 --sortBy "95th" --desc true --limit 10
    ________________________________________________________________________________________________
    | CommitTime    | Min     | 10th    | 50th    | avg     | 95th    | Max     | NumFiles| StdDev  |
    |===============================================================================================|
    | <COMMIT_ID>   | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 2       | 2.3 KB  |
    ....
    ....
```

如果Hudi写入花费的时间更长，那么可以通过观察写放大指标来发现任何异常

```Java
hoodie:trips->stats wa
    __________________________________________________________________________
    | CommitTime    | Total Upserted| Total Written| Write Amplifiation Factor|
    |=========================================================================|
    ....
    ....
```


#### 归档的提交

为了限制DFS上.commit文件的增长量，Hudi将较旧的.commit文件(适当考虑清理策略)归档到commits.archived文件中。
这是一个序列文件，其包含commitNumber => json的映射，及有关提交的原始信息(上面已很好地汇总了相同的信息)。

#### 压缩

要了解压缩和写程序之间的时滞，请使用以下命令列出所有待处理的压缩。

```Java
hoodie:trips->compactions show all
     ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|
    | <INSTANT_1>            | REQUESTED| 35                           |
    | <INSTANT_2>            | INFLIGHT | 27                           |
```

要检查特定的压缩计划，请使用

```Java
hoodie:trips->compaction show --instant <INSTANT_1>
    _________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | Partition Path| File Id | Base Instant  | Data File Path                                    | Total Delta Files| getMetrics                                                                                                                    |
    |================================================================================================================================================================================================================================================
    | 2018/07/17    | <UUID>  | <INSTANT_1>   | viewfs://ns-default/.../../UUID_<INSTANT>.parquet | 1                | {TOTAL_LOG_FILES=1.0, TOTAL_IO_READ_MB=1230.0, TOTAL_LOG_FILES_SIZE=2.51255751E8, TOTAL_IO_WRITE_MB=991.0, TOTAL_IO_MB=2221.0}|

```

要手动调度或运行压缩，请使用以下命令。该命令使用spark启动器执行压缩操作。
注意：确保没有其他应用程序正在同时调度此数据集的压缩

```Java
hoodie:trips->help compaction schedule
Keyword:                   compaction schedule
Description:               Schedule Compaction
 Keyword:                  sparkMemory
   Help:                   Spark executor memory
   Mandatory:              false
   Default if specified:   '__NULL__'
   Default if unspecified: '1G'

* compaction schedule - Schedule Compaction
```

```Java
hoodie:trips->help compaction run
Keyword:                   compaction run
Description:               Run Compaction for given instant time
 Keyword:                  tableName
   Help:                   Table name
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  parallelism
   Help:                   Parallelism for hoodie compaction
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  schemaFilePath
   Help:                   Path for Avro schema file
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  sparkMemory
   Help:                   Spark executor memory
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  retry
   Help:                   Number of retries
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  compactionInstant
   Help:                   Base path for the target hoodie dataset
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

* compaction run - Run Compaction for given instant time
```

##### 验证压缩

验证压缩计划：检查压缩所需的所有文件是否都存在且有效

```Java
hoodie:stock_ticks_mor->compaction validate --instant 20181005222611
...

   COMPACTION PLAN VALID

    ___________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | File Id                             | Base Instant Time| Base Data File                                                                                                                   | Num Delta Files| Valid| Error|
    |==========================================================================================================================================================================================================================|
    | 05320e98-9a57-4c38-b809-a6beaaeb36bd| 20181005222445   | hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/05320e98-9a57-4c38-b809-a6beaaeb36bd_0_20181005222445.parquet| 1              | true |      |



hoodie:stock_ticks_mor->compaction validate --instant 20181005222601

   COMPACTION PLAN INVALID

    _______________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | File Id                             | Base Instant Time| Base Data File                                                                                                                   | Num Delta Files| Valid| Error                                                                           |
    |=====================================================================================================================================================================================================================================================================================================|
    | 05320e98-9a57-4c38-b809-a6beaaeb36bd| 20181005222445   | hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/05320e98-9a57-4c38-b809-a6beaaeb36bd_0_20181005222445.parquet| 1              | false| All log files specified in compaction operation is not present. Missing ....    |
```

##### 注意

必须在其他写入/摄取程序没有运行的情况下执行以下命令。

有时，有必要从压缩计划中删除fileId以便加快或取消压缩操作。
压缩计划之后在此文件上发生的所有新日志文件都将被安全地重命名以便进行保留。Hudi提供以下CLI来支持


##### 取消调度压缩

```Java
hoodie:trips->compaction unscheduleFileId --fileId <FileUUID>
....
No File renames needed to unschedule file from pending compaction. Operation successful.
```

在其他情况下，需要撤销整个压缩计划。以下CLI支持此功能

```Java
hoodie:trips->compaction unschedule --compactionInstant <compactionInstant>
.....
No File renames needed to unschedule pending compaction. Operation successful.
```

##### 修复压缩

上面的压缩取消调度操作有时可能会部分失败(例如：DFS暂时不可用)。
如果发生部分故障，则压缩操作可能与文件切片的状态不一致。
当您运行`压缩验证`时，您会注意到无效的压缩操作(如果有的话)。
在这种情况下，修复命令将立即执行，它将重新排列文件切片，以使文件不丢失，并且文件切片与压缩计划一致

```Java
hoodie:stock_ticks_mor->compaction repair --instant 20181005222611
......
Compaction successfully repaired
.....
```


## 指标 {#metrics}

为Hudi Client配置正确的数据集名称和指标环境后，它将生成以下graphite指标，以帮助调试hudi数据集

 - **提交持续时间** - 这是成功提交一批记录所花费的时间
 - **回滚持续时间** - 同样，撤消失败的提交所剩余的部分数据所花费的时间(每次写入失败后都会自动发生)
 - **文件级别指标** - 显示每次提交中新增、版本、删除(清除)的文件数量
 - **记录级别指标** - 每次提交插入/更新的记录总数
 - **分区级别指标** - 更新的分区数量(对于了解提交持续时间的突然峰值非常有用)

然后可以将这些指标绘制在grafana等标准工具上。以下是提交持续时间图表示例。

<figure>
    <img class="docimage" src="/images/hudi_commit_duration.png" alt="hudi_commit_duration.png" style="max-width: 100%" />
</figure>


## 故障排除 {#troubleshooting}

以下部分通常有助于调试Hudi故障。以下元数据已被添加到每条记录中，可以通过标准Hadoop SQL引擎(Hive/Presto/Spark)检索，来更容易地诊断问题的严重性。

 - **_hoodie_record_key** - 作为每个DFS分区内的主键，是所有更新/插入的基础
 - **_hoodie_commit_time** - 该记录上次的提交
 - **_hoodie_file_name** - 包含记录的实际文件名(对检查重复非常有用)
 - **_hoodie_partition_path** - basePath的路径，该路径标识包含此记录的分区

请注意，到目前为止，Hudi假定应用程序为给定的recordKey传递相同的确定性分区路径。即仅在每个分区内保证recordKey(主键)的唯一性。

#### 缺失记录

请在可能写入记录的窗口中，使用上面的admin命令检查是否存在任何写入错误。
如果确实发现错误，那么记录实际上不是由Hudi写入的，而是交还给应用程序来决定如何处理。

#### 重复

首先，请确保访问Hudi数据集的查询是[没有问题的](sql_queries.html)，并之后确认的确有重复。

 - 如果确认，请使用上面的元数据字段来标识包含记录的物理文件和分区文件。
 - 如果重复的记录存在于不同分区路径下的文件，则意味着您的应用程序正在为同一recordKey生成不同的分区路径，请修复您的应用程序.
 - 如果重复的记录存在于同一分区路径下的多个文件，请使用邮件列表汇报这个问题。这不应该发生。您可以使用`records deduplicate`命令修复数据。

#### Spark故障 {#spark-ui}

典型的upsert() DAG如下所示。请注意，Hudi客户端会缓存中间的RDD，以智能地并调整文件大小和Spark并行度。
另外，由于还显示了探针作业，Spark UI显示了两次sortByKey，但它只是一个排序。
<figure>
    <img class="docimage" src="/images/hudi_upsert_dag.png" alt="hudi_upsert_dag.png" style="max-width: 100%" />
</figure>


概括地说，有两个步骤

**索引查找以标识要更改的文件**

 - Job 1 : 触发输入数据读取，转换为HoodieRecord对象，然后根据输入记录拿到目标分区路径。
 - Job 2 : 加载我们需要检查的文件名集。
 - Job 3  & 4 : 通过联合上面1和2中的RDD，智能调整spark join并行度，然后进行实际查找。
 - Job 5 : 生成带有位置的recordKeys作为标记的RDD。

**执行数据的实际写入**

 - Job 6 : 将记录与recordKey(位置)进行懒惰连接，以提供最终的HoodieRecord集，现在它包含每条记录的文件/分区路径信息(如果插入，则为null)。然后还要再次分析工作负载以确定文件的大小。
 - Job 7 : 实际写入数据(更新 + 插入 + 插入转为更新以保持文件大小)

根据异常源(Hudi/Spark)，上述关于DAG的信息可用于查明实际问题。最常遇到的故障是由YARN/DFS临时故障引起的。
将来，将在项目中添加更复杂的调试/管理UI，以帮助自动进行某些调试。