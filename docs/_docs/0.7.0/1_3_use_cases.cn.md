---
version: 0.6.0
title: 使用案例
keywords: hudi, data ingestion, etl, real time, use cases
permalink: /cn/docs/0.6.0-use_cases.html
summary: "Following are some sample use-cases for Hudi, which illustrate the benefits in terms of faster processing & increased efficiency"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

以下是一些使用Hudi的示例，说明了加快处理速度和提高效率的好处

## 近实时摄取

将外部源(如事件日志、数据库、外部源)的数据摄取到[Hadoop数据湖](http://martinfowler.com/bliki/DataLake.html)是一个众所周知的问题。
尽管这些数据对整个组织来说是最有价值的，但不幸的是，在大多数(如果不是全部)Hadoop部署中都使用零散的方式解决，即使用多个不同的摄取工具。


对于RDBMS摄取，Hudi提供 __通过更新插入达到更快加载__，而不是昂贵且低效的批量加载。例如，您可以读取MySQL BIN日志或[Sqoop增量导入](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html#_incremental_imports)并将其应用于
DFS上的等效Hudi表。这比[批量合并任务](https://sqoop.apache.org/docs/1.4.0-incubating/SqoopUserGuide.html#id1770457)及[复杂的手工合并工作流](http://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/)更快/更有效率。


对于NoSQL数据存储，如[Cassandra](http://cassandra.apache.org/) / [Voldemort](http://www.project-voldemort.com/voldemort/) / [HBase](https://hbase.apache.org/)，即使是中等规模大小也会存储数十亿行。
毫无疑问， __全量加载不可行__，如果摄取需要跟上较高的更新量，那么则需要更有效的方法。


即使对于像[Kafka](kafka.apache.org)这样的不可变数据源，Hudi也可以 __强制在HDFS上使用最小文件大小__, 这采取了综合方式解决[HDFS小文件问题](https://blog.cloudera.com/blog/2009/02/the-small-files-problem/)来改善NameNode的健康状况。这对事件流来说更为重要，因为它通常具有较高容量(例如：点击流)，如果管理不当，可能会对Hadoop集群造成严重损害。

在所有源中，通过`commits`这一概念，Hudi增加了以原子方式向消费者发布新数据的功能，这种功能十分必要。

## 近实时分析

通常，实时[数据集市](https://en.wikipedia.org/wiki/Data_mart)由专业(实时)数据分析存储提供支持，例如[Druid](http://druid.io/)或[Memsql](http://www.memsql.com/)或[OpenTSDB](http://opentsdb.net/)。
这对于较小规模的数据量来说绝对是完美的([相比于这样安装Hadoop](https://blog.twitter.com/2015/hadoop-filesystem-at-twitter))，这种情况需要在亚秒级响应查询，例如系统监控或交互式实时分析。
但是，由于Hadoop上的数据太陈旧了，通常这些系统会被滥用于非交互式查询，这导致利用率不足和硬件/许可证成本的浪费。

另一方面，Hadoop上的交互式SQL解决方案(如Presto和SparkSQL)表现出色，在 __几秒钟内完成查询__。
通过将 __数据新鲜度提高到几分钟__，Hudi可以提供一个更有效的替代方案，并支持存储在DFS中的 __数量级更大的数据集__ 的实时分析。
此外，Hudi没有外部依赖(如专用于实时分析的HBase集群)，因此可以在更新的分析上实现更快的分析，而不会增加操作开销。


## 增量处理管道

Hadoop提供的一个基本能力是构建一系列数据集，这些数据集通过表示为工作流的DAG相互派生。
工作流通常取决于多个上游工作流输出的新数据，新数据的可用性传统上由新的DFS文件夹/Hive分区指示。
让我们举一个具体的例子来说明这点。上游工作流`U`可以每小时创建一个Hive分区，在每小时结束时(processing_time)使用该小时的数据(event_time)，提供1小时的有效新鲜度。
然后，下游工作流`D`在`U`结束后立即启动，并在下一个小时内自行处理，将有效延迟时间增加到2小时。

上面的示例忽略了迟到的数据，即`processing_time`和`event_time`分开时。
不幸的是，在今天的后移动和前物联网世界中，__来自间歇性连接的移动设备和传感器的延迟数据是常态，而不是异常__。
在这种情况下，保证正确性的唯一补救措施是[重新处理最后几个小时](https://falcon.apache.org/FalconDocumentation.html#Handling_late_input_data)的数据，
每小时一遍又一遍，这可能会严重影响整个生态系统的效率。例如; 试想一下，在数百个工作流中每小时重新处理TB数据。

Hudi通过以单个记录为粒度的方式(而不是文件夹/分区)从上游 Hudi数据集`HU`消费新数据(包括迟到数据)，来解决上面的问题。
应用处理逻辑，并使用下游Hudi数据集`HD`高效更新/协调迟到数据。在这里，`HU`和`HD`可以以更频繁的时间被连续调度
比如15分钟，并且`HD`提供端到端30分钟的延迟。

为实现这一目标，Hudi采用了类似于[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html#join-operations)、发布/订阅系统等流处理框架，以及像[Kafka](http://kafka.apache.org/documentation/#theconsumer)
或[Oracle XStream](https://docs.oracle.com/cd/E11882_01/server.112/e16545/xstrm_cncpt.htm#XSTRM187)等数据库复制技术的类似概念。
如果感兴趣，可以在[这里](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)找到有关增量处理(相比于流处理和批处理)好处的更详细解释。

## DFS的数据分发

一个常用场景是先在Hadoop上处理数据，然后将其分发回在线服务存储层，以供应用程序使用。
例如，一个Spark管道可以[确定Hadoop上的紧急制动事件](https://eng.uber.com/telematics/)并将它们加载到服务存储层(如ElasticSearch)中，供Uber应用程序使用以增加安全驾驶。这种用例中，通常架构会在Hadoop和服务存储之间引入`队列`，以防止目标服务存储被压垮。
对于队列的选择，一种流行的选择是Kafka，这个模型经常导致 __在DFS上存储相同数据的冗余(用于计算结果的离线分析)和Kafka(用于分发)__

通过将每次运行的Spark管道更新插入的输出转换为Hudi数据集，Hudi可以再次有效地解决这个问题，然后可以以增量方式获取尾部数据(就像Kafka topic一样)然后写入服务存储层。
