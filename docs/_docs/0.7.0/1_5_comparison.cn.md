---
version: 0.6.0
title: 对比
keywords: apache, hudi, kafka, kudu, hive, hbase, stream processing
permalink: /cn/docs/0.6.0-comparison.html
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

Apache Hudi填补了在DFS上处理数据的巨大空白，并可以和这些技术很好地共存。然而，
通过将Hudi与一些相关系统进行对比，来了解Hudi如何适应当前的大数据生态系统，并知晓这些系统在设计中做的不同权衡仍将非常有用。

## Kudu

[Apache Kudu](https://kudu.apache.org)是一个与Hudi具有相似目标的存储系统，该系统通过对`upserts`支持来对PB级数据进行实时分析。
一个关键的区别是Kudu还试图充当OLTP工作负载的数据存储，而Hudi并不希望这样做。
因此，Kudu不支持增量拉取(截至2017年初)，而Hudi支持以便进行增量处理。

Kudu与分布式文件系统抽象和HDFS完全不同，它自己的一组存储服务器通过RAFT相互通信。
与之不同的是，Hudi旨在与底层Hadoop兼容的文件系统(HDFS，S3或Ceph)一起使用，并且没有自己的存储服务器群，而是依靠Apache Spark来完成繁重的工作。
因此，Hudi可以像其他Spark作业一样轻松扩展，而Kudu则需要硬件和运营支持，特别是HBase或Vertica等数据存储系统。
到目前为止，我们还没有做任何直接的基准测试来比较Kudu和Hudi(鉴于RTTable正在进行中)。
但是，如果我们要使用[CERN](https://db-blog.web.cern.ch/blog/zbigniew-baranowski/2017-01-performance-comparison-different-file-formats-and-storage-engines)，
我们预期Hudi在摄取parquet上有更卓越的性能。

## Hive事务

[Hive事务/ACID](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions)是另一项类似的工作，它试图实现在ORC文件格式之上的存储`读取时合并`。
可以理解，此功能与Hive以及[LLAP](https://cwiki.apache.org/confluence/display/Hive/LLAP)之类的其他工作紧密相关。
Hive事务不提供Hudi提供的读取优化存储选项或增量拉取。
在实现选择方面，Hudi充分利用了类似Spark的处理框架的功能，而Hive事务特性则在用户或Hive Metastore启动的Hive任务/查询的下实现。
根据我们的生产经验，与其他方法相比，将Hudi作为库嵌入到现有的Spark管道中要容易得多，并且操作不会太繁琐。
Hudi还设计用于与Presto/Spark等非Hive引擎合作，并计划引入除parquet以外的文件格式。

## HBase

尽管[HBase](https://hbase.apache.org)最终是OLTP工作负载的键值存储层，但由于与Hadoop的相似性，用户通常倾向于将HBase与分析相关联。
鉴于HBase经过严格的写优化，它支持开箱即用的亚秒级更新，Hive-on-HBase允许用户查询该数据。 但是，就分析工作负载的实际性能而言，Parquet/ORC之类的混合列式存储格式可以轻松击败HBase，因为这些工作负载主要是读取繁重的工作。
Hudi弥补了更快的数据与分析存储格式之间的差距。从运营的角度来看，与管理分析使用的HBase region服务器集群相比，为用户提供可更快给出数据的库更具可扩展性。
最终，HBase不像Hudi这样重点支持`提交时间`、`增量拉取`之类的增量处理原语。

## 流式处理

一个普遍的问题："Hudi与流处理系统有何关系？"，我们将在这里尝试回答。简而言之，Hudi可以与当今的批处理(`写时复制存储`)和流处理(`读时合并存储`)作业集成，以将计算结果存储在Hadoop中。
对于Spark应用程序，这可以通过将Hudi库与Spark/Spark流式DAG直接集成来实现。在非Spark处理系统(例如Flink、Hive)情况下，可以在相应的系统中进行处理，然后通过Kafka主题/DFS中间文件将其发送到Hudi表中。从概念上讲，数据处理
管道仅由三个部分组成：`输入`，`处理`，`输出`，用户最终针对输出运行查询以便使用管道的结果。Hudi可以充当将数据存储在DFS上的输入或输出。Hudi在给定流处理管道上的适用性最终归结为你的查询在Presto/SparkSQL/Hive的适用性。

更高级的用例围绕[增量处理](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)的概念展开，
甚至在`处理`引擎内部也使用Hudi来加速典型的批处理管道。例如：Hudi可用作DAG内的状态存储(类似Flink使用的[rocksDB(https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends.html#the-rocksdbstatebackend))。
这是路线图上的一个项目并将最终以[Beam Runner](https://issues.apache.org/jira/browse/HUDI-60)的形式呈现。
