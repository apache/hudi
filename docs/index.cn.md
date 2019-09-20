---
title: 什么是Hudi?
keywords: big data, stream processing, cloud, hdfs, storage, upserts, change capture
tags: [getting_started]
sidebar: mydoc_sidebar
permalink: index.html
summary: "Hudi为大数据带来流处理，在提供新数据的同时，比传统的批处理效率高出一个数量级。"
---

Hudi（发音为“hoodie”）摄取与管理处于DFS([HDFS](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) 或云存储)之上的大型分析数据集并为查询访问提供三个逻辑视图。

 * **读优化视图** - 在纯列式存储上提供出色的查询性能，非常像[parquet](https://parquet.apache.org/)表。
 * **增量视图** - 在数据集之上提供一个变更流并提供给下游的作业或ETL任务。
 * **准实时的表** - 提供对准实时数据的查询, 联合使用了基于行与列的存储 (例如 Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))



<figure>
    <img class="docimage" src="/images/hudi_intro_1.png" alt="hudi_intro_1.png" />
</figure>

通过仔细地管理数据在存储中的布局和如何将数据暴露给查询，Hudi能够为一个丰富的数据生态系统提供动力，在这个系统中，可以几乎实时地接收外部资源，并使其可用于[presto](https://prestodb.io)和[spark](https://spark.apache.org/sql/)等交互式SQL引擎，同时能够从处理/ETL框架（如[hive](https://hive.apache.org/)& [spark](https://spark.apache.org/docs/latest/)中进行增量消费以构建派生（Hudi）数据集。

Hudi 大体上由一个自包含的Spark库组成，它用于构建数据集并与现有的数据访问查询引擎集成。有关演示，请参见[快速启动](quickstart.html)。
