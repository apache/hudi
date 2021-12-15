---
title: Docker Demo
keywords: [ hudi, docker, demo]
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

## 一个使用 Docker 容器的 Demo

我们来使用一个真实世界的案例，来看看 Hudi 是如何闭环运转的。 为了这个目的，在你的计算机中的本地 Docker 集群中组建了一个自包含的数据基础设施。

以下步骤已经在一台 Mac 笔记本电脑上测试过了。

### 前提条件

  * Docker 安装 :  对于 Mac ，请依照 [https://docs.docker.com/v17.12/docker-for-mac/install/] 当中定义的步骤。 为了运行 Spark-SQL 查询，请确保至少分配给 Docker 6 GB 和 4 个 CPU 。（参见 Docker -> Preferences -> Advanced）。否则，Spark-SQL 查询可能被因为内存问题而被杀停。
  * kafkacat : 一个用于发布/消费 Kafka Topic 的命令行工具集。使用 `brew install kafkacat` 来安装 kafkacat 。
  * /etc/hosts : Demo 通过主机名引用了多个运行在容器中的服务。将下列设置添加到 /etc/hosts ：


```java
   127.0.0.1 adhoc-1
   127.0.0.1 adhoc-2
   127.0.0.1 namenode
   127.0.0.1 datanode1
   127.0.0.1 hiveserver
   127.0.0.1 hivemetastore
   127.0.0.1 kafkabroker
   127.0.0.1 sparkmaster
   127.0.0.1 zookeeper
```

此外，这未在其它一些环境中进行测试，例如 Windows 上的 Docker 。


## 设置 Docker 集群


### 构建 Hudi

构建 Hudi 的第一步：
```java
cd <HUDI_WORKSPACE>
mvn package -DskipTests
```

### 组建 Demo 集群

下一步是运行 Docker 安装脚本并设置配置项以便组建集群。
这需要从 Docker 镜像库拉取 Docker 镜像，并设置 Docker 集群。

```java
cd docker
./setup_demo.sh
....
....
....
Stopping spark-worker-1            ... done
Stopping hiveserver                ... done
Stopping hivemetastore             ... done
Stopping historyserver             ... done
.......
......
Creating network "hudi_demo" with the default driver
Creating hive-metastore-postgresql ... done
Creating namenode                  ... done
Creating zookeeper                 ... done
Creating kafkabroker               ... done
Creating hivemetastore             ... done
Creating historyserver             ... done
Creating hiveserver                ... done
Creating datanode1                 ... done
Creating presto-coordinator-1      ... done
Creating sparkmaster               ... done
Creating presto-worker-1           ... done
Creating adhoc-1                   ... done
Creating adhoc-2                   ... done
Creating spark-worker-1            ... done
Copying spark default config and setting up configs
Copying spark default config and setting up configs
Copying spark default config and setting up configs
$ docker ps
```

至此， Docker 集群将会启动并运行。 Demo 集群提供了下列服务：

   * HDFS 服务（ NameNode, DataNode ）
   * Spark Master 和 Worker
   * Hive 服务（ Metastore, HiveServer2 以及 PostgresDB ）
   * Kafka Broker 和一个 Zookeeper Node （ Kafka 将被用来当做 Demo 的上游数据源 ）
   * 用来运行 Hudi/Hive CLI 命令的 Adhoc 容器

## Demo

Stock Tracker 数据将用来展示不同的 Hudi 视图以及压缩带来的影响。

看一下 `docker/demo/data` 目录。那里有 2 批股票数据——都是 1 分钟粒度的。
第 1 批数据包含一些股票代码在交易窗口（9:30 a.m 至 10:30 a.m）的第一个小时里的行情数据数据。第 2 批包含接下来 30 分钟（10:30 - 11 a.m）的交易数据。 Hudi 将被用来将两个批次的数据采集到一个数据集中，这个数据集将会包含最新的小时级股票行情数据。
两个批次被有意地按窗口切分，这样在第 2 批数据中包含了一些针对第 1 批数据条目的更新数据。

### Step 1 : 将第 1 批数据发布到 Kafka

将第 1 批数据上传到 Kafka 的 Topic “stock ticks” 中 `cat docker/demo/data/batch_1.json | kafkacat -b kafkabroker -t stock_ticks -P`

为了检查新的 Topic 是否出现，使用
```java
kafkacat -b kafkabroker -L -J | jq .
{
  "originating_broker": {
    "id": 1001,
    "name": "kafkabroker:9092/1001"
  },
  "query": {
    "topic": "*"
  },
  "brokers": [
    {
      "id": 1001,
      "name": "kafkabroker:9092"
    }
  ],
  "topics": [
    {
      "topic": "stock_ticks",
      "partitions": [
        {
          "partition": 0,
          "leader": 1001,
          "replicas": [
            {
              "id": 1001
            }
          ],
          "isrs": [
            {
              "id": 1001
            }
          ]
        }
      ]
    }
  ]
}

```

### Step 2: 从 Kafka Topic 中增量采集数据

Hudi 自带一个名为 DeltaStreamer 的工具。 这个工具能连接多种数据源（包括 Kafka），以便拉取变更，并通过 upsert/insert 操作应用到 Hudi 数据集。此处，我们将使用这个工具从 Kafka Topic 下载 JSON 数据，并采集到前面步骤中初始化的 COW 和 MOR 表中。如果数据集不存在，这个工具将自动初始化数据集到文件系统中。

```java
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow dataset in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type COPY_ON_WRITE --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider


# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor dataset in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type MERGE_ON_READ --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider --disable-compaction


# As part of the setup (Look at setup_demo.sh), the configs needed for DeltaStreamer is uploaded to HDFS. The configs
# contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

你可以使用 HDFS 的 Web 浏览器来查看数据集
`http://namenode:50070/explorer#/user/hive/warehouse/stock_ticks_cow`.

你可以浏览在数据集中新创建的分区文件夹，同时还有一个在 .hoodie 目录下的 deltacommit 文件。

在 MOR 数据集中也有类似的设置
`http://namenode:50070/explorer#/user/hive/warehouse/stock_ticks_mor`


### Step 3: 与 Hive 同步

到了这一步，数据集在 HDFS 中可用。我们需要与 Hive 同步来创建新 Hive 表并添加分区，以便在那些数据集上执行 Hive 查询。

```java
docker exec -it adhoc-2 /bin/bash

# THis command takes in HIveServer URL and COW Hudi Dataset location in HDFS and sync the HDFS state to Hive
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_cow --database default --table stock_ticks_cow
.....
2018-09-24 22:22:45,568 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR storage)
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_mor --database default --table stock_ticks_mor
...
2018-09-24 22:23:09,171 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_mor
...
2018-09-24 22:23:09,559 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_mor_rt
....
exit
```
执行了以上命令后，你会发现：

1. 一个名为 `stock_ticks_cow` 的 Hive 表被创建，它为写时复制数据集提供了读优化视图。
2. 两个新表 `stock_ticks_mor` 和 `stock_ticks_mor_rt` 被创建用于读时合并数据集。 前者为 Hudi 数据集提供了读优化视图，而后者为数据集提供了实时视图。


### Step 4 (a): 运行 Hive 查询

执行一个 Hive 查询来为股票 GOOG 找到采集到的最新时间戳。你会注意到读优化视图（ COW 和 MOR 数据集都是如此）和实时视图（仅对 MOR 数据集）给出了相同的值 “10:29 a.m”，这是因为 Hudi 为每个批次的数据创建了一个 Parquet 文件。

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false
# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;
+---------------------+--+
|      tab_name       |
+---------------------+--+
| stock_ticks_cow     |
| stock_ticks_mor     |
| stock_ticks_mor_rt  |
+---------------------+--+
2 rows selected (0.801 seconds)
0: jdbc:hive2://hiveserver:10000>


# Look at partitions that were added
0: jdbc:hive2://hiveserver:10000> show partitions stock_ticks_mor_rt;
+----------------+--+
|   partition    |
+----------------+--+
| dt=2018-08-31  |
+----------------+--+
1 row selected (0.24 seconds)


# COPY-ON-WRITE Queries:
=========================


0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+

Now, run a projection query:

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924221953       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+


# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R dataset. Lets look at both
ReadOptimized and Realtime views supported by M-O-R dataset

# Run against ReadOptimized View. Notice that the latest timestamp is 10:29
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (6.326 seconds)


# Run against Realtime View. Notice that the latest timestamp is again 10:29

0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.606 seconds)


# Run projection query against Read Optimized and Realtime tables

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
exit
```

### Step 4 (b): 执行 Spark-SQL 查询
Hudi 支持以 Spark 作为类似 Hive 的查询引擎。这是在 Spartk-SQL 中执行与 Hive 相同的查询

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --master local[2] --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --executor-memory 3G --num-executors 1  --packages com.databricks:spark-avro_2.11:4.0.0
...

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_181)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
scala> spark.sql("show tables").show(100, false)
+--------+------------------+-----------+
|database|tableName         |isTemporary|
+--------+------------------+-----------+
|default |stock_ticks_cow   |false      |
|default |stock_ticks_mor   |false      |
|default |stock_ticks_mor_rt|false      |
+--------+------------------+-----------+

# Copy-On-Write Table

## Run max timestamp query against COW table

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
[Stage 0:>                                                          (0 + 1) / 1]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes#StaticLoggerBinder for further details.
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

## Projection Query

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924221953     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924221953     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R dataset. Lets look at both
ReadOptimized and Realtime views supported by M-O-R dataset

# Run against ReadOptimized View. Notice that the latest timestamp is 10:29
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


# Run against Realtime View. Notice that the latest timestamp is again 10:29

scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

# Run projection query against Read Optimized and Realtime tables

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924222155     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924222155     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924222155     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924222155     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

```

### Step 4 (c): 执行 Presto 查询

这是 Presto 查询，它们与 Hive 和 Spark 的查询类似。目前 Hudi 的实时视图不支持 Presto 。

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> show catalogs;
  Catalog
-----------
 hive
 jmx
 localfile
 system
(4 rows)

Query 20190817_134851_00000_j8rcz, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:04 [0 rows, 0B] [0 rows/s, 0B/s]

presto> use hive.default;
USE
presto:default> show tables;
       Table
--------------------
 stock_ticks_cow
 stock_ticks_mor
 stock_ticks_mor_rt
(3 rows)

Query 20190822_181000_00001_segyw, FINISHED, 2 nodes
Splits: 19 total, 19 done (100.00%)
0:05 [3 rows, 99B] [0 rows/s, 18B/s]


# COPY-ON-WRITE Queries:
=========================


presto:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181011_00002_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:12 [197 rows, 613B] [16 rows/s, 50B/s]

presto:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180221      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180221      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181141_00003_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [109 rows/s, 341B/s]


# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R dataset. 

# Run against ReadOptimized View. Notice that the latest timestamp is 10:29
presto:default> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181158_00004_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:02 [197 rows, 613B] [110 rows/s, 343B/s]


presto:default>  select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180250      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181256_00006_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [92 rows/s, 286B/s]

presto:default> exit
```

### Step 5: 将第 2 批次上传到 Kafka 并运行 DeltaStreamer 进行采集

上传第 2 批次数据，并使用 DeltaStreamer 采集。由于这个批次不会引入任何新分区，因此不需要执行 Hive 同步。

```java
cat docker/demo/data/batch_2.json | kafkacat -b kafkabroker -t stock_ticks -P

# Within Docker container, run the ingestion command
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow dataset in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type COPY_ON_WRITE --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider


# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor dataset in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type MERGE_ON_READ --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider --disable-compaction

exit
```

使用写时复制表， DeltaStreamer 的第 2 批数据采集将导致 Parquet 文件创建一个新版本。
参考： `http://namenode:50070/explorer#/user/hive/warehouse/stock_ticks_cow/2018/08/31`

使用读时合并表, 第 2 批数据采集仅仅将数据追加到没有合并的 delta （日志） 文件中。看一下 HDFS 文件系统来了解这一点： `http://namenode:50070/explorer#/user/hive/warehouse/stock_ticks_mor/2018/08/31`

### Step 6 (a): 执行 Hive 查询

使用写时复制表，在每一个批次被提交采集并创建新版本的 Parquet 文件时，读优化视图会立即发现变更，这些变更被当第 2 批次的一部分。

使用读时合并表，第 2 批数据采集仅仅将数据追加到没有合并的 delta （日志） 文件中。
此时，读优化视图和实时视图将提供不同的结果。读优化视图仍会返回“10:29 am”，因为它会只会从 Parquet 文件中读取。实时视图会做即时合并并返回最新提交的数据，即“10:59 a.m”。

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false

# Copy On Write Table:

0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.932 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224524       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized View
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Realtime View
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224537       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
exit
```

### Step 6 (b): 执行 Spark SQL 查询

以 Spark SQL 执行类似的查询：

```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages com.databricks:spark-avro_2.11:4.0.0

# Copy On Write Table:

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)

+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224524       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized View
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Realtime View
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224537       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
exit
```

### Step 6 (c): 执行 Presto 查询

在 Presto 中为读优化视图执行类似的查询：


```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> use hive.default;
USE

# Copy On Write Table:

presto:default>select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20190822_181530_00007_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:02 [197 rows, 613B] [125 rows/s, 389B/s]

presto:default>select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180221      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822181433      | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20190822_181545_00008_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [106 rows/s, 332B/s]

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized View
presto:default> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181602_00009_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [139 rows/s, 435B/s]

presto:default>select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180250      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181615_00010_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:01 [197 rows, 613B] [154 rows/s, 480B/s]

presto:default> exit
```


### Step 7 : 写时复制表的增量查询

使用采集的两个批次的数据，我们展示 Hudi 写时复制数据集中支持的增量查询。

我们使用类似的工程查询样例：

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064621       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```

正如你在上面的查询中看到的，有两个提交——按时间线排列是 20180924064621 和 20180924065039 。
当你按照这些步骤执行后，你的提交会得到不同的时间戳。将它们替换到上面时间戳的位置。

为了展示增量查询的影响，我们假设有一位读者已经在第 1 批数据中一部分看到了变化。那么，为了让读者看到第 2 批数据的影响，他/她需要保留第 1 批次提交时间中的开始时间（ 20180924064621 ）并执行增量查询：

Hudi 的增量模式为增量查询提供了高效的扫描，通过 Hudi 管理的元数据，过滤掉了那些不包含候选记录的文件。

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000>  set hoodie.stock_ticks_cow.consume.max.commits=3;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.start.timestamp=20180924064621;
```

使用上面的设置，那些在提交 20180924065039 之后没有任何更新的文件ID将被过滤掉，不进行扫描。
以下是增量查询：

```java
0: jdbc:hive2://hiveserver:10000>
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG' and `_hoodie_commit_time` > '20180924064621';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
1 row selected (0.83 seconds)
0: jdbc:hive2://hiveserver:10000>
```

### 使用 Spark SQL 做增量查询
```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages com.databricks:spark-avro_2.11:4.0.0
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_181)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions

# In the below query, 20180925045257 is the first commit's timestamp
scala> val hoodieIncViewDF =  spark.read.format("org.apache.hudi").option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL).option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20180924064621").load("/user/hive/warehouse/stock_ticks_cow")
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes#StaticLoggerBinder for further details.
hoodieIncViewDF: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 15 more fields]

scala> hoodieIncViewDF.registerTempTable("stock_ticks_cow_incr_tmp1")
warning: there was one deprecation warning; re-run with -deprecation for details

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow_incr_tmp1 where  symbol = 'GOOG'").show(100, false);
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

```


### Step 8: 为读时合并数据集的调度并执行压缩

我们来调度并运行一个压缩来创建一个新版本的列式文件，以便读优化读取器能看到新数据。
再次强调，你可以使用 Hudi CLI 来人工调度并执行压缩。

```java
docker exec -it adhoc-1 /bin/bash
root@adhoc-1:/opt#   /var/hoodie/ws/hudi-cli/hudi-cli.sh
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
hudi->connect --path /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:34 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/09/24 06:59:35 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:35 INFO util.FSUtils: Hadoop Configuration: fs.defaultFS: [hdfs://namenode:8020], Config:[Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml], FileSystem: [DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1261652683_11, ugi=root (auth:SIMPLE)]]]
18/09/24 06:59:35 INFO table.HoodieTableConfig: Loading dataset properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 06:59:36 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded

# Ensure no compactions are present

hoodie:stock_ticks_mor->compactions show all
18/09/24 06:59:54 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924064636__clean__COMPLETED], [20180924064636__deltacommit__COMPLETED], [20180924065057__clean__COMPLETED], [20180924065057__deltacommit__COMPLETED]]
    ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|




# Schedule a compaction. This will use Spark Launcher to schedule compaction
hoodie:stock_ticks_mor->compaction schedule
....
Compaction successfully completed for 20180924070031

# Now refresh and check again. You will see that there is a new compaction requested

hoodie:stock_ticks->connect --path /user/hive/warehouse/stock_ticks_mor
18/09/24 07:01:16 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 07:01:16 INFO util.FSUtils: Hadoop Configuration: fs.defaultFS: [hdfs://namenode:8020], Config:[Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml], FileSystem: [DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1261652683_11, ugi=root (auth:SIMPLE)]]]
18/09/24 07:01:16 INFO table.HoodieTableConfig: Loading dataset properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:01:16 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded



hoodie:stock_ticks_mor->compactions show all
18/09/24 06:34:12 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924041125__clean__COMPLETED], [20180924041125__deltacommit__COMPLETED], [20180924042735__clean__COMPLETED], [20180924042735__deltacommit__COMPLETED], [==>20180924063245__compaction__REQUESTED]]
    ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|
    | 20180924070031         | REQUESTED| 1                            |




# Execute the compaction. The compaction instant value passed below must be the one displayed in the above "compactions show all" query
hoodie:stock_ticks_mor->compaction run --compactionInstant  20180924070031 --parallelism 2 --sparkMemory 1G  --schemaFilePath /var/demo/config/schema.avsc --retry 1  
....
Compaction successfully completed for 20180924070031


## Now check if compaction is completed

hoodie:stock_ticks_mor->connect --path /user/hive/warehouse/stock_ticks_mor
18/09/24 07:03:00 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 07:03:00 INFO util.FSUtils: Hadoop Configuration: fs.defaultFS: [hdfs://namenode:8020], Config:[Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml], FileSystem: [DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1261652683_11, ugi=root (auth:SIMPLE)]]]
18/09/24 07:03:00 INFO table.HoodieTableConfig: Loading dataset properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:03:00 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded



hoodie:stock_ticks->compactions show all
18/09/24 07:03:15 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924064636__clean__COMPLETED], [20180924064636__deltacommit__COMPLETED], [20180924065057__clean__COMPLETED], [20180924065057__deltacommit__COMPLETED], [20180924070031__commit__COMPLETED]]
    ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|
    | 20180924070031         | COMPLETED| 1                            |

```

### Step 9: 执行包含增量查询的 Hive 查询

你将看到读优化视图和实时视图都会展示最新提交的数据。
让我们也对 MOR 表执行增量查询。
通过查看下方的查询输出，能够明确 MOR 表的第一次提交时间是 20180924064636 而第二次提交时间是 20180924070031 。

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false

# Read Optimized View
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Realtime View
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Incremental View:

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.mode=INCREMENTAL;
No rows affected (0.008 seconds)
# Max-Commits covers both second batch and compaction commit
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.max.commits=3;
No rows affected (0.007 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.start.timestamp=20180924064636;
No rows affected (0.013 seconds)
# Query:
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG' and `_hoodie_commit_time` > '20180924064636';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
exit
exit
```

### Step 10: 压缩后在 MOR 的读优化视图与实时视图上使用 Spark-SQL

```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages com.databricks:spark-avro_2.11:4.0.0

# Read Optimized View
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Realtime View
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```

### Step 11:  压缩后在 MOR 数据集的读优化视图上进行 Presto 查询

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> use hive.default;
USE

# Read Optimized View
resto:default> select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG';
  symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20190822_182319_00011_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [133 rows/s, 414B/s]

presto:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822181944      | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20190822_182333_00012_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [98 rows/s, 307B/s]

presto:default>

```


Demo 到此结束。

## 在本地 Docker 环境中测试 Hudi

你可以组建一个包含 Hadoop 、 Hive 和 Spark 服务的 Hadoop Docker 环境，并支持 Hudi 。
```java
$ mvn pre-integration-test -DskipTests
```
上面的命令为所有的服务构建了 Docker 镜像，它带有当前安装在 /var/hoodie/ws 的 Hudi 源，并使用一个部署文件引入了这些服务。我们当前在 Docker 镜像中使用 Hadoop （v2.8.4）、 Hive （v2.3.3）和 Spark （v2.3.1）。

要销毁容器：
```java
$ cd hudi-integ-test
$ mvn docker-compose:down
```

如果你想要组建 Docker 容器，使用：
```java
$ cd hudi-integ-test
$  mvn docker-compose:up -DdetachedMode=true
```

Hudi 是一个在包含 Hadoop 、 Hive 和 Spark 的海量数据分析/采集环境中使用的库。与这些系统的互用性是我们的一个关键目标。 我们在积极地向 __hudi-integ-test/src/test/java__ 添加集成测试，这些测试利用了这个 Docker 环境（参考： __hudi-integ-test/src/test/java/org/apache/hudi/integ/ITTestHoodieSanity.java__ ）。


### 构建本地 Docker 容器:

Demo 和执行集成测试所需要的 Docker 镜像已经在 Docker 源中。 Docker 镜像和部署脚本经过了谨慎的实现以便服务与多种目的：

1. Docker 镜像有内建的 Hudi jar 包，它包含一些指向其他 jar 包的环境变量（ HUDI_HADOOP_BUNDLE 等）
2. 为了执行集成测试，我们需要使用本地生成的 jar 包在 Docker 中运行服务。 Docker 部署脚本（参考 `docker/compose/docker-compose_hadoop284_hive233_spark231.yml`）能确保本地 jar 包通过挂载 Docker 地址上挂载本地 Hudi 工作空间，从而覆盖了内建的 jar 包。
3. 当这些 Docker 容器挂载到本地 Hudi 工作空间之后，任何发生在工作空间中的变更将会自动反映到容器中。这对于开发者来说是一种开发和验证 Hudi 的简便方法，这些开发者没有分布式的环境。要注意的是，这是集成测试的执行方式。

这避免了维护分离的 Docker 镜像，也避免了本地构建 Docker 镜像的各个步骤的消耗。
但是如果用户想要在有更低网络带宽的地方测试 Hudi ，他们仍可以构建本地镜像。
在执行 `docker/setup_demo.sh` 之前执行脚本 `docker/build_local_docker_images.sh` 来构建本地 Docker 镜像。

以下是执行的命令:

```java
cd docker
./build_local_docker_images.sh
.....

[INFO] Reactor Summary:
[INFO]
[INFO] hoodie ............................................. SUCCESS [  1.709 s]
[INFO] hudi-common ...................................... SUCCESS [  9.015 s]
[INFO] hudi-hadoop-mr ................................... SUCCESS [  1.108 s]
[INFO] hudi-client ...................................... SUCCESS [  4.409 s]
[INFO] hudi-hive ........................................ SUCCESS [  0.976 s]
[INFO] hudi-spark ....................................... SUCCESS [ 26.522 s]
[INFO] hudi-utilities ................................... SUCCESS [ 16.256 s]
[INFO] hudi-cli ......................................... SUCCESS [ 11.341 s]
[INFO] hudi-hadoop-mr-bundle ............................ SUCCESS [  1.893 s]
[INFO] hudi-hive-bundle ................................. SUCCESS [ 14.099 s]
[INFO] hudi-spark-bundle ................................ SUCCESS [ 58.252 s]
[INFO] hudi-hadoop-docker ............................... SUCCESS [  0.612 s]
[INFO] hudi-hadoop-base-docker .......................... SUCCESS [04:04 min]
[INFO] hudi-hadoop-namenode-docker ...................... SUCCESS [  6.142 s]
[INFO] hudi-hadoop-datanode-docker ...................... SUCCESS [  7.763 s]
[INFO] hudi-hadoop-history-docker ....................... SUCCESS [  5.922 s]
[INFO] hudi-hadoop-hive-docker .......................... SUCCESS [ 56.152 s]
[INFO] hudi-hadoop-sparkbase-docker ..................... SUCCESS [01:18 min]
[INFO] hudi-hadoop-sparkmaster-docker ................... SUCCESS [  2.964 s]
[INFO] hudi-hadoop-sparkworker-docker ................... SUCCESS [  3.032 s]
[INFO] hudi-hadoop-sparkadhoc-docker .................... SUCCESS [  2.764 s]
[INFO] hudi-integ-test .................................. SUCCESS [  1.785 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 09:15 min
[INFO] Finished at: 2018-09-10T17:47:37-07:00
[INFO] Final Memory: 236M/1848M
[INFO] ------------------------------------------------------------------------
```
