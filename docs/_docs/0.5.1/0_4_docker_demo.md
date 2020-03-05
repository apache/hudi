---
version: 0.5.1
title: Docker Demo
keywords: hudi, docker, demo
permalink: /docs/0.5.1-docker_demo.html
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

## A Demo using docker containers

Lets use a real world example to see how hudi works end to end. For this purpose, a self contained
data infrastructure is brought up in a local docker cluster within your computer.

The steps have been tested on a Mac laptop

### Prerequisites

  * Docker Setup :  For Mac, Please follow the steps as defined in [https://docs.docker.com/v17.12/docker-for-mac/install/]. For running Spark-SQL queries, please ensure atleast 6 GB and 4 CPUs are allocated to Docker (See Docker -> Preferences -> Advanced). Otherwise, spark-SQL queries could be killed because of memory issues.
  * kafkacat : A command-line utility to publish/consume from kafka topics. Use `brew install kafkacat` to install kafkacat
  * /etc/hosts : The demo references many services running in container by the hostname. Add the following settings to /etc/hosts


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

Also, this has not been tested on some environments like Docker on Windows.


## Setting up Docker Cluster


### Build Hudi

The first step is to build hudi. **Note** This step builds hudi on default supported scala version - 2.11.
```java
cd <HUDI_WORKSPACE>
mvn package -DskipTests
```

### Bringing up Demo Cluster

The next step is to run the docker compose script and setup configs for bringing up the cluster.
This should pull the docker images from docker hub and setup docker cluster.

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
Creating network "compose_default" with the default driver
Creating volume "compose_namenode" with default driver
Creating volume "compose_historyserver" with default driver
Creating volume "compose_hive-metastore-postgresql" with default driver
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

At this point, the docker cluster will be up and running. The demo cluster brings up the following services

   * HDFS Services (NameNode, DataNode)
   * Spark Master and Worker
   * Hive Services (Metastore, HiveServer2 along with PostgresDB)
   * Kafka Broker and a Zookeeper Node (Kakfa will be used as upstream source for the demo)
   * Adhoc containers to run Hudi/Hive CLI commands

## Demo

Stock Tracker data will be used to showcase different Hudi query types and the effects of Compaction.

Take a look at the directory `docker/demo/data`. There are 2 batches of stock data - each at 1 minute granularity.
The first batch contains stocker tracker data for some stock symbols during the first hour of trading window
(9:30 a.m to 10:30 a.m). The second batch contains tracker data for next 30 mins (10:30 - 11 a.m). Hudi will
be used to ingest these batches to a table which will contain the latest stock tracker data at hour level granularity.
The batches are windowed intentionally so that the second batch contains updates to some of the rows in the first batch.

### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks' `cat docker/demo/data/batch_1.json | kafkacat -b kafkabroker -t stock_ticks -P`

To check if the new topic shows up, use
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

### Step 2: Incrementally ingest data from Kafka topic

Hudi comes with a tool named DeltaStreamer. This tool can connect to variety of data sources (including Kafka) to
pull changes and apply to Hudi table using upsert/insert primitives. Here, we will use the tool to download
json data from kafka topic and ingest to both COW and MOR tables we initialized in the previous step. This tool
automatically initializes the tables in the file-system if they do not exist yet.

```java
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow table in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --table-type COPY_ON_WRITE --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider


# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor table in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --table-type MERGE_ON_READ --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider --disable-compaction


# As part of the setup (Look at setup_demo.sh), the configs needed for DeltaStreamer is uploaded to HDFS. The configs
# contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

You can use HDFS web-browser to look at the tables
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow`.

You can explore the new partition folder created in the table along with a "commit" / "deltacommit"
file under .hoodie which signals a successful commit.

There will be a similar setup when you browse the MOR table
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor`


### Step 3: Sync with Hive

At this step, the tables are available in HDFS. We need to sync with Hive to create new Hive tables and add partitions
inorder to run Hive queries against those tables.

```java
docker exec -it adhoc-2 /bin/bash

# THis command takes in HIveServer URL and COW Hudi table location in HDFS and sync the HDFS state to Hive
/var/hoodie/ws/hudi-hive/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_cow --database default --table stock_ticks_cow
.....
2020-01-25 19:51:28,953 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR table type)
/var/hoodie/ws/hudi-hive/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_mor --database default --table stock_ticks_mor
...
2020-01-25 19:51:51,066 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_ro
...
2020-01-25 19:51:51,569 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_rt
....
exit
```
After executing the above command, you will notice

1. A hive table named `stock_ticks_cow` created which supports Snapshot and Incremental queries on Copy On Write table.
2. Two new tables `stock_ticks_mor_rt` and `stock_ticks_mor_ro` created for the Merge On Read table. The former
supports Snapshot and Incremental queries (providing near-real time data) while the later supports ReadOptimized queries.


### Step 4 (a): Run Hive Queries

Run a hive query to find the latest timestamp ingested for stock symbol 'GOOG'. You will notice that both snapshot 
(for both COW and MOR _rt table) and read-optimized queries (for MOR _ro table) give the same value "10:29 a.m" as Hudi create a
parquet file for the first batch of data.

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false
# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;
+---------------------+--+
|      tab_name       |
+---------------------+--+
| stock_ticks_cow     |
| stock_ticks_mor_ro  |
| stock_ticks_mor_rt  |
+---------------------+--+
3 rows selected (1.199 seconds)
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

Lets run similar queries against M-O-R table. Lets look at both 
ReadOptimized and Snapshot(realtime data) queries supported by M-O-R table

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (6.326 seconds)


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.606 seconds)


# Run Read Optimized and Snapshot project queries

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
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

### Step 4 (b): Run Spark-SQL Queries
Hudi support Spark as query processor just like Hive. Here are the same hive queries
running in spark-sql

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --master local[2] --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --executor-memory 3G --num-executors 1  --packages org.apache.spark:spark-avro_2.11:2.4.4
...

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_212)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
scala> spark.sql("show tables").show(100, false)
+--------+------------------+-----------+
|database|tableName         |isTemporary|
+--------+------------------+-----------+
|default |stock_ticks_cow   |false      |
|default |stock_ticks_mor_ro|false      |
|default |stock_ticks_mor_rt|false      |
+--------+------------------+-----------+

# Copy-On-Write Table

## Run max timestamp query against COW table

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
[Stage 0:>                                                          (0 + 1) / 1]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
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

Lets run similar queries against M-O-R table. Lets look at both
ReadOptimized and Snapshot queries supported by M-O-R table

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

# Run Read Optimized and Snapshot project queries

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
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

### Step 4 (c): Run Presto Queries

Here are the Presto queries for similar Hive and Spark queries. Currently, Presto does not support snapshot or incremental queries on Hudi tables.

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
 stock_ticks_mor_ro
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

Lets run similar queries against M-O-R table. 

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
    presto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181158_00004_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:02 [197 rows, 613B] [110 rows/s, 343B/s]


presto:default>  select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
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

### Step 5: Upload second batch to Kafka and run DeltaStreamer to ingest

Upload the second batch of data and ingest this batch using delta-streamer. As this batch does not bring in any new
partitions, there is no need to run hive-sync

```java
cat docker/demo/data/batch_2.json | kafkacat -b kafkabroker -t stock_ticks -P

# Within Docker container, run the ingestion command
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow table in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --table-type COPY_ON_WRITE --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider


# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor table in HDFS
spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --table-type MERGE_ON_READ --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider --disable-compaction

exit
```

With Copy-On-Write table, the second ingestion by DeltaStreamer resulted in a new version of Parquet file getting created.
See `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow/2018/08/31`

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
Take a look at the HDFS filesystem to get an idea: `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor/2018/08/31`

### Step 6(a): Run Hive Queries

With Copy-On-Write table, the Snapshot query immediately sees the changes as part of second batch once the batch
got committed as each ingestion creates newer versions of parquet files.

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
This is the time, when ReadOptimized and Snapshot queries will provide different results. ReadOptimized query will still
return "10:29 am" as it will only read from the Parquet file. Snapshot query will do on-the-fly merge and return
latest committed data which is "10:59 a.m".

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

# Read Optimized Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
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

### Step 6(b): Run Spark SQL Queries

Running the same queries in Spark-SQL:

```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages org.apache.spark:spark-avro_2.11:2.4.4

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

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
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

### Step 6(c): Run Presto Queries

Running the same queries on Presto for ReadOptimized queries. 


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

# Read Optimized Query
presto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181602_00009_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [139 rows/s, 435B/s]

presto:default>select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
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


### Step 7 : Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write tables

Lets take the same projection query example

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

As you notice from the above queries, there are 2 commits - 20180924064621 and 20180924065039 in timeline order.
When you follow the steps, you will be getting different timestamps for commits. Substitute them
in place of the above timestamps.

To show the effects of incremental-query, let us assume that a reader has already seen the changes as part of
ingesting first batch. Now, for the reader to see effect of the second batch, he/she has to keep the start timestamp to
the commit time of the first batch (20180924064621) and run incremental query

Hudi incremental mode provides efficient scanning for incremental queries by filtering out files that do not have any
candidate rows using hudi-managed metadata.

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000>  set hoodie.stock_ticks_cow.consume.max.commits=3;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.start.timestamp=20180924064621;
```

With the above setting, file-ids that do not have any updates from the commit 20180924065039 is filtered out without scanning.
Here is the incremental query :

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

### Incremental Query with Spark SQL:
```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages org.apache.spark:spark-avro_2.11:2.4.4
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_212)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions

# In the below query, 20180925045257 is the first commit's timestamp
scala> val hoodieIncViewDF =  spark.read.format("org.apache.hudi").option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20180924064621").load("/user/hive/warehouse/stock_ticks_cow")
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
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


### Step 8: Schedule and Run Compaction for Merge-On-Read table

Lets schedule and run a compaction to create a new version of columnar  file so that read-optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```java
docker exec -it adhoc-1 /bin/bash
root@adhoc-1:/opt#   /var/hoodie/ws/hudi-cli/hudi-cli.sh
...
Table command getting loaded
HoodieSplashScreen loaded
===================================================================
*         ___                          ___                        *
*        /\__\          ___           /\  \           ___         *
*       / /  /         /\__\         /  \  \         /\  \        *
*      / /__/         / /  /        / /\ \  \        \ \  \       *
*     /  \  \ ___    / /  /        / /  \ \__\       /  \__\      *
*    / /\ \  /\__\  / /__/  ___   / /__/ \ |__|     / /\/__/      *
*    \/  \ \/ /  /  \ \  \ /\__\  \ \  \ / /  /  /\/ /  /         *
*         \  /  /    \ \  / /  /   \ \  / /  /   \  /__/          *
*         / /  /      \ \/ /  /     \ \/ /  /     \ \__\          *
*        / /  /        \  /  /       \  /  /       \/__/          *
*        \/__/          \/__/         \/__/    Apache Hudi CLI    *
*                                                                 *
===================================================================

Welcome to Apache Hudi CLI. Please type help if you are looking for help.
hudi->connect --path /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:34 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/09/24 06:59:35 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:35 INFO util.FSUtils: Hadoop Configuration: fs.defaultFS: [hdfs://namenode:8020], Config:[Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml], FileSystem: [DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1261652683_11, ugi=root (auth:SIMPLE)]]]
18/09/24 06:59:35 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 06:59:36 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
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
18/09/24 07:01:16 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:01:16 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
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
18/09/24 07:03:00 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:03:00 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded



hoodie:stock_ticks->compactions show all
18/09/24 07:03:15 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924064636__clean__COMPLETED], [20180924064636__deltacommit__COMPLETED], [20180924065057__clean__COMPLETED], [20180924065057__deltacommit__COMPLETED], [20180924070031__commit__COMPLETED]]
    ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|
    | 20180924070031         | COMPLETED| 1                            |

```

### Step 9: Run Hive Queries including incremental queries

You will see that both ReadOptimized and Snapshot queries will show the latest committed data.
Lets also run the incremental query for MOR table.
From looking at the below query output, it will be clear that the fist commit time for the MOR table is 20180924064636
and the second commit time is 20180924070031

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false

# Read Optimized Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
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

# Incremental Query:

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.mode=INCREMENTAL;
No rows affected (0.008 seconds)
# Max-Commits covers both second batch and compaction commit
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.max.commits=3;
No rows affected (0.007 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.start.timestamp=20180924064636;
No rows affected (0.013 seconds)
# Query:
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG' and `_hoodie_commit_time` > '20180924064636';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
exit
exit
```

### Step 10: Read Optimized and Snapshot queries for MOR with Spark-SQL after compaction

```java
docker exec -it adhoc-1 /bin/bash
bash-4.4# $SPARK_INSTALL/bin/spark-shell --jars $HUDI_SPARK_BUNDLE --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --master local[2] --executor-memory 3G --num-executors 1  --packages org.apache.spark:spark-avro_2.11:2.4.4

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
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

### Step 11:  Presto Read Optimized queries on MOR table after compaction

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> use hive.default;
USE

# Read Optimized Query
resto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
  symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20190822_182319_00011_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [133 rows/s, 414B/s]

presto:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
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


This brings the demo to an end.

## Testing Hudi in Local Docker environment

You can bring up a hadoop docker environment containing Hadoop, Hive and Spark services with support for hudi.
```java
$ mvn pre-integration-test -DskipTests
```
The above command builds docker images for all the services with
current Hudi source installed at /var/hoodie/ws and also brings up the services using a compose file. We
currently use Hadoop (v2.8.4), Hive (v2.3.3) and Spark (v2.4.4) in docker images.

To bring down the containers
```java
$ cd hudi-integ-test
$ mvn docker-compose:down
```

If you want to bring up the docker containers, use
```java
$ cd hudi-integ-test
$  mvn docker-compose:up -DdetachedMode=true
```

Hudi is a library that is operated in a broader data analytics/ingestion environment
involving Hadoop, Hive and Spark. Interoperability with all these systems is a key objective for us. We are
actively adding integration-tests under __hudi-integ-test/src/test/java__ that makes use of this
docker environment (See __hudi-integ-test/src/test/java/org/apache/hudi/integ/ITTestHoodieSanity.java__ )


### Building Local Docker Containers:

The docker images required for demo and running integration test are already in docker-hub. The docker images
and compose scripts are carefully implemented so that they serve dual-purpose

1. The docker images have inbuilt hudi jar files with environment variable pointing to those jars (HUDI_HADOOP_BUNDLE, ...)
2. For running integration-tests, we need the jars generated locally to be used for running services within docker. The
   docker-compose scripts (see `docker/compose/docker-compose_hadoop284_hive233_spark231.yml`) ensures local jars override
   inbuilt jars by mounting local HUDI workspace over the docker location

This helps avoid maintaining separate docker images and avoids the costly step of building HUDI docker images locally.
But if users want to test hudi from locations with lower network bandwidth, they can still build local images
run the script
`docker/build_local_docker_images.sh` to build local docker images before running `docker/setup_demo.sh`

Here are the commands:

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
