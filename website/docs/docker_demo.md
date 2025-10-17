---
title: Docker Demo
keywords: [ hudi, docker, demo]
toc: true
last_modified_at: 2025-09-26T17:59:57-04:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## A Demo using Docker containers

Let's use a real world example to see how Hudi works end to end. For this purpose, a self contained
data infrastructure is brought up in a local Docker cluster within your computer. It requires the
Hudi repo to have been cloned locally. 

The steps have been tested on a Mac laptop

### Prerequisites

  * Clone the [Hudi repository](https://github.com/apache/hudi) to your local machine.
  * Docker Setup :  For Mac, Please follow the steps as defined in [Install Docker Desktop on Mac](https://docs.docker.com/desktop/install/mac-install/). For running Spark-SQL queries, please ensure atleast 6 GB and 4 CPUs are allocated to Docker (See Docker -> Preferences -> Advanced). Otherwise, spark-SQL queries could be killed because of memory issues.
  * kcat : A command-line utility to publish/consume from kafka topics. Use `brew install kcat` to install kcat.
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
  * Java : Java SE Development Kit 8.
  * Maven : A build automation tool for Java projects.
  * jq : A lightweight and flexible command-line JSON processor. Use `brew install jq` to install jq.
  
Also, this has not been tested on some environments like Docker on Windows.


## Setting up Docker Cluster


### Build Hudi

The first step is to build Hudi. **Note** This step builds Hudi on supported scala version - 2.12.

NOTE: Make sure you've cloned the [Hudi repository](https://github.com/apache/hudi) first. 

```java
cd <HUDI_WORKSPACE>
mvn clean package -Pintegration-tests -DskipTests -Dspark3.5 -Dscala-2.12
```

### Bringing up Demo Cluster

The next step is to run the Docker compose script and setup configs for bringing up the cluster. These files are in the [Hudi repository](https://github.com/apache/hudi) which you should already have locally on your machine from the previous steps. 

<Tabs>
<TabItem value="Note">

<ul>
  <li> The demo must be built and run using the master branch. </li>
  <li> Presto and Trino are not supported in the current demo. </li>
</ul>

Build the required Docker images locally for this demo by running the following command.

```sh
cd docker
./build_docker_images.sh
```

This should setup the Docker cluster.

```java
cd docker
./setup_demo.sh
....
....
....
[+] Running 10/13
⠿ Container zookeeper             Removed                 8.6s
⠿ Container datanode1             Removed                18.3s
⠿ Container spark-worker-1        Removed                16.7s
⠿ Container adhoc-2               Removed                16.9s
⠿ Container graphite              Removed                16.9s
⠿ Container kafkabroker           Removed                14.1s
⠿ Container adhoc-1               Removed                14.1s
.......
......
[+] Running 13/13
⠿ adhoc-1 Pulled                                          2.9s
⠿ graphite Pulled                                         2.8s
⠿ spark-worker-1 Pulled                                   3.0s
⠿ kafka Pulled                                            2.9s
⠿ datanode1 Pulled                                        2.9s
⠿ hivemetastore Pulled                                    2.9s
⠿ hiveserver Pulled                                       3.0s
⠿ hive-metastore-postgresql Pulled                        2.8s
⠿ namenode Pulled                                         2.9s
⠿ sparkmaster Pulled                                      2.9s
⠿ zookeeper Pulled                                        2.8s
⠿ adhoc-2 Pulled                                          2.9s
⠿ historyserver Pulled                                    2.9s
[+] Running 13/13
⠿ Container zookeeper                  Started           41.0s
⠿ Container kafkabroker                Started           41.7s
⠿ Container graphite                   Started           41.5s
⠿ Container hive-metastore-postgresql  Running            0.0s
⠿ Container namenode                   Running            0.0s
⠿ Container hivemetastore              Running            0.0s
⠿ Container historyserver              Started           41.0s
⠿ Container datanode1                  Started           49.9s
⠿ Container hiveserver                 Running            0.0s
⠿ Container sparkmaster                Started           41.9s
⠿ Container spark-worker-1             Started           50.2s
⠿ Container adhoc-2                    Started           38.5s
⠿ Container adhoc-1                    Started           38.5s
Copying spark default config and setting up configs
Copying spark default config and setting up configs
$ docker ps
```

</TabItem>

</Tabs> 

At this point, the Docker cluster will be up and running. The demo cluster brings up the following services

   * HDFS Services (NameNode, DataNode)
   * Spark Master and Worker
   * Hive Services (Metastore, HiveServer2 along with PostgresDB)
   * Kafka Broker and a Zookeeper Node (Kafka will be used as upstream source for the demo)
   * Adhoc containers to run Hudi/Hive CLI commands

## Demo

Stock Tracker data will be used to showcase different Hudi query types and the effects of Compaction.

Take a look at the directory `docker/demo/data`. There are 2 batches of stock data - each at 1 minute granularity.
The first batch contains stocker tracker data for some stock symbols during the first hour of trading window
(9:30 a.m to 10:30 a.m). The second batch contains tracker data for next 30 mins (10:30 - 11 a.m). Hudi will
be used to ingest these batches to a table which will contain the latest stock tracker data at hour level granularity.
The batches are windowed intentionally so that the second batch contains updates to some of the rows in the first batch.

### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks' 

`cat demo/data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P`

To check if the new topic shows up, use
```java
kcat -b kafkabroker -L -J | jq .
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

Hudi comes with a tool named Hudi Streamer. This tool can connect to variety of data sources (including Kafka) to
pull changes and apply to Hudi table using upsert/insert primitives. Here, we will use the tool to download
json data from kafka topic and ingest to both COW and MOR tables we initialized in the previous step. This tool
automatically initializes the tables in the file-system if they do not exist yet.

```java
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_mor table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

# As part of the setup (Look at setup_demo.sh), the configs needed for Hudi Streamer is uploaded to HDFS. The configs
# contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

You can use HDFS web-browser to look at the tables
`http://namenode:9870/explorer.html#/user/hive/warehouse/stock_ticks_cow`.

You can explore the new partition folder created in the table along with a "commit" / "deltacommit"
file under .hoodie which signals a successful commit.

There will be a similar setup when you browse the MOR table
`http://namenode:9870/explorer.html#/user/hive/warehouse/stock_ticks_mor`


### Step 3: Sync with Hive

At this step, the tables are available in HDFS. We need to sync with Hive to create new Hive tables and add partitions
inorder to run Hive queries against those tables.

```java
docker exec -it adhoc-2 /bin/bash

# This command takes in HiveServer URL and COW Hudi table location in HDFS and sync the HDFS state to Hive
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_cow \
  --database default \
  --table stock_ticks_cow \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
.....
2025-09-26 13:57:58,718 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(281)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR table type)
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_mor \
  --database default \
  --table stock_ticks_mor \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
...
2025-09-26 13:58:36,052 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(281)) - Sync complete for stock_ticks_mor_ro
...
2025-09-26 13:58:36,184 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(281)) - Sync complete for stock_ticks_mor_rt
...
2025-09-26 13:58:36,308 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(281)) - Sync complete for stock_ticks_mor
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

beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.vectorized.input.format.excludes=org.apache.hudi.hadoop.HoodieParquetInputFormat \
  --hiveconf parquet.column.index.access=true

# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;
+---------------------+--+
|      tab_name       |
+---------------------+--+
| stock_ticks_cow     |
| stock_ticks_mor     |
| stock_ticks_mor_ro  |
| stock_ticks_mor_rt  |
+---------------------+--+
4 rows selected (1.099 seconds)
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
| 20250926135641514    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926135641514    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
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
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926135725397    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926135725397    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

### Step 4 (b): Run Spark-SQL Queries
Hudi support Spark as query processor just like Hive. Here are the same hive queries
running in spark-sql

```java
docker exec -it adhoc-1 /bin/bash

$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --master local[2] \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --executor-memory 3G \
  --num-executors 1
...

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 1.8.0_342)
Type in expressions to have them evaluated.
Type :help for more information.

scala> spark.sql("show tables").show(100, false)
+--------+------------------+-----------+
|database|tableName         |isTemporary|
+--------+------------------+-----------+
|default |stock_ticks_cow   |false      |
|default |stock_ticks_mor   |false      |
|default |stock_ticks_mor_ro|false      |
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
|20250926135641514  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20250926135641514  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
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
|20250926135725397  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20250926135725397  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20250926135725397  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20250926135725397  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+
```

### Step 5: Upload second batch to Kafka and run Hudi Streamer to ingest

Upload the second batch of data and ingest this batch using Hudi Streamer. As this batch does not bring in any new
partitions, there is no need to run hive-sync

```java
cat demo/data/batch_2.json | kcat -b kafkabroker -t stock_ticks -P

# Within Docker container, run the ingestion command
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_mor table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

exit
```

With Copy-On-Write table, the second ingestion by Hudi Streamer resulted in a new version of Parquet file getting created.
See `http://namenode:9870/explorer.html#/user/hive/warehouse/stock_ticks_cow/2018/08/31`

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
Take a look at the HDFS filesystem to get an idea: `http://namenode:9870/explorer.html#/user/hive/warehouse/stock_ticks_mor/2018/08/31`

### Step 6 (a): Run Hive Queries

With Copy-On-Write table, the Snapshot query immediately sees the changes as part of second batch once the batch
got committed as each ingestion creates newer versions of parquet files.

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
This is the time, when ReadOptimized and Snapshot queries will provide different results. ReadOptimized query will still
return "10:29 am" as it will only read from the Parquet file. Snapshot query will do on-the-fly merge and return
latest committed data which is "10:59 a.m".

```java
docker exec -it adhoc-2 /bin/bash

beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.vectorized.input.format.excludes=org.apache.hudi.hadoop.HoodieParquetInputFormat \
  --hiveconf parquet.column.index.access=true

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
| 20250926135641514    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141521148    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
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
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926135725397    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
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
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

### Step 6 (b): Run Spark SQL Queries

Running the same queries in Spark-SQL:

```java
docker exec -it adhoc-1 /bin/bash

$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

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
| 20250926135641514    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141521148    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |         _c1          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926135725397    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+

# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |         _c1          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+

exit
```

### Step 7 (a): Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write tables

Lets take the same projection query example

```java
docker exec -it adhoc-2 /bin/bash

beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.vectorized.input.format.excludes=org.apache.hudi.hadoop.HoodieParquetInputFormat \
  --hiveconf parquet.column.index.access=true


0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20250926135641514    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141521148    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```

As you notice from the above queries, there are 2 commits - 20250926135641514 and 20250926141521148 in timeline order.
When you follow the steps, you will be getting different timestamps for commits. Substitute them
in place of the above timestamps.

To show the effects of incremental-query, let us assume that a reader has already seen the changes as part of
ingesting first batch. Now, for the reader to see effect of the second batch, he/she has to keep the start timestamp to
the commit time of the first batch (20250926135641514) and run incremental query

Hudi incremental mode provides efficient scanning for incremental queries by filtering out files that do not have any
candidate rows using hudi-managed metadata.

```java
docker exec -it adhoc-2 /bin/bash

beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.vectorized.input.format.excludes=org.apache.hudi.hadoop.HoodieParquetInputFormat \
  --hiveconf parquet.column.index.access=true

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.max.commits=3;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.start.timestamp=20250926135641514;
```

With the above setting, file-ids that do not have any updates from the commit 20250926141521148 is filtered out without scanning.
Here is the incremental query :

```java
0: jdbc:hive2://hiveserver:10000>
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG' and `_hoodie_commit_time` > '20250926135641514';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20250926141521148    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
1 row selected (0.83 seconds)
0: jdbc:hive2://hiveserver:10000>
```

### Step 7 (b): Incremental Query with Spark SQL:

```java
docker exec -it adhoc-1 /bin/bash

$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 1.8.0_342)
Type in expressions to have them evaluated.
Type :help for more information.

# In the below query, 20250926135641514 is the first commit's timestamp
scala> val hoodieIncViewDF = spark.read.format("org.apache.hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", "20250926135641514").load("/user/hive/warehouse/stock_ticks_cow")
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes#StaticLoggerBinder for further details.
hoodieIncViewDF: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 15 more fields]

scala> hoodieIncViewDF.registerTempTable("stock_ticks_cow_incr_tmp1")
warning: there was one deprecation warning; re-run with -deprecation for details

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow_incr_tmp1 where  symbol = 'GOOG'").show(100, false);
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20250926141521148    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+
```

### Step 8: Schedule and Run Compaction for Merge-On-Read table

Lets schedule and run a compaction to create a new version of columnar  file so that read-optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```java
docker exec -it adhoc-1 /bin/bash

root@adhoc-1:/opt# /var/hoodie/ws/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
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
14512 [main] WARN  org.apache.hadoop.util.NativeCodeLoader [] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
14711 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
14711 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
14855 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=2) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded
hoodie:stock_ticks_mor->compactions show all
73614 [main] INFO  org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2 [] - Loaded instants upto : Option{val=[20250926141535482__20250926141539083__deltacommit__COMPLETED]}

╔═════════════════════════╤═══════╤═══════════════════════════════╗
║ Compaction Instant Time │ State │ Total FileIds to be Compacted ║
╠═════════════════════════╧═══════╧═══════════════════════════════╣
║ (empty)                                                         ║
╚═════════════════════════════════════════════════════════════════╝

# Schedule a compaction. This will use Spark Launcher to schedule compaction
hoodie:stock_ticks_mor->compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1
....
Attempted to schedule compaction for stock_ticks_mor

# Now refresh and check again. You will see that there is a new compaction requested

hoodie:stock_ticks_mor->refresh
185420 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
185420 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
185443 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=2) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor refreshed.

hoodie:stock_ticks_mor->compactions show all
216313 [main] INFO  org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2 [] - Loaded instants upto : Option{val=[==>20250926143925260__compaction__REQUESTED]}

╔═════════════════════════╤═══════════╤═══════════════════════════════╗
║ Compaction Instant Time │ State     │ Total FileIds to be Compacted ║
╠═════════════════════════╪═══════════╪═══════════════════════════════╣
║ 20250926143925260       │ REQUESTED │ 1                             ║
╚═════════════════════════╧═══════════╧═══════════════════════════════╝

# Execute the compaction. The compaction instant value passed below must be the one displayed in the above "compactions show all" query
hoodie:stock_ticks_mor->compaction run --compactionInstant  20250926143925260 --parallelism 2 --sparkMemory 1G  --schemaFilePath /var/demo/config/schema.avsc --retry 1  
....
Compaction successfully completed for 20250926143925260

## Now check if compaction is completed

hoodie:stock_ticks_mor->refresh
282367 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
282367 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
282383 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=2) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor refreshed.

hoodie:stock_ticks_mor->compactions show all
298704 [main] INFO  org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2 [] - Loaded instants upto : Option{val=[20250926143925260__20250926144127165__commit__COMPLETED]}

╔═════════════════════════╤═══════════╤═══════════════════════════════╗
║ Compaction Instant Time │ State     │ Total FileIds to be Compacted ║
╠═════════════════════════╪═══════════╪═══════════════════════════════╣
║ 20250926143925260       │ COMPLETED │ 1                             ║
╚═════════════════════════╧═══════════╧═══════════════════════════════╝

```

### Step 9: Run Hive Queries including incremental queries

You will see that both ReadOptimized and Snapshot queries will show the latest committed data.
Lets also run the incremental query for MOR table.
From looking at the below query output, it will be clear that the fist commit time for the MOR table is 20250926135725397
and the second commit time is 20250926141535482

```java
docker exec -it adhoc-2 /bin/bash

beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.vectorized.input.format.excludes=org.apache.hudi.hadoop.HoodieParquetInputFormat \
  --hiveconf parquet.column.index.access=true


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
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
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
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Incremental Query:

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.mode=INCREMENTAL;
No rows affected (0.008 seconds)
# Max-Commits covers both second batch and compaction commit
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.max.commits=3;
No rows affected (0.007 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.start.timestamp=20250926135725397;
No rows affected (0.013 seconds)
# Query:
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG' and `_hoodie_commit_time` > '20250926135725397';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

### Step 10: Read Optimized and Snapshot queries for MOR with Spark-SQL after compaction

```java
docker exec -it adhoc-1 /bin/bash

$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |        max(ts)       |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+

# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |     max(ts)          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20250926135725397    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20250926141535482    | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+
```

This brings the demo to an end.

## Testing Hudi in Local Docker environment

You can bring up a Hadoop Docker environment containing Hadoop, Hive and Spark services with support for Hudi.
```java
$ mvn pre-integration-test -DskipTests
```
The above command builds Docker images for all the services with
current Hudi source installed at /var/hoodie/ws and also brings up the services using a compose file. We
currently use Hadoop (v3.3.4), Hive (v3.1.3) and Spark (v3.5.3) in Docker images.

To bring down the containers
```java
$ cd hudi-integ-test
$ mvn docker-compose:down
```

If you want to bring up the Docker containers, use
```java
$ cd hudi-integ-test
$ mvn docker-compose:up -DdetachedMode=true
```

Hudi is a library that is operated in a broader data analytics/ingestion environment
involving Hadoop, Hive and Spark. Interoperability with all these systems is a key objective for us. We are
actively adding integration-tests under __hudi-integ-test/src/test/java__ that makes use of this
docker environment (See __hudi-integ-test/src/test/java/org/apache/hudi/integ/ITTestHoodieSanity.java__ )


### Building Local Docker Containers:

The Docker images required for demo and running integration test are already in docker-hub. The Docker images
and compose scripts are carefully implemented so that they serve dual-purpose

1. The Docker images have inbuilt Hudi jar files with environment variable pointing to those jars (HUDI_HADOOP_BUNDLE, ...)
2. For running integration-tests, we need the jars generated locally to be used for running services within docker. The
   docker-compose scripts (see `docker/compose/docker-compose_hadoop334_hive313_spark353_arm64.yml`) ensures local jars override
   inbuilt jars by mounting local Hudi workspace over the Docker location
3. As these Docker containers have mounted local Hudi workspace, any changes that happen in the workspace would automatically 
   reflect in the containers. This is a convenient way for developing and verifying Hudi for
   developers who do not own a distributed environment. Note that this is how integration tests are run.

This helps avoid maintaining separate Docker images and avoids the costly step of building Hudi Docker images locally.
But if users want to test Hudi from locations with lower network bandwidth, they can still build local images
run the script
`docker/build_local_docker_images.sh` to build local Docker images before running `docker/setup_demo.sh`

Here are the commands:

```java
cd docker
./build_local_docker_images.sh
.....

[INFO] Reactor Summary:
[INFO]
[INFO] Hudi ............................................... SUCCESS [  2.507 s]
[INFO] hudi-common ........................................ SUCCESS [ 15.181 s]
[INFO] hudi-aws ........................................... SUCCESS [  2.621 s]
[INFO] hudi-timeline-service .............................. SUCCESS [  1.811 s]
[INFO] hudi-client ........................................ SUCCESS [  0.065 s]
[INFO] hudi-client-common ................................. SUCCESS [  8.308 s]
[INFO] hudi-hadoop-mr ..................................... SUCCESS [  3.733 s]
[INFO] hudi-spark-client .................................. SUCCESS [ 18.567 s]
[INFO] hudi-sync-common ................................... SUCCESS [  0.794 s]
[INFO] hudi-hive-sync ..................................... SUCCESS [  3.691 s]
[INFO] hudi-spark-datasource .............................. SUCCESS [  0.121 s]
[INFO] hudi-spark-common_2.12 ............................. SUCCESS [ 12.979 s]
[INFO] hudi-spark2_2.12 ................................... SUCCESS [ 12.516 s]
[INFO] hudi-spark_2.12 .................................... SUCCESS [ 35.649 s]
[INFO] hudi-utilities_2.12 ................................ SUCCESS [  5.881 s]
[INFO] hudi-utilities-bundle_2.12 ......................... SUCCESS [ 12.661 s]
[INFO] hudi-cli ........................................... SUCCESS [ 19.858 s]
[INFO] hudi-java-client ................................... SUCCESS [  3.221 s]
[INFO] hudi-flink-client .................................. SUCCESS [  5.731 s]
[INFO] hudi-spark3_2.12 ................................... SUCCESS [  8.627 s]
[INFO] hudi-dla-sync ...................................... SUCCESS [  1.459 s]
[INFO] hudi-sync .......................................... SUCCESS [  0.053 s]
[INFO] hudi-hadoop-mr-bundle .............................. SUCCESS [  5.652 s]
[INFO] hudi-hive-sync-bundle .............................. SUCCESS [  1.623 s]
[INFO] hudi-spark-bundle_2.12 ............................. SUCCESS [ 10.930 s]
[INFO] hudi-presto-bundle ................................. SUCCESS [  3.652 s]
[INFO] hudi-timeline-server-bundle ........................ SUCCESS [  4.804 s]
[INFO] hudi-trino-bundle .................................. SUCCESS [  5.991 s]
[INFO] hudi-hadoop-docker ................................. SUCCESS [  2.061 s]
[INFO] hudi-hadoop-base-docker ............................ SUCCESS [ 53.372 s]
[INFO] hudi-hadoop-base-java11-docker ..................... SUCCESS [ 48.545 s]
[INFO] hudi-hadoop-namenode-docker ........................ SUCCESS [  6.098 s]
[INFO] hudi-hadoop-datanode-docker ........................ SUCCESS [  4.825 s]
[INFO] hudi-hadoop-history-docker ......................... SUCCESS [  3.829 s]
[INFO] hudi-hadoop-hive-docker ............................ SUCCESS [ 52.660 s]
[INFO] hudi-hadoop-sparkbase-docker ....................... SUCCESS [01:02 min]
[INFO] hudi-hadoop-sparkmaster-docker ..................... SUCCESS [ 12.661 s]
[INFO] hudi-hadoop-sparkworker-docker ..................... SUCCESS [  4.350 s]
[INFO] hudi-hadoop-sparkadhoc-docker ...................... SUCCESS [ 59.083 s]
[INFO] hudi-hadoop-presto-docker .......................... SUCCESS [01:31 min]
[INFO] hudi-hadoop-trinobase-docker ....................... SUCCESS [02:40 min]
[INFO] hudi-hadoop-trinocoordinator-docker ................ SUCCESS [ 14.003 s]
[INFO] hudi-hadoop-trinoworker-docker ..................... SUCCESS [ 12.100 s]
[INFO] hudi-integ-test .................................... SUCCESS [ 13.581 s]
[INFO] hudi-integ-test-bundle ............................. SUCCESS [ 27.212 s]
[INFO] hudi-examples ...................................... SUCCESS [  8.090 s]
[INFO] hudi-flink_2.12 .................................... SUCCESS [  4.217 s]
[INFO] hudi-kafka-connect ................................. SUCCESS [  2.966 s]
[INFO] hudi-flink-bundle_2.12 ............................. SUCCESS [ 11.155 s]
[INFO] hudi-kafka-connect-bundle .......................... SUCCESS [ 12.369 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  14:35 min
[INFO] Finished at: 2025-09-26T18:41:27-08:00
[INFO] ------------------------------------------------------------------------
```
