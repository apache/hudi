---
title: Quickstart
keywords: quickstart
tags: [quickstart]
sidebar: mydoc_sidebar
toc: false
permalink: quickstart.html
---


## Download Hoodie

Check out code and pull it into Intellij as a normal maven project.

Normally build the maven project, from command line
```
$ mvn clean install -DskipTests -DskipITs

To work with older version of Hive (pre Hive-1.2.1), use

$ mvn clean install -DskipTests -DskipITs -Dhive11

```

{% include callout.html content="You might want to add your spark jars folder to project dependencies under 'Module Setttings', to be able to run Spark from IDE" type="info" %}

{% include note.html content="Setup your local hadoop/hive test environment, so you can play with entire ecosystem. See [this](http://www.bytearray.io/2016/05/setting-up-hadoopyarnsparkhive-on-mac.html) for reference" %}


## Supported Versions

Hoodie requires Java 8 to be installed. Hoodie works with Spark-2.x versions. We have verified that hoodie works with the following combination of Hadoop/Hive/Spark.

| Hadoop | Hive  | Spark | Instructions to Build Hoodie | 
| ---- | ----- | ---- | ---- |
| 2.6.0-cdh5.7.2 | 1.1.0-cdh5.7.2 | spark-2.[1-3].x | Use "mvn clean install -DskipTests -Dhive11". Jars will have ".hive11" as suffix |
| Apache hadoop-2.8.4 | Apache hive-2.3.3 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |
| Apache hadoop-2.7.3 | Apache hive-1.2.1 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |

If your environment has other versions of hadoop/hive/spark, please try out hoodie and let us know if there are any issues. We are limited by our bandwidth to certify other combinations. 
It would be of great help if you can reach out to us with your setup and experience with hoodie.

## Generate a Hoodie Dataset

### Requirements & Environment Variable

Please set the following environment variablies according to your setup. We have given an example setup with CDH version

```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
export HIVE_HOME=/var/hadoop/setup/apache-hive-1.1.0-cdh5.7.2-bin
export HADOOP_HOME=/var/hadoop/setup/hadoop-2.6.0-cdh5.7.2
export HADOOP_INSTALL=/var/hadoop/setup/hadoop-2.6.0-cdh5.7.2
export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
export SPARK_HOME=/var/hadoop/setup/spark-2.3.1-bin-hadoop2.7
export SPARK_INSTALL=$SPARK_HOME
export SPARK_CONF_DIR=$SPARK_HOME/conf
export PATH=$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$SPARK_INSTALL/bin:$PATH
```

### DataSource API

Run __hoodie-spark/src/test/java/HoodieJavaApp.java__ class, to place a two commits (commit 1 => 100 inserts, commit 2 => 100 updates to previously inserted 100 records) onto your HDFS/local filesystem. Use the wrapper script
to run from command-line

```
cd hoodie-spark
./run_hoodie_app.sh --help
Usage: <main class> [options]
  Options:
    --help, -h
       Default: false
    --table-name, -n
       table name for Hoodie sample table
       Default: hoodie_rt
    --table-path, -p
       path for Hoodie sample table
       Default: file:///tmp/hoodie/sample-table
    --table-type, -t
       One of COPY_ON_WRITE or MERGE_ON_READ
       Default: COPY_ON_WRITE


```

The class lets you choose table names, output paths and one of the storage types. In your own applications, be sure to include the `hoodie-spark` module as dependency
and follow a similar pattern to write/read datasets via the datasource.

### RDD API

RDD level APIs give you more power and control over things, via the `hoodie-client` module .
Refer to  __hoodie-client/src/test/java/HoodieClientExample.java__ class for an example.



## Register Dataset to Hive Metastore

Now, lets see how we can publish this data into Hive.

#### Starting up Hive locally

```
hdfs namenode # start name node
hdfs datanode # start data node

bin/hive --service metastore  # start metastore
bin/hiveserver2 \
  --hiveconf hive.root.logger=INFO,console \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf ive.stats.autogather=false \
  --hiveconf hive.aux.jars.path=hoodie/packaging/hoodie-hadoop-mr-bundle/target/hoodie-hadoop-mr-bundle-0.4.3-SNAPSHOT.jar

```


#### Hive Sync Tool

Hive Sync Tool will update/create the necessary metadata(schema and partitions) in hive metastore.
This allows for schema evolution and incremental addition of new partitions written to.
It uses an incremental approach by storing the last commit time synced in the TBLPROPERTIES and only syncing the commits from the last sync commit time stored.
This can be run as frequently as the ingestion pipeline to make sure new partitions and schema evolution changes are reflected immediately.

```
cd hoodie-hive
./run_sync_tool.sh
  --user hive
  --pass hive 
  --database default 
  --jdbc-url "jdbc:hive2://localhost:10010/" 
  --base-path tmp/hoodie/sample-table/ 
  --table hoodie_test 
  --partitioned-by field1,field2

```



#### Manually via Beeline
Add in the hoodie-hadoop-mr-bundler jar so, Hive can read the Hoodie dataset and answer the query.
Also, For reading hoodie tables using hive, the following configs needs to be setup

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
hive> set hive.stats.autogather=false;
hive> add jar file:///tmp/hoodie-hadoop-mr-bundle-0.4.3.jar;
Added [file:///tmp/hoodie-hadoop-mr-bundle-0.4.3.jar] to class path
Added resources: [file:///tmp/hoodie-hadoop-mr-bundle-0.4.3.jar]
```

Then, you need to create a __ReadOptimized__ Hive table as below (only type supported as of now)and register the sample partitions

```
drop table hoodie_test;
CREATE EXTERNAL TABLE hoodie_test(`_row_key`  string,
`_hoodie_commit_time` string,
`_hoodie_commit_seqno` string,
 rider string,
 driver string,
 begin_lat double,
 begin_lon double,
 end_lat double,
 end_lon double,
 fare double)
PARTITIONED BY (`datestr` string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
   'com.uber.hoodie.hadoop.HoodieInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   'hdfs:///tmp/hoodie/sample-table';

ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2016-03-15') LOCATION 'hdfs:///tmp/hoodie/sample-table/2016/03/15';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-16') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/16';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-17') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/17';

set mapreduce.framework.name=yarn;
```

And you can generate a __Realtime__ Hive table, as below

```
DROP TABLE hoodie_rt;
CREATE EXTERNAL TABLE hoodie_rt(
`_hoodie_commit_time` string,
`_hoodie_commit_seqno` string,
`_hoodie_record_key` string,
`_hoodie_partition_path` string,
`_hoodie_file_name` string,
 timestamp double,
 `_row_key` string,
 rider string,
 driver string,
 begin_lat double,
 begin_lon double,
 end_lat double,
 end_lon double,
 fare double)
PARTITIONED BY (`datestr` string)
ROW FORMAT SERDE
   'com.uber.hoodie.hadoop.realtime.HoodieParquetSerde'
STORED AS INPUTFORMAT
   'com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   'file:///tmp/hoodie/sample-table';

ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2016-03-15') LOCATION 'file:///tmp/hoodie/sample-table/2016/03/15';
ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2015-03-16') LOCATION 'file:///tmp/hoodie/sample-table/2015/03/16';
ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2015-03-17') LOCATION 'file:///tmp/hoodie/sample-table/2015/03/17';

```



## Querying The Dataset

Now, we can proceed to query the dataset, as we would normally do across all the three query engines supported.

### HiveQL

Let's first perform a query on the latest committed snapshot of the table

```
hive> select count(*) from hoodie_test;
...
OK
100
Time taken: 18.05 seconds, Fetched: 1 row(s)
hive>
```

### SparkSQL

Spark is super easy, once you get Hive working as above. Just spin up a Spark Shell as below

```
$ cd $SPARK_INSTALL
$ spark-shell --jars $HUDI_SRC/packaging/hoodie-spark-bundle/target/hoodie-spark-bundle-0.4.3-SNAPSHOT.jar --driver-class-path $HADOOP_CONF_DIR  --conf spark.sql.hive.convertMetastoreParquet=false --packages com.databricks:spark-avro_2.11:4.0.0

scala> val sqlContext = new org.apache.spark.sql.SQLContext(sc)
scala> sqlContext.sql("show tables").show(10000)
scala> sqlContext.sql("describe hoodie_test").show(10000)
scala> sqlContext.sql("describe hoodie_rt").show(10000)
scala> sqlContext.sql("select count(*) from hoodie_test").show(10000)
```

You can also use the sample queries in __hoodie-utilities/src/test/java/HoodieSparkSQLExample.java__ for running on `hoodie_rt`

### Presto

Checkout the 'master' branch on OSS Presto, build it, and place your installation somewhere.

* Copy the hoodie-hadoop-mr-* jar into $PRESTO_INSTALL/plugin/hive-hadoop2/
* Startup your server and you should be able to query the same Hive table via Presto

```
show columns from hive.default.hoodie_test;
select count(*) from hive.default.hoodie_test
```



## Incremental Queries

Let's now perform a query, to obtain the __ONLY__ changed rows since a commit in the past.

```
hive> set hoodie.hoodie_test.consume.mode=INCREMENTAL;
hive> set hoodie.hoodie_test.consume.start.timestamp=001;
hive> set hoodie.hoodie_test.consume.max.commits=10;
hive> select `_hoodie_commit_time`, rider, driver from hoodie_test where `_hoodie_commit_time` > '001' limit 10;
OK
All commits :[001, 002]
002	rider-001	driver-001
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-001	driver-001
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-002	driver-002
002	rider-001	driver-001
Time taken: 0.056 seconds, Fetched: 10 row(s)
hive>
hive>
```


{% include note.html content="This is only supported for Read-optimized tables for now." %}


## A Demo using docker containers

Lets use a real world example to see how hudi works end to end. For this purpose, a self contained
data infrastructure is brought up in a local docker cluster within your computer. 

The steps assume you are using Mac laptop

### Prerequisites

  * Docker Setup :  For Mac, Please follow the steps as defined in [https://docs.docker.com/v17.12/docker-for-mac/install/]. For running Spark-SQL queries, please ensure atleast 6 GB and 4 CPUs are allocated to Docker (See Docker -> Preferences -> Advanced). Otherwise, spark-SQL queries could be killed because of memory issues.
  * kafkacat : A command-line utility to publish/consume from kafka topics. Use `brew install kafkacat` to install kafkacat
  * /etc/hosts : The demo references many services running in container by the hostname. Add the following settings to /etc/hosts
  
  ```
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

### Setting up Docker Cluster


#### Build Hoodie

The first step is to build hoodie
```
cd <HUDI_WORKSPACE>
mvn package -DskipTests
```

#### Bringing up Demo Cluster

The next step is to run the docker compose script and setup configs for bringing up the cluster.
This should pull the docker images from docker hub and setup docker cluster.

```
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
Creating sparkmaster               ... done
Creating adhoc-1                   ... done
Creating adhoc-2                   ... done
Creating spark-worker-1            ... done
Copying spark default config and setting up configs
Copying spark default config and setting up configs
Copying spark default config and setting up configs
varadarb-C02SG7Q3G8WP:docker varadarb$ docker ps
```

At this point, the docker cluster will be up and running. The demo cluster brings up the following services

   * HDFS Services (NameNode, DataNode)
   * Spark Master and Worker
   * Hive Services (Metastore, HiveServer2 along with PostgresDB)
   * Kafka Broker and a Zookeeper Node (Kakfa will be used as upstream source for the demo) 
   * Adhoc containers to run Hudi/Hive CLI commands

### Demo

Stock Tracker data will be used to showcase both different Hudi Views and the effects of Compaction. 

Take a look at the directory `docker/demo/data`. There are 2 batches of stock data - each at 1 minute granularity. 
The first batch contains stocker tracker data for some stock symbols during the first hour of trading window 
(9:30 a.m to 10:30 a.m). The second batch contains tracker data for next 30 mins (10:30 - 11 a.m). Hudi will
be used to ingest these batches to a dataset which will contain the latest stock tracker data at hour level granularity.
The batches are windowed intentionally so that the second batch contains updates to some of the rows in the first batch.

#### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks'

```
cat docker/demo/data/batch_1.json | kafkacat -b kafkabroker -t stock_ticks -P 

To check if the new topic shows up, use
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

#### Step 2: Incrementally ingest data from Kafka topic

Hudi comes with a tool named DeltaStreamer. This tool can connect to variety of data sources (including Kafka) to
pull changes and apply to Hudi dataset using upsert/insert primitives. Here, we will use the tool to download
json data from kafka topic and ingest to both COW and MOR tables we initialized in the previous step. This tool 
automatically initializes the datasets in the file-system if they do not exist yet.

```
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow dataset in HDFS
spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type COPY_ON_WRITE --source-class com.uber.hoodie.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties
....
....
2018-09-24 22:20:00 INFO  OutputCommitCoordinator$OutputCommitCoordinatorEndpoint:54 - OutputCommitCoordinator stopped!
2018-09-24 22:20:00 INFO  SparkContext:54 - Successfully stopped SparkContext
# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor dataset in HDFS
spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type MERGE_ON_READ --source-class com.uber.hoodie.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties
....
2018-09-24 22:22:01 INFO  OutputCommitCoordinator$OutputCommitCoordinatorEndpoint:54 - OutputCommitCoordinator stopped!
2018-09-24 22:22:01 INFO  SparkContext:54 - Successfully stopped SparkContext
....

# As part of the setup (Look at setup_demo.sh), the configs needed for DeltaStreamer is uploaded to HDFS. The configs
# contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

You can use HDFS web-browser to look at the datasets 
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow`. 

You can explore the new partition folder created in the dataset along with a "deltacommit"
file under .hoodie which signals a successful commit.

There will be a similar setup when you browse the MOR dataset
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor`


#### Step 3: Sync with Hive

At this step, the datasets are available in HDFS. We need to sync with Hive to create new Hive tables and add partitions
inorder to run Hive queries against those datasets.

```
docker exec -it adhoc-2 /bin/bash

# THis command takes in HIveServer URL and COW Hudi Dataset location in HDFS and sync the HDFS state to Hive
/var/hoodie/ws/hoodie-hive/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_cow --database default --table stock_ticks_cow
.....
2018-09-24 22:22:45,568 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR storage)
/var/hoodie/ws/hoodie-hive/run_sync_tool.sh  --jdbc-url jdbc:hive2://hiveserver:10000 --user hive --pass hive --partitioned-by dt --base-path /user/hive/warehouse/stock_ticks_mor --database default --table stock_ticks_mor
...
2018-09-24 22:23:09,171 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_mor
...
2018-09-24 22:23:09,559 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(112)) - Sync complete for stock_ticks_mor_rt
....
exit
```
After executing the above command, you will notice 

1. A hive table named `stock_ticks_cow` created which provides Read-Optimized view for the Copy On Write dataset.
2. Two new tables `stock_ticks_mor` and `stock_ticks_mor_rt` created for the Merge On Read dataset. The former
provides the ReadOptimized view for the Hudi dataset and the later provides the realtime-view for the dataset.


#### Step 4 (a): Run Hive Queries

Run a hive query to find the latest timestamp ingested for stock symbol 'GOOG'. You will notice that both read-optimized
(for both COW and MOR dataset)and realtime views (for MOR dataset)give the same value "10:29 a.m" as Hudi create a 
parquet file for the first batch of data.

```
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

#### Step 4 (b): Run Spark-SQL Queries
Hudi support Spark as query processor just like Hive. Here are the same hive queries
running in spark-sql

```
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


#### Step 5: Upload second batch to Kafka and run DeltaStreamer to ingest

Upload the second batch of data and ingest this batch using delta-streamer. As this batch does not bring in any new
partitions, there is no need to run hive-sync

```
cat docker/demo/data/batch_2.json | kafkacat -b kafkabroker -t stock_ticks -P 

# Within Docker container, run the ingestion command
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_cow dataset in HDFS
spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type COPY_ON_WRITE --source-class com.uber.hoodie.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_cow --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties

# Run the following spark-submit command to execute the delta-streamer and ingest to stock_ticks_mor dataset in HDFS
spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE --storage-type MERGE_ON_READ --source-class com.uber.hoodie.utilities.sources.JsonKafkaSource --source-ordering-field ts  --target-base-path /user/hive/warehouse/stock_ticks_mor --target-table stock_ticks_mor --props /var/demo/config/kafka-source.properties

exit
```
With Copy-On-Write table, the second ingestion by DeltaStreamer resulted in a new version of Parquet file getting created.
See `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow/2018/08/31`

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file. 
Take a look at the HDFS filesystem to get an idea: `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor/2018/08/31`

#### Step 6(a): Run Hive Queries

With Copy-On-Write table, the read-optimized view immediately sees the changes as part of second batch once the batch 
got committed as each ingestion creates newer versions of parquet files. 

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file. 
This is the time, when ReadOptimized and Realtime views will provide different results. ReadOptimized view will still
return "10:29 am" as it will only read from the Parquet file. Realtime View will do on-the-fly merge and return
latest committed data which is "10:59 a.m".

```
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

#### Step 6(b): Run Spark SQL Queries

Running the same queries in Spark-SQL:

```
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

#### Step 7 : Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write datasets

Lets take the same projection query example
```
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

`Hudi incremental mode` provides efficient scanning for incremental queries by filtering out files that do not have any 
candidate rows using hudi-managed metadata. 

```
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat --hiveconf hive.stats.autogather=false
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000>  set hoodie.stock_ticks_cow.consume.max.commits=3;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.start.timestamp=20180924064621;

# With the above setting, file-ids that do not have any updates from the commit 20180924065039 is filtered out without scanning.
# Here is the incremental query :

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

##### Incremental Query with Spark SQL:
```
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

scala> import com.uber.hoodie.DataSourceReadOptions
import com.uber.hoodie.DataSourceReadOptions

# In the below query, 20180925045257 is the first commit's timestamp
scala> val hoodieIncViewDF =  spark.read.format("com.uber.hoodie").option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL).option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20180924064621").load("/user/hive/warehouse/stock_ticks_cow")
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


#### Step 8: Schedule and Run Compaction for Merge-On-Read dataset

Lets schedule and run a compaction to create a new version of columnar  file so that read-optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```
docker exec -it adhoc-1 /bin/bash
^[[Aroot@adhoc-1:/opt#   /var/hoodie/ws/hoodie-cli/hoodie-cli.sh
============================================
*                                          *
*     _    _                 _ _           *
*    | |  | |               | (_)          *
*    | |__| | ___   ___   __| |_  ___      *
*    |  __  |/ _ \ / _ \ / _` | |/ _ \     *
*    | |  | | (_) | (_) | (_| | |  __/     *
*    |_|  |_|\___/ \___/ \__,_|_|\___|     *
*                                          *
============================================

Welcome to Hoodie CLI. Please type help if you are looking for help.
hoodie->connect --path /user/hive/warehouse/stock_ticks_mor
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

#### Step 9: Run Hive Queries including incremental queries

You will see that both ReadOptimized and Realtime Views will show the latest committed data.
Lets also run the incremental query for MOR table.
From looking at the below query output, it will be clear that the fist commit time for the MOR table is 20180924064636
and the second commit time is 20180924070031

```
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

##### Read Optimized and Realtime Views for MOR with Spark-SQL after compaction

```
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


This brings the demo to an end.

## Testing Hoodie in Local Docker environment

You can bring up a hadoop docker environment containing Hadoop, Hive and Spark services with support for hoodie.
```
$ mvn pre-integration-test -DskipTests
```
The above command builds docker images for all the services with 
current hoodie source installed at /var/hoodie/ws and also brings up the services using a compose file. We 
currently use Hadoop (v2.8.4), Hive (v2.3.3) and Spark (v2.3.1) in docker images. 

To bring down the containers
```
$ cd hoodie-integ-test
$ mvn docker-compose:down
```

If you want to bring up the docker containers, use
```
$ cd hoodie-integ-test
$  mvn docker-compose:up -DdetachedMode=true
```

Hoodie is a library that is operated in a broader data analytics/ingestion environment 
involving Hadoop, Hive and Spark. Interoperability with all these systems is a key objective for us. We are
actively adding integration-tests under __hoodie-integ-test/src/test/java__ that makes use of this 
docker environment (See __hoodie-integ-test/src/test/java/com/uber/hoodie/integ/ITTestHoodieSanity.java__ )


#### Building Local Docker Containers:

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

```
cd docker
./build_local_docker_images.sh
.....

[INFO] Reactor Summary:
[INFO]
[INFO] Hoodie ............................................. SUCCESS [  1.709 s]
[INFO] hoodie-common ...................................... SUCCESS [  9.015 s]
[INFO] hoodie-hadoop-mr ................................... SUCCESS [  1.108 s]
[INFO] hoodie-client ...................................... SUCCESS [  4.409 s]
[INFO] hoodie-hive ........................................ SUCCESS [  0.976 s]
[INFO] hoodie-spark ....................................... SUCCESS [ 26.522 s]
[INFO] hoodie-utilities ................................... SUCCESS [ 16.256 s]
[INFO] hoodie-cli ......................................... SUCCESS [ 11.341 s]
[INFO] hoodie-hadoop-mr-bundle ............................ SUCCESS [  1.893 s]
[INFO] hoodie-hive-bundle ................................. SUCCESS [ 14.099 s]
[INFO] hoodie-spark-bundle ................................ SUCCESS [ 58.252 s]
[INFO] hoodie-hadoop-docker ............................... SUCCESS [  0.612 s]
[INFO] hoodie-hadoop-base-docker .......................... SUCCESS [04:04 min]
[INFO] hoodie-hadoop-namenode-docker ...................... SUCCESS [  6.142 s]
[INFO] hoodie-hadoop-datanode-docker ...................... SUCCESS [  7.763 s]
[INFO] hoodie-hadoop-history-docker ....................... SUCCESS [  5.922 s]
[INFO] hoodie-hadoop-hive-docker .......................... SUCCESS [ 56.152 s]
[INFO] hoodie-hadoop-sparkbase-docker ..................... SUCCESS [01:18 min]
[INFO] hoodie-hadoop-sparkmaster-docker ................... SUCCESS [  2.964 s]
[INFO] hoodie-hadoop-sparkworker-docker ................... SUCCESS [  3.032 s]
[INFO] hoodie-hadoop-sparkadhoc-docker .................... SUCCESS [  2.764 s]
[INFO] hoodie-integ-test .................................. SUCCESS [  1.785 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 09:15 min
[INFO] Finished at: 2018-09-10T17:47:37-07:00
[INFO] Final Memory: 236M/1848M
[INFO] ------------------------------------------------------------------------
```