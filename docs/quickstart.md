---
title: Quickstart
keywords: quickstart
tags: [quickstart]
sidebar: mydoc_sidebar
permalink: quickstart.html
---


## Download Hoodie

Check out code and pull it into Intellij as a normal maven project.

Normally build the maven project, from command line
```
$ mvn clean install -DskipTests
```

{% include callout.html content="You might want to add your spark assembly jar to project dependencies under 'Module Setttings', to be able to run Spark from IDE" type="info" %}

{% include note.html content="Setup your local hadoop/hive test environment, so you can play with entire ecosystem. See [this](http://www.bytearray.io/2016/05/setting-up-hadoopyarnsparkhive-on-mac.html) for reference" %}



## Generate a Hoodie Dataset


You can run the __hoodie-client/src/test/java/HoodieClientExample.java__ class, to place a two commits (commit 1 => 100 inserts, commit 2 => 100 updates to previously inserted 100 records) onto your HDFS/local filesystem
```

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

The class lets you choose table names, output paths and one of the storage types.


## Register Dataset to Hive Metastore

Now, lets see how we can publish this data into Hive.

#### Starting up Hive locally

```
hdfs namenode # start name node
hdfs datanode # start data node

bin/hive --service metastore -p 10000 # start metastore
bin/hiveserver2 \
  --hiveconf hive.server2.thrift.port=10010 \
  --hiveconf hive.root.logger=INFO,console \
  --hiveconf hive.aux.jars.path=hoodie/hoodie-hadoop-mr/target/hoodie-hadoop-mr-0.3.6-SNAPSHOT.jar

```


#### Hive Sync Tool

Once Hive is up and running, the sync tool can be used to sync commits done above to a Hive table, as follows.

```
java -cp target/hoodie-hive-0.3.1-SNAPSHOT-jar-with-dependencies.jar:target/jars/* com.uber.hoodie.hive.HiveSyncTool \
  --base-path file:///tmp/hoodie/sample-table/ \
  --database default \
  --table hoodie_test \
  --user hive \
  --pass hive \
  --jdbc-url jdbc:hive2://localhost:10010/

```

{% include callout.html content="Hive sync tools does not yet support Merge-On-Read tables." type="info" %}



#### Manually via Beeline
Add in the hoodie-hadoop-mr jar so, Hive can read the Hoodie dataset and answer the query.

```
hive> add jar file:///tmp/hoodie-hadoop-mr-0.2.7.jar;
Added [file:///tmp/hoodie-hadoop-mr-0.2.7.jar] to class path
Added resources: [file:///tmp/hoodie-hadoop-mr-0.2.7.jar]
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
$ export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
$ spark-shell --jars /tmp/hoodie-hadoop-mr-0.2.7.jar --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false

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





