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
$ mvn clean install -DskipTests

To work with older version of Hive (pre Hive-1.2.1), use

$ mvn clean install -DskipTests -Dhive11

```

{% include callout.html content="You might want to add your spark jars folder to project dependencies under 'Module Setttings', to be able to run Spark from IDE" type="info" %}

{% include note.html content="Setup your local hadoop/hive test environment, so you can play with entire ecosystem. See [this](http://www.bytearray.io/2016/05/setting-up-hadoopyarnsparkhive-on-mac.html) for reference" %}


## Version Compatibility

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

### Supported API's

Use the DataSource API to quickly start reading or writing hoodie datasets in few lines of code. Ideal for most 
ingestion use-cases.
Use the RDD API to perform more involved actions on a hoodie dataset

#### DataSource API

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

#### RDD API

RDD level APIs give you more power and control over things, via the `hoodie-client` module .
Refer to  __hoodie-client/src/test/java/HoodieClientExample.java__ class for an example.



## Query a Hoodie dataset

### Register Dataset to Hive Metastore

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



### Using different query engines

Now, we can proceed to query the dataset, as we would normally do across all the three query engines supported.

#### HiveQL

Let's first perform a query on the latest committed snapshot of the table

```
hive> select count(*) from hoodie_test;
...
OK
100
Time taken: 18.05 seconds, Fetched: 1 row(s)
hive>
```

#### SparkSQL

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

#### Presto

Checkout the 'master' branch on OSS Presto, build it, and place your installation somewhere.

* Copy the hoodie-hadoop-mr-* jar into $PRESTO_INSTALL/plugin/hive-hadoop2/
* Startup your server and you should be able to query the same Hive table via Presto

```
show columns from hive.default.hoodie_test;
select count(*) from hive.default.hoodie_test
```



## Incremental Queries of a Hoodie dataset

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





