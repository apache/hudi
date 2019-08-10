---
title: Quickstart
keywords: hudi, quickstart
tags: [quickstart]
sidebar: mydoc_sidebar
toc: false
permalink: quickstart.html
---
<br/>
To get a quick peek at Hudi's capabilities, we have put together a [demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) 
that showcases this on a docker based setup with all dependent systems running locally. We recommend you replicate the same setup 
and run the demo yourself, by following steps [here](docker_demo.html). Also, if you are looking for ways to migrate your existing data to Hudi, 
refer to [migration guide](migration_guide.html).

If you have Hive, Hadoop, Spark installed already & prefer to do it on your own setup, read on.

## Download Hudi

Check out [code](https://github.com/apache/incubator-hudi) or download [latest release](https://github.com/apache/incubator-hudi/archive/hudi-0.4.5.zip) 
and normally build the maven project, from command line

```
$ mvn clean install -DskipTests -DskipITs
```

To work with older version of Hive (pre Hive-1.2.1), use
```
$ mvn clean install -DskipTests -DskipITs -Dhive11
```

{% include callout.html content="For IDE, you can pull in the code into IntelliJ as a normal maven project. 
You might want to add your spark jars folder to project dependencies under 'Module Setttings', to be able to run from IDE." 
type="info" %}


### Version Compatibility

Hudi requires Java 8 to be installed on a *nix system. Hudi works with Spark-2.x versions. 
Further, we have verified that Hudi works with the following combination of Hadoop/Hive/Spark.

| Hadoop | Hive  | Spark | Instructions to Build Hudi |
| ---- | ----- | ---- | ---- |
| 2.6.0-cdh5.7.2 | 1.1.0-cdh5.7.2 | spark-2.[1-3].x | Use “mvn clean install -DskipTests -Dhadoop.version=2.6.0-cdh5.7.2 -Dhive.version=1.1.0-cdh5.7.2” |
| Apache hadoop-2.8.4 | Apache hive-2.3.3 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |
| Apache hadoop-2.7.3 | Apache hive-1.2.1 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |

{% include callout.html content="If your environment has other versions of hadoop/hive/spark, please try out Hudi 
and let us know if there are any issues. "  type="info" %}

## Generate Sample Dataset

### Environment Variables

Please set the following environment variables according to your setup. We have given an example setup with CDH version

```
cd incubator-hudi 
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

### Run HoodieJavaApp

Run __hudi-spark/src/test/java/HoodieJavaApp.java__ class, to place a two commits (commit 1 => 100 inserts, commit 2 => 100 updates to previously inserted 100 records) onto your DFS/local filesystem. Use the wrapper script
to run from command-line

```
cd hudi-spark
./run_hoodie_app.sh --help
Usage: <main class> [options]
  Options:
    --help, -h
       Default: false
    --table-name, -n
       table name for Hudi sample table
       Default: hoodie_rt
    --table-path, -p
       path for Hudi sample table
       Default: file:///tmp/hoodie/sample-table
    --table-type, -t
       One of COPY_ON_WRITE or MERGE_ON_READ
       Default: COPY_ON_WRITE
```

The class lets you choose table names, output paths and one of the storage types. In your own applications, be sure to include the `hudi-spark` module as dependency
and follow a similar pattern to write/read datasets via the datasource. 

## Query a Hudi dataset

Next, we will register the sample dataset into Hive metastore and try to query using [Hive](#hive), [Spark](#spark) & [Presto](#presto)

### Start Hive Server locally

```
hdfs namenode # start name node
hdfs datanode # start data node

bin/hive --service metastore  # start metastore
bin/hiveserver2 \
  --hiveconf hive.root.logger=INFO,console \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.aux.jars.path=/path/to/packaging/hudi-hive-bundle/target/hudi-hive-bundle-0.4.6-SNAPSHOT.jar

```

### Run Hive Sync Tool
Hive Sync Tool will update/create the necessary metadata(schema and partitions) in hive metastore. This allows for schema evolution and incremental addition of new partitions written to.
It uses an incremental approach by storing the last commit time synced in the TBLPROPERTIES and only syncing the commits from the last sync commit time stored.
Both [Spark Datasource](writing_data.html#datasource-writer) & [DeltaStreamer](writing_data.html#deltastreamer) have capability to do this, after each write.

```
cd hudi-hive
./run_sync_tool.sh
  --user hive
  --pass hive
  --database default
  --jdbc-url "jdbc:hive2://localhost:10010/"
  --base-path tmp/hoodie/sample-table/
  --table hoodie_test
  --partitioned-by field1,field2

```
{% include callout.html content="For some reason, if you want to do this by hand. Please 
follow [this](https://cwiki.apache.org/confluence/display/HUDI/Registering+sample+dataset+to+Hive+via+beeline)." 
type="info" %}


### HiveQL {#hive}

Let's first perform a query on the latest committed snapshot of the table

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
hive> set hive.stats.autogather=false;
hive> add jar file:///path/to/hudi-hive-bundle-0.4.6-SNAPSHOT.jar;
hive> select count(*) from hoodie_test;
...
OK
100
Time taken: 18.05 seconds, Fetched: 1 row(s)
hive>
```

### SparkSQL {#spark}

Spark is super easy, once you get Hive working as above. Just spin up a Spark Shell as below

```
$ cd $SPARK_INSTALL
$ spark-shell --jars $HUDI_SRC/packaging/hudi-spark-bundle/target/hudi-spark-bundle-0.4.6-SNAPSHOT.jar --driver-class-path $HADOOP_CONF_DIR  --conf spark.sql.hive.convertMetastoreParquet=false --packages com.databricks:spark-avro_2.11:4.0.0

scala> val sqlContext = new org.apache.spark.sql.SQLContext(sc)
scala> sqlContext.sql("show tables").show(10000)
scala> sqlContext.sql("describe hoodie_test").show(10000)
scala> sqlContext.sql("describe hoodie_test_rt").show(10000)
scala> sqlContext.sql("select count(*) from hoodie_test").show(10000)
```

### Presto {#presto}

Checkout the 'master' branch on OSS Presto, build it, and place your installation somewhere.

* Copy the hudi/packaging/hudi-presto-bundle/target/hudi-presto-bundle-*.jar into $PRESTO_INSTALL/plugin/hive-hadoop2/
* Startup your server and you should be able to query the same Hive table via Presto

```
show columns from hive.default.hoodie_test;
select count(*) from hive.default.hoodie_test
```

### Incremental HiveQL

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

{% include note.html content="This is only supported for Read-optimized view for now." %}
