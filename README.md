Hoodie - Spark Library For Upserts & Incremental Consumption
=============================================================

- - - -

# Core Functionality #

Hoodie provides the following abilities on a Hive table 

 * Upsert                     (how do I change the table efficiently?)
 * Incremental consumption    (how do I obtain records that changed?)


Ultimately, make the built Hive table, queryable via Spark & Presto as well. 


# Code & Project Structure #

 * hoodie-client     : Spark client library to take a bunch of inserts + updates and apply them to a Hoodie table
 * hoodie-common     : Common code shared between different artifacts of Hoodie 
 
 
 We have embraced the [Google Java code style](https://google.github.io/styleguide/javaguide.html). Please setup your IDE accordingly with style files from [here] (https://github.com/google/styleguide)


# Quickstart #

Check out code and pull it into Intellij as a normal maven project. 
> You might want to add your spark assembly jar to project dependencies under "Module Setttings", to be able to run Spark from IDE

Setup your local hadoop/hive test environment. See [this](http://www.bytearray.io/2016/05/setting-up-hadoopyarnsparkhive-on-mac.html) for reference

## Run the Hoodie Test Job ##

Create the output folder on your local HDFS
```
hdfs dfs -mkdir -p /tmp/hoodie/sample-table
```

You can run the __HoodieClientExample__ class, to place a set of inserts + updates onto your HDFS at /tmp/hoodie/sample-table

## Access via Hive ##

Add in the hoodie-mr jar so, Hive can pick up the right files to hit, to answer the query.

```
hive> add jar file:///tmp/hoodie-mr-0.1.jar;
Added [file:///tmp/hoodie-mr-0.1.jar] to class path
Added resources: [file:///tmp/hoodie-mr-0.1.jar]
```

Then, you need to create a table and register the sample partitions


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
   'com.uber.hoodie.hadoop.HoodieOutputFormat'
LOCATION
   'hdfs:///tmp/hoodie/sample-table';

ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2016-03-15') LOCATION 'hdfs:///tmp/hoodie/sample-table/2016/03/15';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-16') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/16';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-17') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/17';
```

Let's first perform a query on the latest committed snapshot of the table

```
hive> select count(*) from hoodie_test;
...
OK
100
Time taken: 18.05 seconds, Fetched: 1 row(s)
hive>
```


Let's now perform a query, to obtain the changed rows since a commit in the past

```
hive> set hoodie.scan.mode=INCREMENTAL;
hive> set hoodie.last.commitTs=001;
hive> select `_hoodie_commit_time`, rider, driver from hoodie_test limit 10;
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


## Access via Spark ##

Spark is super easy, once you get Hive working as above. Just spin up a Spark Shell as below

```
$ cd $SPARK_INSTALL
$ export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
$ spark-shell --jars /tmp/hoodie-mr-0.1.jar --driver-class-path $HADOOP_CONF_DIR --conf spark.sql.hive.convertMetastoreParquet=false


scala> sqlContext.sql("show tables").show(10000)
scala> sqlContext.sql("describe hoodie_test").show(10000)
scala> sqlContext.sql("select count(*) from hoodie_test").show(10000)
```



## Access via Presto ##

Checkout the 'hoodie-integration' branch, build off it, and place your installation somewhere. 

* Copy the hoodie-mr jar into $PRESTO_INSTALL/plugin/hive-hadoop2/

* Change your catalog config, to make presto respect the __HoodieInputFormat__

```
$ cat etc/catalog/hive.properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:10000
hive.respect-input-format-splits=true
```

startup your server and you should be able to query the same Hive table via Presto

```
show columns from hive.default.hoodie_test;
select count(*) from hive.default.hoodie_test
```

> NOTE: As of now, Presto has trouble accessing HDFS locally, hence create a new table as above, backed on local filesystem file:// as a workaround

# Planned #
* Support for Self Joins - As of now, you cannot incrementally consume the same table more than once, since the InputFormat does not understand the QueryPlan.
* Hoodie Spark Datasource -  Allows for reading and writing data back using Apache Spark natively (without falling back to InputFormat), which can be more performant
* Hoodie Presto Connector - Allows for querying data managed by Hoodie using Presto natively, which can again boost [performance](https://prestodb.io/docs/current/release/release-0.138.html)


# Hoodie Admin CLI
# Launching Command Line #

<todo - change this after packaging is done>

* mvn clean install in hoodie-cli
* ./hoodie-cli

If all is good you should get a command prompt similar to this one
```
prasanna@:~/hoodie/hoodie-cli$ ./hoodie-cli.sh 
16/07/13 21:27:47 INFO xml.XmlBeanDefinitionReader: Loading XML bean definitions from URL [jar:file:/home/prasanna/hoodie/hoodie-cli/target/hoodie-cli-0.1-SNAPSHOT.jar!/META-INF/spring/spring-shell-plugin.xml]
16/07/13 21:27:47 INFO support.GenericApplicationContext: Refreshing org.springframework.context.support.GenericApplicationContext@372688e8: startup date [Wed Jul 13 21:27:47 UTC 2016]; root of context hierarchy
16/07/13 21:27:47 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring
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
hoodie->
```

# Commands #

 * connect --path [dataset_path]                 : Connect to the specific dataset by its path
 * commits show                                  : Show all details about the commits  
 * commits refresh                               : Refresh the commits from HDFS
 * commit rollback      --commit [commitTime]    : Rollback a commit
 * commit showfiles      --commit [commitTime]   : Show details of a commit (lists all the files modified along with other metrics)
 * commit showpartitions --commit [commitTime]   : Show details of a commit (lists statistics aggregated at partition level)

 * commits compare --path [otherBasePath]        : Compares the current dataset commits with the path provided and tells you how many commits behind or ahead
 * stats wa                                      : Calculate commit level and overall write amplification factor (total records written / total records upserted)
 * help

## Contributing
We :heart: contributions. If you find a bug in the library or would like to add new features, go ahead and open
issues or pull requests against this repo. Before you do so, please sign the
[Uber CLA](https://docs.google.com/a/uber.com/forms/d/1pAwS_-dA1KhPlfxzYLBqK6rsSWwRwH95OCCZrcsY5rk/viewform).
Also, be sure to write unit tests for your bug fix or feature to show that it works as expected.
