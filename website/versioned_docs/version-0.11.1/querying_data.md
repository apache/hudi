---
title: Querying Data
keywords: [ hudi, hive, spark, sql, presto]
summary: In this page, we go over how to enable SQL queries on Hudi built tables.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained [before](/docs/concepts#query-types). 
Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi
bundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Spark Datasource API and PrestoDB.

In sections, below we will discuss specific setup to access different query types from different query engines. 

## Spark Datasource

The Spark Datasource API is a popular way of authoring Spark ETL pipelines. Hudi tables can be queried via the Spark datasource with a simple `spark.read.parquet`.
See the [Spark Quick Start](/docs/quick-start-guide) for more examples of Spark datasource reading queries. 

To setup Spark for querying Hudi, see the [Query Engine Setup](/docs/query_engine_setup#Spark-DataSource) page.

### Snapshot query {#spark-snap-query}
Retrieve the data table at the present point in time.

```scala
val hudiIncQueryDF = spark
     .read()
     .format("hudi")
     .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
     .load(tablePath) 
```

### Incremental query {#spark-incr-query}
Of special interest to spark pipelines, is Hudi's ability to support incremental queries, like below. A sample incremental query, that will obtain all records written since `beginInstantTime`, looks like below.
Thanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts by only processing the changed records.

The following snippet shows how to obtain all records changed after `beginInstantTime` and run some SQL on them.

```java
 Dataset<Row> hudiIncQueryDF = spark.read()
     .format("org.apache.hudi")
     .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
     .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), <beginInstantTime>)
     .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY(), "/year=2020/month=*/day=*") // Optional, use glob pattern if querying certain partitions
     .load(tablePath); // For incremental query, pass in the root/base path of table
     
hudiIncQueryDF.createOrReplaceTempView("hudi_trips_incremental")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
```

For examples, refer to [Incremental Queries](/docs/quick-start-guide#incremental-query) in the Spark quickstart. 
Please refer to [configurations](/docs/configurations#SPARK_DATASOURCE) section, to view all datasource options.

Additionally, `HoodieReadClient` offers the following functionality using Hudi's implicit indexing.

| **API** | **Description** |
|-------|--------|
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided `RDD[HoodieRecord]`. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hudi table |

### Spark SQL
Once the Hudi tables have been registered to the Hive metastore, they can be queried using the Spark-Hive integration.
By default, Spark SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables.
The following are important settings to consider when querying COPY_ON_WRITE or MERGE_ON_READ tables. 

#### Copy On Write tables
For COPY_ON_WRITE tables, Spark's default parquet reader can be used to retain Sparks built-in optimizations for reading parquet files like vectorized reading on Hudi Hive tables.
If using the default parquet reader, a path filter needs to be pushed into sparkContext as follows.

```scala
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```

#### Merge On Read tables
No special configurations are needed for querying MERGE_ON_READ tables with Hudi version 0.9.0+

If you are querying MERGE_ON_READ tables using Hudi version <= 0.8.0, you need to turn off the SparkSQL default parquet reader by setting: `spark.sql.hive.convertMetastoreParquet=false`.

```java
$ spark-shell --driver-class-path /etc/hive/conf  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3,org.apache.spark:spark-avro_2.11:2.4.4 --conf spark.sql.hive.convertMetastoreParquet=false

scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
```
:::note
Note: COPY_ON_WRITE tables can also still be read if you turn off the default parquet reader.
:::

## Flink SQL
Once the flink Hudi tables have been registered to the Flink catalog, it can be queried using the Flink SQL. It supports all query types across both Hudi table types,
relying on the custom Hudi input formats again like Hive. Typically notebook users and Flink SQL CLI users leverage flink sql for querying Hudi tables. Please add hudi-flink-bundle as described in the [Flink Quickstart](/docs/flink-quick-start-guide).

By default, Flink SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

```sql
-- this defines a COPY_ON_WRITE table named 't1'
CREATE TABLE t1(
  uuid VARCHAR(20), -- you can use 'PRIMARY KEY NOT ENFORCED' syntax to specify the field as record key
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'table_base+path'
);

-- query the data
select * from t1 where `partition` = 'par1';
```

Flink's built-in support parquet is used for both COPY_ON_WRITE and MERGE_ON_READ tables,
additionally partition prune is applied by Flink engine internally if a partition path is specified
in the filter. Filters push down is not supported yet (already on the roadmap).

For MERGE_ON_READ table, in order to query hudi table as a streaming, you need to add option `'read.streaming.enabled' = 'true'`,
when querying the table, a Flink streaming pipeline starts and never ends until the user cancel the job manually.
You can specify the start commit with option `read.streaming.start-commit` and source monitoring interval with option
`read.streaming.check-interval`.

## Hive
To setup Hive for querying Hudi, see the [Query Engine Setup](/docs/query_engine_setup#hive) page.

### Incremental query
`HiveIncrementalPuller` allows incrementally extracting changes from large fact/dimension tables via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and
incremental primitives (speed up querying tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the hive query and saves its results in a temp table.
that can later be upserted. Upsert utility (`HoodieDeltaStreamer`) has all the state it needs from the directory structure to know what should be the commit time on the target table.
e.g: `/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}`.The Delta Hive table registered will be of the form `{tmpdb}.{source_table}_{last_commit_included}`.

The following are the configuration options for HiveIncrementalPuller

| **Config** | **Description** | **Default** |
|-------|--------|--------|
|hiveUrl| Hive Server 2 URL to connect to |  |
|hiveUser| Hive Server 2 Username |  |
|hivePass| Hive Server 2 Password |  |
|queue| YARN Queue name |  |
|tmp| Directory where the temporary delta data is stored in DFS. The directory structure will follow conventions. Please see the below section.  |  |
|extractSQLFile| The SQL to execute on the source table to extract the data. The data extracted will be all the rows that changed since a particular point in time. |  |
|sourceTable| Source Table Name. Needed to set hive environment properties. |  |
|sourceDb| Source DB name. Needed to set hive environment properties.| |
|targetTable| Target Table Name. Needed for the intermediate storage directory structure.  |  |
|targetDb| Target table's DB name.| |
|tmpdb| The database to which the intermediate temp delta table will be created | hoodie_temp |
|fromCommitTime| This is the most important parameter. This is the point in time from which the changed records are queried from.  |  |
|maxCommits| Number of commits to include in the query. Setting this to -1 will include all the commits from fromCommitTime. Setting this to a value > 0, will include records that ONLY changed in the specified number of commits after fromCommitTime. This may be needed if you need to catch up say 2 commits at a time. | 3 |
|help| Utility Help |  |


Setting fromCommitTime=0 and maxCommits=-1 will fetch the entire source table and can be used to initiate backfills. If the target table is a Hudi table,
then the utility can determine if the target table has no commits or is behind more than 24 hour (this is configurable),
it will automatically use the backfill configuration, since applying the last 24 hours incrementally could take more time than doing a backfill. The current limitation of the tool
is the lack of support for self-joining the same table in mixed mode (snapshot and incremental modes).

**NOTE on Hive incremental queries that are executed using Fetch task:**
Since Fetch tasks invoke InputFormat.listStatus() per partition, Hoodie metadata can be listed in
every such listStatus() call. In order to avoid this, it might be useful to disable fetch tasks
using the hive session property for incremental queries: `set hive.fetch.task.conversion=none;` This
would ensure Map Reduce execution is chosen for a Hive query, which combines partitions (comma
separated) and calls InputFormat.listStatus() only once with all those partitions.

## PrestoDB
To setup PrestoDB for querying Hudi, see the [Query Engine Setup](/docs/query_engine_setup#prestodb) page.

## Trino
To setup Trino for querying Hudi, see the [Query Engine Setup](/docs/query_engine_setup#trino) page.

## Impala (3.4 or later)

### Snapshot Query

Impala is able to query Hudi Copy-on-write table as an [EXTERNAL TABLE](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_tables.html#external_tables) on HDFS.  

To create a Hudi read optimized table on Impala:
```
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
```
Impala is able to take advantage of the physical partition structure to improve the query performance.
To create a partitioned table, the folder should follow the naming convention like `year=2020/month=1`.
Impala use `=` to separate partition name and partition value.  
To create a partitioned Hudi read optimized table on Impala:
```
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
PARTITION BY (year int, month int, day int)
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
ALTER TABLE database.table_name RECOVER PARTITIONS;
```
After Hudi made a new commit, refresh the Impala table to get the latest results.
```
REFRESH database.table_name
```

## Support Matrix

Following tables show whether a given query is supported on specific query engine.

### Copy-On-Write tables

|Query Engine|Snapshot Queries|Incremental Queries|
|------------|--------|-----------|
|**Hive**|Y|Y|
|**Spark SQL**|Y|Y|
|**Spark Datasource**|Y|Y|
|**Flink SQL**|Y|N|
|**PrestoDB**|Y|N|
|**Trino**|Y|N|
|**Impala**|Y|N|


Note that `Read Optimized` queries are not applicable for COPY_ON_WRITE tables.

### Merge-On-Read tables

|Query Engine|Snapshot Queries|Incremental Queries|Read Optimized Queries|
|------------|--------|-----------|--------------|
|**Hive**|Y|Y|Y|
|**Spark SQL**|Y|Y|Y|
|**Spark Datasource**|Y|Y|Y|
|**Flink SQL**|Y|Y|Y|
|**PrestoDB**|Y|N|Y|
|**Trino**|Y|N|Y|
|**Impala**|N|N|Y|

