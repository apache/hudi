---
version: 0.8.0
title: Querying Data 
keywords: [ hudi, hive, spark, sql, presto]
summary: In this page, we go over how to enable SQL queries on Hudi built tables.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained [before](/docs/concepts#query-types). 
Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi
bundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Spark Datasource API and PrestoDB.

Specifically, following Hive tables are registered based off [table name](/docs/configurations#TABLE_NAME_OPT_KEY) 
and [table type](/docs/configurations#TABLE_TYPE_OPT_KEY) configs passed during write.   

If `table name = hudi_trips` and `table type = COPY_ON_WRITE`, then we get: 
 - `hudi_trips` supports snapshot query and incremental query on the table backed by `HoodieParquetInputFormat`, exposing purely columnar data.


If `table name = hudi_trips` and `table type = MERGE_ON_READ`, then we get:
 - `hudi_trips_rt` supports snapshot query and incremental query (providing near-real time data) on the table  backed by `HoodieParquetRealtimeInputFormat`, exposing merged view of base and log data.
 - `hudi_trips_ro` supports read optimized query on the table backed by `HoodieParquetInputFormat`, exposing purely columnar data stored in base files.

As discussed in the concepts section, the one key capability needed for [incrementally processing](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop),
is obtaining a change stream/log from a table. Hudi tables can be queried incrementally, which means you can get ALL and ONLY the updated & new rows 
since a specified instant time. This, together with upserts, is particularly useful for building data pipelines where 1 or more source Hudi tables are incrementally queried (streams/facts),
joined with other tables (tables/dimensions), to [write out deltas](/docs/writing_data) to a target Hudi table. Incremental queries are realized by querying one of the tables above, 
with special configurations that indicates to query planning that only incremental data needs to be fetched out of the table. 


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
|**Impala**|Y|N|
|**Redshift Spectrum**|Y|N|


Note that `Read Optimized` queries are not applicable for COPY_ON_WRITE tables.

### Merge-On-Read tables

|Query Engine|Snapshot Queries|Incremental Queries|Read Optimized Queries|
|------------|--------|-----------|--------------|
|**Hive**|Y|Y|Y|
|**Spark SQL**|Y|Y|Y|
|**Spark Datasource**|Y|Y|Y|
|**Flink SQL**|Y|Y|Y|
|**PrestoDB**|Y|N|Y|
|**Impala**|N|N|Y|


In sections, below we will discuss specific setup to access different query types from different query engines. 

## Hive

In order for Hive to recognize Hudi tables and query correctly, 
 - the HiveServer2 needs to be provided with the `hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar` in its [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf#concept_nc3_mms_lr). This will ensure the input format 
classes with its dependencies are available for query planning & execution. 
 - For MERGE_ON_READ tables, additionally the bundle needs to be put on the hadoop/hive installation across the cluster, so that queries can pick up the custom RecordReader as well.

In addition to setup above, for beeline cli access, the `hive.input.format` variable needs to be set to the fully qualified path name of the 
inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. For Tez, additionally the `hive.tez.input.format` needs to be set 
to `org.apache.hadoop.hive.ql.io.HiveInputFormat`. Then proceed to query the table like any other Hive table.

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

## Spark SQL
Once the Hudi tables have been registered to the Hive metastore, it can be queried using the Spark-Hive integration. It supports all query types across both Hudi table types, 
relying on the custom Hudi input formats again like Hive. Typically notebook users and spark-shell users leverage spark sql for querying Hudi tables. Please add hudi-spark-bundle as described above via --jars or --packages.
 
By default, Spark SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables. However, for MERGE_ON_READ tables which has 
both parquet and avro data, this default setting needs to be turned off using set `spark.sql.hive.convertMetastoreParquet=false`. 
This will force Spark to fallback to using the Hive Serde to read the data (planning/executions is still Spark). 

```java
$ spark-shell --driver-class-path /etc/hive/conf  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3,org.apache.spark:spark-avro_2.11:2.4.4 --conf spark.sql.hive.convertMetastoreParquet=false --num-executors 10 --driver-memory 7g --executor-memory 2g  --master yarn-client

scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
```

For COPY_ON_WRITE tables, either Hive SerDe can be used by turning off `spark.sql.hive.convertMetastoreParquet=false` as described above or Spark's built in support can be leveraged. 
If using spark's built in support, additionally a path filter needs to be pushed into sparkContext as follows. This method retains Spark built-in optimizations for reading parquet files like vectorized reading on Hudi Hive tables.

```scala
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```

## Spark Datasource

The Spark Datasource API is a popular way of authoring Spark ETL pipelines. Hudi COPY_ON_WRITE and MERGE_ON_READ tables can be queried via Spark datasource similar to how standard 
datasources work (e.g: `spark.read.parquet`). MERGE_ON_READ table supports snapshot querying and COPY_ON_WRITE table supports both snapshot and incremental querying via Spark datasource. Typically spark jobs require adding `--jars <path to jar>/hudi-spark-bundle_2.11-<hudi version>.jar` to classpath of drivers 
and executors. Alternatively, hudi-spark-bundle can also fetched via the `--packages` options (e.g: `--packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3`).

### Snapshot query {#spark-snap-query}
This method can be used to retrieve the data table at the present point in time.
Note: The file path must be suffixed with a number of wildcard asterisk (`/*`) one greater than the number of partition levels. Eg: with table file path "tablePath" partitioned by columns "a", "b", and "c", the load path must be `tablePath + "/*/*/*/*"`

```scala
val hudiIncQueryDF = spark
     .read()
     .format("org.apache.hudi")
     .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
     .load(tablePath + "/*") //The number of wildcard asterisks here must be one greater than the number of partition
```

### Incremental query {#spark-incr-query}
Of special interest to spark pipelines, is Hudi's ability to support incremental queries, like below. A sample incremental query, that will obtain all records written since `beginInstantTime`, looks like below.
Thanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts, by only processing the changed records.
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

For examples, refer to [Setup spark-shell in quickstart](/docs/quick-start-guide#setup-spark-shell). 
Please refer to [configurations](/docs/configurations#spark-datasource) section, to view all datasource options.

Additionally, `HoodieReadClient` offers the following functionality using Hudi's implicit indexing.

| **API** | **Description** |
|-------|--------|
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided `RDD[HoodieRecord]`. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hudi table |

## Flink SQL
Once the flink Hudi tables have been registered to the Flink catalog, it can be queried using the Flink SQL. It supports all query types across both Hudi table types,
relying on the custom Hudi input formats again like Hive. Typically notebook users and Flink SQL CLI users leverage flink sql for querying Hudi tables. Please add hudi-flink-bundle as described above via --jars.

By default, Flink SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

```sql
-- this defines a COPY_ON_WRITE table named 't1'
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'schema://base-path'
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

## PrestoDB

PrestoDB is a popular query engine, providing interactive query performance. PrestoDB currently supports snapshot querying on COPY_ON_WRITE tables. 
Both snapshot and read optimized queries are supported on MERGE_ON_READ Hudi tables. Since PrestoDB-Hudi integration has evolved over time, the installation
instructions for PrestoDB would vary based on versions. Please check the below table for query types supported and installation instructions 
for different versions of PrestoDB.


| **PrestoDB Version** | **Installation description** | **Query types supported** |
|----------------------|------------------------------|---------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| >= 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| >= 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency. | Snapshot querying on both COW and MOR tables |

## Impala (3.4 or later)

### Snapshot Query

Impala is able to query Hudi Copy-on-write table as an [EXTERNAL TABLE](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_tables#external_tables) on HDFS.  

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
## Redshift Spectrum
Copy on Write Tables in Apache Hudi versions 0.5.2, 0.6.0, 0.7.0, 0.8.0, 0.9.0, 0.10.x, 0.11.x and 0.12.0 can be queried via Amazon Redshift Spectrum external tables.
To be able to query Hudi versions 0.10.0 and above please try latest versions of Redshift.
:::note
Hudi tables are supported only when AWS Glue Data Catalog is used. It's not supported when you use an Apache Hive metastore as the external catalog.
:::

Please refer to [Redshift Spectrum Integration with Apache Hudi](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html#c-spectrum-column-mapping-hudi)
for more details.
