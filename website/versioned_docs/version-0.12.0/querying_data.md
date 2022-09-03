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

### Streaming Query
By default, the hoodie table is read as batch, that is to read the latest snapshot data set and returns. Turns on the streaming read
mode by setting option `read.streaming.enabled` as `true`. Sets up option `read.start-commit` to specify the read start offset, specifies the
value as `earliest` if you want to consume all the history data set.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.streaming.enabled` | false | `false` | Specify `true` to read as streaming |
| `read.start-commit` | false | the latest commit | Start commit time in format 'yyyyMMddHHmmss', use `earliest` to consume from the start commit |
| `read.streaming.skip_compaction` | false | `false` | Whether to skip compaction commits while reading, generally for two purposes: 1) Avoid consuming duplications from the compaction instants 2) When change log mode is enabled, to only consume change logs for right semantics. |
| `clean.retain_commits` | false | `10` | The max number of commits to retain before cleaning, when change log mode is enabled, tweaks this option to adjust the change log live time. For example, the default strategy keeps 50 minutes of change logs if the checkpoint interval is set up as 5 minutes. |

:::note
When option `read.streaming.skip_compaction` turns on and the streaming reader lags behind by commits of number
`clean.retain_commits`, the data loss may occur. The compaction keeps the original instant time as the per-record metadata,
the streaming reader would read and skip the whole base files if the log has been consumed. For efficiency, option `read.streaming.skip_compaction`
is till suggested being `true`.
:::

### Incremental Query
There are 3 use cases for incremental query:
1. Streaming query: specify the start commit with option `read.start-commit`;
2. Batch query: specify the start commit with option `read.start-commit` and end commit with option `read.end-commit`,
   the interval is a closed one: both start commit and end commit are inclusive;
3. TimeTravel: consume as batch for an instant time, specify the `read.end-commit` is enough because the start commit is latest by default.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.start-commit` | `false` | the latest commit | Specify `earliest` to consume from the start commit |
| `read.end-commit` | `false` | the latest commit | -- |

### Metadata Table
The metadata table holds the metadata index per hudi table, it holds the file list and all kinds of indexes that we called multi-model index.
Current these indexes are supported:

1. partition -> files mapping
2. column max/min statistics for each file
3. bloom filter for each file

The partition -> files mappings can be used for fetching the file list for writing/reading path, it is cost friendly for object storage that charges per visit,
for HDFS, it can ease the access burden of the NameNode.

The column max/min statistics per file is used for query acceleration, in the writing path, when enable this feature, hudi would
book-keep the max/min values for each column in real-time, thus would decrease the writing throughput. In the reading path, hudi uses
this statistics to filter out the useless files first before scanning.

The bloom filter index is currently only used for spark bloom filter index, not for query acceleration yet.

In general, enable the metadata table would increase the commit time, it is not very friendly for the use cases for very short checkpoint interval (say 30s).
And for these use cases you should test the stability first.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `metadata.enabled` | `false` | false | Set to `true` to enable |
| `read.data.skipping.enabled` | `false` | false | Whether to enable data skipping for batch snapshot read, by default disabled |
| `hoodie.metadata.index.column.stats.enable` | `false` | false | Whether to enable column statistics (max/min) |
| `hoodie.metadata.index.column.stats.column.list` | `false` | N/A | Columns(separated by comma) to collect the column statistics  |

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

## Redshift Spectrum
To set up Redshift Spectrum for querying Hudi, see the [Query Engine Setup](/docs/next/query_engine_setup#redshift-spectrum) page.


## StarRocks
To set up StarRocks for querying Hudi, see the [Query Engine Setup](/docs/query_engine_setup#starrocks) page.

## Support Matrix

Following tables show whether a given query is supported on specific query engine.

### Copy-On-Write tables

| Query Engine          |Snapshot Queries|Incremental Queries|
|-----------------------|--------|-----------|
| **Hive**              |Y|Y|
| **Spark SQL**         |Y|Y|
| **Spark Datasource**  |Y|Y|
| **Flink SQL**         |Y|N|
| **PrestoDB**          |Y|N|
| **Trino**             |Y|N|
| **Impala**            |Y|N|
| **Redshift Spectrum** |Y|N|
| **StarRocks**         |Y|N|



Note that `Read Optimized` queries are not applicable for COPY_ON_WRITE tables.

### Merge-On-Read tables

|Query Engine|Snapshot Queries|Incremental Queries|Read Optimized Queries|
|------------|--------|-----------|--------------|
|**Hive**|Y|Y|Y|
|**Spark SQL**|Y|Y|Y|
|**Spark Datasource**|Y|Y|Y|
|**Flink SQL**|Y|Y|Y|
|**PrestoDB**|Y|N|Y|
|**Trino**|N|N|Y|
|**Impala**|N|N|Y|

