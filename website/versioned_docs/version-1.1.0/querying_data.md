---
title: Querying Data
keywords: [ hudi, hive, spark, sql, presto]
summary: In this page, we go over how to process data in Hudi tables.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

:::danger
This page is no longer maintained. Please refer to Hudi [SQL DDL](sql_ddl), [SQL DML](sql_dml), [SQL Queries](sql_queries) and [Procedures](procedures) for the latest documentation.
:::

Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained [before](concepts#query-types). 
Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi
bundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Flink, Trino and PrestoDB.

In sections, below we will discuss specific setup to access different query types from different query engines. 

## Spark Datasource

The Spark Datasource API is a popular way of authoring Spark ETL pipelines. Hudi tables can be queried via the Spark datasource with a simple `spark.read.parquet`.
See the [Spark Quick Start](quick-start-guide) for more examples of Spark datasource reading queries. 

**Setup**

If your Spark environment does not have the Hudi jars installed, add [hudi-spark-bundle](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark-bundle) jar to the
classpath of drivers and executors using `--jars` option. Alternatively, hudi-spark-bundle can also fetched via the
--packages options (e.g: --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1).

### Snapshot query {#spark-snap-query}
Retrieve the data table at the present point in time.

```scala
val hudiSnapshotQueryDF = spark
     .read
     .format("hudi")
     .option("hoodie.datasource.query.type", "snapshot")
     .load(tablePath) 
```

### Incremental query {#spark-incr-query}
Of special interest to spark pipelines, is Hudi's ability to support incremental queries, like below. A sample incremental query, that will obtain all records written since `beginInstantTime`, looks like below.
Thanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts by only processing the changed records.

The following snippet shows how to obtain all records changed after `beginInstantTime` and run some SQL on them.

```java
Dataset<Row> hudiIncQueryDF = spark.read()
     .format("org.apache.hudi")
     .option("hoodie.datasource.query.type", "incremental"())
     .option("hoodie.datasource.read.begin.instanttime", <beginInstantTime>)
     .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY(), "/year=2020/month=*/day=*") // Optional, use glob pattern if querying certain partitions
     .load(tablePath); // For incremental query, pass in the root/base path of table
     
hudiIncQueryDF.createOrReplaceTempView("hudi_trips_incremental")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
```

For examples, refer to [Incremental Queries](quick-start-guide#incremental-query) in the Spark quickstart. 
Please refer to [configurations](configurations#SPARK_DATASOURCE) section, to view all datasource options.

Additionally, `HoodieReadClient` offers the following functionality using Hudi's implicit indexing.

| **API** | **Description** |
|-------|--------|
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided `RDD[HoodieRecord]`. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hudi table |

### Incremental query
`HiveIncrementalPuller` allows incrementally extracting changes from large fact/dimension tables via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and
incremental primitives (speed up querying tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the hive query and saves its results in a temp table.
that can later be upserted. Upsert utility (Hudi Streamer) has all the state it needs from the directory structure to know what should be the commit time on the target table.
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
