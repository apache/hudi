---
title: Querying Hudi Datasets
keywords: hudi, hive, spark, sql, presto
permalink: /docs/querying_data
summary: In this page, we go over how to enable SQL queries on Hudi built tables.
toc: true
---

Conceptually, Hudi stores data physically once on DFS, while providing 3 logical views on top, as explained [before](concepts.html#views). 
Once the dataset is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi
bundle has been provided, the dataset can be queried by popular query engines like Hive, Spark and Presto.

Specifically, there are two Hive tables named off [table name](configurations.html#TABLE_NAME_OPT_KEY) passed during write. 
For e.g, if `table name = hudi_tbl`, then we get  

 - `hudi_tbl` realizes the read optimized view of the dataset backed by `HoodieParquetInputFormat`, exposing purely columnar data.
 - `hudi_tbl_rt` realizes the real time view of the dataset  backed by `HoodieParquetRealtimeInputFormat`, exposing merged view of base and log data.

As discussed in the concepts section, the one key primitive needed for [incrementally processing](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop),
is `incremental pulls` (to obtain a change stream/log from a dataset). Hudi datasets can be pulled incrementally, which means you can get ALL and ONLY the updated & new rows 
since a specified instant time. This, together with upserts, are particularly useful for building data pipelines where 1 or more source Hudi tables are incrementally pulled (streams/facts),
joined with other tables (datasets/dimensions), to [write out deltas](writing_data.html) to a target Hudi dataset. Incremental view is realized by querying one of the tables above, 
with special configurations that indicates to query planning that only incremental data needs to be fetched out of the dataset. 

In sections, below we will discuss in detail how to access all the 3 views on each query engine.

## Hive

In order for Hive to recognize Hudi datasets and query correctly, the HiveServer2 needs to be provided with the `hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar` 
in its [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr). This will ensure the input format 
classes with its dependencies are available for query planning & execution. 

### Read Optimized table
In addition to setup above, for beeline cli access, the `hive.input.format` variable needs to be set to the  fully qualified path name of the 
inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. For Tez, additionally the `hive.tez.input.format` needs to be set 
to `org.apache.hadoop.hive.ql.io.HiveInputFormat`

### Real time table
In addition to installing the hive bundle jar on the HiveServer2, it needs to be put on the hadoop/hive installation across the cluster, so that
queries can pick up the custom RecordReader as well.

### Incremental Pulling

`HiveIncrementalPuller` allows incrementally extracting changes from large fact/dimension tables via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and 
incremental primitives (speed up query by pulling tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the hive query and saves its results in a temp table.
that can later be upserted. Upsert utility (`HoodieDeltaStreamer`) has all the state it needs from the directory structure to know what should be the commit time on the target table.
e.g: `/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}`.The Delta Hive table registered will be of the form `{tmpdb}.{source_table}_{last_commit_included}`.

The following are the configuration options for HiveIncrementalPuller

| **Config** | **Description** | **Default** |
|hiveUrl| Hive Server 2 URL to connect to |  |
|hiveUser| Hive Server 2 Username |  |
|hivePass| Hive Server 2 Password |  |
|queue| YARN Queue name |  |
|tmp| Directory where the temporary delta data is stored in DFS. The directory structure will follow conventions. Please see the below section.  |  |
|extractSQLFile| The SQL to execute on the source table to extract the data. The data extracted will be all the rows that changed since a particular point in time. |  |
|sourceTable| Source Table Name. Needed to set hive environment properties. |  |
|targetTable| Target Table Name. Needed for the intermediate storage directory structure.  |  |
|sourceDataPath| Source DFS Base Path. This is where the Hudi metadata will be read. |  |
|targetDataPath| Target DFS Base path. This is needed to compute the fromCommitTime. This is not needed if fromCommitTime is specified explicitly. |  |
|tmpdb| The database to which the intermediate temp delta table will be created | hoodie_temp |
|fromCommitTime| This is the most important parameter. This is the point in time from which the changed records are pulled from.  |  |
|maxCommits| Number of commits to include in the pull. Setting this to -1 will include all the commits from fromCommitTime. Setting this to a value > 0, will include records that ONLY changed in the specified number of commits after fromCommitTime. This may be needed if you need to catch up say 2 commits at a time. | 3 |
|help| Utility Help |  |


Setting fromCommitTime=0 and maxCommits=-1 will pull in the entire source dataset and can be used to initiate backfills. If the target dataset is a Hudi dataset,
then the utility can determine if the target dataset has no commits or is behind more than 24 hour (this is configurable),
it will automatically use the backfill configuration, since applying the last 24 hours incrementally could take more time than doing a backfill. The current limitation of the tool
is the lack of support for self-joining the same table in mixed mode (normal and incremental modes).

**NOTE on Hive queries that are executed using Fetch task:**
Since Fetch tasks invoke InputFormat.listStatus() per partition, Hoodie metadata can be listed in
every such listStatus() call. In order to avoid this, it might be useful to disable fetch tasks
using the hive session property for incremental queries: `set hive.fetch.task.conversion=none;` This
would ensure Map Reduce execution is chosen for a Hive query, which combines partitions (comma
separated) and calls InputFormat.listStatus() only once with all those partitions.

## Spark

Spark provides much easier deployment & management of Hudi jars and bundles into jobs/notebooks. At a high level, there are two ways to access Hudi datasets in Spark.

 - **Hudi DataSource** : Supports Read Optimized, Incremental Pulls similar to how standard datasources (e.g: `spark.read.parquet`) work.
 - **Read as Hive tables** : Supports all three views, including the real time view, relying on the custom Hudi input formats again like Hive.
 
 In general, your spark job needs a dependency to `hudi-spark` or `hudi-spark-bundle-x.y.z.jar` needs to be on the class path of driver & executors (hint: use `--jars` argument)
 
### Read Optimized table

To read RO table as a Hive table using SparkSQL, simply push a path filter into sparkContext as follows. 
This method retains Spark built-in optimizations for reading Parquet files like vectorized reading on Hudi tables.

```scala
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```

If you prefer to glob paths on DFS via the datasource, you can simply do something like below to get a Spark dataframe to work with. 

```java
Dataset<Row> hoodieROViewDF = spark.read().format("org.apache.hudi")
// pass any path glob, can include hudi & non-hudi datasets
.load("/glob/path/pattern");
```
 
### Real time table {#spark-rt-view}
Currently, real time table can only be queried as a Hive table in Spark. In order to do this, set `spark.sql.hive.convertMetastoreParquet=false`, forcing Spark to fallback 
to using the Hive Serde to read the data (planning/executions is still Spark). 

```java
$ spark-shell --jars hudi-spark-bundle-x.y.z-SNAPSHOT.jar --driver-class-path /etc/hive/conf  --packages com.databricks:spark-avro_2.11:4.0.0 --conf spark.sql.hive.convertMetastoreParquet=false --num-executors 10 --driver-memory 7g --executor-memory 2g  --master yarn-client

scala> sqlContext.sql("select count(*) from hudi_rt where datestr = '2016-10-02'").show()
```

### Incremental Pulling {#spark-incr-pull}
The `hudi-spark` module offers the DataSource API, a more elegant way to pull data from Hudi dataset and process it via Spark.
A sample incremental pull, that will obtain all records written since `beginInstantTime`, looks like below.

```java
 Dataset<Row> hoodieIncViewDF = spark.read()
     .format("org.apache.hudi")
     .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(),
             DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
     .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),
            <beginInstantTime>)
     .load(tablePath); // For incremental view, pass in the root/base path of dataset
```

Please refer to [configurations](configurations.html#spark-datasource) section, to view all datasource options.

Additionally, `HoodieReadClient` offers the following functionality using Hudi's implicit indexing.

| **API** | **Description** |
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided RDD[HoodieRecord]. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hudi dataset |


## Presto

Presto is a popular query engine, providing interactive query performance. Hudi RO tables can be queries seamlessly in Presto. 
This requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation.
