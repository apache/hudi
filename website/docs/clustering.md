---
title: Clustering
summary: "In this page, we describe async compaction in Hudi."
toc: true
last_modified_at:
---

## Background

Apache Hudi brings stream processing to big data, providing fresh data while being an order of magnitude efficient over traditional batch processing. In a data lake/warehouse, one of the key trade-offs is between ingestion speed and query performance. Data ingestion typically prefers small files to improve parallelism and make data available to queries as soon as possible. However, query performance degrades poorly with a lot of small files. Also, during ingestion, data is typically co-located based on arrival time. However, the query engines perform better when the data frequently queried is co-located together. In most architectures each of these systems tend to add optimizations independently to improve performance which hits limitations due to un-optimized data layouts. This doc introduces a new kind of table service called clustering [[RFC-19]](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance) to reorganize data for improved query performance without compromising on ingestion speed.
<!--truncate-->

## How is compaction different from clustering?

Hudi is modeled like a log-structured storage engine with multiple versions of the data.
Particularly, [Merge-On-Read](table_types#merge-on-read-table)
tables in Hudi store data using a combination of base file in columnar format and row-based delta logs that contain
updates. Compaction is a way to merge the delta logs with base files to produce the latest file slices with the most
recent snapshot of data. Compaction helps to keep the query performance in check (larger delta log files would incur
longer merge times on query side). On the other hand, clustering is a data layout optimization technique. One can stitch
together small files into larger files using clustering. Additionally, data can be clustered by sort key so that queries
can take advantage of data locality.

## Clustering Architecture

At a high level, Hudi provides different operations such as insert/upsert/bulk_insert through it’s write client API to be able to write data to a Hudi table. To be able to choose a trade-off between file size and ingestion speed, Hudi provides a knob `hoodie.parquet.small.file.limit` to be able to configure the smallest allowable file size. Users are able to configure the small file [soft limit](https://hudi.apache.org/docs/configurations/#hoodieparquetsmallfilelimit) to `0` to force new data to go into a new set of filegroups or set it to a higher value to ensure new data gets “padded” to existing files until it meets that limit that adds to ingestion latencies.



To be able to support an architecture that allows for fast ingestion without compromising query performance, we have introduced a ‘clustering’ service to rewrite the data to optimize Hudi data lake file layout.

Clustering table service can run asynchronously or synchronously adding a new action type called “REPLACE”, that will mark the clustering action in the Hudi metadata timeline.



### Overall, there are 2 steps to clustering

1.  Scheduling clustering: Create a clustering plan using a pluggable clustering strategy.
2.  Execute clustering: Process the plan using an execution strategy to create new files and replace old files.


### Schedule clustering

Following steps are followed to schedule clustering.

1.  Identify files that are eligible for clustering: Depending on the clustering strategy chosen, the scheduling logic will identify the files eligible for clustering.
2.  Group files that are eligible for clustering based on specific criteria. Each group is expected to have data size in multiples of ‘targetFileSize’. Grouping is done as part of ‘strategy’ defined in the plan. Additionally, there is an option to put a cap on group size to improve parallelism and avoid shuffling large amounts of data.
3.  Finally, the clustering plan is saved to the timeline in an avro [metadata format](https://github.com/apache/hudi/blob/master/hudi-common/src/main/avro/HoodieClusteringPlan.avsc).


### Execute clustering

1.  Read the clustering plan and get the ‘clusteringGroups’ that mark the file groups that need to be clustered.
2.  For each group, we instantiate appropriate strategy class with strategyParams (example: sortColumns) and apply that strategy to rewrite the data.
3.  Create a “REPLACE” commit and update the metadata in [HoodieReplaceCommitMetadata](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieReplaceCommitMetadata.java).


Clustering Service builds on Hudi’s MVCC based design to allow for writers to continue to insert new data while clustering action runs in the background to reformat data layout, ensuring snapshot isolation between concurrent readers and writers.

NOTE: Clustering can only be scheduled for tables / partitions not receiving any concurrent updates. In the future, concurrent updates use-case will be supported as well.

![Clustering example](/assets/images/blog/clustering/clustering1_new.png)
_Figure: Illustrating query performance improvements by clustering_

## Clustering Usecases

### Batching small files

As mentioned in the intro, streaming ingestion generally results in smaller files in your data lake. But having a lot of
such small files could lead to higher query latency. From our experience supporting community users, there are quite a
few users who are using Hudi just for small file handling capabilities. So, you could employ clustering to batch a lot
of such small files into larger ones.

![Batching small files](/assets/images/blog/clustering/clustering2_new.png)

### Cluster by sort key

Another classic problem in data lake is the arrival time vs event time problem. Generally you write data based on
arrival time, while query predicates do not sit well with it. With clustering, you can re-write your data by sorting
based on query predicates and so, your data skipping will be very efficient and your query can ignore scanning a lot of
unnecessary data.

![Batching small files](/assets/images/blog/clustering/clustering_3.png)

## Clustering Strategies

On a high level, clustering creates a plan based on a configurable strategy, groups eligible files based on specific
criteria and then executes the plan. As mentioned before, clustering plan as well as execution depends on configurable
strategy. These strategies can be broadly classified into three types: clustering plan strategy, execution strategy and
update strategy.

### Plan Strategy

This strategy comes into play while creating clustering plan. It helps to decide what file groups should be clustered
and how many output file groups should the clustering produce. Note that these strategies are easily pluggable using the
config [hoodie.clustering.plan.strategy.class](configurations#hoodieclusteringplanstrategyclass).

Different plan strategies are as follows:

#### Size-based clustering strategies

This strategy creates clustering groups based on max size allowed per group. Also, it excludes files that are greater
than the small file limit from the clustering plan. Available strategies depending on write client
are: `SparkSizeBasedClusteringPlanStrategy`, `FlinkSizeBasedClusteringPlanStrategy`
and `JavaSizeBasedClusteringPlanStrategy`. Furthermore, Hudi provides flexibility to include or exclude partitions for
clustering, tune the file size limits, maximum number of output groups. Please refer to [hoodie.clustering.plan.strategy.small.file.limit](https://hudi.apache.org/docs/next/configurations/#hoodieclusteringplanstrategysmallfilelimit)
, [hoodie.clustering.plan.strategy.max.num.groups](https://hudi.apache.org/docs/next/configurations/#hoodieclusteringplanstrategymaxnumgroups), [hoodie.clustering.plan.strategy.max.bytes.per.group](https://hudi.apache.org/docs/next/configurations/#hoodieclusteringplanstrategymaxbytespergroup)
, [hoodie.clustering.plan.strategy.target.file.max.bytes](https://hudi.apache.org/docs/next/configurations/#hoodieclusteringplanstrategytargetfilemaxbytes) for more details.

| Config Name                                             | Default            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|---------------------------------------------------------| -------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.clustering.plan.strategy.partition.selected      | N/A **(Required)** | Comma separated list of partitions to run clustering<br /><br />`Config Param: PARTITION_SELECTED`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| hoodie.clustering.plan.strategy.partition.regex.pattern | N/A **(Required)** | Filter clustering partitions that matched regex pattern<br /><br />`Config Param: PARTITION_REGEX_PATTERN`<br />`Since Version: 0.11.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| hoodie.clustering.plan.partition.filter.mode            | NONE (Optional)    | Partition filter mode used in the creation of clustering plan. Possible values:<br /><ul><li>`NONE`: Do not filter partitions. The clustering plan will include all partitions that have clustering candidates.</li><li>`RECENT_DAYS`: This filter assumes that your data is partitioned by date. The clustering plan will only include partitions from K days ago to N days ago, where K &gt;= N. K is determined by `hoodie.clustering.plan.strategy.daybased.lookback.partitions` and N is determined by `hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions`.</li><li>`SELECTED_PARTITIONS`: The clustering plan will include only partition paths with names that sort within the inclusive range [`hoodie.clustering.plan.strategy.cluster.begin.partition`, `hoodie.clustering.plan.strategy.cluster.end.partition`].</li><li>`DAY_ROLLING`: To determine the partitions in the clustering plan, the eligible partitions will be sorted in ascending order. Each partition will have an index i in that list. The clustering plan will only contain partitions such that i mod 24 = H, where H is the current hour of the day (from 0 to 23).</li></ul><br />`Config Param: PLAN_PARTITION_FILTER_MODE_NAME`<br />`Since Version: 0.11.0` |


#### SparkSingleFileSortPlanStrategy

In this strategy, clustering group for each partition is built in the same way as `SparkSizeBasedClusteringPlanStrategy`
. The difference is that the output group is 1 and file group id remains the same,
while `SparkSizeBasedClusteringPlanStrategy` can create multiple file groups with newer fileIds.

#### SparkConsistentBucketClusteringPlanStrategy

This strategy is specifically used for consistent bucket index. This will be leveraged to expand your bucket index (from
static partitioning to dynamic). Typically, users don’t need to use this strategy. Hudi internally uses this for
dynamically expanding the buckets for bucket index datasets.

:::note The latter two strategies are applicable only for the Spark engine.
:::

### Execution Strategy

After building the clustering groups in the planning phase, Hudi applies execution strategy, for each group, primarily
based on sort columns and size. The strategy can be specified using the
config [hoodie.clustering.execution.strategy.class](configurations/#hoodieclusteringexecutionstrategyclass). By
default, Hudi sorts the file groups in the plan by the specified columns, while meeting the configured target file
sizes.

| Config Name                                 | Default                                                                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------| ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| hoodie.clustering.execution.strategy.class  | org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy (Optional)     | Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the  clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while  meeting the configured target file sizes.<br /><br />`Config Param: EXECUTION_STRATEGY_CLASS_NAME`<br />`Since Version: 0.7.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

The available strategies are as follows:

1. `SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY`: Uses bulk_insert to re-write data from input file groups.
   1. Set `hoodie.clustering.execution.strategy.class`
      to `org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy`.
   2. `hoodie.clustering.plan.strategy.sort.columns`: Columns to sort the data while clustering. This goes in
      conjunction with layout optimization strategies depending on your query predicates. One can set comma separated
      list of columns that needs to be sorted in this config.
2. `JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY`: Similar to `SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY`, for the Java and Flink
   engines. Set `hoodie.clustering.execution.strategy.class`
   to `org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy`.
3. `SPARK_CONSISTENT_BUCKET_EXECUTION_STRATEGY`: As the name implies, this is applicable to dynamically expand
   consistent bucket index and only applicable to the Spark engine. Set `hoodie.clustering.execution.strategy.class`
   to `org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy`.

### Update Strategy

Currently, clustering can only be scheduled for tables/partitions not receiving any concurrent updates. By default,
the config for update strategy - [`hoodie.clustering.updates.strategy`](configurations/#hoodieclusteringupdatesstrategy) is set to ***SparkRejectUpdateStrategy***. If some file group has updates during clustering then it will reject updates and throw an
exception. However, in some use-cases updates are very sparse and do not touch most file groups. The default strategy to
simply reject updates does not seem fair. In such use-cases, users can set the config to ***SparkAllowUpdateStrategy***.

We discussed the critical strategy configurations. All other configurations related to clustering are
listed [here](configurations/#Clustering-Configs). Out of this list, a few configurations that will be very useful
for inline or async clustering are shown below with code samples.

## Inline clustering

Inline clustering happens synchronously with the regular ingestion writer or as part of the data ingestion pipeline. This means the next round of ingestion cannot proceed until the clustering is complete With inline clustering, Hudi will schedule, plan clustering operations after each commit is completed and execute the clustering plans after it’s created. This is the simplest deployment model to run because it’s easier to manage than running different asynchronous Spark jobs. This mode is supported on Spark Datasource, Flink, Spark-SQL and Hudi Streamer in a sync-once mode.

For this deployment mode, please enable and set: `hoodie.clustering.inline` 

To choose how often clustering is triggered, also set: `hoodie.clustering.inline.max.commits`. 

Inline clustering can be setup easily using spark dataframe options.
See sample below:

```scala
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._


val df =  //generate data frame
df.write.format("org.apache.hudi").
        options(getQuickstartWriteConfigs).
        option("hoodie.table.ordering.fields", "ts").
        option("hoodie.datasource.write.recordkey.field", "uuid").
        option("hoodie.datasource.write.partitionpath.field", "partitionpath").
        option("hoodie.table.name", "tableName").
        option("hoodie.parquet.small.file.limit", "0").
        option("hoodie.clustering.inline", "true").
        option("hoodie.clustering.inline.max.commits", "4").
        option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824").
        option("hoodie.clustering.plan.strategy.small.file.limit", "629145600").
        option("hoodie.clustering.plan.strategy.sort.columns", "column1,column2"). //optional, if sorting is needed as part of rewriting data
        mode(Append).
        save("dfs://location");
```

## Async Clustering

Async clustering runs the clustering table service in the background without blocking the regular ingestions writers. There are three different ways to deploy an asynchronous clustering process: 

- **Asynchronous execution within the same process**: In this deployment mode, Hudi will schedule and plan the clustering operations after each commit is completed as part of the ingestion pipeline. Separately, Hudi spins up another thread within the same job and executes the clustering table service. This is supported by Spark Streaming, Flink and Hudi Streamer in continuous mode. For this deployment mode, please enable `hoodie.clustering.async.enabled` and `hoodie.clustering.async.max.commits​`. 
- **Asynchronous scheduling and execution by a separate process**: In this deployment mode, the application will write data to a Hudi table as part of the ingestion pipeline. A separate clustering job will schedule, plan and execute the clustering operation. By running a different job for the clustering operation, it rebalances how Hudi uses compute resources: fewer compute resources are needed for the ingestion, which makes ingestion latency stable, and an independent set of compute resources are reserved for the clustering process. Please configure the lock providers for the concurrency control among all jobs (both writer and table service jobs). In general, configure lock providers when there are two different jobs or two different processes occurring. All writers support this deployment model. For this deployment mode, no clustering configs should be set for the ingestion writer. 
- **Scheduling inline and executing async**: In this deployment mode, the application ingests data and schedules the clustering in one job; in another, the application executes the clustering plan. The supported writers (see below) won’t be blocked from ingesting data. If the metadata table is enabled, a lock provider is not needed. However, if the metadata table is enabled, please ensure all jobs have the lock providers configured for concurrency control. All writers support this deployment option. For this deployment mode, please enable, `hoodie.clustering.schedule.inline` and `hoodie.clustering.async.enabled`.

Hudi supports [multi-writers](https://hudi.apache.org/docs/concurrency_control#enabling-multi-writing) which provides
snapshot isolation between multiple table services, thus allowing writers to continue with ingestion while clustering
runs in the background.

| Config Name                                                                                         | Default                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| --------------------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| hoodie.clustering.async.enabled                                        | false (Optional)                        | Enable running of clustering service, asynchronously as inserts happen on the table.<br /><br />`Config Param: ASYNC_CLUSTERING_ENABLE`<br />`Since Version: 0.7.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| hoodie.clustering.async.max.commits                                                            | 4 (Optional)                                                                                    | Config to control frequency of async clustering<br /><br />`Config Param: ASYNC_CLUSTERING_MAX_COMMITS`<br />`Since Version: 0.9.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## Setup Asynchronous Clustering
Users can leverage [HoodieClusteringJob](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance#RFC19Clusteringdataforfreshnessandqueryperformance-SetupforAsyncclusteringJob)
to setup 2-step asynchronous clustering.

### HoodieClusteringJob
By specifying the `scheduleAndExecute` mode both schedule as well as clustering can be achieved in the same step. 
The appropriate mode can be specified using `-mode` or `-m` option. There are three modes:

1. `schedule`: Make a clustering plan. This gives an instant which can be passed in execute mode.
2. `execute`: Execute a clustering plan at a particular instant. If no instant-time is specified, HoodieClusteringJob will execute for the earliest instant on the Hudi timeline.
3. `scheduleAndExecute`: Make a clustering plan first and execute that plan immediately.

Note that to run this job while the original writer is still running, please enable multi-writing:
```
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
```

A sample spark-submit command to setup HoodieClusteringJob is as below:

```bash
spark-submit \
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.2.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.2.jar" \
--class org.apache.hudi.utilities.HoodieClusteringJob \
/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.2.jar \
--props /path/to/config/clusteringjob.properties \
--mode scheduleAndExecute \
--base-path /path/to/hudi_table/basePath \
--table-name hudi_table_schedule_clustering \
--spark-memory 1g
```
A sample `clusteringjob.properties` file:
```
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=4
hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824
hoodie.clustering.plan.strategy.small.file.limit=629145600
hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy
hoodie.clustering.plan.strategy.sort.columns=column1,column2
```

### Hudi Streamer

This brings us to our users' favorite utility in Hudi. Now, we can trigger asynchronous clustering with Hudi Streamer.
Just set the `hoodie.clustering.async.enabled` config to true and specify other clustering config in properties file
whose location can be pased as `—props` when starting the Hudi Streamer (just like in the case of HoodieClusteringJob).

A sample spark-submit command to setup Hudi Streamer is as below:


```bash
spark-submit \
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.2.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.2.jar" \
--class org.apache.hudi.utilities.streamer.HoodieStreamer \
/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.2.jar \
--props /path/to/config/clustering_kafka.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
--source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
--source-ordering-field impresssiontime \
--table-type COPY_ON_WRITE \
--target-base-path /path/to/hudi_table/basePath \
--target-table impressions_cow_cluster \
--op INSERT \
--hoodie-conf hoodie.clustering.async.enabled=true \
--continuous
```

### Spark Structured Streaming

We can also enable asynchronous clustering with Spark structured streaming sink as shown below.
```scala
val commonOpts = Map(
   "hoodie.insert.shuffle.parallelism" -> "4",
   "hoodie.upsert.shuffle.parallelism" -> "4",
   "hoodie.datasource.write.recordkey.field" -> "_row_key",
   "hoodie.datasource.write.partitionpath.field" -> "partition",
   "hoodie.table.ordering.fields" -> "timestamp",
   "hoodie.table.name" -> "hoodie_test"
)

def getAsyncClusteringOpts(isAsyncClustering: String, 
                           clusteringNumCommit: String, 
                           executionStrategy: String):Map[String, String] = {
   commonOpts + (DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key -> isAsyncClustering,
           HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key -> clusteringNumCommit,
           HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key -> executionStrategy
   )
}

def initStreamingWriteFuture(hudiOptions: Map[String, String]): Future[Unit] = {
   val streamingInput = // define the source of streaming
   Future {
      println("streaming starting")
      streamingInput
              .writeStream
              .format("org.apache.hudi")
              .options(hudiOptions)
              .option("checkpointLocation", basePath + "/checkpoint")
              .mode(Append)
              .start()
              .awaitTermination(10000)
      println("streaming ends")
   }
}

def structuredStreamingWithClustering(): Unit = {
   val df = //generate data frame
   val hudiOptions = getClusteringOpts("true", "1", "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy")
   val f1 = initStreamingWriteFuture(hudiOptions)
   Await.result(f1, Duration.Inf)
}
```

## Java Client

Clustering is also supported via Java client. Plan strategy `org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy`
and execution strategy `org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy` are supported
out-of-the-box. Note that as of now only linear sort is supported in Java execution strategy.

## Related Resources

<h3>Blogs</h3>
[Apache Hudi Z-Order and Hilbert Space Filling Curves](https://www.onehouse.ai/blog/apachehudi-z-order-and-hilbert-space-filling-curves)
[Hudi Z-Order and Hilbert Space-filling Curves](https://medium.com/apache-hudi-blogs/hudi-z-order-and-hilbert-space-filling-curves-68fa28bffaf0)

<h3>Videos</h3>

* [Understanding Clustering in Apache Hudi and the Benefits of Asynchronous Clustering](https://www.youtube.com/watch?v=R_sm4wlGXuE)
