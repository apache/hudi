---
title: "Asynchronous Clustering using Hudi"
excerpt: "How to setup Hudi for asynchronous clustering"
author: codope
category: blog
image: /assets/images/blog/clustering/example_perf_improvement.png
---

In one of the [previous blog](/blog/2021/01/27/hudi-clustering-intro) posts, we introduced a new
kind of table service called clustering to reorganize data for improved query performance without compromising on
ingestion speed. We learnt how to setup inline clustering. In this post, we will discuss what has changed since then and
see how asynchronous clustering can be setup using HoodieClusteringJob as well as DeltaStreamer utility.

<!--truncate-->

## Introduction

On a high level, clustering creates a plan based on a configurable strategy, groups eligible files based on specific
criteria and then executes the plan. Hudi supports [multi-writers](https://hudi.apache.org/docs/concurrency_control#enabling-multi-writing) which provides
snapshot isolation between multiple table services, thus allowing writers to continue with ingestion while clustering
runs in the background. For a more detailed overview of the clustering architecture please check out the previous blog
post.

## Clustering Strategies

As mentioned before, clustering plan as well as execution depends on configurable strategy. These strategies can be
broadly classified into three types: clustering plan strategy, execution strategy and update strategy.

### Plan Strategy

This strategy comes into play while creating clustering plan. It helps to decide what file groups should be clustered.
Let's look at different plan strategies that are available with Hudi. Note that these strategies are easily pluggable
using this [config](/docs/next/configurations#hoodieclusteringplanstrategyclass).

1. `SparkSizeBasedClusteringPlanStrategy`: It selects file slices based on
   the [small file limit](/docs/next/configurations/#hoodieclusteringplanstrategysmallfilelimit)
   of base files and creates clustering groups upto max file size allowed per group. The max size can be specified using
   this [config](/docs/next/configurations/#hoodieclusteringplanstrategymaxbytespergroup). This
   strategy is useful for stitching together medium-sized files into larger ones to reduce lot of files spread across
   cold partitions.
2. `SparkRecentDaysClusteringPlanStrategy`: It looks back previous 'N' days partitions and creates a plan that will
   cluster the 'small' file slices within those partitions. This is the default strategy. It could be useful when the
   workload is predictable and data is partitioned by time.
3. `SparkSelectedPartitionsClusteringPlanStrategy`: In case you want to cluster only specific partitions within a range,
   no matter how old or new are those partitions, then this strategy could be useful. To use this strategy, one needs
   to set below two configs additionally (both begin and end partitions are inclusive):

```
hoodie.clustering.plan.strategy.cluster.begin.partition
hoodie.clustering.plan.strategy.cluster.end.partition
```

:::note
All the strategies are partition-aware and the latter two are still bound by the size limits of the first strategy.
:::

### Execution Strategy

After building the clustering groups in the planning phase, Hudi applies execution strategy, for each group, primarily
based on sort columns and size. The strategy can be specified using this [config](/docs/next/configurations/#hoodieclusteringexecutionstrategyclass).

`SparkSortAndSizeExecutionStrategy` is the default strategy. Users can specify the columns to sort the data by, when
clustering using
this [config](/docs/next/configurations/#hoodieclusteringplanstrategysortcolumns). Apart from
that, we can also set [max file size](/docs/next/configurations/#hoodieparquetmaxfilesize)
for the parquet files produced due to clustering. The strategy uses bulk insert to write data into new files, in which
case, Hudi implicitly uses a partitioner that does sorting based on specified columns. In this way, the strategy changes
the data layout in a way that not only improves query performance but also balance rewrite overhead automatically.

Now this strategy can be executed either as a single spark job or multiple jobs depending on number of clustering groups
created in the planning phase. By default, Hudi will submit multiple spark jobs and union the results. In case you want
to force Hudi to use single spark job, set the execution strategy
class [config](/docs/next/configurations/#hoodieclusteringexecutionstrategyclass)
to `SingleSparkJobExecutionStrategy`.

### Update Strategy

Currently, clustering can only be scheduled for tables/partitions not receiving any concurrent updates. By default,
the [config for update strategy](/docs/next/configurations/#hoodieclusteringupdatesstrategy) is
set to ***SparkRejectUpdateStrategy***. If some file group has updates during clustering then it will reject updates and
throw an exception. However, in some use-cases updates are very sparse and do not touch most file groups. The default
strategy to simply reject updates does not seem fair. In such use-cases, users can set the config to ***SparkAllowUpdateStrategy***.

We discussed the critical strategy configurations. All other configurations related to clustering are
listed [here](/docs/next/configurations/#Clustering-Configs). Out of this list, a few
configurations that will be very useful are:

|  Config key  | Remarks | Default |
|  -----------  | -------  | ------- |
| `hoodie.clustering.async.enabled` | Enable running of clustering service, asynchronously as writes happen on the table. | False |
| `hoodie.clustering.async.max.commits` | Control frequency of async clustering by specifying after how many commits clustering should be triggered. | 4 |
| `hoodie.clustering.preserve.commit.metadata` | When rewriting data, preserves existing _hoodie_commit_time. This means users can run incremental queries on clustered data without any side-effects. | False |

## Asynchronous Clustering

Previously, we have seen how users
can [setup inline clustering](/blog/2021/01/27/hudi-clustering-intro#setting-up-clustering).
Additionally, users can
leverage [HoodieClusteringJob](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance#RFC19Clusteringdataforfreshnessandqueryperformance-SetupforAsyncclusteringJob)
to setup 2-step asynchronous clustering.

### HoodieClusteringJob

With the release of Hudi version 0.9.0, we can schedule as well as execute clustering in the same step. We just need to
specify the `—mode` or `-m` option. There are three modes:

1. `schedule`: Make a clustering plan. This gives an instant which can be passed in execute mode.
2. `execute`: Execute a clustering plan at given instant which means --instant-time is required here.
3. `scheduleAndExecute`: Make a clustering plan first and execute that plan immediately.

Note that to run this job while the original writer is still running, please enable multi-writing:
```
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
```

A sample spark-submit command to setup HoodieClusteringJob is as below:

```bash
spark-submit \
--class org.apache.hudi.utilities.HoodieClusteringJob \
/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.9.0-SNAPSHOT.jar \
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

### HoodieDeltaStreamer

This brings us to our users' favorite utility in Hudi. Now, we can trigger asynchronous clustering with DeltaStreamer.
Just set the `hoodie.clustering.async.enabled` config to true and specify other clustering config in properties file
whose location can be pased as `—props` when starting the deltastreamer (just like in the case of HoodieClusteringJob).

A sample spark-submit command to setup HoodieDeltaStreamer is as below:

```bash
spark-submit \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.9.0-SNAPSHOT.jar \
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
   DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
   DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
   DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
   HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
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

## Conclusion and Future Work

In this post, we discussed different clustering strategies and how to setup asynchronous clustering. The story is not
over yet and future work entails:

- Support clustering with updates.
- CLI tools to support clustering.

Please follow this [JIRA](https://issues.apache.org/jira/browse/HUDI-1042) to learn more about active development on
this issue. We look forward to contributions from the community. Hope you enjoyed this post. Put your Hudi on and keep
streaming!
