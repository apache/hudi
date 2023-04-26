---
title: Clustering
summary: "In this page, we describe async compaction in Hudi."
toc: true
last_modified_at:
---

In data lakes/warehouses one of the key trade-offs is between write speeds (data freshness) and query performance. Data writes are faster with small files since data is processed as soon as it’s available, avoiding an intermediate aggregation step to combine smaller files into larger ones. However, query performance degrades poorly with a lot of small files. In particular, small files cost more disk seeks and slow down compute in downstream distributed computing applications. Please refer to the [file size](https://hudi.apache.org/docs/file_sizing) documentation about the small file problem. Hudi provides the clustering table service to:

- decouple file sizing from ingestion into a separate table service where smaller files are combined to larger files without impacting write speeds and data freshness
- improve query performance by reorganizing the data layout via techniques such as sorting data on different columns

## Data ingestion and file sizing trade-offs

When writing data to a Hudi table, there are different write modes one can choose that will impact the tradeoff between write speed and file sizes. For example, BULK_INSERT without any additional configurations will have very fast ingestion, but this mode may create a lot of small files. You can optionally set the sorting mode for BULK_INSERT to fix the file sizing at the time of ingestion, but it comes with the cost of having a higher write latency.

Another example is how to manage faster inserts and better file sizing. Hudi has a configuration called `hoodie.parquet.small.file.limit` to specify the maximum file size for a small Parquet file. This configuration helps Hudi determine where inserts will get written to. Small file groups with size under this limit are considered for accepting new inserts than larger file groups. By setting an appropriate limit, you can control how Hudi handles small files to improve query performance and storage efficiency while maintaining a balance with write speed. For example, if you set the `hoodie.parquet.small.file.limit` to a value such as 50MB, Hudi will consider any Parquet file with a size less than 50MB as a small file. These small files will then be targeted for merging new records into file groups to create larger files. 

Please refer to the [write operations](https://hudi.apache.org/docs/write_operations) documentation for more details about the different write operations and their configurations.

## Clustering allows fast ingestion without compromising query performance 

The previous section discussed trade-offs to consider when maintaining appropriate file sizes and balancing the write speeds. With the clustering service, you can optimize the Hudi data lake file layout by rewriting data by combining smaller files to new larger files  while maintaining write speeds. The clustering service has different processes to help schedule, plan, and execute a clustering strategy. Once the plan is executed, it can be deployed either inline with your data pipeline (along with the writes) or in a separate job where it runs asynchronously along with the writes. When clustering is performed, a `REPLACECOMMIT` action will appear in the Hudi [timeline](https://hudi.apache.org/docs/timeline). 

![Batching small files](/assets/images/clustering_small_files.gif)

## The difference between compaction and clustering

Hudi is modeled like a log-structured storage engine with multiple versions of the data. Particularly, [Merge-On-Read tables](https://hudi.apache.org/docs/table_types#merge-on-read-table) in Hudi store data using a combination of base file in columnar format and row-based delta logs that contain updates. [Compaction](https://hudi.apache.org/docs/next/compaction) is a way to merge the delta logs with base files to produce the latest file slices with the most recent snapshot of data. Compaction helps to keep the query performance in check (larger delta log files would incur longer merge times on query side). On the other hand, clustering is a data layout optimization technique. One can stitch together small files into larger files using clustering. Additionally, data can be clustered by sort key so that queries can take advantage of data locality.

By default, the clustering service is not enabled. The following section will cover how to enable, manage and deploy the clustering service in more detail.

## Schedule, plan, execute and deploy a clustering service

The clustering service is composed of a scheduling and an execution process. The schedule process allows you to set a clustering schedule so the clustering process can commence and generate a clustering plan base on the configurations provided. The execution process reads the clustering plan and helps rewrite the data. These processes can be broken down into 3 parts that you’ll need to consider for your application: 

1. **Schedule the clustering interval**: You’ll set a clustering schedule so the clustering process commences. When a threshold is reached, the clustering plan is triggered. 
2. **Create a clustering plan**: Based on the clustering configuration for file groups, layout and/or file sizing, the clustering service generates a clustering plan to be execution.
3. **Execute the clustering plan**: Hudi reads the clustering plan and executes appropriately to rewrite the data.

For the clustering service to be enabled in an application, you first need to configure what deployment model you want. This is how the clustering plan will be executed. There are three ways to configure this:

- **Inline**: Use this deployment model if you want to schedule, plan and execute the clustering service inline with the the data pipeline. To use this mode, configure `hoodie.clustering.inline = true`. 
- **Schedule inline and execute asynchronous**: Use this deployment model if you want to schedule and plan the clustering service inline with the data pipeline. Separately, the execution part of the clustering plan will be run in another job outside of the data pipeline. To use this mode, configure `hoodie.clustering.schedule.inline=true` and `hoodie.clustering.inline=false`.
- **Asynchronous**: Use this deployment model if you want to schedule, plan and execute the clustering plan in a separate job from the data pipeline. To use this mode, configure `hoodie.clustering.async.enabled=true`.

The rest of this document will cover the different parts in detail. 

### Schedule the clustering interval 

The clustering schedule is triggered by the number of regular completed commits in the Hudi timeline. Here’s how to schedule the clustering service base on the deployment model configured: 

- **Inline**: You can schedule clustering for this deployment model with this configuration: `hoodie.clustering.inline.max.commits`. 
- **Schedule inline and execute asynchronously**: You can schedule clustering for this deployment model with this configuration: `hoodie.clustering.async.max.commits`.
- **Asynchronous**: You can schedule clustering for this deployment model with this configuration: `hoodie.clustering.async.max.commits`. 

#### Configuration details to schedule the clustering interval:
`hoodie.clustering.inline.max.commits`​: This configuration controls how frequently the clustering plan is triggered. Use this configuration to plan on an inline deployment model. The default value is 4. 

`hoodie.clustering.async.max.commits`​: This configuration controls how frequently the clustering plan is triggered. Use this configuration to plan on an async deployment model. The default value is 4. 

### Create a clustering plan
Hudi applies the clustering plan to help decide what file groups should be clustered. Before diving in further, here are important concepts to understand:

- **A cluster group**: A group of file groups. 
- **Parallelism**: The number of spark tasks or threads to run for the clustering service.

The most important parts of the clustering plan are:

1. **Identifying files eligible for clustering**: Depending on the chosen clustering strategy, the scheduling logic will identify the files eligible for clustering.
2. **Grouping files eligible for clustering based on specific criteria**: One or multiple data files are designated to a clustering group. Each clustering group writes to one or more data files based on the data file size configured. The maximum size of each clustering group is determined by the `hoodie.clustering.plan.strategy.max.bytes.per.group` configuration. Grouping is done as part of the strategy defined in the plan. Smaller clustering group in size improves parallelism and avoids shuffling large amounts of data because each Spark task executes each group. However, if sorting is important,  it’s best to have all the data within a partition be in 1 large cluster group. You can sort the data by setting `hoodie.clustering.plan.strategy.sort.columns`. More details about sorting are described below. 


### Clustering plan strategy
Hudi applies the planning strategy through the [hoodie.clustering.plan.strategy.class](https://hudi.apache.org/docs/configurations#hoodieclusteringplanstrategyclass). First, let's look at different plan strategies that are available with this configuration. 

All the following strategies select file slices based on the [small file limit](https://hudi.apache.org/docs/configurations/#hoodieclusteringplanstrategysmallfilelimit), determined by `hoodie.clustering.plan.strategy.small.file.limit`, of base files and create clustering groups up to the max file size allowed per group. The max size can be specified using the `hoodie.clustering.plan.strategy.max.bytes.per.group`. This strategy is useful for stitching together medium-sized files into larger ones to reduce the number of files.

1. `SparkSizeBasedClusteringPlanStrategy`: This is the default strategy.  It plans the clustering for all partitions. There’s no extra configuration needed for this strategy. 

2. `SparkRecentDaysClusteringPlanStrategy`: This strategy applies clustering to recent partitions in a table partitioned by dates. It looks at the partitions of previous 'N' days only and creates a plan to cluster the 'small' file slices within them. To optimize compute resources, it could be helpful to limit the clustering service to recent data only. If this strategy is chosen, you’ll need to see these additional configurations. 
   - `hoodie.clustering.plan.strategy.daybased.lookback.partitions`​: This is the number (N) of partitions that will be considered for constructing a clustering plan.
   - `hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions`​: This is the number of partitions to skip (SKIP) or not be clustered from (N).

   - **Example**: If you have partitions: 2023-04-01, 2023-04-02, 2023-04-03, 2023-04-04, 2023-04-05, 2023-04-06 and N=5, SKIP=2:
      - 2023-04-05 and 2023-04-06 **ARE NOT** clustered
      - 2023-04-02, 2023-04-03, 2023-04-04 **ARE** clustered

3. `SparkSelectedPartitionsClusteringPlanStrategy`: This strategy is useful to cluster only specific partitions within a range, no matter how old or new those partitions are. To use this strategy, set additionally two configs (both begin and end partitions are inclusive):
   - `hoodie.clustering.plan.strategy.cluster.begin.partition`
   - `hoodie.clustering.plan.strategy.cluster.end.partition`

### Other configuration details to create the clustering plan
These configurations are optional for the clustering plan, but it could be useful for fine-tuning the clustering behavior: 

- `hoodie.clustering.plan.strategy.max.num.groups`: This configuration determines the maximum number of cluster groups a clustering plan will have. Increasing the number of file groups increases the parallelism or the number of Spark tasks or threads Hudi can run. By setting an appropriate value for this property, you can control the number of groups created during the clustering process. The default value is 30. 
   - For example, if you set `hoodie.clustering.plan.strategy.max.num.groups` to 100, Hudi will create no more than 100 groups of small files to be merged together during the clustering process. 
- `hoodie.clustering.plan.strategy.max.bytes.per.group`: This configuration sets the maximum total size for a cluster group consisting of small files targeted for clustering. The default value is 2147483648 (2GB) 
    - For example, if you set `hoodie.clustering.plan.strategy.max.bytes.per.group` to 500MB, Hudi will group small files together for clustering so that the total size of the group does not exceed 500MB. 

- `hoodie.clustering.plan.strategy.target.file.max.bytes`: This configuration property sets the target maximum file size for the output files created during the clustering process. This configuration helps Hudi determine the optimal size for the new and larger files generated as a result of clustering smaller files together. By setting an appropriate value for this property, you can control the size of the output files created by the clustering operation, ensuring they are neither too small (which would lead to the small file problem) nor too large (which could impact the parallelism of reading files and degrade query performance). The default value is 1073741824 (1GB). 
   - For example, if you set `hoodie.clustering.plan.strategy.target.file.max.bytes` to 128MB, Hudi will aim to create output files that are approximately 128MB in size during the clustering process. This helps achieve a balance between query performance and storage efficiency while minimizing the impact of the small file problem.

#### Example walkthrough of the planning phase:
Let’s explore an example to understand how the planning phase operates in Hudi clustering:

Imagine the application has a Hudi table with 100 file groups, each being 100MB in size. The configuration for this table includes the following settings:

```
// omitted other configs 

hoodie.clustering.plan.strategy.max.num.groups = 10
hoodie.clustering.plan.strategy.max.bytes.per.group = 524288000
hoodie.clustering.plan.strategy.target.file.max.bytes = 262144000
```
 
Implicitly, Hudi will apply the `SparkSizeBasedClusteringPlanStrategy`. Hudi will form a cluster group consisting of 5 file groups (500MB/100MB = 5 file groups). This process repeats until 10 cluster groups are created, as specified by the `hoodie.clustering.plan.strategy.max.num.groups` configuration. Each cluster group’s execution is parallelized using Spark.

After clustering is complete, Hudi generates 2 output file groups per cluster group (500MB/250MB = 2). The total amount of data processed in a clustering operation is determined by these two properties: `clustering.plan.strategy.max.bytes.per.group` * `hoodie.clustering.plan.strategy.max.num.groups`. Once all planning configurations are set, Hudi stores the clustering plan in the timeline using Avro metadata format, creating a **REPLACECOMMIT.requested** instant in the timeline.

These are not all the planning configurations!  Here are a few other configurations to consider.

- `hoodie.clustering.schedule.inline`: This configuration determines whether the clustering scheduling should be performed inline (synchronously) during writes.  This is often used to schedule clustering inline only, without executing the clustering plan in the same job. 

   - If you set both `hoodie.clustering.inline` and `hoodie.clustering.schedule.inline` to false, both scheduling and execution will be performed asynchronously. Therefore, you need to set `hoodie.clustering.async.enabled` to true. 

   - If `hoodie.clustering.inline` is set to false, and `hoodie.clustering.schedule.inline` is set to true, writers will schedule clustering inline, but the application should trigger an  asynchronous job for the execution. 

- `hoodie.clustering.plan.strategy.sort.columns`: This configuration specifies the columns used to sort the data during the clustering process. Sorting data based on these columns can improve query performance, especially for range or predicate-based queries, by ensuring that related data records are stored together in the same or nearby data files. When defining the sort columns, consider the columns that are frequently used in the application’s queries’ filter conditions or join operations. This helps with data skipping because the query engine can ignore scanning a lot of unnecessary data. There is no default value, meaning that there is no sorting.
   - Set the configuration for multiple columns or just 1 column: 
     - `hoodie.clustering.plan.strategy.sort.columns = “column1, column2, column3” ` 
     - `hoodie.clustering.plan.strategy.sort.columns = “column1” `
  - If you specify a column, the default layout strategy will be linear, unless otherwise specified. Please see `hoodie.layout.optimize.strategy` for more details. 
With clustering, you can re-write your data by sorting based on query predicates and so, 
 
 ![Batching small files](/assets/images/clustering_sort.gif)
 
- `hoodie.layout.optimize.strategy`: This configuration is used when `hoodie.clustering.plan.strategy.sort.columns` is specified. The layout strategy optimizes the data layout on disk by reorganizing the data according to the chosen ordering strategy based on specific columns and can help improve query performance. There are three values to set for this strategy: `linear`, `z-order` and `hilbert`. The default value is linear. 

- `hoodie.clustering.plan.strategy.small.file.limit`: This configuration determines the threshold of what constitutes a small file. During the clustering process, a data file whose size is less than this specified threshold is considered a small file and is included as part of the clustering operation where they are merged into larger files. The default value is  314572800 (300MB). 

## Execute the clustering​ plan

After building the clustering groups in the planning phase, Hudi applies an execution strategy for each group, primarily based on file groups or partitions sort columns and size. How the execution plan is executed is determined by the deployment model that’ll be described in a later section. When the file groups have been rewritten, a **REPLACECOMMIT.commit** file is created on the Hudi timeline. At a high level, here’s how Hudi executes the clustering service:
1. Hudi reads the clustering plan and gathers the clustering groups. The clustering groups mark which file groups need to be clustered. 
2. For each cluster group, Hudi instantiates an appropriate execution strategy class with the parameters that are necessary for executing the clustering, i.e., the sorting columns via hoodie.clustering.plan.strategy.sort.columns, and applies that strategy to rewrite the data.
3. Hudi creates a **REPLACECOMMIT.commit** in the .hoodie folder and updates the metadata in HoodieReplaceCommitMetadata.

Clustering Service builds on Hudi’s MVCC-based design to allow the writers to continue to insert new data while clustering action runs in the background to reformat the data layout, ensuring snapshot isolation between concurrent readers and writers.

:::note
If there is a pending clustering, i.e., REPLACECOMMIT.requested instant is created in the Hudi timeline, Hudi can’t apply new updates to those file groups. In the future, concurrent updates use-case will be supported as well.
:::

### Clustering execution strategy 
The execution strategy described earlier can be specified using this configuration: hoodie.clustering.execution.strategy.class. Below describes the different strategies you can set for this:

- `SparkSortAndSizeExecutionStrategy`: This is the default strategy. The strategy uses bulk insert to write data into new files. From there, Hudi implicitly uses a partitioner that can optionally sort data based on the specified columns. This changes the data layout in a way that not only improves query performance but also balances rewrite overhead automatically. This strategy can be executed either as a single Spark job or multiple Spark jobs depending on the number of clustering groups created in the planning phase. By default, Hudi will submit multiple Spark tasks and union the write stats to write metadata. To force Hudi to use a single Spark task, set hoodie.clustering.execution.strategy.class to SingleSparkJobExecutionStrategy.
You can specify the columns to sort the data by using the hoodie.clustering.plan.strategy.sort.columns. 

- `SparkConsistentBucketClusteringExecutionStrategy`: This is used for a clustering execution strategy specifically for the consistent hashing index.

- `JavaSortAndSizeExecutionStrategy`: This is used for a clustering execution strategy specifically for a Java engine only. Please do not use this strategy for any other writers, i.e., DeltaStreamer, Spark Datasource and etc.


![Clustering example](/assets/images/blog/clustering/example_perf_improvement.png)
_Figure: Illustrating query performance improvements by clustering_

#### Other configurations to consider
- `hoodie.clustering.updates.strategy`: This configuration determines how to handle updates and deletes to file groups that are under clustering. The default strategy is SparkRejectUpdateStrategy which rejects the updates for file groups that are part of an ongoing clustering operation. This is to prevent data inconsistencies that could occur when updates and clustering operations are performed simultaneously on the same file groups. You can create a custom update strategy class by extending the `org.apache.hudi.client.clustering.update.UpdateStrategy` interface. 


## Clustering Deployment Models 
Once the clustering strategy is configured for the application, it can be deployed in several ways:

### Inline Clustering​
Inline clustering refers to the process of executing a clustering process as part of the data ingestion pipeline, rather than running them asynchronously as a separate job. With inline clustering, Hudi will schedule, plan clustering operations after each commit is completed and execute the clustering plans after it’s created. This is the simplest deployment model to run because it’s easier to manage than running different asynchronous Spark jobs (see below). This mode is supported on Spark Datasource, Flink, Spark-SQL and DeltaStreamer in sync-once mode. In the next section, we’ll go over code snippets you can use to get started with inline clustering.

For this deployment mode, please enable and set: `hoodie.clustering.inline` and `hoodie.clustering.inline.max.commits`. 

### Asynchronous Clustering​
There are three ways to execute an asynchronous clustering process:

- **Asynchronous execution within the same process**: In this deployment mode, Hudi will schedule and plan the clustering operations after each commit is completed as part of the ingestion pipeline. Separately, Hudi spins up another thread within the same job and executes the clustering table service. This is supported by Spark Streaming, Flink and DeltaStreamer in continuous mode. For this deployment mode, please enable `hoodie.clustering.async.enabled` and`hoodie.clustering.async.max.commits​`. 


- **Asynchronous scheduling and execution by a separate process**: In this deployment mode, the application will write data to a Hudi table as part of the ingestion pipeline. A separate clustering job will schedule, plan and execute the clustering operation. By running a different job for the clustering operation, it rebalances how Hudi uses compute resources: fewer compute resources are needed for the ingestion, which makes ingestion latency stable, and an indepedent set of compute resources are reserved for the clustering process. Please configure the lock providers for the concurrency control among all jobs (both writer and table service jobs). In general, configure lock providers when there are two different jobs or two different processes occurring. All writers support this deployment model.
For this deployment mode, no clustering configs should be set for the ingestion writer. 

- **Scheduling inline and executing async**: In this deployment mode, the application ingests data and schedules the clustering in one job; in another, the application executes the clustering plan. The supported writers (see below) won’t be blocked from ingesting data. If the metadata table is enabled, a lock provider is not needed. However, if the metadata table is enabled, please ensure all jobs have the lock providers configured for concurrency control. All writers support this deployment option. 
For this deployment mode, please enable,`hoodie.clustering.schedule.inline` and `hoodie.clustering.async.enabled`.

### HoodieClusteringJob​

This helps to schedule and execute clustering operations as a separate, standalone job. This class is designed to be used with Hudi’s Spark integration, allowing Hudi to run clustering jobs on the datasets outside of the data ingestion pipeline asynchronously.

You can leverage the HoodieClusteringJob to set up a 2-step asynchronous clustering job. The appropriate mode can be specified using -mode or -m option. There are three modes:
1. **“schedule”**: Make a clustering plan. This gives an instant that can be passed in execute mode.
2. **“execute”**: Execute a clustering plan at a particular instant. If no instant time is specified, HoodieClusteringJob will execute for the earliest instant on the Hudi timeline.
3. **“scheduleAndExecute”**: Both the planning and execution can be achieved in the same step. First, the clustering plan is made. Then, the plan is executed immediately.

To run this job while during ingestion, please configure concurrency control:
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider


## Limitations and considerations with the clustering service
​​There are some limitations within the clustering service that’s worth mentioning:  
- Clustering doesn't work with the incremental timeline. Be default, please keep this setting to false: `hoodie.filesystem.view.incr.timeline.sync.enable: false`
- The clustering process can increase storage usage: When larger files are created as the result of the clustering process, the storage use will increase because the old small files still exist until the cleaning operation is completed.

## Code Examples
### Inline clustering example
This is a code example for inline clustering. The example below shows how inline clustering can be configured easily using Spark Dataframe options. 

```
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val df =  //generate data frame
df.write.format("org.apache.hudi").
       options(getQuickstartWriteConfigs).
       option(PRECOMBINE_FIELD_OPT_KEY, "ts").
       option(RECORDKEY_FIELD_OPT_KEY, "uuid").
       option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
       option(TABLE_NAME, "tableName").
       option("hoodie.clustering.inline", "true").

       mode(Append).
       save("dfs://location");
```

### Async clustering example
This is a code example for async clustering. The example below shows how async clustering can be deployed easily using a Spark job. You need to set the `hoodie.clustering.async.enable`d` configuration to true and specify other clustering configuration in the properties file whose location can be placed as —props when starting DeltaStreamer (just like in the case of HoodieClusteringJob).


A sample spark-submit command to setup HoodieClusteringJob is as below:

```
spark-submit \
--class org.apache.hudi.utilities.HoodieClusteringJob \
/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.13.0.jar \
--props /path/to/config/clusteringjob.properties \
--mode scheduleAndExecute \
--base-path /path/to/hudi_table/basePath \
--table-name hudi_table_schedule_clustering \
--spark-memory 1g

A sample clusteringjob.properties file:
hoodie.clustering.async.max.commits=4
hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824
hoodie.clustering.plan.strategy.small.file.limit=629145600

hoodie.clustering.plan.strategy.sort.columns=column1,column2
```

### HoodieDeltaStreamer​
You can deploy an asynchronous clustering with DeltaStreamer. Below, is an example of a spark-submit command to setup HoodieDeltaStreamer is as below:

```
spark-submit \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.13.0.jar \
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

### Spark Structured Streaming​
You can also enable asynchronous clustering with Spark Structured Streaming sink as shown below.

```python
val commonOpts = Map(

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