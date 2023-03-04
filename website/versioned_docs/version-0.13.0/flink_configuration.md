---
title: Flink Setup
toc: true
---

[Apache Flink](https://flink.apache.org/what-is-flink/flink-architecture/) is a powerful streaming-batch integrated engine that provides a stream processing framework. Flink can process events at an incredible speed with low latency. Along with Hudi, you can use streaming ingestion and consumption with sources like Kafka; and also perform batch workloads like bulk ingest, snapshot queries and incremental queries. 

There are three execution modes a user can configure for Flink, and within each execution mode, users can use Flink SQL writing to configure their job options. The following section describes the necessary configs for different job conditions.   

## Configure Flink Execution Modes
You can configure the execution mode via the `execution.runtime-mode` setting. There are three possible modes:

- **STREAMING**: The classic DataStream execution mode. This is the default setting for the `StreamExecutionEnvironment`. 
- **BATCH**: Batch-style execution on the DataStream API
- **AUTOMATIC**: Let the system decide based on the boundedness of the sources

You can configured the execution mode via the command line:

```sh
$ bin/flink run -Dexecution.runtime-mode=BATCH <jarFile>

```

Separately, you can programmatically create and configure the `StreamExecutionEnvironment`, a Flink programming API. This execution environment is how all data pipelines are created and maintained.

You can configure the execution mode programmatically. Below is an example of how to set the `BATCH` mode.

```sh
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```
See the [Flink docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/execution_mode/) for more details.

## Global Configurations​

The global configurations are used to tune Flink for throughput, memory management and/or checkpoints (disaster recovery i.e., data loss). Two of the most important global configurations for a Flink job are parallelism and memory. For a long-running job, the initial resource configuration is crucial because open-source Flink does not support auto-pilot yet, where you can automatically scale up or down resources when there’s high or low data ingestion. So, you might waste or underutilize resources. 

All Hudi-specific parallelism and memory configurations depend on your Flink job resources.

When using Flink, you can set some global configurations in `$FLINK_HOME/conf/flink-conf.yaml`.

### Parallelism​

If your system has a lot of data to ingest, increasing the parallelism can improve throughput significantly. Hudi supplies flexible config options for specific operators, but at a high level, a default global parallelism can reduce the complexity of manual configuration. Try the default configuration and adjust as necessary. 

| Property Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `taskmanager.numberOfTaskSlots` | 1 | The is the number of parallel operators or user function instances that a single TaskManager can run. We recommend setting this value > 4, and the actual value needs to be set according to the amount of data | n/a | 0.9.0 |
| `parallelism.default` | 1 | The is the default parallelism used when no parallelism is specified anywhere (default: 1). For example, if the value of [`write.bucket_assign.tasks`](#parallelism-1) is not set, this value will be used | n/a | 0.9.0.|

### Memory​
The `JobManager` and `TaskManager` memory configuration is very important for a Flink job to work smoothly. Below, we'll describe these configurations. 

#### JobManager
The JobManager handles all the instants coordination. It keeps an in-memory fs view for all the file handles on the filesystem within its embedded timeline server. We need to ensure enough memory is allocated to avoid OOM errors. The configs below allow you to allocate the necessary memory. 

| Property Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `jobmanager.memory.process.size` | -- |This is the total process memory size for the JobManager. This includes all the memory that a JobManager JVM process consumes: Total Flink Memory, JVM Metaspace, and JVM Overhead | n/a | 0.9.0


#### TaskManager
The TaskManager is a container for the writing and table service tasks. For regular Parquet file flushing, we need to allocate enough memory to read and write files. At the same time, there must be enough resources for  MOR table compaction because it’s memory intensive: we need to read and merge all the log files into an output Parquet file. Below are the configs you can set for the TaskManager to allocate enough memory for these services. 

| Property Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `taskmanager.memory.task.heap.size` | -- | This is the task heap memory size for TaskExecutors. This is the size of JVM heap memory reserved for write cache. | n/a | 0.9.0 |
| `taskmanager.memory.managed.size` | -- | This is the managed memory size for TaskExecutors. This is the size of off-heap memory managed by the memory manager, reserved for sorting and RocksDB state backend. If you choose RocksDB as the state backend, you need to set this memory | n/a | 0.9.0 |

#### Checkpoint​
Checkpoint is a disaster recovery mechanism for Flink. When a job fails, users can choose to recover the job from the latest checkpoint to keep the latest data correctness. To keep the transaction integrity, we flush the memory buffer into a Hudi table for persistence during the checkpointing lifecycle. It’s important to note the Hudi transaction cannot be committed without enabling the checkpoint config.

Please read the Flink docs for [checkpoints](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/) for more details. 

| Property Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------
| `execution.checkpointing.interval` | -- | Setting this value as `execution.checkpointing.interval = 150000ms`, 150000ms = 2.5min. Configuring this parameter is equivalent to enabling the checkpoint | n/a | 0.9.0|
| `state.backend` | -- | This is the state backend to be used to store state. We recommend setting the store state as `rocksdb` : `state.backend: rocksdb`  | n/a | 0.9.0 |
| `state.backend.rocksdb.localdir` | -- | This is the local directory (on the TaskManager) where RocksDB stores its files. | n/a | 0.9.0 |
| `state.checkpoints.dir` | -- | This is the default directory used for storing the data files and meta data of checkpoints in a Flink supported filesystem. The storage path must be accessible from all participating processes/nodes(i.e., all **TaskManagers** and **JobManagers**), like the HDFS and OSS path. | n/a | 0.9.0 |
| `state.backend.incremental` |  --  | This is the option whether the state backend should create incremental checkpoints. For an incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the complete checkpoint state. If the store state setting is `rocksdb`, we recommend to set this config. | n/a | 0.9.0 |

### Per Job Options​
​
You can manually configure each job with respect to your workload. For example, if your workloads change, you can manually configure more memory or parallelism. You configure each Flink job with the SQL syntax through the WITH clause. Please take a look at the example below on how to start configuring your Flink job.

If you wanted to configure a Flink job to have more parallelism because you have high throughput, you can write something like this: 

```sql
CREATE TABLE hudi_table (
  …
) WITH (
  ‘connector’ = ‘hudi’,
  ‘write.tasks’ = ‘8’, –- configure the write task parallelism
);

```

Please use the configs down below to configure your Flink job:

#### Configs for job options

**Memory**​

When optimizing memory, we need be mindful of the: 
- memory configuration 
- number of taskManagers
- parallelism of write tasks (write.tasks: 4)

After we confirm each **TaskManager** has enough allocated memory, we can try to set these memory options for each task.


| Property Name | Default  | Description | Scope | Since Version  |
|----------------|--------|----------|---------------|--------------------------------------
| `write.task.max.size` | 1024D |  The memory reserved for write buffer is `write.task.max.size` - `compaction.max_memory`. This is the maximum memory in MB for a write task. When the threshold hits, it flushes the max size data bucket to avoid OOM. When the total buffer of write tasks reach this threshold, the largest buffer in the memory will be flushed | Write task | 0.9.0 |
| `write.batch.size`  | 64D | In order to improve the efficiency of writing, Flink write task will cache data in the buffer according to the write bucket until the memory reaches the threshold. When it reaches the threshold, the data buffer would be flushed out. | Write task | 0.9.0 |
| `write.log_block.size` | 128 | Hudi's log writer will not flush data immediately after receiving data. The writer will flush data to disk in the unit of `LogBlock`. Before `LogBlock` reaches a threshold, records will be buffered in the writer in form of serialized bytes. | Write task | 0.9.0 |
| `write.merge.max_memory` | 100 | If the write type is `COPY_ON_WRITE`, Hudi will merge the incremental data and base file data. The incremental data will be cached and spilled to disk. This threshold controls the max heap size that can be used. | Write task, compaction task | 0.9.0| 
| `compaction.max_memory` | 100 | This is the same concept as `write.merge.max_memory`, but occurs during compaction. If there is online compaction, it can be turned up when resources are sufficient, such as setting it to `1024MB` | Compaction task | 0.9.0 |

**Parallelism​**

| Property Name | Default  | Description | Scope | Since Version  |
|----------------|--------|----------|---------------|--------------------------------------
| `write.tasks` | Global default parallelism | The parallelism of writer tasks. Each write task writes 1 to N buckets in a sequence. | Write task | 0.9.0 |
| `write.bucket_assign.tasks` |  Global default parallelism| The parallelism of the bucket assigner operators. | Bucket assign task | 0.9.0 |
| `write.bucket_assign.tasks`  | Global default parallelism | The parallelism of the bucket assigner operators. | Bucket assign task | 0.9.0 |
| `write.index_boostrap.tasks` | Same as `write.tasks`| The parallelism of index bootstrap. Increasing parallelism can speed up the efficiency of the bootstrap stage. The bootstrap stage will block checkpointing. Therefore, it is necessary to set more checkpoint failure tolerance times. | Bootstrap task | 0.9.0 |
| `read.tasks` | Global default parallelism | The parallelism of read operators (batch and stream).| Read task | 0.9.0 |
| `compaction.tasks` | Global default parallelism | The parallelism of online compaction. The online compaction will occupy the resources of the write task. It is recommended to use [offline compaction](https://hudi.apache.org/docs/compaction/#flink-offline-compaction).| Compaction task | 0.9.0 |



**Compaction​**

Online asynchronous compaction is enabled by default. However, we suggest keeping the behavior for small data set jobs to avoid the additional complexity of maintaining the separate compaction job.
We suggest using offline compaction for a job that has high throughput. Moving to offline compaction where you don’t impact the writing job can make the writing pipeline more stable: the compaction task will not consume additional resources that the writing task needs. 

To enable offline compaction, you must first disable online compaction. These are the configurations to turn off online compaction:
- `compaction.async.enabled = false`
- `compaction.schedule.enable = true`  
From there, you can then execute an offline/async compaction.


| Property Name | Default  | Description | Scope | Since Version  |
|----------------|--------|----------|---------------|--------------------------------------
| `compaction.schedule.enabled` | true | If you want to generate a compaction plan periodically, we recommend to turn it on, even if the `compaction.async.enabled = false` | Job level | 0.9.0 |
| `compaction.async.enabled` | true | This is an  async compaction, that's enabled by default for MOR. If you want to turn off online compaction, you can disable this option. | Job level | 0.9.0 |
| `compaction.trigger.strategy` | num_commits | This is a strategy to trigger compaction. The options are `num_commits`: trigger compaction when reach N delta commits; `time_elapsed`: trigger compaction when time elapsed > N seconds since last compaction; `num_and_time`: trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; `num_or_time`: trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied.| Job level | 0.9.0 |
| `compaction.delta_commits` | 5 | This is the max delta commits needed to trigger compaction.| Job level | 0.9.0 |
| `compaction.delta_seconds` | 3600 |This is the max delta seconds time needed to trigger compaction. | Job level | 0.9.0 |
| `compaction.max_memory` | 100 | This is the max memory in MB for compaction spillable map, default 100MB. If your have sufficient resources, wse recommend to adjust the config to 1024MB.| Compaction task | 0.9.0 |
| `compaction.target_io` | 512000 |This is the target IO per compaction (both read and write), default 500GB. | Single compaction plan | 0.9.0 |



### Memory Optimization
In general, compaction is memory-intensive because there are many compute-heavy resources being used for reading and writing data into files. For example, in a Merge On Read (MOR) table, a separate compaction pipeline is needed and requires additional memory resources for the payloads to read and merge the log files into a new Parquet file. It’s important to ensure you have enough memory resources to avoid the OOM exceptions. You can learn more about MOR tables in this [blog](https://www.onehouse.ai/blog/comparing-apache-hudis-mor-and-cow-tables-use-cases-from-uber-and-shopee). 

**MOR​**
- [Set the Flink state backend to rocksdb](https://hudi.apache.org/docs/flink_configuration#checkpoint) (the default in memory state backend is very memory intensive).
- If there is enough memory, compaction.max_memory can be set larger (100MB by default, and can be adjust to 1024MB).
- Pay attention to the memory allocated to each write task by taskManager to ensure that each write task can be allocated to the desired memory size write.task.max.size. For example, the taskManager has 4GB of memory running two streamWriteFunction, so each write task can be allocated with 2GB memory. Please reserve some buffers because the network buffer and other types of tasks on taskManager (such as bucketAssignFunction) will also consume memory.
- Pay attention to the memory changes of compaction. The `compaction.max_memory` controls the maximum memory that each task can be used when compaction tasks read logs. The`compaction.tasks` controls the parallelism of compaction tasks.

**COW​**
- [Set the Flink state backend to rocksdb](https://hudi.apache.org/docs/flink_configuration#checkpoint) (the default in memory state backend is very memory intensive).
- Increase both `write.task.max.size` and `write.merge.max_memory` (1024MB and 100MB by default, adjust to 2014MB and 1024MB).
- Pay attention to the memory allocated to each write task by taskManager to ensure that each write task can be allocated to the desired memory size `write.task.max.size`. For example, **taskManager** has 4GB of memory running two write tasks, so each write task can be allocated with 2GB memory. Please reserve some buffers because the network buffer and other types of tasks on taskManager (such as the **BucketAssignFunction**) will also consume memory.

## Write Rate Limit​
Suppose you have an upstream data source consisting of historical and fresh data. You use Kafka to send the data from the upstream data source to downstream consumers; the downstream consumer is Flink. The reading throughput for Flink will be high because Kafka will just automatically dump all the data. However, this will cause Flink to block the data flushing and cause the checkpoint to time out. If the checkpoint is timed out, the transaction is aborted or failed.


We can enable the `write.rate.limit` on the Flink-Hudi sink to ensure Kafka doesn’t drown data into Flink and cause the transactions to be aborted or failed. The rate limit is a tactic for active backpressure to adjust the high throughput for streaming ingestion. By enabling the `write.rate.limit`, you’ll improve the write performance and write pipeline stability from Kafka -> Flink -> Hudi. 

| Property Name | Default  | Description | Scope | Since Version  |
|----------------|--------|----------|---------------|--------------------------------------
| `write.rate.limit` | 0 | This configs enables the rate limit of writers; the value 0 means no rate limit is set | Write task | 0.10.0 | 