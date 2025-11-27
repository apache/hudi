---
title: Flink Tuning Guide
toc: true
---

## Global Configurations
When using Flink, you can set some global configurations in `$FLINK_HOME/conf/flink-conf.yaml`

### Parallelism

|  Option Name  | Default | Type | Description |
|  -----------  | -------  | ------- | ------- |
| `taskmanager.numberOfTaskSlots` | `1` | `Integer` | The number of parallel operator or user function instances that a single TaskManager can run. We recommend setting this value > 4, and the actual value needs to be set according to the amount of data |
| `parallelism.default` | `1` | `Integer` | The default parallelism used when no parallelism is specified anywhere (default: 1). For example, If the value of [`write.bucket_assign.tasks`](#parallelism-1) is not set, this value will be used |

### Memory

|  Option Name  | Default | Type | Description |
|  -----------  | -------  | ------- | ------- |
| `jobmanager.memory.process.size` | `(none)` | `MemorySize` | Total Process Memory size for the JobManager. This includes all the memory that a JobManager JVM process consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead |
| `taskmanager.memory.task.heap.size` | `(none)` | `MemorySize` | Task Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved for write cache |
| `taskmanager.memory.managed.size`  |  `(none)`  | `MemorySize` | Managed Memory size for TaskExecutors. This is the size of off-heap memory managed by the memory manager, reserved for sorting and RocksDB state backend. If you choose RocksDB as the state backend, you need to set this memory |

### Checkpoint

|  Option Name  | Default | Type | Description |
|  -----------  | -------  | ------- | ------- |
| `execution.checkpointing.interval` | `(none)` | `Duration` | Setting this value as `execution.checkpointing.interval = 150000ms`, 150000ms = 2.5min. Configuring this parameter is equivalent to enabling the checkpoint |
| `state.backend` | `(none)` | `String` | The state backend to be used to store state. We recommend setting store state as `rocksdb` : `state.backend: rocksdb`  |
| `state.backend.rocksdb.localdir` | `(none)` | `String` | The local directory (on the TaskManager) where RocksDB puts its files |
| `state.checkpoints.dir` | `(none)` | `String` | The default directory used for storing the data files and meta data of checkpoints in a Flink supported filesystem. The storage path must be accessible from all participating processes/nodes(i.e. all TaskManagers and JobManagers), like hdfs and oss path |
| `state.backend.incremental`  |  `false`  | `Boolean` | Option whether the state backend should create incremental checkpoints, if possible. For an incremental checkpoint, only a diff from the previous checkpoint is stored, rather than the complete checkpoint state. If store state is setting as `rocksdb`, recommending to turn on |

## Table Options

Flink SQL jobs can be configured through options in the `WITH` clause.
The actual datasource level configs are listed below.

### Memory

:::note
When optimizing memory, we need to pay attention to the memory configuration
and the number of taskManagers, parallelism of write tasks (write.tasks : 4) first. After confirm each write task to be
allocated with enough memory, we can try to set these memory options.
:::

|  Option Name  | Description | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.task.max.size` | Maximum memory in MB for a write task, when the threshold hits, it flushes the max size data bucket to avoid OOM. Default `1024MB` | `1024D` | The memory reserved for write buffer is `write.task.max.size` - `compaction.max_memory`. When total buffer of write tasks reach the threshold, the largest buffer in the memory will be flushed |
| `write.batch.size`  | In order to improve the efficiency of writing, Flink write task will cache data in buffer according to the write bucket until the memory reaches the threshold. When reached threshold, the data buffer would be flushed out. Default `64MB` | `64D` |  Recommend to use the default settings  |
| `write.log_block.size` | The log writer of Hudi will not flush the data immediately after receiving data. The writer flush data to the disk in the unit of `LogBlock`. Before `LogBlock` reached threshold, records will be buffered in the writer in form of serialized bytes. Default `128MB`  | `128` |  Recommend to use the default settings  |
| `write.merge.max_memory` | If write type is `COPY_ON_WRITE`, Hudi will merge the incremental data and base file data. The incremental data will be cached and spilled to disk. this threshold controls the max heap size that can be used. Default `100MB`  | `100` | Recommend to use the default settings |
| `compaction.max_memory` | Same as `write.merge.max_memory`, but occurs during compaction. Default `100MB` | `100` | If it is online compaction, it can be turned up when resources are sufficient, such as setting as `1024MB` |

### Parallelism

|  Option Name  | Description | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.tasks` |  The parallelism of writer tasks. Each write task writes 1 to `N` buckets in sequence. Default `4` | `4` | Increases the parallelism has no effect on the number of small files |
| `write.bucket_assign.tasks`  |  The parallelism of bucket assigner operators. No default value, using Flink `parallelism.default`  | [`parallelism.default`](#parallelism) |  Increases the parallelism also increases the number of buckets, thus the number of small files (small buckets)  |
| `write.index_boostrap.tasks` |  The parallelism of index bootstrap. Increasing parallelism can speed up the efficiency of the bootstrap stage. The bootstrap stage will block checkpointing. Therefore, it is necessary to set more checkpoint failure tolerance times. Default using Flink `parallelism.default` | [`parallelism.default`](#parallelism) | It only take effect when `index.bootsrap.enabled` is `true` |
| `read.tasks` | The parallelism of read operators (batch and stream). Default `4`  | `4` |  |
| `compaction.tasks` | The parallelism of online compaction. Default `4` | `4` | `Online compaction` will occupy the resources of the write task. It is recommended to use [`offline compaction`](compaction/#flink-offline-compaction) |

### Compaction

:::note
These are options only for `online compaction`.
:::

:::note
Turn off online compaction by setting `compaction.async.enabled` = `false`, but we still recommend turning on `compaction.schedule.enable` for the writing job. You can then execute the compaction plan by [`offline compaction`](#offline-compaction).
:::

|  Option Name  | Description | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `compaction.schedule.enabled` | Whether to generate compaction plan periodically | `true` | Recommend to turn it on, even if `compaction.async.enabled` = `false` |
| `compaction.async.enabled`  |  Async Compaction, enabled by default for MOR | `true` | Turn off `online compaction` by turning off this option |
| `compaction.trigger.strategy`  | Strategy to trigger compaction | `num_commits` | Options are `num_commits`: trigger compaction when reach N delta commits; `time_elapsed`: trigger compaction when time elapsed > N seconds since last compaction; `num_and_time`: trigger compaction when both `NUM_COMMITS` and `TIME_ELAPSED` are satisfied; `num_or_time`: trigger compaction when `NUM_COMMITS` or `TIME_ELAPSED` is satisfied. |
| `compaction.delta_commits` | Max delta commits needed to trigger compaction, default `5` commits | `5` | -- |
| `compaction.delta_seconds`  |  Max delta seconds time needed to trigger compaction, default `1` hour | `3600` | -- |
| `compaction.max_memory` | Max memory in MB for compaction spillable map, default `100MB` | `100` | If your have sufficient resources, recommend to adjust to `1024MB` |
| `compaction.target_io`  |  Target IO per compaction (both read and write), default `500GB`| `512000` | -- |

## Memory Optimization

### MOR

1. [Setting Flink state backend to `rocksdb`](#checkpoint) (the default `in memory` state backend is very memory intensive).
2. If there is enough memory, `compaction.max_memory` can be set larger (`100MB` by default, and can be adjust to `1024MB`).
3. Pay attention to the memory allocated to each write task by taskManager to ensure that each write task can be allocated to the
   desired memory size `write.task.max.size`. For example, taskManager has `4GB` of memory running two streamWriteFunction, so each write task
   can be allocated with `2GB` memory. Please reserve some buffers because the network buffer and other types of tasks on taskManager (such as bucketAssignFunction) will also consume memory.
4. Pay attention to the memory changes of compaction. `compaction.max_memory` controls the maximum memory that each task can be used when compaction tasks read
   logs. `compaction.tasks` controls the parallelism of compaction tasks.

### COW

1. [Setting Flink state backend to `rocksdb`](#checkpoint) (the default `in memory` state backend is very memory intensive).
2. Increase both `write.task.max.size` and `write.merge.max_memory` (`1024MB` and `100MB` by default, adjust to `2014MB` and `1024MB`).
3. Pay attention to the memory allocated to each write task by taskManager to ensure that each write task can be allocated to the
   desired memory size `write.task.max.size`. For example, taskManager has `4GB` of memory running two write tasks, so each write task
   can be allocated with `2GB` memory. Please reserve some buffers because the network buffer and other types of tasks on taskManager (such as `BucketAssignFunction`) will also consume memory.


## Write Rate Limit

In the existing data synchronization, `snapshot data` and `incremental data` are send to kafka first, and then streaming write
to Hudi by Flink. Because the direct consumption of `snapshot data` will lead to problems such as high throughput and serious
disorder (writing partition randomly), which will lead to write performance degradation and throughput glitches. At this time,
the `write.rate.limit` option can be turned on to ensure smooth writing.

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.rate.limit` | `false` | `0` | Turn off by default |
