---
title: Using Flink
keywords: [hudi, flink, streamer, ingestion]
last_modified_at: 2025-11-22T12:53:57+08:00
---

## CDC Ingestion

CDC (change data capture) keeps track of data changes evolving in a source system so a downstream process or system can act on those changes.
We recommend two ways for syncing CDC data into Hudi:

![slide1 title](/assets/images/cdc-2-hudi.png)

1. Use the Ververica [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) to directly connect to the database server and sync binlog data into Hudi.
   The advantage is that it does not rely on message queues, but the disadvantage is that it puts pressure on the database server.
2. Consume data from a message queue (e.g., Kafka) using the Flink CDC format. The advantage is that it is highly scalable,
   but the disadvantage is that it relies on message queues.

:::note
If the upstream data cannot guarantee ordering, you need to explicitly specify the `write.precombine.field` option.
:::

## Bulk Insert

For snapshot data import requirements, if the snapshot data comes from other data sources, use the `bulk_insert` mode to quickly
import the snapshot data into Hudi.

:::note
`bulk_insert` eliminates serialization and data merging. Data deduplication is skipped, so the user needs to guarantee data uniqueness.
:::

:::note
`bulk_insert` is more efficient in `batch execution mode`. By default, `batch execution mode` sorts the input records
by partition path and writes these records to Hudi, which can avoid write‑performance degradation caused by
frequent file‑handle switching.
:::

:::note
The parallelism of `bulk_insert` is specified by `write.tasks`. The parallelism affects the number of small files.
In theory, the parallelism of `bulk_insert` equals the number of buckets. (In particular, when each bucket writes to the maximum file size, it
rolls over to a new file handle.) Finally, the number of files ≥ [`write.bucket_assign.tasks`](configurations#writebucket_assigntasks).
:::

### Options

| Option Name                       | Required | Default  | Remarks                                                                                                                                                        |
| --------------------------------- | -------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `write.operation`                 | `true`   | `upsert` | Set to `bulk_insert` to enable this function                                                                                                                   |
| `write.tasks`                     | `false`  | `4`      | The parallelism of `bulk_insert`; the number of files ≥ [`write.bucket_assign.tasks`](configurations#writebucket_assigntasks)                                  |
| `write.bulk_insert.shuffle_input` | `false`  | `true`   | Whether to shuffle data by the input field before writing. Enabling this option reduces the number of small files but may introduce data‑skew risk             |
| `write.bulk_insert.sort_input`    | `false`  | `true`   | Whether to sort data by the input field before writing. Enabling this option reduces the number of small files when a write task writes to multiple partitions |
| `write.sort.memory`               | `false`  | `128`    | Available managed memory for the sort operator; default is 128 MB                                                                                              |

## Index Bootstrap

For importing both snapshot data and incremental data: if the snapshot data has already been inserted into Hudi via [bulk insert](#bulk-insert),
users can insert incremental data in real time and ensure the data is not duplicated by using the index bootstrap function.

:::note
If you find this process very time‑consuming, you can add resources to write in streaming mode while writing snapshot data,
then reduce the resources when writing incremental data (or enable the rate‑limit function).
:::

### Options

| Option Name               | Required | Default | Remarks                                                                                                                    |
| ------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| `index.bootstrap.enabled` | `true`   | `false` | When index bootstrap is enabled, the remaining records in the Hudi table are loaded into the Flink state at once           |
| `index.partition.regex`   | `false`  | `*`     | Optimization option. Set a regular expression to filter partitions. By default, all partitions are loaded into Flink state |

### How to use

1. Use `CREATE TABLE` to create a statement corresponding to the Hudi table. Note that `table.type` must be correct.
2. Set `index.bootstrap.enabled` = `true` to enable the index bootstrap function.
3. Set the Flink checkpoint failure tolerance in `flink-conf.yaml`: `execution.checkpointing.tolerable-failed-checkpoints = n` (depending on Flink checkpoint scheduling times).
4. Wait until the first checkpoint succeeds, indicating that the index bootstrap has completed.
5. After the index bootstrap completes, users can exit and save the savepoint (or directly use the externalized checkpoint).
6. Restart the job, setting `index.bootstrap.enable` to `false`.

:::note

1. Index bootstrap is blocking, so checkpoints cannot complete during index bootstrap.
2. Index bootstrap is triggered by the input data. Users need to ensure that there is at least one record in each partition.
3. Index bootstrap executes concurrently. Users can search logs for `finish loading the index under partition` and `Load record from file` to observe the index‑bootstrap progress.
4. The first successful checkpoint indicates that the index bootstrap has completed. There is no need to load the index again when recovering from the checkpoint.

:::

## Changelog Mode

Hudi can keep all the intermediate changes (I / -U / U / D) of messages, then consume them through stateful computing in Flink to build a near‑real‑time
data‑warehouse ETL pipeline (incremental computing). Hudi MOR tables store messages as rows, which supports the retention of all change logs (integration at the format level).
All changelog records can be consumed with the Flink streaming reader.

### Options

| Option Name         | Required | Default | Remarks                                                                                                                                                                                                 |
| ------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `changelog.enabled` | `false`  | `false` | It is turned off by default, to have the `upsert` semantics, only the merged messages are ensured to be kept, intermediate changes may be merged. Setting to true to support consumption of all changes |

:::note
Batch (snapshot) reads still merge all the intermediate changes, regardless of whether the format has stored the intermediate changelog messages.
:::

:::note
After setting `changelog.enable` to `true`, the retention of changelog records is best‑effort only: the asynchronous compaction task will merge the changelog records into one record, so if the
stream source does not consume in a timely manner, only the merged record for each key can be read after compaction. The solution is to reserve buffer time for the reader by adjusting the compaction strategy, such as
the compaction options `compaction.delta_commits` and `compaction.delta_seconds`.
:::

## Append Mode

For `INSERT` mode write operations, the current workflow is:

- For Merge‑on‑Read tables, auto‑file sizing is enabled by default
- For Copy‑on‑Write tables, new parquet files are written directly; no small‑file handling is applied

Hudi supports rich clustering strategies to optimize the files layout for `INSERT` mode:

### Inline Clustering

:::note
Only Copy‑on‑Write tables are supported.
:::

| Option Name            | Required | Default | Remarks                                                                                                                                                                              |
| ---------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `write.insert.cluster` | `false`  | `false` | Whether to merge small files while ingesting. For COW tables, enable this option to use the small‑file merging strategy (no deduplication for keys, but throughput will be affected) |

### Async Clustering

| Option Name                                      | Required | Default          | Remarks                                                                                                 |
| ------------------------------------------------ | -------- | ---------------- | ------------------------------------------------------------------------------------------------------- |
| `clustering.schedule.enabled`                    | `false`  | `false`          | Whether to schedule the clustering plan during the write process; default is false                      |
| `clustering.delta_commits`                       | `false`  | `4`              | Delta commits for scheduling the clustering plan; only valid when `clustering.schedule.enabled` is true |
| `clustering.async.enabled`                       | `false`  | `false`          | Whether to execute the clustering plan asynchronously; default is false                                 |
| `clustering.tasks`                               | `false`  | `4`              | Parallelism of the clustering tasks                                                                     |
| `clustering.plan.strategy.target.file.max.bytes` | `false`  | `1024*1024*1024` | The target file size for the clustering group; default is 1 GB                                          |
| `clustering.plan.strategy.small.file.limit`      | `false`  | `600`            | Files smaller than the threshold (in MB) are candidates for clustering                                  |
| `clustering.plan.strategy.sort.columns`          | `false`  | `N/A`            | The columns to sort by when clustering                                                                  |

### Clustering Plan Strategy

Custom clustering strategy is supported.

| Option Name                                             | Required | Default | Remarks                                                                                                                                          |
| ------------------------------------------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `clustering.plan.partition.filter.mode`                 | `false`  | `NONE`  | Valid options 1) `NONE`: no limit; 2) `RECENT_DAYS`: choose partitions that represent recent days; 3) `SELECTED_PARTITIONS`: specific partitions |
| `clustering.plan.strategy.daybased.lookback.partitions` | `false`  | `2`     | Number of partitions to look back; valid for `RECENT_DAYS` mode                                                                                  |
| `clustering.plan.strategy.cluster.begin.partition`      | `false`  | `N/A`   | Valid for `SELECTED_PARTITIONS` mode; specify the partition to begin with (inclusive)                                                            |
| `clustering.plan.strategy.cluster.end.partition`        | `false`  | `N/A`   | Valid for `SELECTED_PARTITIONS` mode; specify the partition to end with (inclusive)                                                              |
| `clustering.plan.strategy.partition.regex.pattern`      | `false`  | `N/A`   | The regex to filter the partitions                                                                                                               |
| `clustering.plan.strategy.partition.selected`           | `false`  | `N/A`   | Specific partitions, separated by commas                                                                                                         |

## Use Bucket Index

Hudi Flink writer supports two types of writer indexes:

- Flink state (default)
- Bucket

### Comparison

| Feature                  | Bucket Index                                                          | Flink State Index                                                                                                 |
| ------------------------ | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| How It Works             | Uses a deterministic hash algorithm to shuffle records into buckets   | Uses the Flink state backend to store index data: mappings of record keys to their residing file group's file IDs |
| Computing/Storage Cost   | No cost for state‑backend indexing                                    | Has computing and storage cost for maintaining state; can become a bottleneck when working with large Hudi tables |
| Performance              | Better performance due to no state overhead                           | Performance depends on state backend efficiency                                                                   |
| Dynamic Bucket Expansion | Cannot expand buckets dynamically; immutable once set                 | Can expand buckets dynamically based on current file layout                                                       |
| Cross‑Partition Changes  | Cannot handle changes among partitions (unless input is a CDC stream) | No limit on handling cross‑partition changes                                                                      |

### Options

| Option Name                       | Required | Default       | Remarks                                                      |
| --------------------------------- | -------- | ------------- | ------------------------------------------------------------ |
| `index.type`                      | `false`  | `FLINK_STATE` | Set to `BUCKET` to use the bucket index                      |
| `hoodie.bucket.index.hash.field`  | `false`  | Record key    | Can be a subset of the record key; default is the record key |
| `hoodie.bucket.index.num.buckets` | `false`  | `4`           | The number of buckets per partition; immutable once set      |

## Rate Limiting

There are many use cases where users put the full‑history dataset onto the message queue together with real‑time incremental data. Then they consume the data from the queue into Hudi from the earliest offset using Flink. Consuming the full‑history dataset has problems:

* The instantaneous throughput is huge
* It has serious disorder (with random write partitions), which leads to degraded write performance and throughput glitches.

For such cases, the `write.rate.limit` option can be enabled to ensure smooth writing of the flow.### Options

| Option Name        | Required | Default | Remarks                                       |
| ------------------ | -------- | ------- | --------------------------------------------- |
| `write.rate.limit` | `false`  | `0`     | Rate limit disabled by default (0 = no limit) |
