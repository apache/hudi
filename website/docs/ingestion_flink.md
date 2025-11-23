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
|-----------------------------------|----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
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
|---------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------|
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
|---------------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
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
- For Copy‑on‑Write tables, new parquet files are written directly; auto-file sizing is not applied

### In-Memory Buffer Sort

For append-only workloads, Hudi supports in-memory buffer sorting to improve Parquet compression ratio. When enabled, data is sorted within the write buffer before being flushed to disk. This improves columnar file compression efficiency by grouping similar values together.

| Option Name                 | Required | Default | Remarks                                                                                                                       |
|-----------------------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------|
| `write.buffer.sort.enabled` | `false`  | `false` | Whether to enable buffer sort within append write function. Improves Parquet compression ratio by sorting data before writing |
| `write.buffer.sort.keys`    | `false`  | `N/A`   | Sort keys concatenated by comma (e.g., `col1,col2`). Required when `write.buffer.sort.enabled` is `true`                      |
| `write.buffer.size`         | `false`  | `1000`  | Buffer size in number of records. When buffer reaches this size, data is sorted and flushed to disk                           |

### Disable Meta Fields

For append-only workloads where Hudi metadata fields (e.g., `_hoodie_commit_time`, `_hoodie_record_key`) are not needed, you can disable them to reduce storage overhead. This is useful when integrating with external systems that don't require Hudi-specific metadata.

| Option Name                   | Required | Default | Remarks                                                                                                                                                        |
|-------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hoodie.populate.meta.fields` | `false`  | `true`  | Whether to populate Hudi meta fields. Set to `false` for append-only workloads to reduce storage overhead. Note: Some Hudi features may not work when disabled |

Hudi supports rich clustering strategies to optimize the files layout for `INSERT` mode:

### Inline Clustering

:::note
Only Copy‑on‑Write tables are supported.
:::

| Option Name            | Required | Default | Remarks                                                                                                                                                                              |
|------------------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `write.insert.cluster` | `false`  | `false` | Whether to merge small files while ingesting. For COW tables, enable this option to use the small‑file merging strategy (no deduplication for keys, but throughput will be affected) |

### Async Clustering

| Option Name                                      | Required | Default          | Remarks                                                                                                 |
|--------------------------------------------------|----------|------------------|---------------------------------------------------------------------------------------------------------|
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
|---------------------------------------------------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `clustering.plan.partition.filter.mode`                 | `false`  | `NONE`  | Valid options 1) `NONE`: no limit; 2) `RECENT_DAYS`: choose partitions that represent recent days; 3) `SELECTED_PARTITIONS`: specific partitions |
| `clustering.plan.strategy.daybased.lookback.partitions` | `false`  | `2`     | Number of partitions to look back; valid for `RECENT_DAYS` mode                                                                                  |
| `clustering.plan.strategy.cluster.begin.partition`      | `false`  | `N/A`   | Valid for `SELECTED_PARTITIONS` mode; specify the partition to begin with (inclusive)                                                            |
| `clustering.plan.strategy.cluster.end.partition`        | `false`  | `N/A`   | Valid for `SELECTED_PARTITIONS` mode; specify the partition to end with (inclusive)                                                              |
| `clustering.plan.strategy.partition.regex.pattern`      | `false`  | `N/A`   | The regex to filter the partitions                                                                                                               |
| `clustering.plan.strategy.partition.selected`           | `false`  | `N/A`   | Specific partitions, separated by commas                                                                                                         |

## Using Bucket Index

Hudi Flink writer supports two types of writer indexes:

- Flink state (default)
- Bucket index (3 variants: simple, partition-level, consistent hashing)

### Comparison

| Feature                 | Bucket Index                                                                                                                                                                                                                                                                | Flink State Index                                                                                                      |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| How It Works            | Uses a deterministic hash algorithm to shuffle records into buckets                                                                                                                                                                                                         | Uses the Flink state backend to store index data: mappings of record keys to their residing file group's file IDs      |
| Computing/Storage Cost  | No cost for state‑backend indexing                                                                                                                                                                                                                                          | Has computing and storage cost for maintaining state; can become a bottleneck when working with large Hudi tables      |
| Performance             | Better performance due to no state overhead                                                                                                                                                                                                                                 | Performance depends on state backend efficiency                                                                        |
| File Group Flexibility  | **Simple**: Fixed number of buckets (file groups) per partition, immutable once set<br/>**Partition-Level**: Different fixed buckets per partition via regex patterns (rescaling requires Spark procedure)<br/>**Consistent Hashing**: Auto-resizing buckets via clustering | Dynamically assigns records to file groups based on current table layout; no pre-configured limits on file group count |
| Cross‑Partition Changes | Cannot handle changes among partitions (unless input is a CDC stream)                                                                                                                                                                                                       | No limit on handling cross‑partition changes                                                                           |

:::note
Bucket index supports only the `UPSERT` write operation and cannot be used with the [append mode](#append-mode) in Flink.
:::

### Bucket Index Examples

#### Simple Bucket Index

Fixed number of buckets across all partitions:

```sql
CREATE TABLE orders_simple_bucket (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  order_date STRING,
  ts BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (order_date)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs:///warehouse/orders_simple',
  'table.type' = 'MERGE_ON_READ',
  
  -- Bucket Index Configuration
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.engine' = 'SIMPLE',
  'hoodie.bucket.index.hash.field' = 'order_id',
  'hoodie.bucket.index.num.buckets' = '16'  -- Fixed 16 buckets for ALL partitions
);

-- Insert data
INSERT INTO orders_simple_bucket VALUES
  (1, 100, 99.99, '2024-01-15', 1000),
  (2, 101, 49.99, '2024-02-20', 2000);
```

#### Partition-Level Bucket Index

Different bucket counts for different partitions based on regex patterns:

```sql
CREATE TABLE orders_partition_bucket (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  order_date STRING,
  ts BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (order_date)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs:///warehouse/orders_partition',
  'table.type' = 'MERGE_ON_READ',
  
  -- Bucket Index Configuration
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.engine' = 'SIMPLE',
  'hoodie.bucket.index.hash.field' = 'order_id',
  
  -- Partition-Level Configuration
  'hoodie.bucket.index.num.buckets' = '8',  -- Default for non-matching partitions
  'hoodie.bucket.index.partition.rule.type' = 'regex',
  -- Black Friday (11-24), Cyber Monday (11-27), Christmas (12-25) get 128 buckets
  -- All other dates get 8 buckets (default)
  'hoodie.bucket.index.partition.expressions' = '\\d{4}-(11-(24|27)|12-25),128'
);

-- Insert data - bucket count varies by partition
INSERT INTO orders_partition_bucket VALUES
  (1, 100, 999.99, '2024-11-24', 1000),  -- Black Friday: 128 buckets
  (2, 101, 499.99, '2024-11-27', 2000),  -- Cyber Monday: 128 buckets
  (3, 102, 299.99, '2024-12-25', 3000),  -- Christmas: 128 buckets
  (4, 103, 49.99, '2024-01-15', 4000);   -- Regular day: 8 buckets
```

:::note
For existing simple bucket index tables, use the Spark `partition_bucket_index_manager` procedure to upgrade to partition-level bucket index. After upgrade, Flink writers automatically load the expressions from table metadata.
:::

#### Consistent Hashing Bucket Index

Auto-expanding buckets via clustering (requires Spark for execution):

```sql
CREATE TABLE orders_consistent_hashing (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  order_date STRING,
  ts BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (order_date)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs:///warehouse/orders_consistent',
  'table.type' = 'MERGE_ON_READ',
  
  -- Consistent Hashing Bucket Index
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.engine' = 'CONSISTENT_HASHING',
  'hoodie.bucket.index.hash.field' = 'order_id',
  
  -- Initial and boundary configuration
  'hoodie.bucket.index.num.buckets' = '4',      -- Initial bucket count
  'hoodie.bucket.index.min.num.buckets' = '2',  -- Minimum allowed
  'hoodie.bucket.index.max.num.buckets' = '128', -- Maximum allowed
  
  -- Clustering configuration (required for auto-resizing)
  'clustering.schedule.enabled' = 'true',
  'clustering.delta_commits' = '5',  -- Schedule clustering every 5 commits
  'clustering.plan.strategy.class' = 'org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy',
  
  -- File size thresholds for bucket resizing
  'hoodie.clustering.plan.strategy.target.file.max.bytes' = '1073741824',  -- 1GB max
  'hoodie.clustering.plan.strategy.small.file.limit' = '314572800'  -- 300MB min
);

-- Insert data - buckets auto-adjust based on file sizes
INSERT INTO orders_consistent_hashing 
SELECT * FROM source_stream;
```

:::note
**Consistent hashing bucket index** automatically adjusts bucket counts via clustering. Flink can schedule clustering plans, but execution currently requires Spark. Start with `hoodie.bucket.index.num.buckets` and the system dynamically resizes based on file sizes within the min/max bounds.
:::

### Configuration Reference

| Option                                      | Applies To         | Default       | Description                                                               |
|---------------------------------------------|--------------------|---------------|---------------------------------------------------------------------------|
| `index.type`                                | All                | `FLINK_STATE` | Set to `BUCKET` to enable bucket index                                    |
| `hoodie.bucket.index.engine`                | All                | `SIMPLE`      | Engine type: `SIMPLE` or `CONSISTENT_HASHING`                             |
| `hoodie.bucket.index.hash.field`            | All                | Record key    | Fields to hash for bucketing; can be a subset of record key               |
| `hoodie.bucket.index.num.buckets`           | All                | `4`           | Default bucket count per partition (initial count for consistent hashing) |
| `hoodie.bucket.index.partition.expressions` | Partition-Level    | N/A           | Regex patterns and bucket counts: `pattern1,count1;pattern2,count2`       |
| `hoodie.bucket.index.partition.rule.type`   | Partition-Level    | `regex`       | Rule parser type                                                          |
| `hoodie.bucket.index.min.num.buckets`       | Consistent Hashing | N/A           | Minimum bucket count (prevents over-merging)                              |
| `hoodie.bucket.index.max.num.buckets`       | Consistent Hashing | N/A           | Maximum bucket count (prevents unlimited expansion)                       |
| `clustering.schedule.enabled`               | Consistent Hashing | `false`       | Must be `true` for auto-resizing                                          |
| `clustering.plan.strategy.class`            | Consistent Hashing | N/A           | Set to `FlinkConsistentBucketClusteringPlanStrategy`                      |

## Rate Limiting

Hudi provides rate limiting capabilities for both writes and streaming reads to control data flow and prevent performance degradation.

### Write Rate Limiting

In many scenarios, users publish both historical snapshot data and real‑time incremental updates to the same message queue, then consume from the earliest offset using Flink to ingest everything into Hudi. This backfill pattern can cause performance issues:

- **High burst throughput**: The entire historical dataset arrives at once, overwhelming the writer with a massive volume of records
- **Scattered writes across table partitions**: Historical records arrive scattered across many different table partitions (e.g., records from many different dates if the table is partitioned by date). This forces the writer to constantly switch between partitions, keeping many file handles open simultaneously and causing memory pressure, which degrades write performance and causes throughput instability

The `write.rate.limit` option helps smooth out the ingestion flow, preventing traffic jitter and improving stability during backfill operations.

### Streaming Read Rate Limiting

For Flink streaming reads, rate limiting helps avoid backpressure when processing large workloads. The `read.splits.limit` option controls the maximum number of input splits allowed to be read in each check interval. This feature is particularly useful when:

- Reading from tables with a large backlog of commits
- Preventing downstream operators from being overwhelmed
- Controlling resource consumption during catch-up scenarios

The average read rate can be calculated as: **`read.splits.limit` / `read.streaming.check-interval`** splits per second.

### Options

| Option Name                     | Required | Default             | Remarks                                                                                                                                                                          |
|---------------------------------|----------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `write.rate.limit`              | `false`  | `0`                 | Write record rate limit per second to prevent traffic jitter and improve stability. Default is 0 (no limit)                                                                      |
| `read.splits.limit`             | `false`  | `Integer.MAX_VALUE` | Maximum number of splits allowed to read in each instant check for streaming reads. Average read rate = `read.splits.limit`/`read.streaming.check-interval`. Default is no limit |
| `read.streaming.check-interval` | `false`  | `60`                | Check interval in seconds for streaming reads. Default is 60 seconds (1 minute)                                                                                                  |
