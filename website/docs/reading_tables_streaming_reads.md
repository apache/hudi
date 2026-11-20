---
title: Streaming Reads
keywords: [hudi, spark, flink, streaming, processing]
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Spark Streaming

Structured Streaming reads are based on Hudi's Incremental Query feature, therefore streaming read can return data for which
commits and base files were not yet removed by the cleaner. You can control commits retention time.

<Tabs
groupId="programming-language"
defaultValue="python"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
// reload data
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
  mode(Overwrite).
  save(basePath)

// read stream and output results to console
spark.readStream.
  format("hudi").
  load(basePath).
  writeStream.
  format("console").
  start()

// read stream to streaming df
val df = spark.readStream.
        format("hudi").
        load(basePath)

```
</TabItem>

<TabItem value="python">

```python
# pyspark
# reload data
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(
    dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.table.ordering.fields': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)

# read stream to streaming df
df = spark.readStream \
    .format("hudi") \
    .load(basePath)

# read stream and output results to console
spark.readStream \
    .format("hudi") \
    .load(basePath) \
    .writeStream \
    .format("console") \
    .start()

```
</TabItem>

</Tabs
>

:::info
Spark SQL can be used within ForeachBatch sink to do INSERT, UPDATE, DELETE and MERGE INTO.
Target table must exist before write.
:::

### Limit commits per micro-batch

Starting with Hudi 1.3.0, set `hoodie.datasource.read.streaming.max.commits.per.trigger`
to a positive integer to limit the completed write commits admitted in each
incremental micro-batch. The option is unset by default, which leaves reads
unbounded. Invalid values fail when loading the streaming source.

```python
source = (spark.readStream.format("hudi")
    .option("hoodie.datasource.read.streaming.max.commits.per.trigger", "5")
    .load(basePath))

query = (source.writeStream
    .format("console")
    .option("checkpointLocation", checkpointPath)
    .trigger(availableNow=True)
    .start())
```

The limit applies to Copy-on-Write and Merge-on-Read tables with both streaming
offset versions. It counts completed writes from active and archived timelines.
Clean and rollback actions do not consume the limit. With
`hoodie.datasource.read.incr.skip_compact=true`, MOR compactions are excluded before
counting. Clustering remains included, matching Spark incremental read support.

Processing-time triggers consume the backlog in capped batches.
`Trigger.AvailableNow` captures the end offset when the query starts and drains
that backlog in capped batches; writes arriving afterward wait for the next run.
Restarting with the same checkpoint resumes from the last processed offset.
`Trigger.Once` requests all available data and ignores the cap, following Spark's
contract. Use AvailableNow for capped triggered reads.

A commit can contain arbitrarily many rows or bytes, so this is not a memory,
row-count, or file-size bound. When offsets use completion time, commits sharing
the same completion timestamp cannot be split and may exceed the configured
count. Snapshot fallback and recovery reads are not divided into capped reads.
Retain timeline history and data files long enough for consumers to catch up;
this option does not restore cleaned files.

### Resuming a Spark V1 checkpoint after upgrading

Spark V1 streams in `BLOCK` mode use the last checkpointed instant's requested
time as the lower bound after upgrading to Hudi 1.3.0. Older readers configured
with `hoodie.write.table.version` 8 or later used that instant's completion time.
During the first catch-up after upgrading, the stream may re-emit records it
previously read from commits requested after the checkpointed instant's
requested time and at or before its completion time. This can occur with
asynchronous compaction or concurrent writers, including when the commit cap is
unset.

## Flink Streaming Read

Flink can continuously consume new commits from a Hudi table as a streaming source. Enable this by setting `read.streaming.enabled=true` and optionally a `read.start-commit`.

```sql
CREATE TABLE hudi_table (
  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = '${path}',
  'table.type' = 'MERGE_ON_READ',
  'read.streaming.enabled' = 'true',          -- enable streaming read
  'read.start-commit' = '20210316134557',      -- start from this instant (omit for latest)
  'read.streaming.check-interval' = '60'       -- poll interval in seconds
);

SELECT * FROM hudi_table;
```

### Source V2 for Streaming

As of Hudi 1.2.0, the [FLIP-27-based Source V2](ingestion_flink.md#flink-source-v2) is available as an opt-in for streaming reads. Source V2 participates in Flink's checkpoint protocol for finer-grained recovery and supports partition pruning:

```sql
WITH (
  'connector' = 'hudi',
  'path' = '${path}',
  'read.streaming.enabled' = 'true',
  'read.source-v2.enabled' = 'true'   -- enable FLIP-27 source (Hudi 1.2.0+)
)
```

:::warning
Savepoints taken with the legacy source are not compatible with Source V2. Start a fresh job when switching. See [Flink Source V2](ingestion_flink.md#flink-source-v2) for migration details.
:::

For a full list of Flink streaming read options (rate limiting, commits limit, CDC mode, etc.), see [Using Flink](ingestion_flink.md).

