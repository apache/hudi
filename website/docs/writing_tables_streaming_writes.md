---
title: Streaming Writes
keywords: [hudi, spark, flink, streaming, processing]
last_modified_at: 2026-10-28T00:00:00+08:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Spark Streaming

You can write Hudi tables using spark's structured streaming.

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
// prepare to stream write to new table
import org.apache.spark.sql.streaming.Trigger

val streamingTableName = "hudi_trips_cow_streaming"
val baseStreamingPath = "file:///tmp/hudi_trips_cow_streaming"
val checkpointLocation = "file:///tmp/checkpoints/hudi_trips_cow_streaming"

// create streaming df
val df = spark.readStream.
        format("hudi").
        load(basePath)

// write stream to new hudi table
df.writeStream.format("hudi").
  options(getQuickstartWriteConfigs).
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", streamingTableName).
  outputMode("append").
  option("path", baseStreamingPath).
  option("checkpointLocation", checkpointLocation).
  trigger(Trigger.Once()).
  start()

```

</TabItem>
<TabItem value="python">

```python
# pyspark
# prepare to stream write to new table
streamingTableName = "hudi_trips_cow_streaming"
baseStreamingPath = "file:///tmp/hudi_trips_cow_streaming"
checkpointLocation = "file:///tmp/checkpoints/hudi_trips_cow_streaming"

hudi_streaming_options = {
    'hoodie.table.name': streamingTableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': streamingTableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.table.ordering.fields': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

# create streaming df
df = spark.readStream 
    .format("hudi") 
    .load(basePath)

# write stream to new hudi table
df.writeStream.format("hudi") 
    .options(**hudi_streaming_options) 
    .outputMode("append") 
    .option("path", baseStreamingPath) 
    .option("checkpointLocation", checkpointLocation) 
    .trigger(once=True) 
    .start()

```

</TabItem>

</Tabs
>

### Session settings and upgrade compatibility

In the upcoming release, Structured Streaming sinks honor the active session's
`spark.hoodie.*` settings. Previously, the streaming sink ignored these settings,
even though batch DataFrame writes supported them. Review ambient session configuration
when upgrading an existing `writeStream` application.

For example, this setting selects inserts for a subsequently started streaming query:

```python
spark.conf.set("spark.hoodie.datasource.write.operation", "insert")
```

Explicit writer options take precedence. To retain upsert behavior despite that session setting:

```python
query = (df.writeStream.format("hudi")
    .options(**hudi_streaming_options)
    .option("hoodie.datasource.write.operation", "upsert")
    .option("path", baseStreamingPath)
    .option("checkpointLocation", checkpointLocation)
    .start())
```

The precedence is explicit writer/storage options, then session `spark.hoodie.*`,
then Hudi defaults. The sink strips the `spark.` prefix before validation and writing.
Explicit options may use either prefix; if both forms are supplied explicitly,
the canonical `hoodie.*` option wins. Bare `hoodie.*` session settings are not forwarded.

Settings are captured when the sink is created. Changing the session afterward does not
change an existing query; restart it to capture new settings. The same captured options
control multi-writer validation, checkpoint deduplication, and micro-batch writes.
Table identity options must still agree with the persisted Hudi table. An incompatible
record key, ordering field, partition field, key generator, or record-merge setting
fails validation when opening an existing table, including during checkpoint replay.

For flow-based integrations such as the proposed Spark Declarative Pipelines integration
([RFC-112](https://github.com/apache/hudi/pull/19997)), supply operation and writer tuning
through flow-scoped `SET spark.hoodie.*`, and reserve table properties for table identity.
A `SET` remains active for subsequent flow definitions until overridden. Legacy table
storage options take precedence over session settings: the sink logs conflicting option
keys without logging their values. Remove conflicting behavior settings from the table's
storage options before expecting a flow's `SET` to control them.

## Related Resources

<h3>Blogs</h3>
* [An Introduction to the Hudi and Flink Integration](https://www.onehouse.ai/blog/intro-to-hudi-and-flink)
* [Bulk Insert Sort Modes with Apache Hudi](https://medium.com/@simpsons/bulk-insert-sort-modes-with-apache-hudi-c781e77841bc)


