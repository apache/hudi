---
title: Streaming Writes
keywords: [hudi, spark, flink, streaming, processing]
last_modified_at: 2024-03-13T15:59:57-04:00
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

## Related Resources

<h3>Blogs</h3>
* [An Introduction to the Hudi and Flink Integration](https://www.onehouse.ai/blog/intro-to-hudi-and-flink)
* [Bulk Insert Sort Modes with Apache Hudi](https://medium.com/@simpsons/bulk-insert-sort-modes-with-apache-hudi-c781e77841bc)

