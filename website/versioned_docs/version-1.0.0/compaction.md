---
title: Compaction
summary: "In this page, we describe async compaction in Hudi."
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
last_modified_at:
---
## Background
Compaction is a table service employed by Hudi specifically in Merge On Read(MOR) tables to merge updates from row-based log 
files to the corresponding columnar-based base file periodically to produce a new version of the base file. Compaction is 
not applicable to Copy On Write(COW) tables and only applies to MOR tables. 

### Why MOR tables need compaction?
To understand the significance of compaction in MOR tables, it is helpful to understand the MOR table layout first. In Hudi, 
data is organized in terms of [file groups](storage_layouts). Each file group in a MOR table 
consists of a base file and one or more log files. Typically, during writes, inserts are stored in the base file, and updates 
are appended to log files.

![mor_table_file_layout](/assets/images/hudi_mor_file_layout.jpg)
_Figure: MOR  table file layout showing different file groups with base data file and log files_

During the compaction process, updates from the log files are merged with the base file to form a new version of the 
base file as shown below. Since MOR is designed to be write-optimized, on new writes, after index tagging is complete, 
Hudi appends the records pertaining to each file groups as log blocks in log files. There is no synchronous merge 
happening during write, resulting in a lower write amplification and better write latency. In contrast, on new writes to a 
COW table, Hudi combines the new writes with the older base file to produce a new version of the base file resulting in 
a higher write amplification and higher write latencies.

![mor_table_file_layout](/assets/images/hudi_mor_file_layout_post_compaction.jpg)
_Figure: Compaction on a given file group_

While serving the read query(snapshot read), for each file group, records in base file and all its corresponding log 
files are merged together and served. And hence the read latency for MOR snapshot query might be higher compared to 
COW table since there is no merge involved in case of COW at read time. Compaction takes care of merging the updates from
log files with the base file at regular intervals to bound the growth of log files and to ensure the read latencies do not
spike up.

## Compaction Architecture
There are two steps to compaction. 
- ***Compaction Scheduling***: In this step, Hudi scans the partitions and selects file slices to be compacted. A compaction 
  plan is finally written to Hudi timeline.
- ***Compaction Execution***: In this step the compaction plan is read and file slices are compacted.

### Strategies in Compaction Scheduling
There are two strategies involved in scheduling the compaction:
- Trigger Strategy: Determines how often to trigger scheduling of the compaction. 
- Compaction Strategy: Determines which file groups to compact.

Hudi provides various options for both these strategies as discussed below.

#### Trigger Strategies

| Config Name                                        | Default                 | Description                                                                                                                                                 |
|----------------------------------------------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.compact.inline.trigger.strategy             | NUM_COMMITS (Optional)  | org.apache.hudi.table.action.compact.CompactionTriggerStrategy: Controls when compaction is scheduled.<br />`Config Param: INLINE_COMPACT_TRIGGER_STRATEGY` <br/>
<ul><li>`NUM_COMMITS`: triggers compaction when there are at least N delta commits after last completed compaction.</li><li>`NUM_COMMITS_AFTER_LAST_REQUEST`: triggers compaction when there are at least N delta commits after last completed or requested compaction.</li><li>`TIME_ELAPSED`: triggers compaction after N seconds since last compaction.</li><li>`NUM_AND_TIME`: triggers compaction when both there are at least N delta commits and N seconds elapsed (both must be satisfied) after last completed compaction.</li><li>`NUM_OR_TIME`: triggers compaction when both there are at least N delta commits or N seconds elapsed (either condition is satisfied) after last completed compaction.</li></ul>|

#### Compaction Strategies
| Config Name                                        | Default                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.compaction.strategy                         | org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy (Optional) | Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data. <br /><br />`Config Param: COMPACTION_STRATEGY` |

Available Strategies (Provide the full package name when using the strategy): <ul><li>`LogFileNumBasedCompactionStrategy`: 
orders the compactions based on the total log files count, filters the file group with log files count greater than the 
threshold and limits the compactions within a configured IO bound.</li><li>`LogFileSizeBasedCompactionStrategy`: orders 
the compactions based on the total log files size, filters the file group which log files size is greater than the 
threshold and limits the compactions within a configured IO bound.</li><li>`BoundedIOCompactionStrategy`: CompactionStrategy
which looks at total IO to be done for the compaction (read + write) and limits the list of compactions to be under a 
configured limit on the IO.</li><li>`BoundedPartitionAwareCompactionStrategy`:This strategy ensures that the last N partitions 
are picked up even if there are later partitions created for the table. lastNPartitions is defined as the N partitions before 
the currentDate. currentDay = 2018/01/01 The table has partitions for 2018/02/02 and 2018/03/03 beyond the currentDay This 
strategy will pick up the following partitions for compaction : (2018/01/01, allPartitionsInRange[(2018/01/01 - lastNPartitions) 
to 2018/01/01), 2018/02/02, 2018/03/03)</li><li>`DayBasedCompactionStrategy`:This strategy orders compactions in reverse 
order of creation of Hive Partitions. It helps to compact data in latest partitions first and then older capped at the 
Total_IO allowed.</li><li>`UnBoundedCompactionStrategy`: UnBoundedCompactionStrategy will not change ordering or filter 
any compaction. It is a pass-through and will compact all the base files which has a log file. This usually means 
no-intelligence on compaction.</li><li>`UnBoundedPartitionAwareCompactionStrategy`:UnBoundedPartitionAwareCompactionStrategy is a custom UnBounded Strategy. This will filter all the partitions that
are eligible to be compacted by a \{@link BoundedPartitionAwareCompactionStrategy} and return the result. This is done
so that a long running UnBoundedPartitionAwareCompactionStrategy does not step over partitions in a shorter running
BoundedPartitionAwareCompactionStrategy. Essentially, this is an inverse of the partitions chosen in
BoundedPartitionAwareCompactionStrategy</li></ul>

:::note
Please refer to [advanced configs](https://hudi.apache.org/docs/next/configurations#Compaction-Configs) for more details.
:::

## Ways to trigger Compaction

### Inline
By default, compaction is run asynchronously.

If latency of ingesting records is important for you, you are most likely using Merge-On-Read tables.
Merge-On-Read tables store data using a combination of columnar (e.g parquet) + row based (e.g avro) file formats.
Updates are logged to delta files & later compacted to produce new versions of columnar files.
To improve ingestion latency, Async Compaction is the default configuration.

If immediate read performance of a new commit is important for you, or you want simplicity of not managing separate compaction jobs,
you may want synchronous inline compaction, which means that as a commit is written it is also compacted by the same job.

For this deployment mode, please use `hoodie.compact.inline = true` for Spark Datasource and Spark SQL writers. For
HoodieStreamer sync once mode inline compaction can be achieved by passing the flag `--disable-compaction` (Meaning to
disable async compaction). Further in HoodieStreamer when both 
ingestion and compaction is running in the same spark context, you can use resource allocation configuration
in Hudi Streamer CLI such as (`--delta-sync-scheduling-weight`,
`--compact-scheduling-weight`, `--delta-sync-scheduling-minshare`, and `--compact-scheduling-minshare`)
to control executor allocation between ingestion and compaction.


### Async & Offline Compaction models

There are a couple of ways here to trigger compaction .

#### Async execution within the same process
In streaming ingestion write models like HoodieStreamer 
continuous mode, Flink and Spark Streaming, async compaction is enabled by default and runs alongside without blocking 
regular ingestion. 

##### Spark Structured Streaming

Compactions are scheduled and executed asynchronously inside the
streaming job.Here is an example snippet in java

```properties
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.ProcessingTime;


 DataStreamWriter<Row> writer = streamingInput.writeStream().format("org.apache.hudi")
        .option("hoodie.datasource.write.operation", operationType)
        .option("hoodie.datasource.write.table.type", tableType)
        .option("hoodie.datasource.write.recordkey.field", "_row_key")
        .option("hoodie.datasource.write.partitionpath.field", "partition")
        .option("hoodie.datasource.write.precombine.field"(), "timestamp")
        .option("hoodie.compact.inline.max.delta.commits", "10")
        .option("hoodie.datasource.compaction.async.enable", "true")
        .option("hoodie.table.name", tableName).option("checkpointLocation", checkpointLocation)
        .outputMode(OutputMode.Append());
 writer.trigger(new ProcessingTime(30000)).start(tablePath);
```

##### Hudi Streamer Continuous Mode
Hudi Streamer provides continuous ingestion mode where a single long running spark application  
ingests data to Hudi table continuously from upstream sources. In this mode, Hudi supports managing asynchronous
compactions. Here is an example snippet for running in continuous mode with async compactions

```properties
spark-submit --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0 \
--class org.apache.hudi.utilities.streamer.HoodieStreamer \
--table-type MERGE_ON_READ \
--target-base-path <hudi_base_path> \
--target-table <hudi_table> \
--source-class org.apache.hudi.utilities.sources.JsonDFSSource \
--source-ordering-field ts \
--props /path/to/source.properties \
--continous
```

#### Scheduling and Execution by a separate process
For some use cases with long running table services, instead of having the regular writes block, users have the option to run 
both steps of the compaction ([scheduling and execution](#compaction-architecture)) offline in a separate process altogether. 
This allows for regular writers to not bother about these compaction steps and allows users to provide more resources for
the compaction job as needed. 

:::note
This model needs a lock provider configured for all jobs - the regular writer as well as the offline compaction job.
:::

#### Scheduling inline and executing async

In this model, it is possible for a Spark Datasource writer or a Flink job to just schedule the compaction inline ( that 
will serialize the compaction plan in the timeline but will not execute it). And then a separate utility like 
HudiCompactor or HoodieFlinkCompactor can take care of periodically executing the compaction plan.

:::note
This model may need a lock provider **if** metadata table is enabled.
:::

#### Hudi Compactor Utility
Hudi provides a standalone tool to execute specific compactions asynchronously. Below is an example and you can read more in the [deployment guide](/docs/cli#compactions)
The compactor utility allows to do scheduling and execution of compaction.

Example:
```properties
spark-submit --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0 \
--class org.apache.hudi.utilities.HoodieCompactor \
--base-path <base_path> \
--table-name <table_name> \
--schema-file <schema_file> \
--instant-time <compaction_instant>
```

Note, the `instant-time` parameter is now optional for the Hudi Compactor Utility. If using the utility without `--instant time`,
the spark-submit will execute the earliest scheduled compaction on the Hudi timeline.  

#### Hudi CLI
Hudi CLI is yet another way to execute specific compactions asynchronously. Here is an example and you can read more in the [deployment guide](/docs/cli#compactions)

Example:
```properties
hudi:trips->compaction run --tableName <table_name> --parallelism <parallelism> --compactionInstant <InstantTime>
...
```

#### Flink Offline Compaction
Offline compaction needs to submit the Flink task on the command line. The program entry is as follows: `hudi-flink-bundle_2.11-0.9.0-SNAPSHOT.jar` :
`org.apache.hudi.sink.compact.HoodieFlinkCompactor`

```bash
# Command line
./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.9.0.jar --path hdfs://xxx:9000/table
```

#### Options

|  Option Name | Default     | Description |
|  ----------- |-------------------------------------------------------------------------------------------------------------------------------| ------- |
| `--path` | `n/a **(Required)**` | The path where the target table is stored on Hudi                                                                             |
| `--compaction-max-memory` | `100` (Optional)     | The index map size of log data during compaction, 100 MB by default. If you have enough memory, you can turn up this parameter |
| `--schedule` | `false` (Optional)   | whether to execute the operation of scheduling compaction plan. When the write process is still writing， turning on this parameter have a risk of losing data. Therefore, it must be ensured that there are no write tasks currently writing data to this table when this parameter is turned on |
| `--seq` | `LIFO`  (Optional)   | The order in which compaction tasks are executed. Executing from the latest compaction plan by default. `LIFO`: executing from the latest plan. `FIFO`: executing from the oldest plan. |
| `--service` | `false`  (Optional)  | Whether to start a monitoring service that checks and schedules new compaction task in configured interval. |
| `--min-compaction-interval-seconds` | `600(s)` (optional)  | The checking interval for service mode, by default 10 minutes. |
