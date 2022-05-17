---
title: Compaction
summary: "In this page, we describe async compaction in Hudi."
toc: true
last_modified_at:
---

## Async Compaction
Compaction is executed asynchronously with Hudi by default. Async Compaction is performed in 2 steps:

1. ***Compaction Scheduling***: This is done by the ingestion job. In this step, Hudi scans the partitions and selects **file
   slices** to be compacted. A compaction plan is finally written to Hudi timeline.
1. ***Compaction Execution***: A separate process reads the compaction plan and performs compaction of file slices.

There are few ways by which we can execute compactions asynchronously.

### Spark Structured Streaming

Compactions are scheduled and executed asynchronously inside the
streaming job.  Async Compactions are enabled by default for structured streaming jobs
on Merge-On-Read table.

Here is an example snippet in java

```properties
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.ProcessingTime;


 DataStreamWriter<Row> writer = streamingInput.writeStream().format("org.apache.hudi")
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), operationType)
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, "10")
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE_OPT_KEY(), "true")
        .option(HoodieWriteConfig.TABLE_NAME, tableName).option("checkpointLocation", checkpointLocation)
        .outputMode(OutputMode.Append());
 writer.trigger(new ProcessingTime(30000)).start(tablePath);
```

### DeltaStreamer Continuous Mode
Hudi DeltaStreamer provides continuous ingestion mode where a single long running spark application  
ingests data to Hudi table continuously from upstream sources. In this mode, Hudi supports managing asynchronous
compactions. Here is an example snippet for running in continuous mode with async compactions

```properties
spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.6.0 \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--table-type MERGE_ON_READ \
--target-base-path <hudi_base_path> \
--target-table <hudi_table> \
--source-class org.apache.hudi.utilities.sources.JsonDFSSource \
--source-ordering-field ts \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--props /path/to/source.properties \
--continous
```

## Synchronous Compaction
By default, compaction is run asynchronously.

If latency of ingesting records is important for you, you are most likely using Merge-On-Read tables.
Merge-On-Read tables store data using a combination of columnar (e.g parquet) + row based (e.g avro) file formats.
Updates are logged to delta files & later compacted to produce new versions of columnar files. 
To improve ingestion latency, Async Compaction is the default configuration.

If immediate read performance of a new commit is important for you, or you want simplicity of not managing separate compaction jobs,
you may want Synchronous compaction, which means that as a commit is written it is also compacted by the same job.

Compaction is run synchronously by passing the flag "--disable-compaction" (Meaning to disable async compaction scheduling).
When both ingestion and compaction is running in the same spark context, you can use resource allocation configuration 
in DeltaStreamer CLI such as ("--delta-sync-scheduling-weight",
"--compact-scheduling-weight", ""--delta-sync-scheduling-minshare", and "--compact-scheduling-minshare")
to control executor allocation between ingestion and compaction.


## Offline Compaction

The compaction of the MERGE_ON_READ table is enabled by default. The trigger strategy is to perform compaction after completing
five commits. Because compaction consumes a lot of memory and is placed in the same pipeline with the write operation, it's easy to
interfere with the write operation when there is a large amount of data (> 100000 per second). As this time, it is more stable to execute
the compaction task by using offline compaction.

:::note
The execution of a compaction task includes two parts: schedule compaction plan and execute compaction plan. It's recommended that
the process of schedule compaction plan be triggered periodically by the write task, and the write parameter `compaction.schedule.enable`
is enabled by default.
:::

### Hudi Compactor Utility
Hudi provides a standalone tool to execute specific compactions asynchronously. Below is an example and you can read more in the [deployment guide](/docs/deployment#compactions)

Example:
```properties
spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.6.0 \
--class org.apache.hudi.utilities.HoodieCompactor \
--base-path <base_path> \
--table-name <table_name> \
--schema-file <schema_file> \
--instant-time <compaction_instant>
```

Note, the `instant-time` parameter is now optional for the Hudi Compactor Utility. If using the utility without `--instant time`,
the spark-submit will execute the earliest scheduled compaction on the Hudi timeline.

### Hudi CLI
Hudi CLI is yet another way to execute specific compactions asynchronously. Here is an example and you can read more in the [deployment guide](/docs/cli#compactions)

Example:
```properties
hudi:trips->compaction run --tableName <table_name> --parallelism <parallelism> --compactionInstant <InstantTime>
...
```

### Flink Offline Compaction
Offline compaction needs to submit the Flink task on the command line. The program entry is as follows: `hudi-flink-bundle_2.11-0.9.0-SNAPSHOT.jar` :
`org.apache.hudi.sink.compact.HoodieFlinkCompactor`

```bash
# Command line
./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.9.0.jar --path hdfs://xxx:9000/table
```

#### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `--path` | `frue` | `--` | The path where the target table is stored on Hudi |
| `--compaction-max-memory` | `false` | `100` | The index map size of log data during compaction, 100 MB by default. If you have enough memory, you can turn up this parameter |
| `--schedule` | `false` | `false` | whether to execute the operation of scheduling compaction plan. When the write process is still writing， turning on this parameter have a risk of losing data. Therefore, it must be ensured that there are no write tasks currently writing data to this table when this parameter is turned on |
| `--seq` | `false` | `LIFO` | The order in which compaction tasks are executed. Executing from the latest compaction plan by default. `LIFO`: executing from the latest plan. `FIFO`: executing from the oldest plan. |
