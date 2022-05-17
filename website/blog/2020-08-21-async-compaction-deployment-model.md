---
title: "Async Compaction Deployment Models"
excerpt: "Mechanisms for executing compaction jobs in Hudi asynchronously"
author: vbalaji
category: blog
---

We will look at different deployment models for executing compactions asynchronously.
<!--truncate-->
## Compaction

For Merge-On-Read table, data is stored using a combination of columnar (e.g parquet) + row based (e.g avro) file formats. 
Updates are logged to delta files & later compacted to produce new versions of columnar files synchronously or 
asynchronously. One of th main motivations behind Merge-On-Read is to reduce data latency when ingesting records.
Hence, it makes sense to run compaction asynchronously without blocking ingestion.


## Async Compaction

Async Compaction is performed in 2 steps:

1. ***Compaction Scheduling***: This is done by the ingestion job. In this step, Hudi scans the partitions and selects **file 
slices** to be compacted. A compaction plan is finally written to Hudi timeline.
1. ***Compaction Execution***: A separate process reads the compaction plan and performs compaction of file slices.

  
## Deployment Models

There are few ways by which we can execute compactions asynchronously. 

### Spark Structured Streaming

With 0.6.0, we now have support for running async compactions in Spark 
Structured Streaming jobs. Compactions are scheduled and executed asynchronously inside the 
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

### Hudi CLI
Hudi CLI is yet another way to execute specific compactions asynchronously. Here is an example 

```properties
hudi:trips->compaction run --tableName <table_name> --parallelism <parallelism> --compactionInstant <InstantTime>
...
```

### Hudi Compactor Script
Hudi provides a standalone tool to also execute specific compactions asynchronously. Here is an example

```properties
spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.6.0 \
--class org.apache.hudi.utilities.HoodieCompactor \
--base-path <base_path> \
--table-name <table_name> \
--instant-time <compaction_instant> \
--schema-file <schema_file>
```
