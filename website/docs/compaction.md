---
title: Compaction
summary: "In this page, we describe async compaction in Hudi."
toc: true
last_modified_at:
---

Hudi has a table service called compaction, where files are compacted together to form a new file version. In Hudi, two different storage types are available, Copy-On-Write (COW) and Merge-On-Read (MOR). Each storage type uses distinct file types: COW uses base files comprised of Parquet files (columnar-base); MOR uses base and log files consisting of Parquet files (columnar-base) and Avro files (row-base), respectively. Compaction is a mandatory table service in Hudi that only applies to MOR tables, not COW tables. 

## WHY MOR TABLES NEED COMPACTION
In Hudi, data is organized in terms of [file groups](https://hudi.apache.org/docs/file_layouts/). Each file group in a MOR table consists of a base file and one or more log files. Typically during writes, updates are stored in log files, and inserts are stored in base files. During the compaction process, log files get compacted into a new base file version. Since Hudi writes updates directly to log files, MOR tables are write-optimized and have a lower write amplification than COW tables because no synchronous merge occurs during writes. In contrast, COW tables have a higher write amplification because Hudi applies each write to a new base file version, where a synchronous merge combines new writes with the older base files and forms a new base file version. 

![MOR table file layout](/assets/images/docs/compaction/mor-layout-2-02.png)
 Figure: Hudi’s MOR table file layouts showing diff file groups and its constituents.

 However, MOR tables may have higher read amplification for snapshot reads than COW tables because base files with their constituent log files are merged and served for each file group during query time. It’s essential to have compaction for MOR tables at regular intervals to bind the growth of log files and ensure the read latencies do not spike up enormously. However, in COW tables, no merge operation is needed during queries because all writes are in base files. As a result, COW tables have a lower read amplification.

![Compaction process](/assets/images/docs/compaction/mor-layout-1-01.png)
 Figure: Pictorial representation of Apache Hudi’s Compaction on a given file group. 

## Strategies for compaction 
To execute compaction, you need to configure two strategies: 
- schedule strategy 
- compaction strategy

Once you configure the compaction strategy, you can deploy the compaction process in two different ways:
- inline compaction
- asynchronous or offline compaction 

Below, we’ll go over all the different strategies with compaction and how you can schedule, create, execute and deploy it. 

### Schedule strategy
The scheduling strategy determines how often to schedule compaction. You schedule compaction based on the commit number or time elapsed. There are multiple trigger options available. For example, you can set a trigger strategy to occur after 5 commits or after 10 minutes. After compaction is triggered, Hudi prepares a compaction strategy to decide what to compact (the compaction strategy is described later in the document).

To set a trigger strategy that's suitable for your application, use the below configuration:

- `hoodie.compact.inline.trigger.strategy`: This configuration is used for both async and inline compaction. It controls how the compaction schedule is triggered. Triggering is based on time, the number of delta commits or a combination of both. The valid options are: `NUM_COMMITS`,`NUM_COMMITS_AFTER_LAST_REQUEST`,`TIME_ELAPSED`,`NUM_AND_TIME` and `NUM_OR_TIME`. The default value is: `NUM_COMMITS`.

Depending on those values for `hoodie.compact.inline.trigger.strategy`, you may need to set additional configurations. Below are the explanation of the values and additional configurations that need to be set: 
- `NUM_COMMITS`: You schedule the compaction based on the number of delta commits (N). Use `hoodie.compact.inline.max.delta.commits` to define the value for (N). 
- `NUM_COMMITS_AFTER_LAST_REQUEST`: You schedule the compaction based on the number of delta commits (N) after the last compaction. Again, you can use `hoodie.compact.inline.max.delta.commits` to define the value for the number of commits. 
- `TIME_ELAPSED`: You schedule the compaction based on the time elapsed. To set the elapsed time, you can use the configurations, `hoodie.compact.inline.max.delta.seconds`.
- `NUM_AND_TIME`: You schedule the compaction if the number of commits and time elapsed occurred. Use `hoodie.compact.inline.max.delta.commits` and `hoodie.compact.inline.max.delta.seconds` to define the value for (N) and set the elapsed time, respectively. 
- `NUM_OR_TIME`: You schedule the compaction if the number of delta commits occurred OR the time has elapsed. Use `hoodie.compact.inline.max.delta.commits` and `hoodie.compact.inline.max.delta.seconds` to define the value for (N) and set the elapsed time, respectively. 

The additional configurations based on the strategy values are below:

- `hoodie.compact.inline.max.delta.commits`: This is the number of delta commits after the last compaction before the new compaction is scheduled. This configuration takes effect only for the compaction triggering strategy based on the number of commits: `NUM_COMMITS`, `NUM_COMMITS_AFTER_LAST_REQUEST`, `NUM_AND_TIME` and `NUM_OR_TIME`.
- `hoodie.compact.inline.max.delta.seconds`: This is the number of elapsed seconds after the last compaction, before scheduling a new one. This configuration takes effect only for the compaction triggering strategy based on the elapsed time:  `TIME_ELAPSED`, `NUM_AND_TIME` and `NUM_OR_TIME`.

:::note 
Corner cases with performing compaction with indexes:
For most cases with bloom, global_bloom, simple and global_simple, at write time, log files will consist of updates, and base files will consist of inserts. However, updates can go to base files due to small file handling. In the case of hbase index and bucket index, inserts may go to log files. 
:::

### Compaction Strategy
The schedule strategy we just covered will only dictate when to schedule compaction. What exactly will get compacted, like which file groups, will be determined by the compaction strategy. Below is the configuration to set the compaction strategy:

`hoodie.compaction.strategy`: This compaction strategy decides which file groups are picked up for compaction during each compaction run. Hudi picks the log file with the most accumulated unmerged data by default. Depending on the values chosen, you may need to set additional configurations. Below are the explanation of the values and additional configurations that need to be set: 

- `LogFileSizeBasedCompactionStrategy`: This orders the compaction based on the total log file size. In this strategy, Hudi evaluates the whole table and retrieves all the file groups that do not have a pending compaction and have a log file size equal to or greater than the `hoodie.compaction.logfile.size.threshold`. The file groups that meet these requirements will be further trimmed by the IO bound. This IO-bound configuration has a 500GB default value, which you can override. Hudi accumulates all the file groups whose total IO required for compaction has a maximum target IO per the configuration set. The file groups that meet the target IO bound will only be added to the compaction plan, and a `.compaction.requested` instant is created in the `.hoodie` folder with all the plan details. For instance, if 100 file groups are eligible per the log file size threshold, but the IO required to compact 10 file groups amounts to the target IO config set, only 10 file groups will be considered for the compaction plan/schedule of interest. 

- `LogFileNumBasedCompactionStrategy`: This orders the compaction based on the total log file count. In this strategy, Hudi evaluates the whole table and retrieves all the file groups that do not have a pending compaction and have a log file number equal to or greater to the `hoodie.compaction.logfile.number.threshold`. The IO bound will further trim the file groups that meet these requirements. This IO-bound configuration has a 500GB default value, which you can override. Hudi accumulates all the file groups whose total IO required for compaction has a maximum target IO per the configuration set. The file groups that meet the target IO bound will only be added to the compaction plan, and a `.compaction.requested` instant is created in the `.hoodie` folder with all the plan details.  

- `BoundedIOCompactionStrategy`: This compaction strategy looks at all the table’s file groups, ignoring the file groups that are pending for compaction, and keeps accumulating the file group’s log files to either meet or be under the threshold set by the hoodie.compaction.target.io value. This IO-bound configuration has a 500GB default value, which you can override. Hudi accumulates all the file groups whose total IO required for compaction has a maximum target IO per the configuration set. The file groups that meet the target IO bound will only be added to the compaction plan, and a `.compaction.requested` instant is created in the `.hoodie` folder with all the plan details. 

- `DayBasedCompactionStrategy`: This denotes the number of latest partitions to compact during a compaction run. The default value is 10. This configuration is used for `BoundedPartitionAwareCompactionStrategy` and `UnBoundedPartitionAwareCompactionStrategy`.

- `BoundedPartitionAwareCompactionStrategy`: This is a partition-level compaction where the strategy only works when the data set has a date partition in the YYYY/MM/DD format. Hudi compacts the current partitions between hoodie.compaction.daybased.target.partitions value, starting with the most current partition first. For example, if `hoodie.compaction.daybased.target.partitions=3` and the current or latest partition on a Hudi table is 2023/03/15, Hudi will prep the partition from 2023/03/15, 2023/03/14 and 2023/03/13 for compaction. In this compaction strategy, the process is bounded between the latest partition available and the value set for the `hoodie.compaction.daybased.target.partitions`.

- `UnBoundedPartitionAwareCompactionStrategy`: This is a partition-level compaction where the strategy only works when the data set has a date partition in the YYYY/MM/DD format. This strategy keeps compacting all partitions before the latest partition is available via `hoodie.compaction.daybased.target.partitions`. For example, if `hoodie.compaction.daybased.target.partitions=3` and the current or latest partition on a Hudi table is 2023/03/15, Hudi will prep all the partitions from 2023/03/12 and earlier for compaction. In this compaction strategy, the process is unbounded starting from the n-th latest partition available, where n is the value set for the `hoodie.compaction.daybased.target.partitions`, till the earliest partition in the dataset.

- `UnBoundedCompactionStrategy`: This strategy compacts all base files with its respective log files. For example, if you use a schedule strategy, like `num_commit=5`, all file groups with updates between those 5 commits will be prepped for compaction. There are no further trimming or settings you need to take into consideration. 

The additional configurations based on the strategy values are below:

- `hoodie.compaction.target.io`: This is the number of MBs to spend during the compaction run for the `LogFileSizeBasedCompactionStrategy`, `LogFileNumBasedCompactionStrategy` and `BoundedIOCompactionStrategy`. This value helps bound ingestion latency while compaction runs. 

#### Example of how a compaction plan and strategy work in Hudi: 

Let’s say you set the schedule and compaction strategy to these values: 

```
hoodie.compact.inline.trigger.strategy = num_commits 
hoodie.compact.inline.max.delta.commits = 5
hoodie.compaction.strategy = org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy
hoodie.compaction.logfile.size.threshold=104857600
hoodie.compaction.target.io = 10240
```

After reaching 5 commits, Hudi evaluates the entire table to identify file groups with log files that meet or exceed the 100MB file size threshold. Let’s assume that out of 100 file groups, 50 file groups have log files that meet or exceed this threshold. Among these 50 file groups, Hudi accumulates log files up to 10GB, which is the specified target threshold. Once the 10GB log files limit is reached, Hudi finalizes the compaction plan and creates a `.compaction.requested` instant in the `.hoodie` directory. This results in a pending compaction. From there, the compaction plan is executed. 

## Compaction deployment models:
Once you have configured your compaction strategy, you need to execute it. Below are several options for how you can do this:

### Inline Compaction
Inline compaction refers to the process of executing the compaction process as part of the data ingestion pipeline, rather than running them asynchronously as a separate job. With inline compaction, Hudi will schedule, plan and execute the compaction operations after each commit is completed. This is the simplest deployment model to run because it’s easier to manage than running different asynchronus Spark jobs (see below). This mode is only supported on Spark Datasource, Spark-SQL and DeltaStreamer in sync-once mode. In the next section, we’ll go over code snippets you can use to get started with inline clustering. To run inline compaction with DeltaStreamer in continuous mode, you must pass the flag `--disable-compaction` so the async compaction is disabled (see below). With inline compaction, your data latency could be higher because you'll block any writer from ingesting data when compaction is being executed.

**DeltaStreamer**: When both ingestion and compaction are running in the same spark context, you can use resource allocation configuration in DeltaStreamer CLI such as (`--delta-sync-scheduling-weight`, `--compact-scheduling-weight`, `--delta-sync-scheduling-minshare`, and `--compact-scheduling-minshare`) to control executor allocation between ingestion and compaction.

### Asynchronous Compaction
There are three ways you can execute an asynchronous clustering process:

**Asynchronous execution within the same process**: In this deployment mode, Hudi will schedule, plan and execute the compaction operations after each commit is completed as part of the ingestion pipeline. Separately, Hudi spins up another thread within the same job and executes the compaction table service. This is supported by Spark Datasource, Spark streaming, Flink, DeltaStreamer in continuous mode and Spark-SQL.

For this deployment mode, you need to enable `hoodie.compact.inline.trigger.strategy`.

**Asynchronous scheduling and execution by a separate process**: In this deployment mode, you’ll write data into a Hudi table as part of the ingestion pipeline. In another job, you schedule and execute the compaction service. By running a different job for the compaction process, you rebalance how Hudi uses compute resources. You’ll use fewer compute resources for the writes and reserve more compute resources for the compaction process. Please ensure you have configured the lock providers for all jobs (both writer and table service jobs). In general, you’ll need to configure lock providers when we have two different jobs or two different processes occurring. All writers except the Spark streaming support this deployment model. For streaming ingestion use cases with DeltaStreamer in continuous mode, you’ll need to include the flag, `--disable-compaction`, so the default async compaction is disabled.

For this deployment mode, you need to enable `hoodie.compact.inline.trigger.strategy`.

**Scheduling inline and executing async**: In this deployment mode, you ingest data and schedule the compaction in one job; in another, you execute the compaction strategy. If you enable the metadata table, you do not need to provision lock providers. However, if you disable the metadata table, please ensure all jobs have the lock providers configured. Spark Datasource and Flink support this deployment option. 

For this deployment mode, you need to enable `hoodie.compact.inline.trigger.strategy`.

## Code Examples
### Inline clustering example
This is the most straightforward deployment option, where compaction will be triggered when the schedule compaction strategy threshold is met. The example below shows you how to run compaction every 4 commits: 

```
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=4
# Note: we are relying on default values for rest of the compaction configs like: 
# hoodie.compact.inline.trigger.strategy=NUM_COMMITS
# hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy
```

### Async clustering example
#### Spark Structured Streaming​ with the default async deployment model

Asynchronous execution within the same process are enabled by default for streaming jobs. Here is an example snippet in java:

```java
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

#### DeltaStreamer in continuous mode​ with the default async deployment model

Hudi DeltaStreamer provides continuous ingestion mode where a single long running spark application ingests data to Hudi table continuously from upstream sources. Here is an example snippet for running DeltaStreamer in continuous mode with the default async deployment option:

```
spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.6.0 \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--table-type MERGE_ON_READ \
--target-base-path <hudi_base_path> \
--target-table <hudi_table> \
--source-class org.apache.hudi.utilities.sources.JsonDFSSource \
--source-ordering-field ts \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--props /path/to/source.properties \
--continuous
```

#### Hudi Compactor Utility​
Hudi provides a standalone tool to execute specific compactions asynchronously. Below is an example and you can read more in the [deployment guide](https://hudi.apache.org/docs/deployment#compactions):

```
spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.13.0 \
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

### Flink Offline/Async Compaction​
You can run an offline/async compaction with Flink on the command line. The program entry is as follows:
`hudi-flink-bundle_2.11-0.13.0-SNAPSHOT.jar` :
`org.apache.hudi.sink.compact.HoodieFlinkCompactor` with the default async compaction deployment option:

```bash
# Command line
./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.13.0.jar --path hdfs://xxx:9000/table
```

## Flink configuration options​
Below are the configuration options you can set in the data ingestion pipeline:

- `FlinkOptions.COMPACTION_TRIGGER_STRATEGY`: This is the same strategy and values that was described in the scheduled strategy described earlier.
- `FlinkOptions.ARCHIVE_MAX_COMMITS`: This is the maximum number of commits to keep before archiving older commits into a sequential log. The default value is 30,
- `FlinkOptions.ARCHIVE_MIN_COMMITS`: This is the minimum number of commits to keep before archiving older commits into a sequential log. The default value is 20.
- `FlinkOptions.CLEAN_RETAIN_COMMITS`: This is the number of commits to retain in the active timeline. Data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table. The default value is 10.
- `FlinkOptions.COMPACTION_DELTA_COMMITS`: This is the maximum of delta commits needed to trigger compaction. The default value is 1 commit. 
- `FlinkOptions.COMPACTION_DELTA_SECONDS`: This is the maximum of delta seconds time needed to trigger compaction, default 3600 seconds or 1 hour.
- `FlinkOptions.COMPACTION_MAX_MEMORY`: This is the max memory in MB for compaction spillable map, default 100MB.
- `FlinkOptions.COMPACTION_TARGET_IO`: This is the number of MBs to spend during the compaction run. This value helps bound ingestion latency while compaction runs. 
- `FlinkOptions.COMPACTION_TASKS`: This is the parallelism of tasks that do actual compaction. The default value is the same with the write tasks.
- `FlinkOptions.CLEAN_ASYNC_ENABLED`: This configuration decides whether to cleanup the old commits immediately on new commits. It is enabled by default.
- `FlinkOptions.COMPACTION_ASYNC_ENABLED`: This is whether async compaction is enabled. The default value for Flink is false.
- `FlinkOptions.COMPACTION_SCHEDULE_ENABLED`: This configuration triggers the compaction plan scheduling after each successful delta commit. The default value is enabled.

For offline compaction, you need to run the Flink app manually. Here's a sample command below:

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `--path` | `true` | `--` | The path where the target table is stored on Hudi |
| `--compaction-max-memory` | `false` | `100` | The index map size of log data during compaction, 100 MB by default. If you have enough memory, you can turn up this parameter |
| `--schedule` | `false` | `false` | whether to execute the operation of scheduling compaction plan. When the write process is still writing， turning on this parameter have a risk of losing data. Therefore, it must be ensured that there are no write tasks currently writing data to this table when this parameter is turned on |
| `--seq` | `false` | `LIFO` | The order in which compaction tasks are executed. Executing from the latest compaction plan by default. `LIFO`: executing from the latest plan. `FIFO`: executing from the oldest plan. |
| `--service` | `false` | `false` | Whether to start a monitoring service that checks and schedules new compaction task in configured interval. |
| `--min-compaction-interval-seconds` | `false` | `600(s)` | The checking interval for service mode, by default 10 minutes. |
