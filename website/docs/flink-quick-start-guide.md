---
title: "Flink Guide"
toc: true
last_modified_at: 2020-08-12T15:19:57+08:00
---

This guide provides a document at Hudi's capabilities using Flink SQL. We can feel the unique charm of Flink stream computing engine on Hudi.
Reading this guide, you can quickly start using Flink to write to(read from) Hudi, have a deeper understanding of configuration and optimization:

- **Quick Start** : Read [Quick Start](#quick-start) to get started quickly flink sql client to write to(read from) Hudi.
- **Configuration** : For [Flink Configuration](#flink-configuration), sets up through `$FLINK_HOME/conf/flink-conf.yaml`. For per job configuration, sets up through [Table Option](#table-option).
- **Writing Data** : Flink supports different writing data use cases, such as [Bulk Insert](#bulk-insert), [Index Bootstrap](#index-bootstrap), [Changelog Mode](#changelog-mode), [Insert Mode](#insert-mode)  and [Offline Compaction](#offline-compaction).
- **Querying Data** : Flink supports different querying data use cases, such as [Hive Query](#hive-query), [Presto Query](#presto-query).
- **Optimization** : For write/read tasks, this guide gives some optimization suggestions, such as [Memory Optimization](#memory-optimization) and [Write Rate Limit](#write-rate-limit).

## Quick Start

### Setup

We use the [Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/) because it's a good
quick start tool for SQL users.

#### Step.1 download Flink jar
Hudi works with Flink-1.11.x version. You can follow instructions [here](https://flink.apache.org/downloads) for setting up Flink.
The hudi-flink-bundle jar is archived with scala 2.11, so it’s recommended to use flink 1.11 bundled with scala 2.11.

#### Step.2 start Flink cluster
Start a standalone Flink cluster within hadoop environment.
Before you start up the cluster, we suggest to config the cluster as follows:

- in `$FLINK_HOME/conf/flink-conf.yaml`, add config option `taskmanager.numberOfTaskSlots: 4`
- in `$FLINK_HOME/conf/flink-conf.yaml`, [add other global configurations according to the characteristics of your task](#flink-configuration)
- in `$FLINK_HOME/conf/workers`, add item `localhost` as 4 lines so that there are 4 workers on the local cluster

Now starts the cluster:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the Flink standalone cluster
./bin/start-cluster.sh
```
#### Step.3 start Flink SQL client

Hudi has a prepared bundle jar for Flink, which should be loaded in the Flink SQL Client when it starts up.
You can build the jar manually under path `hudi-source-dir/packaging/hudi-flink-bundle`, or download it from the
[Apache Official Repository](https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink-bundle_2.11/).

Now starts the SQL CLI:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

<div className="notice--info">
  <h4>Please note the following: </h4>
<ul>
  <li>We suggest hadoop 2.9.x+ version because some of the object storage has filesystem implementation only after that</li>
  <li>The flink-parquet and flink-avro formats are already packaged into the hudi-flink-bundle jar</li>
</ul>
</div>

Setup table name, base path and operate using SQL for this guide.
The SQL CLI only executes the SQL line by line.

### Insert Data

Creates a Flink Hudi table first and insert data into the Hudi table using SQL `VALUES` as below.

```sql
-- sets up the result mode to tableau to show the results directly in the CLI
set execution.result-mode=tableau;

CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'schema://base-path',
  'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
);

-- insert data using values
INSERT INTO t1 VALUES
  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),
  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),
  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),
  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),
  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),
  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),
  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),
  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');
```

### Query Data

```sql
-- query from the Hudi table
select * from t1;
```

This query provides snapshot querying of the ingested data. 
Refer to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.
{: .notice--info}

### Update Data

This is similar to inserting new data.

```sql
-- this would update the record with key 'id1'
insert into t1 values
  ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');
```

Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
[Querying](#query-data) the data again will now show updated records. Each write operation generates a new [commit](/docs/concepts) 
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `age` fields for the same `_hoodie_record_key`s in previous commit. 
{: .notice--info}

### Streaming Query

Hudi Flink also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's streaming querying and providing a start time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

```sql
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'MERGE_ON_READ',
  'read.streaming.enabled' = 'true',  -- this option enable the streaming read
  'read.streaming.start-commit' = '20210316134557' -- specifies the start commit instant time
  'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits, default 60s.
);

-- Then query the table in stream mode
select * from t1;
``` 

This will give all changes that happened after the `read.streaming.start-commit` commit. The unique thing about this
feature is that it now lets you author streaming pipelines on streaming or batch data source.
{: .notice--info}

### Delete Data {#deletes}

When consuming data in streaming query, Hudi Flink source can also accepts the change logs from the underneath data source,
it can then applies the UPDATE and DELETE by per-row level. You can then sync a NEAR-REAL-TIME snapshot on Hudi for all kinds
of RDBMS.

## Flink Configuration

Before using Flink, you need to set some global configurations in `$FLINK_HOME/conf/flink-conf.yaml`

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

## Table Option

Flink SQL job can be configured through the options in [`WITH`](#table-option) clause.
The actual datasource level configs are listed below.

### Memory

:::note
When optimizing memory, we need to pay attention to the [memory configuration](#memory) 
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
| `compaction.tasks` | The parallelism of online compaction. Default `10` | `10` | `Online compaction` will occupy the resources of the write task. It is recommended to use [`offline compaction`](#offline-compaction) |

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
| `compaction.target_io`  |  Target IO per compaction (both read and write), default `5GB`| `5120` | The default value for `offline compaction` is `500GB` |

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

## Bulk Insert

For the demand of snapshot data import. If the snapshot data comes from other data sources, use the `bulk_insert` mode to quickly
import the snapshot data into Hudi.


:::note
 `bulk_insert` eliminates the serialization and data merging. The data deduplication is skipped, so the user need to guarantee the uniqueness of the data.
:::

:::note
`bulk_insert` is more efficient in the `batch execution mode`. By default, the `batch execution mode` sorts the input records 
by the partition path and writes these records to Hudi, which can avoid write performance degradation caused by 
frequent `file handle` switching.  
:::

:::note  
The parallelism of `bulk_insert` is specified by `write.tasks`. The parallelism will affect the number of small files.
In theory, the parallelism of `bulk_insert` is the number of `bucket`s (In particular, when each bucket writes to maximum file size, it 
will rollover to the new file handle. Finally, `the number of files` >= [`write.bucket_assign.tasks`](#parallelism)).
:::

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.operation` | `true` | `upsert` | Setting as `bulk_insert` to open this function  |
| `write.tasks`  |  `false`  | `4` | The parallelism of `bulk_insert`, `the number of files` >= [`write.bucket_assign.tasks`](#parallelism) |
| `write.bulk_insert.shuffle_by_partition` | `false` | `true` | Whether to shuffle data according to the partition field before writing. Enabling this option will reduce the number of small files, but there may be a risk of data skew  |
| `write.bulk_insert.sort_by_partition` | `false`  | `true` | Whether to sort data according to the partition field before writing. Enabling this option will reduce the number of small files when a write task writes multiple partitions  |
| `write.sort.memory` | `false` | `128` | Available managed memory of sort operator. default  `128` MB |

## Index Bootstrap

For the demand of `snapshot data` + `incremental data` import. If the `snapshot data` already insert into Hudi by  [bulk insert](#bulk-insert). 
User can insert `incremental data` in real time and ensure the data is not repeated by using the index bootstrap function.

:::note
If you think this process is very time-consuming, you can add resources to write in streaming mode while writing `snapshot data`, 
and then reduce the resources to write `incremental data` (or open the rate limit function).
:::

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `index.bootstrap.enabled` | `true` | `false` | When index bootstrap is enabled, the remain records in Hudi table will be loaded into the Flink state at one time |
| `index.partition.regex`  |  `false`  | `*` | Optimize option. Setting regular expressions to filter partitions. By default, all partitions are loaded into flink state |

### How To Use

1. `CREATE TABLE` creates a statement corresponding to the Hudi table. Note that the `table.type` must be correct.
2. Setting `index.bootstrap.enabled` = `true` to enable the index bootstrap function.
3. Setting Flink checkpoint failure tolerance in `flink-conf.yaml` : `execution.checkpointing.tolerable-failed-checkpoints = n` (depending on Flink checkpoint scheduling times).
4. Waiting until the first checkpoint succeeds, indicating that the index bootstrap completed.
5. After the index bootstrap completed, user can exit and save the savepoint (or directly use the externalized checkpoint).
6. Restart the job, setting `index.bootstrap.enable` as `false`.

:::note
1. Index bootstrap is blocking, so checkpoint cannot be completed during index bootstrap.
2. Index bootstrap triggers by the input data. User need to ensure that there is at least one record in each partition.
3. Index bootstrap executes concurrently. User can search in log by `finish loading the index under partition` and `Load record form file` to observe the progress of index bootstrap.
4. The first successful checkpoint indicates that the index bootstrap completed. There is no need to load the index again when recovering from the checkpoint.
:::

## Changelog Mode
Hudi can keep all the intermediate changes (I / -U / U / D) of messages, then consumes through stateful computing of flink to have a near-real-time
data warehouse ETL pipeline (Incremental computing). Hudi MOR table stores messages in the forms of rows, which supports the retention of all change logs (Integration at the format level).
All changelog records can be consumed with Flink streaming reader.

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `changelog.enabled` | `false` | `false` | It is turned off by default, to have the `upsert` semantics, only the merged messages are ensured to be kept, intermediate changes may be merged. Setting to true to support consumption of all changes. |

:::note
Batch (Snapshot) read still merge all the intermediate changes, regardless of whether the format has stored the intermediate changelog messages.
:::

:::note
After setting `changelog.enable` as `true`, the retention of changelog records are only best effort: the asynchronous compaction task will merge the changelog records into one record, so if the
stream source does not consume timely, only the merged record for each key can be read after compaction. The solution is to reserve some buffer time for the reader by adjusting the compaction strategy, such as
the compaction options: [`compaction.delta_commits`](#compaction) and [`compaction.delta_seconds`](#compaction).
:::


## Insert Mode

Hudi apply the small file strategy for the insert mode by default: MOR appends delta records to log files, COW merges the base parquet files (the incremental data set will be deduplicated).
This strategy lead to performance degradation.

If you want to forbid the behavior of file merge, sets `write.insert.deduplicate` as `false`,the deduplication is skipped.
Each flush behavior directly writes a now parquet file (MOR table also directly write parquet file).

### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.insert.deduplicate` | `false` | `true` | Insert mode enable deduplication by default. After this option is turned off, each flush behavior directly writes a now parquet file |

## Hive Query

### Install

Now you can git clone Hudi master branch to test Flink hive sync. The first step is to install Hudi to get `hudi-flink-bundle_2.11-0.x.jar`.
`hudi-flink-bundle` module pom.xml sets the scope related to hive as `provided` by default. If you want to use hive sync, you need to use the
profile `flink-bundle-shade-hive` during packaging. Executing command below to install:

```bash
# Maven install command
mvn install -DskipTests -Drat.skip=true -Pflink-bundle-shade-hive2

# For hive1, you need to use profile -Pflink-bundle-shade-hive1
# For hive3, you need to use profile -Pflink-bundle-shade-hive3 
```

:::note 
Hive1.x can only synchronize metadata to hive, but cannot use hive query now. If you need to query, you can use spark to query hive table.
:::

:::note 
If using hive profile, you need to modify the hive version in the profile to your hive cluster version (Only need to modify the hive version in this profile).
The location of this `pom.xml` is `packaging/hudi-flink-bundle/pom.xml`, and the corresponding profile is at the bottom of this file.
:::

### Hive Environment

1. Import `hudi-hadoop-mr-bundle` into hive. Creating `auxlib/` folder under the root directory of hive, and moving `hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` into `auxlib`.
`hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` is at `packaging/hudi-hadoop-mr-bundle/target`.

2. When Flink sql client connects hive metastore remotely, `hive metastore` and `hiveserver2` services need to be enabled, and the port number need to
be set correctly. Command to turn on the services:

```bash
# Enable hive metastore and hiveserver2
nohup ./bin/hive --service metastore &
nohup ./bin/hive --service hiveserver2 &

# While modifying the jar package under auxlib, you need to restart the service.
```

### Sync Template

Flink hive sync now supports two hive sync mode, `hms` and `jdbc`. `hms` mode only needs to configure metastore uris. For 
the `jdbc` mode, the JDBC attributes and metastore uris both need to be configured. The options template is as below:

```sql
-- hms mode template
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'COPY_ON_WRITE',  --If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',  -- Required. To enable hive synchronization
  'hive_sync.mode' = 'hms' -- Required. Setting hive sync mode to hms, default jdbc
  'hive_sync.metastore.uris' = 'thrift://ip:9083' -- Required. The port need set on hive-site.xml
);


-- jdbc mode template
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'COPY_ON_WRITE',  --If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',  -- Required. To enable hive synchronization
  'hive_sync.mode' = 'hms' -- Required. Setting hive sync mode to hms, default jdbc
  'hive_sync.metastore.uris' = 'thrift://ip:9083' -- Required. The port need set on hive-site.xml
  'hive_sync.jdbc_url'='jdbc:hive2://ip:10000',    -- required, hiveServer port
  'hive_sync.table'='t1',                          -- required, hive table name
  'hive_sync.db'='testDB',                         -- required, hive database name
  'hive_sync.username'='root',                     -- required, HMS username
  'hive_sync.password'='your password'             -- required, HMS password
);
```

### Query

While using hive beeline query, you need to enter settings:
```bash
set hive.input.format = org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
```

### Conflict

When there is a `flink-sql-connector-hive-xxx.jar` under Flink lib/, there will be a jar conflicts between `flink-sql-connector-hive-xxx.jar`
and `hudi-flink-bundle_2.11.xxx.jar`. The solution is to use another profile `include-flink-sql-connector-hive` when install and delete
the `flink-sql-connector-hive-xxx.jar` under Flink lib/. install command :

```bash
# Maven install command
mvn install -DskipTests -Drat.skip=true -Pflink-bundle-shade-hive2 -Pinclude-flink-sql-connector-hive
```

## Presto Query

### Hive Sync
First, you need to sync Hudi table metadata to hive according to the above steps of [Hive Query](#hive-query).

### Presto Environment
1. Configure Presto according to the [Presto configuration document](https://prestodb.io/docs/current/installation/deployment.html).
2. Configure hive catalog in ` /presto-server-0.2xxx/etc/catalog/hive.properties` as follows:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

### Query

Beginning query by connecting hive metastore with presto client. The presto client connection command is as follows:

```bash
# The presto client connection command
./presto --server xxx.xxx.xxx.xxx:9999 --catalog hive --schema default
```

:::note
1. Presto-server-0.2445 is a lower version. When querying the `rt table` of MERGE_ON_WRITE, there will be a package conflict, this bug is in fix.
2. When Presto-server-xxx version < 0.233, the `hudi-presto-bundle.jar` needs to manually import into `{presto_install_dir}/plugin/hive-hadoop2/`.
:::


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

Offline compaction needs to submit the Flink task on the command line. The program entry is as follows: `hudi-flink-bundle_2.11-0.9.0-SNAPSHOT.jar` :
`org.apache.hudi.sink.compact.HoodieFlinkCompactor`

```bash
# Command line
x./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.9.0.jar --path hdfs://xxx:9000/table
```

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `--path` | `frue` | `--` | The path where the target table is stored on Hudi |
| `--compaction-max-memory` | `false` | `100` | The index map size of log data during compaction, 100 MB by default. If you have enough memory, you can turn up this parameter |
| `--schedule` | `false` | `false` | whether to execute the operation of scheduling compaction plan. When the write process is still writing， turning on this parameter have a risk of losing data. Therefore, it must be ensured that there are no write tasks currently writing data to this table when this parameter is turned on |
| `--seq` | `false` | `LIFO` | The order in which compaction tasks are executed. Executing from the latest compaction plan by default. `LIFO`: executing from the latest plan. `FIFO`: executing from the oldest plan. |

## Write Rate Limit

In the existing data synchronization, `snapshot data` and `incremental data` are send to kafka first, and then streaming write
to Hudi by Flink. Because the direct consumption of `snapshot data` will lead to problems such as high throughput and serious 
disorder (writing partition randomly), which will lead to write performance degradation and throughput glitches. At this time, 
the `write.rate.limit` option can be turned on to ensure smooth writing.

### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.rate.limit` | `false` | `0` | Turn off by default |

## Where To Go From Here?

We used Flink here to show case the capabilities of Hudi. However, Hudi can support multiple table types/query types and 
Hudi tables can be queried from query engines like Hive, Spark, Flink, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/docker_demo) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/migration_guide). 
