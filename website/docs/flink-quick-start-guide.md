---
title: "Flink Guide"
toc: true
last_modified_at: 2023-08-16T12:53:57+08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces Flink-Hudi integration. We can feel the unique charm of how Flink brings in the power of streaming into Hudi.
This guide helps you quickly start using Flink on Hudi, and learn different modes for reading/writing Hudi by Flink.

## Setup
<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

We use the [Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/) because it's a good
quick start tool for SQL users.

### Download Flink

Hudi works with Flink 1.13, Flink 1.14, Flink 1.15, Flink 1.16 and Flink 1.17. You can follow the
instructions [here](https://flink.apache.org/downloads) for setting up Flink.

### Start Flink cluster
Start a standalone Flink cluster within hadoop environment. In case we are trying on local setup, then we could download hadoop binaries and set HADOOP_HOME.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the Flink standalone cluster
./bin/start-cluster.sh
```
### Start Flink SQL client

Hudi supports packaged bundle jar for Flink, which should be loaded in the Flink SQL Client when it starts up.
You can build the jar manually under path `hudi-source-dir/packaging/hudi-flink-bundle`(see [Build Flink Bundle Jar](/docs/syncing_metastore#install)), or download it from the
[Apache Official Repository](https://repo.maven.apache.org/maven2/org/apache/hudi/).

Now start the SQL CLI:

```bash
# For Flink versions: 1.13 - 1.17
export FLINK_VERSION=1.17 
export HUDI_VERSION=0.14.0
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink${FLINK_VERSION}-bundle/${HUDI_VERSION}/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar -P $FLINK_HOME/lib/
./bin/sql-client.sh embedded -j lib/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar shell
```


### Flink Support Matrix


| Hudi            | Supported Flink version                 |
|:----------------|:----------------------------------------|
| 0.14.x          | 1.13.x, 1.14.x, 1.15.x, 1.16.x, 1.17.x  |
| 0.13.x          | 1.13.x, 1.14.x, 1.15.x, 1.16.x          |
| 0.12.x          | 1.13.x, 1.14.x, 1.15.x                  |
| 0.11.x          | 1.13.x, 1.14.x                          |


<div className="notice--info">
  <h4>Please note the following: </h4>
<ul>
  <li>We suggest hadoop 2.9.x+ version because some of the object storage has filesystem implementation only after that</li>
  <li>The flink-parquet and flink-avro formats are already packaged into the hudi-flink-bundle jar</li>
</ul>
</div>

Setup table name, base path and operate using SQL for this guide.
The SQL CLI only executes the SQL line by line.
</TabItem>

<TabItem value="dataStream">

Hudi works with Flink 1.13, Flink 1.14, Flink 1.15, Flink 1.16 and Flink 1.17. Please add the desired
dependency to your project:
```xml
<!-- For Flink versions 1.13 - 1.17-->
<properties>
    <flink.version>1.17.0</flink.version>
    <flink.binary.version>1.17</flink.binary.version>
    <hudi.version>0.14.0</hudi.version>
</properties>
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-flink${flink.binary.version}-bundle</artifactId>
    <version>${hudi.version}</version>
</dependency>
```

</TabItem>

</Tabs
>

## Create Table

First, let's create a Hudi table. Here, we use a partitioned table for illustration, but Hudi also supports non-partitioned tables.

<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

Here is an example of creating a flink Hudi table.

```sql
-- sets up the result mode to tableau to show the results directly in the CLI
set sql-client.execution.result-mode = tableau;
DROP TABLE hudi_table;
CREATE TABLE hudi_table(
    ts BIGINT,
    uuid VARCHAR(40) PRIMARY KEY NOT ENFORCED,
    rider VARCHAR(20),
    driver VARCHAR(20),
    fare DOUBLE,
    city VARCHAR(20)
)
PARTITIONED BY (`city`)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi_table',
  'table.type' = 'MERGE_ON_READ'
);
```

</TabItem>

<TabItem value="dataStream">

```java
// Java
// First commit will auto-initialize the table, if it did not exist in the specified base path. 
```

</TabItem>


</Tabs
>

## Insert Data

<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

Creates a Flink Hudi table first and insert data into the Hudi table using SQL `VALUES` as below.

```sql
-- insert data using values
INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
```
</TabItem>

<TabItem value="dataStream">

Creates a Flink Hudi table first and insert data into the Hudi table using DataStream API as below.

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
String targetTable = "hudi_table";
String basePath = "file:///tmp/hudi_table";

Map<String, String> options = new HashMap<>();
options.put(FlinkOptions.PATH.key(), basePath);
options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");

DataStream<RowData> dataStream = env.addSource(...);
HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
    .column("uuid VARCHAR(20)")
    .column("name VARCHAR(10)")
    .column("age INT")
    .column("ts TIMESTAMP(3)")
    .column("`partition` VARCHAR(20)")
    .pk("uuid")
    .partition("partition")
    .options(options);

builder.sink(dataStream, false); // The second parameter indicating whether the input data stream is bounded
env.execute("Api_Sink");

// Full Quickstart Example - https://gist.github.com/ad1happy2go/1716e2e8aef6dcfe620792d6e6d86d36
```
</TabItem>

</Tabs
>

## Query Data

<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

```sql
-- query from the Hudi table
select * from hudi_table;
```
</TabItem>

<TabItem value="dataStream">

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
String targetTable = "hudi_table";
String basePath = "file:///tmp/hudi_table";

Map<String, String> options = new HashMap<>();
options.put(FlinkOptions.PATH.key(), basePath);
options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
options.put(FlinkOptions.READ_AS_STREAMING.key(), "true"); // this option enable the streaming read
options.put(FlinkOptions.READ_START_COMMIT.key(), "20210316134557"); // specifies the start commit instant time
    
HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
    .column("uuid VARCHAR(20)")
    .column("name VARCHAR(10)")
    .column("age INT")
    .column("ts TIMESTAMP(3)")
    .column("`partition` VARCHAR(20)")
    .pk("uuid")
    .partition("partition")
    .options(options);

DataStream<RowData> rowDataDataStream = builder.source(env);
rowDataDataStream.print();
env.execute("Api_Source");
```
</TabItem>

</Tabs
>

This statement queries snapshot view of the dataset. 
Refers to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.

## Update Data

This is similar to inserting new data.

```sql
SET 'execution.runtime-mode' = 'batch';
UPDATE hudi_table SET fare = 25.0 WHERE uuid = '334e26e9-8355-45cc-97c6-c31daf0df330';
```

Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
[Querying](#query-data) the data again will now show updated records. Each write operation generates a new [commit](/docs/concepts) 
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `age` fields for the same `_hoodie_record_key`s in previous commit.

:::note
The `UPDATE` statement is supported since Flink 1.17, so only Hudi Flink bundle compiled with Flink 1.17+ supplies this functionality.
Only **batch** queries on Hudi table with primary key work correctly. 
:::

## Delete Data {#deletes}

### Row-level Delete
When consuming data in streaming query, Hudi Flink source can also accept the change logs from the upstream data source if the `RowKind` is set up per-row,
it can then apply the UPDATE and DELETE in row level. You can then sync a NEAR-REAL-TIME snapshot on Hudi for all kinds
of RDBMS.

### Batch Delete

```sql
-- delete all the records with age greater than 23
-- NOTE: only works for batch sql queries
SET 'execution.runtime-mode' = 'batch';
DELETE FROM t1 WHERE age > 23;
```

:::note
The `DELETE` statement is supported since Flink 1.17, so only Hudi Flink bundle compiled with Flink 1.17+ supplies this functionality.
Only **batch** queries on Hudi table with primary key work correctly.
:::

## Streaming Query

Hudi Flink also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's streaming querying and providing a start time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case).

```sql
CREATE TABLE t1(
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
  'read.streaming.enabled' = 'true',  -- this option enable the streaming read
  'read.start-commit' = '20210316134557', -- specifies the start commit instant time
  'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits, default 60s.
);

-- Then query the table in stream mode
select * from t1;
``` 


:::info Key requirements
The bundle jar with **hive profile** is needed for streaming query, by default the officially released flink bundle is built **without**
**hive profile**, the jar needs to be built manually, see [Build Flink Bundle Jar](/docs/syncing_metastore#install) for more details.
:::

## Change Data Capture Query

Hudi Flink also provides capability to obtain a stream of records with Change Data Capture.
CDC queries are useful for applications that need to obtain all the changes, along with before/after images of records.

```sql
set sql-client.execution.result-mode = tableau;

CREATE TABLE hudi_table(
    ts BIGINT,
    uuid VARCHAR(40) PRIMARY KEY NOT ENFORCED,
    rider VARCHAR(20),
    driver VARCHAR(20),
    fare DOUBLE,
    city VARCHAR(20)
)
PARTITIONED BY (`city`)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi_table',
  'table.type' = 'MERGE_ON_READ',
  'changelog.enabled' = 'true',  -- this option enable the change log enabled
  'cdc.enabled' = 'true' -- this option enable the cdc log enabled
);
-- insert data using values
INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
SET 'execution.runtime-mode' = 'batch';
UPDATE hudi_table SET fare = 25.0 WHERE uuid = '334e26e9-8355-45cc-97c6-c31daf0df330';
-- Query the table in stream mode in another shell to see change logs
SET 'execution.runtime-mode' = 'streaming';
select * from hudi_table/*+ OPTIONS('read.streaming.enabled'='true')*/;
``` 

This will give all changes that happened after the `read.start-commit` commit. The unique thing about this
feature is that it now lets you author streaming pipelines on streaming or batch data source.

## Non-Blocking Concurrency Control

Hudi Flink supports a new non-blocking concurrency control mode, where multiple writer tasks can be executed
concurrently without blocking each other. One can read more about this mode in
the [concurrency control](/docs/next/concurrency_control#model-c-multi-writer) docs. Let us see it in action here.

In the below example, we have two streaming ingestion pipelines that concurrently update the same table. One of the
pipeline is responsible for the compaction and cleaning table services, while the other pipeline is just for data
ingestion.

```sql
-- set the interval as 30 seconds
execution.checkpointing.interval: 30000
state.backend: rocksdb

-- This is a datagen source that can generates records continuously
CREATE TABLE sourceT (
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` as 'par1'
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '200'
);

-- pipeline1: by default enable the compaction and cleaning services
CREATE TABLE t1(
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` varchar(20)
) WITH (
    'connector' = 'hudi',
    'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
    'table.type' = 'MERGE_ON_READ',
    'index.type' = 'BUCKET',
    'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'write.tasks' = '2'
);

-- pipeline2: disable the compaction and cleaning services manually
CREATE TABLE t1_2(
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` varchar(20)
) WITH (
    'connector' = 'hudi',
    'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
    'table.type' = 'MERGE_ON_READ',
    'index.type' = 'BUCKET',
    'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'write.tasks' = '2',
    'compaction.schedule.enabled' = 'false',
    'compaction.async.enabled' = 'false',
    'clean.async.enabled' = 'false'
);

-- submit the pipelines
insert into t1 select * from sourceT;
insert into t1_2 select * from sourceT;

select * from t1 limit 20;
```

As you can see from the above example, we have two pipelines with multiple tasks that concurrently write to the
same table. To use the new concurrency mode, all you need to do is set the `hoodie.write.concurrency.mode`
to `NON_BLOCKING_CONCURRENCY_CONTROL`. The `write.tasks` option is used to specify the number of write tasks that will
be used for writing to the table. The `compaction.schedule.enabled`, `compaction.async.enabled`
and `clean.async.enabled` options are used to disable the compaction and cleaning services for the second pipeline. 
This is done to ensure that the compaction and cleaning services are not executed twice for the same table.

## Where To Go From Here?
- **Quick Start** : Read [Quick Start](#quick-start) to get started quickly Flink sql client to write to(read from) Hudi.
- **Configuration** : For [Global Configuration](/docs/next/flink_tuning#global-configurations), sets up through `$FLINK_HOME/conf/flink-conf.yaml`. For per job configuration, sets up through [Table Option](/docs/next/flink_tuning#table-options).
- **Writing Data** : Flink supports different modes for writing, such as [CDC Ingestion](/docs/hoodie_streaming_ingestion#cdc-ingestion), [Bulk Insert](/docs/hoodie_streaming_ingestion#bulk-insert), [Index Bootstrap](/docs/hoodie_streaming_ingestion#index-bootstrap), [Changelog Mode](/docs/hoodie_streaming_ingestion#changelog-mode) and [Append Mode](/docs/hoodie_streaming_ingestion#append-mode).
- **Querying Data** : Flink supports different modes for reading, such as [Streaming Query](/docs/querying_data#streaming-query) and [Incremental Query](/docs/querying_data#incremental-query).
- **Tuning** : For write/read tasks, this guide gives some tuning suggestions, such as [Memory Optimization](/docs/next/flink_tuning#memory-optimization) and [Write Rate Limit](/docs/next/flink_tuning#write-rate-limit).
- **Optimization**: Offline compaction is supported [Offline Compaction](/docs/compaction#flink-offline-compaction).
- **Query Engines**: Besides Flink, many other engines are integrated: [Hive Query](/docs/syncing_metastore#flink-setup), [Presto Query](/docs/querying_data#prestodb).
- **Catalog**: A Hudi specific catalog is supported: [Hudi Catalog](/docs/querying_data/#hudi-catalog).

If you are relatively new to Apache Hudi, it is important to be familiar with a few core concepts:
  - [Hudi Timeline](/docs/next/timeline) – How Hudi manages transactions and other table services
  - [Hudi File Layout](/docs/next/file_layouts) - How the files are laid out on storage
  - [Hudi Table Types](/docs/next/table_types) – `COPY_ON_WRITE` and `MERGE_ON_READ`
  - [Hudi Query Types](/docs/next/table_types#query-types) – Snapshot Queries, Incremental Queries, Read-Optimized Queries

See more in the "Concepts" section of the docs.

Take a look at recent [blog posts](/blog) that go in depth on certain topics or use cases.

Hudi tables can be queried from query engines like Hive, Spark, Flink, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/docker_demo) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/migration_guide). 
