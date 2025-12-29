---
title: "Flink Quick Start"
toc: true
last_modified_at: 2025-11-22T14:30:00+08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces Flink–Hudi integration and demonstrates how Flink brings the power of streaming to Hudi.

## Setup

### Flink Support Matrix

| Hudi   | Supported Flink versions                                               |
| :----- | :--------------------------------------------------------------------- |
| 1.1.x  | 1.17.x, 1.18.x, 1.19.x, 1.20.x (default build), 2.0.x                  |
| 1.0.x  | 1.14.x, 1.15.x, 1.16.x, 1.17.x, 1.18.x, 1.19.x, 1.20.x (default build) |
| 0.15.x | 1.14.x, 1.15.x, 1.16.x, 1.17.x, 1.18.x                                 |
| 0.14.x | 1.13.x, 1.14.x, 1.15.x, 1.16.x, 1.17.x                                 |

### Download Flink and Start Flink cluster

- You can follow the [instructions here](https://flink.apache.org/downloads) for setting up Flink
- Then start a standalone Flink cluster within a Hadoop environment

For local setup, you can download Hadoop binaries and set `HADOOP_HOME` as follows:

```shell
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the Flink standalone cluster
./bin/start-cluster.sh
```
<div className="notice--info">
  <h4>Please note the following: </h4>
<ul>
  <li>We recommend Hadoop 2.9.x+ because some object storage systems have filesystem implementations only from that version onward</li>
  <li>The flink-parquet and flink-avro formats are already packaged into the hudi-flink-bundle jar</li>
</ul>
</div>

<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>
<TabItem value="flinksql">

We use the [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/) because it's a good
quick-start tool for SQL users.

### Start Flink SQL Client

Hudi supports a packaged bundle jar for Flink, which should be loaded in the Flink SQL Client when it starts up.
You can build the jar manually under path `hudi-source-dir/packaging/hudi-flink-bundle`(see [Build Flink Bundle Jar](syncing_metastore.md#install)), or download it from the
[Apache Official Repository](https://repo.maven.apache.org/maven2/org/apache/hudi/).

Now start the SQL CLI:

```bash
# For Flink versions: 1.17-1.20, 2.0
export FLINK_VERSION=1.20 
export HUDI_VERSION=1.1.1
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink${FLINK_VERSION}-bundle/${HUDI_VERSION}/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar -P /tmp/
./bin/sql-client.sh embedded -j /tmp/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar shell
```

Setup table name, base path and operate using SQL for this guide.
The SQL CLI only executes the SQL line by line.
</TabItem>

<TabItem value="dataStream">

Please add the desired dependency to your project:
```xml
<!-- For Flink versions 1.17-1.20, 2.0-->
<properties>
    <flink.version>1.20.0</flink.version>
    <flink.binary.version>1.20</flink.binary.version>
    <hudi.version>1.1.1</hudi.version>
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

Here is an example of creating a Flink Hudi table.

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
// The first commit will auto-initialize the table if it does not exist in the specified base path.
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

Insert data into the Hudi table using SQL `VALUES`.

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

Add some streaming source to flink and load the data in hudi table. Since, this is the first write, it will also auto-create the table.

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
options.put("path", basePath);
options.put("table.type", HoodieTableType.MERGE_ON_READ.name());
options.put("ordering.fields", "ts");

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
```
Refer Full Quickstart Example [here](https://github.com/ad1happy2go/hudi-examples/blob/main/flink/src/main/java/com/hudi/flink/quickstart/HudiDataStreamWriter.java)

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
options.put("path", basePath);
options.put("table.type", HoodieTableType.MERGE_ON_READ.name());
options.put("read.streaming.enabled", "true"); // this option enable the streaming read
options.put("read.start-commit", "20210316134557"); // specifies the start commit instant time
    
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
Refer Full Streaming Reader Example [here](https://github.com/ad1happy2go/hudi-examples/blob/main/flink/src/main/java/com/hudi/flink/quickstart/HudiDataStreamReader.java)

</TabItem>

</Tabs
>

This statement queries snapshot view of the dataset. 
Refers to [Table types and queries](concepts.md#table-types--queries) for more info on all table types and query types supported.

## Update Data

This is similar to inserting new data.

<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

Hudi tables can be updated by either inserting records with the same record key or using a standard UPDATE statement as shown below.

```sql
-- Update Queries only works with batch execution mode
SET 'execution.runtime-mode' = 'batch';
UPDATE hudi_table SET fare = 25.0 WHERE uuid = '334e26e9-8355-45cc-97c6-c31daf0df330';
```

:::note
The `UPDATE` statement is supported since Flink 1.17, so only Hudi Flink bundle compiled with Flink 1.17+ supplies this functionality.
Only **batch** queries on Hudi table with record key work correctly.
:::
</TabItem>

<TabItem value="dataStream">

Add a streaming source to Flink and load the data into the Hudi table using the DataStream API as [above](#insert-data).
When new rows with the same record key arrive in the stream, they will be updated.
In the insert example, incoming rows with the same record ID will be updated.

Refer Update Example [here](https://github.com/ad1happy2go/hudi-examples/blob/main/flink/src/main/java/com/hudi/flink/quickstart/HudiDataStreamWriter.java)

</TabItem>

</Tabs
>

[Querying](#query-data) the data again will now show updated records. Each write operation generates a new [commit](concepts.md)
denoted by the timestamp.


## Delete Data {#deletes}
<Tabs
defaultValue="flinksql"
values={[
{ label: 'Flink SQL', value: 'flinksql', },
{ label: 'DataStream API', value: 'dataStream', },
]}
>

<TabItem value="flinksql">

### Row‑Level Delete

When consuming data in a streaming query, the Hudi Flink source can also accept change logs from the upstream data source if the `RowKind` is set up per row;
it can then apply UPDATE and DELETE at the row level. You can then sync a near‑real‑time snapshot on Hudi for all kinds
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
Only **batch** queries on Hudi table with record key work correctly.
:::

</TabItem>

<TabItem value="dataStream">

First, create a Flink Hudi table and insert data into the Hudi table using the DataStream API as below.
When new rows with the same record key and Row Kind as DELETE arrive in the stream, they will be deleted.

Refer Delete Example [here](https://github.com/ad1happy2go/hudi-examples/blob/main/flink/src/main/java/com/hudi/flink/quickstart/HudiDataStreamWriter.java)

</TabItem
>

</Tabs
>

## Streaming Query

Hudi Flink also provides the capability to obtain a stream of records that changed since a given commit timestamp.
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
  'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits; default is 60s.
);

-- Then query the table in stream mode
select * from t1;
```

## Change Data Capture Query

Hudi Flink also provides the capability to obtain a stream of records with Change Data Capture.
CDC queries are useful for applications that need to obtain all changes, along with before/after images of records.

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
  'table.type' = 'COPY_ON_WRITE',
  'cdc.enabled' = 'true' -- this option enables CDC logging
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
feature is that it lets you author streaming pipelines on streaming or batch data sources.

## Where To Go From Here?

- **Quick Start**: Read the Quick Start section above to get started quickly with the Flink SQL client to write to (and read from) Hudi.
- **Configuration**: For [Global Configuration](flink_tuning.md#global-configurations), set up through `$FLINK_HOME/conf/flink-conf.yaml`. For per-job configuration, set up through [Table Option](flink_tuning.md#table-options).
- **Writing Data** : Flink supports different modes for writing, such as [CDC Ingestion](ingestion_flink.md#cdc-ingestion), [Bulk Insert](ingestion_flink.md#bulk-insert), [Index Bootstrap](ingestion_flink.md#index-bootstrap), [Changelog Mode](ingestion_flink.md#changelog-mode) and [Append Mode](ingestion_flink.md#append-mode). Flink also supports multiple streaming writers with [non-blocking concurrency control](sql_dml.md#non-blocking-concurrency-control-experimental).
- **Reading Data** : Flink supports different modes for reading, such as [Streaming Query](sql_queries.md#streaming-query) and [Incremental Query](sql_queries.md#incremental-query).
- **Tuning**: For write/read tasks, this guide provides some tuning suggestions, such as [Memory Optimization](flink_tuning.md#memory-optimization) and [Write Rate Limit](flink_tuning.md#write-rate-limit).
- **Optimization**: Offline compaction is supported: [Offline Compaction](compaction.md#flink-offline-compaction).
- **Query Engines**: Besides Flink, many other engines are integrated: [Hive Query](syncing_metastore.md#flink-setup), [Presto Query](sql_queries.md#presto).
- **Catalog**: A Hudi‑specific catalog is supported: [Hudi Catalog](sql_ddl/#create-catalog).

If you are relatively new to Apache Hudi, it is important to be familiar with a few core concepts:

- [Hudi Timeline](timeline.md) – How Hudi manages transactions and other table services
- [Hudi Storage Layout](storage_layouts.md) – How files are laid out on storage
- [Hudi Table Types](table_types.md) – `COPY_ON_WRITE` and `MERGE_ON_READ`
- [Hudi Query Types](table_types.md#query-types) – Snapshot Queries, Incremental Queries, Read‑Optimized Queries

See more in the [concepts](concepts.md) docs page.

Take a look at recent [blog posts](/blog) that go in depth on certain topics or use cases.

Hudi tables can be queried from query engines like Hive, Spark, Flink, Presto, and much more. We have put together a
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that showcases all of this on a Docker‑based setup with all
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself by following
the steps in the [docker demo](docker_demo.md) to get a taste for it. Also, if you are looking for ways to migrate your existing data
to Hudi, refer to the [migration guide](migration_guide.md).
