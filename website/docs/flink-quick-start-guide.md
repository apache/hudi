---
title: "Flink Guide"
toc: true
last_modified_at: 2020-08-12T15:19:57+08:00
---

This page introduces Flink-Hudi integration. We can feel the unique charm of how Flink brings in the power of streaming into Hudi.
This guide helps you quickly start using Flink on Hudi, and learn different modes for reading/writing Hudi by Flink:

- **Quick Start** : Read [Quick Start](#quick-start) to get started quickly Flink sql client to write to(read from) Hudi.
- **Configuration** : For [Global Configuration](flink_configuration#global-configurations), sets up through `$FLINK_HOME/conf/flink-conf.yaml`. For per job configuration, sets up through [Table Option](flink_configuration#table-options).
- **Writing Data** : Flink supports different modes for writing, such as [CDC Ingestion](hoodie_deltastreamer#cdc-ingestion), [Bulk Insert](hoodie_deltastreamer#bulk-insert), [Index Bootstrap](hoodie_deltastreamer#index-bootstrap), [Changelog Mode](hoodie_deltastreamer#changelog-mode) and [Append Mode](hoodie_deltastreamer#append-mode).
- **Querying Data** : Flink supports different modes for reading, such as [Streaming Query](querying_data#streaming-query) and [Incremental Query](querying_data#incremental-query).
- **Tuning** : For write/read tasks, this guide gives some tuning suggestions, such as [Memory Optimization](flink_configuration#memory-optimization) and [Write Rate Limit](flink_configuration#write-rate-limit).
- **Optimization**: Offline compaction is supported [Offline Compaction](compaction#flink-offline-compaction).
- **Query Engines**: Besides Flink, many other engines are integrated: [Hive Query](syncing_metastore#flink-setup), [Presto Query](query_engine_setup#prestodb).

## Quick Start

### Setup

We use the [Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/) because it's a good
quick start tool for SQL users.

#### Step.1 download Flink jar

Hudi works with both Flink 1.13, Flink 1.14, Flink 1.15. You can follow the
instructions [here](https://flink.apache.org/downloads) for setting up Flink. Then choose the desired Hudi-Flink bundle
jar to work with different Flink and Scala versions:

- `hudi-flink1.13-bundle`
- `hudi-flink1.14-bundle`
- `hudi-flink1.15-bundle`

#### Step.2 start Flink cluster
Start a standalone Flink cluster within hadoop environment.
Before you start up the cluster, we suggest to config the cluster as follows:

- in `$FLINK_HOME/conf/flink-conf.yaml`, add config option `taskmanager.numberOfTaskSlots: 4`
- in `$FLINK_HOME/conf/flink-conf.yaml`, [add other global configurations according to the characteristics of your task](flink_configuration#global-configurations)
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

This statement queries snapshot view of the dataset. 
Refers to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.

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

### Streaming Query

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

This will give all changes that happened after the `read.streaming.start-commit` commit. The unique thing about this
feature is that it now lets you author streaming pipelines on streaming or batch data source.

### Delete Data {#deletes}

When consuming data in streaming query, Hudi Flink source can also accepts the change logs from the underneath data source,
it can then applies the UPDATE and DELETE by per-row level. You can then sync a NEAR-REAL-TIME snapshot on Hudi for all kinds
of RDBMS.

## Where To Go From Here?
Check out the [Flink Setup](/docs/next/flink_configuration) how-to page for deeper dive into configuration settings. 

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
