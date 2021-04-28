---
title: "Quick-Start Guide"
permalink: /docs/flink-quick-start-guide.html
toc: true
last_modified_at: 2020-03-16T11:40:57+08:00
---

This guide provides a quick peek at Hudi's capabilities using flink SQL client. Using flink SQL, we will walk through 
code snippets that allows you to insert and update a Hudi table of default table type: 
[Copy on Write](/docs/concepts.html#copy-on-write-table) and [Merge On Read](/docs/concepts.html#merge-on-read-table). 
After each write operation we will also show how to read the data snapshot (incrementally read is already on the roadmap).

## Setup

We use the [Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) because it's a good
quick start tool for SQL users.

### Step.1 download flink jar
Hudi works with Flink-1.12.x version. You can follow instructions [here](https://flink.apache.org/downloads.html) for setting up flink.
The hudi-flink-bundle jar is archived with scala 2.11, so it’s recommended to use flink 1.12.x bundled with scala 2.11.

### Step.2 start flink cluster
Start a standalone flink cluster within hadoop environment.

Now starts the cluster:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the flink standalone cluster
./bin/start-cluster.sh
```
### Step.3 start flink SQL client

Hudi has a prepared bundle jar for flink, which should be loaded in the flink SQL Client when it starts up.
You can build the jar manually under path `hudi-source-dir/packaging/hudi-flink-bundle`, or download it from the
[Apache Official Repository](https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink-bundle_2.11/).

Now starts the SQL CLI:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

<div class="notice--info">
  <h4>Please note the following: </h4>
<ul>
  <li>We suggest hadoop 2.9.x+ version because some of the object storage has filesystem implementation only after that</li>
  <li>The flink-parquet and flink-avro formats are already packaged into the hudi-flink-bundle jar</li>
</ul>
</div>

Setup table name, base path and operate using SQL for this guide.
The SQL CLI only executes the SQL line by line.

## Insert data

Creates a flink hudi table first and insert data into the Hudi table using SQL `VALUES` as below.

```sql
-- sets up the result mode to tableau to show the results directly in the CLI
set execution.result-mode=tableau;

CREATE TABLE t1(
  uuid VARCHAR(20), -- you can use 'PRIMARY KEY NOT ENFORCED' syntax to mark the field as record key
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'table_base_path',
  'write.tasks' = '1', -- default is 4 ,required more resource
  'compaction.tasks' = '1', -- default is 10 ,required more resource
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

## Query data

```sql
-- query from the hudi table
select * from t1;
```

This query provides snapshot querying of the ingested data. 
Refer to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.
{: .notice--info}

## Update data

This is similar to inserting new data.

```sql
-- this would update the record with key 'id1'
insert into t1 values
  ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');
```

Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
[Querying](#query-data) the data again will now show updated records. Each write operation generates a new [commit](/docs/concepts.html) 
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `age` fields for the same `_hoodie_record_key`s in previous commit. 
{: .notice--info}

## Streaming query

Hudi flink also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's streaming querying and providing a start time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

```sql
CREATE TABLE t1(
  uuid VARCHAR(20), -- you can use 'PRIMARY KEY NOT ENFORCED' syntax to mark the field as record key
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'table_base_path',
  'table.type' = 'MERGE_ON_READ',
  'read.tasks' = '1', -- default is 4 ,required more resource
  'read.streaming.enabled' = 'true',  -- this option enable the streaming read
  'read.streaming.start-commit' = '20210316134557', -- specifies the start commit instant time
  'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits, default 60s.
);

-- Then query the table in stream mode
select * from t1;
``` 

This will give all changes that happened after the `read.streaming.start-commit` commit. The unique thing about this
feature is that it now lets you author streaming pipelines on streaming or batch data source.
{: .notice--info}

## Delete data {#deletes}

When consuming data in streaming query, hudi flink source can also accepts the change logs from the underneath data source,
it can then applies the UPDATE and DELETE by per-row level. You can then sync a NEAR-REAL-TIME snapshot on hudi for all kinds
of RDBMS.

## Where to go from here?

We used flink here to show case the capabilities of Hudi. However, Hudi can support multiple table types/query types and 
Hudi tables can be queried from query engines like Hive, Spark, Flink, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/docker_demo.html) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/migration_guide.html). 
