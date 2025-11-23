---
title: SQL DML
summary: "In this page, we go will cover details on how to use DML statements on Hudi tables."
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Spark SQL

SparkSQL provides several Data Manipulation Language (DML) actions for interacting with Hudi tables. These operations allow you to insert, update, merge and delete data 
from your Hudi tables. Let's explore them one by one.

Please refer to [SQL DDL](sql_ddl) for creating Hudi tables using SQL.

### Insert Into

You can use the `INSERT INTO` statement to add data to a Hudi table using Spark SQL. Here are some examples:

```sql
INSERT INTO <table> 
SELECT <columns> FROM <source>;
```

:::info
`INSERT INTO` statement does not support evolving table schema. Please use DDL (e.g., `ALTER TABLE`) or Datasource write (`df.write.format("hudi")....save(basePath)`) to evolve table schema.
:::

:::note Deprecations
From 0.14.0, `hoodie.sql.bulk.insert.enable` and `hoodie.sql.insert.mode` are deprecated. Users are expected to use `hoodie.spark.sql.insert.into.operation` instead.
To manage duplicates with `INSERT INTO`, please check out [insert dup policy config](configurations#hoodiedatasourceinsertduppolicy).
:::

Examples: 

```sql
-- Insert into a copy-on-write (COW) Hudi table
INSERT INTO hudi_cow_nonpcf_tbl SELECT 1, 'a1', 20;

-- Insert into a merge-on-read (MOR) Hudi table
INSERT INTO hudi_mor_tbl SELECT 1, 'a1', 20, 1000;

-- Insert into a COW Hudi table with static partition
INSERT INTO hudi_cow_pt_tbl PARTITION(dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000;

-- Insert into a COW Hudi table with dynamic partition
INSERT INTO hudi_cow_pt_tbl PARTITION(dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh;
```

:::note Mapping to write operations
Hudi offers flexibility in choosing the underlying [write operation](write_operations) of a `INSERT INTO` statement using 
the `hoodie.spark.sql.insert.into.operation` configuration. Possible options include *"bulk_insert"* (large inserts), *"insert"* (with small file management), 
and *"upsert"* (with deduplication/merging). If ordering fields are not set, *"insert"* is chosen as the default. For a table with ordering fields set (via `preCombineField`),
*"upsert"* is chosen as the default operation.
:::


### Insert Overwrite

The `INSERT OVERWRITE` statement is used to replace existing data in a Hudi table. 

```sql
INSERT OVERWRITE <table> 
SELECT <columns> FROM <source>;
```

All existing partitions that are affected by the `INSERT OVERWRITE` statement will replaced with the source data.
Here are some examples:

```sql
-- Overwrite non-partitioned table
INSERT OVERWRITE hudi_mor_tbl SELECT 99, 'a99', 20.0, 900;
INSERT OVERWRITE hudi_cow_nonpcf_tbl SELECT 99, 'a99', 20.0;

-- Overwrite partitioned table with dynamic partition
INSERT OVERWRITE TABLE hudi_cow_pt_tbl SELECT 10, 'a10', 1100, '2021-12-09', '10';

-- Overwrite partitioned table with static partition
INSERT OVERWRITE hudi_cow_pt_tbl PARTITION(dt = '2021-12-09', hh='12') SELECT 13, 'a13', 1100;
```

### Update
You can use the `UPDATE` statement to modify existing data in a Hudi table directly. 

```sql
UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]
```

Here's an example:

```sql
-- Update data in a Hudi table
UPDATE hudi_mor_tbl SET price = price * 2, ts = 1111 WHERE id = 1;

-- Update data in a partitioned Hudi table
UPDATE hudi_cow_pt_tbl SET name = 'a1_1', ts = 1001 WHERE id = 1;

-- update using non-PK field
update hudi_cow_pt_tbl set ts = 1001 where name = 'a1';
```

:::info
The `UPDATE` operation requires the specification of ordering fields (via `preCombineField`).
:::

### Merge Into

The `MERGE INTO` statement allows you to perform more complex updates and merges against source data. The `MERGE INTO` statement 
is similar to the `UPDATE` statement, but it allows you to specify different actions for matched and unmatched records.

```sql
MERGE INTO tableIdentifier AS target_alias
USING (sub_query | tableIdentifier) AS source_alias
ON <merge_condition>
[ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
[ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]

<merge_condition> =A equal bool condition 
<matched_action>  =
  DELETE  |
  UPDATE SET *  |
  UPDATE SET column1 = expression1 [, column2 = expression2 ...]
<not_matched_action>  =
  INSERT *  |
  INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])
```

:::info
`MERGE INTO` statement does not support evolving table schema. Please use DDL (e.g., `ALTER TABLE`) or Datasource write (`df.write.format("hudi")....save(basePath)`) to evolve table schema.

`WHEN NOT MATCHED` clauses specify the action to perform if the values do not match.
There are two kinds of `INSERT` clauses:
1. `INSERT *` clauses require that the source table has the same columns as those in the target table.
2. `INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])` clauses do not require to specify all the columns of the target table. For unspecified target columns, insert the `NULL` value.

For a Hudi table with user configured primary keys, the join condition and the `UPDATE`/`INSERT INTO` clause in `MERGE INTO` is expected to contain the primary keys of the table. 

For a table where Hudi auto generates primary keys, the join condition in `MERGE INTO` can be on any arbitrary data columns. 

if the `hoodie.record.merge.mode` is set to `EVENT_TIME_ORDERING`, ordering fields (via `preCombineField`) are required to be set with value in the `UPDATE`/`INSERT` clause. 

It is enforced that if the target table has primary key and partition key column, the source table counterparts must enforce the same data type accordingly. Plus, if the target table is configured with `hoodie.record.merge.mode` = `EVENT_TIME_ORDERING` where target table is expected to have valid ordering fields configuration, the source table counterpart must also have the same data type.
:::

Examples below

```sql
-- source table using hudi for testing merging into non-partitioned table
create table merge_source (id int, name string, price double, ts bigint) using hudi
tblproperties (primaryKey = 'id', preCombineField = 'ts');
insert into merge_source values (1, "old_a1", 22.22, 900), (2, "new_a2", 33.33, 2000), (3, "new_a3", 44.44, 2000);

merge into hudi_mor_tbl as target
using merge_source as source
on target.id = source.id
when matched then update set *
when not matched then insert *
;

-- source table using parquet for testing merging into partitioned table
create table merge_source2 (id int, name string, flag string, dt string, hh string) using parquet;
insert into merge_source2 values (1, "new_a1", 'update', '2021-12-09', '10'), (2, "new_a2", 'delete', '2021-12-09', '11'), (3, "new_a3", 'insert', '2021-12-09', '12');

MERGE into hudi_cow_pt_tbl as target
using (
  select id, name, '1000' as ts, flag, dt, hh from merge_source2
) source
on target.id = source.id
when matched and flag != 'delete' then
 update set id = source.id, name = source.name, ts = source.ts, dt = source.dt, hh = source.hh
when matched and flag = 'delete' then delete
when not matched then
 insert (id, name, ts, dt, hh) values(source.id, source.name, source.ts, source.dt, source.hh)
;

```

:::note Key requirements
For a Hudi table with user configured primary keys, the join condition in `Merge Into` is expected to contain the primary keys of the table.
For a Table where Hudi auto generates primary keys, the join condition in MIT can be on any arbitrary data columns.
:::

### Merge Into with Partial Updates {#merge-into-partial-update}

Partial updates only write updated columns instead of full change records. This is useful when you have wide tables (typical for ML feature stores) 
with hundreds of columns and only a few columns are updated. It reduces the write amplification as well as helps in lowering the query
latency. `MERGE INTO` statement above can be modified to use partial updates as shown below.

```sql
-- Create a Merge-on-Read table
CREATE TABLE tableName (
  id INT,
  name STRING,
  price DOUBLE,
  _ts LONG,
  description STRING
) USING hudi 
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'id',
  preCombineField = '_ts'
)
LOCATION '/location/to/basePath';

-- Insert values into the table
INSERT INTO tableName VALUES
  (1, 'a1', 10, 1000, 'a1: desc1'),
  (2, 'a2', 20, 1200, 'a2: desc2'),
  (3, 'a3', 30, 1250, 'a3: desc3');

-- Perform partial updates using a MERGE INTO statement
MERGE INTO tableName t0
  USING (
    SELECT 1 AS id, 'a1' AS name, 12 AS price, 1001 AS ts
    UNION ALL
    SELECT 3 AS id, 'a3' AS name, 25 AS price, 1260 AS ts
  ) s0
  ON t0.id = s0.id
  WHEN MATCHED THEN UPDATE SET
    price = s0.price,
    _ts = s0.ts;

SELECT id, name, price, _ts, description FROM tableName;
```

Notice, instead of `UPDATE SET *`, we are updating only the `price` and `_ts` columns.

:::note
Partial update is not yet supported in the following cases:
1. When the target table is a bootstrapped table. 
2. When virtual keys is enabled.
3. When schema on read is enabled. 
4. When there is an enum field in the source data.
5. When a global index is enabled, e.g., GLOBAL_SIMPLE.

For a Hudi table with user configured primary keys, the join condition and the `UPDATE`/`INSERT INTO` clause in `MERGE INTO` is expected to contain the primary keys of the table.
:::

### Delete From

You can remove data from a Hudi table using the `DELETE FROM` statement.

```sql
DELETE FROM tableIdentifier [ WHERE boolExpression ]
```

Examples below

```sql
-- Delete data from a Hudi table
DELETE FROM hudi_cow_nonpcf_tbl WHERE uuid = 1;

-- Delete data from a MOR Hudi table based on a condition
DELETE FROM hudi_mor_tbl WHERE id % 2 = 0;

-- Delete data using a non-primary key field
DELETE FROM hudi_cow_pt_tbl WHERE name = 'a1';
```

### Data Skipping and Indexing 

DML operations can be sped up using column statistics for data skipping and using indexes to reduce the amount of data scanned.
For e.g. the following helps speed up the `DELETE` operation on a Hudi table, by using the record level index.

```sql
SET hoodie.metadata.record.index.enable=true;

DELETE from hudi_table where uuid = 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa';
```

These DML operations give you powerful tools for managing your tables using Spark SQL. 
You can control the behavior of these operations using various configuration options, as explained in the documentation.

## Flink SQL

Flink SQL provides several Data Manipulation Language (DML) actions for interacting with Hudi tables. These operations allow you to insert, update and delete data from your Hudi tables. Let's explore them one by one.

### Insert Into

You can utilize the INSERT INTO statement to incorporate data into a Hudi table using Flink SQL. Here are a few illustrative examples:

```sql
INSERT INTO <table> 
SELECT <columns> FROM <source>;
```

Examples:

```sql
-- Insert into a Hudi table
INSERT INTO hudi_table SELECT 1, 'a1', 20;
```

If the `write.operation` is 'upsert,' the INSERT INTO statement will not only insert new records but also update existing rows with the same record key.

```sql
-- Insert into a Hudi table in upsert mode
INSERT INTO hudi_table/*+ OPTIONS('write.operation'='upsert')*/ SELECT 1, 'a1', 20;
```

### Update
With Flink SQL, you can use update command to update the hudi table. Here are a few illustrative examples:

```sql
UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]
```

```sql
UPDATE hudi_table SET price = price * 2, ts = 1111 WHERE id = 1;
```

:::note Key requirements
Update query only work with batch excution mode.
:::

### Delete From
With Flink SQL, you can use delete command to delete the rows from hudi table. Here are a few illustrative examples:

```sql
DELETE FROM tableIdentifier [ WHERE boolExpression ]
```

```sql
DELETE FROM hudi_table WHERE price < 100;
```


```sql
DELETE FROM hudi_table WHERE price < 100;
```

:::note Key requirements
Delete query only work with batch excution mode.
:::

### Lookup Joins

A lookup join is typically used to enrich a table with data that is queried from an external system. The join requires
one table to have a processing time attribute and the other table to be backed by a lookup source connector.

```sql
CREATE TABLE datagen_source(
    id int,
    name STRING,
    proctime as PROCTIME()
) WITH (
'connector' = 'datagen',
'rows-per-second'='1',
'number-of-rows' = '2',
'fields.id.kind'='sequence',
'fields.id.start'='1',
'fields.id.end'='2'
);

SELECT o.id,o.name,b.id as id2
FROM datagen_source AS o
JOIN hudi_table/*+ OPTIONS('lookup.join.cache.ttl'= '2 day') */ FOR SYSTEM_TIME AS OF o.proctime AS b on o.id = b.id; 
```

### Setting Writer/Reader Configs
With Flink SQL, you can additionally set the writer/reader writer configs along with the query.

```sql
INSERT INTO hudi_table/*+ OPTIONS('${hoodie.config.key1}'='${hoodie.config.value1}')*/
```

```sql
INSERT INTO hudi_table/*+ OPTIONS('hoodie.keep.max.commits'='true')*/
```

## Flink SQL In Action

The hudi-flink module defines the Flink SQL connector for both hudi source and sink.
There are a number of options available for the sink table:

| Option Name                                | Required | Default                              | Remarks                                                                                                                                                                                                                                                               |
|--------------------------------------------|----------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                       | Y        | N/A                                  | Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully                                                                                                                  |
| table.type                                 | N        | COPY_ON_WRITE                        | Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ                                                                                                                                                                                                              |
| write.operation                            | N        | upsert                               | The write operation, that this write should do (insert or upsert is supported)                                                                                                                                                                                        |
| write.precombine.field                     | N        | (no default)                         | Field used for ordering records before actual write. When two records have the same key value, we will pick the one with the largest value for the ordering field, determined by Object.compareTo(..). Note: This config is deprecated, use `ordering.fields` instead |
| write.payload.class                        | N        | OverwriteWithLatestAvroPayload.class | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective                                                                                                     |
| write.insert.drop.duplicates               | N        | false                                | Flag to indicate whether to drop duplicates upon insert. By default insert will accept duplicates, to gain extra performance                                                                                                                                          |
| write.ignore.failed                        | N        | true                                 | Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. By default true (in favor of streaming progressing over data integrity)                                                                               |
| hoodie.datasource.write.recordkey.field    | N        | uuid                                 | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c`                                           |
| hoodie.datasource.write.keygenerator.class | N        | SimpleAvroKeyGenerator.class         | Key generator class, that implements will extract the key out of incoming record                                                                                                                                                                                      |
| write.tasks                                | N        | 4                                    | Parallelism of tasks that do actual write, default is 4                                                                                                                                                                                                               |
| write.batch.size.MB                        | N        | 128                                  | Batch buffer size in MB to flush data into the underneath filesystem                                                                                                                                                                                                  |

If the table type is MERGE_ON_READ, you can also specify the asynchronous compaction strategy through options:

| Option Name                 | Required | Default     | Remarks                                                                                                                                                                                                                                                                                                                                                                                              |
|-----------------------------|----------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| compaction.async.enabled    | N        | true        | Async Compaction, enabled by default for MOR                                                                                                                                                                                                                                                                                                                                                         |
| compaction.trigger.strategy | N        | num_commits | Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits; 'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction; 'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; 'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied. Default is 'num_commits' |
| compaction.delta_commits    | N        | 5           | Max delta commits needed to trigger compaction, default 5 commits                                                                                                                                                                                                                                                                                                                                    |
| compaction.delta_seconds    | N        | 3600        | Max delta seconds time needed to trigger compaction, default 1 hour                                                                                                                                                                                                                                                                                                                                  |

You can write the data using the SQL `INSERT INTO` statements:
```sql
INSERT INTO hudi_table select ... from ...; 
```

**Note**: INSERT OVERWRITE is not supported yet but already on the roadmap.


### Non-Blocking Concurrency Control (Experimental)

Hudi Flink supports a new non-blocking concurrency control mode, where multiple writer tasks can be executed
concurrently without blocking each other. One can read more about this mode in
the [concurrency control](concurrency_control#model-c-multi-writer) docs. Let us see it in action here.

In the below example, we have two streaming ingestion pipelines that concurrently update the same table. One of the
pipeline is responsible for the compaction and cleaning table services, while the other pipeline is just for data
ingestion.

In order to commit the dataset, the checkpoint needs to be enabled, here is an example configuration for a flink-conf.yaml:
```yaml
-- set the interval as 30 seconds
execution.checkpointing.interval: 30000
state.backend: rocksdb
```

```sql
-- This is a datagen source that can generate records continuously
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
    'path' = '${work_path}/hudi-demo/t1',
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
    'path' = '${work_path}/hudi-demo/t1',
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


### Consistent hashing index (Experimental)

We have introduced the Consistent Hashing Bucket Index since [0.13.0 release](/releases/release-0.13.0#consistent-hashing-index). This is one of three [bucket index](indexes#additional-writer-side-indexes) variants available in Hudi. The consistent hashing bucket index offers dynamic scalability of data buckets for the writer. 
You can find the [RFC](https://github.com/apache/hudi/blob/master/rfc/rfc-42/rfc-42.md) for the design of this feature.
In the 0.13.X release, the Consistent Hashing Index is supported only for Spark engine. And since [release 0.14.0](/releases/release-0.14.0#consistent-hashing-index-support), the index is supported for Flink engine.

To utilize this feature, configure the option `index.type` as `BUCKET` and set `hoodie.index.bucket.engine` to `CONSISTENT_HASHING`.
When enabling the consistent hashing index, it's important to enable clustering scheduling within the writer. During this process, the writer will perform dual writes for both the old and new data buckets while the clustering is pending. Although the dual write does not impact correctness, it is strongly recommended to execute clustering as quickly as possible.

In the below example, we will create a datagen source and do streaming ingestion into Hudi table with consistent bucket index. In order to commit the dataset, the checkpoint needs to be enabled, here is an example configuration for a flink-conf.yaml:
```yaml
-- set the interval as 30 seconds
execution.checkpointing.interval: 30000
state.backend: rocksdb
```

```sql
-- This is a datagen source that can generate records continuously
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

-- Create the hudi table with consistent bucket index
CREATE TABLE t1(
    uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,
    name VARCHAR(10),
    age INT,
    ts TIMESTAMP(3),
    `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
    'connector'='hudi',
    'path' = '${work_path}/hudi-demo/hudiT',
    'table.type' = 'MERGE_ON_READ',
    'index.type' = 'BUCKET',
    'clustering.schedule.enabled'='true',
    'hoodie.index.bucket.engine'='CONSISTENT_HASHING',
    'hoodie.clustering.plan.strategy.class'='org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy',
    'hoodie.clustering.execution.strategy.class'='org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy',
    'hoodie.bucket.index.num.buckets'='8',
    'hoodie.bucket.index.max.num.buckets'='128',
    'hoodie.bucket.index.min.num.buckets'='8',
    'hoodie.bucket.index.split.threshold'='1.5',
    'write.tasks'='2'
);

-- submit the pipelines
insert into t1 select * from sourceT;

select * from t1 limit 20;
```

:::caution
Consistent Hashing Index is supported for Flink engine since [release 0.14.0](/releases/release-0.14.0#consistent-hashing-index-support) and currently there are some limitations to use it as of 0.14.0:

- This index is supported only for MOR table. This limitation also exists even if using Spark engine.
- It does not work with metadata table enabled. This limitation also exists even if using Spark engine.
- Consistent hashing index does not work with bulk-insert using Flink engine yet, please use simple bucket index or Spark engine for bulk-insert pipelines.
- The resize plan which generated by Flink engine only supports merging small file groups, the file splitting is not supported yet.
- The resize plan should be executed through an offline Spark job. Flink engine does not support execute resize plan yet. 
  :::
