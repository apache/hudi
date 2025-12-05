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
and *"upsert"* (with deduplication/merging). If a precombine field is not set, *"insert"* is chosen as the default. For a table with preCombine field set,
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
The `UPDATE` operation requires the specification of a `preCombineField`.
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

### Delete Data

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
SET hoodie.enable.data.skipping=true;
SET hoodie.metadata.record.index.enable=true;
SET hoodie.metadata.enable=true;

DELETE from hudi_table where uuid = 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa';
```

These DML operations give you powerful tools for managing your tables using Spark SQL. 
You can control the behavior of these operations using various configuration options, as explained in the documentation.

## Flink 

Flink SQL also provides several Data Manipulation Language (DML) actions for interacting with Hudi tables. All these operations are already 
showcased in the [Flink Quickstart](flink-quick-start-guide).


