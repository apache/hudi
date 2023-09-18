---
title: SQL DML
summary: "In this page, we go will cover details on how to modify data with Hudi tables"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Spark DML Operations

The following are SparkSQL DML actions available:

- Insert Into
- Merge Into
- Update
- Delete

You can ingest data, modify or delete data from a given hudi table using these operations. Lets go over them one by one. 

Please refer to [SQL DDL](/docs/next/sql_ddl) for creating Hudi tables using Spark SQL. This page goes over ingesting and modifying data once the
table is created. 

## Insert Into

Users can use 'INSERT INTO' to insert data into a hudi table using spark-sql.

```sql
insert into hudi_cow_nonpcf_tbl select 1, 'a1', 20;
insert into hudi_mor_tbl select 1, 'a1', 20, 1000;

-- insert static partition
insert into hudi_cow_pt_tbl partition(dt = '2021-12-09', hh='11') select 2, 'a2', 1000;

-- insert dynamic partition
insert into hudi_cow_pt_tbl partition (dt, hh)
select 1 as id, 'a1' as name, 1000 as ts, '2021-12-09' as dt, '10' as hh;

```

:::note
- `hoodie.spark.sql.insert.into.operation` will determine how records ingested via spark-sql INSERT INTO will be treated. Possible values are "bulk_insert", "insert"
  and "upsert". If "bulk_insert" is chosen, hudi writes incoming records as is without any automatic small file management.
  When "insert" is chosen, hudi inserts the new incoming records and also does small file management.
  When "upsert" is used, hudi takes upsert flow, where incoming batch will be de-duped before ingest and also merged with previous versions of the record in storage.
  For a table without any precombine key set, "insert" is chosen as the default value for this config. For a table with precombine key set,
  "upsert" is chosen as the default value for this config.
- From 0.14.0, `hoodie.sql.bulk.insert.enable` and `hoodie.sql.insert.mode` are depecrated. Users are expected to use `hoodie.spark.sql.insert.into.operation` instead.
- When operation type is set to "insert", users can optionally enforce a dedup policy using `hoodie.datasource.insert.dup.policy`. 
This policy will be employed when records being ingested already exists in storage. Default policy is none and no action will be taken. 
Another option is to choose "drop", on which matching records from incoming will be dropped and the rest will be ingested. 
Third option is "fail" which will fail the write operation when same records are re-ingested. In other words, a given record 
as deduced by the key generation policy can be ingested only once to the target table of interest.
:::

Here are examples to override the spark.sql.insert.into.operation.

```sql
-- upserts using INSERT_INTO 
set hoodie.spark.sql.insert.into.operation = 'upsert' 

insert into hudi_mor_tbl select 1, 'a1_1', 20, 1001;
select id, name, price, ts from hudi_mor_tbl;
1	a1_1	20.0	1001

-- bulk_insert using INSERT_INTO 
set hoodie.spark.sql.insert.into.operation = 'bulk_insert' 

insert into hudi_mor_tbl select 1, 'a1_2', 20, 1002;
select id, name, price, ts from hudi_mor_tbl;
1	a1_1	20.0	1001
1	a1_2	20.0	1002
```

### Insert overwrite 

Users can execute `insert_overwrite` on a given hudi table. Matching partition records on storage will be overridden with those 
incoming.

```sql
-- insert overwrite non-partitioned table
insert overwrite hudi_mor_tbl select 99, 'a99', 20.0, 900;
insert overwrite hudi_cow_nonpcf_tbl select 99, 'a99', 20.0;

-- insert overwrite partitioned table with dynamic partition
insert overwrite table hudi_cow_pt_tbl select 10, 'a10', 1100, '2021-12-09', '10';

-- insert overwrite partitioned table with static partition
insert overwrite hudi_cow_pt_tbl partition(dt = '2021-12-09', hh='12') select 13, 'a13', 1100;

```

## Update data

Spark SQL supports two kinds of DML to update hudi table: Update and Merge-Into.

### Update

**Syntax**
```sql
UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]
```
**Case**
```sql
update hudi_mor_tbl set price = price * 2, ts = 1111 where id = 1;

update hudi_cow_pt_tbl set name = 'a1_1', ts = 1001 where id = 1;

-- update using non-PK field
update hudi_cow_pt_tbl set ts = 1001 where name = 'a1';
```
:::note
`Update` operation requires `preCombineField` specified.
:::

### MergeInto

**Syntax**

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
**Example**
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

merge into hudi_cow_pt_tbl as target
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

:::note
For a Hudi table with user configured primary keys, the join condition in `Merge Into` is expected to contain the primary keys of the table.
For a Table where Hudi auto generates primary keys, the join condition in MIT can be on any arbitrary data columns.
:::


## Delete data

**Syntax**
```sql
DELETE FROM tableIdentifier [ WHERE BOOL_EXPRESSION]
```
**Example**
```sql
DELETE FROM hudi_cow_nonpcf_tbl where uuid = 1;

DELETE FROM hudi_mor_tbl where id % 2 = 0;

-- delete using non-PK field
DELETE FROM hudi_cow_pt_tbl where name = 'a1';
```
