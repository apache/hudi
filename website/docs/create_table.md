---
title: Create Table
summary: "In this page, we introduce how to create tables with Hudi."
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## Spark
:::note
Only SparkSQL needs an explicit Create Table command. No Create Table command is required in Spark when using Scala or Python. The first batch of a [Write](https://hudi.apache.org/docs/writing_data) to a table will create the table if it does not exist.
:::

### Create Table Options

Users can set table options while creating a hudi table.

| Parameter Name | Description | (Optional/Required) : Default Value |
|------------|--------|--------|
| primaryKey | The primary key names of the table, multiple fields separated by commas. | (Optional) : `id`|
| type       | The type of table to create ([read more](https://hudi.apache.org/docs/table_types)). <br></br> `cow` = COPY-ON-WRITE, `mor` = MERGE-ON-READ.| (Optional) : `cow` |
| preCombineField | The Pre-Combine field of the table. | (Optional) : `ts`|

To set any custom hudi config(like index type, max parquet size, etc), see the  "Set hudi config section" .

### Table Type
Here is an example of creating a COW table.

```sql
-- create a non-primary key table
create table if not exists hudi_table2(
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'cow'
);
```

### Primary Key
Here is an example of creating COW table with a primary key 'id'.

```sql
-- create a managed cow table
create table if not exists hudi_table0 (
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'cow',
  primaryKey = 'id'
);
```

### PreCombineField
Here is an example of creating an MOR external table. The **preCombineField** option
is used to specify the preCombine field for merge.

```sql
-- create an external mor table
create table if not exists hudi_table1 (
  id int, 
  name string, 
  price double,
  ts bigint
) using hudi
options (
  type = 'mor',
  primaryKey = 'id,name',
  preCombineField = 'ts' 
);
```

### Partitioned Table
Here is an example of creating a COW partitioned table.
```sql
create table if not exists hudi_table_p0 (
id bigint,
name string,
dt string，
hh string  
) using hudi
options (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts'
 ) 
partitioned by (dt, hh);
```

### Create Table for an External Hudi Table
You can create an External table using the `location` statement. If an external location is not specified it is considered a managed table. You can read more about external vs managed tables [here](https://sparkbyexamples.com/apache-hive/difference-between-hive-internal-tables-and-external-tables/).
An external table is useful if you need to read/write to/from a pre-existing hudi table.

```sql
 create table h_p1 using hudi 
 options (
    primaryKey = 'id',
    preCombineField = 'ts'
 )
 partitioned by (dt)
 location '/path/to/hudi';
```

### Create Table AS SELECT

Hudi supports CTAS(Create table as select) on spark sql. <br/>
**Note:** For better performance to load data to hudi table, CTAS uses **bulk insert** as the write operation.

**Example CTAS command to create a non-partitioned COW table.**

```sql 
create table h3 using hudi
as
select 1 as id, 'a1' as name, 10 as price;
```

**Example CTAS command to create a partitioned, primary key COW table.**

```sql
create table h2 using hudi
options (type = 'cow', primaryKey = 'id')
partitioned by (dt)
as
select 1 as id, 'a1' as name, 10 as price, 1000 as dt;
```

**Example CTAS command to load data from another table.**

```sql
# create managed parquet table 
create table parquet_mngd using parquet location 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into hudi table
create table hudi_tbl using hudi location 'file:/tmp/hudi/hudi_tbl/' options ( 
  type = 'cow', 
  primaryKey = 'id', 
  preCombineField = 'ts' 
 ) 
partitioned by (datestr) as select * from parquet_mngd;
```

## Flink