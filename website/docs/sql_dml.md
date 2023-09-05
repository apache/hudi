---
title: SQL DDL
summary: "In this page, we introduce how to create tables with Hudi."
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The following are SparkSQL DDL actions available:

## Spark Create Table
:::note
Only SparkSQL needs an explicit Create Table command. No Create Table command is required in Spark when using Scala or 
Python. The first batch of a [Write](/docs/writing_data) to a table will create the table if it does not exist.
:::

### Options

Users can set table options while creating a hudi table.

| Parameter Name | Description                                                                                                                                                                                                        | (Optional/Required) : Default Value |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| primaryKey | The primary key names of the table, multiple fields separated by commas. When set, hudi will ensure uniqueness during updates and deletes. When this config is skipped, hudi treats the table as a key less table. | (Optional) : `id`|
| type       | The type of table to create ([read more](/docs/table_types)). <br></br> `cow` = COPY-ON-WRITE, `mor` = MERGE-ON-READ.                                                                                              | (Optional) : `cow` |
| preCombineField | The Pre-Combine field of the table. This field will be used in resolving the final version of the record when two versions are combined with merges or updates.                                                    | (Optional) : `ts`|

To set any custom hudi config(like index type, max parquet size, etc), see the section [Set hudi config options](#set-hoodie-config-options) .

### Table Type
Here is an example of creating a COW table.

```sql
-- create a non-primary key (or key less) table
create table if not exists hudi_table2(
  id int, 
  name string, 
  price double
) using hudi
tblproperties (
  type = 'cow'
);
```

There could be datasets where primary key may not be feasible. For such use-cases, user don't need to elect any primary key for 
the table and hudi will treat them as key less table. Users can still perform Merge Into, updates and deletes based on any random data column. 

### Primary Key
Here is an example of creating COW table with a primary key 'id'. For mutable datasets, it is recommended to set appropriate primary key. 

```sql
-- create a managed cow table
create table if not exists hudi_table0 (
  id int, 
  name string, 
  price double
) using hudi
tblproperties (
  type = 'cow',
  primaryKey = 'id'
);
```

### PreCombineField
Here is an example of creating an MOR external table. The **preCombineField** option
is used to specify the preCombine field for merge. Generally 'event time' or some other similar column will be used for 
ordering purpose. Hudi will be able to handle out of order data using the precombine field value.

```sql
-- create an external mor table
create table if not exists hudi_table1 (
  id int, 
  name string, 
  price double,
  ts bigint
) using hudi
tblproperties (
  type = 'mor',
  primaryKey = 'id,name',
  preCombineField = 'ts' 
);
```

### Partitioned Table
:::note
When created in spark-sql, partition columns will always be the last columns of the table. 
:::
Here is an example of creating a COW partitioned key less table.
```sql
create table if not exists hudi_table_p0 (
id bigint,
name string,
dt string,
hh string  
) using hudi
tblproperties (
  type = 'cow',
  primaryKey = 'id'
 ) 
partitioned by dt;
```

Here is an example of creating a MOR partitioned table with preCombine field.
```sql
create table if not exists hudi_table_p0 (
id bigint,
name string,
dt string,
hh string  
) using hudi
tblproperties (
  type = 'mor',
  primaryKey = 'id',
  preCombineField = 'ts'
 ) 
partitioned by dt;
```

### Un-Partitioned Table
Here is an example of creating a COW un-partitioned table.

```sql
-- create a cow table, with primaryKey 'uuid' and unpartitioned. 
create table hudi_cow_nonpcf_tbl (
  uuid int,
  name string,
  price double
) using hudi
tblproperties (
  primaryKey = 'uuid'
);
```

Here is an example of creating a MOR un-partitioned table.

```sql
-- create a mor non-partitioned table with preCombineField provided
create table hudi_mor_tbl (
    id int,
    name string,
    price double,
    ts bigint
) using hudi
tblproperties (
    type = 'mor',
    primaryKey = 'id',
    preCombineField = 'ts'
);
```

### Create Table for an External Hudi Table
You can create an External table using the `location` statement. If an external location is not specified it is considered a managed table. 
You can read more about external vs managed tables [here](https://sparkbyexamples.com/apache-hive/difference-between-hive-internal-tables-and-external-tables/).
An external table is useful if you need to read/write to/from a pre-existing hudi table.

```sql
 create table h_p1 using hudi
 location '/path/to/hudi';
```

:::tip
You don't need to specify schema and any properties except the partitioned columns if existed. Hudi can automatically recognize the schema and configurations.
:::

### Create Table AS SELECT

Hudi supports CTAS(Create table as select) on spark sql. <br/>
**Note:** For better performance to load data to hudi table, CTAS uses **bulk insert** as the write operation.

**Example CTAS command to create a non-partitioned COW key less table.**

```sql 
create table h3 using hudi
tblproperties (type = 'cow')
as
select 1 as id, 'a1' as name, 10 as price;
```

**Example CTAS command to create a partitioned, primary keyed COW table.**

```sql
create table h2 using hudi
tblproperties (type = 'cow', primaryKey = 'id')
partitioned by (dt)
as
select 1 as id, 'a1' as name, 10 as price, 1000 as dt;
```

**Example CTAS command to load data from another table.**

```sql
# create managed parquet table 
create table parquet_mngd using parquet location 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into hudi table
create table hudi_tbl using hudi location 'file:/tmp/hudi/hudi_tbl/' tblproperties ( 
  type = 'cow', 
  primaryKey = 'id', 
  preCombineField = 'ts' 
 ) 
partitioned by (datestr) as select * from parquet_mngd;
```

### Set hoodie config options
You can also set the config with table options when creating table which will work for
the table scope only and override the config set by the SET command.
```sql
create table if not exists h3(
  id bigint, 
  name string, 
  price double
) using hudi
tblproperties (
  primaryKey = 'id',
  type = 'mor',
  ${hoodie.config.key1} = '${hoodie.config.value2}',
  ${hoodie.config.key2} = '${hoodie.config.value2}',
  ....
);

e.g.
create table if not exists h3(
  id bigint, 
  name string, 
  price double
) using hudi
tblproperties (
  primaryKey = 'id',
  type = 'mor',
  hoodie.cleaner.fileversions.retained = '20',
  hoodie.keep.max.commits = '20'
);
```

## Spark Alter Table
### Syntax
```sql
-- Alter table name
ALTER TABLE oldTableName RENAME TO newTableName

-- Alter table add columns
ALTER TABLE tableIdentifier ADD COLUMNS(colAndType (,colAndType)*)

-- Alter table column type
ALTER TABLE tableIdentifier CHANGE COLUMN colName colName colType
```

:::note
`ALTER TABLE ... RENAME TO ...` is not supported when using AWS Glue Data Catalog as hive metastore as Glue itself does 
not support table renames.
:::

### Examples
```sql
alter table h0 rename to h0_1;

alter table h0_1 add columns(ext0 string);

alter table h0_1 change column id id bigint;
```
### Alter hoodie config options
You can also alter the write config for a table by the **ALTER SERDEPROPERTIES**

Example:
```sql
 alter table h3 set serdeproperties (hoodie.keep.max.commits = '10') 
```

### Use set command
You can use the **set** command to set any custom hudi's config, which will work for the
whole spark session scope.
```sql
set hoodie.insert.shuffle.parallelism = 100;
set hoodie.upsert.shuffle.parallelism = 100;
set hoodie.delete.shuffle.parallelism = 100;
```

## Flink

### Create Catalog

The catalog helps to manage the SQL tables, the table can be shared among CLI sessions if the catalog persists the table DDLs.
For `hms` mode, the catalog also supplements the hive syncing options.

HMS mode catalog SQL demo:
```sql
CREATE CATALOG hoodie_catalog
  WITH (
    'type'='hudi',
    'catalog.path' = '${catalog default root path}',
    'hive.conf.dir' = '${directory where hive-site.xml is located}',
    'mode'='hms' -- supports 'dfs' mode that uses the DFS backend for table DDLs persistence
  );
```

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `catalog.path` | true | -- | Default root path for the catalog, the path is used to infer the table path automatically, the default table path: `${catalog.path}/${db_name}/${table_name}` |
| `default-database` | false | default | default database name |
| `hive.conf.dir` | false | -- | The directory where hive-site.xml is located, only valid in `hms` mode |
| `mode` | false | dfs | Supports `hms` mode that uses HMS to persist the table options |
| `table.external` | false | false | Whether to create the external table, only valid in `hms` mode |

### Create Table

The following is a Flink example to create a table. [Read the Flink Quick Start](/docs/flink-quick-start-guide) guide for more examples.

```sql 
CREATE TABLE hudi_table2(
  id int, 
  name string, 
  price double
)
WITH (
'connector' = 'hudi',
'path' = 's3://bucket-name/hudi/',
'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
);
```

### Alter Table
```sql
alter table h0 rename to h0_1;
```

## Supported Types

| Spark           |     Hudi     |     Notes     |
|-----------------|--------------|---------------|
| boolean         |  boolean     |               |
| byte            |  int         |               |
| short           |  int         |               |
| integer         |  int         |               |
| long            |  long        |               |
| date            |  date        |               |
| timestamp       |  timestamp   |               |
| float           |  float       |               |
| double          |  double      |               |
| string          |  string      |               |
| decimal         |  decimal     |               |
| binary          |  bytes       |               |
| array           |  array       |               |
| map             |  map         |               |
| struct          |  struct      |               |
| char            |              | not supported |
| varchar         |              | not supported |
| numeric         |              | not supported |
| null            |              | not supported |
| object          |              | not supported |
