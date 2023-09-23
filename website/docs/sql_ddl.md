---
title: SQL DDL
summary: "In this page, we discuss using SQL DDL commands with Hudi"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page describes support for creating and altering Hudi tables using SQL across various engines. 

## Spark SQL

### Create table 

You can create tables using standard CREATE TABLE syntax, which supports partitioning and passing table properties.

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
  [(col_name data_type [COMMENT col_comment], ...)]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name, ...)]
  [ROW FORMAT row_format]
  [STORED AS file_format]
  [LOCATION path]
  [TBLPROPERTIES (property_name=property_value, ...)]
  [AS select_statement];
```

### Create non-partitioned table

Creating a non-partitioned table is as simple as creating a regular table.

```sql
-- create a Hudi table
create table if not exists hudi_table(
  id int, 
  name string, 
  price double
) using hudi;
```

### Create partitioned table
A partitioned table can be created by adding a `partitioned by` clause. Partitioning helps to organize the data into multiple folders based on 
the partition columns. It can also help speed up queries and index lookups by limiting the amount of data scanned.

```sql
create table if not exists hudi_table_p0 (
id bigint,
name string,
dt string,
hh string  
) using hudi
tblproperties (
  type = 'cow'
) 
partitioned by dt;
```

:::note
You can also create a table partitioned by multiple fields by supplying comma-separated field names. For, e.g., "partitioned by dt, hh"
:::

### Create table with record keys and pre-combine fields

As discussed [here](/docs/quick-start-guide#keys), Hudi tables track each record in the table using a record key. Hudi auto-generated a highly compressed 
key automatically for each new record in the examples so far. If you want to use an existing field as the key,
you can set the `primaryKey` option. Typically, this is also accompanied by configuring a `preCombineField` option
to deal with out-of-order data and potential duplicates in the incoming writes.

:::note
You can choose multiple fields as primary keys for a given table on a need basis. For eg, "primaryKey = 'id,name'", and
this materializes a composite key of the two fields, which can be useful for exploring the table.
:::

Here is an example of creating a table using both options. Typically, a field that denotes the time of the event or
fact, e.g., order creation time, event generation time etc., is used as the _preCombineField_. Hudi ensures that the latest
version of the record is picked up based on this field when queries are run on the table.

```sql
create table if not exists hudi_table1 (
  id int, 
  name string, 
  price double,
  ts bigint
) using hudi
tblproperties (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts' 
);
```

### Create table from an external location
Often, Hudi tables are created from streaming writers like the [Streamer tool](/docs/hoodie_streaming_ingestion#hudi-streamer), which
may later need some SQL statements to run on them. You can create an External table using the `location` statement.

```sql
 create table h_p1 using hudi
 location '/path/to/hudi';
```

:::tip
You don't need to specify the schema and any properties except the partitioned columns if they existed. Hudi can automatically
recognize the schema and configurations.
:::

### Create Table As Select (CTAS)

Hudi supports CTAS(Create table as select) to support initial loads into Hudi tables. To ensure this is done efficiently,
even for large loads, CTAS uses **bulk insert** as the write operation

```sql
# create managed parquet table
create table parquet_mngd using parquet location 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into Hudi table
create table hudi_ctas_cow_pt_tbl2 using hudi location 'file:/tmp/hudi/hudi_tbl/' tblproperties (
  type = 'cow',
  preCombineField = 'ts'
 )
partitioned by (datestr) as select * from parquet_mngd;
```

If you prefer explicitly setting the record keys, you can do so by setting `primaryKey` config in table properties.

```sql
create table hudi_tbl using hudi
tblproperties (type = 'cow', primaryKey = 'id')
partitioned by (dt)
as
select 1 as id, 'a1' as name, 10 as price, 1000 as dt;
```

You can also use CTAS to copy data across external locations

```sql
# create managed parquet table 
create table parquet_mngd using parquet location 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into hudi table
create table hudi_tbl using hudi location 'file:/tmp/hudi/hudi_tbl/' 
tblproperties ( 
  type = 'cow'
 ) 
as select * from parquet_mngd;
```

### Setting Hudi configs 

There are different ways you can pass the configs for a given hudi table. 

#### Using set command
You can use the **set** command to set any of Hudi's write configs. This will apply to operations across the whole spark session.
```sql
set hoodie.insert.shuffle.parallelism = 100;
set hoodie.upsert.shuffle.parallelism = 100;
set hoodie.delete.shuffle.parallelism = 100;
```

#### Using table properties
You can also configure table options when creating a table. This will be applied only for the table and override any SET command values.

```sql
create table if not exists h3(
  id bigint, 
  name string, 
  price double
) using hudi
tblproperties (
  primaryKey = 'id',
  type = 'cow',
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
  type = 'cow',
  hoodie.cleaner.fileversions.retained = '20',
  hoodie.keep.max.commits = '20'
);
```

### Table Properties

Users can set table properties while creating a hudi table. The important table properties are discussed below.

| Parameter Name | Default | Description                                                                                                                                                                                                                                                                                 |
|------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primaryKey | uuid | The primary key field names of the table separated by commas. Same as `hoodie.datasource.write.recordkey.field`. If this config is ignored, hudi will auto-generate primary keys. If explicitly set, primary key generation will honor user configuration.                                  |
| preCombineField |  | The pre-combine field of the table. It is used for resolving the final version of the record among multiple versions. Generally, `event time` or another similar column will be used for ordering purposes. Hudi will be able to handle out-of-order data using the preCombine field value. |
| type       | cow | The table type to create. `type = 'cow'` creates a COPY-ON-WRITE table, while `type = 'mor'` creates a MERGE-ON-READ table. Same as `hoodie.datasource.write.table.type`. More details can be found [here](/docs/table_types)                                                               |

:::note
`primaryKey`, `preCombineField`, and `type` and other properties are case-sensitive. 
:::

### Spark Alter Table

```sql
-- Alter table name
ALTER TABLE oldTableName RENAME TO newTableName

-- Alter table add columns
ALTER TABLE tableIdentifier ADD COLUMNS(colAndType [, colAndType])

-- Alter table column type
ALTER TABLE tableIdentifier CHANGE COLUMN colName colName colType

-- Alter table properties
ALTER TABLE tableIdentifier SET TBLPROPERTIES (key = 'value')
```

Some examples below 
```sql
--rename to:
ALTER TABLE hudi_cow_tbl RENAME TO hudi_cow_tbl2;

--add column:
ALTER TABLE hudi_cow_tbl2 add columns(remark string);

--change column:
ALTER TABLE hudi_cow_tbl2 change column uuid uuid bigint;

--set properties;
alter table hudi_cow_tbl2 set tblproperties (hoodie.keep.max.commits = '10');
```

### Modifying Table Properties
**Syntax**
```sql
-- alter table ... set|unset
ALTER TABLE tableName SET|UNSET tblproperties
```

**Examples**

```sql
ALTER TABLE table SET TBLPROPERTIES ('table_property' = 'property_value')
ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key')
```

### Changing a Table Name
**Syntax**
```sql
-- alter table ... rename
ALTER TABLE tableName RENAME TO newTableName
```

**Examples**

```sql
ALTER TABLE table1 RENAME TO table2
```

Schema evolution can be achieved via `ALTER TABLE` commands. Below shows some basic examples.

:::note
For more detailed examples, please prefer to [schema evolution](/docs/schema_evolution)
:::

### Alter config options
You can also alter the write config for a table by the **ALTER TABLE SET SERDEPROPERTIES**

**Example**
```sql
 alter table h3 set serdeproperties (hoodie.keep.max.commits = '10') 
```

### Show and drop partitions

**Syntax**

```sql
-- Show Partitions
SHOW PARTITIONS tableIdentifier

-- Drop Partition
ALTER TABLE tableIdentifier DROP PARTITION ( partition_col_name = partition_col_val [ , ... ] )
```

**Examples**
```sql
--show partition:
show partitions hudi_cow_pt_tbl;

--drop partition：
alter table hudi_cow_pt_tbl drop partition (dt='2021-12-09', hh='10');
```


### Caveats 

Hudi currently has the following limitations when using Spark SQL, to create/alter tables.

 - `ALTER TABLE ... RENAME TO ...` is not supported when using AWS Glue Data Catalog as hive metastore as Glue itself does
   not support table renames. 
 - A new Hudi table created by Spark SQL will by default set `hoodie.datasource.write.hive_style_partitioning=true`, for ease
     of use. This can be overridden using table properties.
 - ?? Currently,  the result of `show partitions` is based on the filesystem table path. It's not precise when delete the whole partition data or drop certain partition directly.

## Flink

### Create Catalog

The catalog helps to manage the SQL tables, the table can be shared among sessions if the catalog persists the table DDLs.
For `hms` mode, the catalog also supplements the hive syncing options.

**Example**
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

The following is an example of creating a Flink table. [Read the Flink Quick Start](/docs/flink-quick-start-guide) guide for more examples.

```sql 
CREATE TABLE hudi_table2(
  id int, 
  name string, 
  price double
)
WITH (
'connector' = 'hudi',
'path' = 's3://bucket-name/hudi/',
'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, default is COPY_ON_WRITE
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
