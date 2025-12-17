---
title: SQL DDL
summary: "In this page, we discuss using SQL DDL commands with Hudi"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page describes support for creating and altering tables using SQL across various engines. 

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

:::note NOTE:
For users running this tutorial locally and have a Spark-Hive(HMS) integration in their environment: If you use 
`default` database or if you don't provide `[LOCATION path]` with the DDL statement, Spark will return 
`java.io.IOException: Mkdirs failed to create file:/user/hive/warehouse/hudi_table/.hoodie` error. 
To get around this, you can follow either of the two options mentioned below:
1. Create a database i.e. `CREATE DATABASE hudidb;` and use it i.e. `USE hudidb;` before running the DDL statement.
2. Or provide a path using `LOCATION` keyword to persist the data with the DDL statement.
:::

### Create non-partitioned table

Creating a non-partitioned table is as simple as creating a regular table.

```sql
-- create a Hudi table
CREATE TABLE IF NOT EXISTS hudi_table (
  id INT,
  name STRING,
  price DOUBLE
) USING hudi;
```

### Create partitioned table
A partitioned table can be created by adding a `partitioned by` clause. Partitioning helps to organize the data into multiple folders 
based on the partition columns. It can also help speed up queries and index lookups by limiting the amount of metadata, index and data scanned.

```sql
CREATE TABLE IF NOT EXISTS hudi_table_partitioned (
  id BIGINT,
  name STRING,
  dt STRING,
  hh STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow'
)
PARTITIONED BY (dt);
```

:::note
You can also create a table partitioned by multiple fields by supplying comma-separated field names.
When creating a table partitioned by multiple fields, ensure that you specify the columns in the `PARTITIONED BY` clause
in the same order as they appear in the `CREATE TABLE` schema. For example, for the above table, the partition fields
should be specified as `PARTITIONED BY (dt, hh)`.
:::

### Create table with record keys and ordering fields

As discussed [here](quick-start-guide.md#keys), tables track each record in the table using a record key. Hudi auto-generated a highly compressed 
key for each new record in the examples so far. If you want to use an existing field as the key, you can set the `primaryKey` option. 
Typically, this is also accompanied by configuring a `preCombineField` option to deal with out-of-order data and potential 
duplicate records with the same key in the incoming writes.

:::note
You can choose multiple fields as primary keys for a given table on a need basis. For eg, "primaryKey = 'id, name'", and
this materializes a composite key of the two fields, which can be useful for exploring the table.
:::

Here is an example of creating a table using both options. Typically, a field that denotes the time of the event or
fact, e.g., order creation time, event generation time etc., is used as the _preCombineField_. Hudi resolves multiple versions
of the same record by ordering based on this field when queries are run on the table.

```sql
CREATE TABLE IF NOT EXISTS hudi_table_keyed (
  id INT,
  name STRING,
  price DOUBLE,
  ts BIGINT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts'
);
```

### Create table from an external location
Often, Hudi tables are created from streaming writers like the [streamer tool](hoodie_streaming_ingestion.md#hudi-streamer), which
may later need some SQL statements to run on them. You can create an External table using the `location` statement.

```sql
CREATE TABLE hudi_table_external
USING hudi
LOCATION 'file:///tmp/hudi_table/';
```

:::tip
You don't need to specify the schema and any properties except the partitioned columns if they exist. Hudi can automatically
recognize the schema and configurations.
:::

### Create Table As Select (CTAS)

Hudi supports CTAS(Create table as select) to support initial loads into Hudi tables. To ensure this is done efficiently,
even for large loads, CTAS uses **bulk insert** as the write operation

```sql
# create managed parquet table
CREATE TABLE parquet_table
USING parquet
LOCATION 'file:///tmp/parquet_dataset/';

# CTAS by loading data into Hudi table
CREATE TABLE hudi_table_ctas
USING hudi
TBLPROPERTIES (
  type = 'cow',
  preCombineField = 'ts'
)
PARTITIONED BY (dt)
AS SELECT * FROM parquet_table;
```

You can create a non-partitioned table as well

```sql
# create managed parquet table
CREATE TABLE parquet_table
USING parquet
LOCATION 'file:///tmp/parquet_dataset/';

# CTAS by loading data into Hudi table
CREATE TABLE hudi_table_ctas
USING hudi
TBLPROPERTIES (
  type = 'cow',
  preCombineField = 'ts'
)
AS SELECT * FROM parquet_table;
```

If you prefer explicitly setting the record keys, you can do so by setting `primaryKey` config in table properties.

```sql
CREATE TABLE hudi_table_ctas
USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id'
)
PARTITIONED BY (dt)
AS
SELECT 1 AS id, 'a1' AS name, 10 AS price, 1000 AS dt;
```

You can also use CTAS to copy data across external locations

```sql
# create managed parquet table
CREATE TABLE parquet_table
USING parquet
LOCATION 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into hudi table
CREATE TABLE hudi_table_ctas
USING hudi
LOCATION 'file:///tmp/hudi/hudi_tbl/'
TBLPROPERTIES (
  type = 'cow'
)
AS SELECT * FROM parquet_table;
```

### Create Index (Experimental)

Hudi supports creating and dropping indexes, including functional indexes, on a table.

:::note
Creating indexes through SQL is in preview in version 1.0.0-beta only. It will be generally available in version 1.0.0.
Please report any issues you find either via [GitHub issues](https://github.com/apache/hudi/issues) or creating a [JIRA](https://issues.apache.org/jira/projects/HUDI/issues).
:::

```sql
-- Create Index
CREATE INDEX [IF NOT EXISTS] index_name ON [TABLE] table_name 
[USING index_type] 
(column_name1 [OPTIONS(key1=value1, key2=value2, ...)], column_name2 [OPTIONS(key1=value1, key2=value2, ...)], ...) 
[OPTIONS (key1=value1, key2=value2, ...)]

-- Drop Index
DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
```

- `index_name` is the name of the index to be created or dropped.
- `table_name` is the name of the table on which the index is created or dropped.
- `index_type` is the type of the index to be created. Currently, only `files`, `column_stats` and `bloom_filters` is supported.
- `column_name` is the name of the column on which the index is created.
- Both index and column on which the index is created can be qualified with some options in the form of key-value pairs.
  We will see this with an example of functional index below. 

#### Create Functional Index

A [functional index](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/rfc/rfc-63/rfc-63.md) 
is an index on a function of a column. It is a new addition to Hudi's [multi-modal indexing](https://hudi.apache.org/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi) 
subsystem which provides faster access method and also absorb partitioning as part of the indexing system. Let us see 
some examples of creating a functional index.

```sql
-- Create a functional index on the column `ts` (unix epoch) of the table `hudi_table` using the function `from_unixtime` with the format `yyyy-MM-dd`
CREATE INDEX IF NOT EXISTS ts_datestr ON hudi_table USING column_stats(ts) OPTIONS(func='from_unixtime', format='yyyy-MM-dd');
-- Create a functional index on the column `ts` (timestamp in yyyy-MM-dd HH:mm:ss) of the table `hudi_table` using the function `hour`
CREATE INDEX ts_hour ON hudi_table USING column_stats(ts) options(func='hour');
```

Few things to note:
- The `func` option is required for creating functional index, and it should be a valid Spark SQL function. Currently, 
  only the functions that take a single column as input are supported. Some useful functions that are supported are listed below.
  - `identity`
  - `from_unixtime`
  - `date_format`
  - `to_date`
  - `to_timestamp`
  - `year`
  - `month`
  - `day`
  - `hour`
  - `lower`
  - `upper`
  - `substring`
  - `regexp_extract`
  - `regexp_replace`
  - `concat`
  - `length`
- Please check the syntax for the above functions in
  the [Spark SQL documentation](https://spark.apache.org/docs/latest/sql-ref-functions.html) and provide the options
  accordingly. For example, the `format` option is required for `from_unixtime` function.
- UDFs are not supported.

<details>
  <summary>Example of creating and using functional index</summary>

```sql
-- create a Hudi table
CREATE TABLE hudi_table_func_index (
    ts STRING,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
tblproperties (primaryKey = 'uuid')
PARTITIONED BY (city)
location 'file:///tmp/hudi_table_func_index';

-- disable small file handling so the each insert creates new file --
set hoodie.parquet.small.file.limit=0;

INSERT INTO hudi_table_func_index VALUES ('2023-09-20 03:58:59','334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco');
INSERT INTO hudi_table_func_index VALUES ('2023-09-19 08:46:34','e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco');
INSERT INTO hudi_table_func_index VALUES ('2023-09-18 17:45:31','9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco');
INSERT INTO hudi_table_func_index VALUES ('2023-09-22 13:12:56','1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco');
INSERT INTO hudi_table_func_index VALUES ('2023-09-24 06:15:45','e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo');
INSERT INTO hudi_table_func_index VALUES ('2023-09-22 15:21:36','7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo');
INSERT INTO hudi_table_func_index VALUES ('2023-09-20 12:35:45','3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai');
INSERT INTO hudi_table_func_index VALUES ('2023-09-19 05:34:56','c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');

-- Query with hour function filter but no idex yet --
spark-sql> SELECT city, fare, rider, driver FROM  hudi_table_func_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
san_francisco	93.5	rider-E	driver-O
san_francisco	33.9	rider-D	driver-L
sao_paulo	43.4	rider-G	driver-Q
Time taken: 0.208 seconds, Fetched 3 row(s)

spark-sql> EXPLAIN COST SELECT city, fare, rider, driver FROM  hudi_table_func_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
== Optimized Logical Plan ==
Project [city#3465, fare#3464, rider#3462, driver#3463], Statistics(sizeInBytes=899.5 KiB)
+- Filter ((isnotnull(city#3465) AND isnotnull(ts#3460)) AND (NOT (city#3465 = chennai) AND (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12))), Statistics(sizeInBytes=2.5 MiB)
   +- Relation default.hudi_table_func_index[_hoodie_commit_time#3455,_hoodie_commit_seqno#3456,_hoodie_record_key#3457,_hoodie_partition_path#3458,_hoodie_file_name#3459,ts#3460,uuid#3461,rider#3462,driver#3463,fare#3464,city#3465] parquet, Statistics(sizeInBytes=2.5 MiB)

== Physical Plan ==
*(1) Project [city#3465, fare#3464, rider#3462, driver#3463]
+- *(1) Filter (isnotnull(ts#3460) AND (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12))
   +- *(1) ColumnarToRow
      +- FileScan parquet default.hudi_table_func_index[ts#3460,rider#3462,driver#3463,fare#3464,city#3465] Batched: true, DataFilters: [isnotnull(ts#3460), (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12)], Format: Parquet, Location: HoodieFileIndex(1 paths)[file:/tmp/hudi_table_func_index], PartitionFilters: [isnotnull(city#3465), NOT (city#3465 = chennai)], PushedFilters: [IsNotNull(ts)], ReadSchema: struct<ts:string,rider:string,driver:string,fare:double>
      
     
-- create the functional index --
CREATE INDEX ts_hour ON hudi_table_func_index USING column_stats(ts) options(func='hour');

-- query after creating the index --
spark-sql> SELECT city, fare, rider, driver FROM  hudi_table_func_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
san_francisco	93.5	rider-E	driver-O
san_francisco	33.9	rider-D	driver-L
sao_paulo	43.4	rider-G	driver-Q
Time taken: 0.202 seconds, Fetched 3 row(s)
spark-sql> EXPLAIN COST SELECT city, fare, rider, driver FROM  hudi_table_func_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
== Optimized Logical Plan ==
Project [city#2970, fare#2969, rider#2967, driver#2968], Statistics(sizeInBytes=449.8 KiB)
+- Filter ((isnotnull(city#2970) AND isnotnull(ts#2965)) AND (NOT (city#2970 = chennai) AND (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12))), Statistics(sizeInBytes=1278.3 KiB)
   +- Relation default.hudi_table_func_index[_hoodie_commit_time#2960,_hoodie_commit_seqno#2961,_hoodie_record_key#2962,_hoodie_partition_path#2963,_hoodie_file_name#2964,ts#2965,uuid#2966,rider#2967,driver#2968,fare#2969,city#2970] parquet, Statistics(sizeInBytes=1278.3 KiB)

== Physical Plan ==
*(1) Project [city#2970, fare#2969, rider#2967, driver#2968]
+- *(1) Filter (isnotnull(ts#2965) AND (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12))
   +- *(1) ColumnarToRow
      +- FileScan parquet default.hudi_table_func_index[ts#2965,rider#2967,driver#2968,fare#2969,city#2970] Batched: true, DataFilters: [isnotnull(ts#2965), (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12)], Format: Parquet, Location: HoodieFileIndex(1 paths)[file:/tmp/hudi_table_func_index], PartitionFilters: [isnotnull(city#2970), NOT (city#2970 = chennai)], PushedFilters: [IsNotNull(ts)], ReadSchema: struct<ts:string,rider:string,driver:string,fare:double>
      
```
</details>

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
CREATE TABLE IF NOT EXISTS tableName (
  colName1 colType1,
  colName2 colType2,
  ...
) USING hudi
TBLPROPERTIES (
  primaryKey = '${colName1}',
  type = 'cow',
  ${hoodie.config.key1} = '${hoodie.config.value1}',
  ${hoodie.config.key2} = '${hoodie.config.value2}',
  ....
);

e.g.
CREATE TABLE IF NOT EXISTS hudi_table (
  id BIGINT,
  name STRING,
  price DOUBLE
) USING hudi
TBLPROPERTIES (
  primaryKey = 'id',
  type = 'cow',
  hoodie.cleaner.fileversions.retained = '20',
  hoodie.keep.max.commits = '20'
);
```

### Table Properties

Users can set table properties while creating a table. The important table properties are discussed below.

| Parameter Name | Default | Description                                                                                                                                                                                                                                                                                 |
|------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type       | cow | The table type to create. `type = 'cow'` creates a COPY-ON-WRITE table, while `type = 'mor'` creates a MERGE-ON-READ table. Same as `hoodie.datasource.write.table.type`. More details can be found [here](table_types.md)                                                               |
| primaryKey | uuid | The primary key field names of the table separated by commas. Same as `hoodie.datasource.write.recordkey.field`. If this config is ignored, hudi will auto-generate primary keys. If explicitly set, primary key generation will honor user configuration.                                  |
| preCombineField |  | The pre-combine field of the table. It is used for resolving the final version of the record among multiple versions. Generally, `event time` or another similar column will be used for ordering purposes. Hudi will be able to handle out-of-order data using the preCombine field value. |

:::note
`primaryKey`, `preCombineField`, and `type` and other properties are case-sensitive. 
:::

#### Passing Lock Providers for Concurrent Writers

Hudi requires a lock provider to support concurrent writers or asynchronous table services when using OCC
and [NBCC](concurrency_control.md#non-blocking-concurrency-control) (Non-Blocking Concurrency Control)
concurrency mode. For NBCC mode, locking is only used to write the commit metadata file in the timeline. Writes are
serialized by completion time. Users can pass these table properties into *TBLPROPERTIES* as well. Below is an example
for a Zookeeper based configuration.

```sql
-- Properties to use Lock configurations to support Multi Writers
TBLPROPERTIES(
  hoodie.write.lock.zookeeper.url = "zookeeper",
  hoodie.write.lock.zookeeper.port = "2181",
  hoodie.write.lock.zookeeper.lock_key = "tableName",
  hoodie.write.lock.provider = "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider",
  hoodie.write.concurrency.mode = "optimistic_concurrency_control",
  hoodie.write.lock.zookeeper.base_path = "/tableName"
)
```

#### Enabling Column Stats / Record Level Index for the table
Hudi provides the ability to leverage rich metadata and index about the table, speed up DMLs and queries. 
For e.g: collection of column statistics can be enabled to perform quick data skipping or a record-level index can be used to perform fast updates or point lookups 
using the following table properties.


For more, see <a href="/docs/configurations/#Metadata-Configs">Metadata Configurations</a>

```sql
TBLPROPERTIES(
  'hoodie.metadata.index.column.stats.enable' = 'true'
  'hoodie.metadata.record.index.enable' = 'true' 
)
```

### Spark Alter Table
**Syntax**
```sql
-- Alter table name
ALTER TABLE oldTableName RENAME TO newTableName;

-- Alter table add columns
ALTER TABLE tableIdentifier ADD COLUMNS(colAndType [, colAndType]);
```
**Examples**

```sql
--rename to:
ALTER TABLE hudi_table RENAME TO hudi_table_renamed;

--add column:
ALTER TABLE hudi_table ADD COLUMNS(remark STRING);
```

### Modifying Table Properties
**Syntax**
```sql
-- alter table ... set|unset
ALTER TABLE tableIdentifier SET|UNSET TBLPROPERTIES (table_property = 'property_value');
```

**Examples**

```sql
ALTER TABLE hudi_table SET TBLPROPERTIES (hoodie.keep.max.commits = '10');
ALTER TABLE hudi_table SET TBLPROPERTIES ("note" = "don't drop this table");

ALTER TABLE hudi_table UNSET TBLPROPERTIES IF EXISTS (hoodie.keep.max.commits);
ALTER TABLE hudi_table UNSET TBLPROPERTIES IF EXISTS ('note');
```

:::note
Currently, trying to change the column type may throw an error ```ALTER TABLE CHANGE COLUMN is not supported for changing column colName with oldColType to colName with newColType.```, due to an [open SPARK issue](https://issues.apache.org/jira/browse/SPARK-21823)
:::

### Alter config options
You can also alter the write config for a table by the **ALTER TABLE SET SERDEPROPERTIES**

**Syntax**

```sql
-- alter table ... set|unset
ALTER TABLE tableName SET SERDEPROPERTIES ('property' = 'property_value');
```

**Example**
```sql
 ALTER TABLE hudi_table SET SERDEPROPERTIES ('key1' = 'value1');
```

### Show and drop partitions

**Syntax**

```sql
-- Show partitions
SHOW PARTITIONS tableIdentifier;

-- Drop partition
ALTER TABLE tableIdentifier DROP PARTITION ( partition_col_name = partition_col_val [ , ... ] );
```

**Examples**
```sql
--Show partition:
SHOW PARTITIONS hudi_table;

--Drop partition：
ALTER TABLE hudi_table DROP PARTITION (dt='2021-12-09', hh='10');
```

### Caveats 

Hudi currently has the following limitations when using Spark SQL, to create/alter tables.

 - `ALTER TABLE ... RENAME TO ...` is not supported when using AWS Glue Data Catalog as hive metastore as Glue itself does
   not support table renames. 
 - A new Hudi table created by Spark SQL will by default set `hoodie.datasource.write.hive_style_partitioning=true`, for ease
     of use. This can be overridden using table properties.

## Flink SQL

### Create Catalog
The catalog helps to manage the SQL tables, the table can be shared among sessions if the catalog persists the table definitions.
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
|  Option Name  | Required | Default | Remarks                                                                                                                                                                  |
|  -----------  | -------  | ------- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `catalog.path` | true | -- | Default path for the catalog's table storage, the path is used to infer the table path automatically, the default table path: `${catalog.path}/${db_name}/${table_name}` |
| `default-database` | false | default | default database name                                                                                                                                                    |
| `hive.conf.dir` | false | -- | The directory where hive-site.xml is located, only valid in `hms` mode                                                                                                   |
| `mode` | false | dfs | Supports `hms` mode that uses HMS to persist the table options                                                                                                           |
| `table.external` | false | false | Whether to create the external table, only valid in `hms` mode                                                                                                           |

### Create Table

You can create tables using standard FLINK SQL CREATE TABLE syntax, which supports partitioning and passing Flink options using WITH.

```sql
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> 
    [ <table_constraint> ][ , ...n]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
```

### Create non-partitioned table

Creating a non-partitioned table is as simple as creating a regular table.

```sql
-- create a Hudi table
CREATE TABLE hudi_table(
  id BIGINT,
  name STRING,
  price DOUBLE
)
WITH (
'connector' = 'hudi',
'path' = 'file:///tmp/hudi_table',
'table.type' = 'MERGE_ON_READ'
);
```

### Create partitioned table

The following is an example of creating a Flink partitioned table.

```sql 
CREATE TABLE hudi_table(
  id BIGINT,
  name STRING,
  dt STRING,
  hh STRING
)
PARTITIONED BY (`dt`)
WITH (
'connector' = 'hudi',
'path' = 'file:///tmp/hudi_table',
'table.type' = 'MERGE_ON_READ'
);
```

### Create table with record keys and ordering fields

The following is an example of creating a Flink table with record key and ordering field similarly to spark.

```sql
CREATE TABLE hudi_table(
  id BIGINT PRIMARY KEY NOT ENFORCED,
  name STRING,
  price DOUBLE,
  ts BIGINT
)
PARTITIONED BY (`dt`)
WITH (
'connector' = 'hudi',
'path' = 'file:///tmp/hudi_table',
'table.type' = 'MERGE_ON_READ',
'precombine.field' = 'ts'
);
```

### Alter Table
```sql
ALTER TABLE tableA RENAME TO tableB;
```

### Setting Hudi configs

#### Using table options
You can configure hoodie configs in table options when creating a table. You can refer Flink specific hoodie configs [here](configurations.md#FLINK_SQL)
These configs will be applied to all the operations on that table.

```sql
CREATE TABLE IF NOT EXISTS tableName (
  colName1 colType1 PRIMARY KEY NOT ENFORCED,
  colName2 colType2,
  ...
)
WITH (
  'connector' = 'hudi',
  'path' = '${path}',
  ${hoodie.config.key1} = '${hoodie.config.value1}',
  ${hoodie.config.key2} = '${hoodie.config.value2}',
  ....
);

e.g.
CREATE TABLE hudi_table(
  id BIGINT PRIMARY KEY NOT ENFORCED,
  name STRING,
  price DOUBLE,
  ts BIGINT
)
PARTITIONED BY (`dt`)
WITH (
'connector' = 'hudi',
'path' = 'file:///tmp/hudi_table',
'table.type' = 'MERGE_ON_READ',
'precombine.field' = 'ts',
'hoodie.cleaner.fileversions.retained' = '20',
'hoodie.keep.max.commits' = '20',
'hoodie.datasource.write.hive_style_partitioning' = 'true'
);
```

## Supported Types

| Spark         |     Hudi     |     Notes     |
|---------------|--------------|---------------|
| boolean       |  boolean     |               |
| byte          |  int         |               |
| short         |  int         |               |
| integer       |  int         |               |
| long          |  long        |               |
| date          |  date        |               |
| timestamp     |  timestamp   |               |
| float         |  float       |               |
| double        |  double      |               |
| string        |  string      |               |
| decimal       |  decimal     |               |
| binary        |  bytes       |               |
| array         |  array       |               |
| map           |  map         |               |
| struct        |  struct      |               |
| char          |              | not supported |
| varchar       |              | not supported |
| numeric       |              | not supported |
| null          |              | not supported |
| object        |              | not supported |
