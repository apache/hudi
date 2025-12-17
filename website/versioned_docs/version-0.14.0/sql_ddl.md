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
Hudi requires a lock provider to support concurrent writers or asynchronous table services. Users can pass these table 
properties into *TBLPROPERTIES* as well. Below is an example for a Zookeeper based configuration.

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

## Flink

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

The following is an example of creating a Flink table. Read the [Flink Quick Start](flink-quick-start-guide.md) guide for more examples.

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
ALTER TABLE tableA RENAME TO tableB;
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
