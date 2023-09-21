---
title: SQL DDL
summary: "In this page, we discuss DDL commands in Spark-SQL with Hudi"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The following DDL actions are available via SparkSQL:

# Spark Create Table
:::note
Only SparkSQL needs an explicit Create Table command. No explicit Create Table is required for Spark Datasource APIs (both Scala or 
Python, batch or streaming). The first batch of a [Write](/docs/writing_data) to a table will create the table if it does not exist. 
Feel free to take a look at our (quick start guide)[/docs/next/quick-start-guide] for spark datasource writes using scala or pyspark. 
:::

### Create table Properties

Users can set table properties while creating a hudi table. Critical options are listed here.

| Parameter Name | Default | Description                                                                                                                                                                                                                                                                                                                                      |
|------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primaryKey | uuid | The primary key field names of the table, multiple fields separated by commas. Same as `hoodie.datasource.write.recordkey.field`. If this config is ignored (from 0.14.0), hudi will auto generate primary keys. If explicitly set, primary key generation will honor user configugration.                                                       |
| preCombineField |  | The pre-combine field of the table. Same as `hoodie.datasource.write.precombine.field` and is used for resolving the final version of the record among multiple versions. Generally `event time` or some other similar column will be used for ordering purpose. Hudi will be able to handle out of order data using the preCombine field value. |
| type       | cow | The table type to create. type = 'cow' means a COPY-ON-WRITE table, while type = 'mor' means a MERGE-ON-READ table. Same as `hoodie.datasource.write.table.type`. More details can be found [here](/docs/table_types)                                                                                                                            |

:::note
1. `primaryKey`, `preCombineField`, and `type` are case-sensitive.
2. While setting `primaryKey`, `preCombineField`, `type` or other Hudi configs, `tblproperties` is preferred over `options`.
3. A new Hudi table created by Spark SQL will by default set `hoodie.datasource.write.hive_style_partitioning=true`.
:::

## Creating a Hudi table 
Here is an example of creating a non-partitioned Hudi table. 

```sql
-- create a Hudi table
create table if not exists hudi_table(
  id int, 
  name string, 
  price double
) using hudi;
```

:::note
There could be cases where electing a primary key may not be feasible due to various reasons. Some payloads actually 
don't have a naturally present record key: for ex, when ingesting some kind of "logs" into Hudi there 
might be no unique identifier held in every record that could serve the purpose of being record key, while meeting global 
uniqueness requirements of the primary key. So, users can choose to ignore configuring 'primaryKey' in which case, 
Hudi will auto generate primary keys for the records during ingestion. Users can still perform Merge Into, updates and 
deletes based on any random data column for such tables. 
::: 

### Creating a Hudi table w/ user configured primary keys
Here is an example of creating a COW non-partitioned table with primary keyed on `id`. For mutable datasets, 
it is recommended to set appropriate primary key configs. 

```sql
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

:::note
You can elect multiple fields as primary keys for a given table on a need basis. For eg, "primaryKey = 'id,name'". 
:::

### PreCombineField
Here is an example of creating an COW table. The **preCombineField** option
is used to specify the preCombine field for merge. Generally 'event time' or a similar column will be used for 
ordering purpose. Hudi will be able to handle out of order data using the preCombine field value.

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

### Partitioned Table
:::note
When created in spark-sql, partition columns will always be the last columns in the table.
:::
Here is an example of creating a COW partitioned table. Adding `partitioned by` clause to the create table syntax, refers 
to a partitioned dataset. 

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
You can create multi-field partitioned table by giving comma separated field names. for eg "partitioned by dt,hh". 
:::

### Create Table for an External Hudi Table
You can create an External table using the `location` statement. If an external location is not specified it is considered a managed table. 
You can read more about external vs managed tables [here](https://sparkbyexamples.com/apache-hive/difference-between-hive-internal-tables-and-external-tables/).
An external table is useful if you need to read/write to/from a pre-existing hudi table.

```sql
 create table h_p1 using hudi
 location '/path/to/hudi';
```

:::tip
You don't need to specify schema and any properties except the partitioned columns if existed. Hudi can automatically 
recognize the schema and configurations.
:::

## Create Table As Select (CTAS)

Hudi supports CTAS(Create table as select) on spark sql. <br/>
**Note:** For better performance to load data to hudi table, CTAS uses **bulk insert** as the write operation.

***Create a Hudi Table using CTAS by loading data from another parquet table***

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

:::note
If you prefer to explicitly set the primary keys, you can do so by setting `primaryKey` config in tblproperties.
:::

### CTAS Hudi table

```sql 
create table hudi_tbl using hudi
as
select 1 as id, 'a1' as name, 10 as price;
```

### CTAS Hudi table with user configured Primary key

```sql
create table hudi_tbl using hudi
tblproperties (type = 'cow', primaryKey = 'id')
partitioned by (dt)
as
select 1 as id, 'a1' as name, 10 as price, 1000 as dt;
```

### CTAS by loading data from another table

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

## Setting configs 

There are differet ways you can set the configs for a given hudi table. 

### Using set command
You can use the **set** command to set any custom hudi's write config. This will work for the whole spark session scope.
```sql
set hoodie.insert.shuffle.parallelism = 100;
set hoodie.upsert.shuffle.parallelism = 100;
set hoodie.delete.shuffle.parallelism = 100;
```

### Using tblproperties
You can also set the config with table options when creating table. This will work for
the table scope only and override the config set by the SET command.

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


## Spark Alter Table
### Syntax
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

:::note
`ALTER TABLE ... RENAME TO ...` is not supported when using AWS Glue Data Catalog as hive metastore as Glue itself does 
not support table renames.
:::

### Examples
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

Schema evolution can be achieved via `ALTER TABLE` commands. Below shows some basic examples.

:::note
For more detailed examples, please prefer to [schema evolution](/docs/schema_evolution)
:::

### Alter hoodie config options
You can also alter the write config for a table by the **ALTER TABLE SET SERDEPROPERTIES**

**Example**
```sql
 alter table h3 set serdeproperties (hoodie.keep.max.commits = '10') 
```

### Show and Drop partitions

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

:::note
Currently,  the result of `show partitions` is based on the filesystem table path. It's not precise when delete the whole partition data or drop certain partition directly.
:::

## Flink

### Create Catalog

The catalog helps to manage the SQL tables, the table can be shared among CLI sessions if the catalog persists the table DDLs.
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
