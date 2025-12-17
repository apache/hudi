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

### Create table with merge modes {#create-table-with-record-merge-mode}

Hudi supports different [record merge modes](record_merger.md) to handle merge of incoming records with existing
records. To create a table with specific record merge mode, you can set `recordMergeMode` option.

```sql
CREATE TABLE IF NOT EXISTS hudi_table_merge_mode (
  id INT,
  name STRING,
  ts LONG,
  price DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'id',
  precombineField = 'ts',
  recordMergeMode = 'EVENT_TIME_ORDERING'
)
LOCATION 'file:///tmp/hudi_table_merge_mode/';
```

With `EVENT_TIME_ORDERING`, the record with the larger event time (`precombineField`) overwrites the record with the
smaller event time on the same key, regardless of transaction's commit time. Users can set `CUSTOM` mode to provide their own
merge logic. With `CUSTOM` merge mode, you can provide a custom class that implements the merge logic. The interfaces 
to implement is explained in detail [here](record_merger.md#custom).

```sql
CREATE TABLE IF NOT EXISTS hudi_table_merge_mode_custom (
  id INT,
  name STRING,
  ts LONG,
  price DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'id',
  precombineField = 'ts',
  recordMergeMode = 'CUSTOM',
  'hoodie.record.merge.strategy.id' = '<unique-uuid>'
)
LOCATION 'file:///tmp/hudi_table_merge_mode_custom/';
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

### Create Index

Hudi supports creating and dropping different types of indexes on a table. For more information on different
type of indexes please refer [multi-modal indexing](indexes.md#multi-modal-indexing). Secondary 
index, expression index and record indexes can be created using SQL create index command.

```sql
-- Create Index
CREATE INDEX [IF NOT EXISTS] index_name ON [TABLE] table_name 
[USING index_type] 
(column_name1 [OPTIONS(key1=value1, key2=value2, ...)], column_name2 [OPTIONS(key1=value1, key2=value2, ...)], ...) 
[OPTIONS (key1=value1, key2=value2, ...)]

-- Record index syntax
CREATE INDEX indexName ON tableIdentifier (primaryKey1 [, primayKey2 ...]);

-- Secondary Index Syntax
CREATE INDEX indexName ON tableIdentifier (nonPrimaryKey);

-- Expression Index Syntax
CREATE INDEX indexName ON tableIdentifier USING column_stats(col) OPTIONS(expr='expr_val', format='format_val');
CREATE INDEX indexName ON tableIdentifier USING bloom_filters(col) OPTIONS(expr='expr_val');

-- Drop Index
DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
```

- `index_name` is the name of the index to be created or dropped.
- `table_name` is the name of the table on which the index is created or dropped.
- `index_type` is the type of the index to be created. Currently, only `column_stats` and `bloom_filters` is supported. 
   If the `using ..` clause is omitted, a secondary record index is created.
- `column_name` is the name of the column on which the index is created.

Both index and column on which the index is created can be qualified with some options in the form of key-value pairs.

:::note
Please note in order to create secondary index:
1. The table must have a primary key and merge mode should be [COMMIT_TIME_ORDERING](record_merger.md#commit_time_ordering).
2. Record index must be enabled. This can be done by setting `hoodie.metadata.record.index.enable=true` and then creating `record_index`. Please note the example below.
3. Secondary index is not supported for [complex types](https://avro.apache.org/docs/1.11.1/specification/#complex-types).
:::

**Examples**
```sql
-- Create a table with primary key
CREATE TABLE hudi_indexed_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
options(
    primaryKey ='uuid',
    hoodie.write.record.merge.mode = "COMMIT_TIME_ORDERING"
)
PARTITIONED BY (city);

-- Add some data.
INSERT INTO hudi_indexed_table
VALUES
 ...

-- Create bloom filter expression index on driver column
CREATE INDEX idx_bloom_driver ON hudi_indexed_table USING bloom_filters(driver) OPTIONS(expr='identity');
-- It would show bloom filter expression index
SHOW INDEXES FROM hudi_indexed_table;
-- Query on driver column would prune the data using the idx_bloom_driver index
SELECT uuid, rider FROM hudi_indexed_table WHERE driver = 'driver-S';

-- Create column stat expression index on ts column
CREATE INDEX idx_column_ts ON hudi_indexed_table USING column_stats(ts) OPTIONS(expr='from_unixtime', format = 'yyyy-MM-dd');
-- Shows both expression indexes
SHOW INDEXES FROM hudi_indexed_table;
-- Query on ts column would prune the data using the idx_column_ts index
SELECT * FROM hudi_indexed_table WHERE from_unixtime(ts, 'yyyy-MM-dd') = '2023-09-24';

-- Create secondary index on rider column
CREATE INDEX record_index ON hudi_indexed_table (uuid);
CREATE INDEX idx_rider ON hudi_indexed_table (rider);
SET hoodie.metadata.record.index.enable=true;
-- Expression index and secondary index should show up
SHOW INDEXES FROM hudi_indexed_table;
-- Query on rider column would leverage the secondary index idx_rider
SELECT * FROM hudi_indexed_table WHERE rider = 'rider-E';

```

### Create Expression Index

A [expression index](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/rfc/rfc-63/rfc-63.md) is an index on a function of a column. 
It is a new addition to Hudi's [multi-modal indexing](https://hudi.apache.org/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi) 
subsystem. Expression indexes can be used to implement logical partitioning of a table, by creating `column_stats` indexes 
on an expression of a column. For e.g. an expression index extracting a date from a timestamp field, can effectively implement 
date based partitioning, provide same benefits to queries, even if the physical layout is different.

```sql
-- Create an expression index on the column `ts` (unix epoch) of the table `hudi_table` using the function `from_unixtime` with the format `yyyy-MM-dd`
CREATE INDEX IF NOT EXISTS ts_datestr ON hudi_table 
  USING column_stats(ts) 
  OPTIONS(expr='from_unixtime', format='yyyy-MM-dd');
-- Create an expression index on the column `ts` (timestamp in yyyy-MM-dd HH:mm:ss) of the table `hudi_table` using the function `hour`
CREATE INDEX ts_hour ON hudi_table 
  USING column_stats(ts) 
  options(expr='hour');
```

:::note
1. Expression index can only be created for Spark engine using SQL. It is not supported yet with Spark DataSource API.
2. Expression index is not yet supported for [complex types](https://avro.apache.org/docs/1.11.1/specification/#complex-types).
3. Expression index is supported for unary and certain binary expressions. Please check [SQL DDL docs](sql_ddl.md#create-expression-index) for more details.
   :::

The `expr` option is required for creating expression index, and it should be a valid Spark SQL function. Please check the syntax 
for the above functions in the [Spark SQL documentation](https://spark.apache.org/docs/latest/sql-ref-functions.html) and provide the options accordingly. For example, 
the `format` option is required for `from_unixtime` function.

Some useful functions that are supported are listed below.

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

Note that, only functions that take a single column as input are supported currently and UDFs are not supported.

<details>
  <summary>Full example of creating and using expression index</summary>

```sql
CREATE TABLE hudi_table_expr_index (
    ts STRING,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
tblproperties (primaryKey = 'uuid')
PARTITIONED BY (city)
location 'file:///tmp/hudi_table_expr_index';

-- Query with hour function filter but no index yet --
spark-sql> SELECT city, fare, rider, driver FROM  hudi_table_expr_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
san_francisco	93.5	rider-E	driver-O
san_francisco	33.9	rider-D	driver-L
sao_paulo	43.4	rider-G	driver-Q
Time taken: 0.208 seconds, Fetched 3 row(s)

spark-sql> EXPLAIN COST SELECT city, fare, rider, driver FROM  hudi_table_expr_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
== Optimized Logical Plan ==
Project [city#3465, fare#3464, rider#3462, driver#3463], Statistics(sizeInBytes=899.5 KiB)
+- Filter ((isnotnull(city#3465) AND isnotnull(ts#3460)) AND (NOT (city#3465 = chennai) AND (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12))), Statistics(sizeInBytes=2.5 MiB)
   +- Relation default.hudi_table_expr_index[_hoodie_commit_time#3455,_hoodie_commit_seqno#3456,_hoodie_record_key#3457,_hoodie_partition_path#3458,_hoodie_file_name#3459,ts#3460,uuid#3461,rider#3462,driver#3463,fare#3464,city#3465] parquet, Statistics(sizeInBytes=2.5 MiB)

== Physical Plan ==
*(1) Project [city#3465, fare#3464, rider#3462, driver#3463]
+- *(1) Filter (isnotnull(ts#3460) AND (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12))
   +- *(1) ColumnarToRow
      +- FileScan parquet default.hudi_table_expr_index[ts#3460,rider#3462,driver#3463,fare#3464,city#3465] Batched: true, DataFilters: [isnotnull(ts#3460), (hour(cast(ts#3460 as timestamp), Some(Asia/Kolkata)) > 12)], Format: Parquet, Location: HoodieFileIndex(1 paths)[file:/tmp/hudi_table_expr_index], PartitionFilters: [isnotnull(city#3465), NOT (city#3465 = chennai)], PushedFilters: [IsNotNull(ts)], ReadSchema: struct<ts:string,rider:string,driver:string,fare:double>
      
     
-- create the expression index --
CREATE INDEX ts_hour ON hudi_table_expr_index USING column_stats(ts) options(expr='hour');

-- query after creating the index --
spark-sql> SELECT city, fare, rider, driver FROM  hudi_table_expr_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
san_francisco	93.5	rider-E	driver-O
san_francisco	33.9	rider-D	driver-L
sao_paulo	43.4	rider-G	driver-Q
Time taken: 0.202 seconds, Fetched 3 row(s)
spark-sql> EXPLAIN COST SELECT city, fare, rider, driver FROM  hudi_table_expr_index WHERE  city NOT IN ('chennai') AND hour(ts) > 12;
== Optimized Logical Plan ==
Project [city#2970, fare#2969, rider#2967, driver#2968], Statistics(sizeInBytes=449.8 KiB)
+- Filter ((isnotnull(city#2970) AND isnotnull(ts#2965)) AND (NOT (city#2970 = chennai) AND (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12))), Statistics(sizeInBytes=1278.3 KiB)
   +- Relation default.hudi_table_expr_index[_hoodie_commit_time#2960,_hoodie_commit_seqno#2961,_hoodie_record_key#2962,_hoodie_partition_path#2963,_hoodie_file_name#2964,ts#2965,uuid#2966,rider#2967,driver#2968,fare#2969,city#2970] parquet, Statistics(sizeInBytes=1278.3 KiB)

== Physical Plan ==
*(1) Project [city#2970, fare#2969, rider#2967, driver#2968]
+- *(1) Filter (isnotnull(ts#2965) AND (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12))
   +- *(1) ColumnarToRow
      +- FileScan parquet default.hudi_table_expr_index[ts#2965,rider#2967,driver#2968,fare#2969,city#2970] Batched: true, DataFilters: [isnotnull(ts#2965), (hour(cast(ts#2965 as timestamp), Some(Asia/Kolkata)) > 12)], Format: Parquet, Location: HoodieFileIndex(1 paths)[file:/tmp/hudi_table_expr_index], PartitionFilters: [isnotnull(city#2970), NOT (city#2970 = chennai)], PushedFilters: [IsNotNull(ts)], ReadSchema: struct<ts:string,rider:string,driver:string,fare:double>
      
```
</details>

### Create Partition Stats Index

Partition stats index is similar to column stats, in the sense that it tracks - `min, max, null, count, ..` statistics on columns in the 
table, useful in query planning. The key difference being, while `column_stats` tracks statistics about files, the partition_stats index 
tracks aggregated statistics at the storage partition path level, to help more efficiently skip entire folder paths during query planning
and execution. 

To enable partition stats index, simply set `hoodie.metadata.index.partition.stats.enable = 'true'` in create table options.

:::note
1. `column_stats` index is required to be enabled for `partition_stats` index. Both go hand in hand. 
2. `partition_stats` index is not created automatically for all columns. Users must specify list of columns for which they want to create partition stats index.
3. `column_stats` and `partition_stats` index is not yet supported for [complex types](https://avro.apache.org/docs/1.11.1/specification/#complex-types).
:::

### Create Secondary Index

Secondary indexes are record level indexes built on any column in the table. It supports multiple records having the same
secondary column value efficiently and is built on top of the existing record level index built on the table's record key.
Secondary indexes are hash based indexes that offer horizontally scalable write performance by splitting key space into shards 
by hashing, as well as fast lookups by employing row-based file formats.

Let us now look at an example of creating a table with multiple indexes and how the query leverage the indexes for both
partition pruning and data skipping.

```sql
DROP TABLE IF EXISTS hudi_table;
-- Let us create a table with multiple partition fields, and enable record index and partition stats index 
CREATE TABLE hudi_table (
    ts BIGINT,
    id STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING,
    state STRING
) USING hudi
 OPTIONS(
    primaryKey ='id',
    hoodie.metadata.record.index.enable = 'true', -- enable record index
    hoodie.metadata.index.partition.stats.enable = 'true', -- enable partition stats index
    hoodie.metadata.index.column.stats.enable = 'true', -- enable column stats
    hoodie.metadata.index.column.stats.column.list = 'rider', -- create column stats index on rider column
    hoodie.write.record.merge.mode = "COMMIT_TIME_ORDERING" -- enable commit time ordering, required for secondary index
)
PARTITIONED BY (city, state)
LOCATION 'file:///tmp/hudi_test_table';

INSERT INTO hudi_table VALUES (1695159649,'trip1','rider-A','driver-K',19.10,'san_francisco','california');
INSERT INTO hudi_table VALUES (1695091554,'trip2','rider-C','driver-M',27.70,'sunnyvale','california');
INSERT INTO hudi_table VALUES (1695332066,'trip3','rider-E','driver-O',93.50,'austin','texas');
INSERT INTO hudi_table VALUES (1695516137,'trip4','rider-F','driver-P',34.15,'houston','texas');
    
-- simple partition predicate --
select * from hudi_table where city = 'sunnyvale';
20240710215107477	20240710215107477_0_0	trip2	city=sunnyvale/state=california	1dcb14a9-bc4a-4eac-aab5-015f2254b7ec-0_0-40-75_20240710215107477.parquet	1695091554	trip2	rider-C	driver-M	27.7	sunnyvale	california
Time taken: 0.58 seconds, Fetched 1 row(s)

-- simple partition predicate on other partition field --
select * from hudi_table where state = 'texas';
20240710215119846	20240710215119846_0_0	trip4	city=houston/state=texas	08c6ed2c-a87b-4798-8f70-6d8b16cb1932-0_0-74-133_20240710215119846.parquet	1695516137	trip4	rider-F	driver-P	34.15	houston	texas
20240710215110584	20240710215110584_0_0	trip3	city=austin/state=texas	0ab2243c-cc08-4da3-8302-4ce0b4c47a08-0_0-57-104_20240710215110584.parquet	1695332066	trip3	rider-E	driver-O	93.5	austin	texas
Time taken: 0.124 seconds, Fetched 2 row(s)

-- predicate on a column for which partition stats are present --
select id, rider, city, state from hudi_table where rider > 'rider-D';
trip4	rider-F	houston	texas
trip3	rider-E	austin	texas
Time taken: 0.703 seconds, Fetched 2 row(s)
      
-- record key predicate --
SELECT id, rider, driver FROM hudi_table WHERE id = 'trip1';
trip1	rider-A	driver-K
Time taken: 0.368 seconds, Fetched 1 row(s)
      
-- create secondary index on driver --
CREATE INDEX driver_idx ON hudi_table (driver);

-- secondary key predicate --
SELECT id, driver, city, state FROM hudi_table WHERE driver IN ('driver-K', 'driver-M');
trip1	driver-K	san_francisco	california
trip2	driver-M	sunnyvale	california
Time taken: 0.83 seconds, Fetched 2 row(s)
```

### Create Bloom Filter Index

Bloom filter indexes store a bloom filter per file, on the column or column expression being index. It can be very 
effective in skipping files that don't contain a high cardinality column value e.g. uuids.

```sql
-- Create a bloom filter index on the column derived from expression `lower(rider)` of the table `hudi_table`
CREATE INDEX idx_bloom_rider ON hudi_indexed_table USING bloom_filters(rider) OPTIONS(expr='lower');
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

### Show and drop index

**Syntax**

```sql
-- Show Indexes
SHOW INDEXES FROM tableIdentifier;

-- Drop partition
DROP INDEX indexIdentifier ON tableIdentifier;
```

**Examples**
```sql
-- Show indexes
SHOW INDEXES FROM hudi_indexed_table;

-- Drop Index
DROP INDEX record_index ON hudi_indexed_table;
```

### Show create table

**Syntax**

```sql
SHOW CREATE TABLE tableIdentifier;
```

**Examples**
```sql
SHOW CREATE TABLE hudi_table;
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

### Create Table in Non-Blocking Concurrency Control Mode

The following is an example of creating a Flink table in [Non-Blocking Concurrency Control mode](concurrency_control.md#non-blocking-concurrency-control).

```sql
-- This is a datagen source that can generate records continuously
CREATE TABLE sourceT (
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` AS 'par1'
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '200'
);

-- pipeline1: by default, enable the compaction and cleaning services
CREATE TABLE t1 (
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
) WITH (
  'connector' = 'hudi',
  'path' = '/tmp/hudi-demo/t1',
  'table.type' = 'MERGE_ON_READ',
  'index.type' = 'BUCKET',
  'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
  'write.tasks' = '2'
);

-- pipeline2: disable the compaction and cleaning services manually
CREATE TABLE t1_2 (
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
) WITH (
  'connector' = 'hudi',
  'path' = '/tmp/hudi-demo/t1',
  'table.type' = 'MERGE_ON_READ',
  'index.type' = 'BUCKET',
  'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
  'write.tasks' = '2',
  'compaction.schedule.enabled' = 'false',
  'compaction.async.enabled' = 'false',
  'clean.async.enabled' = 'false'
);

-- Submit the pipelines
INSERT INTO t1
SELECT * FROM sourceT;

INSERT INTO t1_2
SELECT * FROM sourceT;

SELECT * FROM t1 LIMIT 20;
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
