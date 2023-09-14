---
title: "Spark Guide"
sidebar_position: 2
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide provides a quick peek at Hudi's capabilities using spark-shell. Using Spark datasources, we will walk through
code snippets that allows you to insert and update a Hudi table of default table type:
[Copy on Write](/docs/table_types#copy-on-write-table). After each write operation we will also show how to read the
data both snapshot and incrementally.

## Setup

Hudi works with Spark-2.4.3+ & Spark 3.x versions. You can follow instructions [here](https://spark.apache.org/downloads) for setting up Spark.

**Spark 3 Support Matrix**

| Hudi            | Supported Spark 3 version                       |
|:----------------|:------------------------------------------------|
| 0.11.0          | 3.2.x (default build, Spark bundle only), 3.1.x |
| 0.10.0          | 3.1.x (default build), 3.0.x                    |
| 0.7.0 - 0.9.0   | 3.0.x                                           |
| 0.6.0 and prior | not supported                                   |

The *default build* Spark version indicates that it is used to build the `hudi-spark3-bundle`.

:::note
In 0.11.0, there are changes on using Spark bundles, please refer
to [0.11.0 release notes](https://hudi.apache.org/releases/release-0.11.0/#spark-versions-and-bundles) for detailed
instructions.
:::

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>
<TabItem value="scala">

From the extracted directory run spark-shell with Hudi:

```shell
# Spark 3.2
spark-shell \
  --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
```shell
# Spark 3.1
spark-shell \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.11.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
```shell
# Spark 2.4
spark-shell \
  --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.11.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
</TabItem>

<TabItem value="python">

From the extracted directory run pyspark with Hudi:

```shell
# Spark 3.2
export PYSPARK_PYTHON=$(which python3)
pyspark \
--packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
```shell
# Spark 3.1
export PYSPARK_PYTHON=$(which python3)
pyspark \
--packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
```shell
# Spark 2.4
export PYSPARK_PYTHON=$(which python3)
pyspark \
--packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
</TabItem>

<TabItem value="sparksql">

Hudi support using Spark SQL to write and read data with the **HoodieSparkSessionExtension** sql extension.
From the extracted directory run Spark SQL with Hudi:

```shell
# Spark 3.2
spark-sql --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
```
```shell
# Spark 3.1
spark-sql --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```
```shell
# Spark 2.4
spark-sql --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.11.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```

</TabItem>

</Tabs
>

:::note Please note the following
<ul>
  <li> For Spark 3.2, the additional spark_catalog config is required: 
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' </li>
  <li> We have used hudi-spark-bundle built for scala 2.12 since the spark-avro module used can also depend on 2.12. </li>
</ul>
:::

Setup table name, base path and a data generator to generate records for this guide.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord

val tableName = "hudi_trips_cow"
val basePath = "file:///tmp/hudi_trips_cow"
val dataGen = new DataGenerator
```

</TabItem>
<TabItem value="python">

```python
# pyspark
tableName = "hudi_trips_cow"
basePath = "file:///tmp/hudi_trips_cow"
dataGen = sc._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()
```

</TabItem>

</Tabs
>

:::tip
The [DataGenerator](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L51) 
can generate sample inserts and updates based on the the sample trip schema [here](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)
:::

## Create Table

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>
<TabItem value="scala">

```scala
// scala
// No separate create table command required in spark. First batch of write to a table will create the table if not exists. 
```

</TabItem>
<TabItem value="python">

```python
# pyspark
# No separate create table command required in spark. First batch of write to a table will create the table if not exists.
```

</TabItem>
<TabItem value="sparksql">

Spark SQL needs an explicit create table command.

**Table Concepts**

- Table types

  Both Hudi's table types, Copy-On-Write (COW) and Merge-On-Read (MOR), can be created using Spark SQL.
  While creating the table, table type can be specified using **type** option: **type = 'cow'** or **type = 'mor'**.

- Partitioned & Non-Partitioned tables

  Users can create a partitioned table or a non-partitioned table in Spark SQL. To create a partitioned table, one needs
  to use **partitioned by** statement to specify the partition columns to create a partitioned table. When there is
  no **partitioned by** statement with create table command, table is considered to be a non-partitioned table.

- Managed & External tables

  In general, Spark SQL supports two kinds of tables, namely managed and external. If one specifies a location using **
  location** statement or use `create external table` to create table explicitly, it is an external table, else its
  considered a managed table. You can read more about external vs managed
  tables [here](https://sparkbyexamples.com/apache-hive/difference-between-hive-internal-tables-and-external-tables/).

*Read more in the [table management](/docs/table_management) guide.*

:::note
1. Since Hudi 0.10.0, `primaryKey` is required. It aligns with Hudi DataSource writer’s and resolves behavioural
   discrepancies reported in previous versions. Non-primary-key tables are no longer supported. Any Hudi table created
   pre-0.10.0 without a `primaryKey` needs to be re-created with a `primaryKey` field with 0.10.0.
2. Similar to `hoodie.datasource.write.recordkey.field`, `uuid` is used as primary key by default; if that's the case
   for your table, you can skip setting `primaryKey` in `tblproperties`.
3. `primaryKey`, `preCombineField`, and `type` are case-sensitive.
4. `preCombineField` is required for MOR tables.
5. When set `primaryKey`, `preCombineField`, `type` or other Hudi configs, `tblproperties` is preferred over `options`.
6. A new Hudi table created by Spark SQL will by default
   set `hoodie.table.keygenerator.class=org.apache.hudi.keygen.ComplexKeyGenerator` and
   `hoodie.datasource.write.hive_style_partitioning=true`.
:::

**Create a Non-Partitioned Table**

```sql
-- create a cow table, with default primaryKey 'uuid' and without preCombineField provided
create table hudi_cow_nonpcf_tbl (
  uuid int,
  name string,
  price double
) using hudi;


-- create a mor non-partitioned table without preCombineField provided
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

Here is an example of creating an external COW partitioned table.

**Create Partitioned Table**

```sql
-- create a partitioned, preCombineField-provided cow table
create table hudi_cow_pt_tbl (
  id bigint,
  name string,
  ts bigint,
  dt string,
  hh string
) using hudi
tblproperties (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts'
 )
partitioned by (dt, hh)
location '/tmp/hudi/hudi_cow_pt_tbl';
```

**Create Table for an existing Hudi Table**

We can create a table on an existing hudi table(created with spark-shell or deltastreamer). This is useful to
read/write to/from a pre-existing hudi table.

```sql
create table hudi_existing_tbl using hudi
location '/tmp/hudi/hudi_existing_table';
```

:::tip
You don't need to specify schema and any properties except the partitioned columns if existed. Hudi can automatically recognize the schema and configurations.
:::

**CTAS**

Hudi supports CTAS (Create Table As Select) on Spark SQL. <br/>
Note: For better performance to load data to hudi table, CTAS uses the **bulk insert** as the write operation.

Example CTAS command to create a non-partitioned COW table without preCombineField.

```sql
-- CTAS: create a non-partitioned cow table without preCombineField
create table hudi_ctas_cow_nonpcf_tbl
using hudi
tblproperties (primaryKey = 'id')
as
select 1 as id, 'a1' as name, 10 as price;
```

Example CTAS command to create a partitioned, primary key COW table.

```sql
-- CTAS: create a partitioned, preCombineField-provided cow table
create table hudi_ctas_cow_pt_tbl
using hudi
tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')
partitioned by (dt)
as
select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-12-01' as dt;

```

Example CTAS command to load data from another table.

```sql
# create managed parquet table
create table parquet_mngd using parquet location 'file:///tmp/parquet_dataset/*.parquet';

# CTAS by loading data into hudi table
create table hudi_ctas_cow_pt_tbl2 using hudi location 'file:/tmp/hudi/hudi_tbl/' options (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts'
 )
partitioned by (datestr) as select * from parquet_mngd;
```

**Create Table Properties**

Users can set table properties while creating a hudi table. Critical options are listed here.

| Parameter Name | Default | Introduction |
|------------------|--------|------------|
| primaryKey | uuid | The primary key names of the table, multiple fields separated by commas. Same as `hoodie.datasource.write.recordkey.field` |
| preCombineField |  | The pre-combine field of the table. Same as `hoodie.datasource.write.precombine.field` |
| type       | cow | The table type to create. type = 'cow' means a COPY-ON-WRITE table, while type = 'mor' means a MERGE-ON-READ table. Same as `hoodie.datasource.write.table.type` |

To set any custom hudi config(like index type, max parquet size, etc), see the  "Set hudi config section" .

</TabItem>

</Tabs
>


## Insert data

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

```scala
// spark-shell
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```
:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. We provided a record key
(`uuid` in [schema](https://github.com/apache/hudi/blob/2e6e302efec2fa848ded4f88a95540ad2adb7798/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/2e6e302efec2fa848ded4f88a95540ad2adb7798/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](https://hudi.apache.org/learn/faq/#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](/docs/writing_data).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](/docs/write_operations)
:::
</TabItem>

<TabItem value="python">
Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

```python
# pyspark
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi").
    options(**hudi_options).
    mode("overwrite").
    save(basePath)
```
:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. We provided a record key
(`uuid` in [schema](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](/docs/writing_data).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](/docs/write_operations)
:::
</TabItem>

<TabItem value="sparksql">

```sql
-- insert into non-partitioned table
insert into hudi_cow_nonpcf_tbl select 1, 'a1', 20;
insert into hudi_mor_tbl select 1, 'a1', 20, 1000;

-- insert dynamic partition
insert into hudi_cow_pt_tbl partition (dt, hh)
select 1 as id, 'a1' as name, 1000 as ts, '2021-12-09' as dt, '10' as hh;

-- insert static partition
insert into hudi_cow_pt_tbl partition(dt = '2021-12-09', hh='11') select 2, 'a2', 1000;
```

**NOTICE**
- By default,  if `preCombineKey `  is provided,  `insert into` use `upsert` as the type of write operation, otherwise use `insert`.
- We support to use `bulk_insert` as the type of write operation, just need to set two configs: `hoodie.sql.bulk.insert.enable` and `hoodie.sql.insert.mode`. Example as follow: 

```sql
-- upsert mode for preCombineField-provided table
insert into hudi_mor_tbl select 1, 'a1_1', 20, 1001;
select id, name, price, ts from hudi_mor_tbl;
1	a1_1	20.0	1001

-- bulk_insert mode for preCombineField-provided table
set hoodie.sql.bulk.insert.enable=true;
set hoodie.sql.insert.mode=non-strict;

insert into hudi_mor_tbl select 1, 'a1_2', 20, 1002;
select id, name, price, ts from hudi_mor_tbl;
1	a1_1	20.0	1001
1	a1_2	20.0	1002
```

</TabItem>

</Tabs
>


Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for various key generator options, like Timestamp based,
complex, custom, NonPartitioned Key gen, etc. 

## Query data 

Load the data files into a DataFrame.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
```
</TabItem>
<TabItem value="python">

```python
# pyspark
tripsSnapshotDF = spark. \
  read. \
  format("hudi"). \
  load(basePath)
# load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery

tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
```
</TabItem>
<TabItem value="sparksql">

```sql
 select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0
```
</TabItem>

</Tabs
>

:::info
Since 0.9.0 hudi has support a hudi built-in FileIndex: **HoodieFileIndex** to query hudi table,
which supports partition pruning and metatable for query. This will help improve query performance.
It also supports non-global query path which means users can query the table by the base path without
specifing the "*" in the query path. This feature has enabled by default for the non-global query path.
For the global query path, hudi uses the old query path.
Refer to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.
:::

### Time Travel Query

Hudi supports time travel query since 0.9.0. Currently three query time formats are supported as given below.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

```scala
spark.read.
  format("hudi").
  option("as.of.instant", "20210728141108100").
  load(basePath)

spark.read.
  format("hudi").
  option("as.of.instant", "2021-07-28 14:11:08.200").
  load(basePath)

// It is equal to "as.of.instant = 2021-07-28 00:00:00"
spark.read.
  format("hudi").
  option("as.of.instant", "2021-07-28").
  load(basePath)

```

</TabItem>
<TabItem value="python">

```python
#pyspark
spark.read. \
  format("hudi"). \
  option("as.of.instant", "20210728141108"). \
  load(basePath)

spark.read. \
  format("hudi"). \
  option("as.of.instant", "2021-07-28 14: 11: 08"). \
  load(basePath)

// It is equal to "as.of.instant = 2021-07-28 00:00:00"
spark.read. \
  format("hudi"). \
  option("as.of.instant", "2021-07-28"). \
  load(basePath)
```

</TabItem>
<TabItem value="sparksql">

:::note
Requires Spark 3.2+
:::

```sql
create table hudi_cow_pt_tbl (
  id bigint,
  name string,
  ts bigint,
  dt string,
  hh string
) using hudi
tblproperties (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'ts'
 )
partitioned by (dt, hh)
location '/tmp/hudi/hudi_cow_pt_tbl';

insert into hudi_cow_pt_tbl select 1, 'a0', 1000, '2021-12-09', '10';
select * from hudi_cow_pt_tbl;

-- record id=1 changes `name`
insert into hudi_cow_pt_tbl select 1, 'a1', 1001, '2021-12-09', '10';
select * from hudi_cow_pt_tbl;

-- time travel based on first commit time, assume `20220307091628793`
select * from hudi_cow_pt_tbl timestamp as of '20220307091628793' where id = 1;
-- time travel based on different timestamp formats
select * from hudi_cow_pt_tbl timestamp as of '2022-03-07 09:16:28.100' where id = 1;
select * from hudi_cow_pt_tbl timestamp as of '2022-03-08' where id = 1;
```

</TabItem>

</Tabs
>

## Update data

This is similar to inserting new data. Generate updates to existing trips using the data generator, load into a DataFrame 
and write DataFrame into the hudi table.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
val updates = convertToStringList(dataGen.generateUpdates(10))
val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```
:::note
Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
[Querying](#query-data) the data again will now show updated trips. Each write operation generates a new [commit](/docs/concepts)
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `rider`, `driver` fields for the same `_hoodie_record_key`s in previous commit.
:::
</TabItem>
<TabItem value="sparksql">

Spark SQL supports two kinds of DML to update hudi table: Merge-Into and Update.

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

</TabItem>
<TabItem value="python">

```python
# pyspark
updates = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateUpdates(10))
df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
df.write.format("hudi"). \
  options(**hudi_options). \
  mode("append"). \
  save(basePath)
```
:::note
Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
[Querying](#query-data) the data again will now show updated trips. Each write operation generates a new [commit](/docs/concepts)
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `rider`, `driver` fields for the same `_hoodie_record_key`s in previous commit.
:::

</TabItem>

</Tabs
>


## Incremental query

Hudi also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's incremental querying and providing a begin time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
// reload data
spark.
  read.
  format("hudi").
  load(basePath).
  createOrReplaceTempView("hudi_trips_snapshot")

val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).take(50)
val beginTime = commits(commits.length - 2) // commit time we are interested in

// incrementally query data
val tripsIncrementalDF = spark.read.format("hudi").
  option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
  load(basePath)
tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
```

</TabItem>
<TabItem value="python">

```python
# pyspark
# reload data
spark. \
  read. \
  format("hudi"). \
  load(basePath). \
  createOrReplaceTempView("hudi_trips_snapshot")

commits = list(map(lambda row: row[0], spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").limit(50).collect()))
beginTime = commits[len(commits) - 2] # commit time we are interested in

# incrementally query data
incremental_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.begin.instanttime': beginTime,
}

tripsIncrementalDF = spark.read.format("hudi"). \
  options(**incremental_read_options). \
  load(basePath)
tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
```

</TabItem>

</Tabs
>

:::info
This will give all changes that happened after the beginTime commit with the filter of fare > 20.0. The unique thing about this
feature is that it now lets you author streaming pipelines on batch data.
:::

## Structured Streaming

Hudi supports Spark Structured Streaming reads and writes.
Structured Streaming reads are based on Hudi Incremental Query feature, therefore streaming read can return data for which commits and base files were not yet removed by the cleaner. You can control commits retention time.

### Streaming Read
<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
// reload data
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)

// read stream and output results to console
spark.readStream.
  format("hudi").
  load(basePath).
  writeStream.
  format("console").
  start()

// read stream to streaming df
val df = spark.readStream.
        format("hudi").
        load(basePath)

```

</TabItem>
<TabItem value="python">

```python
# pyspark
# reload data
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(
    dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)

# read stream to streaming df
df = spark.readStream \
    .format("hudi") \
    .load(basePath)

# read stream and output results to console
spark.readStream \
    .format("hudi") \
    .load(basePath) \
    .writeStream \
    .format("console") \
    .start()

```

</TabItem>

</Tabs
>

### Streaming Write

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
// prepare to stream write to new table
import org.apache.spark.sql.streaming.Trigger

val streamingTableName = "hudi_trips_cow_streaming"
val baseStreamingPath = "file:///tmp/hudi_trips_cow_streaming"
val checkpointLocation = "file:///tmp/checkpoints/hudi_trips_cow_streaming"

// create streaming df
val df = spark.readStream.
        format("hudi").
        load(basePath)

// write stream to new hudi table
df.writeStream.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, streamingTableName).
  outputMode("append").
  option("path", baseStreamingPath).
  option("checkpointLocation", checkpointLocation).
  trigger(Trigger.Once()).
  start()

```

</TabItem>
<TabItem value="python">

```python
# pyspark
# prepare to stream write to new table
streamingTableName = "hudi_trips_cow_streaming"
baseStreamingPath = "file:///tmp/hudi_trips_cow_streaming"
checkpointLocation = "file:///tmp/checkpoints/hudi_trips_cow_streaming"

hudi_streaming_options = {
    'hoodie.table.name': streamingTableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': streamingTableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

# create streaming df
df = spark.readStream \
    .format("hudi") \
    .load(basePath)

# write stream to new hudi table
df.writeStream.format("hudi") \
    .options(**hudi_streaming_options) \
    .outputMode("append") \
    .option("path", baseStreamingPath) \
    .option("checkpointLocation", checkpointLocation) \
    .trigger(once=True) \
    .start()

```

</TabItem>

</Tabs
>

:::info
Spark SQL can be used within ForeachBatch sink to do INSERT, UPDATE, DELETE and MERGE INTO.
Target table must exist before write.
:::

### Table maintenance
Hudi can run async or inline table services while running Structured Streaming query and takes care of cleaning, compaction and clustering. There's no operational overhead for the user.  
For CoW tables, table services work in inline mode by default.  
For MoR tables, some async services are enabled by default.

:::note
Since Hudi 0.11 Metadata Table is enabled by default. When using async table services with Metadata Table enabled you must use Optimistic Concurrency Control to avoid the risk of data loss (even in single writer scenario). See [Metadata Table deployment considerations](/docs/0.11.0/metadata#deployment-considerations) for detailed instructions.

If you're using Foreach or ForeachBatch streaming sink you must use inline table services, async table services are not supported.
:::

Hive Sync works with Structured Streaming, it will create table if not exists and synchronize table to metastore aftear each streaming write.

## Point in time query

Lets look at how to query data as of a specific time. The specific time can be represented by pointing endTime to a 
specific commit time and beginTime to "000" (denoting earliest possible commit time). 

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
val beginTime = "000" // Represents all commits > this time.
val endTime = commits(commits.length - 2) // commit time we are interested in

//incrementally query data
val tripsPointInTimeDF = spark.read.format("hudi").
  option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
  option(END_INSTANTTIME_OPT_KEY, endTime).
  load(basePath)
tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()
```

</TabItem>
<TabItem value="python">

```python
# pyspark
beginTime = "000" # Represents all commits > this time.
endTime = commits[len(commits) - 2]

# query point in time data
point_in_time_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.end.instanttime': endTime,
  'hoodie.datasource.read.begin.instanttime': beginTime
}

tripsPointInTimeDF = spark.read.format("hudi"). \
  options(**point_in_time_read_options). \
  load(basePath)

tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()
```

</TabItem>

</Tabs
>

## Delete data {#deletes}

Apache Hudi supports two types of deletes: (1) **Soft Deletes**: retaining the record key and just nulling out the values
for all the other fields (records with nulls in soft deletes are always persisted in storage and never removed);
(2) **Hard Deletes**: physically removing any trace of the record from the table.  See the
[deletion section](/docs/writing_data#deletes) of the writing data page for more details.

### Soft Deletes

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', }
]}
>

<TabItem value="scala">

```scala
// spark-shell
spark.
  read.
  format("hudi").
  load(basePath).
  createOrReplaceTempView("hudi_trips_snapshot")
// fetch total records count
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
spark.sql("select uuid, partitionpath from hudi_trips_snapshot where rider is not null").count()
// fetch two records for soft deletes
val softDeleteDs = spark.sql("select * from hudi_trips_snapshot").limit(2)

// prepare the soft deletes by ensuring the appropriate fields are nullified
val nullifyColumns = softDeleteDs.schema.fields.
  map(field => (field.name, field.dataType.typeName)).
  filter(pair => (!HoodieRecord.HOODIE_META_COLUMNS.contains(pair._1)
    && !Array("ts", "uuid", "partitionpath").contains(pair._1)))

val softDeleteDf = nullifyColumns.
  foldLeft(softDeleteDs.drop(HoodieRecord.HOODIE_META_COLUMNS: _*))(
    (ds, col) => ds.withColumn(col._1, lit(null).cast(col._2)))

// simply upsert the table after setting these fields to null
softDeleteDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "upsert").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

// reload data
spark.
  read.
  format("hudi").
  load(basePath).
  createOrReplaceTempView("hudi_trips_snapshot")

// This should return the same total count as before
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
// This should return (total - 2) count as two records are updated with nulls
spark.sql("select uuid, partitionpath from hudi_trips_snapshot where rider is not null").count()
```
:::note
Notice that the save mode is `Append`.
:::
</TabItem>
<TabItem value="python">

```python
# pyspark
from pyspark.sql.functions import lit
from functools import reduce

spark.read.format("hudi"). \
  load(basePath). \
  createOrReplaceTempView("hudi_trips_snapshot")
# fetch total records count
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
spark.sql("select uuid, partitionpath from hudi_trips_snapshot where rider is not null").count()
# fetch two records for soft deletes
soft_delete_ds = spark.sql("select * from hudi_trips_snapshot").limit(2)

# prepare the soft deletes by ensuring the appropriate fields are nullified
meta_columns = ["_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", \
  "_hoodie_partition_path", "_hoodie_file_name"]
excluded_columns = meta_columns + ["ts", "uuid", "partitionpath"]
nullify_columns = list(filter(lambda field: field[0] not in excluded_columns, \
  list(map(lambda field: (field.name, field.dataType), softDeleteDs.schema.fields))))

hudi_soft_delete_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'uuid',
  'hoodie.datasource.write.partitionpath.field': 'partitionpath',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

soft_delete_df = reduce(lambda df,col: df.withColumn(col[0], lit(None).cast(col[1])), \
  nullify_columns, reduce(lambda df,col: df.drop(col[0]), meta_columns, soft_delete_ds))

# simply upsert the table after setting these fields to null
soft_delete_df.write.format("hudi"). \
  options(**hudi_soft_delete_options). \
  mode("append"). \
  save(basePath)

# reload data
spark.read.format("hudi"). \
  load(basePath). \
  createOrReplaceTempView("hudi_trips_snapshot")

# This should return the same total count as before
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
# This should return (total - 2) count as two records are updated with nulls
spark.sql("select uuid, partitionpath from hudi_trips_snapshot where rider is not null").count()
```
:::note
Notice that the save mode is `Append`.
:::
</TabItem>

</Tabs
>


### Hard Deletes

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">
Delete records for the HoodieKeys passed in.<br/>

```scala
// spark-shell
// fetch total records count
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
// fetch two records to be deleted
val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

// issue deletes
val deletes = dataGen.generateDeletes(ds.collectAsList())
val hardDeleteDf = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

hardDeleteDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY,"delete").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

// run the same read query as above.
val roAfterDeleteViewDF = spark.
  read.
  format("hudi").
  load(basePath)

roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
// fetch should return (total - 2) records
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
```
:::note
Only `Append` mode is supported for delete operation.
:::
</TabItem>
<TabItem value="sparksql">

**Syntax**
```sql
DELETE FROM tableIdentifier [ WHERE BOOL_EXPRESSION]
```
**Example**
```sql
delete from hudi_cow_nonpcf_tbl where uuid = 1;

delete from hudi_mor_tbl where id % 2 = 0;

-- delete using non-PK field
delete from hudi_cow_pt_tbl where name = 'a1';
```

</TabItem>
<TabItem value="python">
Delete records for the HoodieKeys passed in.<br/>

```python
# pyspark
# fetch total records count
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
# fetch two records to be deleted
ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

# issue deletes
hudi_hard_delete_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'uuid',
  'hoodie.datasource.write.partitionpath.field': 'partitionpath',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'delete',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

from pyspark.sql.functions import lit
deletes = list(map(lambda row: (row[0], row[1]), ds.collect()))
hard_delete_df = spark.sparkContext.parallelize(deletes).toDF(['uuid', 'partitionpath']).withColumn('ts', lit(0.0))
hard_delete_df.write.format("hudi"). \
  options(**hudi_hard_delete_options). \
  mode("append"). \
  save(basePath)

# run the same read query as above.
roAfterDeleteViewDF = spark. \
  read. \
  format("hudi"). \
  load(basePath) 
roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
# fetch should return (total - 2) records
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
```
:::note
Only `Append` mode is supported for delete operation.
:::
</TabItem>

</Tabs
>

## Insert Overwrite

Generate some new trips, overwrite the all the partitions that are present in the input. This operation can be faster
than `upsert` for batch ETL jobs, that are recomputing entire target partitions at once (as opposed to incrementally
updating the target tables). This is because, we are able to bypass indexing, precombining and other repartitioning
steps in the upsert write path completely.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

```scala
// spark-shell
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  sort("partitionpath","uuid").
  show(100, false)

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.
  read.json(spark.sparkContext.parallelize(inserts, 2)).
  filter("partitionpath = 'americas/united_states/san_francisco'")
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION.key(),"insert_overwrite").
  option(PRECOMBINE_FIELD.key(), "ts").
  option(RECORDKEY_FIELD.key(), "uuid").
  option(PARTITIONPATH_FIELD.key(), "partitionpath").
  option(TBL_NAME.key(), tableName).
  mode(Append).
  save(basePath)

// Should have different keys now for San Francisco alone, from query before.
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  sort("partitionpath","uuid").
  show(100, false)
```
</TabItem>

<TabItem value="sparksql">

`insert overwrite` a partitioned table use the `INSERT_OVERWRITE` type of write operation, while a non-partitioned table to `INSERT_OVERWRITE_TABLE`.

```sql
-- insert overwrite non-partitioned table
insert overwrite hudi_mor_tbl select 99, 'a99', 20.0, 900;
insert overwrite hudi_cow_nonpcf_tbl select 99, 'a99', 20.0;

-- insert overwrite partitioned table with dynamic partition
insert overwrite table hudi_cow_pt_tbl select 10, 'a10', 1100, '2021-12-09', '10';

-- insert overwrite partitioned table with static partition
insert overwrite hudi_cow_pt_tbl partition(dt = '2021-12-09', hh='12') select 13, 'a13', 1100;
```
</TabItem>

</Tabs
>

## More Spark SQL Commands

### Alter Table

Schema evolution can be achieved via `ALTER TABLE` commands. Below shows some basic examples.

:::note
For more detailed examples, please prefer to [schema evolution](schema_evolution)
:::

**Syntax**
```sql
-- Alter table name
ALTER TABLE oldTableName RENAME TO newTableName

-- Alter table add columns
ALTER TABLE tableIdentifier ADD COLUMNS(colAndType (,colAndType)*)

-- Alter table column type
ALTER TABLE tableIdentifier CHANGE COLUMN colName colName colType

-- Alter table properties
ALTER TABLE tableIdentifier SET TBLPROPERTIES (key = 'value')
```
**Examples**
```sql
--rename to:
ALTER TABLE hudi_cow_nonpcf_tbl RENAME TO hudi_cow_nonpcf_tbl2;

--add column:
ALTER TABLE hudi_cow_nonpcf_tbl2 add columns(remark string);

--change column:
ALTER TABLE hudi_cow_nonpcf_tbl2 change column uuid uuid bigint;

--set properties;
alter table hudi_cow_nonpcf_tbl2 set tblproperties (hoodie.keep.max.commits = '10');
```

### Partition SQL Command

**Syntax**

```sql
-- Drop Partition
ALTER TABLE tableIdentifier DROP PARTITION ( partition_col_name = partition_col_val [ , ... ] )

-- Show Partitions
SHOW PARTITIONS tableIdentifier
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

### Procedures

**Syntax**
```sql
--Call procedure by positional arguments
CALL system.procedure_name(arg_1, arg_2, ... arg_n)

--Call procedure by named arguments
CALL system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1, ... arg_name_n => arg_n)
```
**Examples**
```sql
--show commit's info
call show_commits(table => 'test_hudi_table', limit => 10);
```

Call command has already support some commit procedures and table optimization procedures, 
more details please refer to [procedures](procedures).

## Where to go from here?

You can also do the quickstart by [building hudi yourself](https://github.com/apache/hudi#building-apache-hudi-from-source), 
and using `--jars <path to hudi_code>/packaging/hudi-spark-bundle/target/hudi-spark3.2-bundle_2.1?-*.*.*-SNAPSHOT.jar` in the spark-shell command above
instead of `--packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.0`. Hudi also supports scala 2.12. Refer [build with scala 2.12](https://github.com/apache/hudi#build-with-different-spark-versions)
for more info.

Also, we used Spark here to show case the capabilities of Hudi. However, Hudi can support multiple table types/query types and 
Hudi tables can be queried from query engines like Hive, Spark, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/docker_demo) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/migration_guide).
