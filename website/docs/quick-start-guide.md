---
title: "Spark Guide"
sidebar_position: 2
toc: true
last_modified_at: 2023-08-23T21:14:52+09:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide provides a quick peek at Hudi's capabilities using Spark. Using Spark Datasource APIs(both scala and python) and using Spark SQL, 
we will walk through code snippets that allows you to insert, update, delete and query a Hudi table.

## Setup

Hudi works with Spark-2.4.3+ & Spark 3.x versions. You can follow instructions [here](https://spark.apache.org/downloads) for setting up Spark.

**Spark 3 Support Matrix**

| Hudi            | Supported Spark 3 version                         |
|:----------------|:--------------------------------------------------|
| 0.14.x          | 3.4.x (default build), 3.3.x, 3.2.x, 3.1.x, 3.0.x |
| 0.13.x          | 3.3.x (default build), 3.2.x, 3.1.x               |
| 0.12.x          | 3.3.x (default build), 3.2.x, 3.1.x               |
| 0.11.x          | 3.2.x (default build, Spark bundle only), 3.1.x   |
| 0.10.x          | 3.1.x (default build), 3.0.x                      |
| 0.7.0 - 0.9.0   | 3.0.x                                             |
| 0.6.0 and prior | not supported                                     |

The *default build* Spark version indicates that it is used to build the `hudi-spark3-bundle`.

:::note
In 0.14.0, we introduce the support for Spark 3.4.x and add back the support for Spark 3.0.x.
In 0.12.0, we introduce the experimental support for Spark 3.3.0.
In 0.11.0, there are changes on using Spark bundles, please refer
to [0.11.0 release notes](https://hudi.apache.org/releases/release-0.11.0/#spark-versions-and-bundles) for detailed
instructions.
:::

<Tabs
groupId="programming-language"
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
# For Spark versions: 3.2 - 3.4
export SPARK_VERSION=3.4 # Can be any of [3.2, 3.3, 3.4]
spark-shell \
  --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export SPARK_VERSION=3.1 # Can be any of [3.0, 3.1]
spark-shell \
  --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
spark-shell \
  --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
</TabItem>

<TabItem value="python">

From the extracted directory run pyspark with Hudi:

```shell
# For Spark versions: 3.2 - 3.4
export PYSPARK_PYTHON=$(which python3)
export SPARK_VERSION=3.4 # Can be any of [3.2, 3.3, 3.4]
pyspark \
--packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export PYSPARK_PYTHON=$(which python3)
export SPARK_VERSION=3.1 # Can be any of [3.0, 3.1]
pyspark \
--packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
export PYSPARK_PYTHON=$(which python3)
pyspark \
--packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
</TabItem>

<TabItem value="sparksql">

Hudi support using Spark SQL to write and read data with the **HoodieSparkSessionExtension** sql extension.
From the extracted directory run Spark SQL with Hudi:

```shell
# For Spark versions: 3.2 - 3.4
export SPARK_VERSION=3.4 # Can be any of [3.2, 3.3, 3.4]
spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export SPARK_VERSION=3.1 # Can be any of [3.0, 3.1]
spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
spark-sql --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

</TabItem>

</Tabs
>

:::note Please note the following
<ul>
  <li> For Spark 3.2 and above, the additional spark_catalog config is required: 
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' </li>
  <li> We have used hudi-spark-bundle built for scala 2.12 since the spark-avro module used can also depend on 2.12. </li>
</ul>
:::

Setup table name, base path and a data generator to generate records for this guide.

<Tabs
groupId="programming-language"
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
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.key
gen.constant.KeyGeneratorOptions._
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
<TabItem value="sparksql">

```sql
// Next section will go over create table commands
```

</TabItem>
</Tabs
>

:::tip
The [DataGenerator](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L51) 
can generate sample inserts and updates based on the the sample trip schema [here](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)
:::

## Create Table

Lets take a look at creating a Hudi table. For the purpose of quick start guide, we will go with COW table type which 
is partitioned. You can read more on different table types [here](/docs/table_types).

<Tabs
groupId="programming-language"
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
// No separate create table command required here. First batch of write to a table will create the table if not exists. 
```

</TabItem>
<TabItem value="python">

```python
# pyspark
# No separate create table command required here. First batch of write to a table will create the table if not exists.
```

</TabItem>
<TabItem value="sparksql">

Spark SQL needs an explicit create table command.

**Create Table Properties**

Users can set table properties while creating a Hudi table. Critical options are listed here.

| Parameter Name | Default | Introduction                                                                                                                                                                                                                                                                                                                                     |
|------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primaryKey | uuid | The primary key field names of the table, multiple fields separated by commas. Same as `hoodie.datasource.write.recordkey.field`. If this config is ignored, Hudi will auto generate keys for the records of interest (from 0.14.0).                                                                                                             |
| preCombineField |  | The pre-combine field of the table. Same as `hoodie.datasource.write.precombine.field` and is used for resolving the final version of the record among multiple versions. Generally `event time` or some other similar column will be used for ordering purpose. Hudi will be able to handle out of order data using the preCombine field value. |
| type       | cow | The table type to create. type = 'cow' means a COPY-ON-WRITE table, while type = 'mor' means a MERGE-ON-READ table. Same as `hoodie.datasource.write.table.type`. More details can be found [here](/docs/table_types)                                                                                                                            |

:::note
1. `primaryKey`, `preCombineField`, and `type` are case-sensitive.
2. While setting `primaryKey`, `preCombineField`, `type` or other Hudi configs, `tblproperties` is preferred over `options`.
3. A new Hudi table created by Spark SQL will by default set `hoodie.datasource.write.hive_style_partitioning=true`.
:::

Here is an example of creating a Hudi table.

```sql
-- create a Hudi table that is partitioned.
create table hudi_cow_pt_tbl (
  id bigint,
  name string,
  ts bigint,
  dt string,
  hh string
) using hudi
tblproperties (
  type = 'cow'
 )
partitioned by dt
location '/tmp/hudi/hudi_cow_pt_tbl';
```

Default value for type is 'cow' and hence could be skipped. 

**CTAS**

Hudi supports CTAS (Create Table As Select) on Spark SQL. <br/>
:::note
CTAS uses the **bulk insert** as the write operation for better writer performance.
:::

***Create a Hudi Table using CTAS***

```sql
-- CTAS: create a partitioned Hudi table. 
create table hudi_ctas_cow_pt_tbl
using hudi
tblproperties (
  type = 'cow', 
  preCombineField = 'name'
 )
partitioned by (dt)
as
select 1 as id, 'a1' as name, '2021-12-01'' as dt;
```

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

:::note
For the purpose of quick start guide, we covering just COW table type, partitioned and external tables. For more
options, please refer to [SQL DDL](/docs/next/sql_ddl) and [SQL DML](/docs/next/sql_dml) reference guide.  
:::

</TabItem>

</Tabs
>


## Insert data

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

First insert will also create the table with spark datasource writes.

**Creating a Hudi table**
```scala
// spark-shell
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PARTITIONPATH_FIELD_NAME.key(), "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```

:::note
- Users can explicitly set RECORDKEY_FIELD_NAME.key() config to configure primary keys for a given Hudi table. In the absence of
  this config, Hudi will auto generate record keys (primary keys) for the records being ingested. 
- When primary key configs are set by the user, it is recommended to also configure `PRECOMBINE_FIELD_NAME.key()`.  
:::

:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. For more info, refer to
[Modeling data stored in Hudi](https://hudi.apache.org/docs/faq#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](/docs/next/writing_data).
For a table where primary keys are not configured by user, `bulk_insert` will be chosen as the default operation. If primary key 
configuration (RECORDKEY_FIELD_NAME.key()) is set by the user, `upsert` is chosen as the default operation. If you have a workload 
without updates, you can issue `insert` or `bulk_insert` operations which could be faster. To know more, refer to 
[Write operations](/docs/next/write_operations)
:::

</TabItem>
<TabItem value="python">

Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

**Creating to a Hudi table**
```python
# pyspark
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)
```

:::note
- Users can explicitly set RECORDKEY_FIELD_NAME.key() config to configure primary keys for a given Hudi table. In the absence of
  this config, Hudi will auto generate record keys (primary keys) for the records being ingested.
- When primary key configs are set by the user, it is recommended to also configure `PRECOMBINE_FIELD_NAME.key()`.  
:::

:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. For more info, refer to
[Modeling data stored in Hudi](https://hudi.apache.org/docs/faq#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](/docs/next/writing_data).
For a table where primary keys are not configured by user, `bulk_insert` will be chosen as the default operation. If primary key
configuration (RECORDKEY_FIELD_NAME.key()) is set by the user, `upsert` is chosen as the default operation. If you have a workload
without updates, you can issue `insert` or `bulk_insert` operations which could be faster. To know more, refer to
[Write operations](/docs/next/write_operations)
:::

</TabItem>

<TabItem value="sparksql">

Users can use 'INSERT INTO' to insert data into a Hudi table using spark-sql. 

```sql
insert into hudi_cow_pt_tbl select 2, 'a2', '2021-12-02';
```

:::note
- `hoodie.spark.sql.insert.into.operation` will determine how records ingested via spark-sql INSERT INTO will be treated. Possible values are:
  - "bulk_insert": Here, Hudi writes incoming records as is without any automatic small file management.
  - "insert": Here, Hudi inserts the new incoming records and also does small file management.
  - "upsert": Here, Hudi takes upsert flow, where incoming batch will be de-duped before ingest and also merged with previous versions of the record in storage.
  
  For a table without any preCombine key set, "insert" is chosen as the default value for this config. For a table with preCombine key set, 
  "upsert" is chosen as the default value for this config.
- From 0.14.0, `hoodie.sql.bulk.insert.enable` and `hoodie.sql.insert.mode` are depecrated. Users are expected to use `hoodie.spark.sql.insert.into.operation` instead.
- To manage duplicates with `INSERT INTO`, please do check out [insert dup policy config](/docs/next/configurations#hoodiedatasourceinsertduppolicy).
:::

Here are examples to override the spark.sql.insert.into.operation.

```sql
-- upserts using INSERT_INTO 
set hoodie.spark.sql.insert.into.operation = 'upsert' 

insert into hudi_cow_pt_tbl select 2, 'a2', '2021-12-03';
insert into hudi_cow_pt_tbl select 2, 'a2', '2021-12-04';
select id, name, dt from hudi_cow_pt_tbl;
2	a2   	2021-12-04

-- bulk_insert using INSERT_INTO 
set hoodie.spark.sql.insert.into.operation = 'bulk_insert' 

insert into hudi_cow_pt_tbl select 2, 'a2', '2021-12-03';
insert into hudi_cow_pt_tbl select 2, 'a2', '2021-12-04';
select id, name, dt from hudi_cow_pt_tbl;
2	a2   	2021-12-03
2	a2   	2021-12-04
```

</TabItem>

</Tabs
>


Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for various key generator options, like Timestamp based,
complex, custom, NonPartitioned Key gen, etc.


:::tip
With [externalized config file](/docs/next/configurations#externalized-config-file),
instead of directly passing configuration settings to every Hudi job, 
you can also centrally set them in a configuration file `hudi-default.conf`.
:::

## Query data 

Load the data files into a DataFrame.

<Tabs
groupId="programming-language"
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
Since 0.9.0 Hudi has support for a built-in FileIndex: **HoodieFileIndex** to query Hudi table,
which supports partition pruning and metatable for query which helps improve query performance.
It also supports non-global query path which means users can query the table by the base path without
specifing the "*" in the query path. This feature has been enabled by default for the non-global query path.
For the global query path, Hudi uses the old query path.
Refer to [Table types and queries](/docs/next/concepts#table-types--queries) for more info on all table types and query types supported.
:::

## Update data

Lets take a look at how to update existing data for a Hudi table. 

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>

<TabItem value="scala">

```scala
// Lets read data from target Hudi table, modify fare column and update it. 
val updatesDf = spark.read.format("hudi").load(basePath).withColumn("fare",col("fare")*100)

updatesDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_NAME.key(), "ts").
  option(PARTITIONPATH_FIELD_NAME.key(), "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```
[Querying](#query-data) the data again will now show updated trips. Each write operation generates a new [commit](/docs/next/concepts)
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `fare` fields for the same `_hoodie_record_key`s in previous commit.

:::note
- Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
- For a table with Hudi generated primary keys, updates with spark-datasource is feasible when the dataframe being ingested contains Hudi's meta fields.
If not, its recommended to use spark-sql's `Merge Into` and `Update` to update such tables. For a table with user defined primary keys, there are no 
such constraints with updates via spark-datasource (updates would work for a dataframe with just the data columns).
:::

</TabItem>
<TabItem value="sparksql">

Spark SQL supports two kinds of DML to update Hudi table: Merge-Into and Update.

### Update

**Syntax**
```sql
UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]
```

**Example**
```sql
update hudi_cow_pt_tbl set name = 'a1_1' where id = 1;

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
-- source table using Hudi for testing merging into target Hudi table
create table merge_source (id int, name string, dt string) using hudi
tblproperties (primaryKey = 'id', preCombineField = 'name');
insert into merge_source values (1, 'a1_new', 2021-12-05), (2, "a2_new", 2021-12-05), (3, "a3_new", 2021-12-05);

merge into hudi_cow_pt_tbl as target
using merge_source as source
on target.id = source.id
when matched then update set *
when not matched then insert *
;

```

:::note
For a Hudi table with user defined primary keys, the join condition in `Merge Into` is expected to contain the primary keys of the table.
For a Hudi table with Hudi generated primary keys, the join condition in `Merge Into` can be on any arbitrary data columns.
:::


</TabItem>
<TabItem value="python">

```python
# pyspark

// Lets read data from target Hudi table, modify fare column and update it. 
val updatesDf = spark.read.format("hudi").load(basePath).withColumn("fare",col("fare")*100)

updatesDf.write.format("hudi"). \
  options(**hudi_options). \
  mode("append"). \
  save(basePath)
```

[Querying](#query-data) the data again will now show updated trips. Each write operation generates a new [commit](/docs/next/concepts)
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `fare` fields for the same `_hoodie_record_key`s in previous commit.
:::note
- Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
- For a table with Hudi generated primary keys, updates with spark-datasource is feasible when the dataframe being ingested contains Hudi's meta fields.
  If not, its recommended to use spark-sql's `Merge Into` and `Update` to update such tables. For a table with user defined primary keys, there are no
  such constraints with updates via spark-datasource (updates would work for a dataframe with just the data columns).
:::

</TabItem>

</Tabs
>


## Delete data {#deletes}

Delete operation removes/deletes the records  of interest from the table. For example, this code snippet deletes records
for the HoodieKeys passed in. Check out the [deletion section](/docs/next/writing_data#deletes) for more details.

<Tabs
groupId="programming-language"
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
val deletesDf = spark.read.format("hudi").load(basePath).limit(2)

deletesDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "delete").
  option(PARTITIONPATH_FIELD_NAME.key(), "partitionpath").
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
- Only `Append` mode is supported for delete operation.
- For a table with Hudi generated primary keys, deletes with spark-datasource is feasible when the dataframe being ingested contains Hudi's meta fields.
If not, its recommended to use spark-sql's `Delete` to update such tables. For a table with user defined primary keys, there are no
such constraints with deleteing via spark-datasource (deletes would work for a dataframe with just the data columns).
:::

</TabItem>
<TabItem value="sparksql">

**Syntax**
```sql
DELETE FROM tableIdentifier [ WHERE BOOL_EXPRESSION]
```
**Example**
```sql
DELETE FROM hudi_cow_pt_tbl where id = 1;
```

</TabItem>

<TabItem value="python">

```python
# pyspark

val deletesDf = spark.read.format("hudi").load(basePath).limit(2)

# issue deletes
hudi_hard_delete_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.partitionpath.field': 'partitionpath',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'delete',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

from pyspark.sql.functions import lit

val hard_delete_df = spark.read.format("hudi").load(basePath).limit(2)

# run the same read query as above.
roAfterDeleteViewDF = spark. \
  read. \
  format("hudi"). \
  load(basePath)
```

```python
roAfterDeleteViewDF.createOrReplaceTempView("hudi_trips_snapshot") 

# fetch should return (total - 2) records
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
```

:::note
- Only `Append` mode is supported for delete operation.
- For a table with Hudi generated primary keys, deletes with spark-datasource is feasible when the dataframe being ingested contains Hudi's meta fields.
  If not, its recommended to use spark-sql's `Delete` to update such tables. For a table with user defined primary keys, there are no
  such constraints with deleteing via spark-datasource (deletes would work for a dataframe with just the data columns).
:::

</TabItem>

</Tabs
>

## Advanced Write operations
Apart from inserts, updates and deletes, Apache Hudi supports plethora of write operations like bulk_insert, insert_overwrite, 
delete_partition etc to cater to different needs of the users. Please refer [here](/docs/next/writing_data) for more 
advanced write operations.

## Advanced Query types
In addition to snapshot query, Apache Hudi also offers additional querying capabilities. 

### Time Travel Query

Hudi supports time travel query since 0.9.0. Currently three query time formats are supported as given below.

<Tabs
groupId="programming-language"
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
  option("as.of.instant", "2021-07-28 14:11:08.000"). \
  load(basePath)

# It is equal to "as.of.instant = 2021-07-28 00:00:00"
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

-- time travel based on commit time, for eg: `20220307091628793`
select * from hudi_cow_pt_tbl timestamp as of '20220307091628793' where id = 1;
-- time travel based on different timestamp formats
select * from hudi_cow_pt_tbl timestamp as of '2022-03-07 09:16:28.100' where id = 1;
select * from hudi_cow_pt_tbl timestamp as of '2022-03-08' where id = 1;
```

</TabItem>

</Tabs
>


### Incremental query

Hudi also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's incremental querying by providing a begin time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', }
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
  option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME.key(), beginTime).
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

<TabItem value="sparksql">

```sql
-- syntax
hudi_table_changes(table or path, queryType, beginTime [, endTime]);  
-- table or path: table identifier, example: db.tableName, tableName, 
--                or path for of your table, example: path/to/hudiTable  
--                in this case table does not need to exist in the metastore,
-- queryType: incremental query mode, example: latest_state, cdc  
--            (for cdc query, first enable cdc for your table by setting cdc.enabled=true),
-- beginTime: instantTime to begin query from, example: earliest, 202305150000, 
-- endTime: optional instantTime to end query at, example: 202305160000, 

-- incrementally query data by table name
-- start from earliest available commit, end at latest available commit.  
select * from hudi_table_changes('db.table', 'latest_state', 'earliest');

-- start from earliest, end at 202305160000.  
select * from hudi_table_changes('table', 'latest_state', 'earliest', '202305160000');  

-- start from 202305150000, end at 202305160000.
select * from hudi_table_changes('table', 'latest_state', '202305150000', '202305160000');

-- incrementally query data by path
-- start from earliest available commit, end at latest available commit.
select * from hudi_table_changes('path/to/table', 'cdc', 'earliest');

-- start from earliest, end at 202305160000.
select * from hudi_table_changes('path/to/table', 'cdc', 'earliest', '202305160000');

-- start from 202305150000, end at 202305160000.
select * from hudi_table_changes('path/to/table', 'cdc', '202305150000', '202305160000');

```

</TabItem>

</Tabs
>

:::info
This will give all changes that happened after the beginTime commit with the filter of fare > 20.0. The unique thing about this
feature is that it now lets you author streaming pipelines on batch data.
:::

## Streaming writers
### Hudi Streamer
Hudi provides an ingestion tool - HoodieStreamer, to assist with ingesting data into Hudi from various different sources in a streaming manner. 
This has lot of niceties like auto checkpointing, schema enforcement via schema provider, transformation support and so on.
Please refer to [here](/docs/next/hoodie_streaming_ingestion#hudi-streamer) for more info. 

### Structured Streaming

Hudi supports Spark Structured Streaming reads and writes as well. Please refer to [here](/docs/next/hoodie_streaming_ingestion#structured-streaming) for more info.

## More of Spark SQL

For advanced usage of spark SQL, please refer to [Spark SQL DDL](/docs/next/sql_ddl) and [Spark SQL DML](/docs/next/sql_dml) reference guides. 

### Alter tables
For alter table commands, check out [this](/docs/next/sql_ddl#spark-alter-table).

### Procedures

If you are interested in trying out Stored procedures available when use Hudi SparkSQL to assist with monitoring,
managing and operationalizing Hudi tables, please check [this](/docs/next/procedures) out.

## Where to go from here?

You can also do the quickstart by [building hudi yourself](https://github.com/apache/hudi#building-apache-hudi-from-source), 
and using `--jars <path to hudi_code>/packaging/hudi-spark-bundle/target/hudi-spark3.2-bundle_2.1?-*.*.*-SNAPSHOT.jar` in the spark-shell command above
instead of `--packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0`. Hudi also supports scala 2.12. Refer [build with scala 2.12](https://github.com/apache/hudi#build-with-different-spark-versions)
for more info.

Also, we used Spark here to show case the capabilities of Hudi. However, Hudi can support multiple table types/query types and 
Hudi tables can be queried from query engines like Hive, Spark, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/next/docker_demo) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/next/migration_guide).
