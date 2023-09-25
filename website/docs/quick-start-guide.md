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

### Spark 3 Support Matrix

| Hudi            | Supported Spark 3 version                         |
|:----------------|:--------------------------------------------------|
| 0.14.x          | 3.4.x (default build), 3.3.x, 3.2.x, 3.1.x, 3.0.x |
| 0.13.x          | 3.3.x (default build), 3.2.x, 3.1.x               |
| 0.12.x          | 3.3.x (default build), 3.2.x, 3.1.x               |
| 0.11.x          | 3.2.x (default build, Spark bundle only), 3.1.x   |
| 0.10.x          | 3.1.x (default build), 3.0.x                      |
| 0.7.0 - 0.9.0   | 3.0.x                                             |
| 0.6.0 and prior | not supported                                     |

The *default build* Spark version indicates how we build `hudi-spark3-bundle`.

:::note Change summary
In 0.14.0, we introduced the support for Spark 3.4.x and bring back the support for Spark 3.0.x.
In 0.12.0, we introduced the experimental support for Spark 3.3.0.
In 0.11.0, there are changes on using Spark bundles, please refer to [0.11.0 release notes](https://hudi.apache.org/releases/release-0.11.0/#spark-versions-and-bundles) for detailed instructions.
:::

### Spark Shell/SQL

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
export SPARK_VERSION=3.4
spark-shell --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export SPARK_VERSION=3.1
spark-shell --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
spark-shell --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
</TabItem>

<TabItem value="python">

From the extracted directory run pyspark with Hudi:

```shell
# For Spark versions: 3.2 - 3.4
export PYSPARK_PYTHON=$(which python3)
export SPARK_VERSION=3.4
pyspark --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export PYSPARK_PYTHON=$(which python3)
export SPARK_VERSION=3.1
pyspark --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
export PYSPARK_PYTHON=$(which python3)
pyspark --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
</TabItem>

<TabItem value="sparksql">

Hudi support using Spark SQL to write and read data with the **HoodieSparkSessionExtension** sql extension.
From the extracted directory run Spark SQL with Hudi:

```shell
# For Spark versions: 3.2 - 3.4
export SPARK_VERSION=3.4
spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark versions: 3.0 - 3.1
export SPARK_VERSION=3.1
spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
```shell
# For Spark version: 2.4
spark-sql --packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

</TabItem>

</Tabs
>

:::note for Spark 3.2 and higher versions
Use scala 2.12 builds with an additional config: --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
:::

### Data Generation

The [DataGenerator](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L51) can generate sample inserts and updates based on the sample trip schema [here](https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58).
Below, we setup table name, base path and a data generator to generate records for this guide.

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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord

val tableName = "hudi_table"
val basePath = "file:///tmp/hudi_trips_table"
val dataGen = new DataGenerator
```

</TabItem>
<TabItem value="python">

```python
# pyspark
tableName = "hudi_table"
basePath = "file:///tmp/hudi_trips_table"
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

## Create Table

First, let's create a Hudi table. Here, we use a partitioned table for illustration, but Hudi also supports non-partitioned tables.

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
// First commit will auto-initialize the table, if it did not exist in the specified base path. 
```

</TabItem>
<TabItem value="python">

```python
# pyspark
# First commit will auto-initialize the table, if it did not exist in the specified base path. 
```

</TabItem>
<TabItem value="sparksql">



Here is an example of creating a Hudi table.

```sql
-- create a Hudi table that is partitioned.
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
PARTITIONED BY (city);
```

For more options for creating Hudi tables, please refer to [SQL DDL](/docs/sql_ddl) reference guide.  

</TabItem>

</Tabs
>


## Insert data {#inserts}

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

Generate some new records as a DataFrame and write the DataFrame into a Hudi table.
Since, this is the first write, it will also auto-create the table. 

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

:::info Mapping to Hudi write operations
Hudi provides a wide range of [write operations](/docs/next/write_operations) - both batch and incremental - to write data into Hudi tables,
with different semantics and performance. When record keys are not configured (see [keys](#keys) below), `bulk_insert` will be chosen as 
the write operation, matching the out-of-behavior of Spark's Parquet Datasource. 
:::

</TabItem>
<TabItem value="python">

Generate some new records as a DataFrame and write the DataFrame into a Hudi table.
Since, this is the first write, it will also auto-create the table.

```python
# pyspark
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName
}

df.write.format("hudi").
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)
```

:::info Mapping to Hudi write operations
Hudi provides a wide range of [write operations](/docs/next/write_operations) - both batch and incremental - to write data into Hudi tables,
with different semantics and performance. When record keys are not configured (see [keys](#keys) below), `bulk_insert` will be chosen as
the write operation, matching the out-of-behavior of Spark's Parquet Datasource.
:::

</TabItem>

<TabItem value="sparksql">

Users can use 'INSERT INTO' to insert data into a Hudi table. See [Insert Into](/docs/sql_dml#insert-into) for more advanced options.

```sql
INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'    ),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'    ),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'      ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
```

If you want to control the Hudi write operation used for the INSERT statement, you can set the following config before issuing 
the INSERT statement:

```sql
-- bulk_insert using INSERT_INTO 
SET hoodie.spark.sql.insert.into.operation = 'bulk_insert' 
```

</TabItem>

</Tabs
>

## Query data {#querying}

Hudi tables can be queried back into a DataFrame or Spark SQL. 

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
val tripsSnapshotDF = spark.read.format("hudi").load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("SELECT fare, begin_lon, begin_lat, ts FROM  hudi_trips_snapshot WHERE fare > 20.0").show()
spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM  hudi_trips_snapshot").show()
```
</TabItem>
<TabItem value="python">

```python
# pyspark
tripsSnapshotDF = spark.read.format("hudi").load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("SELECT fare, begin_lon, begin_lat, ts FROM  hudi_trips_snapshot WHERE fare > 20.0").show()
spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM  hudi_trips_snapshot").show()
```
</TabItem>
<TabItem value="sparksql">

```sql
 SELECT ts, fare, rider, driver, city FROM  hudi_table WHERE fare > 20.0;
```
</TabItem>

</Tabs
>

## Update data {#upserts}

Hudi tables can be updated by streaming in a DataFrame or using a standard UPDATE statement.

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
val updatesDf = spark.read.format("hudi").load(basePath).withColumn("fare", col("fare") * 100)

updatesDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_NAME.key(), "ts").
  option(OPERATION_OPT_KEY, "upsert").
  option(PARTITIONPATH_FIELD_NAME.key(), "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```

:::info Key requirements
Updates with spark-datasource is feasible only when the source dataframe contains Hudi's meta fields or a [key field](#keys) is configured.
Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
:::



</TabItem>
<TabItem value="sparksql">

Hudi table can be update using a regular UPDATE statement. See [Update](/docs/sql_dml#update) for more advanced options.

```sql
UPDATE hudi_table SET fare = 25.0 WHERE rider = 'rider-D';
```

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

:::info Key requirements
Updates with spark-datasource is feasible only when the source dataframe contains Hudi's meta fields or a [key field](#keys) is configured.
Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the table for the first time.
:::

</TabItem>

</Tabs
>

[Querying](#query-data) the data again will now show updated records. Each write operation generates a new [commit](/docs/next/concepts).
Look for changes in `_hoodie_commit_time`, `fare` fields for the given `_hoodie_record_key` value from a previous commit.

## Merging Data {#merge}

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
```
</TabItem>

<TabItem value="python">

```scala
    // spark-shell
```
</TabItem>

<TabItem value="sparksql">

```sql
-- source table using Hudi for testing merging into target Hudi table
CREATE TABLE fare_adjustment (ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING) 
USING HUDI;
INSERT INTO fare_adjustment VALUES 
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',-2.70 ,'san_francisco'),
(1695530237068,'3f3d9565-7261-40e6-9b39-b8aa784f95e2','rider-K','driver-U',64.20 ,'san_francisco'),
(1695241330902,'ea4c36ff-2069-4148-9927-ef8c1a5abd24','rider-H','driver-R',66.60 ,'sao_paulo'    ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',1.85,'chennai'      );


MERGE INTO hudi_table AS target
USING fare_adjustment AS source
ON target.uuid = source.uuid
WHEN MATCHED THEN UPDATE SET target.fare = target.fare + source.fare
WHEN NOT MATCHED THEN INSERT *
;

```

:::info Key requirements
1. For a Hudi table with user defined primary record [keys](#keys), the join condition is expected to contain the primary keys of the table.
For a Hudi table with Hudi generated primary keys, the join condition can be on any arbitrary data columns.
2. For Merge-On-Read tables, partial column updates are not yet supported, i.e. **all columns** need to be SET from a 
MERGE statement either using `SET *` or using `SET column1 = expression1 [, column2 = expression2 ...]`. 
:::
</TabItem>
</Tabs>

## Delete data {#deletes}

Delete operation removes the records specified from the table. For example, this code snippet deletes records
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
val roAfterDeleteViewDF = spark.read.format("hudi").load(basePath)
roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")

// fetch should return (total - 2) records
spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
```

:::info Key requirements
Deletes with spark-datasource is supported only when the source dataframe contains Hudi's meta fields or a [key field](#keys) is configured.
Notice that the save mode is again `Append`. 
:::

</TabItem>
<TabItem value="sparksql">

```sql
DELETE FROM hudi_table WHERE uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';
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
roAfterDeleteViewDF = spark.read.format("hudi").load(basePath)
```

```python
roAfterDeleteViewDF.createOrReplaceTempView("hudi_trips_snapshot") 

# fetch should return (total - 2) records
spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
```
:::info Key requirements
Deletes with spark-datasource is supported only when the source dataframe contains Hudi's meta fields or a [key field](#keys) is configured.
Notice that the save mode is again `Append`.
:::

</TabItem>

</Tabs
>


## Time Travel Query {#timetravel}

Hudi supports time travel query to query the table as of a point-in-time in history. Three timestamp formats are supported as illustrated below.

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
spark.read.format("hudi").
  option("as.of.instant", "20210728141108100").
  load(basePath)

spark.read.format("hudi").
  option("as.of.instant", "2021-07-28 14:11:08.200").
  load(basePath)

// It is equal to "as.of.instant = 2021-07-28 00:00:00"
spark.read.format("hudi").
  option("as.of.instant", "2021-07-28").
  load(basePath)

```

</TabItem>
<TabItem value="python">

```python
#pyspark
spark.read.format("hudi"). \
  option("as.of.instant", "20210728141108100"). \
  load(basePath)

spark.read.format("hudi"). \
  option("as.of.instant", "2021-07-28 14:11:08.000"). \
  load(basePath)

# It is equal to "as.of.instant = 2021-07-28 00:00:00"
spark.read.format("hudi"). \
  option("as.of.instant", "2021-07-28"). \
  load(basePath)
```

</TabItem>
<TabItem value="sparksql">



```sql

-- time travel based on commit time, for eg: `20220307091628793`
SELECT * FROM hudi_table TIMESTAMP AS OF '20220307091628793' WHERE id = 1;
-- time travel based on different timestamp formats
SELECT * FROM hudi_table TIMESTAMP AS OF '2022-03-07 09:16:28.100' WHERE id = 1;
SELECT * FROM hudi_table TIMESTAMP AS OF '2022-03-08' WHERE id = 1;
```
:::note
Requires Spark 3.2+
:::

</TabItem>

</Tabs
>


## Incremental query {#incremental-query}
Hudi provides the unique capability to obtain a set of records that changed between a start and end commit time, providing you with the
"latest state" for each such record as of the end commit time. By default, Hudi tables are configured to support incremental queries, using
record level [metadata tracking](https://hudi.apache.org/blog/2023/05/19/hudi-metafields-demystified).

Below, we fetch changes since a given begin time while the end time defaults to the latest commit on the table. Users can also specify an
end time using `END_INSTANTTIME.key()` option. 

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
spark.read.format("hudi")
  .load(basePath)
  .createOrReplaceTempView("hudi_trips_snapshot")

val commits = spark.sql("SELECT DISTINCT(_hoodie_commit_time) AS commitTime FROM  hudi_trips_snapshot ORDER BY commitTime")
  .map(k => k.getString(0)).take(50)
val beginTime = commits(commits.length - 2) // commit time we are interested in

// incrementally query data
val tripsIncrementalDF = spark.read.format("hudi").
  option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME.key(), beginTime).
  load(basePath)
tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

spark.sql("SELECT `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts FROM  hudi_trips_incremental WHERE fare > 20.0").show()
```

</TabItem>
<TabItem value="python">

```python
# pyspark
# reload data
spark.read.format("hudi"). \
  load(basePath). \
  createOrReplaceTempView("hudi_trips_snapshot")

commits = list(map(lambda row: row[0], spark.sql("SELECT DISTINCT(_hoodie_commit_time) AS commitTime FROM  hudi_trips_snapshot ORDER BY commitTime").limit(50).collect()))
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

spark.sql("SELECT `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts FROM hudi_trips_incremental WHERE fare > 20.0").show()
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
SELECT * FROM hudi_table_changes('db.table', 'latest_state', 'earliest');

-- start from earliest, end at 202305160000.  
SELECT * FROM hudi_table_changes('table', 'latest_state', 'earliest', '202305160000');  

-- start from 202305150000, end at 202305160000.
SELECT * FROM hudi_table_changes('table', 'latest_state', '202305150000', '202305160000');
```

</TabItem>

</Tabs
>


## Change Data Capture Query {#cdc-query}

Hudi also exposes first-class support for Change Data Capture (CDC) queries. CDC queries are useful for applications that need to
obtain all the changes, along with before/after images of records, given a commit time range.

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
```
</TabItem>

<TabItem value="python">

```scala
    // spark-shell
```
</TabItem>

<TabItem value="sparksql">

```sql
-- incrementally query data by path
-- start from earliest available commit, end at latest available commit.
SELECT * FROM hudi_table_changes('path/to/table', 'cdc', 'earliest');

-- start from earliest, end at 202305160000.
SELECT * FROM hudi_table_changes('path/to/table', 'cdc', 'earliest', '202305160000');

-- start from 202305150000, end at 202305160000.
SELECT * FROM hudi_table_changes('path/to/table', 'cdc', '202305150000', '202305160000');
```
</TabItem
>
</Tabs
>

:::info Key requirements
Note that CDC queries are currently only supported on Copy-on-Write tables.
:::

## Table Types 

The examples thus far have showcased one of the two table types, that Hudi supports - Copy-on-Write (COW) tables. Hudi also supports
a more advanced write-optimized table type called Merge-on-Read (MOR) tables, that can balance read and write performance in a more
flexible manner. See [table types](/docs/table_types) for more details.

Any of these examples can be run on a Merge-on-Read table by simply changing the table type to MOR, while creating the table, as below.

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
```
</TabItem>

<TabItem value="python">

```scala
    // spark-shell
```
</TabItem>

<TabItem value="sparksql">

```sql
CREATE TABLE hudi_table (
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI TBLPROPERTIES (type = 'mor')
PARTITIONED BY (city);
```
</TabItem
>
</Tabs
>


## Keys

Hudi also allows users to specify a record key, which will be used to uniquely identify a record within a Hudi table. This is useful and 
critical to support features like indexing and clustering, which speed up upserts and queries respectively, in a consistent manner. Some of the other
benefits of keys are explained in detail [here](https://hudi.apache.org/blog/2023/05/19/hudi-metafields-demystified). To this end, Hudi supports a 
wide range of built-in [key generators](https://hudi.apache.org/blog/2021/02/13/hudi-key-generators), that make it easy to generate record 
keys for a given table. In the absence of a user configured key, Hudi will auto generate record keys, which are highly compressible.

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
```

</TabItem>

<TabItem value="python">

```scala
    // spark-shell
```


</TabItem>

<TabItem value="sparksql">

```sql
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI TBLPROPERTIES (primaryKey = 'uuid')
PARTITIONED BY (city);
```
</TabItem
>
</Tabs
>

:::note Implications of defining record keys
Configuring keys for a Hudi table, has a new implications on the table. If record key is set by the user, `upsert` is chosen as the [write operation](/docs/next/write_operations).
Also if a record key is configured, then it's also advisable to specify a precombine or ordering field, to correctly handle cases where the source data has 
multiple records with the same key. See section below. 
:::

## Ordering Field
Hudi also allows users to specify a _precombine_ field, which will be used to order and resolve conflicts between multiple versions of the same record. This is very important for 
use-cases like applying database CDC logs to a Hudi table, where a given record may be appear multiple times in the source data due to repeated upstream updates. 
Hudi also uses this mechanism to support out-of-order data arrival into a table, where records may need to be resolved in a different order than their commit time. 
For e.g using a _created_at_ timestamp field as the precombine field will prevent older versions of a record from overwriting newer ones or being exposed to queries, even 
if they are written at a later commit time to the table. This is one of the key features, that makes Hudi, best suited for dealing with streaming data.

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
updatesDf.write.format("hudi").
  ...
  option(PRECOMBINE_FIELD_NAME.key(), "ts").
  ...
```

</TabItem>

<TabItem value="python">

```python
    // spark-shell
```


</TabItem>

<TabItem value="sparksql">

```sql
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI TBLPROPERTIES (preCombineField = 'ts')
PARTITIONED BY (city);
```
</TabItem
>
</Tabs
>


## Where to go from here?
You can also [build hudi yourself](https://github.com/apache/hudi#building-apache-hudi-from-source) and try this quickstart using `--jars <path to spark bundle jar>`(see also [build with scala 2.12](https://github.com/apache/hudi#build-with-different-spark-versions))
for more info. If you are looking for ways to migrate your existing data to Hudi, refer to [migration guide](/docs/next/migration_guide).

### Spark SQL Reference

For advanced usage of spark SQL, please refer to [Spark SQL DDL](/docs/next/sql_ddl) and [Spark SQL DML](/docs/next/sql_dml) reference guides. 
For alter table commands, check out [this](/docs/next/sql_ddl#spark-alter-table). Stored procedures provide a lot of powerful capabilities using Hudi SparkSQL to assist with monitoring, managing and operating Hudi tables, please check [this](/docs/next/procedures) out.

### Streaming workloads

Hudi provides industry-leading performance and functionality for streaming data. 

**Hudi Streamer** - Hudi provides an incremental ingestion/ETL tool - [HoodieStreamer](/docs/hoodie_streaming_ingestion#hudi-streamer), to assist with ingesting data into Hudi 
from various different sources in a streaming manner, with powerful built-in capabilities like auto checkpointing, schema enforcement via schema provider, 
transformation support, automatic table services and so on.

**Structured Streaming** - Hudi supports Spark Structured Streaming reads and writes as well. Please see [here](/docs/next/hoodie_streaming_ingestion#structured-streaming) for more.

Check out more information on [modeling data in Hudi](/docs/faq#how-do-i-model-the-data-stored-in-hudi) and different ways to [writing Hudi Tables](/docs/next/writing_data).

### Dockerized Demo
Even as we showcased the core capabilities, Hudi supports a lot more advanced functionality that can make it easy
to get your transactional data lakes up and running quickly, across a variety query engines like Hive, Flink, Spark, Presto, Trino and much more.
We have put together a [demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that showcases all of this on a docker based setup with all
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following
steps [here](/docs/next/docker_demo) to get a taste for it. 

