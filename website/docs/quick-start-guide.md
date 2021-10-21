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
[Copy on Write](/docs/concepts#copy-on-write-table). 
After each write operation we will also show how to read the data both snapshot and incrementally.

## Setup

Hudi works with Spark-2.4.3+ & Spark 3.x versions. You can follow instructions [here](https://spark.apache.org/downloads) for setting up spark.
As of 0.9.0 release, spark-sql dml support has been added and is experimental.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

From the extracted directory run spark-shell with Hudi as:

```scala
// spark-shell for spark 3
spark-shell \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
  
// spark-shell for spark 2 with scala 2.12
spark-shell \
  --packages org.apache.hudi:hudi-spark-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:2.4.4 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
  
// spark-shell for spark 2 with scala 2.11
spark-shell \
  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0,org.apache.spark:spark-avro_2.11:2.4.4 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```

</TabItem>
<TabItem value="sparksql">

Hudi support using spark sql to write and read data with the **HoodieSparkSessionExtension** sql extension.
From the extracted directory run spark-sql with Hudi as:

```shell
# spark sql for spark 3
spark-sql --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'

# spark-sql for spark 2 with scala 2.11
spark-sql --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0,org.apache.spark:spark-avro_2.11:2.4.4 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'

# spark-sql for spark 2 with scala 2.12
spark-sql \
  --packages org.apache.hudi:hudi-spark-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:2.4.4 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
```

</TabItem>
<TabItem value="python">

From the extracted directory run pyspark with Hudi as:

```python
# pyspark
export PYSPARK_PYTHON=$(which python3)

# for spark3
pyspark
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

# for spark2 with scala 2.12
pyspark
--packages org.apache.hudi:hudi-spark-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:2.4.4
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

# for spark2 with scala 2.11
pyspark
--packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0,org.apache.spark:spark-avro_2.11:2.4.4
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```

</TabItem>
</Tabs>

:::note Please note the following
<ul>
  <li>spark-avro module needs to be specified in --packages as it is not included with spark-shell by default</li>
  <li>spark-avro and spark versions must match (we have used 3.0.1 for both above)</li>
  <li>we have used hudi-spark-bundle built for scala 2.12 since the spark-avro module used also depends on 2.12. 
         If spark-avro_2.11 is used, correspondingly hudi-spark-bundle_2.11 needs to be used. </li>
</ul>
:::

Setup table name, base path and a data generator to generate records for this guide.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}>
<TabItem value="scala">

```scala
// spark-shell
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

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
</Tabs>

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
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

```scala
# scala
 // No separate create table command required in spark. First batch of write to a table will create the table if not exists. 
```

</TabItem>
<TabItem value="python">

```python
# pyspark
 // No separate create table command required in spark. First batch of write to a table will create the table if not exists.
```

</TabItem>
<TabItem value="sparksql">

```sql
-- 
create table if not exists hudi_table2(
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'cow'
);
```

</TabItem>
</Tabs>


## Insert data

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
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
(`uuid` in [schema](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L58)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](/docs/writing_data).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](/docs/writing_data#write-operations)
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
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](/docs/writing_data#write-operations)
:::
</TabItem>

<TabItem value="sparksql">

```sql
insert into hudi_table2 select 1, 'a1', 20;

-- insert static partition
insert into hudi_table2 partition(dt = '2021-01-02') select 1, 'a1';

-- insert overwrite table
insert overwrite table h0 select 1, 'a1', 20;
```

**NOTICE**

- Insert mode : Hudi supports two insert modes when inserting data to a table with primary key(we call it pk-table as followed):<br/>
  Using `strict` mode, insert statement will keep the primary key uniqueness constraint for COW table which do not allow 
  duplicate records. If a record already exists during insert, a HoodieDuplicateKeyException will be thrown
  for COW table. For MOR table, updates are allowed to existing record.<br/>
  Using `non-strict` mode, hudi uses the same code path used by `insert` operation in spark data source for the pk-table. <br/>
  One can set the insert mode by using the config: **hoodie.sql.insert.mode**

- Bulk Insert : By default, hudi uses the normal insert operation for insert statements. Users can set **hoodie.sql.bulk.insert.enable** 
   to true to enable the bulk insert for insert statement.

</TabItem>
</Tabs>


Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for various key generator options, like Timestamp based,
complex, custom, NonPartitioned Key gen, etc. 

## Query data 

Load the data files into a DataFrame.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

```scala
// spark-shell
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
//load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
```

### Time Travel Query

Hudi support time travel query since 0.9.0. Currently three query time formats are supported as given below.
```scala
spark.read.
  format("hudi").
  option("as.of.instant", "20210728141108").
  load(basePath)

spark.read.
  format("hudi").
  option("as.of.instant", "2021-07-28 14: 11: 08").
  load(basePath)

// It is equal to "as.of.instant = 2021-07-28 00:00:00"
spark.read.
  format("hudi").
  option("as.of.instant", "2021-07-28").
  load(basePath)

```

:::info
Since 0.9.0 hudi has support a hudi built-in FileIndex: **HoodieFileIndex** to query hudi table,
which supports partition pruning and metatable for query. This will help improve query performance.
It also supports non-global query path which means users can query the table by the base path without
specifing the "*" in the query path. This feature has enabled by default for the non-global query path.
For the global query path, hudi uses the old query path.
Refer to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.
:::
</TabItem>
<TabItem value="sparksql">

```sql
 select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0
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

**Time Travel Query**

Hudi support time travel query since 0.9.0. Currently three query time formats are supported as given below.
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

:::info
Since 0.9.0 hudi has support a hudi built-in FileIndex: **HoodieFileIndex** to query hudi table,
which supports partition pruning and metatable for query. This will help improve query performance.
It also supports non-global query path which means users can query the table by the base path without
specifing the "*" in the query path. This feature has enabled by default for the non-global query path.
For the global query path, hudi uses the old query path.
Refer to [Table types and queries](/docs/concepts#table-types--queries) for more info on all table types and query types supported.
:::
</TabItem>
</Tabs>


## Update data

This is similar to inserting new data. Generate updates to existing trips using the data generator, load into a DataFrame 
and write DataFrame into the hudi table.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
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

Spark sql supports two kinds of DML to update hudi table: Merge-Into and Update.

### MergeInto

**Syntax**

```sql
MERGE INTO tableIdentifier AS target_alias
USING (sub_query | tableIdentifier) AS source_alias
ON <merge_condition>
[ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
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
merge into h0 as target
using (
  select id, name, price, flag from s
) source
on target.id = source.id
when matched then update set *
when not matched then insert *
;

merge into h0
using (
  select id, name, price, flag from s
) source
on h0.id = source.id
when matched and flag != 'delete' then update set id = source.id, name = source.name, price = source.price * 2
when matched and flag = 'delete' then delete
when not matched then insert (id,name,price) values(id, name, price)
;
```
**Notice**

- The merge-on condition can be only on primary keys. Support to merge based on other fields will be added in future.  
- Support for partial updates is supported for cow table.
e.g.
```sql
 merge into h0 using s0
 on h0.id = s0.id
 when matched then update set price = s0.price * 2
```
This works well for Cow-On-Write table which supports update based on the **price** field. 
For Merge-on-Read table this will be supported in the future.
- Target table's fields cannot be the right-value of the update expression for Merge-On-Read table.
e.g.
```sql
 merge into h0 using s0
 on h0.id = s0.id
 when matched then update set id = s0.id, 
                   name = h0.name,
                   price = s0.price + h0.price
```
This works well for Cow-On-Write table, but not yet supported for Merge-On-Read table.

### Update
**Syntax**
```sql
 UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]
```
**Case**
```sql
 update h0 set price = price + 20 where id = 1;
 update h0 set price = price *2, name = 'a2' where id = 2;
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
</Tabs>


## Incremental query

Hudi also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's incremental querying and providing a begin time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}>
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
</Tabs>

:::info
This will give all changes that happened after the beginTime commit with the filter of fare > 20.0. The unique thing about this
feature is that it now lets you author streaming pipelines on batch data.
:::

## Point in time query

Lets look at how to query data as of a specific time. The specific time can be represented by pointing endTime to a 
specific commit time and beginTime to "000" (denoting earliest possible commit time). 

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}>
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
</Tabs>

## Delete data {#deletes}

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
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
val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

df.write.format("hudi").
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
delete from h0 where id = 1;
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
hudi_delete_options = {
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
df = spark.sparkContext.parallelize(deletes).toDF(['uuid', 'partitionpath']).withColumn('ts', lit(0.0))
df.write.format("hudi"). \
  options(**hudi_delete_options). \
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
</Tabs>


See the [deletion section](/docs/writing_data#deletes) of the writing data page for more details.


## Where to go from here?

You can also do the quickstart by [building hudi yourself](https://github.com/apache/hudi#building-apache-hudi-from-source), 
and using `--jars <path to hudi_code>/packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.1?-*.*.*-SNAPSHOT.jar` in the spark-shell command above
instead of `--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0`. Hudi also supports scala 2.12. Refer [build with scala 2.12](https://github.com/apache/hudi#build-with-scala-212)
for more info.

Also, we used Spark here to show case the capabilities of Hudi. However, Hudi can support multiple table types/query types and 
Hudi tables can be queried from query engines like Hive, Spark, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/docker_demo) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/migration_guide). 
