---
version: 0.5.0
title: "Quick-Start Guide"
permalink: /docs/0.5.0-quick-start-guide.html
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

This guide provides a quick peek at Hudi's capabilities using spark-shell. Using Spark datasources, we will walk through 
code snippets that allows you to insert and update a Hudi dataset of default storage type: 
[Copy on Write](/docs/0.5.0-concepts.html#copy-on-write-storage). 
After each write operation we will also show how to read the data both snapshot and incrementally.

## Setup spark-shell

Hudi works with Spark-2.x versions. You can follow instructions [here](https://spark.apache.org/downloads.html) for setting up spark. 
From the extracted directory run spark-shell with Hudi as:

```scala
bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.0-incubating \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```

Setup table name, base path and a data generator to generate records for this guide.

```scala
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val tableName = "hudi_cow_table"
val basePath = "file:///tmp/hudi_cow_table"
val dataGen = new DataGenerator
```

The [DataGenerator](https://github.com/apache/incubator-hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L50) 
can generate sample inserts and updates based on the the sample trip schema [here](https://github.com/apache/incubator-hudi/blob/master/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L57)
{: .notice--info}


## Insert data

Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.

```scala
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("org.apache.hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, tableName).
    mode(Overwrite).
    save(basePath);
``` 

`mode(Overwrite)` overwrites and recreates the dataset if it already exists.
You can check the data generated under `/tmp/hudi_cow_table/<region>/<country>/<city>/`. We provided a record key 
(`uuid` in [schema](#sample-schema)), partition field (`region/county/city`) and combine logic (`ts` in 
[schema](#sample-schema)) to ensure trip records are unique within each partition. For more info, refer to 
[Modeling data stored in Hudi](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Datasets](/docs/0.5.0-writing_data.html).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue 
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](/docs/0.5.0-writing_data#write-operations)
{: .notice--info}
 
## Query data 

Load the data files into a DataFrame.

```scala
val roViewDF = spark.
    read.
    format("org.apache.hudi").
    load(basePath + "/*/*/*/*")
roViewDF.createOrReplaceTempView("hudi_ro_table")
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show()
```

This query provides a read optimized view of the ingested data. Since our partition path (`region/country/city`) is 3 levels nested 
from base path we ve used `load(basePath + "/*/*/*/*")`. 
Refer to [Storage Types and Views](/docs/0.5.0-concepts#storage-types--views) for more info on all storage types and views supported.
{: .notice--info}

## Update data

This is similar to inserting new data. Generate updates to existing trips using the data generator, load into a DataFrame 
and write DataFrame into the hudi dataset.

```scala
val updates = convertToStringList(dataGen.generateUpdates(10))
val df = spark.read.json(spark.sparkContext.parallelize(updates, 2));
df.write.format("org.apache.hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, tableName).
    mode(Append).
    save(basePath);
```

Notice that the save mode is now `Append`. In general, always use append mode unless you are trying to create the dataset for the first time.
[Querying](#query-data) the data again will now show updated trips. Each write operation generates a new [commit](http://hudi.incubator.apache.org/concepts.html) 
denoted by the timestamp. Look for changes in `_hoodie_commit_time`, `rider`, `driver` fields for the same `_hoodie_record_key`s in previous commit. 
{: .notice--info}

## Incremental query

Hudi also provides capability to obtain a stream of records that changed since given commit timestamp. 
This can be achieved using Hudi's incremental view and providing a begin time from which changes need to be streamed. 
We do not need to specify endTime, if we want all changes after the given commit (as is the common case). 

```scala
// reload data
spark.
    read.
    format("org.apache.hudi").
    load(basePath + "/*/*/*/*").
    createOrReplaceTempView("hudi_ro_table")

val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
val beginTime = commits(commits.length - 2) // commit time we are interested in

// incrementally query data
val incViewDF = spark.
    read.
    format("org.apache.hudi").
    option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL).
    option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
    load(basePath);
incViewDF.registerTempTable("hudi_incr_table")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()
``` 

This will give all changes that happened after the beginTime commit with the filter of fare > 20.0. The unique thing about this
feature is that it now lets you author streaming pipelines on batch data.
{: .notice--info}

## Point in time query

Lets look at how to query data as of a specific time. The specific time can be represented by pointing endTime to a 
specific commit time and beginTime to "000" (denoting earliest possible commit time). 

```scala
val beginTime = "000" // Represents all commits > this time.
val endTime = commits(commits.length - 2) // commit time we are interested in

//incrementally query data
val incViewDF = spark.read.format("org.apache.hudi").
    option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL).
    option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
    option(END_INSTANTTIME_OPT_KEY, endTime).
    load(basePath);
incViewDF.registerTempTable("hudi_incr_table")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()
```

## Where to go from here?

You can also do the quickstart by [building hudi yourself](https://github.com/apache/incubator-hudi#building-apache-hudi-from-source), 
and using `--jars <path to hudi_code>/packaging/hudi-spark-bundle/target/hudi-spark-bundle-*.*.*-SNAPSHOT.jar` in the spark-shell command above
instead of `--packages org.apache.hudi:hudi-spark-bundle:0.5.0-incubating`

Also, we used Spark here to show case the capabilities of Hudi. However, Hudi can support multiple storage types/views and 
Hudi datasets can be queried from query engines like Hive, Spark, Presto and much more. We have put together a 
[demo video](https://www.youtube.com/watch?v=VhNgUsxdrD0) that show cases all of this on a docker based setup with all 
dependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following 
steps [here](/docs/0.5.0-docker_demo.html) to get a taste for it. Also, if you are looking for ways to migrate your existing data 
to Hudi, refer to [migration guide](/docs/0.5.0-migration_guide.html). 
