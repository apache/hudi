---
title: "Virtual keys support in Hudi"
excerpt: "Supporting Virtual keys in Hudi by reducing storage overhead"
author: shivnarayan
category: blog
---

Apache Hudi helps you build and manage data lakes with different table types, config knobs to cater to everyone's need.
Hudi adds per record metadata like the record key, partition path, commit time etc which serves multiple purpose. 
This assists in avoiding re-computing the record key, partition path during merges, compaction and other table operations 
and also assists in supporting incremental queries. But one of the repeated asks from the community is to leverage 
existing fields and not to add additional meta fields. So, Hudi is adding Virtual keys support to cater to such needs. 
<!--truncate-->

# Virtual key support
Hudi now supports Virtual keys, where Hudi meta fields can be computed on demand from existing user
fields for all records. In regular path, these are computed once and stored as per record metadata and re-used during 
various operations like merging incoming records to those in storage, compaction, etc. Hudi also stores commit time at 
record level to support incremental queries. If one does not need incremental support, they can start leverageing 
Hudi's Virutal key support and still go about using Hudi to build and manage their data lake to reduce the storage 
overhead due to per record metadata. 

## Configurations
Virtual keys can be enabled for a given table using the below config. When disabled, 
Hudi will enforce virtual keys for the corresponding table. Default value for this config is true, which means, all 
meta fields will be added by default. <br/> <br/>
`"hoodie.populate.meta.fields"`

Note: 
Once virtual keys are enabled, it can't be disabled for a given hudi table, because already stored records may not have 
the meta fields populated. But if you have an existing table from an older version of hudi, virtual keys can be enabled. 
Just that going back is not feasible. 
Another constraint wrt virtual key support is that, Key generator properties for a given table cannot be changed through
the course of the lifecycle of a given hudi table.
For instance, if you configure record key to point to field5 for few batches of write and later switch to field10, 
it may not pan out well with hudi table where virtual keys are enabled. 

As its evident, record keys and partition path will have to be re-computed everytime when in need (merges, compaction, 
MOR snapshot read). Hence we are supporting only built-in key generators with Virtual Keys for COW table type. Incase of 
MOR, we support only SimpleKeyGenerator (i.e. both record key and partition path has to refer
to an existing user field ) for now. If we zoom into Merge On Read table's snapshot query, hudi does real time merging of base 
data file with records from delta log files and hence query latencies will shoot up if we were to support all different
types of key generators. 

### Supported Key Generators with CopyOnWrite(COW) table:
SimpleKeyGenerator, ComplexKeyGenerator, CustomKeyGenerator, TimestampBasedKeyGenerator and NonPartitionedKeyGenerator. 

### Supported Key Generators with MergeOnRead(MOR) table:
SimpleKeyGenerator

### Supported Index types: 
Only "SIMPLE" and "GLOBAL_SIMPLE" index types are supported in the first cut. We plan to add support for other index 
(BLOOM, etc) in future releases. 

## Supported Operations
Good news is that, all existing operations are supported for a hudi table with virtual keys except the incremental 
query support. Which means, cleaning, archiving, metadata table, clustering, etc can be enabled for a hudi table with 
virtual keys enabled. So, if one's requirement fits into this model, would recommend using virtual keys as it reduces 
the storage overhead. 

## Code snippet
We can go through our quick start and see how it plays out when virtual keys are enabled.

### Inserts
```
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

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME.key(), tableName).
  option("hoodie.populate.meta.fields", "false").
  option("hoodie.index.type","SIMPLE").
  mode(Overwrite).
  save(basePath)
```

### Query
```
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath + "/*/*/*/*")
//load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
```

```
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
```

#### Output
```
+------------------+-------------------+-------------------+-------------+
|              fare|          begin_lon|          begin_lat|           ts|
+------------------+-------------------+-------------------+-------------+
| 27.79478688582596| 0.6273212202489661|0.11488393157088261|1628951609798|
| 93.56018115236618|0.14285051259466197|0.21624150367601136|1629012489526|
| 33.92216483948643| 0.9694586417848392| 0.1856488085068272|1629163264651|
| 64.27696295884016| 0.4923479652912024| 0.5731835407930634|1628701606278|
|  43.4923811219014| 0.8779402295427752| 0.6100070562136587|1628787101240|
| 66.62084366450246|0.03844104444445928| 0.0750588760043035|1628802740084|
|34.158284716382845|0.46157858450465483| 0.4726905879569653|1629018593339|
| 41.06290929046368| 0.8192868687714224|  0.651058505660742|1629131594334|
+------------------+-------------------+-------------------+-------------+
```

```
spark.sql("select uuid, partitionpath, rider, driver, fare from  hudi_trips_snapshot").show(false)
```

#### Output
```
+------------------------------------+------------------------------------+---------+----------+------------------+
|uuid                                |partitionpath                       |rider    |driver    |fare              |
+------------------------------------+------------------------------------+---------+----------+------------------+
|eb7819f1-6f04-429d-8371-df77620b9527|americas/united_states/san_francisco|rider-213|driver-213|27.79478688582596 |
|37ea44f1-fda7-4ec4-84de-f43f5b5a4d84|americas/united_states/san_francisco|rider-213|driver-213|19.179139106643607|
|aa601d6b-7cc5-4b82-9687-675d0081616e|americas/united_states/san_francisco|rider-213|driver-213|93.56018115236618 |
|494bc080-881c-48be-8f8a-8f1739781816|americas/united_states/san_francisco|rider-213|driver-213|33.92216483948643 |
|09573277-e1c1-4cdd-9b45-57176f184d4d|americas/united_states/san_francisco|rider-213|driver-213|64.27696295884016 |
|c9b055ed-cd28-4397-9704-93da8b2e601f|americas/brazil/sao_paulo           |rider-213|driver-213|43.4923811219014  |
|e707355a-b8c0-432d-a80f-723b93dc13a8|americas/brazil/sao_paulo           |rider-213|driver-213|66.62084366450246 |
|d3c39c9e-d128-497a-bf3e-368882f45c28|americas/brazil/sao_paulo           |rider-213|driver-213|34.158284716382845|
|159441b0-545b-460a-b671-7cc2d509f47b|asia/india/chennai                  |rider-213|driver-213|41.06290929046368 |
|16031faf-ad8d-4968-90ff-16cead211d3c|asia/india/chennai                  |rider-213|driver-213|17.851135255091155|
+------------------------------------+------------------------------------+---------+----------+------------------+
```

```
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
```

#### Output
```
+-------------------+------------------+----------------------+---------+----------+------------------+
|_hoodie_commit_time|_hoodie_record_key|_hoodie_partition_path|    rider|    driver|              fare|
+-------------------+------------------+----------------------+---------+----------+------------------+
|               null|              null|                  null|rider-213|driver-213|19.179139106643607|
|               null|              null|                  null|rider-213|driver-213| 33.92216483948643|
|               null|              null|                  null|rider-213|driver-213| 27.79478688582596|
|               null|              null|                  null|rider-213|driver-213| 64.27696295884016|
|               null|              null|                  null|rider-213|driver-213| 93.56018115236618|
|               null|              null|                  null|rider-213|driver-213| 66.62084366450246|
|               null|              null|                  null|rider-213|driver-213|  43.4923811219014|
|               null|              null|                  null|rider-213|driver-213|34.158284716382845|
|               null|              null|                  null|rider-213|driver-213|17.851135255091155|
|               null|              null|                  null|rider-213|driver-213| 41.06290929046368|
+-------------------+------------------+----------------------+---------+----------+------------------+
```
Note: all meta fields are null in storage.

### Updates
```
val updates = convertToStringList(dataGen.generateUpdates(10))
val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME.key(), tableName).
  option("hoodie.populate.meta.fields", "false").
  option("hoodie.index.type","SIMPLE").
  mode(Append).
  save(basePath)
```

#### Query
```
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath + "/*/*/*/*")
//load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
```

```
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
```

#### Output
```
+------------------+-------------------+-------------------+-------------+
|              fare|          begin_lon|          begin_lat|           ts|
+------------------+-------------------+-------------------+-------------+
|  98.3428192817987| 0.3349917833248327| 0.4777395067707303|1628817512031|
| 93.56018115236618|0.14285051259466197|0.21624150367601136|1629012489526|
|  90.9053809533154|0.19949323322922063|0.18294079059016366|1628892817879|
|49.527694252432056| 0.5142184937933181| 0.7340133901254792|1629024386800|
|  43.4923811219014| 0.8779402295427752| 0.6100070562136587|1628787101240|
| 63.72504913279929|  0.888493603696927| 0.6570857443423376|1628741998684|
| 91.99515909032544| 0.2783086084578943| 0.2110206104048945|1628958326565|
| 90.25710109008239| 0.4006983139989222|0.08528650347654165|1629259201710|
+------------------+-------------------+-------------------+-------------+
```

```
spark.sql("select uuid, partitionpath, rider, driver, fare from  hudi_trips_snapshot").show(false)
```

#### Output
```
+------------------------------------+------------------------------------+---------+----------+------------------+
|uuid                                |partitionpath                       |rider    |driver    |fare              |
+------------------------------------+------------------------------------+---------+----------+------------------+
|eb7819f1-6f04-429d-8371-df77620b9527|americas/united_states/san_francisco|rider-284|driver-284|98.3428192817987  |
|37ea44f1-fda7-4ec4-84de-f43f5b5a4d84|americas/united_states/san_francisco|rider-213|driver-213|19.179139106643607|
|aa601d6b-7cc5-4b82-9687-675d0081616e|americas/united_states/san_francisco|rider-213|driver-213|93.56018115236618 |
|494bc080-881c-48be-8f8a-8f1739781816|americas/united_states/san_francisco|rider-284|driver-284|90.9053809533154  |
|09573277-e1c1-4cdd-9b45-57176f184d4d|americas/united_states/san_francisco|rider-284|driver-284|49.527694252432056|
|c9b055ed-cd28-4397-9704-93da8b2e601f|americas/brazil/sao_paulo           |rider-213|driver-213|43.4923811219014  |
|e707355a-b8c0-432d-a80f-723b93dc13a8|americas/brazil/sao_paulo           |rider-284|driver-284|63.72504913279929 |
|d3c39c9e-d128-497a-bf3e-368882f45c28|americas/brazil/sao_paulo           |rider-284|driver-284|91.99515909032544 |
|159441b0-545b-460a-b671-7cc2d509f47b|asia/india/chennai                  |rider-284|driver-284|9.384124531808036 |
|16031faf-ad8d-4968-90ff-16cead211d3c|asia/india/chennai                  |rider-284|driver-284|90.25710109008239 |
+------------------------------------+------------------------------------+---------+----------+------------------+
```

### Deletes

```
// spark-shell
// fetch total records count
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
```

#### Output
```
res8: Long = 10
```

```
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
  option(TABLE_NAME.key(), tableName).
  option("hoodie.populate.meta.fields", "false").
  option("hoodie.index.type","SIMPLE").
  mode(Append).
  save(basePath)
```

#### Query
```
// run the same read query as above.
val roAfterDeleteViewDF = spark.
read.
format("hudi").
load(basePath + "/*/*/*/*")

roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
// fetch should return (total - 2) records
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
```

#### Output
```aidl
res11: Long = 8
```
Note: Before deletes, there were 10 records and now we have 8 records (as 2 were deleted). 

## Conclusion 
Virtual keys will definitely be beneficial given your requirements adheres to the model listed above. Hope this blog 
was useful for you to learn yet another feature in Apache Hudi. If you are interested in 
Hudi and looking to contribute, do check out [here](https://hudi.apache.org/contribute/get-involved). 








