---
title: "Delete support in Hudi"
excerpt: "Deletes are supported at a record level in Hudi with 0.5.1 release. This blog is a “how to” blog on how to delete records in hudi."
author: shivnarayan
category: blog
tags:
- how-to
- delete
- apache hudi
---

Deletes are supported at a record level in Hudi with 0.5.1 release. This blog is a "how to" blog on how to delete records in hudi. Deletes can be done with 3 flavors: Hudi RDD APIs, with Spark data source and with DeltaStreamer.
<!--truncate-->
### Delete using RDD Level APIs

If you have embedded  _HoodieWriteClient_ , then deletion is as simple as passing in a  _JavaRDD&#60;HoodieKey&#62;_ to the delete api.

```java
// Fetch list of HoodieKeys from elsewhere that needs to be deleted
// convert to JavaRDD if required. JavaRDD<HoodieKey> toBeDeletedKeys
List<WriteStatus> statuses = writeClient.delete(toBeDeletedKeys, commitTime);
```

### Deletion with Datasource

Now we will walk through an example of how to perform deletes on a sample dataset using the Datasource API. Quick Start has the same example as below. Feel free to check it out.

**Step 1** : Launch spark shell

```bash
bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.1-incubating \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```
**Step 2** : Import as required and set up table name, etc for sample dataset

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

**Step 3** : Insert data. Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.

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
**Note:** For non-partitioned table, set
  ```
  option(KEYGENERATOR_CLASS_PROP, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
  ```
 Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for more options

**Step 4** : Query data. Load the data files into a DataFrame.

```scala
val roViewDF = spark.read.
  format("org.apache.hudi").
  load(basePath + "/*/*/*/*")
roViewDF.createOrReplaceTempView("hudi_ro_table")
spark.sql("select count(*) from hudi_ro_table").show() // should return 10 (number of records inserted above)
val riderValue = spark.sql("select distinct rider from hudi_ro_table").show()
// copy the value displayed to be used in next step
```

**Step 5** : Fetch records that needs to be deleted, with the above rider value. This example is just to illustrate how to delete. In real world, use a select query using spark sql to fetch records that needs to be deleted and from the result we could invoke deletes as given below. Example rider value used is "rider-213".

```scala
val df = spark.sql("select uuid, partitionPath from hudi_ro_table where rider = 'rider-213'")
```

// Replace the above query with any other query that will fetch records to be deleted.

**Step 6** : Issue deletes

```scala
val deletes = dataGen.generateDeletes(df.collectAsList())
val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2));
df.write.format("org.apache.hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY,"delete").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath);
```
**Note:** For non-partitioned table, set
  ```
  option(KEYGENERATOR_CLASS_PROP, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
  ```
 Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for more options

**Step 7** : Reload the table and verify that the records are deleted

```scala
val roViewDFAfterDelete = spark.
  read.
  format("org.apache.hudi").
  load(basePath + "/*/*/*/*")
roViewDFAfterDelete.createOrReplaceTempView("hudi_ro_table")
spark.sql("select uuid, partitionPath from hudi_ro_table where rider = 'rider-213'").show() // should not return any rows
```

## Deletion with HoodieDeltaStreamer

Deletion with `HoodieDeltaStreamer` takes the same path as upsert and so it relies on a specific field called  "*_hoodie_is_deleted*" of type boolean in each record.

-   If a record has the field value set to  _false_ or it's not present, then it is considered a regular upsert
-   if not (if the value is set to  _true_ ), then its considered to be deleted record.

This essentially means that the schema has to be changed for the source, to add this field and all incoming records are expected to have this field set. We will be working to relax this in future releases.

Lets say the original schema is:

```json
{
  "type":"record",
  "name":"example_tbl",
  "fields":[{
     "name": "uuid",
     "type": "String"
  }, {
     "name": "ts",
     "type": "string"
  },  {
     "name": "partitionPath",
     "type": "string"
  }, {
     "name": "rank",
     "type": "long"
  }
]}
```

To leverage deletion capabilities of `DeltaStreamer`, you have to change the schema as below.

```json
{
  "type":"record",
  "name":"example_tbl",
  "fields":[{
     "name": "uuid",
     "type": "String"
  }, {
     "name": "ts",
     "type": "string"
  },  {
     "name": "partitionPath",
     "type": "string"
  }, {
     "name": "rank",
     "type": "long"
  }, {
    "name" : "_hoodie_is_deleted",
    "type" : "boolean",
    "default" : false
  }
]}
```

Example incoming record for upsert

```json
{
  "ts": 0.0,
  "uuid":"69cdb048-c93e-4532-adf9-f61ce6afe605",
  "rank": 1034,
  "partitionpath":"americas/brazil/sao_paulo",
  "_hoodie_is_deleted":false
}
```
      

Example incoming record that needs to be deleted
```json
{
  "ts": 0.0,
  "uuid": "19tdb048-c93e-4532-adf9-f61ce6afe10",
  "rank": 1045,
  "partitionpath":"americas/brazil/sao_paulo",
  "_hoodie_is_deleted":true
}
```

These are one time changes. Once these are in, then the DeltaStreamer pipeline will handle both upserts and deletions within every batch. Each batch could contain a mix of upserts and deletes and no additional step or changes are required after this. Note that this is to perform hard deletion instead of soft deletion.

