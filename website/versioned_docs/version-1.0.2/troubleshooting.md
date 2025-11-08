---
title: Troubleshooting
keywords: [hudi, troubleshooting]
toc_min_heading_level: 2
toc_max_heading_level: 5
last_modified_at: 2021-08-18T15:59:57-04:00
---

For performance related issues, please refer to the [tuning guide](https://hudi.apache.org/docs/tuning-guide)

### Writing Tables

#### org.apache.parquet.io.InvalidRecordException: Parquet/Avro schema mismatch: Avro field 'col1' not found
It is recommended that schema should evolve in [backwards compatible way](https://docs.confluent.io/platform/current/schema-registry/avro.html) while using Hudi. Please refer here for more information on avro schema resolution - [https://avro.apache.org/docs/1.8.2/spec.html](https://avro.apache.org/docs/1.8.2/spec.html). This error generally occurs when the schema has evolved in backwards **incompatible** way by deleting some column 'col1' and we are trying to update some record in parquet file which has ay been written with previous schema (which had 'col1'). In such cases, parquet tries to find all the present fields in the incoming record and when it finds 'col1' is not present, the mentioned exception is thrown.

The fix for this is to try and create uber schema using all the schema versions evolved so far for the concerned event and use this uber schema as the target schema. One of the good approaches can be fetching schema from hive metastore and merging it with the current schema.

Sample stacktrace where a field named "toBeDeletedStr" was omitted from new batch of updates : [https://gist.github.com/nsivabalan/cafc53fc9a8681923e4e2fa4eb2133fe](https://gist.github.com/nsivabalan/cafc53fc9a8681923e4e2fa4eb2133fe)

#### java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldIntegerConverter

This error will again occur due to schema evolutions in non-backwards compatible way. Basically there is some incoming update U for a record R which is already written to your Hudi dataset in the concerned parquet file. R contains field F which is having certain data type, let us say long. U has the same field F with updated data type of int type. Such incompatible data type conversions are not supported by Parquet FS.

For such errors, please try to ensure only valid data type conversions are happening in your primary data source from where you are trying to ingest.

Sample stacktrace when trying to evolve a field from Long type to Integer type with Hudi : [https://gist.github.com/nsivabalan/0d81cd60a3e7a0501e6a0cb50bfaacea](https://gist.github.com/nsivabalan/0d81cd60a3e7a0501e6a0cb50bfaacea)
 
#### SchemaCompatabilityException: Unable to validate the rewritten record

This can possibly occur if your schema has some non-nullable field whose value is not present or is null. It is recommended to evolve schema in backwards compatible ways. In essence, this means either have every newly added field as nullable or define default values for every new field. See [Schema Evolution](https://hudi.apache.org/docs/schema_evolution) docs for more.

#### INT96, INT64 and timestamp compatibility

[https://hudi.apache.org/docs/configurations#hoodiedatasourcehive_syncsupport_timestamp](https://hudi.apache.org/docs/configurations#hoodiedatasourcehive_syncsupport_timestamp)

#### I am seeing lot of archive files. How do I control the number of archive commit files generated?

Please note that in cloud stores that do not support log append operations, Hudi is forced to create new archive files to archive old metadata operations. 
You can increase `hoodie.commits.archival.batch` moving forward to increase the number of commits archived per archive file. 
In addition, you can increase the difference between the 2 watermark configurations : `hoodie.keep.max.commits` (default : 30) 
and `hoodie.keep.min.commits` (default : 20). This way, you can reduce the number of archive files created and also 
at the same time increase the number of metadata archived per archive file. Note that post 0.7.0 release where we are 
adding consolidated Hudi metadata ([RFC-15](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=147427331)), 
the follow up work would involve re-organizing archival metadata so that we can do periodic compactions to control 
file-sizing of these archive files.

#### How can I resolve the NoSuchMethodError from HBase when using Hudi with metadata table on HDFS?

From 0.11.0 release, we have upgraded the HBase version to 2.4.9, which is released based on Hadoop 2.x. Hudi's metadata

table uses HFile as the base file format, relying on the HBase library. When enabling metadata table in a Hudi table on

HDFS using Hadoop 3.x, NoSuchMethodError can be thrown due to compatibility issues between Hadoop 2.x and 3.x.

To address this, here's the workaround:

(1) Download HBase source code from [`https://github.com/apache/hbase`](https://github.com/apache/hbase);

(2) Switch to the source code of 2.4.9 release with the tag `rel/2.4.9`:

```bash
git checkout rel/2.4.9
```

(3) Package a new version of HBase 2.4.9 with Hadoop 3 version:

```bash
mvn clean install -Denforcer.skip -DskipTests -Dhadoop.profile=3.0 -Psite-install-step
```

(4) Package Hudi again.

#### How can I resolve the RuntimeException saying `hbase-default.xml file seems to be for an older version of HBase`?

This usually happens when there are other HBase libs provided by the runtime environment in the classpath, such as

Cloudera CDP stack, causing the conflict. To get around the RuntimeException, you can set the

`hbase.defaults.for.version.skip` to `true` in the `hbase-site.xml` configuration file, e.g., overwriting the config

within the Cloudera manager.

#### I see two different records for the same record key value, each record key with a different timestamp format. How is this possible?

This is a known issue with enabling row-writer for bulk_insert operation. When you do a bulk_insert followed by another
write operation such as upsert/insert this might be observed for timestamp fields specifically. For example, bulk_insert might produce
timestamp `2016-12-29 09:54:00.0` for record key whereas non bulk_insert write operation might produce a long value like
`1483023240000000` for the record key thus creating two different records. To fix this, starting 0.10.1 a new config [hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled](https://hudi.apache.org/docs/configurations/#hoodiedatasourcewritekeygeneratorconsistentlogicaltimestampenabled)
is introduced to bring consistency irrespective of whether row writing is enabled on not. However, for the sake of
backwards compatibility and not breaking existing pipelines, this config is set to false by default and will have to be enabled explicitly.



### Querying Tables

#### How can I now query the Hudi dataset I just wrote?

Unless Hive sync is enabled, the dataset written by Hudi using one of the methods above can simply be queries via the Spark datasource like any other source.

```scala
val hudiSnapshotQueryDF = spark
     .read()
     .format("hudi")
     .option("hoodie.datasource.query.type", "snapshot")
     .load(basePath) 
val hudiIncQueryDF = spark.read().format("hudi")
     .option("hoodie.datasource.query.type", "incremental")
     .option("hoodie.datasource.read.begin.instanttime", <beginInstantTime>)
     .load(basePath);
```

if Hive Sync is enabled in the [Hudi Streamer](https://github.com/apache/hudi/blob/f5f0ef6549fedf93863526a2110fe262a3460432/hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/HoodieStreamer.java#L337) tool or [datasource](https://hudi.apache.org/docs/configurations#hoodiedatasourcehive_syncenable), the dataset is available in Hive as a couple of tables, that can now be read using HiveQL, Presto or SparkSQL. See [here](https://hudi.apache.org/docs/querying_data/) for more.

### Data Quality Issues

Section below generally aids in debugging Hudi failures. Off the bat, the following metadata is added to every record to help triage issues easily using standard Hadoop SQL engines (Hive/PrestoDB/Spark)

* **_hoodie_record_key** - Treated as a primary key within each DFS partition, basis of all updates/inserts
* **_hoodie_commit_time** - Last commit that touched this record
* **_hoodie_commit_seqno** - This field contains a unique sequence number for each record within each transaction.
* **_hoodie_file_name** - Actual file name containing the record (super useful to triage duplicates)
* **_hoodie_partition_path** - Path from basePath that identifies the partition containing this record

#### Missing records

Please check if there were any write errors using the admin commands, during the window at which the record could have been written.

If you do find errors, then the record was not actually written by Hudi, but handed back to the application to decide what to do with it.

#### Duplicates

First of all, please confirm if you do indeed have duplicates **AFTER** ensuring the query is accessing the Hudi table [properly](https://hudi.apache.org/docs/querying_data/) .

*   If confirmed, please use the metadata fields above, to identify the physical files & partition files containing the records .
*   If duplicates span files across partitionpath, then this means your application is generating different partitionPaths for same recordKey, Please fix your app
*   if duplicates span multiple files within the same partitionpath, please engage with mailing list. This should not happen. You can use the `records deduplicate` command to fix your data.

### Ingestion

#### java.io.EOFException: Received -1 when reading from channel, socket has likely been closed. at [kafka.utils.Utils$.read](http://kafka.utils.Utils$.read)(Utils.scala:381) at kafka.network.BoundedByteBufferReceive.readFrom(BoundedByteBufferReceive.scala:54)

This might happen if you are ingesting from Kafka source, your cluster is ssl enabled by default and you are using some version of Hudi older than 0.5.1. Previous versions of Hudi were using spark-streaming-kafka-0-8 library. With the release of 0.5.1 version of Hudi, spark was upgraded to 2.4.4 and spark-streaming-kafka library was upgraded to spark-streaming-kafka-0-10. SSL support was introduced from spark-streaming-kafka-0-10. Please see here for reference.

The workaround can be either use Kafka cluster which is not ssl enabled, else upgrade Hudi version to at least 0.5.1 or spark-streaming-kafka library to spark-streaming-kafka-0-10.

#### java.lang.IllegalArgumentException: Could not find a 'KafkaClient' entry in the JAAS configuration. System property 'java.security.auth.login.config' is not set

This might happen when you are trying to ingest from ssl enabled kafka source and your setup is not able to read jars.conf file and its properties. To fix this, you need to pass the required property as part of your spark-submit command something like

```plain
--files jaas.conf,failed_tables.json --conf 'spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf' --conf 'spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf'
```

#### IOException: Write end dead or CIRCULAR REFERENCE while writing to GCS

If you encounter below stacktrace, please set the spark config as suggested below.

```plain
--conf 'spark.hadoop.fs.gs.outputstream.pipe.type=NIO_CHANNEL_PIPE'
```

```plain
 at org.apache.hudi.io.storage.HoodieAvroParquetWriter.close(HoodieAvroParquetWriter.java:84)
	Suppressed: java.io.IOException: Upload failed for 'gs://bucket/b0ee4274-5193-4a26-bcff-d60654fd7b24-0_0-42-671_20230228055305900.parquet'
		at...
		... 44 more
	Caused by: java.io.IOException: Write end dead
		at java.base/java.io.PipedInputStream.read(PipedInputStream.java:310)
		at java.base/java.io.PipedInputStream.read(PipedInputStream.java:377)
		at com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.util.ByteStreams.read(ByteStreams.java:172)
		at ...
		... 3 more
Caused by: [CIRCULAR REFERENCE: java.io.IOException: Write end dead]
```

We have an active patch([https://github.com/apache/hudi/pull/7245](https://github.com/apache/hudi/pull/7245)) on fixing the issue. Until we land this, you can use above config to bypass the issue.

### Hive Sync
#### SQLException: following columns have types incompatible

```plain
Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions : col1,col2
```

This will usually happen when you are trying to add a new column to existing hive table using our [HiveSyncTool.java](https://github.com/apache/hudi/blob/be4dfccbb24794dfac3714818971229870d24a2c/hudi-sync/hudi-hive-sync/src/main/java/org/apache/hudi/hive/HiveSyncTool.java) class. Databases usually will not allow to modify a column datatype from a higher order to lower order or cases where the datatypes may clash with the data that is already stored/will be stored in the table. To fix the same, try setting the following property -

```plain
set hive.metastore.disallow.incompatible.col.type.changes=false;
```

#### HoodieHiveSyncException: Could not convert field Type from `<type1>` to `<type2>` for field col1

This occurs because HiveSyncTool currently supports only few compatible data type conversions. Doing any other incompatible change will throw this exception. Please check the data type evolution for the concerned field and verify if it indeed can be considered as a valid data type conversion as per Hudi code base.

#### org.apache.hadoop.hive.ql.parse.SemanticException: Database does not exist: test_db

This generally occurs if you are trying to do Hive sync for your Hudi dataset and the configured hive_sync database does not exist. Please create the corresponding database on your Hive cluster and try again.

#### org.apache.thrift.TApplicationException: Invalid method name: 'get_table_req'

This issue is caused by hive version conflicts, hudi built with hive-2.3.x version, so if still want hudi work with older hive version

```plain
Steps: (build with hive-2.1.0)
1. git clone git@github.com:apache/incubator-hudi.git
2. rm hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/hive/HoodieCombineHiveInputFormat.java
3. mvn clean package -DskipTests -DskipITs -Dhive.version=2.1.0
```

#### java.lang.UnsupportedOperationException: Table rename is not supported

This issue could occur when syncing to hive. Possible reason is that, hive does not play well if your table name has upper and lower case letter. Try to have all lower case letters for your table name and it should likely get fixed. Related issue: [https://github.com/apache/hudi/issues/2409](https://github.com/apache/hudi/issues/2409)

#### How can I resolve the IllegalArgumentException saying `Partitions must be in the same table` when attempting to sync to a metastore?

This will occur when capital letters are used in the table name. Metastores such as Hive automatically convert table names

to lowercase. While we allow capitalization on Hudi tables, if you would like to use a metastore you may be required to

use all lowercase letters. More details on how this issue presents can be found [here](https://github.com/apache/hudi/issues/6832).

####
