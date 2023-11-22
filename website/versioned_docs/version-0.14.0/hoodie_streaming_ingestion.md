---
title: Streaming Ingestion
keywords: [hudi, streamer, hoodiestreamer, spark_streaming]
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Hudi Streamer
:::danger Important

The following classes were renamed and relocated to `org.apache.hudi.utilities.streamer` package.
- `DeltastreamerMultiWriterCkptUpdateFunc` is renamed to `StreamerMultiWriterCkptUpdateFunc`
- `DeltaSync` is renamed to `StreamSync`
- `HoodieDeltaStreamer` is renamed to `HoodieStreamer`
- `HoodieDeltaStreamerMetrics` is renamed to `HoodieStreamerMetrics`
- `HoodieMultiTableDeltaStreamer` is renamed to `HoodieMultiTableStreamer`

To maintain backward compatiblity, the original classes are still present in the org.apache.hudi.utilities.deltastreamer
package, but have been deprecated.

:::

The `HoodieStreamer` utility (part of `hudi-utilities-bundle`) provides the way to ingest from different sources such as DFS or Kafka, with the following capabilities.

- Exactly once ingestion of new events from Kafka, [incremental imports](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide#_incremental_imports) from Sqoop or output of `HiveIncrementalPuller` or files under a DFS folder
- Support json, avro or a custom record types for the incoming data
- Manage checkpoints, rollback & recovery
- Leverage Avro schemas from DFS or Confluent [schema registry](https://github.com/confluentinc/schema-registry).
- Support for plugging in transformations

Command line options describe capabilities in more detail

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help
Usage: <main class> [options]
Options:
    --checkpoint
      Resume Hudi Streamer from this checkpoint.
    --commit-on-errors
      Commit even when some records failed to be written
      Default: false
    --compact-scheduling-minshare
      Minshare for compaction as defined in
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 0
    --compact-scheduling-weight
      Scheduling weight for compaction as defined in
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 1
    --continuous
      Hudi Streamer runs in continuous mode running source-fetch -> Transform
      -> Hudi Write in loop
      Default: false
    --delta-sync-scheduling-minshare
      Minshare for delta sync as defined in
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 0
    --delta-sync-scheduling-weight
      Scheduling weight for delta sync as defined in
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 1
    --disable-compaction
      Compaction is enabled for MoR table by default. This flag disables it
      Default: false
    --enable-hive-sync
      Enable syncing to hive (Deprecated in favor of --enable-sync and --sync-tool-classes)
      Default: false
    --enable-sync
      Enable syncing meta
      Default: false
    --sync-tool-classes
      Classes (comma-separated) to be used for syncing meta. Shall be used only when --enable-sync or --enable-hive-sync is set to true
      Note: When used with deprecated --enable-hive-sync flag, HiveSyncTool will always be run along with any other classes mentioned in here.
      Default: org.apache.hudi.hive.HiveSyncTool
    --filter-dupes
      Should duplicate records from source be dropped/filtered out before
      insert/bulk-insert
      Default: false
    --help, -h

    --hoodie-conf
      Any configuration that can be set in the properties file (using the CLI
      parameter "--propsFilePath") can also be passed command line using this
      parameter
      Default: []
    --max-pending-compactions
      Maximum number of outstanding inflight/requested compactions. Delta Sync
      will not happen unlessoutstanding compactions is less than this number
      Default: 5
    --min-sync-interval-seconds
      the min sync interval of each sync in continuous mode
      Default: 0
    --op
      Takes one of these values : UPSERT (default), INSERT (use when input is
      purely new data/inserts to gain speed)
      Default: UPSERT
      Possible Values: [UPSERT, INSERT, BULK_INSERT]
    --payload-class
      subclass of HoodieRecordPayload, that works off a GenericRecord.
      Implement your own, if you want to do something other than overwriting
      existing value
      Default: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
    --props
      path to properties file on localfs or dfs, with configurations for
      hoodie client, schema provider, key generator and data source. For
      hoodie client props, sane defaults are used, but recommend use to
      provide basic things like metrics endpoints, hive configs etc. For
      sources, referto individual classes, for supported properties.
      Default: file:///Users/vinoth/bin/hoodie/src/test/resources/streamer-config/dfs-source.properties
    --schemaprovider-class
      subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach
      schemas to input & target table data, built in options:
      org.apache.hudi.utilities.schema.FilebasedSchemaProvider.Source (See
      org.apache.hudi.utilities.sources.Source) implementation can implement
      their own SchemaProvider. For Sources that return Dataset<Row>, the
      schema is obtained implicitly. However, this CLI option allows
      overriding the schemaprovider returned by Source.
    --source-class
      Subclass of org.apache.hudi.utilities.sources to read data. Built-in
      options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), 
      AvroDFSSource, AvroKafkaSource, CsvDFSSource, HiveIncrPullSource, 
      JdbcSource, JsonKafkaSource, ORCDFSSource, ParquetDFSSource, 
      S3EventsHoodieIncrSource, S3EventsSource, SqlSource}
      Default: org.apache.hudi.utilities.sources.JsonDFSSource
    --source-limit
      Maximum amount of data to read from source. Default: No limit For e.g:
      DFS-Source => max bytes to read, Kafka-Source => max events to read
      Default: 9223372036854775807
    --source-ordering-field
      Field within source record to decide how to break ties between records
      with same key in input data. Default: 'ts' holding unix timestamp of
      record
      Default: ts
    --spark-master
      spark master to use.
      Default: local[2]
  * --table-type
      Type of table. COPY_ON_WRITE (or) MERGE_ON_READ
  * --target-base-path
      base path for the target hoodie table. (Will be created if did not exist
      first time around. If exists, expected to be a hoodie table)
  * --target-table
      name of the target table in Hive
    --transformer-class
      subclass of org.apache.hudi.utilities.transform.Transformer. Allows
      transforming raw source Dataset to a target Dataset (conforming to
      target schema) before writing. Default : Not set. E:g -
      org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which
      allows a SQL query templated to be passed as a transformation function)
```

The tool takes a hierarchically composed property file and has pluggable interfaces for extracting data, key generation and providing schema. Sample configs for ingesting from kafka and dfs are
provided under `hudi-utilities/src/test/resources/streamer-config`.

For e.g: once you have Confluent Kafka, Schema registry up & running, produce some test data using ([impressions.avro](https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data) provided by schema-registry repo)

```java
[confluent-5.0.0]$ bin/ksql-datagen schema=../impressions.avro format=avro topic=impressions key=impressionid
```

and then ingest it as follows.

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:\/\/\/tmp/hudi-streamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

In some cases, you may want to migrate your existing table into Hudi beforehand. Please refer to [migration guide](/docs/migration_guide).

### Note on hudi utilities bundle usage for different spark versions
From 0.11.0 release, we start to provide a new `hudi-utilities-slim-bundle` which aims to exclude dependencies that can
cause conflicts and compatibility issues with different versions of Spark.  The `hudi-utilities-slim-bundle` should be
used along with a Hudi Spark bundle corresponding the Spark version used to make utilities work with Spark, e.g.,
`--packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.13.0,org.apache.hudi:hudi-spark3.1-bundle_2.12:0.13.0`,
if using `hudi-utilities-bundle` solely to run `HoodieStreamer` in Spark encounters compatibility issues.

#### MultiTableStreamer

`HoodieMultiTableStreamer`, a wrapper on top of `HoodieStreamer`, enables one to ingest multiple tables at a single go into hudi datasets. Currently it only supports sequential processing of tables to be ingested and COPY_ON_WRITE storage type. The command line options for `HoodieMultiTableStreamer` are pretty much similar to `HoodieStreamer` with the only exception that you are required to provide table wise configs in separate files in a dedicated config folder. The following command line options are introduced

```java
  * --config-folder
    the path to the folder which contains all the table wise config files
    --base-path-prefix
    this is added to enable users to create all the hudi datasets for related tables under one path in FS. The datasets are then created under the path - <base_path_prefix>/<database>/<table_to_be_ingested>. However you can override the paths for every table by setting the property hoodie.streamer.ingestion.targetBasePath
```

The following properties are needed to be set properly to ingest data using `HoodieMultiTableStreamer`.

```java
hoodie.streamer.ingestion.tablesToBeIngested
  comma separated names of tables to be ingested in the format <database>.<table>, for example db1.table1,db1.table2
hoodie.streamer.ingestion.targetBasePath
  if you wish to ingest a particular table in a separate path, you can mention that path here
hoodie.streamer.ingestion.<database>.<table>.configFile
  path to the config file in dedicated config folder which contains table overridden properties for the particular table to be ingested.
```

Sample config files for table wise overridden properties can be found under `hudi-utilities/src/test/resources/streamer-config`. The command to run `HoodieMultiTableStreamer` is also similar to how you run `HoodieStreamer`.

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.streamer.HoodieMultiTableStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/streamer-config/kafka-source.properties \
  --config-folder file://tmp/hudi-ingestion-config \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --base-path-prefix file:\/\/\/tmp/hudi-streamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

For detailed information on how to configure and use `HoodieMultiTableStreamer`, please refer [blog section](/blog/2020/08/22/ingest-multiple-tables-using-hudi).

### Concurrency Control

The `HoodieStreamer` utility (part of hudi-utilities-bundle) provides ways to ingest from different sources such as DFS or Kafka, with the following capabilities.

Using optimistic_concurrency_control via Hudi Streamer requires adding the above configs to the properties file that can be passed to the
job. For example below, adding the configs to kafka-source.properties file and passing them to Hudi Streamer will enable optimistic concurrency.
A Hudi Streamer job can then be triggered as follows:

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:\/\/\/tmp/hudi-streamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

Read more in depth about concurrency control in the [concurrency control concepts](/docs/concurrency_control) section

## Checkpointing
`HoodieStreamer` uses checkpoints to keep track of what data has been read already so it can resume without needing to reprocess all data.
When using a Kafka source, the checkpoint is the [Kafka Offset](https://cwiki.apache.org/confluence/display/KAFKA/Offset+Management) 
When using a DFS source, the checkpoint is the 'last modified' timestamp of the latest file read.
Checkpoints are saved in the .hoodie commit file as `streamer.checkpoint.key`.

If you need to change the checkpoints for reprocessing or replaying data you can use the following options:

- `--checkpoint` will set `streamer.checkpoint.reset_key` in the commit file to overwrite the current checkpoint.
- `--source-limit` will set a maximum amount of data to read from the source. For DFS sources, this is max # of bytes read.
For Kafka, this is the max # of events to read.

## Schema Providers
By default, Spark will infer the schema of the source and use that inferred schema when writing to a table. If you need 
to explicitly define the schema you can use one of the following Schema Providers below.

### Schema Registry Provider
You can obtain the latest schema from an online registry. You pass a URL to the registry and if needed, you can also 
pass userinfo and credentials in the url like: `https://foo:bar@schemaregistry.org` The credentials are then extracted 
and are set on the request as an Authorization Header.

When fetching schemas from a registry, you can specify both the source schema and the target schema separately.

|Config|Description|Example|
|---|---|---|
|hoodie.streamer.schemaprovider.registry.url|The schema of the source you are reading from|https://foo:bar@schemaregistry.org|
|hoodie.streamer.schemaprovider.registry.targetUrl|The schema of the target you are writing to|https://foo:bar@schemaregistry.org|

The above configs are passed to Hudi Streamer spark-submit command like: 
```--hoodie-conf hoodie.streamer.schemaprovider.registry.url=https://foo:bar@schemaregistry.org```

### JDBC Schema Provider
You can obtain the latest schema through a JDBC connection.

|Config|Description|Example|
|---|---|---|
|hoodie.streamer.schemaprovider.source.schema.jdbc.connection.url|The JDBC URL to connect to. You can specify source specific connection properties in the URL|jdbc:postgresql://localhost/test?user=fred&password=secret|
|hoodie.streamer.schemaprovider.source.schema.jdbc.driver.type|The class name of the JDBC driver to use to connect to this URL|org.h2.Driver|
|hoodie.streamer.schemaprovider.source.schema.jdbc.username|username for the connection|fred|
|hoodie.streamer.schemaprovider.source.schema.jdbc.password|password for the connection|secret|
|hoodie.streamer.schemaprovider.source.schema.jdbc.dbtable|The table with the schema to reference|test_database.test1_table or test1_table|
|hoodie.streamer.schemaprovider.source.schema.jdbc.timeout|The number of seconds the driver will wait for a Statement object to execute to the given number of seconds. Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout, e.g., the h2 JDBC driver checks the timeout of each query instead of an entire JDBC batch. It defaults to 0.|0|
|hoodie.streamer.schemaprovider.source.schema.jdbc.nullable|If true, all columns are nullable|true|

The above configs are passed to Hudi Streamer spark-submit command like:
```--hoodie-conf hoodie.streamer.jdbcbasedschemaprovider.connection.url=jdbc:postgresql://localhost/test?user=fred&password=secret```

### File Based Schema Provider
You can use a .avsc file to define your schema. You can then point to this file on DFS as a schema provider.

|Config|Description|Example|
|---|---|---|
|hoodie.streamer.schemaprovider.source.schema.file|The schema of the source you are reading from|[example schema file](https://github.com/apache/hudi/blob/a8fb69656f522648233f0310ca3756188d954281/docker/demo/config/test-suite/source.avsc)|
|hoodie.streamer.schemaprovider.target.schema.file|The schema of the target you are writing to|[example schema file](https://github.com/apache/hudi/blob/a8fb69656f522648233f0310ca3756188d954281/docker/demo/config/test-suite/target.avsc)|


### Hive Schema Provider
You can use hive tables to fetch source and target schema. 

|Config| Description                                           |
|---|-------------------------------------------------------|
|hoodie.streamer.schemaprovider.source.schema.hive.database| Hive database from where source schema can be fetched |
|hoodie.streamer.schemaprovider.source.schema.hive.table| Hive table from where source schema can be fetched    |
|hoodie.streamer.schemaprovider.target.schema.hive.database| Hive database from where target schema can be fetched |
|hoodie.streamer.schemaprovider.target.schema.hive.table| Hive table from where target schema can be fetched    |


### Schema Provider with Post Processor
The SchemaProviderWithPostProcessor, will extract the schema from one of the previously mentioned Schema Providers and 
then will apply a post processor to change the schema before it is used. You can write your own post processor by extending 
this class: https://github.com/apache/hudi/blob/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/schema/SchemaPostProcessor.java

## Sources
Hoodie Streamer can read data from a wide variety of sources. The following are a list of supported sources:

### Distributed File System (DFS)
See the storage configurations page to see some examples of DFS applications Hudi can read from. The following are the 
supported file formats Hudi can read/write with on DFS Sources. (Note: you can still use Spark/Flink readers to read from 
other formats and then write data as Hudi format.)

- CSV
- AVRO
- JSON
- PARQUET
- ORC
- HUDI

For DFS sources the following behaviors are expected:

- For JSON DFS source, you always need to set a schema. If the target Hudi table follows the same schema as from the source file, you just need to set the source schema. If not, you need to set schemas for both source and target. 
- `HoodieStreamer` reads the files under the source base path (`hoodie.streamer.source.dfs.root`) directly, and it won't use the partition paths under this base path as fields of the dataset. Detailed examples can be found [here](https://github.com/apache/hudi/issues/5485).

### Kafka
Hudi can read directly from Kafka clusters. See more details on `HoodieStreamer` to learn how to setup streaming 
ingestion with exactly once semantics, checkpointing, and plugin transformations. The following formats are supported 
when reading data from Kafka:

- AVRO
- JSON

### S3 Events
AWS S3 storage provides an event notification service which will post notifications when certain events happen in your S3 bucket: 
https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
AWS will put these events in a Simple Queue Service (SQS). Apache Hudi provides an S3EventsSource that can read from SQS 
to trigger/processing of new or changed data as soon as it is available on S3.

#### Setup
1. Enable S3 Event Notifications https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
2. Download the aws-java-sdk-sqs jar. 
3. Find the queue URL and Region to set these configurations:
   1. hoodie.streamer.s3.source.queue.url=https://sqs.us-west-2.amazonaws.com/queue/url
   2. hoodie.streamer.s3.source.queue.region=us-west-2 
4. start the S3EventsSource and S3EventsHoodieIncrSource using the `HoodieStreamer` utility as shown in sample commands below:

Insert code sample from this blog: https://hudi.apache.org/blog/2021/08/23/s3-events-source/#configuration-and-setup

### JDBC Source
Hudi can read from a JDBC source with a full fetch of a table, or Hudi can even read incrementally with checkpointing from a JDBC source.

|Config|Description|Example|
|---|---|---|
|hoodie.streamer.jdbc.url|URL of the JDBC connection|jdbc:postgresql://localhost/test|
|hoodie.streamer.jdbc.user|User to use for authentication of the JDBC connection|fred|
|hoodie.streamer.jdbc.password|Password to use for authentication of the JDBC connection|secret|
|hoodie.streamer.jdbc.password.file|If you prefer to use a password file for the connection||
|hoodie.streamer.jdbc.driver.class|Driver class to use for the JDBC connection||
|hoodie.streamer.jdbc.table.name||my_table|
|hoodie.streamer.jdbc.table.incr.column.name|If run in incremental mode, this field will be used to pull new data incrementally||
|hoodie.streamer.jdbc.incr.pull|Will the JDBC connection perform an incremental pull?||
|hoodie.streamer.jdbc.extra.options.|How you pass extra configurations that would normally by specified as spark.read.option()|hoodie.streamer.jdbc.extra.options.fetchSize=100 hoodie.streamer.jdbc.extra.options.upperBound=1 hoodie.streamer.jdbc.extra.options.lowerBound=100|
|hoodie.streamer.jdbc.storage.level|Used to control the persistence level|Default = MEMORY_AND_DISK_SER|
|hoodie.streamer.jdbc.incr.fallback.to.full.fetch|Boolean which if set true makes an incremental fetch fallback to a full fetch if there is any error in the incremental read|FALSE|

### SQL Source
SQL Source that reads from any table, used mainly for backfill jobs which will process specific partition dates. 
This won't update the streamer.checkpoint.key to the processed commit, instead it will fetch the latest successful 
checkpoint key and set that value as this backfill commits checkpoint so that it won't interrupt the regular incremental 
processing. To fetch and use the latest incremental checkpoint, you need to also set this hoodie_conf for Hudi Streamer 
jobs: `hoodie.write.meta.key.prefixes = 'streamer.checkpoint.key'`

Spark SQL should be configured using this hoodie config:
hoodie.streamer.source.sql.sql.query = 'select * from source_table'


## Structured Streaming

Hudi supports Spark Structured Streaming reads and writes.

### Streaming Write
You can write Hudi tables using spark's structured streaming. 

<Tabs
groupId="programming-language"
defaultValue="python"
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

### Streaming Read

Structured Streaming reads are based on Hudi's Incremental Query feature, therefore streaming read can return data for which
commits and base files were not yet removed by the cleaner. You can control commits retention time.

<Tabs
groupId="programming-language"
defaultValue="python"
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

# ead stream and output results to console
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


:::info
Spark SQL can be used within ForeachBatch sink to do INSERT, UPDATE, DELETE and MERGE INTO.
Target table must exist before write.
:::


## Flink Ingestion

### CDC Ingestion
CDC(change data capture) keep track of the data changes evolving in a source system so a downstream process or system can action that change.
We recommend two ways for syncing CDC data into Hudi:

![slide1 title](/assets/images/cdc-2-hudi.png)

1. Using the Ververica [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) directly connect to DB Server to sync the binlog data into Hudi.
   The advantage is that it does not rely on message queues, but the disadvantage is that it puts pressure on the db server;
2. Consume data from a message queue (for e.g, the Kafka) using the flink cdc format, the advantage is that it is highly scalable,
   but the disadvantage is that it relies on message queues.

:::note
- If the upstream data cannot guarantee the order, you need to specify option `write.precombine.field` explicitly;
:::

### Bulk Insert

For the demand of snapshot data import. If the snapshot data comes from other data sources, use the `bulk_insert` mode to quickly
import the snapshot data into Hudi.


:::note
`bulk_insert` eliminates the serialization and data merging. The data deduplication is skipped, so the user need to guarantee the uniqueness of the data.
:::

:::note
`bulk_insert` is more efficient in the `batch execution mode`. By default, the `batch execution mode` sorts the input records
by the partition path and writes these records to Hudi, which can avoid write performance degradation caused by
frequent `file handle` switching.  
:::

:::note  
The parallelism of `bulk_insert` is specified by `write.tasks`. The parallelism will affect the number of small files.
In theory, the parallelism of `bulk_insert` is the number of `bucket`s (In particular, when each bucket writes to maximum file size, it
will rollover to the new file handle. Finally, `the number of files` >= [`write.bucket_assign.tasks`](/docs/configurations#writebucket_assigntasks).
:::

#### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.operation` | `true` | `upsert` | Setting as `bulk_insert` to open this function  |
| `write.tasks`  |  `false`  | `4` | The parallelism of `bulk_insert`, `the number of files` >= [`write.bucket_assign.tasks`](/docs/configurations#writebucket_assigntasks) |
| `write.bulk_insert.shuffle_input` | `false` | `true` | Whether to shuffle data according to the input field before writing. Enabling this option will reduce the number of small files, but there may be a risk of data skew |
| `write.bulk_insert.sort_input` | `false`  | `true` | Whether to sort data according to the input field before writing. Enabling this option will reduce the number of small files when a write task writes multiple partitions |
| `write.sort.memory` | `false` | `128` | Available managed memory of sort operator. default  `128` MB |

### Index Bootstrap

For the demand of `snapshot data` + `incremental data` import. If the `snapshot data` already insert into Hudi by  [bulk insert](#bulk-insert).
User can insert `incremental data` in real time and ensure the data is not repeated by using the index bootstrap function.

:::note
If you think this process is very time-consuming, you can add resources to write in streaming mode while writing `snapshot data`,
and then reduce the resources to write `incremental data` (or open the rate limit function).
:::

#### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `index.bootstrap.enabled` | `true` | `false` | When index bootstrap is enabled, the remain records in Hudi table will be loaded into the Flink state at one time |
| `index.partition.regex`  |  `false`  | `*` | Optimize option. Setting regular expressions to filter partitions. By default, all partitions are loaded into flink state |

#### How To Use

1. `CREATE TABLE` creates a statement corresponding to the Hudi table. Note that the `table.type` must be correct.
2. Setting `index.bootstrap.enabled` = `true` to enable the index bootstrap function.
3. Setting Flink checkpoint failure tolerance in `flink-conf.yaml` : `execution.checkpointing.tolerable-failed-checkpoints = n` (depending on Flink checkpoint scheduling times).
4. Waiting until the first checkpoint succeeds, indicating that the index bootstrap completed.
5. After the index bootstrap completed, user can exit and save the savepoint (or directly use the externalized checkpoint).
6. Restart the job, setting `index.bootstrap.enable` as `false`.

:::note
1. Index bootstrap is blocking, so checkpoint cannot be completed during index bootstrap.
2. Index bootstrap triggers by the input data. User need to ensure that there is at least one record in each partition.
3. Index bootstrap executes concurrently. User can search in log by `finish loading the index under partition` and `Load record form file` to observe the progress of index bootstrap.
4. The first successful checkpoint indicates that the index bootstrap completed. There is no need to load the index again when recovering from the checkpoint.
:::

### Changelog Mode
Hudi can keep all the intermediate changes (I / -U / U / D) of messages, then consumes through stateful computing of flink to have a near-real-time
data warehouse ETL pipeline (Incremental computing). Hudi MOR table stores messages in the forms of rows, which supports the retention of all change logs (Integration at the format level).
All changelog records can be consumed with Flink streaming reader.

#### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `changelog.enabled` | `false` | `false` | It is turned off by default, to have the `upsert` semantics, only the merged messages are ensured to be kept, intermediate changes may be merged. Setting to true to support consumption of all changes |

:::note
Batch (Snapshot) read still merge all the intermediate changes, regardless of whether the format has stored the intermediate changelog messages.
:::

:::note
After setting `changelog.enable` as `true`, the retention of changelog records are only best effort: the asynchronous compaction task will merge the changelog records into one record, so if the
stream source does not consume timely, only the merged record for each key can be read after compaction. The solution is to reserve some buffer time for the reader by adjusting the compaction strategy, such as
the compaction options: [`compaction.delta_commits`](#compaction) and [`compaction.delta_seconds`](#compaction).
:::


### Append Mode

For `INSERT` mode write operation, the current work flow is:

- For Merge_On_Read table, the small file strategies are by default applied: tries to append to the small avro log files first
- For Copy_On_Write table, write new parquet files directly, no small file strategies are applied

Hudi supports rich clustering strategies to optimize the files layout for `INSERT` mode:

#### Inline Clustering

:::note
Only Copy_On_Write table is supported. 
:::

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.insert.cluster` | `false` | `false` | Whether to merge small files while ingesting, for COW table, open the option to enable the small file merging strategy(no deduplication for keys but the throughput will be affected) |

#### Async Clustering

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `clustering.schedule.enabled` | `false` | `false` | Whether to schedule clustering plan during write process, by default false |
| `clustering.delta_commits` | `false` | `4` | Delta commits to schedule the clustering plan, only valid when `clustering.schedule.enabled` is true |
| `clustering.async.enabled` | `false` | `false` | Whether to execute clustering plan asynchronously, by default false |
| `clustering.tasks` | `false` | `4` | Parallelism of the clustering tasks |
| `clustering.plan.strategy.target.file.max.bytes` | `false` | `1024*1024*1024` | The target file size for clustering group, by default 1GB |
| `clustering.plan.strategy.small.file.limit` | `false` | `600` | The file that has less size than the threshold (unit MB) are candidates for clustering |
| `clustering.plan.strategy.sort.columns` | `false` | `N/A` | The columns to sort by when clustering |

#### Clustering Plan Strategy

Custom clustering strategy is supported.

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `clustering.plan.partition.filter.mode` | `false` | `NONE` | Valid options 1) `NONE`: no limit; 2) `RECENT_DAYS`: choose partitions that represent recent days; 3) `SELECTED_PARTITIONS`: specific partitions |
| `clustering.plan.strategy.daybased.lookback.partitions` | `false` | `2` | Valid for `RECENT_DAYS` mode |
| `clustering.plan.strategy.cluster.begin.partition` | `false` | `N/A` | Valid for `SELECTED_PARTITIONS` mode, specify the partition to begin with(inclusive) |
| `clustering.plan.strategy.cluster.end.partition` | `false` | `N/A` | Valid for `SELECTED_PARTITIONS` mode, specify the partition to end with(inclusive) |
| `clustering.plan.strategy.partition.regex.pattern` | `false` | `N/A` | The regex to filter the partitions |
| `clustering.plan.strategy.partition.selected` | `false` | `N/A` | Specific partitions separated by comma `,` |

### Bucket Index

By default, flink uses the state-backend to keep the file index: the mapping from primary key to fileId. When the input data set is large,
there is possibility the cost of the state be a bottleneck, the bucket index use deterministic hash algorithm for shuffling the records into
buckets, thus can avoid the storage and query overhead of indexes.

#### Options

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `index.type` | `false` | `FLINK_STATE` | Set up as `BUCKET` to use bucket index |
| `hoodie.bucket.index.hash.field` | `false` | Primary key | Can be a subset of the primary key |
| `hoodie.bucket.index.num.buckets` | `false` | `4` | The number of buckets per-partition, it is immutable once set up |

Comparing to state index:

- Bucket index has no computing and storage cost of state-backend index, thus has better performance
- Bucket index can not expand the buckets dynamically, the state-backend index can expand the buckets dynamically based on current file layout
- Bucket index can not handle changes among partitions(no limit if the input itself is CDC stream), state-backend index has no limit 

### Rate Limit
There are many use cases that user put the full history data set onto the message queue together with the realtime incremental data. Then they consume the data from the queue into the hudi from the earliest offset using flink. Consuming history data set has these characteristics:
1). The instant throughput is huge 2). It has serious disorder (with random writing partitions). It will lead to degradation of writing performance and throughput glitches. For this case, the speed limit parameter can be turned on to ensure smooth writing of the flow.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `write.rate.limit` | `false` | `0` | Default disable the rate limit |

## Kafka Connect Sink
If you want to perform streaming ingestion into Hudi format similar to `HoodieStreamer`, but you don't want to depend on Spark,
try out the new experimental release of Hudi Kafka Connect Sink. Read the [ReadMe](https://github.com/apache/hudi/tree/master/hudi-kafka-connect) 
for full documentation.

