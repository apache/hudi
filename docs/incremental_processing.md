---
title: Incremental Processing
keywords: incremental processing
sidebar: mydoc_sidebar
permalink: incremental_processing.html
toc: false
summary: In this page, we will discuss some available tools for ingesting data incrementally & consuming the changes.
---

As discussed in the concepts section, the two basic primitives needed for [incrementally processing](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop),
data using Hoodie are `upserts` (to apply changes to a dataset) and `incremental pulls` (to obtain a change stream/log from a dataset). This section
discusses a few tools that can be used to achieve these on different contexts.

## Incremental Ingestion

Following means can be used to apply a delta or an incremental change to a Hoodie dataset. For e.g, the incremental changes could be from a Kafka topic or files uploaded to HDFS or
even changes pulled from another Hoodie dataset.

#### DeltaStreamer Tool

The `HoodieDeltaStreamer` utility provides the way to achieve all of these, by using the capabilities of `HoodieWriteClient`, and support simply row-row ingestion (no transformations)
from different sources such as DFS or Kafka.

The tool is a spark job (part of hoodie-utilities), that provides the following functionality

 - Ability to consume new events from Kafka, incremental imports from Sqoop or output of `HiveIncrementalPuller` or files under a folder on HDFS
 - Support json, avro or a custom payload types for the incoming data
 - New data is written to a Hoodie dataset, with support for checkpointing & schemas and registered onto Hive

Command line options describe capabilities in more detail (first build hoodie-utilities using `mvn clean package`).

```
[hoodie]$ spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer `ls hoodie-utilities/target/hoodie-utilities-*-SNAPSHOT.jar` --help
Usage: <main class> [options]
  Options:
    --help, -h

    --key-generator-class
      Subclass of com.uber.hoodie.KeyGenerator to generate a HoodieKey from
      the given avro record. Built in: SimpleKeyGenerator (uses provided field
      names as recordkey & partitionpath. Nested fields specified via dot
      notation, e.g: a.b.c)
      Default: com.uber.hoodie.SimpleKeyGenerator
    --op
      Takes one of these values : UPSERT (default), INSERT (use when input is
      purely new data/inserts to gain speed)
      Default: UPSERT
      Possible Values: [UPSERT, INSERT, BULK_INSERT]
    --payload-class
      subclass of HoodieRecordPayload, that works off a GenericRecord.
      Implement your own, if you want to do something other than overwriting
      existing value
      Default: com.uber.hoodie.OverwriteWithLatestAvroPayload
    --props
      path to properties file on localfs or dfs, with configurations for
      hoodie client, schema provider, key generator and data source. For
      hoodie client props, sane defaults are used, but recommend use to
      provide basic things like metrics endpoints, hive configs etc. For
      sources, referto individual classes, for supported properties.
      Default: file:///Users/vinoth/bin/hoodie/src/test/resources/delta-streamer-config/dfs-source.properties
    --schemaprovider-class
      subclass of com.uber.hoodie.utilities.schema.SchemaProvider to attach
      schemas to input & target table data, built in options:
      FilebasedSchemaProvider
      Default: com.uber.hoodie.utilities.schema.FilebasedSchemaProvider
    --source-class
      Subclass of com.uber.hoodie.utilities.sources to read data. Built-in
      options: com.uber.hoodie.utilities.sources.{JsonDFSSource (default),
      AvroDFSSource, JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}
      Default: com.uber.hoodie.utilities.sources.JsonDFSSource
    --source-limit
      Maximum amount of data to read from source. Default: No limit For e.g:
      DFSSource => max bytes to read, KafkaSource => max events to read
      Default: 9223372036854775807
    --source-ordering-field
      Field within source record to decide how to break ties between records
      with same key in input data. Default: 'ts' holding unix timestamp of
      record
      Default: ts
    --spark-master
      spark master to use.
      Default: local[2]
  * --target-base-path
      base path for the target hoodie dataset. (Will be created if did not
      exist first time around. If exists, expected to be a hoodie dataset)
  * --target-table
      name of the target table in Hive


```


The tool takes a hierarchically composed property file and has pluggable interfaces for extracting data, key generation and providing schema. Sample configs for ingesting from kafka and dfs are
provided under `hoodie-utilities/src/test/resources/delta-streamer-config`.

For e.g: once you have Confluent Kafka, Schema registry up & running, produce some test data using ([impressions.avro](https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data.html) provided by schema-registry repo)

```
[confluent-5.0.0]$ bin/ksql-datagen schema=../impressions.avro format=avro topic=impressions key=impressionid
```

and then ingest it as follows.

```
[hoodie]$ spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer `ls hoodie-utilities/target/hoodie-utilities-*-SNAPSHOT.jar` \
  --props file://${PWD}/hoodie-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \
  --schemaprovider-class com.uber.hoodie.utilities.schema.SchemaRegistryProvider \
  --source-class com.uber.hoodie.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:///tmp/hoodie-deltastreamer-op --target-table uber.impressions \
  --op BULK_INSERT
```

In some cases, you may want to convert your existing dataset into Hoodie, before you can begin ingesting new data. This can be accomplished using the `hdfsparquetimport` command on the `hoodie-cli`.
Currently, there is support for converting parquet datasets.

#### Via Custom Spark Job

The `hoodie-spark` module offers the DataSource API to write any data frame into a Hoodie dataset. Following is how we can upsert a dataframe, while specifying the field names that need to be used
for `recordKey => _row_key`, `partitionPath => partition` and `precombineKey => timestamp`


```
inputDF.write()
       .format("com.uber.hoodie")
       .options(clientOpts) // any of the hoodie client opts can be passed in as well
       .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
       .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
       .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
       .option(HoodieWriteConfig.TABLE_NAME, tableName)
       .mode(SaveMode.Append)
       .save(basePath);
```

Please refer to [configurations](configurations.html) section, to view all datasource options.

#### Syncing to Hive

Once new data is written to a Hoodie dataset, via tools like above, we need the ability to sync with Hive and reflect the table schema such that queries can pick up new columns and partitions. To do this, Hoodie provides a `HiveSyncTool`, which can be
invoked as below, once you have built the hoodie-hive module.

```
 [hoodie-hive]$ java -cp target/hoodie-hive-0.3.6-SNAPSHOT-jar-with-dependencies.jar:target/jars/* com.uber.hoodie.hive.HiveSyncTool --help
Usage: <main class> [options]
  Options:
  * --base-path
       Basepath of hoodie dataset to sync
  * --database
       name of the target database in Hive
    --help, -h

       Default: false
  * --jdbc-url
       Hive jdbc connect url
  * --pass
       Hive password
  * --table
       name of the target table in Hive
  * --user
       Hive username


```

{% include callout.html content="Note that for now, due to jar mismatches between Spark & Hive, its recommended to run this as a separate Java task in your workflow manager/cron. This is getting fix [here](https://github.com/uber/hoodie/issues/123)" type="info" %}


## Incrementally Pulling

Hoodie datasets can be pulled incrementally, which means you can get ALL and ONLY the updated & new rows since a specified commit timestamp.
This, together with upserts, are particularly useful for building data pipelines where 1 or more source hoodie tables are incrementally pulled (streams/facts),
joined with other tables (datasets/dimensions), to produce deltas to a target hoodie dataset. Then, using the delta streamer tool these deltas can be upserted into the
target hoodie dataset to complete the pipeline.

#### Via Spark Job
The `hoodie-spark` module offers the DataSource API, offers a more elegant way to pull data from Hoodie dataset (plus more) and process it via Spark.
This class can be used within existing Spark jobs and offers the following functionality.

A sample incremental pull, that will obtain all records written since `beginInstantTime`, looks like below.

```
 Dataset<Row> hoodieIncViewDF = spark.read()
     .format("com.uber.hoodie")
     .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(),
             DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
     .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),
            <beginInstantTime>)
     .load(tablePath); // For incremental view, pass in the root/base path of dataset
```

Please refer to [configurations](configurations.html) section, to view all datasource options.


Additionally, `HoodieReadClient` offers the following functionality using Hoodie's implicit indexing.

| **API** | **Description** |
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hoodie's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided RDD[HoodieRecord]. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hoodie dataset |


#### HiveIncrementalPuller Tool
`HiveIncrementalPuller` allows the above to be done via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and incremental primitives
(speed up query by pulling tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the Hive query saving its results in a temp table.
that can later be upserted. Upsert utility (`HoodieDeltaStreamer`) has all the state it needs from the directory structure to know what should be the commit time on the target table.
e.g: `/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}`.The Delta Hive table registered will be of the form `{tmpdb}.{source_table}_{last_commit_included}`.

The following are the configuration options for HiveIncrementalPuller

| **Config** | **Description** | **Default** |
|hiveUrl| Hive Server 2 URL to connect to |  |
|hiveUser| Hive Server 2 Username |  |
|hivePass| Hive Server 2 Password |  |
|queue| YARN Queue name |  |
|tmp| Directory where the temporary delta data is stored in HDFS. The directory structure will follow conventions. Please see the below section.  |  |
|extractSQLFile| The SQL to execute on the source table to extract the data. The data extracted will be all the rows that changed since a particular point in time. |  |
|sourceTable| Source Table Name. Needed to set hive environment properties. |  |
|targetTable| Target Table Name. Needed for the intermediate storage directory structure.  |  |
|sourceDataPath| Source HDFS Base Path. This is where the hoodie metadata will be read. |  |
|targetDataPath| Target HDFS Base path. This is needed to compute the fromCommitTime. This is not needed if fromCommitTime is specified explicitly. |  |
|tmpdb| The database to which the intermediate temp delta table will be created | hoodie_temp |
|fromCommitTime| This is the most important parameter. This is the point in time from which the changed records are pulled from.  |  |
|maxCommits| Number of commits to include in the pull. Setting this to -1 will include all the commits from fromCommitTime. Setting this to a value > 0, will include records that ONLY changed in the specified number of commits after fromCommitTime. This may be needed if you need to catch up say 2 commits at a time. | 3 |
|help| Utility Help |  |


Setting the fromCommitTime=0 and maxCommits=-1 will pull in the entire source dataset and can be used to initiate backfills. If the target dataset is a hoodie dataset,
then the utility can determine if the target dataset has no commits or is behind more than 24 hour (this is configurable),
it will automatically use the backfill configuration, since applying the last 24 hours incrementally could take more time than doing a backfill. The current limitation of the tool
is the lack of support for self-joining the same table in mixed mode (normal and incremental modes).



