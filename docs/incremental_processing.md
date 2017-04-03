---
title: Incremental Processing
keywords: incremental processing
sidebar: mydoc_sidebar
permalink: incremental_processing.html
toc: false
summary: In this page, we will discuss incremental processing primitives that Hoodie has to offer.
---

As discussed in the concepts section, the two basic primitives needed for [incrementally processing](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop),
data using Hoodie are `upserts` (to apply changes to a dataset) and `incremental pulls` (to obtain a change stream/log from a dataset). This section
discusses a few tools that can be used to achieve these on different contexts.

{% include callout.html content="Instructions are currently only for Copy-on-write storage. When merge-on-read storage is added, these tools would be revised to add that support" type="info" %}


## Upserts

Upserts can be used to apply a delta or an incremental change to a Hoodie dataset. For e.g, the incremental changes could be from a Kafka topic or files uploaded to HDFS or
even changes pulled from another Hoodie dataset.


#### DeltaStreamer

The `HoodieDeltaStreamer` utility provides the way to achieve all of these, by using the capabilities of `HoodieWriteClient`.

The tool is a spark job (part of hoodie-utilities), that provides the following functionality

 - Ability to consume new events from Kafka, incremental imports from Sqoop or output of `HiveIncrementalPuller` or files under a folder on HDFS
 - Support json, avro or a custom payload types for the incoming data
 - New data is written to a Hoodie dataset, with support for checkpointing & schemas and registered onto Hive

 To understand more

```

[hoodie]$ spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer hoodie-utilities/target/hoodie-utilities-0.3.6-SNAPSHOT-bin.jar --help
Usage: <main class> [options]
  Options:
    --help, -h
       Default: false
    --hoodie-client-config
       path to properties file on localfs or dfs, with hoodie client config.
       Sane defaultsare used, but recommend use to provide basic things like metrics
       endpoints, hive configs etc
    --key-generator-class
       Subclass of com.uber.hoodie.utilities.common.KeyExtractor to generatea
       HoodieKey from the given avro record. Built in: SimpleKeyGenerator (Uses provided
       field names as recordkey & partitionpath. Nested fields specified via dot
       notation, e.g: a.b.c)
       Default: com.uber.hoodie.utilities.keygen.SimpleKeyGenerator
    --key-generator-config
       Path to properties file on localfs or dfs, with KeyGenerator configs. For
       list of acceptable properites, refer the KeyGenerator class
    --max-input-bytes
       Maximum number of bytes to read from source. Default: 1TB
       Default: 1099511627776
    --op
       Takes one of these values : UPSERT (default), INSERT (use when input is
       purely new data/inserts to gain speed)
       Default: UPSERT
       Possible Values: [UPSERT, INSERT]
    --payload-class
       subclass of HoodieRecordPayload, that works off a GenericRecord. Default:
       SourceWrapperPayload. Implement your own, if you want to do something other than overwriting
       existing value
       Default: com.uber.hoodie.utilities.deltastreamer.DeltaStreamerAvroPayload
    --schemaprovider-class
       subclass of com.uber.hoodie.utilities.schema.SchemaProvider to attach
       schemas to input & target table data, built in options: FilebasedSchemaProvider
       Default: com.uber.hoodie.utilities.schema.FilebasedSchemaProvider
    --schemaprovider-config
       path to properties file on localfs or dfs, with schema configs. For list
       of acceptable properties, refer the schema provider class
    --source-class
       subclass of com.uber.hoodie.utilities.sources.Source to use to read data.
       built-in options: com.uber.hoodie.utilities.common.{DFSSource (default),
       KafkaSource, HiveIncrPullSource}
       Default: com.uber.hoodie.utilities.sources.DFSSource
     --source-config
       path to properties file on localfs or dfs, with source configs. For list
       of acceptable properties, refer the source class
    --source-format
       Format of data in source, JSON (default), Avro. All source data is
       converted to Avro using the provided schema in any case
       Default: JSON
       Possible Values: [AVRO, JSON, ROW, CUSTOM]
    --source-ordering-field
       Field within source record to decide how to break ties between  records
       with same key in input data. Default: 'ts' holding unix timestamp of record
       Default: ts
    --target-base-path
       base path for the target hoodie dataset
    --target-table
       name of the target table in Hive


```

For e.g, followings ingests data from Kafka (avro records as the client example)


```
[hoodie]$ spark-submit --class com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer hoodie-utilities/target/hoodie-utilities-0.3.6-SNAPSHOT-bin.jar \
     --hoodie-client-config hoodie-utilities/src/main/resources/delta-streamer-config/hoodie-client.properties \
     --key-generator-config hoodie-utilities/src/main/resources/delta-streamer-config/key-generator.properties \
      --schemaprovider-config hoodie-utilities/src/main/resources/delta-streamer-config/schema-provider.properties \
      --source-class com.uber.hoodie.utilities.sources.KafkaSource \
      --source-config hoodie-utilities/src/main/resources/delta-streamer-config/source.properties \
      --source-ordering-field rider \
      --target-base-path file:///tmp/hoodie-deltastreamer-op \
      --target-table uber.trips
```


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


## Incremental Pull

Hoodie datasets can be pulled incrementally, which means you can get ALL and ONLY the updated & new rows since a specified commit timestamp.
This, together with upserts, are particularly useful for building data pipelines where 1 or more source hoodie tables are incrementally pulled (streams/facts),
joined with other tables (datasets/dimensions), to produce deltas to a target hoodie dataset. Then, using the delta streamer tool these deltas can be upserted into the
target hoodie dataset to complete the pipeline.

#### Pulling through Hive

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


#### Pulling through Spark

`HoodieReadClient` (inside hoodie-client) offers a more elegant way to pull data from Hoodie dataset (plus more) and process it via Spark.
This class can be used within existing Spark jobs and offers the following functionality.

| **API** | **Description** |
| listCommitsSince(),latestCommit() | Obtain commit times to pull data from |
| readSince(commitTime),readCommit(commitTime) | Provide the data from the commit time as a DataFrame, to process further on |
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hoodie's own index for faster lookup |
| read(paths) | Read out the data under specified path, with the functionality of HoodieInputFormat. An alternative way to do SparkSQL on Hoodie datasets |
| filterExists() | Filter out already existing records from the provided RDD[HoodieRecord]. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hoodie dataset |


## SQL Streamer

work in progress, tool being refactored out into open source Hoodie


{% include callout.html content="Get involved in building this tool [here](https://github.com/uber/hoodie/issues/20)" type="info" %}

