---
title: Writing Hudi Tables
keywords: hudi, incremental, batch, stream, processing, Hive, ETL, Spark SQL
permalink: /docs/writing_data.html
summary: In this page, we will discuss some available tools for incrementally ingesting & storing data.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables using the [DeltaStreamer](#deltastreamer) tool, as well as 
speeding up large Spark jobs via upserts using the [Hudi datasource](#datasource-writer). Such tables can then be [queried](/docs/querying_data.html) using various query engines.


## Write Operations

Before that, it may be helpful to understand the 3 different write operations provided by Hudi datasource or the delta streamer tool and how best to leverage them. These operations
can be chosen/changed across each commit/deltacommit issued against the table.


 - **UPSERT** : This is the default operation where the input records are first tagged as inserts or updates by looking up the index and 
 the records are ultimately written after heuristics are run to determine how best to pack them on storage to optimize for things like file sizing. 
 This operation is recommended for use-cases like database change capture where the input almost certainly contains updates.
 - **INSERT** : This operation is very similar to upsert in terms of heuristics/file sizing but completely skips the index lookup step. Thus, it can be a lot faster than upserts 
 for use-cases like log de-duplication (in conjunction with options to filter duplicates mentioned below). This is also suitable for use-cases where the table can tolerate duplicates, but just 
 need the transactional writes/incremental pull/storage management capabilities of Hudi.
 - **BULK_INSERT** : Both upsert and insert operations keep input records in memory to speed up storage heuristics computations faster (among other things) and thus can be cumbersome for 
 initial loading/bootstrapping a Hudi table at first. Bulk insert provides the same semantics as insert, while implementing a sort-based data writing algorithm, which can scale very well for several hundred TBs 
 of initial load. However, this just does a best-effort job at sizing files vs guaranteeing file sizes like inserts/upserts do. 


## DeltaStreamer

The `HoodieDeltaStreamer` utility (part of hudi-utilities-bundle) provides the way to ingest from different sources such as DFS or Kafka, with the following capabilities.

 - Exactly once ingestion of new events from Kafka, [incremental imports](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html#_incremental_imports) from Sqoop or output of `HiveIncrementalPuller` or files under a DFS folder
 - Support json, avro or a custom record types for the incoming data
 - Manage checkpoints, rollback & recovery 
 - Leverage Avro schemas from DFS or Confluent [schema registry](https://github.com/confluentinc/schema-registry).
 - Support for plugging in transformations

Command line options describe capabilities in more detail

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help
Usage: <main class> [options]
Options:
    --checkpoint
      Resume Delta Streamer from this checkpoint.
    --commit-on-errors
      Commit even when some records failed to be written
      Default: false
    --compact-scheduling-minshare
      Minshare for compaction as defined in
      https://spark.apache.org/docs/latest/job-scheduling.html
      Default: 0
    --compact-scheduling-weight
      Scheduling weight for compaction as defined in
      https://spark.apache.org/docs/latest/job-scheduling.html
      Default: 1
    --continuous
      Delta Streamer runs in continuous mode running source-fetch -> Transform
      -> Hudi Write in loop
      Default: false
    --delta-sync-scheduling-minshare
      Minshare for delta sync as defined in
      https://spark.apache.org/docs/latest/job-scheduling.html
      Default: 0
    --delta-sync-scheduling-weight
      Scheduling weight for delta sync as defined in
      https://spark.apache.org/docs/latest/job-scheduling.html
      Default: 1
    --disable-compaction
      Compaction is enabled for MoR table by default. This flag disables it
      Default: false
    --enable-hive-sync
      Enable syncing to hive
      Default: false
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
      Default: file:///Users/vinoth/bin/hoodie/src/test/resources/delta-streamer-config/dfs-source.properties
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
      AvroDFSSource, JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}
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
provided under `hudi-utilities/src/test/resources/delta-streamer-config`.

For e.g: once you have Confluent Kafka, Schema registry up & running, produce some test data using ([impressions.avro](https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data.html) provided by schema-registry repo)

```java
[confluent-5.0.0]$ bin/ksql-datagen schema=../impressions.avro format=avro topic=impressions key=impressionid
```

and then ingest it as follows.

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:\/\/\/tmp/hudi-deltastreamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

In some cases, you may want to migrate your existing table into Hudi beforehand. Please refer to [migration guide](/docs/migration_guide.html). 

## Datasource Writer

The `hudi-spark` module offers the DataSource API to write (and also read) any data frame into a Hudi table.
Following is how we can upsert a dataframe, while specifying the field names that need to be used
for `recordKey => _row_key`, `partitionPath => partition` and `precombineKey => timestamp`


```java
inputDF.write()
       .format("org.apache.hudi")
       .options(clientOpts) // any of the Hudi client opts can be passed in as well
       .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
       .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
       .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
       .option(HoodieWriteConfig.TABLE_NAME, tableName)
       .mode(SaveMode.Append)
       .save(basePath);
```

## Syncing to Hive

Both tools above support syncing of the table's latest schema to Hive metastore, such that queries can pick up new columns and partitions.
In case, its preferable to run this from commandline or in an independent jvm, Hudi provides a `HiveSyncTool`, which can be invoked as below, 
once you have built the hudi-hive module. Following is how we sync the above Datasource Writer written table to Hive metastore.

```java
cd hudi-hive
./run_sync_tool.sh  --jdbc-url jdbc:hive2:\/\/hiveserver:10000 --user hive --pass hive --partitioned-by partition --base-path <basePath> --database default --table <tableName>
```

Starting with Hudi 0.5.1 version read optimized version of merge-on-read tables are suffixed '_ro' by default. For backwards compatibility with older Hudi versions, 
an optional HiveSyncConfig - `--skip-ro-suffix`, has been provided to turn off '_ro' suffixing if desired. Explore other hive sync options using the following command:

```java
cd hudi-hive
./run_sync_tool.sh
 [hudi-hive]$ ./run_sync_tool.sh --help
```

## Deletes 

Hudi supports implementing two types of deletes on data stored in Hudi tables, by enabling the user to specify a different record payload implementation. 
For more info refer to [Delete support in Hudi](https://cwiki.apache.org/confluence/x/6IqvC).

 - **Soft Deletes** : With soft deletes, user wants to retain the key but just null out the values for all other fields. 
 This can be simply achieved by ensuring the appropriate fields are nullable in the table schema and simply upserting the table after setting these fields to null.
 - **Hard Deletes** : A stronger form of delete is to physically remove any trace of the record from the table. This can be achieved by issuing an upsert with a custom payload implementation
 via either DataSource or DeltaStreamer which always returns Optional.Empty as the combined value. Hudi ships with a built-in `org.apache.hudi.EmptyHoodieRecordPayload` class that does exactly this.
 
```java
 deleteDF // dataframe containing just records to be deleted
   .write().format("org.apache.hudi")
   .option(...) // Add HUDI options like record-key, partition-path and others as needed for your setup
   // specify record_key, partition_key, precombine_fieldkey & usual params
   .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
 
```


## Optimized DFS Access

Hudi also performs several key storage management functions on the data stored in a Hudi table. A key aspect of storing data on DFS is managing file sizes and counts
and reclaiming storage space. For e.g HDFS is infamous for its handling of small files, which exerts memory/RPC pressure on the Name Node and can potentially destabilize
the entire cluster. In general, query engines provide much better performance on adequately sized columnar files, since they can effectively amortize cost of obtaining 
column statistics etc. Even on some cloud data stores, there is often cost to listing directories with large number of small files.

Here are some ways to efficiently manage the storage of your Hudi tables.

 - The [small file handling feature](/docs/configurations.html#compactionSmallFileSize) in Hudi, profiles incoming workload 
   and distributes inserts to existing file groups instead of creating new file groups, which can lead to small files. 
 - Cleaner can be [configured](/docs/configurations.html#retainCommits) to clean up older file slices, more or less aggressively depending on maximum time for queries to run & lookback needed for incremental pull
 - User can also tune the size of the [base/parquet file](/docs/configurations.html#limitFileSize), [log files](/docs/configurations.html#logFileMaxSize) & expected [compression ratio](/docs/configurations.html#parquetCompressionRatio), 
   such that sufficient number of inserts are grouped into the same file group, resulting in well sized base files ultimately.
 - Intelligently tuning the [bulk insert parallelism](/docs/configurations.html#withBulkInsertParallelism), can again in nicely sized initial file groups. It is in fact critical to get this right, since the file groups
   once created cannot be deleted, but simply expanded as explained before.
 - For workloads with heavy updates, the [merge-on-read table](/docs/concepts.html#merge-on-read-table) provides a nice mechanism for ingesting quickly into smaller files and then later merging them into larger base files via compaction.
