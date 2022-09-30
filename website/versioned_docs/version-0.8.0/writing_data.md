---
version: 0.8.0
title: Writing Data
keywords: [ hudi, incremental, batch, stream, processing, Hive, ETL, Spark SQL]
summary: In this page, we will discuss some available tools for incrementally ingesting & storing data.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables using the [DeltaStreamer](#deltastreamer) tool, as well as 
speeding up large Spark jobs via upserts using the [Hudi datasource](#datasource-writer). Such tables can then be [queried](/docs/querying_data) using various query engines.


## Write Operations

Before that, it may be helpful to understand the 3 different write operations provided by Hudi datasource or the delta streamer tool and how best to leverage them. These operations
can be chosen/changed across each commit/deltacommit issued against the table.


 - **UPSERT** : This is the default operation where the input records are first tagged as inserts or updates by looking up the index. 
 The records are ultimately written after heuristics are run to determine how best to pack them on storage to optimize for things like file sizing. 
 This operation is recommended for use-cases like database change capture where the input almost certainly contains updates. The target table will never show duplicates.
 - **INSERT** : This operation is very similar to upsert in terms of heuristics/file sizing but completely skips the index lookup step. Thus, it can be a lot faster than upserts 
 for use-cases like log de-duplication (in conjunction with options to filter duplicates mentioned below). This is also suitable for use-cases where the table can tolerate duplicates, but just 
 need the transactional writes/incremental pull/storage management capabilities of Hudi.
 - **BULK_INSERT** : Both upsert and insert operations keep input records in memory to speed up storage heuristics computations faster (among other things) and thus can be cumbersome for 
 initial loading/bootstrapping a Hudi table at first. Bulk insert provides the same semantics as insert, while implementing a sort-based data writing algorithm, which can scale very well for several hundred TBs 
 of initial load. However, this just does a best-effort job at sizing files vs guaranteeing file sizes like inserts/upserts do. 


## DeltaStreamer

The `HoodieDeltaStreamer` utility (part of hudi-utilities-bundle) provides the way to ingest from different sources such as DFS or Kafka, with the following capabilities.

 - Exactly once ingestion of new events from Kafka, [incremental imports](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide#_incremental_imports) from Sqoop or output of `HiveIncrementalPuller` or files under a DFS folder
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
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 0
    --compact-scheduling-weight
      Scheduling weight for compaction as defined in
      https://spark.apache.org/docs/latest/job-scheduling
      Default: 1
    --continuous
      Delta Streamer runs in continuous mode running source-fetch -> Transform
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

For e.g: once you have Confluent Kafka, Schema registry up & running, produce some test data using ([impressions.avro](https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data) provided by schema-registry repo)

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

In some cases, you may want to migrate your existing table into Hudi beforehand. Please refer to [migration guide](/docs/migration_guide). 

## MultiTableDeltaStreamer

`HoodieMultiTableDeltaStreamer`, a wrapper on top of `HoodieDeltaStreamer`, enables one to ingest multiple tables at a single go into hudi datasets. Currently it only supports sequential processing of tables to be ingested and COPY_ON_WRITE storage type. The command line options for `HoodieMultiTableDeltaStreamer` are pretty much similar to `HoodieDeltaStreamer` with the only exception that you are required to provide table wise configs in separate files in a dedicated config folder. The following command line options are introduced

```java
  * --config-folder
    the path to the folder which contains all the table wise config files
    --base-path-prefix
    this is added to enable users to create all the hudi datasets for related tables under one path in FS. The datasets are then created under the path - <base_path_prefix>/<database>/<table_to_be_ingested>. However you can override the paths for every table by setting the property hoodie.deltastreamer.ingestion.targetBasePath
```

The following properties are needed to be set properly to ingest data using `HoodieMultiTableDeltaStreamer`. 

```java
hoodie.deltastreamer.ingestion.tablesToBeIngested
  comma separated names of tables to be ingested in the format <database>.<table>, for example db1.table1,db1.table2
hoodie.deltastreamer.ingestion.targetBasePath
  if you wish to ingest a particular table in a separate path, you can mention that path here
hoodie.deltastreamer.ingestion.<database>.<table>.configFile
  path to the config file in dedicated config folder which contains table overridden properties for the particular table to be ingested.
```

Sample config files for table wise overridden properties can be found under `hudi-utilities/src/test/resources/delta-streamer-config`. The command to run `HoodieMultiTableDeltaStreamer` is also similar to how you run `HoodieDeltaStreamer`.

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \
  --config-folder file://tmp/hudi-ingestion-config \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --base-path-prefix file:\/\/\/tmp/hudi-deltastreamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

For detailed information on how to configure and use `HoodieMultiTableDeltaStreamer`, please refer [blog section](/blog/2020/08/22/ingest-multiple-tables-using-hudi).

## Datasource Writer

The `hudi-spark` module offers the DataSource API to write (and read) a Spark DataFrame into a Hudi table. There are a number of options available:

**`HoodieWriteConfig`**:

**TABLE_NAME** (Required)<br/>


**`DataSourceWriteOptions`**:

**RECORDKEY_FIELD_OPT_KEY** (Required): Primary key field(s). Record keys uniquely identify a record/row within each partition. If one wants to have a global uniqueness, there are two options. You could either make the dataset non-partitioned, or, you can leverage Global indexes to ensure record keys are unique irrespective of the partition path. Record keys can either be a single column or refer to multiple columns. `KEYGENERATOR_CLASS_OPT_KEY` property should be set accordingly based on whether it is a simple or complex key. For eg: `"col1"` for simple field, `"col1,col2,col3,etc"` for complex field. Nested fields can be specified using the dot notation eg: `a.b.c`. <br/>
Default value: `"uuid"`<br/>

**PARTITIONPATH_FIELD_OPT_KEY** (Required): Columns to be used for partitioning the table. To prevent partitioning, provide empty string as value eg: `""`. Specify partitioning/no partitioning using `KEYGENERATOR_CLASS_OPT_KEY`. If partition path needs to be url encoded, you can set `URL_ENCODE_PARTITIONING_OPT_KEY`. If synchronizing to hive, also specify using `HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.`<br/>
Default value: `"partitionpath"`<br/>

**PRECOMBINE_FIELD_OPT_KEY** (Required): When two records within the same batch have the same key value, the record with the largest value from the field specified will be choosen. If you are using default payload of OverwriteWithLatestAvroPayload for HoodieRecordPayload (`WRITE_PAYLOAD_CLASS`), an incoming record will always takes precendence compared to the one in storage ignoring this `PRECOMBINE_FIELD_OPT_KEY`. <br/>
Default value: `"ts"`<br/>

**OPERATION_OPT_KEY**: The [write operations](#write-operations) to use.<br/>
Available values:<br/>
`UPSERT_OPERATION_OPT_VAL` (default), `BULK_INSERT_OPERATION_OPT_VAL`, `INSERT_OPERATION_OPT_VAL`, `DELETE_OPERATION_OPT_VAL`

**TABLE_TYPE_OPT_KEY**: The [type of table](/docs/concepts#table-types) to write to. Note: After the initial creation of a table, this value must stay consistent when writing to (updating) the table using the Spark `SaveMode.Append` mode.<br/>
Available values:<br/>
[`COW_TABLE_TYPE_OPT_VAL`](/docs/concepts#copy-on-write-table) (default), [`MOR_TABLE_TYPE_OPT_VAL`](/docs/concepts#merge-on-read-table)

**KEYGENERATOR_CLASS_OPT_KEY**: Refer to [Key Generation](#key-generation) section below.

**HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY**: If using hive, specify if the table should or should not be partitioned.<br/>
Available values:<br/>
`classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName` (default), `classOf[MultiPartKeysValueExtractor].getCanonicalName`, `classOf[TimestampBasedKeyGenerator].getCanonicalName`, `classOf[NonPartitionedExtractor].getCanonicalName`, `classOf[GlobalDeleteKeyGenerator].getCanonicalName` (to be used when `OPERATION_OPT_KEY` is set to `DELETE_OPERATION_OPT_VAL`)


Example:
Upsert a DataFrame, specifying the necessary field names for `recordKey => _row_key`, `partitionPath => partition`, and `precombineKey => timestamp`

```java
inputDF.write()
       .format("org.apache.hudi")
       .options(clientOpts) //Where clientOpts is of type Map[String, String]. clientOpts can include any other options necessary.
       .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
       .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
       .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
       .option(HoodieWriteConfig.TABLE_NAME, tableName)
       .mode(SaveMode.Append)
       .save(basePath);
```

## Flink SQL Writer
The hudi-flink module defines the Flink SQL connector for both hudi source and sink.
There are a number of options available for the sink table:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| path | Y | N/A | Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully |
| table.type  | N | COPY_ON_WRITE | Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ |
| write.operation | N | upsert | The write operation, that this write should do (insert or upsert is supported) |
| write.precombine.field | N | ts | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..) |
| write.payload.class | N | OverwriteWithLatestAvroPayload.class | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective |
| write.insert.drop.duplicates | N | false | Flag to indicate whether to drop duplicates upon insert. By default insert will accept duplicates, to gain extra performance |
| write.ignore.failed | N | true | Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. By default true (in favor of streaming progressing over data integrity) |
| hoodie.datasource.write.recordkey.field | N | uuid | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c` |
| hoodie.datasource.write.keygenerator.class | N | SimpleAvroKeyGenerator.class | Key generator class, that implements will extract the key out of incoming record |
| write.tasks | N | 4 | Parallelism of tasks that do actual write, default is 4 |
| write.batch.size.MB | N | 128 | Batch buffer size in MB to flush data into the underneath filesystem |

If the table type is MERGE_ON_READ, you can also specify the asynchronous compaction strategy through options:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| compaction.async.enabled | N | true | Async Compaction, enabled by default for MOR |
| compaction.trigger.strategy | N | num_commits | Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits; 'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction; 'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; 'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied. Default is 'num_commits' |
| compaction.delta_commits | N | 5 | Max delta commits needed to trigger compaction, default 5 commits |
| compaction.delta_seconds | N | 3600 | Max delta seconds time needed to trigger compaction, default 1 hour |

You can write the data using the SQL `INSERT INTO` statements:
```sql
INSERT INTO hudi_table select ... from ...; 
```

**Note**: INSERT OVERWRITE is not supported yet but already on the roadmap.

## Key Generation

Hudi maintains hoodie keys (record key + partition path) for uniquely identifying a particular record. Key generator class will extract these out of incoming record. Both the tools above have configs to specify the 
`hoodie.datasource.write.keygenerator.class` property. For DeltaStreamer this would come from the property file specified in `--props` and 
DataSource writer takes this config directly using `DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY()`.
The default value for this config is `SimpleKeyGenerator`. Note: A custom key generator class can be written/provided here as well. Primary key columns should be provided via `RECORDKEY_FIELD_OPT_KEY` option.<br/>
 
Hudi currently supports different combinations of record keys and partition paths as below - 

 - Simple record key (consisting of only one field) and simple partition path (with optional hive style partitioning)
 - Simple record key and custom timestamp based partition path (with optional hive style partitioning)
 - Composite record keys (combination of multiple fields) and composite partition paths
 - Composite record keys and timestamp based partition paths (composite also supported)
 - Non partitioned table

`CustomKeyGenerator.java` (part of hudi-spark module) class provides great support for generating hoodie keys of all the above listed types. All you need to do is supply values for the following properties properly to create your desired keys - 

```java
hoodie.datasource.write.recordkey.field
hoodie.datasource.write.partitionpath.field
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.CustomKeyGenerator
```

For having composite record keys, you need to provide comma separated fields like
```java
hoodie.datasource.write.recordkey.field=field1,field2
```

This will create your record key in the format `field1:value1,field2:value2` and so on, otherwise you can specify only one field in case of simple record keys. `CustomKeyGenerator` class defines an enum `PartitionKeyType` for configuring partition paths. It can take two possible values - SIMPLE and TIMESTAMP. 
The value for `hoodie.datasource.write.partitionpath.field` property in case of partitioned tables needs to be provided in the format `field1:PartitionKeyType1,field2:PartitionKeyType2` and so on. For example, if you want to create partition path using 2 fields `country` and `date` where the latter has timestamp based values and needs to be customised in a given format, you can specify the following 

```java
hoodie.datasource.write.partitionpath.field=country:SIMPLE,date:TIMESTAMP
``` 
This will create the partition path in the format `<country_name>/<date>` or `country=<country_name>/date=<date>` depending on whether you want hive style partitioning or not.

`TimestampBasedKeyGenerator` class defines the following properties which can be used for doing the customizations for timestamp based partition paths

```java
hoodie.deltastreamer.keygen.timebased.timestamp.type
  This defines the type of the value that your field contains. It can be in string format or epoch format, for example
hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit
  This defines the granularity of your field, whether it contains the values in seconds or milliseconds
hoodie.deltastreamer.keygen.timebased.input.dateformat
  This defines the custom format in which the values are present in your field, for example yyyy/MM/dd
hoodie.deltastreamer.keygen.timebased.output.dateformat
  This defines the custom format in which you want the partition paths to be created, for example dt=yyyyMMdd
hoodie.deltastreamer.keygen.timebased.timezone
  This defines the timezone which the timestamp based values belong to
```

When keygenerator class is `CustomKeyGenerator`, non partitioned table can be handled by simply leaving the property blank like
```java
hoodie.datasource.write.partitionpath.field=
```

For those on hudi versions < 0.6.0, you can use the following key generator classes for fulfilling your use cases - 

 - Simple record key (consisting of only one field) and simple partition path (with optional hive style partitioning) - `SimpleKeyGenerator.java`
 - Simple record key and custom timestamp based partition path (with optional hive style partitioning) - `TimestampBasedKeyGenerator.java`
 - Composite record keys (combination of multiple fields) and composite partition paths - `ComplexKeyGenerator.java`
 - Composite record keys and timestamp based partition paths (composite also supported) - You might need to move to 0.6.0 and use `CustomKeyGenerator.java` class
 - Non partitioned table - `NonpartitionedKeyGenerator.java`. Non-partitioned tables can currently only have a single key column, [HUDI-1053](https://issues.apache.org/jira/browse/HUDI-1053)
 
 
## Syncing to Hive

Both tools above support syncing of the table's latest schema to Hive metastore, such that queries can pick up new columns and partitions.
In case, its preferable to run this from commandline or in an independent jvm, Hudi provides a `HiveSyncTool`, which can be invoked as below, 
once you have built the hudi-hive module. Following is how we sync the above Datasource Writer written table to Hive metastore.

```java
cd hudi-hive
./run_sync_tool.sh  --jdbc-url jdbc:hive2:\/\/hiveserver:10000 --user hive --pass hive --partitioned-by partition --base-path <basePath> --database default --table <tableName>
```

Starting with Hudi 0.5.1 version read optimized version of merge-on-read tables are suffixed '_ro' by default. For backwards compatibility with older Hudi versions, an optional HiveSyncConfig - `--skip-ro-suffix`, has been provided to turn off '_ro' suffixing if desired. Explore other hive sync options using the following command:

```java
cd hudi-hive
./run_sync_tool.sh
 [hudi-hive]$ ./run_sync_tool.sh --help
```

## Deletes 

Hudi supports implementing two types of deletes on data stored in Hudi tables, by enabling the user to specify a different record payload implementation. 
For more info refer to [Delete support in Hudi](https://cwiki.apache.org/confluence/x/6IqvC).

 - **Soft Deletes** : Retain the record key and just null out the values for all the other fields. 
 This can be achieved by ensuring the appropriate fields are nullable in the table schema and simply upserting the table after setting these fields to null.
 
 - **Hard Deletes** : A stronger form of deletion is to physically remove any trace of the record from the table. This can be achieved in 3 different ways.

   1) Using DataSource, set `OPERATION_OPT_KEY` to `DELETE_OPERATION_OPT_VAL`. This will remove all the records in the DataSet being submitted.
   
   2) Using DataSource, set `PAYLOAD_CLASS_OPT_KEY` to `"org.apache.hudi.EmptyHoodieRecordPayload"`. This will remove all the records in the DataSet being submitted. 
   
   3) Using DataSource or DeltaStreamer, add a column named `_hoodie_is_deleted` to DataSet. The value of this column must be set to `true` for all the records to be deleted and either `false` or left null for any records which are to be upserted.
    
Example using hard delete method 2, remove all the records from the table that exist in the DataSet `deleteDF`:
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

 - The [small file handling feature](/docs/configurations#compactionSmallFileSize) in Hudi, profiles incoming workload 
   and distributes inserts to existing file groups instead of creating new file groups, which can lead to small files. 
 - Cleaner can be [configured](/docs/configurations#retainCommits) to clean up older file slices, more or less aggressively depending on maximum time for queries to run & lookback needed for incremental pull
 - User can also tune the size of the [base/parquet file](/docs/configurations#limitFileSize), [log files](/docs/configurations#logFileMaxSize) & expected [compression ratio](/docs/configurations#parquetCompressionRatio), 
   such that sufficient number of inserts are grouped into the same file group, resulting in well sized base files ultimately.
 - Intelligently tuning the [bulk insert parallelism](/docs/configurations#withBulkInsertParallelism), can again in nicely sized initial file groups. It is in fact critical to get this right, since the file groups
   once created cannot be deleted, but simply expanded as explained before.
 - For workloads with heavy updates, the [merge-on-read table](/docs/concepts#merge-on-read-table) provides a nice mechanism for ingesting quickly into smaller files and then later merging them into larger base files via compaction.
