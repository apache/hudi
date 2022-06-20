---
title: "Older Releases"
sidebar_position: 9
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---

This page contains older release information, for bookkeeping purposes. It's recommended that you upgrade to one of the 
more recent releases listed [here](/releases/download)

## [Release 0.6.0](https://github.com/apache/hudi/releases/tag/release-0.6.0) ([docs](/docs/quick-start-guide))

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- With 0.6.0 Hudi is moving from list based rollback to marker based rollbacks. To smoothly aid this transition a
  new property called `hoodie.table.version` is added to `hoodie.properties` file. Whenever Hudi is launched with
  newer table version i.e 1 (or moving from pre 0.6.0 to 0.6.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the `hoodie.table.version` will be updated in property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - `downgrade`) is added if in case some users want to downgrade Hudi from table version 1 to 0 or move from Hudi 0.6.0 to pre 0.6.0
- If you were using a user defined partitioner with bulkInsert() RDD API, the base interface has changed to `BulkInsertPartitioner` and will need minor adjustments to your existing implementations.

## Release Highlights

### Writer side improvements:
- Bootstrapping existing parquet datasets :  Adds support for bootstrapping existing datasets into Hudi, via both Spark datasource writer and
  deltastreamer tool, with support for reading from Hive, SparkSQL, AWS Athena (prestoDB support coming soon). See [RFC-15](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements) for technical details.
  Note that this is an experimental feature, which will be improved upon further in the 0.6.x versions.
- Native row writing for bulk_insert : Avoids any dataframe-rdd conversion for bulk_insert path, which can improve performance of initial bulk loads.
  Although, this is typically not the bottleneck for upsert/deletes, subsequent releases in 0.6.x versions will expand this to other write operations
  to make reasoning about schema management easier, avoiding the spark-avro conversion totally.
- Bulk insert sort modes : Hudi bulk_insert sorts the input globally to optimize file sizes and avoid out-of-memory issues encountered when writing parallely to multiple DFS partitions.
  For users who want to prepare the dataframe for writing outside of Hudi, we have made this configurable using `hoodie.bulkinsert.sort.mode`.
- Cleaning can now be run concurrently with writing, using `hoodie.clean.async=true`which can speed up time taken to finish committing.
- Async compaction for spark streaming writes to hudi table, is now self managed by default, controlling `hoodie.datasource.compaction.async.enable`.
- Rollbacks no longer perform full table listings, by leveraging marker files. To enable, set `hoodie.rollback.using.markers=true`.
- Added a new index `hoodie.index.type=SIMPLE` which can be faster than `BLOOM_INDEX` for cases where updates/deletes spread across a large portion of the table.
- Hudi now supports `Azure Data Lake Storage V2` , `Alluxio` and `Tencent Cloud Object Storage` storages.
- [HoodieMultiDeltaStreamer](https://hudi.apache.org/docs/writing_data#multitabledeltastreamer) adds support for ingesting multiple kafka streams in a single DeltaStreamer deployment, effectively reducing operational burden for using delta streamer
  as your data lake ingestion tool (Experimental feature)
- Added a new tool - InitialCheckPointProvider, to set checkpoints when migrating to DeltaStreamer after an initial load of the table is complete.
- Delta Streamer tool now supports ingesting CSV data sources, chaining of multiple transformers to build more advanced ETL jobs.
- Introducing a new `CustomKeyGenerator` key generator class, that provides flexible configurations to provide enable different types of key, partition path generation in  single class.
  We also added support for more time units and date/time formats in `TimestampBasedKeyGenerator`. See [docs](https://hudi.apache.org/docs/writing_data#key-generation) for more.

### Query side improvements:
- Starting 0.6.0, snapshot queries are feasible on MOR tables using spark datasource. (experimental feature)
- In prior versions we only supported `HoodieCombineHiveInputFormat` for CopyOnWrite tables to ensure that there is a limit on the number of mappers spawned for
  any query. Hudi now supports Merge on Read tables also using `HoodieCombineInputFormat`.
- Speedup spark read queries by caching metaclient in HoodieROPathFilter. This helps reduce listing related overheads in S3 when filtering files for read-optimized queries.

### Usability:
- Spark DAGs are named to aid better debuggability.
- Support pluggable metrics reporting by introducing proper abstraction for user defined metrics. Console, JMX, Prometheus and DataDog metric reporters have been added.
- A new utility called Data snapshot exporter has been added. Latest table snapshot as of a certain point in time can be exported as plain parquet files with this tool.
- Introduce write committed callback hooks for incremental pipelines to be notified and act on new commits in the timeline. For e.g, Apache Airflow jobs can be triggered
  as new commits arrive.
- Added support for deleting savepoints via CLI
- Added a new command - `export instants`, to export metadata of instants

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346663)

## [Release 0.5.3](https://github.com/apache/hudi/releases/tag/release-0.5.3) ([docs](/docs/quick-start-guide))

## Migration Guide for this release
* This is a bug fix only release and no special migration steps needed when upgrading from 0.5.2. If you are upgrading from earlier releases “X”, please make sure you read the migration guide for each subsequent release between “X” and 0.5.3
* 0.5.3 is the first hudi release after graduation. As a result, all hudi jars will no longer have "-incubating" in the version name. In all the places where hudi version is referred, please make sure "-incubating" is no longer present.

For example hudi-spark-bundle pom dependency would look like:
```xml
    <dependency>
        <groupId>org.apache.hudi</groupId>
        <artifactId>hudi-spark-bundle_2.12</artifactId>
        <version>0.5.3</version>
    </dependency>
```
## Release Highlights
* Hudi now supports `aliyun OSS` storage service.
* Embedded Timeline Server is enabled by default for both delta-streamer and spark datasource writes. This feature was in experimental mode before this release. Embedded Timeline Server caches file listing calls in Spark driver and serves them to Spark writer tasks. This reduces the number of file listings needed to be performed for each write.
* Incremental Cleaning is enabled by default for both delta-streamer and spark datasource writes. This feature was also in experimental mode before this release. In the steady state, incremental cleaning avoids the costly step of scanning all partitions and instead uses Hudi metadata to find files to be cleaned up.
* Delta-streamer config files can now be placed in different filesystem than actual data.
* Hudi Hive Sync now supports tables partitioned by date type column.
* Hudi Hive Sync now supports syncing directly via Hive MetaStore. You simply need to set hoodie.datasource.hive_sync.use_jdbc
  =false. Hive Metastore Uri will be read implicitly from environment. For example, when writing through Spark Data Source,

```Scala
 spark.write.format(“hudi”)
 .option(…)
 .option(“hoodie.datasource.hive_sync.username”, “<user>”)
 .option(“hoodie.datasource.hive_sync.password”, “<password>”)
 .option(“hoodie.datasource.hive_sync.partition_fields”, “<partition_fields>”)
 .option(“hoodie.datasource.hive_sync.database”, “<db_name>”)
 .option(“hoodie.datasource.hive_sync.table”, “<table_name>”)
 .option(“hoodie.datasource.hive_sync.use_jdbc”, “false”)
 .mode(APPEND)
 .save(“/path/to/dataset”)
```

* Other Writer Performance related fixes:
  - DataSource Writer now avoids unnecessary loading of data after write.
  - Hudi Writer now leverages spark parallelism when searching for existing files for writing new records.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12348256)


## [Release 0.5.2-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.2-incubating) ([docs](/docs/quick-start-guide))

### Migration Guide for this release
* Write Client restructuring has moved classes around ([HUDI-554](https://issues.apache.org/jira/browse/HUDI-554)). Package `client` now has all the various client classes, that do the transaction management. `func` renamed to `execution` and some helpers moved to `client/utils`. All compaction code under `io` now under `table/compact`. Rollback code under `table/rollback` and in general all code for individual operations under `table`. This change only affects the apps/projects depending on hudi-client. Users of deltastreamer/datasource will not need to change anything.

### Release Highlights
* Support for overwriting the payload implementation in `hoodie.properties` via specifying the `hoodie.compaction.payload.class` config option. Previously, once the payload class is set once in `hoodie.properties`, it cannot be changed. In some cases, if a code refactor is done and the jar updated, one may need to pass the new payload class name.
* `TimestampBasedKeyGenerator` supports for `CharSequence`  types. Previously `TimestampBasedKeyGenerator` only supports `Double`, `Long`, `Float` and `String` 4 data types for the partition key. Now, after data type extending, `CharSequence` has been supported in `TimestampBasedKeyGenerator`.
* Hudi now supports incremental pulling from defined partitions via the `hoodie.datasource.read.incr.path.glob` [config option](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/scala/org/apache/hudi/DataSourceOptions.scala#L111). For some use case that users only need to pull the incremental part of certain partitions, it can run faster by only loading relevant parquet files.
* With 0.5.2, hudi allows partition path to be updated with `GLOBAL_BLOOM` index. Previously, when a record is to be updated with a new partition path, and when set to `GLOBAL_BLOOM` as index, hudi ignores the new partition path and update the record in the original partition path. Now, hudi allows records to be inserted into their new partition paths and delete the records in the old partition paths. A configuration (e.g. `hoodie.index.bloom.update.partition.path=true`) can be added to enable this feature.
* A `JdbcbasedSchemaProvider` schema provider has been provided to get metadata through JDBC. For the use case that users want to synchronize data from MySQL, and at the same time, want to get the schema from the database, it's very helpful.
* Simplify `HoodieBloomIndex` without the need for 2GB limit handling. Prior to spark 2.4.0, each spark partition has a limit of 2GB. In Hudi 0.5.1, after we upgraded to spark 2.4.4, we don't have the limitation anymore. Hence removing the safe parallelism constraint we had in` HoodieBloomIndex`.
* CLI related changes:
    - Allows users to specify option to print additional commit metadata, e.g. *Total Log Blocks*, *Total Rollback Blocks*, *Total Updated Records Compacted* and so on.
    - Supports `temp_query` and `temp_delete` to query and delete temp view. This command creates a temp table. Users can write HiveQL queries against the table to filter the desired row. For example,
```
temp_query --sql "select Instant, NumInserts, NumWrites from satishkotha_debug where FileId='ed33bd99-466f-4417-bd92-5d914fa58a8f' and Instant > '20200123211217' order by Instant"
```

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346606)

## [Release 0.5.1-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.1-incubating) ([docs](/docs/quick-start-guide))

### Migration Guide for this release
* Upgrade hudi readers (query engines) first with 0.5.1-incubating release before upgrading writer.
* In 0.5.1, the community restructured the package of key generators. The key generator related classes have been moved from `org.apache.hudi` to `org.apache.hudi.keygen`.

### Release Highlights
* Dependency Version Upgrades
    - Upgrade from Spark 2.1.0 to Spark 2.4.4
    - Upgrade from Avro 1.7.7 to Avro 1.8.2
    - Upgrade from Parquet 1.8.1 to Parquet 1.10.1
    - Upgrade from Kafka 0.8.2.1 to Kafka 2.0.0 as a result of updating spark-streaming-kafka artifact from 0.8_2.11/2.12 to 0.10_2.11/2.12.
* **IMPORTANT** This version requires your runtime spark version to be upgraded to 2.4+.
* Hudi now supports both Scala 2.11 and Scala 2.12, please refer to [Build with Scala 2.12](https://github.com/apache/hudi#build-with-scala-212) to build with Scala 2.12.
  Also, the packages hudi-spark, hudi-utilities, hudi-spark-bundle and hudi-utilities-bundle are changed correspondingly to hudi-spark_{scala_version}, hudi-spark_{scala_version}, hudi-utilities_{scala_version}, hudi-spark-bundle_{scala_version} and hudi-utilities-bundle_{scala_version}.
  Note that scala_version here is one of (2.11, 2.12).
* With 0.5.1, we added functionality to stop using renames for Hudi timeline metadata operations. This feature is automatically enabled for newly created Hudi tables. For existing tables, this feature is turned off by default. Please read this [section](https://hudi.apache.org/docs/deployment#upgrading), before enabling this feature for existing hudi tables.
  To enable the new hudi timeline layout which avoids renames, use the write config "hoodie.timeline.layout.version=1". Alternatively, you can use "repair overwrite-hoodie-props" to append the line "hoodie.timeline.layout.version=1" to hoodie.properties. Note that in any case, upgrade hudi readers (query engines) first with 0.5.1-incubating release before upgrading writer.
* CLI supports `repair overwrite-hoodie-props` to overwrite the table's hoodie.properties with specified file, for one-time updates to table name or even enabling the new timeline layout above. Note that few queries may temporarily fail while the overwrite happens (few milliseconds).
* DeltaStreamer CLI parameter for capturing table type is changed from `--storage-type` to `--table-type`. Refer to [wiki](https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture) with more latest terminologies.
* Configuration Value change for Kafka Reset Offset Strategies. Enum values are changed from LARGEST to LATEST, SMALLEST to EARLIEST for configuring Kafka reset offset strategies with configuration(auto.offset.reset) in deltastreamer.
* When using spark-shell to give a quick peek at Hudi, please provide `--packages org.apache.spark:spark-avro_2.11:2.4.4`, more details would refer to [latest quickstart docs](https://hudi.apache.org/docs/quick-start-guide)
* Key generator moved to separate package under org.apache.hudi.keygen. If you are using overridden key generator classes (configuration ("hoodie.datasource.write.keygenerator.class")) that comes with hudi package, please ensure the fully qualified class name is changed accordingly.
* Hive Sync tool will register RO tables for MOR with a _ro suffix, so query with _ro suffix. You would use `--skip-ro-suffix` in sync config in sync config to retain the old naming without the _ro suffix.
* With 0.5.1, hudi-hadoop-mr-bundle which is used by query engines such as presto and hive includes shaded avro package to support hudi real time queries through these engines. Hudi supports pluggable logic for merging of records. Users provide their own implementation of [HoodieRecordPayload](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java).
  If you are using this feature, you need to relocate the avro dependencies in your custom record payload class to be consistent with internal hudi shading. You need to add the following relocation when shading the package containing the record payload implementation.

  ```xml
  <relocation>
    <pattern>org.apache.avro.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.avro.</shadedPattern>
  </relocation>
  ```

* Better delete support in DeltaStreamer, please refer to [blog](https://cwiki.apache.org/confluence/display/HUDI/2020/01/15/Delete+support+in+Hudi) for more info.
* Support for AWS Database Migration Service(DMS) in DeltaStreamer, please refer to [blog](https://cwiki.apache.org/confluence/display/HUDI/2020/01/20/Change+Capture+Using+AWS+Database+Migration+Service+and+Hudi) for more info.
* Support for DynamicBloomFilter. This is turned off by default, to enable the DynamicBloomFilter, please use the index config `hoodie.bloom.index.filter.type=DYNAMIC_V0`.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346183)

## [Release 0.5.0-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.0-incubating) ([docs](/docs/quick-start-guide))

### Release Highlights
* Package and format renaming from com.uber.hoodie to org.apache.hudi (See migration guide section below)
* Major redo of Hudi bundles to address class and jar version mismatches in different environments
* Upgrade from Hive 1.x to Hive 2.x for compile time dependencies - Hive 1.x runtime integration still works with a patch : See [the discussion thread](https://lists.apache.org/thread/48b3f0553f47c576fd7072f56bb0d8a24fb47d4003880d179c7f88a3@%3Cdev.hudi.apache.org%3E)
* DeltaStreamer now supports continuous running mode with managed concurrent compaction
* Support for Composite Keys as record key
* HoodieCombinedInputFormat to scale huge hive queries running on Hoodie tables

### Migration Guide for this release
This is the first Apache release for Hudi. Prior to this release, Hudi Jars were published using "com.uber.hoodie" maven co-ordinates. We have a [migration guide](https://cwiki.apache.org/confluence/display/HUDI/Migration+Guide+From+com.uber.hoodie+to+org.apache.hudi)

### Raw Release Notes
The raw release notes are available [here](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346087)

## [Release 0.4.7](https://github.com/apache/hudi/releases/tag/hoodie-0.4.7)

### Release Highlights

* Major releases with fundamental changes to filesystem listing & write failure handling
* Introduced the first version of HoodieTimelineServer that runs embedded on the driver
* With all executors fetching filesystem listing via RPC to timeline server, drastically reduced filesystem listing!
* Failing concurrent write tasks are now handled differently to be robust against spark stage retries
* Bug fixes/clean up around indexing, compaction

### PR LIST

- Skip Meta folder when looking for partitions. [#698](https://github.com/apache/hudi/pull/698)
- HUDI-134 - Disable inline compaction for Hoodie Demo. [#696](https://github.com/apache/hudi/pull/696)
- Default implementation for HBase index qps allocator. [#685](https://github.com/apache/hudi/pull/685)
- Handle duplicate record keys across partitions. [#687](https://github.com/apache/hudi/pull/687)
- Fix up offsets not available on leader exception. [#650](https://github.com/apache/hudi/pull/650)
