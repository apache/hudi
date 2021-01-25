---
title: "Releases"
permalink: /releases
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---
# [Release 0.7.0](https://github.com/apache/hudi/releases/tag/release-0.7.0) ([docs](/docs/0.7.0-quick-start-guide.html))

## Download Information
* Source Release : [Apache Hudi 0.7.0 Source Release](https://downloads.apache.org/hudi/0.7.0/hudi-0.7.0.src.tgz) ([asc](https://downloads.apache.org/hudi/0.7.0/hudi-0.7.0.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.7.0/hudi-0.7.0.src.tgz.sha512))
* Apache Hudi jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- Specifically check upgrade instructions for 0.6.0. This release does not introduce any new table versions.
- The `HoodieRecordPayload` interface deprecated existing methods, in favor of new ones that also lets us pass properties at runtime. Users are
encouraged to migrate out of the deprecated methods, since they will be removed in 0.8.0.

## Release Highlights

### Clustering

0.7.0 brings the ability to cluster your Hudi tables, to optimize for file sizes and also storage layout. Hudi will continue to
enforce file sizes, as it always has been, during the write. Clustering provides more flexibility to increase the file sizes 
down the line or ability to ingest data at much fresher intervals, and later coalesce them into bigger files. [Microbenchmarks](https://gist.github.com/vinothchandar/d7fa1338cddfae68390afcdfe310f94e#gistcomment-3383478)
demonstrate a 3-4x reduction in query performance, for a 10-20x reduction in file sizes.

Additionally, clustering data based on fields that are often used in queries, dramatically 
[improves query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance#RFC19Clusteringdataforfreshnessandqueryperformance-PerformanceEvaluation) by allowing many files to be
completely skipped. This is very similar to the benefits of clustering delivered by [cloud data warehouses](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions.html).
We are proud to announce that such capability is freely available in open source, for the first time, through the 0.7.0 release.

Please see [RFC-19](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance) for more implementation details
and checkout configs [here](/docs/configurations.html#clustering-configs) for how to use it in your pipelines. At the moment, we support both inline and async clustering modes.

### Metadata Table

Since Hudi was born at Uber, on a HDFS backed data lake, we have since been a tad apathetic to the plight of listing performance on cloud storage (partly in hopes that
cloud providers will fix it over time:)). Nonetheless, 0.7.0 changes this and lays out the foundation for storing more indexes, metadata in an internal metadata table, 
which is implemented using a Hudi MOR table - which means it's compacted, cleaned and also incrementally updated like any other Hudi table. Also, unlike similar 
implementations in other projects, we have chosen to index the file listing information as HFiles, which offers point-lookup performance to fetch listings for a single partition. 

In 0.7.0 release, `hoodie.metadata.enable=true` on the writer side, will populate the metadata table with file system listings
so all operations don't have to explicitly use `fs.listStatus()` anymore on data partitions. We have introduced a sync mechanism that
keeps syncing file additions/deletions on the data timeline, to the metadata table, after each write operation.

In our testing, on a large 250K file table, the metadata table delivers [2-3x speedup](https://github.com/apache/hudi/pull/2441#issuecomment-761742963) over parallelized 
listing done by the Hudi spark writer. Please check [RFC-15 (ongoing)](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements)
and the [configurations](/docs/configurations.html#metadata-config), which offer flags to help adopt this feature safely in your production pipelines.

### Java/Flink Writers

Hudi was originally designed with a heavy dependency on Spark, given it had simply solve specific problems at Uber. But, as we have evolved as an Apache 
project, we realized the need for abstracting the internal table format, table services and writing layers of code. In 0.7.0, we have additionally added
Java and Flink based writers, as initial steps in this direction.

Specifically, the `HoodieFlinkStreamer` allows for Hudi Copy-On-Write table to built by streaming data from a Kafka topic.

### Writer side improvements

- **Spark3 Support**: We have added support for writing/querying data using Spark 3. please be sure to use the scala 2.12 hudi-spark-bundle.
- **Parallelized Listing**: We have holistically moved all listings under the `HoodieTableMetadata` interface, which does multi-threaded/spark parallelized list operations. 
  We expect this to improve cleaner, compaction scheduling performance, even when the metadata table is not used.
- **Kafka Commit Callbacks**: We have added `HoodieWriteCommitKafkaCallback`, that publishes an event to Apache Kafka, for every commit operation. This can be used to trigger
  derived/ETL pipelines similar to data [sensors](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/index.html) in Apache Airflow.
- **Insert Overwrite/Insert Overwrite Table**: We have added these two new write operation types, predominantly to help existing batch ETL jobs, which typically overwrite entire 
  tables/partitions each run. These operations are much cheaper, than having to issue upserts, given they are bulk replacing the target table.
  Check [here](/docs/quick-start-guide.html#insert-overwrite-table) for examples.
- **Delete Partition**: For users of WriteClient/RDD level apis, we have added an API to delete an entire partition, again without issuing deletes at the record level.
- The current default `OverwriteWithLatestAvroPayload` will overwrite the value in storage, even if for e.g the upsert was reissued for an older value of the key.
  Added a new `DefaultHoodieRecordPayload` and a new payload config `hoodie.payload.ordering.field` helps specify a field, that the incoming upsert record can be compared with
  the record on storage, to decide whether to overwrite or not. Users are encouraged to adopt this newer, more flexible model.
- Hive sync supports hourly partitions via `SlashEncodedHourPartitionValueExtractor`
- Support for IBM Cloud storage, Open J9 JVM.

### Query side improvements

- **Incremental Query on MOR (Spark Datasource)**: Spark datasource now has experimental support for incremental queries on MOR table. This feature will be hardened and certified 
   in the next release, along with a large overhaul of the spark datasource implementation. (sshh!:))
- **Metadata Table For File Listings**: Users can also leverage the metadata table on the query side for the following query paths. For Hive, setting the `hoodie.metadata.enable=true` session
property and for SparkSQL on Hive registered tables using `--conf spark.hadoop.hoodie.metadata.enable=true`, allows the file listings for the partition to be fetched out of the metadata
  table, instead of listing the underlying DFS partition.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12348721)

# [Release 0.6.0](https://github.com/apache/hudi/releases/tag/release-0.6.0) ([docs](/docs/0.6.0-quick-start-guide.html))

## Download Information
 * Source Release : [Apache Hudi 0.6.0 Source Release](https://downloads.apache.org/hudi/0.6.0/hudi-0.6.0.src.tgz) ([asc](https://downloads.apache.org/hudi/0.6.0/hudi-0.6.0.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.6.0/hudi-0.6.0.src.tgz.sha512))
 * Apache Hudi jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

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
  - [HoodieMultiDeltaStreamer](https://hudi.apache.org/docs/writing_data.html#multitabledeltastreamer) adds support for ingesting multiple kafka streams in a single DeltaStreamer deployment, effectively reducing operational burden for using delta streamer 
    as your data lake ingestion tool (Experimental feature)
  - Added a new tool - InitialCheckPointProvider, to set checkpoints when migrating to DeltaStreamer after an initial load of the table is complete.
  - Delta Streamer tool now supports ingesting CSV data sources, chaining of multiple transformers to build more advanced ETL jobs.
  - Introducing a new `CustomKeyGenerator` key generator class, that provides flexible configurations to provide enable different types of key, partition path generation in  single class.
    We also added support for more time units and date/time formats in `TimestampBasedKeyGenerator`. See [docs](https://hudi.apache.org/docs/writing_data.html#key-generation) for more.

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

# [Release 0.5.3](https://github.com/apache/hudi/releases/tag/release-0.5.3) ([docs](/docs/0.5.3-quick-start-guide.html))

## Download Information
 * Source Release : [Apache Hudi 0.5.3 Source Release](https://downloads.apache.org/hudi/0.5.3/hudi-0.5.3.src.tgz) ([asc](https://downloads.apache.org/hudi/0.5.3/hudi-0.5.3.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.5.3/hudi-0.5.3.src.tgz.sha512))
 * Apache Hudi jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

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


For releases older than these versions, please see [here](/older-releases.html).