---
title: "Release 0.9.0"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2021-08-26T08:40:00-07:00
---
# [Release 0.9.0](https://github.com/apache/hudi/releases/tag/release-0.9.0) ([docs](/docs/quick-start-guide))

## Download Information
* Source Release : [Apache Hudi 0.9.0 Source Release](https://downloads.apache.org/hudi/0.9.0/hudi-0.9.0.src.tgz) ([asc](https://downloads.apache.org/hudi/0.9.0/hudi-0.9.0.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.9.0/hudi-0.9.0.src.tgz.sha512))
* Apache Hudi jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

## Migration Guide for this release
- If migrating from an older release, please also check the upgrade instructions for each subsequent release below.
- With 0.9.0, Hudi is adding more table properties to aid in using an existing hudi table with spark-sql. 
  To smoothly aid this transition these properties added to `hoodie.properties` file. Whenever Hudi is launched with
  newer table version i.e 2 (or moving from pre 0.9.0 to 0.9.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the `hoodie.table.version` will be updated in 
  property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - `downgrade`) is added if in case some users want to 
  downgrade Hudi from table version `2` to `1` or move from Hudi 0.9.0 to pre 0.9.0. This needs to be executed from a 
  0.9.0 `hudi-cli` binary/script.
- With this release, we added a new framework to track config properties in code, moving away from string variables that 
  hold property names and values. This move helps us automate configuration doc generation and much more. While we still
  support the older configs string variables, users are encouraged to use the new `ConfigProperty` equivalents, as noted
  in the deprecation notices. In most cases, it is as simple as calling `.key()` and `.defaultValue()` on the corresponding
  alternative. e.g `RECORDKEY_FIELD_OPT_KEY` can be replaced by `RECORDKEY_FIELD_NAME.key()`

## Release Highlights

### Spark SQL DML and DDL Support

0.9.0 adds **experimental** support for DDL/DMLs using Spark SQL, taking a huge step towards making Hudi more easily accessible and 
operable by all personas (non-engineers, analysts etc). Users can now use `CREATE TABLE....USING HUDI` and `CREAT TABLE .. AS SELECT` 
statements to directly create and manage tables in catalogs like Hive. Users can then use `INSERT`, `UPDATE`, `MERGE INTO` and `DELETE`
sql statements to manipulate data. In addition, `INSERT OVERWRITE` statement can be used to overwrite existing data in the table or partition
for existing batch ETL pipelines. For more information, checkout our docs [here](/docs/quick-start-guide) clicking on `SparkSQL` tab.
Please see [RFC-25](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)
for more implementation details.

### Query side improvements

Hudi tables are now registered with Hive as spark datasource tables, meaning Spark SQL on these tables now uses the datasource as well,
instead of relying on the Hive fallbacks within Spark, which are ill-maintained/cumbersome. This unlocks many optimizations such as the 
use of Hudi's own [FileIndex](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/hudi/HoodieFileIndex.scala#L46) 
implementation for optimized caching and the use of the Hudi metadata table, for faster listing of large tables. We have also added support for 
[timetravel query](/docs/quick-start-guide#time-travel-query), for spark datasource.

### Writer side improvements 

Virtual keys support has been added where users can avoid adding meta fields to hudi table and leverage existing fields to populate record keys and partition paths.
One needs to disable [this](/docs/configurations#hoodiepopulatemetafields) config to enable virtual keys. 

Bulk Insert operations using [row writer enabled](/docs/configurations#hoodiedatasourcewriterowwriterenable) now supports pre-combining, 
sort modes and user defined partitioners and now turned on by default for fast inserts.

Hudi performs automatic cleanup of uncommitted data, which has now been enhanced to be performant over cloud storage, even for
extremely large tables. Specifically, a new marker mechanism has been implemented leveraging the timeline server to perform
centrally co-ordinated batched read/write of file markers to underlying storage. You can turn this using this [config](??) and learn more 
about it on this [blog](??).

Async Clustering support has been added to both DeltaStreamer and Spark Structured Streaming Sink. More on this can be found in this
[blog post](/blog/2021/08/23/async-clustering). In addition, we have added a new utility class [HoodieClusteringJob](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieClusteringJob.java) 
to assist in building and executing a clustering plan together as a standalone spark job.

Users can choose to drop fields used to generate partition paths, using `hoodie.datasource.write.drop.partition.columns=true`, to support 
querying of Hudi snapshots using systems like BigQuery, which cannot handle this.

Hudi uses different types of spillable maps, for internally handling merges (compaction, updates or even MOR snapshot queries). In 0.9.0, we have added
support for [compression](??) for the bitcask style default option and introduced a new spillable map backed by [rocksDB](??).

Added a new write operation "delete_partition" operation, with support in spark. Users can leverage this to delete older partitions in bulk, in addition to
record level deletes. <FIXME:how does one use this?>    

Support for Huawei Cloud Object Storage, BAIDU AFS storage format, Baidu BOS storage in Hudi. 

A pre commit validator framework has been added to spark engine. Users can leverage this to add validations like verifying all valid
files are present for given instant, or all invalid files are removed, etc as part of this framework.
<FIXME: how does one use built in ones, how can i extend and build my own>


### Flink Integration Improvements
- **Insert Overwrite Support**: HUDI-1788
- **Bulk Insert Support**: HUDI-2209
- **Non Partitioned Table Support**: HUDI-1814
- **Streaming Read for COW Tables**: HUDI-1867
- **Emit Deletes for MOR Streaming Read**: HUDI-1738
- **Global Index Support**: HUDI-1908
- Propagate CDC format for Hudi
- Emit deletes for Flink MOR table streaming read 
- Supports hive style partitioning for flink writer
- Support Transformer for HoodieFlinkStreamer
-  Metadata table for flink
-  Support hive3 meta sync for flink writer
-  Support hive1 metadata sync for flink writer
-  Asynchronous Hive sync and commits cleaning for Flink writer
-  Support flink hive sync in batch mode-
-  Support Append only in Flink stream
-  Add marker files for flink writer

## Utilities
### DeltaStreamer

- Couple of sources have been added to deltastreamer. 
  1. JDBC source can assist in reading data from RDBMS sources.
  2. SQLSource can assist in backfill use-cases like adding a new column and backfilling just one column for the past 6 months.
  3. S3EventsHoodieIncrSource and S3EventsSource assists in reading data from S3 reliably and efficiently ingesting data to Hudi. 
     Existing approach of using DFSSource uses last modification time of files as checkpoint to pull in new files, but if large 
     number of files have same modification time, might run into issues of missing some files to be read from source. 
     These two sources (S3EventsHoodieIncrSource and S3EventsSource) together ensures data is reliably ingested from S3 into 
     Hudi by leveraging AWS SNS and SQS services that subscribes to file events from the source bucket.
- In addition to pulling data from kafka for kafka sources using DeltaStreamer using regular offset format 
  (topic_name,partition_num:offset,partition_num:offset,....), we also added two new formats. Timestampd based and 
  Group consumer offset. 
- Added support to pass in basic auth credentials in schema registry provider url with schema provider in deltastreamer.   
- Some improvements have been made to hudi-cli like `SCHEDULE COMPACTION` and `RUN COMPACTION` statements to easily schedule and run
compactions on a Hudi table or path. Clustering Command has been added to hudi-cli to assist in running and scheduling 
  clustering plans with Hudi.
- Grafana dashboard?   

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350027)