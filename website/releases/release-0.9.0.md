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
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- Specifically check upgrade instructions for 0.6.0.
- With 0.9.0 Hudi is adding more table properties to aid in using an existing hudi table with spark-sql. 
  To smoothly aid this transition these properties added to `hoodie.properties` file. Whenever Hudi is launched with
  newer table version i.e 2 (or moving from pre 0.9.0 to 0.9.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the `hoodie.table.version` will be updated in 
  property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - `downgrade`) is added if in case some users want to 
  downgrade Hudi from table version 2 to 1 or move from Hudi 0.9.0 to pre 0.9.0
- This release also replaces string representation of all configs to ConfigProperty. Older configs (strings) are deprecated
and users are encouraged to use the new ConfigProperties. 

## Release Highlights

### Spark SQL DML and DDL Support

0.9.0 brings the ability to perform data definition (create, modify and truncate) and data manipulation (inserts, 
upserts and deletes) on Hudi tables using the familiar Structured Query Language (SQL) via Spark SQL.
This is huge step towards making Hudi more easily accessible and operable by all personas (non-engineers, analysts etc).
Users can now use `INSERT`, `UPDATE`, `MERGE INTO` and `DELETE`
sql statements to manipulate data. In addition, `INSERT OVERWRITE` statement can be used to overwrite existing data in the table or partition
with new values. 

On the data definition side, customers can now use `CREATE TABLE....USING HUDI` statement, which creates and registers the
table as Hudi Spark DataSource table, instead of the earlier approach of having to define Hudi Input Format and Parquet
Serde. They can also use `CREATE TABLE....AS`, `ALTER TABLE` and `TRUNCATE TABLE` statements to create, modify or truncate table
definitions.

Please see [RFC-25](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)
for more implementation details and follow this [page](/docs/quick-start-guide) to get started with DML and DDL!

### Query side improvements
- Hudi tables can now be registered adn read as DataSource Table in spark. 
- **Metadata based File Listing Improvements for Spark**: ?? 
- Added support for timetravel query. Please check the [quick start page](http://localhost:3000/docs/next/quick-start-guide)
  on how to invoke the same. 

### Writer side improvements 
- Virtual keys support has been added where users can avoid adding meta fields to hudi table and leverage existing 
  fields to populate record keys and partition paths. One needs to disable [this](http://localhost:3000/docs/next/configurations#hoodiepopulatemetafields)
  config to enable virtual keys. 
- Clustering improvements:  
    - Async Clustering support has been added to both DeltaStreamer and Spark Streaming. More on this can be found in this 
      blog post and how to use the same.
    - Incremental read works for clustered data as well. ? This is more of a fix right? 
    - HoodieClusteringJob has been added to assist in building and executing a clustering plan together as a standalone job. 
    - Added a config (`hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions`) to skip latest N partitions 
      while creating the cluster plan.  
- Bulk_Insert using row writer path has been enhanced and brought to feature parity with WriteClient's version of Bulk_insert. 
Users can start to use the row writer path for better performance. 
- Added support for HMS in HiveSyncTool. HMSDDLExecutor is a DDLExecutor implementation, based on HMS which uses HMS 
  apis directly for all DDL tasks.
- A pre commit validator framework has been added to spark engine. Users can leverage this to add validations like verifying all valid
files are present for given instant, or all invalid files are removed, etc as part of this framework.
- Users can choose to drop fields used to generate partition paths if need be. (`hoodie.datasource.write.drop.partition.columns`)
- Added support for "delete_partition" operation to spark. Users can leverage this to delete older partitions as and when required.    
- Disk based map improvements? 
- Concurrency control: https://issues.apache.org/jira/browse/HUDI-944
- ORC format support: ?
- Support for Huawei Cloud Object Storage, BAIDU AFS storage format, Baidu BOS storage in Hudi: Should we expand more on this? 
- Marker file management has been enhanced to use batch mode and is now centrally coordinate by timeline server. This benefits 
cloud stores like S3 when large number of marker files are created or deleted during commits. You can read more on this blog.
- Java engine now support HoodieBloomIndex.

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