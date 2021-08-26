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
- Specifically check upgrade instructions for 0.6.0. This release does not introduce any new table versions.
- The `HoodieRecordPayload` interface deprecated existing methods, in favor of new ones that also lets us pass properties at runtime. Users are
encouraged to migrate out of the deprecated methods, since they will be removed in 0.9.0.

## Release Highlights

### Spark SQL DML and DDL Support

0.9.0 brings the ability to perform data manipulation (inserts, upserts and deletes), as well as improves the ability to
perform data definition (create, modify and truncate) using the familiar Structured Query Language (SQL) via Spark SQL.
This is huge step towards making Hudi even more easy to use. Customers can now use `INSERT`, `UPDATE`, `MERGE INTO` and `DELETE`
sql statements to manipulate data. Bulk insert can be performed by setting `hoodie.sql.bulk.insert.enable=true` while using
the INSERT statement. In addition, `INSERT OVERWRITE` statement can be used to overwrite existing data in the table or partition
with new values.

On the data definition side, customers can now use `CREATE TABLE....USING HUDI` statement, which creates and registers the
table as Hudi Spark DataSource table, instead of the earlier approach of having to define Hudi Input Format and Parquet
Serde. They can also use `CREATE TABLE....AS`, `ALTER TABLE` and `TRUNCATE TABLE` statements to create, modify or truncate table
definitions.

From utilities perspective, 0.9.0 introduces `SCHEDULE COMPACTION` and `RUN COMPACTION` statements to easily schedule and run
compactions on a Hudi table or path. All the above mentioned functionality, works for both Spark 2 and Spark 3.

Please see [RFC-25](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)
for more implementation details and follow this [page](/docs/quick-start-guide) to get started with DML and DDL!

### Query side improvements
- **Register and Read as DataSource Table**:
- **Metadata based File Listing Improvements for Spark**:

### Writer side improvements
- **Virtual Keys Support**: HUDI-2235
- **DeltaStreamer Improvements**: HUDI-1897, HUDI-251, HUDI-1910, HUDI-1944, HUDI-1790
- **Async Clustering Support with Spark Streaming and DeltaStreamer**: HUDI-1483
- **Bulk Insert V2 Improvements**:

### Flink Integration Improvements
- **Asynchronous Hive Sync and Clenaing**: HUDI-1729
- **Insert Overwrite Support**: HUDI-1788
- **Bulk Insert Support**: HUDI-2209
- **Non Partitioned Table Support**: HUDI-1814
- **Streaming Read for COW Tables**: HUDI-1867
- **Emit Deletes for MOR Streaming Read**: HUDI-1738
- **Global Index Support**: HUDI-1908

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350027)