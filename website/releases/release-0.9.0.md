---
title: "Release 0.9.0"
sidebar_position: 19
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.9.0](https://github.com/apache/hudi/releases/tag/release-0.9.0)

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
- If set `URL_ENCODE_PARTITIONING_OPT_KEY=true` *and* `<` and `>` are present in the URL partition paths, users would
  need to migrate the table due to encoding logic changed: `<` (encoded as `%3C`) and `>` (encoded as `%3E`) won't be escaped in 0.9.0.

## Release Highlights

### Spark SQL DML and DDL Support

0.9.0 adds **experimental** support for DDL/DMLs using Spark SQL, taking a huge step towards making Hudi more easily accessible and
operable by all personas (non-engineers, analysts etc). Users can now use `CREATE TABLE....USING HUDI` and `CREATE TABLE .. AS SELECT`
statements to directly create and manage tables in catalogs like Hive. Users can then use `INSERT`, `UPDATE`, `MERGE INTO` and `DELETE`
sql statements to manipulate data. In addition, `INSERT OVERWRITE` statement can be used to overwrite existing data in the table or partition
for existing batch ETL pipelines. Please see [RFC-25](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)
for more implementation details.

### Query side improvements

Hudi tables are now registered with Hive as spark datasource tables, meaning Spark SQL on these tables now uses the datasource as well,
instead of relying on the Hive fallbacks within Spark, which are ill-maintained/cumbersome. This unlocks many optimizations such as the
use of Hudi's own [FileIndex](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/hudi/HoodieFileIndex.scala#L46)
implementation for optimized caching and the use of the Hudi metadata table, for faster listing of large tables. We have also added support for time-travel query, for spark datasource.

### Writer side improvements

Virtual keys support has been added where users can avoid adding meta fields to hudi table and leverage existing fields to populate record keys and partition paths.
One needs to disable [this](/docs/configurations#hoodiepopulatemetafields) config to enable virtual keys.

Bulk Insert operations using [row writer enabled](/docs/configurations#hoodiedatasourcewriterowwriterenable) now supports pre-combining,
sort modes and user defined partitioners and now turned on by default for fast inserts.

Hudi performs automatic cleanup of uncommitted data, which has now been enhanced to be performant over cloud storage, even for
extremely large tables. Specifically, a new marker mechanism has been implemented leveraging the timeline server to perform
centrally co-ordinated batched read/write of file markers to underlying storage. You can turn this using this [config](/docs/configurations#hoodiewritemarkerstype) and learn more
about it on this [blog](/blog/2021/08/18/improving-marker-mechanism).

Async Clustering support has been added to both DeltaStreamer and Spark Structured Streaming Sink. More on this can be found in this
[blog post](/blog/2021/08/23/async-clustering). In addition, we have added a new utility class [HoodieClusteringJob](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieClusteringJob.java)
to assist in building and executing a clustering plan together as a standalone spark job.

Users can choose to drop fields used to generate partition paths, using `hoodie.datasource.write.drop.partition.columns=true`, to support
querying of Hudi snapshots using systems like BigQuery, which cannot handle this.

Hudi uses different [types of spillable maps](/docs/configurations#hoodiecommonspillablediskmaptype), for internally handling merges (compaction, updates or even MOR snapshot queries). In 0.9.0, we have added
support for [compression](/docs/configurations#hoodiecommondiskmapcompressionenabled) for the bitcask style default option and introduced a new spillable map backed by rocksDB, which can be more performant for large
bulk updates or working with large base file sizes.

Added a new write operation `delete_partition` operation, with support in spark. Users can leverage this to delete older partitions in bulk, in addition to
record level deletes. Deletion of specific partitions can be done using this [config](/docs/configurations#hoodiedatasourcewritepartitionstodelete).

Support for Huawei Cloud Object Storage, BAIDU AFS storage format, Baidu BOS storage in Hudi.

A [pre commit validator framework](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SparkPreCommitValidator.java)
has been added for spark engine, which can used for DeltaStreamer and Spark Datasource writers. Users can leverage this to add any validations to be executed before committing writes to Hudi.
Three validators come out-of-box
- [org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryEqualityPreCommitValidator.java) can be used to validate for equality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryInequalityPreCommitValidator.java) can be used to validate for inequality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQuerySingleResultPreCommitValidator.java) can be used to validate that a query on the table results in a specific value.

These can be configured by setting `hoodie.precommit.validators=<comma separated list of validator class names>`. Users can also provide their own implementations by extending the abstract class [SparkPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SparkPreCommitValidator.java)
and overriding this method

```java
void validateRecordsBeforeAndAfter(Dataset<Row> before, 
                                   Dataset<Row> after, 
                                   Set<String> partitionsAffected)
```


### Flink Integration Improvements

The Flink writer now supports propagation of CDC format for MOR table, by turning on the option `changelog.enabled=true`. Hudi would then persist all change flags of each record,
using the streaming reader of Flink, user can do stateful computation based on these change logs. Note that when the table is compacted with async compaction service, all the
intermediate changes are merged into one(last record), to only have UPSERT semantics.

Flink writing now also has most feature parity with spark writing, with addition of write operations like `bulk_insert`, `insert_overwrite`, support for non-partitioned tables,
automatic cleanup of uncommitted data, global indexing support, hive style partitioning and handling of partition path updates. Writing also supports a new log append mode, where
no records are de-duplicated and base files are directly written for each flush. To use this mode, set `write.insert.deduplicate=false`.

Flink readers now support streaming reads from COW/MOR tables. Deletions are emitted by default in streaming read mode, the downstream receives the DELETE message as a Hoodie record with empty payload.

Hive sync has been greatly improved by support different Hive versions(1.x, 2.x, 3.x). Hive sync can also now be done asynchronously.

Flink Streamer tool now supports transformers.

### DeltaStreamer

We have enhanced Deltastreamer utility with 3 new sources.

[JDBC source](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java) can take a extraction SQL statement and
incrementally fetch data out of sources supporting JDBC. This can be useful for e.g when reading data from RDBMS sources. Note that, this approach may need periodic re-bootstrapping to ensure data consistency, although being much simpler to operate over CDC based approaches.

[SQLSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/SqlSource.java) takes a Spark SQL statement to fetch data out of existing tables and
can be very useful for easy SQL based backfills use-cases e.g: backfilling just one column for the past N months.

[S3EventsHoodieIncrSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/S3EventsHoodieIncrSource.java) and [S3EventsSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/S3EventsSource.java)
assist in reading data from S3 reliably and efficiently ingesting that to Hudi. Existing approach using `*DFSSource` source classes uses last modification time of files as checkpoint to pull in new files.
But, if large number of files have the same modification time, this might miss some files to be read from the source.  These two sources (S3EventsHoodieIncrSource and S3EventsSource) together ensures data
is reliably ingested from S3 into Hudi by leveraging AWS SNS and SQS services that subscribes to file events from the source bucket. [This blog post](/blog/2021/08/23/s3-events-source) presents a model for
scalable, reliable incremental ingestion by using these two sources in tandem.

In addition to pulling events from kafka using regular offset format, we also added support for timestamp based fetches, that can
help with initial backfill/bootstrap scenarios. We have also added support for passing in basic auth credentials in schema registry provider url with schema provider.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350027)
