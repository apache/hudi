---
title: "Release 0.5.3"
sidebar_position: 6
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---
# [Release 0.5.3](https://github.com/apache/hudi/releases/tag/release-0.5.3) ([docs](/docs/quick-start-guide))

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


For releases older than these versions, please see [here](/releases/older-releases).