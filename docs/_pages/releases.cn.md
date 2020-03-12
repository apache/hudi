---
title: "Releases"
permalink: /cn/releases
layout: releases
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

## [Release 0.5.2-incubating](https://github.com/apache/incubator-hudi/releases/tag/release-0.5.2-incubating) ([docs](/docs/0.5.2-quick-start-guide.html))

### Download Information
 * Source Release : [Apache Hudi(incubating) 0.5.2-incubating Source Release](https://www.apache.org/dist/incubator/hudi/0.5.2-incubating/hudi-0.5.2-incubating.src.tgz) ([asc](https://www.apache.org/dist/incubator/hudi/0.5.2-incubating/hudi-0.5.2-incubating.src.tgz.asc), [sha512](https://www.apache.org/dist/incubator/hudi/0.5.2-incubating/hudi-0.5.2-incubating.src.tgz.sha512))
 * Apache Hudi (incubating) jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

### Migration Guide for this release
 * Write Client restructuring has moved classes around ([HUDI-554](https://issues.apache.org/jira/browse/HUDI-554)). Package `client` now has all the various client classes, that do the transaction management. `func` renamed to `execution` and some helpers moved to `client/utils`. All compaction code under `io` now under `table/compact`. Rollback code under `table/rollback` and in general all code for individual operations under `table`. This change only affects the apps/projects depending on hudi-client. Users of deltastreamer/datasource will not need to change anything.
 * Simplify `HoodieBloomIndex` without the need for 2GB limit handling. Prior to spark 2.4.0, each spark partition has a limit of 2GB. In Hudi 0.5.1, after we upgraded to spark 2.4.4, we don't have the limitation anymore. Hence removing the safe parallelism constraint we had in` HoodieBloomIndex`.

### Release Highlights
 * Support for overwriting the payload implementation in `hoodie.properties` via specifying the `hoodie.compaction.payload.class` config option. Previously, once the payload class is set once in `hoodie.properties`, it cannot be changed. In some cases, if a code refactor is done and the jar updated, one may need to pass the new payload class name.
 * `TimestampBasedKeyGenerator` supports for `CharSequence`  types. Previously `TimestampBasedKeyGenerator` only supports `Double`, `Long`, `Float` and `String` 4 data types for the partition key. Now, after data type extending, `CharSequence` has been supported in `TimestampBasedKeyGenerator`.
 * Hudi now supports incremental pulling from defined partitions via the `hoodie.datasource.read.incr.path.glob` [config option](https://github.com/apache/incubator-hudi/blob/master/hudi-spark/src/main/scala/org/apache/hudi/DataSourceOptions.scala#L111). For some use case that users only need to pull the incremental part of certain partitions, it can run faster by only loading relevant parquet files.
 * With 0.5.2, hudi allows partition path to be updated with `GLOBAL_BLOOM` index. Previously, when a record is to be updated with a new partition path, and when set to `GLOBAL_BLOOM` as index, hudi ignores the new partition path and update the record in the original partition path. Now, hudi allows records to be inserted into their new partition paths and delete the records in the old partition paths. A configuration (e.g. `hoodie.index.bloom.update.partition.path=true`) can be added to enable this feature.
 * A `JdbcbasedSchemaProvider` schema provider has been provided to get metadata through JDBC. For the use case that users want to synchronize data from MySQL, and at the same time, want to get the schema from the database, it's very helpful.
 * CLI related changes:
   - Allows users to specify option to print additional commit metadata, e.g. *Total Log Blocks*, *Total Rollback Blocks*, *Total Updated Records Compacted* and so on.
   - Supports `temp_query` and `temp_delete` to query and delete temp view. This command creates a temp table. Users can write HiveQL queries against the table to filter the desired row. For example,
```
temp_query --sql "select Instant, NumInserts, NumWrites from satishkotha_debug where FileId='ed33bd99-466f-4417-bd92-5d914fa58a8f' and Instant > '20200123211217' order by Instant"
```

### Raw Release Notes
  The raw release notes are available [here](https://issues.apache.org/jira/projects/HUDI/versions/12346606#release-report-tab-body)

## [Release 0.5.0-incubating](https://github.com/apache/incubator-hudi/releases/tag/release-0.5.0-incubating)

### Download Information
 * Source Release : [Apache Hudi(incubating) 0.5.0-incubating Source Release](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz) ([asc](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.asc), [sha512](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.sha512))
 * Apache Hudi (incubating) jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

### Release Highlights
 * Package and format renaming from com.uber.hoodie to org.apache.hudi (See migration guide section below)
 * Major redo of Hudi bundles to address class and jar version mismatches in different environments
 * Upgrade from Hive 1.x to Hive 2.x for compile time dependencies - Hive 1.x runtime integration still works with a patch : See [the discussion thread](https://lists.apache.org/thread.html/48b3f0553f47c576fd7072f56bb0d8a24fb47d4003880d179c7f88a3@%3Cdev.hudi.apache.org%3E)
 * DeltaStreamer now supports continuous running mode with managed concurrent compaction
 * Support for Composite Keys as record key
 * HoodieCombinedInputFormat to scale huge hive queries running on Hoodie tables

### Migration Guide for this release
 This is the first Apache release for Hudi (incubating). Prior to this release, Hudi Jars were published using "com.uber.hoodie" maven co-ordinates. We have a [migration guide](https://cwiki.apache.org/confluence/display/HUDI/Migration+Guide+From+com.uber.hoodie+to+org.apache.hudi)

### Raw Release Notes
 The raw release notes are available [here](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346087)

## [Release 0.4.7](https://github.com/apache/incubator-hudi/releases/tag/hoodie-0.4.7)

### Release Highlights

 * Major releases with fundamental changes to filesystem listing & write failure handling
 * Introduced the first version of HoodieTimelineServer that runs embedded on the driver
 * With all executors fetching filesystem listing via RPC to timeline server, drastically reduced filesystem listing!
 * Failing concurrent write tasks are now handled differently to be robust against spark stage retries
 * Bug fixes/clean up around indexing, compaction

### PR LIST

- Skip Meta folder when looking for partitions. [#698](https://github.com/apache/incubator-hudi/pull/698)
- HUDI-134 - Disable inline compaction for Hoodie Demo. [#696](https://github.com/apache/incubator-hudi/pull/696)
- Default implementation for HBase index qps allocator. [#685](https://github.com/apache/incubator-hudi/pull/685)
- Handle duplicate record keys across partitions. [#687](https://github.com/apache/incubator-hudi/pull/687)
- Fix up offsets not available on leader exception. [#650](https://github.com/apache/incubator-hudi/pull/650)
