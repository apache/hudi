---
title: "Releases"
permalink: /releases
layout: releases
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

## [Release 0.5.1-incubating]

### Download Information
 * Source Release : [Apache Hudi(incubating) 0.5.1-incubating Source Release](https://www.apache.org/dist/incubator/hudi/0.5.1-incubating/hudi-0.5.1-incubating.src.tgz) ([asc](https://www.apache.org/dist/incubator/hudi/0.5.1-incubating/hudi-0.5.1-incubating.src.tgz.asc), [sha512](https://www.apache.org/dist/incubator/hudi/0.5.1-incubating/hudi-0.5.1-incubating.src.tgz.sha512))
 * Apache Hudi (incubating) jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

### Release Highlights
* Dependency Version Upgrades
    * Upgrade from Spark 2.1.0 to Spark 2.4.4
    * Upgrade from Avro 1.7.7 to Avro 1.8.2
    * Upgrade from Parquet 1.8.1 to Parquet 1.10.1
    * Upgrade from Kafka 0.8.2.1 to Kafka 0.20.0
* **IMPORTANT** This version requires your runtime spark version to be upgraded to 2.4+.
* Hudi now supports both Scala 2.11 and Scala 2.12, please refer to [Build with Scala 2.12](https://github.com/apache/incubator-hudi#build-with-scala-212) to build with Scala 2.12.
Also, the packages hudi-spark, hudi-utilities, hudi-spark-bundle and hudi-utilities-bundle are changed correspondingly to hudi-spark_{scala_version}, hudi-spark_{scala_version}, hudi-utilities_{scala_version}, hudi-spark-bundle_{scala_version} and hudi-utilities-bundle_{scala_version}.
Note that scala_version here is one of (2.11, 2.12).
* With 0.5.1, we added functionality to stop using renames for Hudi timeline metadata operations. This feature is automatically enabled for newly created Hudi tables. For existing tables, this feature is turned off by default. Please read this [section](deployment_link), before enabling this feature for existing hudi tables.
To enable the new hudi timeline layout which avoids renames, use the write config "hoodie.timeline.layout.version=1". Alternatively, you can use "repair overwrite-hoodie-props" to append the line "hoodie.timeline.layout.version=1" to hoodie.properties. Note that in any case, upgrade hudi readers (query engines) first with 0.5.1-incubating release before upgrading writer.
* CLI supports `repair overwrite-hoodie-props` to overwrite the table's hoodie.properties with specified file, for one-time updates to table name or even enabling the new timeline layout above. Note that few queries may temporarily fail while the overwrite happens (few milliseconds).
* DeltaStreamer CLI parameter for capturing table type is changed from --storage-type to --table-type. Refer to [wiki](https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture) with more latest terminologies.
* Configuration Value change for Kafka Reset Offset Strategies. Enum values are changed from LARGEST to LATEST, SMALLEST to EARLIEST for configuring Kafka reset offset strategies with configuration(auto.offset.reset) in deltastreamer.
* When using spark-shell to give a quick peek at Hudi, please provide `--packages org.apache.spark:spark-avro_2.11:2.4.4`, more details would refer to [latest quickstart docs](https://hudi.apache.org/docs/quick-start-guide.html)
* Key generator moved to separate package under org.apache.hudi.keygen. If you are using overridden key generator classes (configuration ("hoodie.datasource.write.keygenerator.class")) that comes with hudi package, please ensure the fully qualified class name is changed accordingly.
* Hive Sync tool will register RO tables for MOR with a _ro suffix, so query with _ro suffix. You would use `--skip-ro-suffix` in sync config in sync config to retain the old naming without the _ro suffix.
* With 0.5.1, hudi-hadoop-mr-bundle which is used by query engines such as presto and hive includes shaded avro package to support hudi real time queries through these engines. Hudi supports pluggable logic for merging of records. Users provide their own implementation of [HoodieRecordPayload](https://github.com/apache/incubator-hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java).
If you are using this feature, you need to relocate the avro dependencies in your custom record payload class to be consistent with internal hudi shading. You need to add the following relocation when shading the package containing the record payload implementation.

 ```xml
<relocation>
    <pattern>org.apache.avro.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.avro.</shadedPattern>
</relocation>
 ```

 * Better delete support in DeltaStreamer would refer to [latest quickstart docs](https://hudi.apache.org/docs/quick-start-guide.html)
 * Support for AWS Database Migration Service(DMS) in DeltaStreamer
 * Support for DynamicBloomFilter. This is turned off by default, to enable the DynamicBloomFilter, please use the index config "hoodie.bloom.index.filter.type=DYNAMIC_V0"

### Raw Release Notes
 The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346183)

## [Release 0.5.0-incubating](https://github.com/apache/incubator-hudi/releases/tag/release-0.5.0-incubating) ([docs](/docs/0.5.0-quick-start-guide.html))

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
