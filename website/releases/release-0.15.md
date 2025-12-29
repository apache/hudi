---
title: "Release 0.15"
layout: releases
toc: true
last_modified_at: 2024-05-02T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Version 0.15 Release Notes

This page contains release notes for all Apache Hudi 0.15.x releases, including:
- [Release 0.15.0](#release-0150)
---

## [Release 0.15.0](https://github.com/apache/hudi/releases/tag/release-0.15.0) {#release-0150}

Apache Hudi 0.15.0 release brings enhanced engine integration, new features, and improvements in several areas. These
include Spark 3.5 and Scala 2.13 support, Flink 1.18 support, better Trino Hudi native connector support with newly
introduced Hadoop-agnostic storage and I/O abstractions. We encourage users to review
the [Release Highlights](#release-highlights) and [Migration Guide](#migration-guide-overview) down below on
relevant [module and API changes](#module-and-api-changes) and
[behavior changes](#behavior-changes) before using the 0.15.0 release.

## Migration Guide

This release keeps the same table version (`6`) as [0.14.0 release](/releases/release-0.14#release-0140), and there is no need for
a table version upgrade if you are upgrading from 0.14.0. There are a
few [module and API changes](#module-and-api-changes)
and [behavior changes](#behavior-changes) as
described below, and users are expected to take action accordingly before using 0.15.0 release.

:::caution
If migrating from an older release (pre-0.14.0), please also check the upgrade instructions from each older release in
sequence.
:::

### Bundle Updates

#### New Spark Bundles

We have expanded Hudi support to Spark 3.5 with two new bundles:

- Spark 3.5 and Scala
  2.12: [hudi-spark3.5-bundle_2.12](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.5-bundle_2.12)
- Spark 3.5 and Scala
  2.13: [hudi-spark3.5-bundle_2.13](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.5-bundle_2.13)

#### New Utilities Bundles for Scala 2.13

Besides adding a bundle for Spark 3.5 and Scala 2.13, we have added new utilities bundles to use with Scala
2.13, [hudi-utilities-bundle_2.13](https://mvnrepository.com/artifact/org.apache.hudi/hudi-utilities-bundle_2.13)
and [hudi-utilities-slim-bundle_2.13](https://mvnrepository.com/artifact/org.apache.hudi/hudi-utilities-slim-bundle_2.13).

#### New and Deprecated Flink Bundles

We have expanded Hudi support to Flink 1.18 with a new
bundle, [hudi-flink1.18-bundle](https://mvnrepository.com/artifact/org.apache.hudi/hudi-flink1.18-bundle). This release
removes Hudi support on Flink 1.13.

### Module and API Changes

#### Hudi Storage and I/O Abstractions

This release introduces new storage and I/O abstractions that are Hadoop-agnostic to improve integration with query
engines, including [Trino](https://trino.io/), which uses its own native File System APIs. Core Hudi classes
including [`HoodieTableMetaClient`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableMetaClient.java),
[`HoodieBaseFile`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieBaseFile.java),
[`HoodieLogFile`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieLogFile.java),
[`HoodieEngineContext`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/common/engine/HoodieEngineContext.java),
etc., now depend on new
storage and I/O classes. If you’re using these classes directly in your applications, you need to change your
integration code and usage. For more details, check out [this section](#hudi-storage-and-io-abstractions-1).

#### Module Changes

As part of introducing new storage and I/O abstractions and making core reader logic Hadoop-agnostic, this release
restructures the Hudi modules to clearly reflect the layering. Specifically,

- [`hudi-io` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io) is added
  for I/O related functionality, and the Hudi-native HFile reader implementation sits inside this new module;
- [`hudi-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common)
  contains the core
  implementation of the [Apache Hudi Technical Specification](https://hudi.apache.org/tech-specs) and is now
  Hadoop-independent;
- [`hudi-hadoop-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-hadoop-common)
  contains
  implementation based on Hadoop file system APIs to be used
  with [`hudi-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common)
  on engines
  including Spark,
  Flink, Hive, and Presto.

If you
use [`hudi-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common) as
the dependency before and Hadoop file system APIs and implementations, you should include all three
modules, [`hudi-io`](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io), [`hudi-common`](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common),
and [`hudi-hadoop-common`](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-hadoop-common),
as the dependency now. Note that, Presto and Trino will be released based on Hudi 0.15.0 release with such changes.

#### Lock Provider API Change

The [`LockProvider`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/LockManager.java#L125)
instantiation now expects
the [`StorageConfiguration`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/StorageConfiguration.java)
instance as the second argument of the constructor. If you
extend [`LockProvider`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/common/lock/LockProvider.java)
to implement a custom lock provider
before, you need to change the constructor to match the aforementioned constructor signature. Here's an example:

```java
public class XYZLockProvider implements LockProvider<String>, Serializable {
  ...

    public XYZLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
      ...
    }
  
  ...
}
```

### Behavior Changes

#### Improving Cleaning Table Service

We have improved the default cleaner behavior to only schedule a new cleaner plan if there is no inflight plan, by
flipping the default of [`hoodie.clean.allow.multiple`](/docs/configurations#hoodiecleanallowmultiple) from `true`
to `false`. This simplifies cleaning table service when the metadata table is enabled. The config is deprecated now and
will be removed after the next release.

#### Allowing Duplicates on Inserts

We now allow duplicate keys on `INSERT` operation by default, even if inserts are routed to merge with an existing
file (for ensuring file sizing), by flipping the default
of [`hoodie.merge.allow.duplicate.on.inserts`](/docs/configurations#hoodiemergeallowduplicateoninserts) from `false`
to `true`. This is only relevant for `INSERT` operation, since `UPSERT`, and `DELETE` operations always ensure unique
key constraint.

#### Sync MOR Snapshot to The Metastore

To better support snapshot queries on MOR tables on OLAP engines, the MOR snapshot or RT is synced to the metastore with
the table name by default, by flipping the default
of [`hoodie.meta.sync.sync_snapshot_with_table_name`](/docs/configurations#hoodiemetasyncsync_snapshot_with_table_name)
from `false`
to `true`.

#### Flink Option Default Flips

The default value of [`read.streaming.skip_clustering`](/docs/configurations#readstreamingskip_clustering) is `false`
before this release, which could cause the situation that Flink streaming reading reads the replaced file slices of
clustering and duplicated data (same concern
for [`read.streaming.skip_compaction`](/docs/configurations#readstreamingskip_compaction)). The
0.15.0 release makes Flink streaming read to skip clustering and compaction instants for all cases to avoid reading the
relevant file slices, by flipping the default
of [`read.streaming.skip_clustering`](/docs/configurations#readstreamingskip_clustering)
and [`read.streaming.skip_compaction`](/docs/configurations#readstreamingskip_compaction)
from `false` to `true`.

## Release Highlights

### Hudi Storage and I/O Abstractions

To provide better integration experience with query engines including [Trino](https://trino.io/) which uses its own
native File System APIs, this release introduces new storage and I/O abstractions that are Hadoop-agnostic.

To be specific, the release introduces Hudi storage
abstraction [`HoodieStorage`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/HoodieStorage.java)
which provides all I/O APIs to read and write files and directories on storage, such as `open`, `read`, etc. This class
can be extended to implement storage layer optimizations like caching, federated storage layout, hot/cold storage
separation, etc. This class needs to be implemented based on particular systems, such as
Hadoop’s [`FileSystem`](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) and
Trino’s [`TrinoFileSystem`](https://github.com/trinodb/trino/blob/450/lib/trino-filesystem/src/main/java/io/trino/filesystem/TrinoFileSystem.java).
Core classes are introduced for accessing file systems:

- [`StoragePath`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/StoragePath.java):
  represents a path of a file or directory on storage, which replaces
  Hadoop's [`Path`](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/Path.html).
- [`StoragePathInfo`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/StoragePathInfo.java):
  keeps the path, length, isDirectory, modification time, and other information which are used by Hudi, which replaces
  Hadoop's [`FileStatus`](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileStatus.html).
- [`StorageConfiguration`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/StorageConfiguration.java):
  provides the storage configuration by wrapping the particular configuration class object used by the corresponding
  file system.

The [`HoodieIOFactory`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieIOFactory.java)
abstraction is introduced to provide APIs to create readers and writers for I/O without depending on Hadoop classes.

By using the new storage and I/O abstractions, we make
the [`hudi-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common) and
core reader logic in Hudi
Hadoop-independent in this release. We have introduced a
new [`hudi-hadoop-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-hadoop-common)
which contains
the implementation
of [`HoodieStorage`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/HoodieStorage.java)
and [`HoodieIOFactory`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieIOFactory.java)
based on Hadoop’s file system APIs and implementation, and
existing reader and writer logic that depends on Hadoop-dependent APIs.
The [`hudi-hadoop-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-hadoop-common)
is used by
Spark, Flink, Hive, and Presto integration where the logic remains unchanged.

For the engine that is independent of Hadoop, the integration should use
the [`hudi-common` module](https://github.com/apache/hudi/tree/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common) and
plug in its own
implementation
of [`HoodieStorage`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/storage/HoodieStorage.java)
and [`HoodieIOFactory`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieIOFactory.java)
by setting the new
configs [`hoodie.storage.class`](/docs/configurations#hoodiestorageclass)
and [`hoodie.io.factory.class`](/docs/configurations#hoodieiofactoryclass) through the storage configuration.

### Engine Support

#### Spark 3.5 and Scala 2.13 Support

This release has added the Spark 3.5 support and Scala 2.13 support; users who are on Spark 3.5 can use the new Spark
bundle based on the Scala
version: [hudi-spark3.5-bundle_2.12](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.5-bundle_2.12) and
[hudi-spark3.5-bundle_2.13](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.5-bundle_2.13). Spark 3.4,
3.3, 3.2, 3.1, 3.0 and 2.4 continue to be supported in this release. To quickly get started with Hudi and Spark 3.5, you
can explore our [quick start guide](/docs/quick-start-guide).

#### Flink 1.18 Support

This release has added the Flink 1.18 support with a new compile maven profile `flink1.18` and new Flink
bundle [hudi-flink1.18-bundle](https://mvnrepository.com/artifact/org.apache.hudi/hudi-flink1.18-bundle).

### Hudi-Native HFile Reader

Hudi uses [HFile format](https://hbase.apache.org/book.html#_hfile_format_2) as the base file format for storing various
metadata, e.g., file listing, column stats, and bloom filters, in the metadata table (MDT), as HFile format is optimized
for range scans and point lookups.  [HFile format](https://hbase.apache.org/book.html#_hfile_format_2) is originally
designed and implemented by [HBase](https://hbase.apache.org/). To avoid HBase dependency conflict and make engine
integration easy with Hadoop-independent implementation, we have implemented a
new [HFile reader in Java](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/src/main/java/org/apache/hudi/io/hfile/HFileReaderImpl.java)
which is independent of HBase or Hadoop dependencies. This HFile reader is backwards compatible with existing Hudi
releases and storage format.

We have also written
a [HFile Format Specification](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-io/hfile_format.md),
that defines
the HFile Format required by Hudi. This makes HFile reader and writer implementation possible in any language, for
example, C++ or Rust, by following this Spec.

### New Features in Hudi Utilities

#### StreamContext and SourceProfile Interfaces

For the Hudi streamer, we have introduced the
new [`StreamContext`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/StreamContext.java)
and [`SourceProfile`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/SourceProfile.java)
Interfaces. These are
designed to contain details about how the data should be consumed from the source and written (e.g., parallelism) in the
next sync round in StreamSync. This allows users to control the behavior and performance of source reading and data
writing to the target Hudi table.

#### Enhanced Proto Kafka Source Support

We have added the support for deserializing with the Confluent proto deserializer, through a new
config [`hoodie.streamer.source.kafka.proto.value.deserializer.class`](/docs/configurations#hoodiestreamersourcekafkaprotovaluedeserializerclass)
to specify the Kafka Proto payload deserializer
class.

#### Ignoring Checkpoint in Hudi Streamer

Hudi streamer has a new option, `--ignore-checkpoint`, to ignore the last committed checkpoint for the source. This
options should be set with a unique value, a timestamp value or UUID as recommended. Setting this config indicates that
the subsequent sync should ignore the last committed checkpoint for the source. The config value is stored in the commit
history, so setting the config with same values would not have any affect. This config can be used in scenarios like
kafka topic change where we would want to start ingesting from the latest or earliest offset after switching the topic
(in this case we would want to ignore the previously committed checkpoint, and rely on other configs to pick the
starting offset).

### Meta Sync Improvements

#### Paralleled Listing in Glue Catalog Sync

AWS Glue Catalog sync now supports paralleled listing of partitions to improve the listing performance and reduce meta
sync latency. Three new configs are added to control the listing parallelism:

- [`hoodie.datasource.meta.sync.glue.all_partitions_read_parallelism`](/docs/configurations#hoodiedatasourcemetasyncglueall_partitions_read_parallelism):
  parallelism for listing all partitions (first time sync).
- [`hoodie.datasource.meta.sync.glue.changed_partitions_read_parallelism`](/docs/configurations#hoodiedatasourcemetasyncgluechanged_partitions_read_parallelism):
  parallelism for listing changed partitions (second and subsequent syncs).
- [`hoodie.datasource.meta.sync.glue.partition_change_parallelism`](/docs/configurations#hoodiedatasourcemetasyncgluepartition_change_parallelism):
  parallelism for change operations such as create, update, and delete.

#### BigQuery Sync Optimization with Metadata Table

The BigQuery Sync now loads all partitions from the metadata table once if the metadata table is enabled, to improve the
file listing performance.

### Metrics Reporting to M3

A new MetricsReporter
implementation [`M3MetricsReporter`](https://github.com/apache/hudi/blob/38832854be37cb78ad1edd87f515f01ca5ea6a8a/hudi-common/src/main/java/org/apache/hudi/metrics/m3/M3MetricsReporter.java)
is added to support emitting metrics to [M3](https://m3db.io/). Users can now enable reporting metrics to M3 by
setting [`hoodie.metrics.reporter.type`](/docs/basic_configurations#hoodiemetricsreportertype) as `M3` and their
corresponding
host address and port in [`hoodie.metrics.m3.host`](/docs/basic_configurations#hoodiemetricsreportertype)
and [`hoodie.metrics.m3.port`](/docs/basic_configurations#hoodiemetricsm3port).

### Other Features and Improvements

#### Schema Exception Classification

This release introduces the classification of schema-related
exceptions ([HUDI-7486](https://issues.apache.org/jira/browse/HUDI-7486)) to make it easy for users to
understand the root cause, including the errors during converting the records from Avro to Spark Row due to illegal
schema, or the records are incompatible with the provided schema.

#### Record Size Estimation Improvement

The record size estimation in Hudi is improved by considering replace commits and delta commits additionally
([HUDI-7429](https://issues.apache.org/jira/browse/HUDI-7429)).

#### Using `s3` Scheme for Athena

Recent Athena version silently drops Hudi data when the partition location has a `s3a` scheme. Recreating the table with
partition `s3` scheme fixes the issue. We have added a fix to use `s3` scheme for the Hudi table partitions in AWS Glue
Catalog sync ([HUDI-7362](https://issues.apache.org/jira/browse/HUDI-7362)).

## Known Regressions
The Hudi 0.15.0 release introduces a regression related to Complex Key generation when the record key consists of a
single field. This issue was also present in version 0.14.1. When upgrading a table from previous versions,
it may silently ingest duplicate records.

:::tip
Avoid upgrading any existing table to 0.14.1 and 0.15.0 from any prior version if you are using ComplexKeyGenerator with single field as record key and multiple partition fields.
:::

## Raw Release Notes

The raw release notes are
available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12353381).