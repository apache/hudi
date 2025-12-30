---
title: "Release 1.0"
layout: releases
toc: true
last_modified_at: 2024-05-02T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page contains release notes for all Apache Hudi 1.0.x releases, including:

- [Release 1.0.2](#release-102)
- [Release 1.0.1](#release-101)
- [Release 1.0.0](#release-100)
- [Release 1.0.0-beta2](#release-100-beta2)
- [Release 1.0.0-beta1](#release-100-beta1)

---

## [Release 1.0.2](https://github.com/apache/hudi/releases/tag/release-1.0.2) {#release-102}

## Migration Guide

* This release (1.0.2) does not introduce any new table version, thus no migration is needed if you are on 1.0.1.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0),
  [0.14.0](/releases/release-0.14#release-0140) and [1.0.0](/releases/release-1.0#release-100)

### Bug fixes

The 1.0.2 release primarily focuses on bug fixes, stability enhancements, and critical improvements, particularly around migration and backwards compatibility. The changes span across various components, including:

* **Metadata Table (MDT):** Numerous fixes and improvements related to validation, writing, reading, compaction, indexing (column stats), and backwards compatibility (especially for table version 6).
* **Spark Integration:** Enhancements and fixes for Spark SQL (MERGE INTO, query behavior), datasource reader/writer, schema handling, performance, and backward compatibility.
* **Backwards Compatibility:** Significant effort ensuring compatibility with older table versions (specifically v6) and smoother upgrades from 0.x versions, including dedicated writers/readers.
* **File Group Reader:** Validation, fixes, and feature completeness improvements, including making it default for table version 6.
* **Flink Engine:** Fixes and improvements related to streamer checkpoints and bundle validation.
* **Compaction and Table Services:** Fixes related to compaction scheduling, execution (especially with global index or RLI), archival, and cleanup.
* **Indexing:** Fixes and enhancements for Column Stats, Record Level Index (RLI), and Bloom Filters.
* **Performance:** Optimizations in areas like log file writing, schema reuse, and metadata initialization.
* **Testing, CI, and Dependencies:** Fixes for flaky tests, improved code coverage, bundle validation, dependency cleanup (HBase removal), and extensive release testing.

## Known Regressions
We have a ComplexKeyGenerator related regression reported [here](release-0.14#known-regressions). Please refrain from migrating, if you have single field as record key and mutiple partition fields.

:::tip
Avoid upgrading any existing table to 1.0.2 if you are using ComplexKeyGenerator with single record key configured.
:::

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12355558)

:::tip
1.0.2 release also contains all the new features and bug fixes from 1.0.1, of which the release notes are [here](/releases/release-1.0#release-101)
:::

---

## [Release 1.0.1](https://github.com/apache/hudi/releases/tag/release-1.0.1) {#release-101}

## Migration Guide

* This release (1.0.1) does not introduce any new table version, thus no migration is needed if you are on 1.0.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0),
  [0.14.0](/releases/release-0.14#release-0140) and [1.0.0](/releases/release-1.0#release-100)

### Bug fixes

1.0.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including

* Hudi Streamer
* Spark SQL
* Spark datasource writer
* Table services
* Backwards compatible writer
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions
We have a ComplexKeyGenerator related regression reported [here](release-0.14#known-regressions). Please refrain from migrating, if you have single field as record key and multiple partition fields.

:::tip
Avoid upgrading any existing table to 1.0.1 if you are using ComplexKeyGenerator with single record key configured.
:::

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12355195)

:::tip
1.0.1 release also contains all the new features and bug fixes from 1.0.0, of which the release notes are [here](/releases/release-1.0#release-100)
:::

---

## [Release 1.0.0](https://github.com/apache/hudi/releases/tag/release-1.0.0) {#release-100}

Apache Hudi 1.0.0 is a major milestone release of Apache Hudi. This release contains significant format changes and new exciting features
as we will see below.

## Migration Guide

We encourage users to try the **1.0.0** features on new tables first. The 1.0 general availability (GA) release will
support automatic table upgrades from 0.x versions while also ensuring full backward compatibility when reading 0.x
Hudi tables using 1.0, ensuring a seamless migration experience.

This release comes with **backward compatible writes** i.e. 1.0.0 can write in both the table version 8 (latest) and older
table version 6 (corresponds to 0.14 & above) formats. Automatic upgrades for tables from 0.x versions are fully
supported, minimizing migration challenges. Until all the readers are upgraded, users can still deploy 1.0.0 binaries
for the writers and leverage backward compatible writes to continue writing the tables in the older format. Once the readers
are fully upgraded, users can switch to the latest format through a config change. We recommend users to follow the upgrade
steps mentioned in the [migration guide](/docs/deployment#upgrading-to-100) to ensure a smooth transition.

:::caution
Most things are seamlessly handled by the auto upgrade process, but there are some limitations. Please read through the
limitations of the upgrade downgrade process before proceeding to migrate. Please check the [migration guide](/docs/deployment#upgrading-to-100)
and [RFC-78](https://github.com/apache/hudi/blob/master/rfc/rfc-78/rfc-78.md#support-matrix-for-different-readers-and-writers) for more details.
:::

## Bundle Updates

- Same bundles supported in the [0.15.0 release](release-0.15#new-spark-bundles) are still supported.
- New Flink Bundles to support Flink 1.19 and Flink 1.20.
- In this release, we have deprecated support for Spark 3.2 or lower version in Spark 3.

## Highlights

### Format changes

The main epic covering all the format changes is [this GitHub issue](https://github.com/apache/hudi/issues/15964), which is also covered in the [Hudi 1.0 tech specification](/learn/tech-specs-1point0). The following are the main highlights with respect to format changes:

#### Timeline

- The active and archived timeline dichotomy has been done away with a more scalable LSM tree based
  timeline. The timeline layout is now more organized and efficient for time-range queries and scaling to infinite history.
- As a result, timeline layout has been changed, and it has been moved to `.hoodie/timeline` directory under the base
  path of the table.
- There are changes to the timeline instant files as well:
    - All commit metadata is serialized to Avro, allowing for future compatibility and uniformity in metadata across all
      actions.
    - Instant files for completed actions now include a completion time.
    - Action for the pending clustering instant is now renamed to `clustering` to make it distinct from other
      `replacecommit` actions.

#### Log File Format

- In addition to the keys in the log file header, we also store record positions. Refer to the
  latest [spec](/learn/tech-specs-1point0#log-format) for more details. This allows us to do position-based merging (apart
  from key-based merging) and skip pages based on positions.
- Log file name will now have the deltacommit instant time instead of base commit instant time.
- The new log file format also enables fast partial updates with low storage overhead.

### Compatibility with Old Formats

- **Backward Compatible writes:** Hudi 1.0 writes now support writing in both the table version 8 (latest) and older table version 6 (corresponds to 0.14 & above) formats, ensuring seamless
  integration with existing setups.
- **Automatic upgrades**: for tables from 0.x versions are fully supported, minimizing migration challenges. We also recommend users first try migrating to 0.14 &
  above, if you have advanced setups with multiple readers/writers/table services.

### Concurrency Control

1.0.0 introduces **Non-Blocking Concurrency Control (NBCC)**, enabling multi-stream concurrent ingestion without
conflict. This is a general-purpose concurrency model aimed at the stream processing or high-contention/frequent writing
scenarios. In contrast to Optimistic Concurrency Control, where writers abort the transaction if there is a hint of
contention, this innovation allows multiple streaming writes to the same Hudi table without any overhead of conflict
resolution, while keeping the semantics of event-time ordering found in streaming systems, along with asynchronous table
service such as compaction, archiving and cleaning.

To learn more about NBCC, refer to [this blog](/blog/2024/12/06/non-blocking-concurrency-control) which also includes a demo with Flink writers.

### New Indices

1.0.0 introduces new indices to the multi-modal indexing subsystem of Apache Hudi. These indices are designed to improve
query performance through partition pruning and further data skipping.

#### Secondary Index

The **secondary index** allows users to create indexes on columns that are not part of record key columns in Hudi
tables. It can be used to speed up queries with predicates on columns other than record key columns.

#### Partition Stats Index

The **partition stats index** aggregates statistics at the partition level for the columns for which it is enabled. This
helps in efficient partition pruning even for non-partition fields.

#### Expression Index

The **expression index** enables efficient queries on columns derived from expressions. It can collect stats on columns
derived from expressions without materializing them, and can be used to speed up queries with filters containing such
expressions.

To learn more about these indices, refer to the [SQL queries](/docs/sql_queries#snapshot-query-with-index-acceleration) docs.

### Partial Updates

1.0.0 extends support for partial updates to Merge-on-Read tables, which allows users to update only a subset of columns
in a record. This feature is useful when users want to update only a few columns in a record without rewriting the
entire record.

To learn more about partial updates, refer to the [SQL DML](/docs/sql_dml#merge-into-partial-update) docs.

### Multiple Base File Formats in a single table

- Support for multiple base file formats (e.g., **Parquet**, **ORC**, **HFile**) within a single Hudi table, allowing
  tailored formats for specific use cases like indexing and ML applications.
- It is also useful when users want to switch from one file
  format to another, e.g. from ORC to Parquet, without rewriting the whole table.
- **Configuration:** Enable with `hoodie.table.multiple.base.file.formats.enable`.

To learn more about the format changes, refer to the [Hudi 1.0 tech specification](/learn/tech-specs-1point0).

### API Changes

1.0.0 introduces several API changes, including:

#### Record Merger API

`HoodieRecordPayload` interface is deprecated in favor of the new `HoodieRecordMerger` interface. Record merger is a
generic interface that allows users to define custom logic for merging base file and log file records. This release
comes with a few out-of-the-box merge modes, which define how the base and log files are ordered in a file slice and
further how different records with the same record key within that file slice are merged consistently to produce the
same deterministic results for snapshot queries, writers and table services. Specifically, there are three merge modes
supported as a table-level configuration:

- `COMMIT_TIME_ORDERING`: Merging simply picks the record belonging to the latest write (commit time) as the merged
  result.
- `EVENT_TIME_ORDERING`: Merging picks the record with the highest value on a user specified ordering or precombine
  field as the merged result.
- `CUSTOM`: Users can provide custom merger implementation to have better control over the merge logic.

:::note
Going forward, we recommend users to migrate and use the record merger APIs and not write new payload implementations.
:::

#### Positional Merging with Filegroup Reader

- **Position-Based Merging:** Offers an alternative to key-based merging, allowing for page skipping based on record
  positions. Enabled by default for Spark and Hive.
- **Configuration:** Activate positional merging using `hoodie.merge.use.record.positions=true`.

The new reader has shown impressive performance gains for **partial updates** with key-based merging. For a MOR table of
size 1TB with 100 partitions and 80% random updates in subsequent commits, the new reader is **5.7x faster** for
snapshot queries with **70x reduced write amplification**.

### Flink Enhancements

- **Lookup Joins:** Flink now supports lookup joins, enabling table enrichment with external data sources.
- **Partition Stats Index Support:** As mentioned above, partition stats support is now available for Flink, bringing
  efficient partition pruning to streaming workloads.
- **Non-Blocking Concurrency Control:** NBCC is now available for Flink streaming writers, allowing for multi-stream
  concurrent ingestion without conflict.

## Call to Action

The 1.0.0 GA release is the culmination of extensive development, testing, and feedback. We invite you to upgrade and
experience the new features and enhancements.

## Known Regressions
- We discovered a regression in Hudi 1.0.0 release for backwards compatible writer for MOR table.
  It can silently deletes committed data after upgrade when new data is ingested to the table.
- We also have a ComplexKeyGenerator related regression reported [here](release-0.14#known-regressions). Please refrain from migrating, if you have single field as record key and multiple fields as partition fields.

:::tip
Avoid upgrading any existing table to 1.0.0 if any of the above scenario matches your workload. Incase of backwards compatible writer for MOR table, you are good to upgrade to 1.0.2 release.
:::

---

## [Release 1.0.0-beta2](https://github.com/apache/hudi/releases/tag/release-1.0.0-beta2) {#release-100-beta2}

Apache Hudi 1.0.0-beta2 is the second beta release of Apache Hudi. This release is meant for early adopters to try
out the new features and provide feedback. The release is not meant for production use.

## Migration Guide

This release contains major format changes as we will see in highlights below. We encourage users to try out the
**1.0.0-beta2** features on new tables. The 1.0 general availability (GA) release will support automatic table upgrades
from 0.x versions, while also ensuring full backward compatibility when reading 0.x Hudi tables using 1.0, ensuring a
seamless migration experience.

:::caution
Given that timeline format and log file format has changed in this **beta release**, it is recommended not to attempt to do
rolling upgrades from older versions to this release.
:::

## Highlights

### Format changes

[This GitHub issue](https://github.com/apache/hudi/issues/15964) is the main epic covering all the format changes proposals,
which are also partly covered in the [Hudi 1.0 tech specification](/learn/tech-specs-1point0). The following are the main
changes in this release:

#### Timeline

No major changes in this release. Refer to [1.0.0-beta1#timeline](/releases/release-1.0#timeline-2) for more details.

#### Log File Format

In addition to the fields in the log file header added in [1.0.0-beta1](/releases/release-1.0#log-file-format-2), we also
store a flag, `IS_PARTIAL` to indicate whether the log block contains partial updates or not.

### Metadata indexes

In 1.0.0-beta1, we added support for functional index. In 1.0.0-beta2, we have added support for secondary indexes and
partition stats index to the [multi-modal indexing](/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi) subsystem.

#### Secondary Index

Secondary indexes allow users to create indexes on columns that are not part of record key columns in Hudi tables (for
record key fields, Hudi supports [Record-level Index](/blog/2023/11/01/record-level-index). Secondary indexes can be used to speed up
queries with predicate on columns other than record key columns.

#### Partition Stats Index

Partition stats index aggregates statistics at the partition level for the columns for which it is enabled. This helps
in efficient partition pruning even for non-partition fields.

To try out these features, refer to the [SQL guide](/docs/next/sql_ddl#create-partition-stats-index).

### API Changes

#### Positional Merging with Filegroup Reader

In 1.0.0-beta1, we added a new [filegroup reader](/releases/release-1.0#new-filegroup-reader), which provides
5.7x performance benefits for snapshot queries on Merge-on-Read tables with updates. The reader now
provides position-based merging, as an alternative to existing key-based merging, and skipping pages based on record
positions. The new filegroup reader is integrated with Spark and Hive, and enabled by default. To enable positional
merging set below configs:

```properties
hoodie.merge.use.record.positions=true
```

### Hudi-Flink Enhancements

This release comes with the support for [lookup joins](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/joins/#lookup-join).
A lookup join is typically used to enrich a table with data that is queried from an external system. The join requires
one table to have a processing time attribute and the other table to be backed by a lookup source connector. Head over
to the [FLink SQL guide](/docs/next/sql_dml#lookup-joins) to try out this feature.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12354810).

---

## [Release 1.0.0-beta1](https://github.com/apache/hudi/releases/tag/release-1.0.0-beta1) {#release-100-beta1}

Apache Hudi 1.0.0-beta1 is the first beta release of Apache Hudi. This release is meant for early adopters to try
out the new features and provide feedback. The release is not meant for production use.

## Migration Guide

This release contains major format changes as we will see in highlights below. We encourage users to try out the
**1.0.0-beta1** features on new tables. The 1.0 general availability (GA) release will support automatic table upgrades
from 0.x versions, while also ensuring full backward compatibility when reading 0.x Hudi tables using 1.0, ensuring a
seamless migration experience.


:::caution
Given that timeline format and log file format has changed in this **beta release**, it is recommended not to attempt to do
rolling upgrades from older versions to this release.
:::

## Highlights

### Format changes

[This GitHub issue](https://github.com/apache/hudi/issues/15964) is the main epic covering all the format changes proposals,
which are also partly covered in the [Hudi 1.0 tech specification](/learn/tech-specs-1point0). The following are the main
changes in this release:

#### Timeline

- Now all commit metadata is serialized to avro. This allows us to add new fields in the future without breaking
  compatibility and also maintain uniformity in metadata across all actions.
- All completed commit metadata file name will also have completion time. All the actions in requested/inflight states
  are stored in the active timeline as files named \<begin_instant_time>.\<action_type>.\<requested|inflight>. Completed
  actions are stored along with a time that denotes when the action was completed, in a file named <
  begin_instant_time>_\<completion_instant_time>.\<action_type>. This allows us to implement file slicing for non-blocking
  concurrecy control.
- Completed actions, their plans and completion metadata are stored in a more
  scalable [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) based timeline organized in an *
  *_archived_** storage location under the .hoodie metadata path. It consists of Apache Parquet files with action
  instant data and bookkeeping metadata files, in the following manner. Checkout [timeline](/docs/next/timeline#lsm-timeline-history) docs for more details.

#### Log File Format

- In addition to the fields in the log file header, we also store record positions. Refer to the
  latest [spec](/learn/tech-specs-1point0#log-format) for more details. This allows us to do
  position-based merging (apart from key-based merging) and skip pages based on positions.
- Log file name will now have the deltacommit instant time instead of base commit instant time.

#### Multiple base file formats

Now you can have multiple base files formats in a Hudi table. Even the same filegroup can have multiple base file
formats. We need to set a table config `hoodie.table.multiple.base.file.formats.enable` to use this feature. And
whenever we need to change the format, then just specify the format in the `hoodie.base.file.format"` config. Currently,
only Parquet, Orc and HFile formats are supported. This unlocks multiple benefits including choosing file format
suitable to index, and supporting emerging formats for ML/AI.

### Concurrency Control

A new concurrency control mode called `NON_BLOCKING_CONCURRENCY_CONTROL` is introduced in this release, where unlike
OCC, multiple writers can operate on the table with non-blocking conflict resolution. The writers can write into the
same file group with the conflicts resolved automatically by the query reader and the compactor. The new concurrency
mode is currently available for preview in version 1.0.0-beta only. You can read more about it under
section [Model C: Multi-writer](/docs/next/concurrency_control#non-blocking-concurrency-control). A complete example with multiple
Flink streaming writers is available [here](/docs/next/sql_dml#non-blocking-concurrency-control-experimental). You
can follow the [RFC](https://github.com/apache/hudi/blob/master/rfc/rfc-66/rfc-66.md) and
the [JIRA](https://issues.apache.org/jira/browse/HUDI-6640) for more details.

### Functional Index

A [functional index](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/rfc/rfc-63/rfc-63.md)
is an index on a function of a column. It is a new addition to Hudi's [multi-modal indexing](https://hudi.apache.org/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi)
subsystem which provides faster access method and also absorbs partitioning as part of the indexing system. Now you can
simply create and drop index using SQL syntax as follows:

```sql
-- Create Index
CREATE INDEX [IF NOT EXISTS] index_name ON [TABLE] table_name 
[USING index_type] 
(column_name1 [OPTIONS(key1=value1, key2=value2, ...)], column_name2 [OPTIONS(key1=value1, key2=value2, ...)], ...) 
[OPTIONS (key1=value1, key2=value2, ...)]

-- Drop Index
DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
```

- `index_name` is the name of the index to be created or dropped.
- `table_name` is the name of the table on which the index is created or dropped.
- `index_type` is the type of the index to be created. Currently, only `files`, `column_stats` and `bloom_filters` is supported.
- `column_name` is the name of the column on which the index is created.
- Both index and column on which the index is created can be qualified with some options in the form of key-value pairs.

To see some examples of creating and using a functional index, please checkout the Spark SQL DDL
docs [here](/docs/next/sql_ddl#create-index). You can follow
the [RFC](https://github.com/apache/hudi/blob/master/rfc/rfc-63/rfc-63.md) and
the [JIRA](https://issues.apache.org/jira/browse/HUDI-512) to keep track of ongoing work on this feature.

### API changes

#### Record Merger API

The `HoodieRecordPayload` interface was deprecated in favor of the new `HoodieRecordMerger` interface in version 0.13.0.
The new interface has been further enhanced to support all kinds of merging operations. In particular, the new interface
supports partial merge and support custom checks before flushing merged records to disk. Please check
the [javadoc](https://github.com/apache/hudi/blob/3a1d4fb03b1ab8e3cf27073053a4fab0a56a26d2/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java)
of the API for more details.

#### New FileGroup Reader

In addition to key-based merging of records in log files with base files for queries on MOR table, we have implemented
position-based merging and skipping pages based on positions. The new reader has shown impressive performance gains for
**partial updates** with key-based merging. For a MOR table of size 1TB with 100 partitions and 80% random updates in
subsequent commits, the new reader is **5.7x faster** for snapshot queries with **70x reduced write amplification**.
However, for position-based merging, the gains are yet to be realized as filter pushdown support
is [in progress](https://github.com/apache/hudi/pull/10030). The new reader is enabled by default for all new tables.
Following configs are used to control the reader:
```
# enabled by default
hoodie.file.group.reader.enabled=true
hoodie.datasource.read.use.new.parquet.file.format=true
# need to enable position-based merging if required
hoodie.merge.use.record.positions=true
```

Few things to note for the new reader:

- It is only applicable to COW or MOR tables with base files in Parquet format.
- Only snapshot queries for COW table, and snapshot queries and read-optimized queries for MOR table are supported.
- Currently, the reader will not be able to push down the data filters to scan. It is recommended to use key-based
  merging for now.

You can follow [this GitHub issue](https://github.com/apache/hudi/issues/15965)
and [HUDI-6722](https://issues.apache.org/jira/browse/HUDI-6722) to keep track of ongoing work related to reader/writer
API changes and performance improvements.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351210).
