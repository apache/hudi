---
title: "Release 1.0.0"
layout: releases
toc: true
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

 - Same bundles supported in the [0.15.0 release](release-0.15.0#new-spark-bundles) are still supported.
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
- We also have a ComplexKeyGenerator related regression reported [here](release-0.14.1.md#known-regressions). Please refrain from migrating, if you have single field as record key and multiple fields as partition fields.

:::tip
Avoid upgrading any existing table to 1.0.0 if any of the above scenario matches your workload. Incase of backwards compatible writer for MOR table, you are good to upgrade to 1.0.2 release. 
:::

