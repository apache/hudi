---
title: "Release 1.0.0-beta2"
sidebar_position: 6
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.0.0-beta2](https://github.com/apache/hudi/releases/tag/release-1.0.0-beta2)

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

[HUDI-6242](https://issues.apache.org/jira/browse/HUDI-6242) is the main epic covering all the format changes proposals,
which are also partly covered in the [Hudi 1.0 tech specification](/learn/tech-specs-1point0). The following are the main
changes in this release:

#### Timeline

No major changes in this release. Refer to [1.0.0-beta1#timeline](release-1.0.0-beta1.md#timeline) for more details.

#### Log File Format

In addition to the fields in the log file header added in [1.0.0-beta1](release-1.0.0-beta1.md#log-file-format), we also
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

In 1.0.0-beta1, we added a new [filegroup reader](/releases/release-1.0.0-beta1#new-filegroup-reader), which provides
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
