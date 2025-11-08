---
title: "Release 1.0.0-beta1"
sidebar_position: 7
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.0.0-beta1](https://github.com/apache/hudi/releases/tag/release-1.0.0-beta1)

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

[HUDI-6242](https://issues.apache.org/jira/browse/HUDI-6242) is the main epic covering all the format changes proposals,
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

You can follow [HUDI-6243](https://issues.apache.org/jira/browse/HUDI-6243)
and [HUDI-6722](https://issues.apache.org/jira/browse/HUDI-6722) to keep track of ongoing work related to reader/writer
API changes and performance improvements.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351210).
