---
title: "Release 1.0.0-beta2"
sidebar_position: 3
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.0.0-beta2](https://github.com/apache/hudi/releases/tag/release-1.0.0-beta2) ([docs](/docs/next/quick-start-guide))

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
which are also partly covered in the [Hudi 1.0 tech specification](/tech-specs-1point0). The following are the main
changes in this release:

#### Timeline

No major changes in this release. Refer to [1.0.0-beta1#timeline](release-1.0.0-beta1.md#timeline) for more details.

#### Log File Format

In addition to the fields in the log file header added in [1.0.0-beta1](release-1.0.0-beta1.md#log-file-format), we also
store a flag, `IS_PARTIAL` to indicate whether the log block contains [partial updates](TODO: add link) or not.

### Partial Updates

Partial updates are a new feature in Hudi 1.0.0-beta2. Partial updates allow users to update only a subset of columns in
a record, without having to write the entire record. This is particularly useful when dealing with large records, where
only a few columns need to be updated. Partial updates are supported in both Copy-On-Write (COW) and Merge-On-Read (MOR)
tables? (TODO: confirm this).

### Secondary Indexes

Secondary indexes are a new feature in Hudi 1.0.0-beta2. Secondary indexes allow users to create indexes on columns that
are not part of record key columns in Hudi tables (for record key fields, Hudi
supports [Record-level Index](../blog/2023-11-01-record-level-index.md)). Secondary indexes can be used to speed up
queries with predicate on columns other than record key columns. Secondary indexes are supported in both Copy-On-Write (
COW) and Merge-On-Read (MOR) tables.

### Partition Stats Index

Partition stats index is a new feature in Hudi 1.0.0-beta2. Partition stats index allows users to create indexes on
partition columns in Hudi tables. Partition stats index can be used to speed up queries with predicate on partition
columns. Partition stats index is supported in both Copy-On-Write (COW) and Merge-On-Read (MOR) tables.