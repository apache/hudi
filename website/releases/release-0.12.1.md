---
title: "Release 0.12.1"
sidebar_position: 13
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## [Release 0.12.1](https://github.com/apache/hudi/releases/tag/release-0.12.1)

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  The latest 0.12 release is [0.12.3](/releases/release-0.12.3).

## Migration Guide

* This release (0.12.1) does not introduce any new table version, thus no migration is needed if you are on 0.12.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), and [0.12.0](/releases/release-0.12.0).


## Release Highlights

### Improve Hudi Cli

Add command to repair deprecated partition, rename partition and trace file group through a range of commits.

### Fix invalid record key stats in Parquet metadata

Crux of the problem was that min/max statistics for the record keys were computed incorrectly during (Spark-specific) row-writing
Bulk Insert operation affecting Key Range Pruning flow w/in Hoodie Bloom Index tagging sequence, resulting into updated records being incorrectly tagged
as "inserts" and not as "updates", leading to duplicated records in the table.

If all of the following is applicable to you:

1. Using Spark as an execution engine
2. Using Bulk Insert (using row-writing
   https://hudi.apache.org/docs/next/configurations#hoodiedatasourcewriterowwriterenable,
   enabled *by default*)
3. Using Bloom Index (with range-pruning
   https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges
   enabled, enabled *by default*) for "UPSERT" operations

Recommended to upgrading to 0.12.1 to avoid getting duplicate records in your pipeline.

### Bug fixes

0.12.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including
* DeltaStreamer
* Table config
* Table services
* Metadata table
* Spark SQL support
* Presto support
* Hive Sync
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions

We discovered a regression in Hudi 0.12.1 release related to metadata table and timeline server interplay with streaming ingestion pipelines.

The FileSystemView that Hudi maintains internally could go out of sync due to a occasional race conditions when table services are involved
(compaction, clustering) and could result in updates and deletes routed to older file versions and hence resulting in missed updates and deletes.

Here are the user-flows that could potentially be impacted with this.

- This impacts pipelines using Deltastreamer in **continuous mode** (sync once is not impacted), Spark streaming, or if you have been directly
  using write client across batches/commits instead of the standard ways to write to Hudi. In other words, batch writes should not be impacted.
- Among these write models, this could have an impact only when table services are enabled.
    - COW: clustering enabled (inline or async)
    - MOR: compaction enabled (by default, inline or async)
- Also, the impact is applicable only when metadata table is enabled, and timeline server is enabled (which are defaults as of 0.12.1)

Based on some production data, we expect this issue might impact roughly < 1% of updates to be missed, since its a race condition
and table services are generally scheduled once every N commits. The percentage of update misses could be even less if the
frequency of table services is less.

[Here](https://issues.apache.org/jira/browse/HUDI-5863) is the jira for the issue of interest and the fix has already been landed in master.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8079). Until we have a 0.12.3 release, we recommend you to disable metadata table
(`hoodie.metadata.enable=false`) to mitigate the issue.

We also discovered a regression for Flink streaming writer with the hive meta sync which is introduced by HUDI-3730, the refactoring to `HiveSyncConfig`
causes the Hive `Resources` config objects leaking, which finally leads to an OOM exception for the JobManager if the streaming job runs continuously for weeks.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8050). Until we have a 0.12.3 release, we recommend you to cherry-pick the fix to local
if hive meta sync is required.

Sorry about the inconvenience caused.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12352182)
