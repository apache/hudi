---
title: "Release 0.12.1"
sidebar_position: 4
layout: releases
toc: true
last_modified_at: 2022-08-17T10:30:00+05:30
---
# [Release 0.12.1](https://github.com/apache/hudi/releases/tag/release-0.12.1) ([docs](/docs/quick-start-guide))

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  The latest 0.12 release is [0.12.2](/releases/release-0.12.2).

## Migration Guide

* This release (0.12.1) does not introduce any new table version, thus no migration is needed if you are on 0.12.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/older-releases#migration-guide-for-this-release),
  [0.9.0](/releases/release-0.9.0#migration-guide-for-this-release), [0.10.0](/releases/release-0.10.0#migration-guide),
  [0.11.0](/releases/release-0.11.0#migration-guide), and [0.12.0](/releases/release-0.12.0#migration-guide).


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
   <https://hudi.apache.org/docs/next/configurations#hoodiedatasourcewriterowwriterenable>,
   enabled *by default*)
3. Using Bloom Index (with range-pruning
   <https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges>
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

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12352182)